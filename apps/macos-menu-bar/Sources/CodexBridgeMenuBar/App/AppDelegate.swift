import AppKit
import Darwin
import Foundation

@main
enum CodexBridgeMenuBarMain {
    static func main() {
        let app = NSApplication.shared
        let delegate = AppDelegate()
        app.delegate = delegate
        app.setActivationPolicy(.accessory)
        app.run()
        _ = delegate
    }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    private let client = BridgeCommandClient()
    private let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
    private var status: BridgeStatus = .loading
    private var isWorking = false
    private var lastUpdated: Date?
    private var lastError: String?
    private var stateWatchSources: [DispatchSourceFileSystemObject] = []
    private var watchedStateFolderPath: String?
    private var observedRefreshTask: Task<Void, Never>?
    private var periodicRefreshTimer: Timer?

    private struct ModePresentation {
        var menuTitle: String
        var shortTitle: String
        var systemImage: String
        var statusTitle: String
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        configureStatusItem()
        startPeriodicRefresh()
        rebuildMenu()
        Task { await refresh() }
    }

    func applicationWillTerminate(_ notification: Notification) {
        stopStateMonitoring()
        periodicRefreshTimer?.invalidate()
        observedRefreshTask?.cancel()
    }

    private func configureStatusItem() {
        guard let button = statusItem.button else {
            return
        }
        button.imagePosition = .imageLeading
        button.target = self
        button.action = #selector(showMenu(_:))
        updateStatusItemButton()
    }

    @objc private func showMenu(_ sender: NSStatusBarButton) {
        Task {
            await refresh()
            statusItem.menu?.popUp(positioning: nil, at: NSPoint(x: 0, y: sender.bounds.height), in: sender)
        }
    }

    private func rebuildMenu() {
        let menu = NSMenu()
        menu.addItem(disabledItem(statusTitle))
        menu.addItem(disabledItem(status.detail))
        menu.addItem(disabledItem(connectionTitle))
        menu.addItem(disabledItem(queueTitle))
        menu.addItem(disabledItem(channelTitle))

        if let lastUpdated {
            menu.addItem(disabledItem("Updated: \(lastUpdated.formatted(date: .omitted, time: .standard))"))
        }

        if let issue = currentIssue {
            menu.addItem(.separator())
            menu.addItem(disabledItem("Issue: \(truncated(issue))"))
        }

        menu.addItem(.separator())
        menu.addItem(primaryRemoteActionItem())
        menu.addItem(actionItem("Repair Connection", #selector(repair(_:)), enabled: !isWorking))
        menu.addItem(actionItem("Refresh Status", #selector(refreshFromMenu(_:)), enabled: !isWorking))

        menu.addItem(.separator())
        menu.addItem(actionItem("Open Config File", #selector(openConfig(_:)), enabled: status.configPath != nil))
        menu.addItem(actionItem("Open State Folder", #selector(openStateFolder(_:)), enabled: status.stateFolderPath != nil))

        menu.addItem(.separator())
        menu.addItem(actionItem("Quit Codex Bridge", #selector(quit(_:)), enabled: true))
        statusItem.menu = menu
    }

    private func disabledItem(_ title: String) -> NSMenuItem {
        let item = NSMenuItem(title: title, action: nil, keyEquivalent: "")
        item.isEnabled = false
        return item
    }

    private func actionItem(_ title: String, _ action: Selector, enabled: Bool) -> NSMenuItem {
        let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
        item.target = self
        item.isEnabled = enabled
        return item
    }

    private func primaryRemoteActionItem() -> NSMenuItem {
        switch status.mode {
        case .away:
            return actionItem("Stop Remote Mode", #selector(turnBack(_:)), enabled: !isWorking)
        case .back:
            return actionItem("Start Remote Mode", #selector(turnAway(_:)), enabled: !isWorking)
        case .unavailable, .unknown:
            return actionItem("Start Remote Mode", #selector(turnAway(_:)), enabled: false)
        }
    }

    @objc private func turnAway(_ sender: NSMenuItem) {
        Task { await runAction { try await self.client.turnAway() } }
    }

    @objc private func turnBack(_ sender: NSMenuItem) {
        Task { await runAction { try await self.client.turnBack() } }
    }

    @objc private func repair(_ sender: NSMenuItem) {
        Task { await runAction { try await self.client.repair() } }
    }

    @objc private func refreshFromMenu(_ sender: NSMenuItem) {
        Task { await refresh() }
    }

    @objc private func openConfig(_ sender: NSMenuItem) {
        openPath(status.configPath)
    }

    @objc private func openStateFolder(_ sender: NSMenuItem) {
        openPath(status.stateFolderPath)
    }

    @objc private func quit(_ sender: NSMenuItem) {
        NSApp.terminate(nil)
    }

    private func refresh() async {
        guard !isWorking else {
            return
        }
        isWorking = true
        updateStatusItemButton()
        rebuildMenu()

        do {
            try await updateStatus()
            lastError = nil
        } catch {
            lastError = error.localizedDescription
        }

        isWorking = false
        updateStatusItemButton()
        rebuildMenu()
    }

    private func runAction(_ action: @escaping () async throws -> BridgeStatus) async {
        guard !isWorking else {
            return
        }
        isWorking = true
        updateStatusItemButton()
        rebuildMenu()

        do {
            status = try await action()
            lastUpdated = Date()
            lastError = nil
        } catch {
            lastError = error.localizedDescription
            try? await updateStatus()
        }

        isWorking = false
        updateStatusItemButton()
        rebuildMenu()
    }

    private func updateStatus() async throws {
        status = try await client.status()
        lastUpdated = Date()
        configureStateMonitoring(folderPath: status.stateFolderPath)
    }

    private func startPeriodicRefresh() {
        periodicRefreshTimer?.invalidate()
        let timer = Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            Task { @MainActor in
                await self?.refresh()
            }
        }
        timer.tolerance = 5
        periodicRefreshTimer = timer
    }

    private func configureStateMonitoring(folderPath: String?) {
        guard let folderPath, !folderPath.isEmpty, watchedStateFolderPath != folderPath else {
            return
        }

        stopStateMonitoring()
        watchedStateFolderPath = folderPath

        for filename in ["remote-mode.json", "live-backend.json"] {
            startMonitoring(path: URL(fileURLWithPath: folderPath).appendingPathComponent(filename).path)
        }
    }

    private func startMonitoring(path: String) {
        guard FileManager.default.fileExists(atPath: path) else {
            return
        }

        let fileDescriptor = open(path, O_EVTONLY)
        guard fileDescriptor >= 0 else {
            return
        }

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: fileDescriptor,
            eventMask: [.write, .delete, .rename],
            queue: .main
        )
        source.setEventHandler { [weak self] in
            Task { @MainActor in
                self?.scheduleObservedStateRefresh()
            }
        }
        source.setCancelHandler {
            close(fileDescriptor)
        }
        source.resume()
        stateWatchSources.append(source)
    }

    private func stopStateMonitoring() {
        stateWatchSources.forEach { $0.cancel() }
        stateWatchSources.removeAll()
        watchedStateFolderPath = nil
    }

    private func scheduleObservedStateRefresh() {
        observedRefreshTask?.cancel()
        observedRefreshTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 350_000_000)
            await self?.refresh()
        }
    }

    private func updateStatusItemButton() {
        guard let button = statusItem.button else {
            return
        }
        let image = NSImage(systemSymbolName: menuSystemImage, accessibilityDescription: menuTitle)
        image?.isTemplate = true
        button.image = image
        button.title = " \(shortTitle)"
        button.toolTip = menuTitle
    }

    private var menuTitle: String {
        if isWorking {
            return "Codex remote mode is updating"
        }
        return modePresentation.menuTitle
    }

    private var shortTitle: String {
        if isWorking {
            return "Codex"
        }
        return modePresentation.shortTitle
    }

    private var menuSystemImage: String {
        if isWorking {
            return "arrow.triangle.2.circlepath"
        }
        return modePresentation.systemImage
    }

    private var statusTitle: String {
        modePresentation.statusTitle
    }

    private var modePresentation: ModePresentation {
        switch status.mode {
        case .away:
            return ModePresentation(
                menuTitle: "Codex remote mode is on",
                shortTitle: "Remote",
                systemImage: "paperplane.fill",
                statusTitle: "Remote Mode: On"
            )
        case .back:
            return ModePresentation(
                menuTitle: "Codex remote mode is off",
                shortTitle: "Local",
                systemImage: "desktopcomputer",
                statusTitle: "Remote Mode: Off"
            )
        case .unavailable:
            return ModePresentation(
                menuTitle: "Codex bridge setup needed",
                shortTitle: "Setup",
                systemImage: "exclamationmark.triangle",
                statusTitle: "Remote Mode: Setup Needed"
            )
        case .unknown:
            return ModePresentation(
                menuTitle: "Codex bridge status unknown",
                shortTitle: "Codex",
                systemImage: "questionmark.circle",
                statusTitle: "Remote Mode: Checking"
            )
        }
    }

    private var connectionTitle: String {
        switch status.backendState {
        case "idle":
            return "Connection: Idle"
        case "ready":
            return "Connection: Ready"
        case "blocked":
            return "Connection: Needs Attention"
        case "unhealthy" where status.backendRequired:
            return "Connection: Needs Attention"
        case "unhealthy":
            return "Connection: Idle"
        default:
            break
        }
        switch status.backendHealthy {
        case .some(true):
            return "Connection: Ready"
        case .some(false):
            return status.backendRequired ? "Connection: Needs Attention" : "Connection: Idle"
        case .none:
            return "Connection: Not Checked"
        }
    }

    private var queueTitle: String {
        if status.pendingNotifications == 0 {
            return "Queued Updates: None"
        }
        if status.pendingNotifications == 1 {
            return "Queued Updates: 1"
        }
        return "Queued Updates: \(status.pendingNotifications)"
    }

    private var channelTitle: String {
        let telegram = status.telegramConfigured ? "Telegram On" : "Telegram Setup"
        return "Channel: \(telegram)"
    }

    private var currentIssue: String? {
        lastError ?? status.backendError
    }

    private func truncated(_ value: String) -> String {
        if value.count <= 80 {
            return value
        }
        return "\(value.prefix(77))..."
    }

    private func openPath(_ path: String?) {
        guard let path, !path.isEmpty else {
            return
        }
        NSWorkspace.shared.open(URL(fileURLWithPath: path))
    }
}

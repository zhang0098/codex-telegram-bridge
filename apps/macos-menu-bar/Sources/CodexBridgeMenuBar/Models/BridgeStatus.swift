import Foundation

struct BridgeStatus: Equatable {
    enum Mode: Equatable {
        case unknown
        case away
        case back
        case unavailable
    }

    var mode: Mode
    var backendHealthy: Bool?
    var backendState: String?
    var backendRequired: Bool
    var backendError: String?
    var pendingNotifications: Int
    var telegramConfigured: Bool
    var configPath: String?
    var stateFolderPath: String?
    var detail: String

    static let loading = BridgeStatus(
        mode: .unknown,
        backendHealthy: nil,
        backendState: nil,
        backendRequired: false,
        backendError: nil,
        pendingNotifications: 0,
        telegramConfigured: false,
        configPath: nil,
        stateFolderPath: nil,
        detail: "Loading"
    )

    init(payload: BridgeStatusPayload) {
        let isAway = payload.away?.away
        let configured = payload.configured ?? false
        let codexConfigured = payload.codexConfigured ?? false

        self.backendHealthy = payload.backend?.healthy
        self.backendState = payload.backend?.state
        self.backendRequired = payload.backend?.required ?? false
        self.backendError = self.backendRequired ? payload.backend?.lastError : nil
        self.pendingNotifications = payload.pending ?? 0
        self.telegramConfigured = payload.telegramConfigured ?? false
        self.configPath = payload.configPath
        self.stateFolderPath = payload.stateFolderPath

        if !configured || !codexConfigured {
            self.mode = .unavailable
            self.detail = "Run setup to enable remote control."
        } else if isAway == true {
            self.mode = .away
            self.detail = "Remote notifications are on."
        } else if isAway == false {
            self.mode = .back
            self.detail = "Remote notifications are off."
        } else {
            self.mode = .unknown
            self.detail = "Status is not available yet."
        }
    }

    private init(
        mode: Mode,
        backendHealthy: Bool?,
        backendState: String?,
        backendRequired: Bool,
        backendError: String?,
        pendingNotifications: Int,
        telegramConfigured: Bool,
        configPath: String?,
        stateFolderPath: String?,
        detail: String
    ) {
        self.mode = mode
        self.backendHealthy = backendHealthy
        self.backendState = backendState
        self.backendRequired = backendRequired
        self.backendError = backendError
        self.pendingNotifications = pendingNotifications
        self.telegramConfigured = telegramConfigured
        self.configPath = configPath
        self.stateFolderPath = stateFolderPath
        self.detail = detail
    }
}

struct BridgeStatusPayload: Decodable {
    let configured: Bool?
    let codexConfigured: Bool?
    let telegramConfigured: Bool?
    let away: BridgeAwayPayload?
    let backend: BridgeBackendPayload?
    let pending: Int?
    let configPath: String?
    let stateFolderPath: String?
}

struct BridgeAwayPayload: Decodable {
    let away: Bool?
}

struct BridgeBackendPayload: Decodable {
    let healthy: Bool?
    let required: Bool?
    let state: String?
    let lastError: String?
}

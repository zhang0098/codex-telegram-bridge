import Foundation

enum BridgeCommandError: LocalizedError {
    case missingBinary([String])
    case invalidJSON(String)
    case commandFailed(code: Int32, message: String)

    var errorDescription: String? {
        switch self {
        case let .missingBinary(candidates):
            return "Could not find codex-telegram-bridge. Checked: \(candidates.joined(separator: ", "))"
        case let .invalidJSON(output):
            return "Bridge returned invalid JSON: \(output)"
        case let .commandFailed(_, message):
            return message
        }
    }
}

struct BridgeCommandClient {
    private let resolver = BridgeBinaryResolver()

    func status() async throws -> BridgeStatus {
        try await runStatus(arguments: ["remote", "status"])
    }

    func turnAway() async throws -> BridgeStatus {
        try await runStatus(arguments: ["remote", "on"])
    }

    func turnBack() async throws -> BridgeStatus {
        try await runStatus(arguments: ["remote", "off"])
    }

    func repair() async throws -> BridgeStatus {
        try await runStatus(arguments: ["remote", "repair"])
    }

    private func runStatus(arguments: [String]) async throws -> BridgeStatus {
        let payload = try await runJSON(arguments: arguments, as: BridgeStatusPayload.self)
        return BridgeStatus(payload: payload)
    }

    private func runJSON<T: Decodable>(arguments: [String], as type: T.Type) async throws -> T {
        let output = try await run(arguments: arguments)
        let payloadData = output.stdout.isEmpty ? output.stderr : output.stdout
        if output.exitCode != 0 {
            throw BridgeCommandError.commandFailed(
                code: output.exitCode,
                message: Self.errorMessage(from: payloadData)
            )
        }
        do {
            return try JSONDecoder().decode(type, from: payloadData)
        } catch {
            let output = String(data: payloadData, encoding: .utf8) ?? ""
            throw BridgeCommandError.invalidJSON(output)
        }
    }

    private func run(arguments: [String]) async throws -> CommandOutput {
        let binary = try resolver.resolve()
        return try await Task.detached(priority: .userInitiated) {
            let process = Process()
            process.executableURL = binary
            process.arguments = arguments
            process.environment = Self.processEnvironment()

            let stdout = Pipe()
            let stderr = Pipe()
            process.standardOutput = stdout
            process.standardError = stderr

            try process.run()
            process.waitUntilExit()

            return CommandOutput(
                stdout: stdout.fileHandleForReading.readDataToEndOfFile(),
                stderr: stderr.fileHandleForReading.readDataToEndOfFile(),
                exitCode: process.terminationStatus
            )
        }.value
    }

    private static func processEnvironment() -> [String: String] {
        var environment = ProcessInfo.processInfo.environment
        var defaultPath = [
            "/opt/homebrew/bin",
            "/opt/homebrew/sbin",
            "/usr/local/bin",
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin"
        ]
        if let home = environment["HOME"], !home.isEmpty {
            defaultPath.insert("\(home)/.local/bin", at: 0)
            defaultPath.insert("\(home)/.cargo/bin", at: 0)
        }
        environment["PATH"] = [environment["PATH"], defaultPath.joined(separator: ":")]
            .compactMap { $0 }
            .joined(separator: ":")
        return environment
    }

    private static func errorMessage(from data: Data) -> String {
        if let payload = try? JSONDecoder().decode(BridgeFailurePayload.self, from: data),
           let message = payload.bestMessage {
            return message
        }
        let output = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
        return output?.isEmpty == false ? output! : "Bridge command failed"
    }
}

struct CommandOutput {
    var stdout: Data
    var stderr: Data
    var exitCode: Int32
}

private struct GenericBridgePayload: Decodable {}

private struct BridgeFailurePayload: Decodable {
    let topLevelMessage: String?
    let error: BridgeFailureError?

    enum CodingKeys: String, CodingKey {
        case topLevelMessage = "message"
        case error
    }

    var bestMessage: String? {
        error?.message ?? topLevelMessage
    }
}

private struct BridgeFailureError: Decodable {
    let message: String?
}

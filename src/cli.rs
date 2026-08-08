use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "codex-telegram-bridge")]
#[command(version)]
#[command(about = "Control Codex locally and keep working through Telegram when away")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    #[command(
        about = "Configure Telegram delivery, shared live backend, daemon service, and optional Hermes MCP"
    )]
    Setup {
        #[arg(long)]
        bot_token: Option<String>,
        #[arg(long)]
        chat_id: Option<String>,
        #[arg(long)]
        allowed_user_id: Option<String>,
        #[arg(long, default_value = crate::DEFAULT_NOTIFICATION_EVENTS)]
        events: String,
        #[arg(long, default_value = "codex-telegram-bridge")]
        bridge_command: String,
        #[arg(long, default_value = crate::DEFAULT_CODEX_WEBSOCKET_URL)]
        websocket_url: String,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        daemon_label: String,
        #[arg(long = "no-install-daemon", default_value_t = true, action = clap::ArgAction::SetFalse)]
        install_daemon: bool,
        #[arg(long = "no-start-daemon", default_value_t = true, action = clap::ArgAction::SetFalse)]
        start_daemon: bool,
        #[arg(long, default_value_t = false)]
        register_hermes: bool,
        #[arg(long, default_value = "codex")]
        hermes_server_name: String,
        #[arg(long, default_value = "hermes")]
        hermes_command: String,
        #[arg(long, default_value_t = 60_000)]
        pair_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Inspect Codex setup")]
    Doctor,
    #[command(about = "Choose whether Codex may notify you remotely")]
    Away {
        #[command(subcommand)]
        command: AwayCommands,
    },
    #[command(about = "Control full remote Codex mode for local desktop integrations")]
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
    #[command(about = "List recent Codex threads")]
    Threads {
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(hide = true, about = "Stream thread updates")]
    Follow {
        thread_id: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = 3000)]
        duration: u64,
        #[arg(long = "poll-interval", default_value_t = 1000)]
        poll_interval: u64,
        #[arg(long)]
        events: Option<String>,
    },
    #[command(hide = true, about = "Unarchive a Codex thread")]
    Unarchive {
        thread_id: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "List threads waiting for user attention")]
    Waiting {
        #[arg(long)]
        project: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(about = "List actionable thread inbox rows")]
    Inbox {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        attention: Option<String>,
        #[arg(long = "waiting-on")]
        waiting_on: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(hide = true, about = "Watch for thread changes and emit JSON events")]
    Watch {
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long)]
        exec: Option<String>,
        #[arg(long)]
        events: Option<String>,
    },
    #[command(about = "Run and manage the proactive Codex notification daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    #[command(
        about = "Configure direct Telegram delivery, shared live backend, and reply routing"
    )]
    Telegram {
        #[command(subcommand)]
        command: TelegramCommands,
    },
    #[command(about = "Configure Discord delivery, shared live backend, and reply routing")]
    Discord {
        #[command(subcommand)]
        command: DiscordCommands,
    },
    #[command(about = "Manage the curated project registry for Telegram-created threads")]
    Projects {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    #[command(
        hide = true,
        about = "Sync live Codex thread state into the local cache"
    )]
    Sync {
        #[arg(long, default_value_t = 50)]
        limit: u64,
    },
    #[command(hide = true, about = "Start a new Codex thread")]
    New {
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = false)]
        stream: bool,
        #[arg(long, default_value_t = 3000)]
        duration: u64,
        #[arg(long = "poll-interval", default_value_t = 1000)]
        poll_interval: u64,
        #[arg(long)]
        events: Option<String>,
        prompt: Vec<String>,
    },
    #[command(hide = true, about = "Fork a Codex thread")]
    Fork {
        thread_id: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = false)]
        stream: bool,
        #[arg(long, default_value_t = 3000)]
        duration: u64,
        #[arg(long = "poll-interval", default_value_t = 1000)]
        poll_interval: u64,
        #[arg(long)]
        events: Option<String>,
        prompt: Vec<String>,
    },
    #[command(hide = true, about = "Archive explicit threads or selected inbox rows")]
    Archive {
        #[arg(long = "thread-id")]
        thread_id_option: Option<String>,
        thread_ids: Vec<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        attention: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        yes: bool,
    },
    #[command(about = "Show a thread with derived bridge state")]
    Show { thread_id: String },
    #[command(about = "Reply to a waiting Codex thread")]
    Reply {
        thread_id: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = false)]
        stream: bool,
        #[arg(long, default_value_t = 3000)]
        duration: u64,
        #[arg(long = "poll-interval", default_value_t = 1000)]
        poll_interval: u64,
        #[arg(long)]
        events: Option<String>,
        prompt: Vec<String>,
    },
    #[command(about = "Approve or deny a waiting approval prompt")]
    Approve {
        thread_id: String,
        #[arg(long)]
        decision: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = false)]
        stream: bool,
        #[arg(long, default_value_t = 3000)]
        duration: u64,
        #[arg(long = "poll-interval", default_value_t = 1000)]
        poll_interval: u64,
        #[arg(long)]
        events: Option<String>,
        positional_decision: Option<String>,
    },
    #[command(about = "Reset runtime state to initial (keeps config.json)")]
    Reset {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Run a stdio MCP server for Hermes")]
    Mcp,
    #[command(about = "Install or inspect Hermes integration helpers")]
    Hermes {
        #[command(subcommand)]
        command: HermesCommands,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AwayCommands {
    On,
    Off,
    Status,
}

#[derive(Subcommand, Debug)]
pub(crate) enum RemoteCommands {
    #[command(about = "Start or reuse the shared live backend and enable away mode")]
    On,
    #[command(about = "Disable away mode and clear pending Telegram notifications")]
    Off,
    #[command(about = "Restart the shared live backend and keep away mode enabled")]
    Repair,
    #[command(about = "Show remote mode, backend, and pending notification state")]
    Status,
}

#[derive(Subcommand, Debug)]
pub(crate) enum HermesCommands {
    #[command(about = "Register this bridge with Hermes MCP")]
    Install {
        #[arg(long, default_value = "codex")]
        server_name: String,
        #[arg(long, default_value = "hermes")]
        hermes_command: String,
        #[arg(long, default_value = "codex-telegram-bridge")]
        bridge_command: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum DaemonCommands {
    #[command(about = "Run the proactive Codex notification daemon")]
    Run {
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long = "poll-interval", default_value_t = 1500)]
        poll_interval: u64,
        #[arg(long, default_value_t = 10000)]
        timeout_ms: u64,
    },
    #[command(about = "Install the proactive notification daemon as a user service")]
    Install {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
        #[arg(long, default_value = "codex-telegram-bridge")]
        bridge_command: String,
    },
    #[command(about = "Uninstall the proactive notification daemon user service")]
    Uninstall {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Start the proactive notification daemon user service")]
    Start {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Stop the proactive notification daemon user service")]
    Stop {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Show proactive notification daemon status")]
    Status {
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Show proactive notification daemon log paths")]
    Logs {
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum TelegramCommands {
    #[command(about = "Configure the bridge-owned Telegram bot transport and shared live backend")]
    Setup {
        #[arg(long)]
        bot_token: Option<String>,
        #[arg(long)]
        chat_id: Option<String>,
        #[arg(long)]
        allowed_user_id: Option<String>,
        #[arg(long, default_value = crate::DEFAULT_NOTIFICATION_EVENTS)]
        events: String,
        #[arg(long, default_value = "codex-telegram-bridge")]
        bridge_command: String,
        #[arg(long, default_value = crate::DEFAULT_CODEX_WEBSOCKET_URL)]
        websocket_url: String,
        #[arg(long, default_value_t = 60_000)]
        pair_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Show direct Telegram transport configuration")]
    Status,
    #[command(about = "Enable direct Telegram delivery and bot polling without changing setup")]
    Enable {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Send a test Telegram notification using the bridge config")]
    Test {
        #[arg(long, default_value = "Codex Telegram bridge test")]
        message: String,
        #[arg(long, default_value_t = 10000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Disable direct Telegram delivery and bot polling without deleting setup")]
    Disable {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum DiscordCommands {
    #[command(about = "Configure the bridge-owned Discord bot transport and shared live backend")]
    Setup {
        #[arg(long)]
        bot_token: Option<String>,
        #[arg(long = "channel-id", required = true)]
        channel_ids: Vec<String>,
        #[arg(long)]
        allowed_user_id: Option<String>,
        #[arg(long, default_value = crate::DEFAULT_NOTIFICATION_EVENTS)]
        events: String,
        #[arg(long, default_value = "codex-telegram-bridge")]
        bridge_command: String,
        #[arg(long, default_value = crate::DEFAULT_CODEX_WEBSOCKET_URL)]
        websocket_url: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Show Discord transport configuration")]
    Status,
    #[command(about = "Enable Discord delivery and bot polling without changing setup")]
    Enable {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Send a test Discord notification using the bridge config")]
    Test {
        #[arg(long, default_value = "Codex Discord bridge test")]
        message: String,
        #[arg(long, default_value_t = 10000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Disable Discord delivery and bot polling without deleting setup")]
    Disable {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ProjectCommands {
    #[command(about = "List configured projects and observed workspace candidates")]
    List {
        #[arg(long, default_value_t = 10)]
        observed_limit: u64,
    },
    #[command(about = "Add one curated project to the registry")]
    Add {
        cwd: String,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long = "alias")]
        aliases: Vec<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Import curated project entries from observed Codex workspaces")]
    Import {
        #[arg(long, default_value_t = 25)]
        limit: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Remove one project from the curated registry")]
    Remove {
        id: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn cli_accepts_hermes_install_command() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "hermes",
            "install",
            "--server-name",
            "codex",
            "--hermes-command",
            "hermes-se",
            "--bridge-command",
            "codex-telegram-bridge",
            "--dry-run",
        ]);
        assert!(parsed.is_ok(), "hermes install should parse: {parsed:?}");
    }

    #[test]
    fn cli_accepts_mcp_server_command() {
        let parsed = Cli::try_parse_from(["codex-telegram-bridge", "mcp"]);
        assert!(parsed.is_ok(), "mcp command should parse: {parsed:?}");

        let mut command = Cli::command();
        let help = command.render_long_help().to_string();
        assert!(help.contains("Run a stdio MCP server for Hermes"));
    }

    #[test]
    fn cli_accepts_daemon_lifecycle_commands() {
        for args in [
            vec!["codex-telegram-bridge", "daemon", "run", "--once"],
            vec![
                "codex-telegram-bridge",
                "daemon",
                "install",
                "--dry-run",
                "--bridge-command",
                "codex-telegram-bridge",
            ],
            vec!["codex-telegram-bridge", "daemon", "start", "--dry-run"],
            vec!["codex-telegram-bridge", "daemon", "stop", "--dry-run"],
            vec!["codex-telegram-bridge", "daemon", "status"],
            vec!["codex-telegram-bridge", "daemon", "logs"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "daemon command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_accepts_ts_composed_follow_flags() {
        let new_cli = Cli::try_parse_from([
            "codex-telegram-bridge",
            "new",
            "--message",
            "hello",
            "--follow",
            "--stream",
            "--duration",
            "500",
            "--poll-interval",
            "100",
            "--events",
            "follow_snapshot,item_completed",
        ]);
        assert!(
            new_cli.is_ok(),
            "new should accept TS follow flags: {new_cli:?}"
        );

        let reply_cli = Cli::try_parse_from([
            "codex-telegram-bridge",
            "reply",
            "thr_1",
            "--message",
            "ok",
            "--follow",
            "--duration",
            "500",
            "--events",
            "item_completed",
        ]);
        assert!(
            reply_cli.is_ok(),
            "reply should accept TS follow flags: {reply_cli:?}"
        );

        let approve_cli = Cli::try_parse_from([
            "codex-telegram-bridge",
            "approve",
            "thr_1",
            "approve",
            "--follow",
            "--stream",
        ]);
        assert!(
            approve_cli.is_ok(),
            "approve should accept positional decision and follow flags: {approve_cli:?}"
        );

        let fork_cli = Cli::try_parse_from([
            "codex-telegram-bridge",
            "fork",
            "thr_1",
            "--message",
            "try again",
            "--follow",
        ]);
        assert!(
            fork_cli.is_ok(),
            "fork should accept TS follow flags: {fork_cli:?}"
        );
    }

    #[test]
    fn cli_accepts_telegram_setup_and_status_commands() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "telegram",
            "setup",
            "--bot-token",
            "123:abc",
            "--chat-id",
            "456",
            "--websocket-url",
            "ws://127.0.0.1:4500",
            "--dry-run",
        ])
        .expect("telegram setup should parse");
        match parsed.command {
            Commands::Telegram {
                command:
                    TelegramCommands::Setup {
                        websocket_url,
                        dry_run,
                        ..
                    },
            } => {
                assert_eq!(websocket_url, "ws://127.0.0.1:4500");
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }

        for args in [
            vec!["codex-telegram-bridge", "telegram", "status"],
            vec!["codex-telegram-bridge", "telegram", "enable", "--dry-run"],
            vec![
                "codex-telegram-bridge",
                "telegram",
                "test",
                "--message",
                "hello",
                "--dry-run",
            ],
            vec!["codex-telegram-bridge", "telegram", "disable", "--dry-run"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "telegram command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_accepts_discord_setup_and_status_commands() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "discord",
            "setup",
            "--bot-token",
            "secret",
            "--channel-id",
            "123",
            "--allowed-user-id",
            "456",
            "--websocket-url",
            "ws://127.0.0.1:4500",
            "--dry-run",
        ])
        .expect("discord setup should parse");
        match parsed.command {
            Commands::Discord {
                command:
                    DiscordCommands::Setup {
                        channel_ids,
                        websocket_url,
                        dry_run,
                        ..
                    },
            } => {
                assert_eq!(channel_ids, vec!["123"]);
                assert_eq!(websocket_url, "ws://127.0.0.1:4500");
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }

        for args in [
            vec!["codex-telegram-bridge", "discord", "status"],
            vec!["codex-telegram-bridge", "discord", "enable", "--dry-run"],
            vec![
                "codex-telegram-bridge",
                "discord",
                "test",
                "--message",
                "hello",
                "--dry-run",
            ],
            vec!["codex-telegram-bridge", "discord", "disable", "--dry-run"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "discord command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_accepts_status_surface_commands() {
        for args in [
            vec!["codex-telegram-bridge", "doctor"],
            vec!["codex-telegram-bridge", "daemon", "status"],
            vec!["codex-telegram-bridge", "telegram", "status"],
            vec!["codex-telegram-bridge", "discord", "status"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "status command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_rejects_legacy_hermes_notification_flags() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "hermes",
            "install",
            "--hermes-command",
            "hermes-se",
            "--webhook-deliver",
            "telegram",
            "--webhook-deliver-chat-id",
            "12345",
            "--webhook-secret",
            "test-secret",
            "--dry-run",
        ]);
        assert!(
            parsed.is_err(),
            "Hermes install should stay MCP-only; Telegram setup owns notifications"
        );
    }

    #[test]
    fn cli_rejects_legacy_hermes_post_webhook_command() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "hermes",
            "post-webhook",
            "--url",
            "http://localhost:8644/webhooks/codex-watch",
            "--secret",
            "test-secret",
        ]);
        assert!(
            parsed.is_err(),
            "hermes post-webhook should not be part of the product surface"
        );
    }

    #[test]
    fn cli_accepts_product_setup_command() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "setup",
            "--bot-token",
            "123:abc",
            "--chat-id",
            "456",
            "--allowed-user-id",
            "789",
            "--websocket-url",
            "ws://127.0.0.1:4500",
            "--no-install-daemon",
            "--no-start-daemon",
            "--dry-run",
        ])
        .expect("setup command should parse");
        match parsed.command {
            Commands::Setup {
                websocket_url,
                install_daemon,
                start_daemon,
                dry_run,
                ..
            } => {
                assert_eq!(websocket_url, "ws://127.0.0.1:4500");
                assert!(!install_daemon);
                assert!(!start_daemon);
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn cli_rejects_experimental_realtime_flag_for_stable_surface() {
        let parsed = Cli::try_parse_from([
            "codex-telegram-bridge",
            "follow",
            "thr_1",
            "--experimental-realtime",
        ]);
        assert!(
            parsed.is_err(),
            "experimental realtime should not be part of the stable v0.1 CLI"
        );
    }

    #[test]
    fn cli_accepts_ts_archive_filter_flags() {
        let cli = Cli::try_parse_from([
            "codex-telegram-bridge",
            "archive",
            "--thread-id",
            "thr_1,thr_2",
            "--project",
            "bridge",
            "--status",
            "active",
            "--attention",
            "completed",
            "--limit",
            "5",
            "--dry-run",
        ]);
        assert!(
            cli.is_ok(),
            "archive should accept TS filter flags: {cli:?}"
        );
    }

    #[test]
    fn cli_exposes_version_and_command_descriptions() {
        let mut command = Cli::command();
        assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));

        let help = command.render_long_help().to_string();
        assert!(help.contains("Inspect Codex setup"));
        assert!(help.contains("Configure Telegram delivery"));
        assert!(!help.contains("Stream thread updates"));
        assert!(!help.contains("Watch for thread changes"));
        assert!(!help.contains("Sync live Codex thread state"));
        assert!(!help.contains("Start a new Codex thread"));
        assert!(!help.contains("Fork a Codex thread"));
        assert!(!help.contains("Archive explicit threads"));
        assert!(!help.contains("Unarchive a Codex thread"));
    }

    #[test]
    fn cli_accepts_reset_command() {
        let parsed = Cli::try_parse_from(["codex-telegram-bridge", "reset", "--dry-run"])
            .expect("reset should parse");
        match parsed.command {
            Commands::Reset { dry_run } => assert!(dry_run),
            other => panic!("unexpected command: {other:?}"),
        }

        let parsed = Cli::try_parse_from(["codex-telegram-bridge", "reset"])
            .expect("reset without flags should parse");
        match parsed.command {
            Commands::Reset { dry_run } => assert!(!dry_run),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn cli_accepts_project_registry_commands() {
        for args in [
            vec!["codex-telegram-bridge", "projects", "list"],
            vec![
                "codex-telegram-bridge",
                "projects",
                "add",
                "/Users/hanifcarroll/projects/tools/codex-telegram-bridge",
                "--id",
                "bridge",
                "--label",
                "Codex Telegram Bridge",
                "--alias",
                "codex",
            ],
            vec![
                "codex-telegram-bridge",
                "projects",
                "import",
                "--limit",
                "10",
            ],
            vec![
                "codex-telegram-bridge",
                "projects",
                "remove",
                "bridge",
                "--dry-run",
            ],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "projects command should parse: {args:?}: {parsed:?}"
            );
        }
    }
}

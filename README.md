# codex-telegram-bridge

This project is a fork of [HanifCarroll/codex-telegram-bridge](https://github.com/HanifCarroll/codex-telegram-bridge).

`codex-telegram-bridge` lets a local assistant inspect and control Codex threads, and lets you keep working through Telegram when you explicitly mark yourself away.

The product rule is simple:

- when you are present at your computer, Codex does not send remote notifications
- when you send `/away` in Telegram or Discord, the bridge starts the shared local Codex backend and turns remote mode on
- replying directly to a bridge-sent Telegram or Discord message sends that reply back to the originating Codex thread
- `/back` stops outbound remote notifications again

Hermes is optional. It uses the local MCP server when you ask an agent to inspect, reply to, or approve Codex work. Hermes and MCP do not own notification delivery.

You do not need Hermes for the default product flow. A normal install is Codex plus Telegram or Discord plus the local daemon. Add Hermes only if you also want a Hermes agent to call the bridge tools directly.

## Core Product Surface

- Product setup: `setup`
- Presence gate: `away on`, `away off`, `away status`
- Full remote mode gate: `remote on`, `remote off`, `remote repair`, `remote status`
- Direct Telegram transport: `telegram setup/status/test/enable/disable`
- Direct Discord transport: `discord setup/status/test/enable/disable`
- Telegram remote controls: `/away`, `/back`, `/repair`, `/status`, `/threads`, `/help`, `/new`, `/project`
- Native macOS companion: `apps/macos-menu-bar`
- Proactive daemon: `daemon run/install/start/stop/status/logs`
- Project registry: `projects list/add/import/remove`
- Thread inspection: `threads`, `show`, `waiting`, `inbox`
- Thread actions: `reply`, `approve`

Advanced commands such as `sync`, `follow`, `watch`, `new`, `fork`, `archive`, `unarchive`, and `watch --exec` remain available for local automation and maintenance, but they are hidden from default help and are not the recommended OSS onboarding path.

## Optional Agent Adapter

MCP is an optional local adapter for Hermes and other trusted agent clients. It exposes only `doctor`, `threads`, `inbox`, `waiting`, `show`, `reply`, and `approve` through `codex-telegram-bridge mcp`.

MCP does not send proactive notifications, install the daemon, configure Telegram or Discord, read transport updates, or expose the advanced event stream. Use `setup`, `away`, and the daemon for the remote-control product flow.

## Install

Build locally:

```bash
cargo build
```

Install the binary:

```bash
cargo install --path .
```

Install from Git:

```bash
cargo install --git https://github.com/hanifcarroll/codex-telegram-bridge
```

Download a prebuilt archive from [GitHub Releases](https://github.com/HanifCarroll/codex-telegram-bridge/releases) when a tagged release is available.

Run through the wrapper without installing:

```bash
bin/codex-telegram-bridge --help
```

The wrapper prefers `target/release/codex-telegram-bridge`, falls back to `target/debug/codex-telegram-bridge`, and builds the release binary on first use.

If you need a manual recovery template instead of the interactive setup path, copy [examples/config.example.json](examples/config.example.json) to `~/.codex-telegram-bridge/config.json`, replace the placeholder values, and keep the file mode user-only (`chmod 600 ~/.codex-telegram-bridge/config.json`). For token-only setup from environment, see [examples/telegram.env.example](examples/telegram.env.example).

## Quick Start

Inspect your local Codex and bridge setup:

```bash
codex-telegram-bridge doctor
```

Configure Telegram and the daemon in one command:

```bash
codex-telegram-bridge setup --bot-token <telegram-bot-token>
```

No Hermes setup is required for Telegram notifications or Telegram replies.

To use Discord instead, create a new Discord application/bot, invite it to the target server/channel, then configure the bridge:

```bash
codex-telegram-bridge discord setup \
  --bot-token <discord-bot-token> \
  --channel-id <discord-channel-id>

# add more Discord targets by repeating --channel-id
codex-telegram-bridge discord setup \
  --bot-token <discord-bot-token> \
  --channel-id <openclaw-agent-channel-id> \
  --channel-id <hermes-agent-channel-id>
```

For non-interactive setup:

```bash
codex-telegram-bridge setup \
  --bot-token <telegram-bot-token> \
  --chat-id <telegram-chat-id> \
  --allowed-user-id <telegram-user-id>
```

Test Telegram delivery:

```bash
codex-telegram-bridge telegram test --message "Codex bridge is ready"
```

Turn on remote notifications when you leave your computer:

```text
/away
```

Send that command to your Telegram bot. It starts or reuses the shared local `codex app-server`, turns away mode on, and makes Telegram replies use the same live backend.

Turn them off when you are back:

```text
/back
```

Optional: if you want Hermes to control Codex when you ask it directly:

```bash
codex-telegram-bridge hermes install --dry-run
codex-telegram-bridge hermes install
```

Restart Hermes after registration so it reconnects to MCP servers and discovers the `codex_*` tools.

## How It Works

`setup` writes `~/.codex-telegram-bridge/config.json` with user-only permissions, clears any existing Telegram webhook for the bot token, installs the local daemon service unless disabled, and can optionally register the Hermes MCP server.

The daemon runs locally. Each cycle:

1. syncs Codex thread state through the configured shared websocket backend
2. checks the local away state
3. enqueues new notification events only when away is on
4. sends queued events to Telegram and/or Discord
5. processes Telegram updates and Discord channel replies

Inbound Telegram and Discord replies are processed whenever the daemon is running and the shared live backend is reachable. Reply and approval handling starts the Codex turn and returns immediately; completed answers are picked up by the next daemon sync and delivered through the normal outbound notification path. The away gate only controls outbound notifications.

When an inbound reply, approval, or remote `/new` prompt starts a Codex turn, the daemon refreshes the platform typing indicator so Telegram and Discord show that the bot is working until the answer is delivered or the short-lived typing window expires.

Telegram notifications use a compact header, keep Codex's answer body verbatim, and omit internal thread ids. To continue the conversation remotely, use Telegram's Reply action on the specific Codex notification.

Use `/threads` in Telegram to fetch the 5 most recent Codex threads, or `/threads 10` to choose a count. The bridge sends one Telegram message per thread using the same compact update template, records each message id locally, and routes replies back to the matching Codex thread.

If replies stop reaching Codex, send `/repair` in Telegram. It restarts the shared local backend on the configured websocket URL and keeps remote mode on.

Telegram-created threads run in an explicit registered project working directory. Set the current project from Telegram with `/project <id>`, inspect choices with `/project`, or manage the registry locally with `codex-telegram-bridge projects ...`.

Use a Telegram bot token dedicated to this bridge. Telegram update delivery should have one owner.
Use a new Discord bot dedicated to this bridge when configuring Discord.

## Commands

### Setup And Doctor

```bash
codex-telegram-bridge setup --bot-token <telegram-bot-token>
codex-telegram-bridge setup --bot-token <telegram-bot-token> --register-hermes
codex-telegram-bridge doctor
```

Useful setup flags:

- `--chat-id <id>`: skip `/start` pairing
- `--allowed-user-id <id>`: restrict inbound replies/buttons to one Telegram user
- `--websocket-url <url>`: set the loopback shared Codex backend URL, default `ws://127.0.0.1:4500`
- `--no-install-daemon`: write config without installing a service
- `--no-start-daemon`: install without starting the service
- `--register-hermes`: also run `hermes mcp add`
- `--dry-run`: print the planned shape without changing files or services

### Presence Gate

```bash
codex-telegram-bridge away status
codex-telegram-bridge away on
codex-telegram-bridge away off
```

`away on` starts a new away session. The daemon only sends events observed after that session started, so old waiting threads do not flood Telegram when you leave. `away off` clears pending outbound notifications so delayed retries do not notify you after you return.

For desktop integrations such as the macOS menu bar app, use full remote mode instead:

```bash
codex-telegram-bridge remote status
codex-telegram-bridge remote on
codex-telegram-bridge remote off
codex-telegram-bridge remote repair
```

`remote on` starts or reuses the shared local `codex app-server` before enabling away mode. `remote off` behaves like `/back`: it disables away mode and clears pending Telegram notifications. `remote repair` restarts the shared backend and keeps remote mode on.

When remote mode is off, the shared backend is optional. A stopped backend is reported as idle instead of repair-needed. While remote mode is on, the daemon reconciles the bridge-owned backend automatically and keeps `remote repair` as the explicit force-reset path.

### macOS Menu Bar App

Build the native companion app from the repo root:

```bash
scripts/build_macos_menu_bar_app.sh
```

The script builds the Rust bridge and Swift menu bar executable, then writes:

```text
target/macos-menu-bar/Codex Bridge.app
```

The app embeds the bridge binary inside the bundle so it can run from Finder or Login Items without relying on a shell `PATH`. Its menu shows the next useful remote-mode action: `Start Remote Mode` when remote mode is off, and `Stop Remote Mode` when it is on. It also exposes `Repair Connection`, `Refresh Status`, config access, and state-folder access.

To install it as a normal local app, register it to open after restart, and launch it:

```bash
scripts/install_macos_menu_bar_app.sh
```

The installer copies the bundle to `~/Applications/Codex Bridge.app` by default. Use `--install-dir <path>`, `--no-login-item`, or `--no-open` when needed.

### Telegram

```bash
codex-telegram-bridge telegram setup --bot-token <telegram-bot-token>
codex-telegram-bridge telegram setup \
  --bot-token <telegram-bot-token> \
  --chat-id <telegram-chat-id> \
  --allowed-user-id <telegram-user-id>
codex-telegram-bridge telegram test --message "test"
codex-telegram-bridge telegram status
codex-telegram-bridge telegram enable
codex-telegram-bridge telegram disable
```

See [docs/telegram.md](docs/telegram.md).

### Discord

```bash
codex-telegram-bridge discord setup \
  --bot-token <discord-bot-token> \
  --channel-id <discord-channel-id>
codex-telegram-bridge discord test --message "test"
codex-telegram-bridge discord status
codex-telegram-bridge discord enable
codex-telegram-bridge discord disable
```

See [docs/discord.md](docs/discord.md).

Release mechanics are documented in [docs/releasing.md](docs/releasing.md).

### Projects

```bash
codex-telegram-bridge projects list
codex-telegram-bridge projects add /absolute/path --id bridge --label "Codex Telegram Bridge"
codex-telegram-bridge projects import --limit 25
codex-telegram-bridge projects remove bridge
```

Use the project registry to give Telegram-created threads deterministic working directories. `projects import` suggests entries from observed Codex thread `cwd` values in the local state cache; `projects add` is the explicit source of truth.

### Daemon

```bash
codex-telegram-bridge daemon run --once
codex-telegram-bridge daemon run
codex-telegram-bridge daemon install --dry-run
codex-telegram-bridge daemon install
codex-telegram-bridge daemon start
codex-telegram-bridge daemon stop
codex-telegram-bridge daemon status
codex-telegram-bridge daemon logs
```

`daemon install` writes a user service:

- macOS: `~/Library/LaunchAgents/com.hanifcarroll.codex-telegram-bridge.plist`
- Linux: `~/.config/systemd/user/com.hanifcarroll.codex-telegram-bridge.service`

### Inspect And Act On Threads

```bash
codex-telegram-bridge threads --limit 25
codex-telegram-bridge show <threadId>
codex-telegram-bridge waiting --limit 25
codex-telegram-bridge inbox --limit 25
codex-telegram-bridge reply <threadId> --message "please continue"
codex-telegram-bridge approve <threadId> --decision approve
```

### Follow And Watch

```bash
codex-telegram-bridge follow <threadId>
codex-telegram-bridge follow <threadId> --message "please continue" --duration 3000
codex-telegram-bridge follow <threadId> --events follow_snapshot,item_completed
codex-telegram-bridge watch --once
codex-telegram-bridge watch --events thread_waiting,thread_completed,item_completed
codex-telegram-bridge watch --exec "python3 examples/print-hook-event.py"
```

`watch --exec` is for trusted local automation. It pipes each event to the command on stdin.

### Optional Hermes MCP

```bash
codex-telegram-bridge mcp
codex-telegram-bridge hermes install --dry-run
codex-telegram-bridge hermes install
codex-telegram-bridge hermes install --hermes-command hermes-se
```

The MCP server exposes `doctor`, `threads`, `inbox`, `waiting`, `show`, `reply`, and `approve` tools, plus thread resources and safe prompts.

See [docs/hermes.md](docs/hermes.md).

## Event Schema

The stream is newline-delimited JSON. Common event types include:

- `watch_started`
- `thread_waiting`
- `thread_completed`
- `thread_status_changed`
- `turn_started`
- `item_started`
- `item_completed`
- `notification`
- `follow_started`
- `follow_snapshot`
- `follow_turn_started`
- `thread_error`

Example `thread_waiting` event:

```json
{
  "type": "thread_waiting",
  "threadId": "thr_123",
  "promptKind": "reply",
  "thread": {
    "threadId": "thr_123",
    "name": "Need reply",
    "statusType": "active"
  }
}
```

## Notes

- Hook commands are arbitrary local code. Only run trusted commands with `watch --exec`.
- MCP tools can read and mutate local Codex threads. Only register this server with trusted local agents.
- The Telegram bot token is stored in the local bridge config and redacted from command output.
- `doctor` is the fastest way to verify that the Codex binary and bridge configuration are discoverable from your environment.

# Changelog

All notable changes to `codex-telegram-bridge` will be documented here.

## Unreleased

No unreleased changes yet.

## 0.1.1 - 2026-08-08

- Remove the Discord transport entirely: the `discord` CLI surface, DiscordConfig, daemon polling/delivery paths, and `docs/discord.md` are gone.
- Remove the `/discord_on`, `/discord_off`, `/telegram_on`, and `/telegram_off` chat commands.
- Remove `telegram enable`/`telegram disable` and the `TelegramConfig.enabled` flag: Telegram is always treated as enabled once configured, so the daemon never silently stops polling the channel.
- Drop `telegramEnabled`/`discordEnabled` from `doctor`, `remote status`, `telegram status`, and the macOS menu bar app.
- Add a Chinese translation of the README with a language switcher on the repository homepage.
- Point install, release, and crate metadata URLs at the maintained repository.

## 0.1.0 - 2026-04-15

Initial OSS release candidate.

- Inspect Codex threads with `threads`, `show`, `waiting`, `inbox`, and `sync`.
- Take thread actions with `new`, `fork`, `reply`, `approve`, `archive`, and `unarchive`.
- Stream normalized JSON events with `follow` and `watch`.
- Run trusted local hooks with `watch --exec`.
- Configure the full product path with `setup`.
- Gate outbound Telegram notifications with `away on/off` so users are not notified while present.
- Send proactive Codex notifications directly through Telegram with `telegram setup`, `telegram test`, and the local daemon.
- Route Telegram reply-to-message text and approval buttons back to the originating Codex thread.
- Expose a stdio MCP server for Hermes with structured Codex control tools.
- Expose MCP resources and prompts for Codex thread context and safer Hermes workflows.
- Add a `hermes install` helper that registers the bridge through `hermes mcp add`.
- Prune legacy away-summary and hidden MCP control paths so the daemon and documented MCP tools are the only notification/control lanes.
- Hide advanced local sync, event-stream, and maintenance commands from default CLI help while keeping them available for automation.
- Reframe MCP as an optional local agent adapter while keeping Telegram away-mode as the primary product flow.
- Keep hook examples generic and leave Telegram delivery to the bridge daemon.

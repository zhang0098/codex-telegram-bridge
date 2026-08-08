# Codex Bridge Menu Bar

Native macOS menu bar companion for `codex-telegram-bridge`.

The app shells out to the bridge CLI and uses:

- `codex-telegram-bridge remote status`
- `codex-telegram-bridge remote on`
- `codex-telegram-bridge remote off`
- `codex-telegram-bridge remote repair`

For a packaged app bundle, run from the repo root:

```sh
scripts/build_macos_menu_bar_app.sh
```

The bundle is written to:

```text
target/macos-menu-bar/Codex Bridge.app
```

The script embeds the Rust bridge binary inside the app bundle so the app works when macOS launches it without a developer shell `PATH`.

From the repo root, install it into `~/Applications`, register it as a Login Item, and launch it:

```sh
scripts/install_macos_menu_bar_app.sh
```

Use `--install-dir <path>`, `--no-login-item`, or `--no-open` to change installer behavior.

Menu wording follows the bridge product model:

- `Remote Mode: On` means remote notifications are enabled for the active channels.
- `Remote Mode: Off` means remote notifications stay local.
- The menu shows the next useful remote-mode action and the configured Telegram channel state.
- The primary action is state-aware: off shows `Start Remote Mode`, on shows `Stop Remote Mode`.
- The menu refreshes from bridge state on open, observes `remote-mode.json` and `live-backend.json` for Telegram-side changes, and falls back to a periodic refresh.
- `Connection: Idle` means the shared backend is not required because remote mode is off.
- `Connection: Ready` means the shared backend is reachable.
- `Connection: Needs Attention` means automatic reconciliation could not make the backend usable; use `Repair Connection` or inspect the issue text.

For development:

```sh
cargo build
swift run --package-path apps/macos-menu-bar CodexBridgeMenuBar
```

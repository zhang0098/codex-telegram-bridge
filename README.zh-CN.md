# codex-telegram-bridge

[English](README.md) | **简体中文**

本项目是 [HanifCarroll/codex-telegram-bridge](https://github.com/HanifCarroll/codex-telegram-bridge) 的一个分支。

`codex-telegram-bridge` 让本地助手可以检查和控制 Codex 线程，并且当你明确标记自己离开时，通过 Telegram 继续远程工作。

产品规则很简单：

- 当你在电脑前时，Codex 不会发送远程通知
- 当你在 Telegram 发送 `/away` 时，bridge 会启动共享的本地 Codex 后端并开启远程模式
- 直接回复 bridge 发送的 Telegram 消息，回复会原样发回对应的 Codex 线程
- `/back` 再次关闭出站远程通知

Hermes 是可选的。当你让 agent 检查、回复或批准 Codex 工作时，它会使用本地 MCP 服务器。Hermes 和 MCP 不负责通知投递。

默认产品流程不需要 Hermes。常规安装是 Codex + Telegram + 本地 daemon。只有当你还希望 Hermes agent 直接调用 bridge 工具时，才需要添加 Hermes。

## 核心产品面

- 产品配置：`setup`
- 在场门控：`away on`、`away off`、`away status`
- 完整远程模式门控：`remote on`、`remote off`、`remote repair`、`remote status`
- 直接 Telegram 传输：`telegram setup/status/test`
- Telegram 远程控制：`/away`、`/back`、`/repair`、`/status`、`/threads`、`/help`、`/new`、`/project`
- 原生 macOS 伴生应用：`apps/macos-menu-bar`
- 主动通知 daemon：`daemon run/install/start/stop/status/logs`
- 项目注册表：`projects list/add/import/remove`
- 线程检查：`threads`、`show`、`waiting`、`inbox`
- 线程操作：`reply`、`approve`

`sync`、`follow`、`watch`、`new`、`fork`、`archive`、`unarchive`、`watch --exec` 等高级命令仍可用于本地自动化和维护，但它们在默认帮助中隐藏，也不是推荐的 OSS 入门路径。

## 可选 Agent 适配器

MCP 是面向 Hermes 和其他可信 agent 客户端的可选本地适配器。它通过 `codex-telegram-bridge mcp` 只暴露 `doctor`、`threads`、`inbox`、`waiting`、`show`、`reply` 和 `approve`。

MCP 不会发送主动通知、安装 daemon、配置 Telegram、读取传输更新或暴露高级事件流。远程控制产品流程请使用 `setup`、`away` 和 daemon。

## 安装

本地构建：

```bash
cargo build
```

安装二进制：

```bash
cargo install --path .
```

从 Git 安装：

```bash
cargo install --git https://github.com/zhang0098/codex-telegram-bridge
```

当有标签发布时，可以从 [GitHub Releases](https://github.com/zhang0098/codex-telegram-bridge/releases) 下载预构建压缩包。

不安装直接通过包装脚本运行：

```bash
bin/codex-telegram-bridge --help
```

包装脚本优先使用 `target/release/codex-telegram-bridge`，回退到 `target/debug/codex-telegram-bridge`，并在首次使用时构建 release 二进制。

如果你需要手动恢复模板而不是交互式配置路径，请将 [examples/config.example.json](examples/config.example.json) 复制到 `~/.codex-telegram-bridge/config.json`，替换占位值，并保持文件权限为仅当前用户（`chmod 600 ~/.codex-telegram-bridge/config.json`）。如需仅通过环境变量设置 token，请参阅 [examples/telegram.env.example](examples/telegram.env.example)。

## 快速开始

检查本地 Codex 和 bridge 配置：

```bash
codex-telegram-bridge doctor
```

一条命令配置 Telegram 和 daemon：

```bash
codex-telegram-bridge setup --bot-token <telegram-bot-token>
```

Telegram 通知和回复不需要 Hermes 配置。

非交互式配置：

```bash
codex-telegram-bridge setup \
  --bot-token <telegram-bot-token> \
  --chat-id <telegram-chat-id> \
  --allowed-user-id <telegram-user-id>
```

测试 Telegram 投递：

```bash
codex-telegram-bridge telegram test --message "Codex bridge is ready"
```

离开电脑时开启远程通知：

```text
/away
```

把这条命令发给你的 Telegram bot。它会启动或复用共享的本地 `codex app-server`、开启 away 模式，并让 Telegram 回复使用同一个实时后端。

回来后关闭：

```text
/back
```

可选：如果你希望直接让 Hermes 控制 Codex：

```bash
codex-telegram-bridge hermes install --dry-run
codex-telegram-bridge hermes install
```

注册后重启 Hermes，让它重新连接 MCP 服务器并发现 `codex_*` 工具。

## 工作原理

`setup` 以仅当前用户权限写入 `~/.codex-telegram-bridge/config.json`，清除该 bot token 的任何现有 Telegram webhook，安装本地 daemon 服务（除非禁用），并可选择注册 Hermes MCP 服务器。

daemon 在本地运行。每个周期：

1. 通过配置的共享 websocket 后端同步 Codex 线程状态
2. 检查本地 away 状态
3. 仅在 away 开启时入队新的通知事件
4. 将排队事件发送到 Telegram
5. 处理 Telegram 更新和回复

只要 daemon 在运行且共享实时后端可达，入站 Telegram 回复就会被处理。回复和审批处理会立即启动 Codex turn；完成的答案由下一次 daemon 同步拾取，并通过正常的出站通知路径投递。away 门控只控制出站通知。

当入站回复、审批或远程 `/new` 提示启动 Codex turn 时，daemon 会刷新平台的打字指示器，让 Telegram 显示 bot 正在工作，直到答案投递或短时打字窗口过期。

Telegram 通知使用紧凑的头部，原样保留 Codex 的答案正文，并省略内部线程 id。要远程继续对话，请对具体的 Codex 通知使用 Telegram 的 Reply 操作。

在 Telegram 中使用 `/threads` 获取最近 5 个 Codex 线程，或使用 `/threads 10` 指定数量。bridge 对每个线程发送一条使用相同紧凑更新模板的 Telegram 消息，在本地记录每个消息 id，并将回复路由回匹配的 Codex 线程。

如果回复无法到达 Codex，请在 Telegram 中发送 `/repair`。它会在配置的 websocket URL 上重启共享本地后端并保持远程模式开启。

Telegram 创建的线程在显式注册的项目工作目录中运行。用 Telegram 的 `/project <id>` 设置当前项目，用 `/project` 查看选项，或用 `codex-telegram-bridge projects ...` 在本地管理注册表。

使用专门用于此 bridge 的 Telegram bot token。Telegram 更新投递应只有一个所有者。

## 命令

### 配置和诊断

```bash
codex-telegram-bridge setup --bot-token <telegram-bot-token>
codex-telegram-bridge setup --bot-token <telegram-bot-token> --register-hermes
codex-telegram-bridge doctor
```

常用配置参数：

- `--chat-id <id>`：跳过 `/start` 配对
- `--allowed-user-id <id>`：将入站回复/按钮限制为单个 Telegram 用户
- `--websocket-url <url>`：设置环回共享 Codex 后端 URL，默认 `ws://127.0.0.1:4500`
- `--no-install-daemon`：只写配置，不安装服务
- `--no-start-daemon`：安装但不启动服务
- `--register-hermes`：同时运行 `hermes mcp add`
- `--dry-run`：打印预期结果而不修改文件或服务

### 在场门控

```bash
codex-telegram-bridge away status
codex-telegram-bridge away on
codex-telegram-bridge away off
```

`away on` 开启一个新的 away 会话。daemon 只发送该会话开始后观察到的事件，所以旧的等待线程不会在你离开时刷屏 Telegram。`away off` 清空待处理的出站通知，这样延迟重试不会在你回来后继续通知你。

桌面集成（如 macOS 菜单栏 App）请使用完整远程模式：

```bash
codex-telegram-bridge remote status
codex-telegram-bridge remote on
codex-telegram-bridge remote off
codex-telegram-bridge remote repair
```

`remote on` 在开启 away 模式之前启动或复用共享的本地 `codex app-server`。`remote off` 与 `/back` 行为一致：关闭 away 模式并清空待处理的 Telegram 通知。`remote repair` 重启共享后端并保持远程模式开启。

远程模式关闭时，共享后端是可选的。停止的后端会被报告为 idle 而不是需要修复。远程模式开启时，daemon 会自动协调 bridge 拥有的后端，并保留 `remote repair` 作为显式强制重置路径。

### macOS 菜单栏 App

从仓库根目录构建原生伴生应用：

```bash
scripts/build_macos_menu_bar_app.sh
```

该脚本构建 Rust bridge 和 Swift 菜单栏可执行文件，然后写入：

```text
target/macos-menu-bar/Codex Bridge.app
```

应用将 bridge 二进制嵌入 bundle，因此无需依赖 shell `PATH` 即可从 Finder 或登录项运行。它的菜单显示下一个有用的远程模式操作：远程模式关闭时显示 `Start Remote Mode`，开启时显示 `Stop Remote Mode`。它还提供 `Repair Connection`、`Refresh Status`、配置访问和状态文件夹访问。

安装为普通本地应用、注册开机启动并启动：

```bash
scripts/install_macos_menu_bar_app.sh
```

安装器默认将 bundle 复制到 `~/Applications/Codex Bridge.app`。需要时使用 `--install-dir <path>`、`--no-login-item` 或 `--no-open`。

### Telegram

```bash
codex-telegram-bridge telegram setup --bot-token <telegram-bot-token>
codex-telegram-bridge telegram setup \
  --bot-token <telegram-bot-token> \
  --chat-id <telegram-chat-id> \
  --allowed-user-id <telegram-user-id>
codex-telegram-bridge telegram test --message "test"
codex-telegram-bridge telegram status
```

参见 [docs/telegram.md](docs/telegram.md)。

发布机制记录在 [docs/releasing.md](docs/releasing.md)。

### 项目注册表

```bash
codex-telegram-bridge projects list
codex-telegram-bridge projects add /absolute/path --id bridge --label "Codex Telegram Bridge"
codex-telegram-bridge projects import --limit 25
codex-telegram-bridge projects remove bridge
```

使用项目注册表为 Telegram 创建的线程提供确定的工作目录。`projects import` 从本地状态缓存中观察到的 Codex 线程 `cwd` 值建议条目；`projects add` 是显式的权威来源。

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

`daemon install` 写入一个用户服务：

- macOS：`~/Library/LaunchAgents/com.hanifcarroll.codex-telegram-bridge.plist`
- Linux：`~/.config/systemd/user/com.hanifcarroll.codex-telegram-bridge.service`

### 检查和处理线程

```bash
codex-telegram-bridge threads --limit 25
codex-telegram-bridge show <threadId>
codex-telegram-bridge waiting --limit 25
codex-telegram-bridge inbox --limit 25
codex-telegram-bridge reply <threadId> --message "please continue"
codex-telegram-bridge approve <threadId> --decision approve
```

### Follow 和 Watch

```bash
codex-telegram-bridge follow <threadId>
codex-telegram-bridge follow <threadId> --message "please continue" --duration 3000
codex-telegram-bridge follow <threadId> --events follow_snapshot,item_completed
codex-telegram-bridge watch --once
codex-telegram-bridge watch --events thread_waiting,thread_completed,item_completed
codex-telegram-bridge watch --exec "python3 examples/print-hook-event.py"
```

`watch --exec` 适用于可信的本地自动化。它通过 stdin 将每个事件管道传给命令。

### 可选的 Hermes MCP

```bash
codex-telegram-bridge mcp
codex-telegram-bridge hermes install --dry-run
codex-telegram-bridge hermes install
codex-telegram-bridge hermes install --hermes-command hermes-se
```

MCP 服务器暴露 `doctor`、`threads`、`inbox`、`waiting`、`show`、`reply` 和 `approve` 工具，以及线程资源和安全提示。

参见 [docs/hermes.md](docs/hermes.md)。

## 事件模式

事件流是换行分隔的 JSON。常见事件类型包括：

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

`thread_waiting` 事件示例：

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

## 备注

- Hook 命令是任意本地代码。只对可信命令使用 `watch --exec`。
- MCP 工具可以读取和修改本地 Codex 线程。只向可信的本地 agent 注册此服务器。
- Telegram bot token 存储在本地 bridge 配置中，并从命令输出中脱敏。
- `doctor` 是验证 Codex 二进制和 bridge 配置在你的环境中可发现的最快方式。

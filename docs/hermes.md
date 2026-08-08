# Optional Hermes MCP Adapter

Hermes is an optional local agent adapter for `codex-telegram-bridge`.

Use Hermes when you want to ask an agent to inspect Codex work, reply to a thread, or approve a waiting prompt. Use `codex-telegram-bridge setup`, the bridge daemon, and Telegram for proactive notifications and remote reply routing.

MCP is not required for the core product loop. It does not send Telegram messages, install or run the daemon, process Telegram replies, or expose the event stream.

For Telegram setup, see [telegram.md](telegram.md).

## Install

From a local checkout:

```bash
cargo install --path .
```

From Git:

```bash
cargo install --git https://github.com/zhang0098/codex-telegram-bridge
```

Verify the bridge can resolve Codex:

```bash
codex-telegram-bridge doctor
```

## Register With Hermes

If your default `hermes` command targets the profile you use:

```bash
codex-telegram-bridge hermes install
```

If you use a profile wrapper:

```bash
codex-telegram-bridge hermes install --hermes-command hermes-se
```

Inspect the command without changing Hermes config:

```bash
codex-telegram-bridge hermes install --dry-run
```

The installer delegates to Hermes' own MCP config command:

```bash
hermes mcp add codex --command codex-telegram-bridge --args mcp
```

Restart Hermes after registration. Hermes discovers MCP tools when the profile starts.

## Manual Config

You can also edit the Hermes profile config directly:

```yaml
mcp_servers:
  codex:
    command: "codex-telegram-bridge"
    args: ["mcp"]
    connect_timeout: 60
    timeout: 180
    sampling:
      enabled: false
```

Use the profile config for the agent you actually run. For example, a profile wrapper may set `HERMES_HOME=$HOME/.hermes/profiles/software-engineering`.

## Verify

List configured MCP servers:

```bash
hermes mcp list
```

Test the bridge server:

```bash
hermes mcp test codex
```

Hermes tool names are usually prefixed by the MCP server name. With server name `codex`, expect tools such as:

- `mcp_codex_codex_doctor`
- `mcp_codex_codex_threads`
- `mcp_codex_codex_inbox`
- `mcp_codex_codex_reply`
- `mcp_codex_codex_approve`

## Bounded Tool Surface

The MCP server exposes these bridge tools:

- `codex_doctor`: inspect the local Codex executable and bridge configuration.
- `codex_threads`: sync and list recent threads.
- `codex_inbox`: list actionable inbox rows.
- `codex_waiting`: list threads waiting on user input or approval.
- `codex_show`: read one thread with derived state.
- `codex_reply`: resume and reply to a thread.
- `codex_approve`: send `YES` or `NO` for an approval prompt.

`setup`, `daemon`, `telegram`, `watch`, `follow`, `sync`, `new`, `fork`, `archive`, `unarchive`, and `away` are not exposed as MCP tools. The MCP lane stays bounded and action-focused; proactive notification belongs in the daemon and Telegram transport.

## Resources

The MCP server exposes bounded JSON resources for clients that prefer context reads over tool calls:

- `codex://threads`: recent thread summaries.
- `codex://inbox`: actionable inbox rows.
- `codex://waiting`: threads waiting on user input or approval.
- `codex://thread/{threadId}`: one thread by id, exposed through a resource template.

These resources still read local Codex state. Treat them with the same trust boundary as tools.

## Prompts

The MCP server exposes small user-controlled prompts:

- `codex_check_inbox`: inspect Codex state before taking action.
- `codex_reply`: prepare a reply action for a specific thread and exact message.
- `codex_approve`: prepare an approval or denial action for a specific thread.

Prompts do not mutate Codex. They give Hermes safer phrasing for when and how to call the tools.

## Suggested Hermes Instruction

If your Hermes setup supports custom profile instructions or skills, add:

```text
When the user asks about Codex work, use the Codex MCP tools first. Check codex_inbox or codex_waiting before replying or approving. Use codex_reply only when the user gives the exact reply text. Use codex_approve only when the user explicitly approves or denies a prompt.
```

## Security Notes

- This is a local trust boundary. Do not expose `codex-telegram-bridge mcp` on a remote transport.
- MCP tools can read thread content and perform local Codex actions.
- Telegram notifications and reply routing are configured with `codex-telegram-bridge setup`, not through Hermes or MCP.
- Keep `sampling.enabled: false` unless you have a specific reason to let this server request model sampling from the client.
- Use `dryRun: true` for reply and approve checks when you want Hermes to show the action shape before mutating Codex.

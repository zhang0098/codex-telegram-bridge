mod api;
pub(crate) mod render;

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::thread;
use std::time::{Duration, Instant};

use crate::codex::{
    normalized_message, set_away_mode, start_thread_in_cwd, sync_state_from_live, text_input_value,
    CodexAppServerClient,
};
use crate::live::EnsureLiveBackendResult;
use crate::projects::{resolve_new_thread_request, resolve_project_query};
use crate::state::{
    delete_setting, get_setting_number, get_telegram_current_project_id,
    insert_telegram_callback_route, insert_telegram_command_route, insert_telegram_message_route,
    list_recent_thread_snapshots_from_db, lookup_telegram_command_route,
    lookup_telegram_message_route, mark_telegram_callback_route_used,
    mark_telegram_command_route_used, observed_workspaces_from_db, record_action,
    record_telegram_inbound_processed, set_setting, set_setting_text,
    set_telegram_current_project_id, telegram_inbound_processed,
    update_telegram_callback_message_id, BridgeThreadSnapshot, TelegramCallbackAction,
    TelegramCommandRouteKind, TelegramInboundLogContext,
};
use crate::ws::validate_shared_websocket_url;
use crate::{
    daemon_config_path, ensure_live_backend, load_daemon_config, merged_daemon_config,
    read_daemon_config_raw, redacted_daemon_config, reset_live_backend, resolve_telegram_bot_token,
    write_daemon_config, CodexConfig, CodexLiveMode, DaemonConfig, RegisteredProject,
    TelegramConfig, TelegramSetupOptions,
};

use self::api::{
    telegram_answer_callback_query, telegram_bot_commands, telegram_chat_id,
    telegram_delete_webhook, telegram_from_user_id, telegram_get_updates, telegram_message_id,
    telegram_send_chat_action, telegram_send_message, telegram_send_text,
    telegram_send_text_message_id, telegram_updates_array,
};
use self::render::{
    prepare_telegram_delivery, prepare_telegram_thread_snapshot_delivery, telegram_help_text,
    telegram_new_thread_confirmation_text, telegram_project_text, telegram_projects_text,
    telegram_status_text, PreparedTelegramDelivery,
};

pub(crate) use self::api::{telegram_bot_id, telegram_set_my_commands};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TelegramInboundCommand {
    Start,
    Help,
    Away,
    Back,
    Repair,
    Status,
    TelegramOn,
    TelegramOff,
    DiscordOn,
    DiscordOff,
    Threads(Option<String>),
    NewThread(Option<String>),
    Project(Option<String>),
    Unknown(String),
}

const DEFAULT_TELEGRAM_THREADS_LIMIT: u64 = 5;
const MAX_TELEGRAM_THREADS_LIMIT: u64 = 25;
const TELEGRAM_TYPING_TTL_MS: u64 = 120_000;
const TELEGRAM_TYPING_REFRESH_MS: u64 = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramCommandPromptReply {
    pub(crate) kind: TelegramCommandRouteKind,
    pub(crate) message: String,
    pub(crate) project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramReply {
    pub(crate) thread_id: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramCallback {
    pub(crate) callback_query_id: String,
    pub(crate) callback_id: String,
    pub(crate) thread_id: String,
    pub(crate) action: TelegramCallbackAction,
}

pub(crate) fn telegram_setup_result(options: TelegramSetupOptions<'_>) -> Result<Value> {
    let bot_token = resolve_telegram_bot_token(options.bot_token)?;
    let events = options.events.trim();
    let bridge_command = options.bridge_command.trim();
    let websocket_url = options.websocket_url.trim();
    if events.is_empty() {
        bail!("telegram setup events cannot be empty");
    }
    if bridge_command.is_empty() {
        bail!("telegram setup bridge command cannot be empty");
    }
    if websocket_url.is_empty() {
        bail!("telegram setup websocket url cannot be empty");
    }
    validate_shared_websocket_url(websocket_url)
        .context("telegram setup websocket url is invalid")?;
    if !options.dry_run {
        telegram_delete_webhook(&bot_token, Duration::from_secs(10))
            .context("failed to clear existing Telegram webhook before enabling long polling")?;
    }

    let pair_hint = json!({
        "message": "Send /start to the Telegram bot to pair this chat automatically.",
        "timeoutMs": options.pair_timeout_ms
    });
    let paired = if let Some(chat_id) = options.chat_id.map(str::trim).filter(|v| !v.is_empty()) {
        TelegramConfig {
            bot_token: bot_token.clone(),
            chat_id: chat_id.to_string(),
            allowed_user_id: options
                .allowed_user_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            enabled: true,
        }
    } else if options.dry_run {
        TelegramConfig {
            bot_token: bot_token.clone(),
            chat_id: "<paired by /start>".to_string(),
            allowed_user_id: options
                .allowed_user_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            enabled: true,
        }
    } else {
        discover_telegram_pairing(&bot_token, options.pair_timeout_ms)?
    };

    let existing = read_daemon_config_raw()?;
    let config = merged_daemon_config(
        existing.as_ref(),
        bridge_command,
        events,
        paired.clone(),
        CodexConfig {
            live_mode: CodexLiveMode::Shared,
            websocket_url: websocket_url.to_string(),
        },
    );
    let commands = telegram_bot_commands();
    let commands_registration = if options.dry_run {
        json!({ "registered": false, "dryRun": true, "commands": commands })
    } else {
        json!({
            "registered": true,
            "commands": commands,
            "response": telegram_set_my_commands(&paired, Duration::from_secs(10))
                .context("failed to register Telegram slash commands")?
        })
    };

    let config_path = if options.dry_run {
        daemon_config_path()?
    } else {
        write_daemon_config(&config)?
    };

    Ok(json!({
        "ok": true,
        "action": "telegram_setup",
        "dryRun": options.dry_run,
        "configPath": config_path.display().to_string(),
        "telegram": {
            "configured": true,
            "botToken": "<redacted>",
            "chatId": paired.chat_id,
            "allowedUserId": paired.allowed_user_id,
            "pairing": if options.chat_id.is_some() { Value::Null } else { pair_hint },
            "commands": commands_registration
        },
        "config": redacted_daemon_config(&config),
        "daemonCommand": crate::daemon_run_command(bridge_command),
        "daemonInstallCommand": format!(
            "{} daemon install --bridge-command {}",
            crate::shell_quote(bridge_command),
            crate::shell_quote(bridge_command)
        ),
        "nextStep": "Install and start the daemon. Send /away to the Telegram bot before leaving; replies, approvals, and new threads will use the shared Codex live backend."
    }))
}

pub(crate) fn telegram_status_result() -> Result<Value> {
    let config = read_daemon_config_raw()?;
    let telegram = config.as_ref().and_then(|config| config.telegram.as_ref());
    Ok(json!({
        "ok": true,
        "action": "telegram_status",
        "configPath": daemon_config_path()?.display().to_string(),
        "configured": telegram.is_some(),
        "enabled": telegram.map(|telegram| telegram.enabled).unwrap_or(false),
        "config": config.as_ref().map(redacted_daemon_config)
    }))
}

#[cfg(not(test))]
fn send_telegram_command_text(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_send_text(telegram, text, timeout)
}

#[cfg(test)]
fn send_telegram_command_text(
    _telegram: &TelegramConfig,
    text: &str,
    _timeout: Duration,
) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "result": {
            "message_id": 1,
            "text": text
        }
    }))
}

pub(crate) fn telegram_test_result(
    message: &str,
    timeout: Duration,
    dry_run: bool,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let telegram = config
        .telegram
        .as_ref()
        .context("Telegram is not configured. Run telegram setup first.")?;
    let text = normalized_message(Some(message))
        .unwrap_or_else(|| "Codex Telegram bridge test".to_string());
    let payload = json!({
        "chat_id": telegram.chat_id,
        "text": text,
        "disable_web_page_preview": true
    });
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "telegram_test",
            "dryRun": true,
            "payload": payload
        }));
    }
    let sent = telegram_send_message(telegram, &payload, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_test",
        "dryRun": false,
        "messageId": sent.pointer("/result/message_id").cloned().unwrap_or(Value::Null)
    }))
}

pub(crate) fn telegram_set_enabled_result(enabled: bool, dry_run: bool) -> Result<Value> {
    let path = daemon_config_path()?;
    let mut config =
        read_daemon_config_raw()?.context("daemon config is missing. Run telegram setup first.")?;
    let telegram = config
        .telegram
        .as_mut()
        .context("Telegram is not configured. Run telegram setup first.")?;
    let previous_enabled = telegram.enabled;
    telegram.enabled = enabled;
    if !dry_run {
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": if enabled { "telegram_enable" } else { "telegram_disable" },
        "dryRun": dry_run,
        "configured": true,
        "previousEnabled": previous_enabled,
        "enabled": enabled,
        "configPath": path.display().to_string(),
        "config": redacted_daemon_config(&config)
    }))
}

#[cfg(test)]
#[cfg(test)]
pub(crate) fn telegram_disable_result(dry_run: bool) -> Result<Value> {
    telegram_set_enabled_result(false, dry_run)
}

fn discover_telegram_pairing(bot_token: &str, timeout_ms: u64) -> Result<TelegramConfig> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
    let mut offset = None;
    while std::time::Instant::now() < deadline {
        let updates = telegram_get_updates(bot_token, offset, 10, Duration::from_secs(15))?;
        for update in telegram_updates_array(&updates)? {
            if let Some(update_id) = update.get("update_id").and_then(Value::as_i64) {
                offset = Some(update_id.saturating_add(1));
            }
            if let Some(message) = update.get("message") {
                let text = message.get("text").and_then(Value::as_str).unwrap_or("");
                if text.trim() != "/start" {
                    continue;
                }
                let chat_id = telegram_chat_id(message)
                    .context("Telegram /start update did not include chat.id")?;
                let allowed_user_id = telegram_from_user_id(message);
                return Ok(TelegramConfig {
                    bot_token: bot_token.to_string(),
                    chat_id,
                    allowed_user_id,
                    enabled: true,
                });
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
    bail!("Timed out waiting for Telegram /start. Send /start to the bot and rerun telegram setup.")
}

fn telegram_authorized(
    telegram: &TelegramConfig,
    chat_id: Option<&str>,
    user_id: Option<&str>,
) -> bool {
    if chat_id != Some(telegram.chat_id.as_str()) {
        return false;
    }
    match telegram.allowed_user_id.as_deref() {
        Some(allowed) => user_id == Some(allowed),
        None => true,
    }
}

fn parse_telegram_command_text(text: &str) -> Option<TelegramInboundCommand> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let raw_command = parts.next().unwrap_or_default();
    let rest = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let command = raw_command
        .split_once('@')
        .map(|(name, _)| name)
        .unwrap_or(raw_command)
        .to_ascii_lowercase();
    match command.as_str() {
        "/start" => Some(TelegramInboundCommand::Start),
        "/help" => Some(TelegramInboundCommand::Help),
        "/away" => Some(TelegramInboundCommand::Away),
        "/back" => Some(TelegramInboundCommand::Back),
        "/repair" => Some(TelegramInboundCommand::Repair),
        "/status" => Some(TelegramInboundCommand::Status),
        "/telegram_on" => Some(TelegramInboundCommand::TelegramOn),
        "/telegram_off" => Some(TelegramInboundCommand::TelegramOff),
        "/discord_on" => Some(TelegramInboundCommand::DiscordOn),
        "/discord_off" => Some(TelegramInboundCommand::DiscordOff),
        "/threads" => Some(TelegramInboundCommand::Threads(rest.map(str::to_string))),
        "/new" => Some(TelegramInboundCommand::NewThread(rest.map(str::to_string))),
        "/project" => Some(TelegramInboundCommand::Project(rest.map(str::to_string))),
        _ => Some(TelegramInboundCommand::Unknown(raw_command.to_string())),
    }
}

fn parse_telegram_threads_limit(raw: Option<&str>) -> Result<u64> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_TELEGRAM_THREADS_LIMIT);
    };
    let limit = raw.parse::<u64>().with_context(|| {
        format!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_TELEGRAM_THREADS_LIMIT}"
        )
    })?;
    if !(1..=MAX_TELEGRAM_THREADS_LIMIT).contains(&limit) {
        bail!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_TELEGRAM_THREADS_LIMIT}"
        );
    }
    Ok(limit)
}

fn extract_telegram_command(
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<TelegramInboundCommand>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    if message.get("reply_to_message").is_some() {
        return Ok(None);
    }
    Ok(message
        .get("text")
        .and_then(Value::as_str)
        .and_then(parse_telegram_command_text))
}

pub(crate) fn extract_telegram_reply_route(
    conn: &Connection,
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramReply>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let reply_message_id = message
        .get("reply_to_message")
        .and_then(telegram_message_id);
    let Some(reply_message_id) = reply_message_id else {
        return Ok(None);
    };
    let text = message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(text) = text else {
        return Ok(None);
    };
    let Some(chat_id) = chat_id else {
        return Ok(None);
    };
    let thread_id = lookup_telegram_message_route(conn, &chat_id, reply_message_id)?;
    Ok(thread_id.map(|thread_id| RoutedTelegramReply {
        thread_id,
        message: text.to_string(),
    }))
}

pub(crate) fn extract_telegram_command_prompt_reply(
    conn: &Connection,
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramCommandPromptReply>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let Some(chat_id) = chat_id else {
        return Ok(None);
    };
    let Some(reply_message_id) = message
        .get("reply_to_message")
        .and_then(telegram_message_id)
    else {
        return Ok(None);
    };
    let Some(message_text) = message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some((kind, payload)) = lookup_telegram_command_route(conn, &chat_id, reply_message_id)?
    else {
        return Ok(None);
    };
    Ok(Some(RoutedTelegramCommandPromptReply {
        kind,
        message: message_text.to_string(),
        project_id: payload
            .as_ref()
            .and_then(|value| value.get("projectId"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

pub(crate) fn extract_telegram_callback_route(
    conn: &Connection,
    callback_query: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramCallback>> {
    let message = callback_query.get("message");
    let chat_id = message.and_then(telegram_chat_id);
    let user_id = callback_query
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        });
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let callback_query_id = callback_query
        .get("id")
        .and_then(Value::as_str)
        .context("callback query missing id")?;
    let Some(callback_id) = callback_query
        .get("data")
        .and_then(Value::as_str)
        .and_then(|data| data.strip_prefix("codex:"))
    else {
        return Ok(None);
    };
    let route = conn
        .query_row(
            "SELECT thread_id, action FROM telegram_callback_routes
             WHERE callback_id = ?1 AND used_at IS NULL",
            params![callback_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    Ok(route.and_then(|(thread_id, action)| {
        TelegramCallbackAction::from_str(&action).map(|action| RoutedTelegramCallback {
            callback_query_id: callback_query_id.to_string(),
            callback_id: callback_id.to_string(),
            thread_id,
            action,
        })
    }))
}

pub(crate) fn deliver_telegram_event(
    conn: &Connection,
    telegram: &TelegramConfig,
    event: &Value,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut prepared = prepare_telegram_delivery(&telegram.chat_id, event)?;
    deliver_prepared_telegram_delivery(conn, telegram, &mut prepared, now, timeout)
}

fn deliver_prepared_telegram_delivery(
    conn: &Connection,
    telegram: &TelegramConfig,
    prepared: &mut PreparedTelegramDelivery,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    for route in &prepared.callback_routes {
        insert_telegram_callback_route(conn, route, now)?;
    }
    let mut message_ids = Vec::with_capacity(prepared.payloads.len());
    for payload in &prepared.payloads {
        let response = telegram_send_message(telegram, payload, timeout)?;
        let message_id = response
            .pointer("/result/message_id")
            .and_then(Value::as_i64)
            .context("Telegram sendMessage response missing result.message_id")?;
        if let Some(thread_id) = prepared.thread_id.as_deref() {
            insert_telegram_message_route(
                conn,
                &telegram.chat_id,
                message_id,
                thread_id,
                &prepared.event_id,
                now,
            )?;
        }
        message_ids.push(message_id);
    }
    let first_message_id = *message_ids
        .first()
        .context("Telegram delivery did not send any messages")?;
    for route in &mut prepared.callback_routes {
        route.message_id = Some(first_message_id);
        update_telegram_callback_message_id(conn, &route.callback_id, first_message_id)?;
    }
    if let Some(thread_id) = prepared.thread_id.as_deref() {
        clear_telegram_typing_indicator(conn, telegram, thread_id)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "telegram",
        "messageId": first_message_id,
        "messageIds": message_ids,
        "chunks": prepared.payloads.len(),
        "threadId": prepared.thread_id,
        "callbacks": prepared.callback_routes.len()
    }))
}

fn send_recent_thread_snapshot(
    conn: &Connection,
    telegram: &TelegramConfig,
    snapshot: &BridgeThreadSnapshot,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut prepared = prepare_telegram_thread_snapshot_delivery(&telegram.chat_id, snapshot)?;
    deliver_prepared_telegram_delivery(conn, telegram, &mut prepared, now, timeout)
}

fn telegram_typing_key(chat_id: &str, thread_id: &str) -> String {
    format!("telegram_typing:{chat_id}:{thread_id}")
}

fn register_telegram_typing_indicator(
    conn: &Connection,
    telegram: &TelegramConfig,
    thread_id: &str,
    now: u64,
) -> Result<()> {
    set_setting_text(
        conn,
        &telegram_typing_key(&telegram.chat_id, thread_id),
        &json!({
            "chatId": telegram.chat_id,
            "threadId": thread_id,
            "until": now + TELEGRAM_TYPING_TTL_MS,
            "nextAt": now
        })
        .to_string(),
    )
}

pub(crate) fn refresh_telegram_typing_indicators(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let rows = crate::state::list_settings_with_prefix(conn, "telegram_typing:")?;
    let mut active = 0usize;
    let mut sent = 0usize;
    let mut expired = 0usize;
    let mut failed = 0usize;
    for (key, raw) in rows {
        let value: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => {
                delete_setting(conn, &key)?;
                expired += 1;
                continue;
            }
        };
        let chat_id = value.get("chatId").and_then(Value::as_str);
        if chat_id != Some(telegram.chat_id.as_str()) {
            continue;
        }
        let until = value.get("until").and_then(Value::as_u64).unwrap_or(0);
        if until <= now {
            delete_setting(conn, &key)?;
            expired += 1;
            continue;
        }
        active += 1;
        let next_at = value.get("nextAt").and_then(Value::as_u64).unwrap_or(0);
        if next_at > now {
            continue;
        }
        match telegram_send_chat_action(telegram, "typing", timeout) {
            Ok(_) => {
                sent += 1;
                set_setting_text(
                    conn,
                    &key,
                    &json!({
                        "chatId": telegram.chat_id,
                        "threadId": value.get("threadId").cloned().unwrap_or(Value::Null),
                        "until": until,
                        "nextAt": now + TELEGRAM_TYPING_REFRESH_MS
                    })
                    .to_string(),
                )?;
            }
            Err(_) => failed += 1,
        }
    }
    Ok(json!({
        "ok": failed == 0,
        "transport": "telegram",
        "active": active,
        "sent": sent,
        "expired": expired,
        "failed": failed
    }))
}

fn clear_telegram_typing_indicator(
    conn: &Connection,
    telegram: &TelegramConfig,
    thread_id: &str,
) -> Result<()> {
    delete_setting(conn, &telegram_typing_key(&telegram.chat_id, thread_id))
}

fn codex_log_context_from_result<'a>(
    result: &'a Value,
    thread_id: Option<&'a str>,
    route_message_id: Option<i64>,
) -> TelegramInboundLogContext<'a> {
    TelegramInboundLogContext {
        thread_id,
        route_message_id,
        result_action: result.get("action").and_then(Value::as_str),
        codex_transport: result.pointer("/codex/transport").and_then(Value::as_str),
        codex_app_server_pid: result
            .pointer("/codex/appServerPid")
            .and_then(Value::as_u64)
            .and_then(|value| {
                if value <= u32::MAX as u64 {
                    Some(value as u32)
                } else {
                    None
                }
            }),
    }
}

fn send_codex_reply_to_thread(
    conn: &Connection,
    config: &DaemonConfig,
    thread_id: &str,
    message: &str,
    now: u64,
    deadline: Option<Instant>,
) -> Result<Value> {
    let mut client = CodexAppServerClient::connect_configured(config)?;
    if let Some(deadline) = deadline {
        client.set_deadline(deadline);
    }
    let transport = client.transport_info();
    let resumed = client.request("thread/resume", json!({ "threadId": thread_id }))?;
    let started = client.request(
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [text_input_value(message)]
        }),
    )?;
    let started_turn_id = started
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let delivery = json!({
        "mode": "daemon_sync",
        "status": "turn_started",
        "startedTurnId": started_turn_id.as_deref()
    });
    record_action(
        conn,
        thread_id,
        "telegram_reply",
        json!({
            "message": message,
            "resumed": resumed,
            "started": started,
            "delivery": delivery.clone(),
            "sentAt": now
        }),
        now,
    )?;
    if let Some(telegram) = config.telegram.as_ref() {
        register_telegram_typing_indicator(conn, telegram, thread_id, now)?;
        let _ = refresh_telegram_typing_indicators(conn, telegram, now, Duration::from_secs(5));
    }
    Ok(json!({
        "ok": true,
        "action": "telegram_reply",
        "threadId": thread_id,
        "message": message,
        "codex": {
            "transport": transport.transport,
            "appServerPid": transport.app_server_pid
        },
        "delivery": delivery,
        "sentAt": now
    }))
}

fn send_codex_approval_to_thread(
    conn: &Connection,
    config: &DaemonConfig,
    thread_id: &str,
    action: TelegramCallbackAction,
    now: u64,
    deadline: Option<Instant>,
) -> Result<Value> {
    let sent_text = match action {
        TelegramCallbackAction::Approve => "YES",
        TelegramCallbackAction::Deny => "NO",
    };
    let mut client = CodexAppServerClient::connect_configured(config)?;
    if let Some(deadline) = deadline {
        client.set_deadline(deadline);
    }
    let transport = client.transport_info();
    let resumed = client.request("thread/resume", json!({ "threadId": thread_id }))?;
    let started = client.request(
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [text_input_value(sent_text)]
        }),
    )?;
    let started_turn_id = started
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let delivery = json!({
        "mode": "daemon_sync",
        "status": "turn_started",
        "startedTurnId": started_turn_id.as_deref()
    });
    record_action(
        conn,
        thread_id,
        "telegram_approval",
        json!({
            "decision": action.as_str(),
            "sentText": sent_text,
            "resumed": resumed,
            "started": started,
            "delivery": delivery.clone(),
            "sentAt": now
        }),
        now,
    )?;
    if let Some(telegram) = config.telegram.as_ref() {
        register_telegram_typing_indicator(conn, telegram, thread_id, now)?;
        let _ = refresh_telegram_typing_indicators(conn, telegram, now, Duration::from_secs(5));
    }
    Ok(json!({
        "ok": true,
        "action": "telegram_approval",
        "threadId": thread_id,
        "decision": action.as_str(),
        "sentText": sent_text,
        "codex": {
            "transport": transport.transport,
            "appServerPid": transport.app_server_pid
        },
        "delivery": delivery,
        "sentAt": now
    }))
}

fn current_project_for_identity<'a>(
    config: &'a DaemonConfig,
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
) -> Result<Option<&'a RegisteredProject>> {
    if let Some(project_id) = get_telegram_current_project_id(conn, chat_id, user_id)? {
        if let Some(project) = config
            .projects
            .iter()
            .find(|project| project.id == project_id)
        {
            return Ok(Some(project));
        }
    }
    if config.projects.len() == 1 {
        return Ok(config.projects.first());
    }
    Ok(None)
}

fn start_new_thread_from_telegram(
    conn: &Connection,
    config: &DaemonConfig,
    project: &RegisteredProject,
    message: &str,
    now: u64,
    deadline: Option<Instant>,
) -> Result<Value> {
    let mut client = CodexAppServerClient::connect_configured(config)?;
    if let Some(deadline) = deadline {
        client.set_deadline(deadline);
    }
    let transport = client.transport_info();
    let mut result = start_thread_in_cwd(&mut client, Some(&project.cwd), Some(message))?;
    if let Some(object) = result.as_object_mut() {
        object.insert(
            "codex".to_string(),
            json!({
                "transport": transport.transport,
                "appServerPid": transport.app_server_pid
            }),
        );
    }
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("Codex app-server thread/start response missing thread.id")?;
    record_action(
        conn,
        thread_id,
        "telegram_new_thread",
        json!({
            "projectId": project.id,
            "projectLabel": project.label,
            "cwd": project.cwd,
            "message": message,
            "result": result.clone(),
            "sentAt": now
        }),
        now,
    )?;
    if let Some(telegram) = config.telegram.as_ref() {
        register_telegram_typing_indicator(conn, telegram, thread_id, now)?;
        let _ = refresh_telegram_typing_indicators(conn, telegram, now, Duration::from_secs(5));
    }
    Ok(result)
}

fn send_new_thread_confirmation(
    conn: &Connection,
    telegram: &TelegramConfig,
    project: &RegisteredProject,
    result: &Value,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("new thread result missing threadId")?;
    let text = telegram_new_thread_confirmation_text(project, result)?;
    let message_id = telegram_send_text_message_id(telegram, &text, timeout)?;
    insert_telegram_message_route(
        conn,
        &telegram.chat_id,
        message_id,
        thread_id,
        &format!("telegram_new_thread:{thread_id}"),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "telegram_new_thread_confirmation",
        "threadId": thread_id,
        "messageId": message_id
    }))
}

fn send_new_thread_prompt_for_project(
    conn: &Connection,
    telegram: &TelegramConfig,
    project: &RegisteredProject,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let text = format!(
        "What should Codex work on in {}?\n{}\n\nUse Telegram's Reply action on this message with the prompt for the new thread.",
        project.label, project.cwd
    );
    let message_id = telegram_send_text_message_id(telegram, &text, timeout)?;
    insert_telegram_command_route(
        conn,
        &telegram.chat_id,
        message_id,
        TelegramCommandRouteKind::NewThread,
        Some(&json!({ "projectId": project.id })),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "telegram_new_thread_prompt",
        "projectId": project.id,
        "messageId": message_id
    }))
}

fn telegram_live_backend_text(
    title: &str,
    backend: &EnsureLiveBackendResult,
    away: &Value,
) -> String {
    let pid = backend
        .status
        .pid
        .map(|pid| pid.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let health = if backend.status.healthy {
        "healthy"
    } else {
        "unhealthy"
    };
    let away_state = if away.get("away").and_then(Value::as_bool) == Some(true) {
        "on"
    } else {
        "off"
    };

    format!(
        "{title}\nBackend: {action}, {health}, {url}, pid {pid}.\nAway mode: {away_state}.",
        action = backend.action.as_str(),
        url = backend.status.websocket_url.as_str()
    )
}

fn telegram_live_failure_text(title: &str, error: &anyhow::Error) -> String {
    format!("{title}\nError: {error:#}\nTry /repair. If that keeps failing, check `codex-telegram-bridge doctor` locally.")
}

fn telegram_live_failure_result(
    telegram: &TelegramConfig,
    action: &str,
    title: &str,
    error: anyhow::Error,
    timeout: Duration,
) -> Result<Value> {
    let message = telegram_live_failure_text(title, &error);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": false,
        "action": action,
        "error": format!("{error:#}"),
        "sent": sent
    }))
}

fn execute_away_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let codex = config
        .codex
        .as_ref()
        .context("shared Codex live backend is not configured; run setup first")?;
    let backend = ensure_live_backend(codex)?;
    let away = set_away_mode(conn, true, now)?;
    let message = telegram_live_backend_text("Remote Codex mode is on.", &backend, &away);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_away",
        "backend": backend,
        "away": away,
        "sent": sent
    }))
}

fn execute_repair_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let codex = config
        .codex
        .as_ref()
        .context("shared Codex live backend is not configured; run setup first")?;
    let backend = reset_live_backend(codex)?;
    let away = set_away_mode(conn, true, now)?;
    let message = telegram_live_backend_text("Remote Codex mode was repaired.", &backend, &away);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_repair",
        "backend": backend,
        "away": away,
        "sent": sent
    }))
}

fn telegram_threads_limit_error_text(error: &anyhow::Error) -> String {
    format!(
        "{error:#}\n\nExamples:\n/threads\n/threads 10\n\nThe maximum is {MAX_TELEGRAM_THREADS_LIMIT}."
    )
}

fn telegram_threads_failure_text(error: &anyhow::Error) -> String {
    format!("I couldn't fetch recent Codex threads.\nError: {error:#}\n\nTry /repair if the shared backend is unhealthy.")
}

fn execute_threads_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    raw_limit: Option<String>,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let limit = match parse_telegram_threads_limit(raw_limit.as_deref()) {
        Ok(limit) => limit,
        Err(error) => {
            let text = telegram_threads_limit_error_text(&error);
            let sent = telegram_send_text(telegram, &text, timeout)?;
            return Ok(json!({
                "ok": true,
                "action": "telegram_threads_invalid_limit",
                "error": format!("{error:#}"),
                "sent": sent
            }));
        }
    };

    let config = load_daemon_config()?;
    let mut client = CodexAppServerClient::connect_configured(&config)?;
    if let Some(deadline) = deadline {
        client.set_deadline(deadline);
    }
    sync_state_from_live(&mut client, conn, now, limit, false)?;
    let snapshots = list_recent_thread_snapshots_from_db(conn, limit)?;
    if snapshots.is_empty() {
        let sent = telegram_send_text(
            telegram,
            "No recent Codex threads are cached yet. Open Codex locally or try again after the daemon syncs.",
            timeout,
        )?;
        return Ok(json!({
            "ok": true,
            "action": "telegram_threads_empty",
            "limit": limit,
            "sent": sent
        }));
    }

    let mut sent = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        sent.push(send_recent_thread_snapshot(
            conn, telegram, snapshot, now, timeout,
        )?);
    }

    Ok(json!({
        "ok": true,
        "action": "telegram_threads",
        "limit": limit,
        "count": snapshots.len(),
        "sent": sent
    }))
}

fn execute_telegram_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    message: &Value,
    command: TelegramInboundCommand,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let chat_id = telegram_chat_id(message).context("Telegram command missing chat.id")?;
    let user_id = telegram_from_user_id(message);
    match command {
        TelegramInboundCommand::Start | TelegramInboundCommand::Help => {
            let sent = telegram_send_text(telegram, &telegram_help_text(), timeout)?;
            Ok(json!({ "ok": true, "action": "telegram_help", "sent": sent }))
        }
        TelegramInboundCommand::Back => {
            let state = set_away_mode(conn, false, now)?;
            let cleared = state
                .get("clearedPendingNotifications")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let sent = send_telegram_command_text(
                telegram,
                &format!("Remote Codex mode is off. Cleared {cleared} pending notification(s)."),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "telegram_back", "state": state, "sent": sent }))
        }
        TelegramInboundCommand::Status => {
            let sent = telegram_send_text(telegram, &telegram_status_text(conn)?, timeout)?;
            Ok(json!({ "ok": true, "action": "telegram_status", "sent": sent }))
        }
        TelegramInboundCommand::TelegramOn => {
            let state = telegram_set_enabled_result(true, false)?;
            let sent = telegram_send_text(telegram, "Telegram channel is enabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "telegram_enable_command", "state": state, "sent": sent }),
            )
        }
        TelegramInboundCommand::TelegramOff => {
            let state = telegram_set_enabled_result(false, false)?;
            let sent = telegram_send_text(telegram, "Telegram channel is disabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "telegram_disable_command", "state": state, "sent": sent }),
            )
        }
        TelegramInboundCommand::DiscordOn => {
            let state = crate::discord::discord_set_enabled_result(true, false)?;
            let sent = telegram_send_text(telegram, "Discord channel is enabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "discord_enable_command", "state": state, "sent": sent }),
            )
        }
        TelegramInboundCommand::DiscordOff => {
            let state = crate::discord::discord_set_enabled_result(false, false)?;
            let sent = telegram_send_text(telegram, "Discord channel is disabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "discord_disable_command", "state": state, "sent": sent }),
            )
        }
        TelegramInboundCommand::Threads(raw_limit) => execute_threads_command(
            conn, telegram, raw_limit, now, timeout, deadline,
        )
        .or_else(|error| {
            let message = telegram_threads_failure_text(&error);
            let sent = telegram_send_text(telegram, &message, timeout)?;
            Ok(json!({
                "ok": false,
                "action": "telegram_threads_failed",
                "error": format!("{error:#}"),
                "sent": sent
            }))
        }),
        TelegramInboundCommand::Away => {
            execute_away_command(conn, telegram, now, timeout).or_else(|error| {
                telegram_live_failure_result(
                    telegram,
                    "telegram_away_failed",
                    "Remote Codex mode could not start.",
                    error,
                    timeout,
                )
            })
        }
        TelegramInboundCommand::Repair => execute_repair_command(conn, telegram, now, timeout)
            .or_else(|error| {
                telegram_live_failure_result(
                    telegram,
                    "telegram_repair_failed",
                    "Remote Codex mode could not repair.",
                    error,
                    timeout,
                )
            }),
        TelegramInboundCommand::NewThread(Some(prompt)) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            match resolve_new_thread_request(&config.projects, current_project, Some(&prompt)) {
                Ok(request) => {
                    if let Some(prompt) = request.prompt.as_deref() {
                        let result = start_new_thread_from_telegram(
                            conn,
                            &config,
                            request.project,
                            prompt,
                            now,
                            deadline,
                        )?;
                        let confirmation = send_new_thread_confirmation(
                            conn,
                            telegram,
                            request.project,
                            &result,
                            timeout,
                            now,
                        )?;
                        Ok(json!({
                            "ok": true,
                            "action": "telegram_new_thread",
                            "projectId": request.project.id,
                            "result": result,
                            "confirmation": confirmation
                        }))
                    } else {
                        send_new_thread_prompt_for_project(
                            conn,
                            telegram,
                            request.project,
                            timeout,
                            now,
                        )
                    }
                }
                Err(error) => {
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_new_thread_needs_project",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::NewThread(None) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            match resolve_new_thread_request(&config.projects, current_project, None) {
                Ok(request) => send_new_thread_prompt_for_project(
                    conn,
                    telegram,
                    request.project,
                    timeout,
                    now,
                ),
                Err(error) => {
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_new_thread_needs_project",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::Project(Some(query)) => {
            let config = load_daemon_config()?;
            match resolve_project_query(&config.projects, &query) {
                Ok(project) => {
                    set_telegram_current_project_id(
                        conn,
                        &chat_id,
                        user_id.as_deref(),
                        &project.id,
                    )?;
                    let sent = telegram_send_text(
                        telegram,
                        &telegram_project_text(Some(project)),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_project_set",
                        "projectId": project.id,
                        "sent": sent
                    }))
                }
                Err(error) => {
                    let current_project =
                        current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_project_not_found",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::Project(None) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
            let sent = telegram_send_text(
                telegram,
                &telegram_projects_text(&config, current_project, &observed),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "telegram_project", "sent": sent }))
        }
        TelegramInboundCommand::Unknown(command) => {
            let sent = telegram_send_text(
                telegram,
                &format!("I don't know {command} yet.\n\n{}", telegram_help_text()),
                timeout,
            )?;
            Ok(json!({
                "ok": true,
                "action": "telegram_unknown_command",
                "command": command,
                "sent": sent
            }))
        }
    }
}

fn execute_telegram_command_prompt_reply(
    conn: &Connection,
    telegram: &TelegramConfig,
    message: &Value,
    route: RoutedTelegramCommandPromptReply,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let chat_id =
        telegram_chat_id(message).context("Telegram command prompt reply missing chat.id")?;
    let user_id = telegram_from_user_id(message);
    let reply_message_id = message
        .get("reply_to_message")
        .and_then(telegram_message_id)
        .context("Telegram command prompt reply missing reply_to_message.message_id")?;
    match route.kind {
        TelegramCommandRouteKind::NewThread => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            let project = match route.project_id.as_deref() {
                Some(project_id) => match config
                    .projects
                    .iter()
                    .find(|project| project.id == project_id)
                {
                    Some(project) => Some(project),
                    None => {
                        let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                        let sent = telegram_send_text(
                            telegram,
                            &format!(
                                "That project is no longer available. Pick a project first, then start the thread again.\n\n{}",
                                telegram_projects_text(&config, current_project, &observed)
                            ),
                            timeout,
                        )?;
                        mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
                        return Ok(json!({
                            "ok": true,
                            "action": "telegram_new_thread_prompt_missing_project",
                            "sent": sent
                        }));
                    }
                },
                None => current_project,
            };
            let Some(project) = project else {
                let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                let sent = telegram_send_text(
                    telegram,
                    &format!(
                        "No project is selected for that prompt. Use /project <id> first, then try /new again.\n\n{}",
                        telegram_projects_text(&config, current_project, &observed)
                    ),
                    timeout,
                )?;
                mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
                return Ok(json!({
                    "ok": true,
                    "action": "telegram_new_thread_prompt_needs_project",
                    "sent": sent
                }));
            };
            let result = start_new_thread_from_telegram(
                conn,
                &config,
                project,
                &route.message,
                now,
                deadline,
            )?;
            mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
            let confirmation =
                send_new_thread_confirmation(conn, telegram, project, &result, timeout, now)?;
            Ok(json!({
                "ok": true,
                "action": "telegram_new_thread_prompt_reply",
                "projectId": project.id,
                "result": result,
                "confirmation": confirmation
            }))
        }
    }
}

pub(crate) fn process_telegram_updates(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Ok(json!({
            "ok": true,
            "transport": "telegram",
            "seen": 0,
            "skipped": "cycle_deadline"
        }));
    }
    let telegram = config
        .telegram
        .as_ref()
        .context("Telegram is not configured. Run setup first.")?;
    let bot_id = telegram_bot_id(&telegram.bot_token);
    let key = format!("telegram_offset:{bot_id}");
    let offset = get_setting_number(conn, &key)?.map(|value| value as i64 + 1);
    let updates = telegram_get_updates(&telegram.bot_token, offset, 0, timeout)?;
    let updates = telegram_updates_array(&updates)?;
    process_telegram_update_batch(
        conn, config, telegram, &bot_id, &key, updates, now, timeout, deadline,
    )
}

fn advance_telegram_ack_offset(max_acked: &mut Option<i64>, update_id: Option<i64>) {
    if let Some(update_id) = update_id {
        *max_acked = Some(max_acked.map_or(update_id, |current: i64| current.max(update_id)));
    }
}

fn process_telegram_update_batch(
    conn: &Connection,
    config: &DaemonConfig,
    telegram: &TelegramConfig,
    bot_id: &str,
    offset_key: &str,
    updates: &[Value],
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let mut seen = 0usize;
    let mut replies = 0usize;
    let mut command_prompt_replies = 0usize;
    let mut commands = 0usize;
    let mut callbacks = 0usize;
    let mut duplicate = 0usize;
    let mut ignored = 0usize;
    let mut failed = 0usize;
    // The Telegram offset only advances past updates that were durably acked in
    // telegram_inbound_log. Unprocessed tail updates are left to the next cycle,
    // so a slow message or an expired batch budget can never skip later messages.
    let mut max_acked_update_id = None;
    for update in updates {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        seen += 1;
        let update_id = update.get("update_id").and_then(Value::as_i64);
        if let Some(update_id) = update_id {
            if telegram_inbound_processed(conn, &bot_id, update_id)? {
                duplicate += 1;
                advance_telegram_ack_offset(&mut max_acked_update_id, Some(update_id));
                continue;
            }
        }
        let outcome: Result<()> = (|| {
            if let Some(message) = update.get("message") {
                let route_message_id = message
                    .get("reply_to_message")
                    .and_then(telegram_message_id);
                if let Some(route) = extract_telegram_reply_route(conn, message, telegram)? {
                    let result = send_codex_reply_to_thread(
                        conn,
                        config,
                        &route.thread_id,
                        &route.message,
                        now,
                        deadline,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            &bot_id,
                            update_id,
                            "telegram_reply",
                            &result,
                            codex_log_context_from_result(
                                &result,
                                Some(&route.thread_id),
                                route_message_id,
                            ),
                            now,
                        )?;
                    }
                    replies += 1;
                } else if let Some(route) =
                    extract_telegram_command_prompt_reply(conn, message, telegram)?
                {
                    let result = execute_telegram_command_prompt_reply(
                        conn, telegram, message, route, now, timeout, deadline,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            &bot_id,
                            update_id,
                            "telegram_command_prompt_reply",
                            &result,
                            TelegramInboundLogContext {
                                thread_id: result
                                    .pointer("/result/threadId")
                                    .and_then(Value::as_str),
                                route_message_id,
                                result_action: result.get("action").and_then(Value::as_str),
                                codex_transport: result
                                    .pointer("/result/codex/transport")
                                    .and_then(Value::as_str),
                                codex_app_server_pid: result
                                    .pointer("/result/codex/appServerPid")
                                    .and_then(Value::as_u64)
                                    .and_then(|value| {
                                        if value <= u32::MAX as u64 {
                                            Some(value as u32)
                                        } else {
                                            None
                                        }
                                    }),
                            },
                            now,
                        )?;
                    }
                    command_prompt_replies += 1;
                } else if let Some(command) = extract_telegram_command(message, telegram)? {
                    let result = execute_telegram_command(
                        conn, telegram, message, command, now, timeout, deadline,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            &bot_id,
                            update_id,
                            "telegram_command",
                            &result,
                            TelegramInboundLogContext {
                                route_message_id,
                                result_action: result.get("action").and_then(Value::as_str),
                                ..TelegramInboundLogContext::default()
                            },
                            now,
                        )?;
                    }
                    commands += 1;
                } else {
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            &bot_id,
                            update_id,
                            "message_ignored",
                            &json!({ "ignored": true }),
                            TelegramInboundLogContext {
                                route_message_id,
                                ..TelegramInboundLogContext::default()
                            },
                            now,
                        )?;
                    }
                    ignored += 1;
                }
            } else if let Some(callback_query) = update.get("callback_query") {
                let route_message_id = callback_query
                    .get("message")
                    .and_then(|message| message.get("message_id"))
                    .and_then(Value::as_i64);
                match extract_telegram_callback_route(conn, callback_query, telegram)? {
                    Some(route) => {
                        let result = send_codex_approval_to_thread(
                            conn,
                            config,
                            &route.thread_id,
                            route.action,
                            now,
                            deadline,
                        )?;
                        if let Some(update_id) = update_id {
                            record_telegram_inbound_processed(
                                conn,
                                &bot_id,
                                update_id,
                                "callback_query",
                                &result,
                                codex_log_context_from_result(
                                    &result,
                                    Some(&route.thread_id),
                                    route_message_id,
                                ),
                                now,
                            )?;
                        }
                        // Consume the callback route once the approval was accepted by
                        // Codex, so retried taps or a failed answerCallbackQuery cannot
                        // send the same YES/NO twice.
                        mark_telegram_callback_route_used(conn, &route.callback_id, now)?;
                        let _ = telegram_answer_callback_query(
                            telegram,
                            &route.callback_query_id,
                            "Sent to Codex",
                            timeout,
                        );
                        callbacks += 1;
                    }
                    None => {
                        if let Some(update_id) = update_id {
                            record_telegram_inbound_processed(
                                conn,
                                &bot_id,
                                update_id,
                                "callback_query_ignored",
                                &json!({ "ignored": true }),
                                TelegramInboundLogContext {
                                    route_message_id,
                                    ..TelegramInboundLogContext::default()
                                },
                                now,
                            )?;
                        }
                        ignored += 1;
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = outcome {
            failed += 1;
            // Acknowledge the failing update so Telegram long polling advances past it.
            // Without this, the daemon re-processes the same update forever and every
            // message behind it is blocked.
            if let Some(update_id) = update_id {
                record_telegram_inbound_processed(
                    conn,
                    &bot_id,
                    update_id,
                    "update_error",
                    &json!({ "error": format!("{error:#}") }),
                    TelegramInboundLogContext::default(),
                    now,
                )?;
                advance_telegram_ack_offset(&mut max_acked_update_id, Some(update_id));
            }
            if update.get("message").is_some() {
                let _ = telegram_send_text(
                    telegram,
                    &format!("Your message could not be processed: {error:#}"),
                    timeout,
                );
            }
        } else {
            advance_telegram_ack_offset(&mut max_acked_update_id, update_id);
        }
    }
    if let Some(update_id) = max_acked_update_id {
        set_setting(conn, offset_key, update_id as u64)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "telegram",
        "seen": seen,
        "replies": replies,
        "commandPromptReplies": command_prompt_replies,
        "commands": commands,
        "callbacks": callbacks,
        "duplicate": duplicate,
        "ignored": ignored,
        "failed": failed
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc::{self, Receiver};
    use std::sync::{Mutex, OnceLock};
    use std::thread::{self, JoinHandle};

    use crate::{daemon_config_path, write_daemon_config, DaemonConfig, TelegramConfig};

    fn config_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct LiveCommandEnv {
        root: PathBuf,
        previous_state_dir: Option<String>,
        previous_fake_spawn: Option<String>,
    }

    impl LiveCommandEnv {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "codex-telegram-live-command-{name}-{}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create live command state dir");
            let previous_state_dir = std::env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR").ok();
            let previous_fake_spawn = std::env::var("CODEX_LIVE_TEST_FAKE_SPAWN").ok();
            std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", &root);
            std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", "1");
            Self {
                root,
                previous_state_dir,
                previous_fake_spawn,
            }
        }
    }

    impl Drop for LiveCommandEnv {
        fn drop(&mut self) {
            crate::live::terminate_all_test_live_backends();
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
            }
            if let Some(previous_fake_spawn) = &self.previous_fake_spawn {
                std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", previous_fake_spawn);
            } else {
                std::env::remove_var("CODEX_LIVE_TEST_FAKE_SPAWN");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn random_websocket_url() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind random port");
        let address = listener.local_addr().expect("random port address");
        drop(listener);
        format!("ws://{address}")
    }

    struct FakeCodexWsServer {
        url: String,
        requests: Receiver<Vec<Value>>,
        join: JoinHandle<()>,
    }

    impl FakeCodexWsServer {
        fn spawn_turn_start_only() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake codex ws");
            let address = listener.local_addr().expect("fake codex ws address");
            let url = format!("ws://{address}");
            let (requests_tx, requests_rx) = mpsc::channel();
            let join = thread::spawn(move || {
                let (stream, _) = listener.accept().expect("accept fake codex ws");
                let mut socket = tungstenite::accept(stream).expect("accept websocket");
                let mut requests = Vec::new();
                loop {
                    let message = socket.read().expect("read websocket request");
                    let text = message.into_text().expect("websocket text");
                    let request: Value = serde_json::from_str(&text).expect("parse request");
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let id = request.get("id").cloned();
                    requests.push(request);
                    let Some(id) = id else {
                        continue;
                    };
                    let mut should_finish = false;
                    let result = match method.as_str() {
                        "initialize" => json!({
                            "protocolVersion": 1,
                            "serverInfo": { "name": "fake-codex", "version": "test" }
                        }),
                        "thread/resume" => json!({ "threadId": "thr_1" }),
                        "turn/start" => {
                            should_finish = true;
                            json!({ "turn": { "id": "turn_1" } })
                        }
                        "thread/read" => {
                            panic!("non-blocking Telegram sends must not call thread/read");
                        }
                        other => panic!("unexpected fake Codex method {other}"),
                    };
                    socket
                        .send(tungstenite::Message::text(
                            serde_json::to_string(&json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": result
                            }))
                            .expect("serialize response"),
                        ))
                        .expect("send response");
                    if should_finish {
                        break;
                    }
                }
                requests_tx.send(requests).expect("send requests");
            });
            Self {
                url,
                requests: requests_rx,
                join,
            }
        }

        fn finish(self) -> Vec<Value> {
            let requests = self.requests.recv().expect("fake codex requests");
            self.join.join().expect("fake codex thread");
            requests
        }
    }

    struct ConfigBackup {
        path: std::path::PathBuf,
        contents: Option<Vec<u8>>,
    }

    impl ConfigBackup {
        fn capture() -> anyhow::Result<Self> {
            let path = daemon_config_path()?;
            let contents = fs::read(&path).ok();
            Ok(Self { path, contents })
        }
    }

    impl Drop for ConfigBackup {
        fn drop(&mut self) {
            match &self.contents {
                Some(contents) => {
                    if let Some(parent) = self.path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let _ = fs::write(&self.path, contents);
                }
                None => {
                    if self.path.exists() {
                        let _ = fs::remove_file(&self.path);
                    }
                }
            }
        }
    }

    #[test]
    fn telegram_setup_dry_run_writes_redacted_daemon_shape() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _backup = ConfigBackup::capture().expect("capture config backup");
        if let Ok(path) = daemon_config_path() {
            let _ = fs::remove_file(path);
        }

        let result = telegram_setup_result(TelegramSetupOptions {
            bot_token: Some("123:secret"),
            chat_id: Some("456"),
            allowed_user_id: Some("789"),
            events: crate::DEFAULT_NOTIFICATION_EVENTS,
            bridge_command: "codex-telegram-bridge",
            websocket_url: "ws://127.0.0.1:4500",
            dry_run: true,
            pair_timeout_ms: 1000,
        })
        .expect("telegram setup dry run");

        assert_eq!(result["action"], "telegram_setup");
        assert_eq!(result["dryRun"], true);
        assert_eq!(result["telegram"]["configured"], true);
        assert_eq!(result["telegram"]["botToken"], "<redacted>");
        assert_eq!(result["config"]["telegram"]["botToken"], "<redacted>");
        assert_eq!(result["config"]["telegram"]["chatId"], "456");
        assert_eq!(result["config"]["telegram"]["allowedUserId"], "789");
        assert_eq!(result["daemonCommand"], "codex-telegram-bridge daemon run");
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("123:secret"),
            "setup output must not leak Telegram bot token"
        );
    }

    #[test]
    fn telegram_disable_dry_run_reports_state_without_deleting_file() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _backup = ConfigBackup::capture().expect("capture config backup");
        let path = write_daemon_config(&DaemonConfig {
            version: 4,
            bridge_command: "codex-telegram-bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
                enabled: true,
            }),
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:4500".to_string(),
            }),
            projects: Vec::new(),
        })
        .expect("write config");

        let result = telegram_disable_result(true).expect("telegram disable dry run");

        assert_eq!(result["action"], "telegram_disable");
        assert_eq!(result["dryRun"], true);
        assert_eq!(result["configured"], true);
        assert_eq!(result["previousEnabled"], true);
        assert_eq!(result["enabled"], false);
        assert_eq!(result["configPath"], path.display().to_string());
        assert_eq!(result["config"]["telegram"]["enabled"], false);
        assert!(path.exists(), "dry run should leave config in place");
    }

    #[test]
    fn telegram_command_parser_supports_core_commands() {
        assert_eq!(
            parse_telegram_command_text("/away"),
            Some(TelegramInboundCommand::Away)
        );
        assert_eq!(
            parse_telegram_command_text("/back@codex_bridge_bot"),
            Some(TelegramInboundCommand::Back)
        );
        assert_eq!(
            parse_telegram_command_text("/repair"),
            Some(TelegramInboundCommand::Repair)
        );
        assert_eq!(
            parse_telegram_command_text("/new Fix the formatter"),
            Some(TelegramInboundCommand::NewThread(Some(
                "Fix the formatter".to_string()
            )))
        );
        assert_eq!(
            parse_telegram_command_text("/new"),
            Some(TelegramInboundCommand::NewThread(None))
        );
        assert_eq!(
            parse_telegram_command_text("/project bridge"),
            Some(TelegramInboundCommand::Project(Some("bridge".to_string())))
        );
        assert_eq!(
            parse_telegram_command_text("/project"),
            Some(TelegramInboundCommand::Project(None))
        );
        for removed in [
            "/away_on",
            "/away_off",
            "/live_on",
            "/live_reset",
            "/new_thread",
            "/projects",
            "/inbox",
            "/waiting",
            "/recent",
            "/settings",
        ] {
            assert_eq!(
                parse_telegram_command_text(removed),
                Some(TelegramInboundCommand::Unknown(removed.to_string())),
                "removed command should not parse as a supported command: {removed}"
            );
        }
        assert_eq!(
            parse_telegram_command_text("/unknown"),
            Some(TelegramInboundCommand::Unknown("/unknown".to_string()))
        );
    }

    #[test]
    fn telegram_command_extraction_requires_standalone_authorized_message() {
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };

        let command = extract_telegram_command(
            &json!({
                "chat": { "id": "456" },
                "from": { "id": "789" },
                "text": "/status"
            }),
            &telegram,
        )
        .expect("extract command");
        assert_eq!(command, Some(TelegramInboundCommand::Status));

        let reply_command = extract_telegram_command(
            &json!({
                "chat": { "id": "456" },
                "from": { "id": "789" },
                "text": "/status",
                "reply_to_message": { "message_id": 1 }
            }),
            &telegram,
        )
        .expect("extract reply command");
        assert_eq!(reply_command, None);

        let unauthorized = extract_telegram_command(
            &json!({
                "chat": { "id": "999" },
                "from": { "id": "789" },
                "text": "/status"
            }),
            &telegram,
        )
        .expect("extract unauthorized command");
        assert_eq!(unauthorized, None);
    }

    #[test]
    fn telegram_reply_starts_turn_without_waiting_for_completion() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let server = FakeCodexWsServer::spawn_turn_start_only();
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: server.url.clone(),
            }),
            projects: Vec::new(),
        };

        let result = send_codex_reply_to_thread(&conn, &config, "thr_1", "continue", 1000, None)
            .expect("reply");

        assert_eq!(
            result.pointer("/codex/transport").and_then(Value::as_str),
            Some("shared_websocket")
        );
        assert_eq!(result.pointer("/codex/appServerPid"), Some(&Value::Null));
        assert_eq!(
            result.pointer("/delivery/mode").and_then(Value::as_str),
            Some("daemon_sync")
        );
        assert_eq!(
            result.pointer("/delivery/status").and_then(Value::as_str),
            Some("turn_started")
        );
        assert!(result.get("follow").is_none());
        let methods = server
            .finish()
            .into_iter()
            .filter_map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec!["initialize", "initialized", "thread/resume", "turn/start"]
        );
    }

    #[test]
    fn telegram_approval_starts_turn_without_waiting_for_completion() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let server = FakeCodexWsServer::spawn_turn_start_only();
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: server.url.clone(),
            }),
            projects: Vec::new(),
        };

        let result = send_codex_approval_to_thread(
            &conn,
            &config,
            "thr_1",
            TelegramCallbackAction::Approve,
            1000,
            None,
        )
        .expect("approval");

        assert_eq!(result["action"], "telegram_approval");
        assert_eq!(result["sentText"], "YES");
        assert_eq!(
            result.pointer("/delivery/mode").and_then(Value::as_str),
            Some("daemon_sync")
        );
        assert_eq!(
            result.pointer("/delivery/status").and_then(Value::as_str),
            Some("turn_started")
        );
        assert!(result.get("follow").is_none());
        let methods = server
            .finish()
            .into_iter()
            .filter_map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec!["initialize", "initialized", "thread/resume", "turn/start"]
        );
    }

    #[test]
    fn remote_commands_start_stop_and_repair_shared_backend() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = LiveCommandEnv::new("start-reset");
        let websocket_url = random_websocket_url();
        write_daemon_config(&DaemonConfig {
            version: 4,
            bridge_command: "codex-telegram-bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
                enabled: true,
            }),
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: websocket_url.clone(),
            }),
            projects: Vec::new(),
        })
        .expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };
        let message = json!({
            "chat": { "id": "456" },
            "from": { "id": "789" }
        });

        let away = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Away,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("away result");
        assert_eq!(away["ok"], true);
        assert_eq!(away["action"], "telegram_away");
        assert_eq!(away["backend"]["action"], "started");
        assert_eq!(away["backend"]["status"]["websocketUrl"], websocket_url);
        assert_eq!(away["away"]["away"], true);
        assert!(away["sent"]["result"]["text"]
            .as_str()
            .expect("away text")
            .contains("Remote Codex mode is on."));

        let repair = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Repair,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("repair result");
        assert_eq!(repair["ok"], true);
        assert_eq!(repair["action"], "telegram_repair");
        assert_eq!(repair["backend"]["action"], "restarted");
        assert_eq!(repair["away"]["away"], true);
        assert!(repair["sent"]["result"]["text"]
            .as_str()
            .expect("repair text")
            .contains("Remote Codex mode was repaired."));

        let back = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Back,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("back result");
        assert_eq!(back["ok"], true);
        assert_eq!(back["action"], "telegram_back");
        assert_eq!(back["state"]["away"], false);
        assert!(back["sent"]["result"]["text"]
            .as_str()
            .expect("back text")
            .contains("Remote Codex mode is off."));
    }

    #[test]
    fn live_mode_command_failures_are_reported_to_telegram() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = LiveCommandEnv::new("failure-response");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };
        let message = json!({
            "chat": { "id": "456" },
            "from": { "id": "789" }
        });

        let away = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Away,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("away failure response");

        assert_eq!(away["ok"], false);
        assert_eq!(away["action"], "telegram_away_failed");
        assert!(away["sent"]["result"]["text"]
            .as_str()
            .expect("failure text")
            .contains("Try /repair"));
    }

    #[test]
    fn failing_telegram_update_is_acked_and_does_not_block_later_updates() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _env = LiveCommandEnv::new("failing-update-batch");
        let config = DaemonConfig {
            version: 4,
            bridge_command: "codex-telegram-bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
                enabled: true,
            }),
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:9".to_string(),
            }),
            projects: Vec::new(),
        };
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram config");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let key = format!("telegram_offset:{bot_id}");
        let updates = vec![
            json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "/discord_on"
                }
            }),
            json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
        ];

        let first = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("first batch");

        // /discord_on fails (Discord is not configured) but is acked as an error,
        // and the later message is still processed: the channel does not wedge.
        assert_eq!(first["failed"], 1);
        assert_eq!(first["commands"], 0);
        assert_eq!(first["ignored"], 1);
        assert_eq!(
            get_setting_number(&conn, &key)
                .expect("offset lookup")
                .expect("offset set"),
            2
        );

        // Re-polling the same updates reports duplicates instead of re-failing forever.
        let second = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("second batch");
        assert_eq!(second["duplicate"], 2);
        assert_eq!(second["failed"], 0);
    }

    #[test]
    fn parses_threads_command_with_optional_limit() {
        assert_eq!(
            parse_telegram_command_text("/threads"),
            Some(TelegramInboundCommand::Threads(None))
        );
        assert_eq!(
            parse_telegram_command_text("/threads 12"),
            Some(TelegramInboundCommand::Threads(Some("12".to_string())))
        );
        assert_eq!(
            parse_telegram_command_text("/threads@codex_remote_bot 3"),
            Some(TelegramInboundCommand::Threads(Some("3".to_string())))
        );
    }

    #[test]
    fn parses_threads_limit_with_default_and_bounds() {
        assert_eq!(parse_telegram_threads_limit(None).expect("default"), 5);
        assert_eq!(
            parse_telegram_threads_limit(Some("10")).expect("explicit"),
            10
        );
        assert!(parse_telegram_threads_limit(Some("0")).is_err());
        assert!(parse_telegram_threads_limit(Some("26")).is_err());
        assert!(parse_telegram_threads_limit(Some("two")).is_err());
    }

    #[test]
    fn telegram_batch_deadline_does_not_advance_offset_past_unacked_updates() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(telegram.clone()),
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:9".to_string(),
            }),
            projects: Vec::new(),
        };
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let key = format!("telegram_offset:{bot_id}");
        let updates = vec![
            json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
            json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
        ];

        let expired = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            Some(Instant::now() - Duration::from_secs(1)),
        )
        .expect("expired batch");
        assert_eq!(expired["seen"], 0);
        assert_eq!(
            get_setting_number(&conn, &key).expect("offset lookup"),
            None
        );

        let full = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("full batch");
        assert_eq!(full["ignored"], 2);
        assert_eq!(
            get_setting_number(&conn, &key)
                .expect("offset lookup")
                .expect("offset set"),
            2
        );
    }

    #[test]
    fn telegram_callback_route_is_consumed_after_use() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };
        insert_telegram_callback_route(
            &conn,
            &crate::state::TelegramCallbackRoute {
                callback_id: "cb_1".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_1".to_string(),
                action: TelegramCallbackAction::Approve,
            },
            1000,
        )
        .expect("insert route");
        let callback_query = json!({
            "id": "cq_1",
            "from": { "id": 789 },
            "message": { "message_id": 10, "chat": { "id": "456" } },
            "data": "codex:cb_1"
        });

        let route =
            extract_telegram_callback_route(&conn, &callback_query, &telegram).expect("route");
        assert!(route.is_some());

        mark_telegram_callback_route_used(&conn, "cb_1", 2000).expect("mark used");
        let after =
            extract_telegram_callback_route(&conn, &callback_query, &telegram).expect("after");
        assert!(after.is_none());
    }
}

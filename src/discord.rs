use anyhow::{anyhow, bail, Context, Result};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::time::Duration;

use crate::codex::{
    normalized_message, set_away_mode, start_thread_in_cwd, sync_state_from_live, text_input_value,
    CodexAppServerClient,
};
use crate::projects::{resolve_new_thread_request, resolve_project_query};
use crate::state::{
    delete_setting, get_setting_number, get_telegram_current_project_id,
    insert_telegram_command_route, insert_telegram_message_route,
    list_recent_thread_snapshots_from_db, lookup_telegram_command_route,
    lookup_telegram_message_route, mark_telegram_command_route_used, observed_workspaces_from_db,
    record_action, set_setting, set_setting_text, set_telegram_current_project_id,
    BridgeThreadSnapshot, TelegramCommandRouteKind,
};
use crate::telegram::render::{
    prepare_telegram_delivery, prepare_telegram_thread_snapshot_delivery, telegram_help_text,
    telegram_new_thread_confirmation_text, telegram_project_text, telegram_projects_text,
    telegram_status_text, PreparedTelegramDelivery,
};
use crate::ws::validate_shared_websocket_url;
use crate::{
    daemon_config_path, discord_config_channel_ids, load_daemon_config, read_daemon_config_raw,
    redacted_daemon_config, resolve_discord_bot_token, write_daemon_config, CodexConfig,
    CodexLiveMode, DaemonConfig, DiscordConfig, DiscordSetupOptions, RegisteredProject,
};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DEFAULT_DISCORD_THREADS_LIMIT: u64 = 5;
const MAX_DISCORD_THREADS_LIMIT: u64 = 25;
const DISCORD_TYPING_TTL_MS: u64 = 120_000;
const DISCORD_TYPING_REFRESH_MS: u64 = 8_000;

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiscordInboundCommand {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutedDiscordCommandPromptReply {
    kind: TelegramCommandRouteKind,
    message: String,
    project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutedDiscordReply {
    thread_id: String,
    message: String,
}

fn discord_api_request(
    discord: &DiscordConfig,
    method: &str,
    path: &str,
    payload: Option<&Value>,
    timeout: Duration,
) -> Result<Value> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .build()
        .new_agent();
    let url = format!("{DISCORD_API_BASE}{path}");
    let response = match payload {
        Some(payload) => match method {
            "POST" => agent
                .post(&url)
                .header("Authorization", &format!("Bot {}", discord.bot_token.trim()))
                .header(
                    "User-Agent",
                    "codex-telegram-bridge (https://github.com/hanifcarroll/codex-telegram-bridge, 0.1)",
                )
                .send_json(payload.clone()),
            other => bail!("unsupported Discord API method {other} with payload"),
        },
        None => match method {
            "GET" => agent
                .get(&url)
                .header("Authorization", &format!("Bot {}", discord.bot_token.trim()))
                .header(
                    "User-Agent",
                    "codex-telegram-bridge (https://github.com/hanifcarroll/codex-telegram-bridge, 0.1)",
                )
                .call(),
            other => bail!("unsupported Discord API method {other} without payload"),
        },
    }
    .map_err(|error| {
        anyhow!(
            "Discord API {method} {path} request failed: {}",
            crate::redact_secret_text(&error.to_string(), &discord.bot_token)
        )
    })?;

    let mut response = response;
    let status = response.status();
    let text = response
        .body_mut()
        .read_to_string()
        .with_context(|| format!("Discord API {method} {path} returned invalid text"))?;
    if !status.is_success() {
        bail!("Discord API {method} {path} returned HTTP {status}: {text}");
    }
    if text.trim().is_empty() {
        return Ok(json!({ "ok": true }));
    }
    serde_json::from_str(&text)
        .with_context(|| format!("Discord API {method} {path} returned invalid JSON"))
}

fn discord_send_message(
    discord: &DiscordConfig,
    content: &str,
    timeout: Duration,
) -> Result<Value> {
    discord_api_request(
        discord,
        "POST",
        &format!("/channels/{}/messages", discord.channel_id),
        Some(&json!({
            "content": content,
            "allowed_mentions": { "parse": [] }
        })),
        timeout,
    )
}

#[cfg(not(test))]
fn discord_trigger_typing(discord: &DiscordConfig, timeout: Duration) -> Result<Value> {
    discord_api_request(
        discord,
        "POST",
        &format!("/channels/{}/typing", discord.channel_id),
        None,
        timeout,
    )
}

#[cfg(test)]
fn discord_trigger_typing(discord: &DiscordConfig, _timeout: Duration) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "channelId": discord.channel_id
    }))
}

fn discord_for_channel(discord: &DiscordConfig, channel_id: &str) -> DiscordConfig {
    DiscordConfig {
        bot_token: discord.bot_token.clone(),
        channel_id: channel_id.to_string(),
        channel_ids: Vec::new(),
        allowed_user_id: discord.allowed_user_id.clone(),
        enabled: discord.enabled,
    }
}

fn discord_message_id(message: &Value) -> Option<i64> {
    message
        .get("id")
        .and_then(Value::as_str)
        .and_then(|id| id.parse::<i64>().ok())
}

fn discord_author_user_id(message: &Value) -> Option<String> {
    message
        .get("author")
        .and_then(|author| author.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn discord_author_is_bot(message: &Value) -> bool {
    message
        .get("author")
        .and_then(|author| author.get("bot"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn discord_referenced_message_id(message: &Value) -> Option<i64> {
    message
        .get("message_reference")
        .and_then(|reference| reference.get("message_id"))
        .and_then(Value::as_str)
        .and_then(|id| id.parse::<i64>().ok())
}

fn discord_authorized(discord: &DiscordConfig, user_id: Option<&str>) -> bool {
    match discord.allowed_user_id.as_deref() {
        Some(allowed) => user_id == Some(allowed),
        None => true,
    }
}

fn discord_message_content(message: &Value) -> Option<String> {
    message
        .get("content")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_discord_command_text(text: &str) -> Option<DiscordInboundCommand> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let command = parts.next().unwrap_or_default().to_ascii_lowercase();
    let rest = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    match command.as_str() {
        "/start" => Some(DiscordInboundCommand::Start),
        "/help" => Some(DiscordInboundCommand::Help),
        "/away" => Some(DiscordInboundCommand::Away),
        "/back" => Some(DiscordInboundCommand::Back),
        "/repair" => Some(DiscordInboundCommand::Repair),
        "/status" => Some(DiscordInboundCommand::Status),
        "/telegram_on" => Some(DiscordInboundCommand::TelegramOn),
        "/telegram_off" => Some(DiscordInboundCommand::TelegramOff),
        "/discord_on" => Some(DiscordInboundCommand::DiscordOn),
        "/discord_off" => Some(DiscordInboundCommand::DiscordOff),
        "/threads" => Some(DiscordInboundCommand::Threads(rest)),
        "/new" => Some(DiscordInboundCommand::NewThread(rest)),
        "/project" => Some(DiscordInboundCommand::Project(rest)),
        _ => Some(DiscordInboundCommand::Unknown(command)),
    }
}

fn discord_help_text() -> String {
    telegram_help_text()
        .replace(
            "Codex remote is ready.",
            "Codex remote is ready in Discord.",
        )
        .replace("Telegram's Reply action", "Discord's Reply action")
        .replace("Telegram threads", "Discord threads")
}

fn discord_status_text(conn: &Connection) -> Result<String> {
    Ok(telegram_status_text(conn)?
        .replace(
            "Pending Telegram notifications",
            "Pending Discord notifications",
        )
        .replace("Telegram's Reply action", "Discord's Reply action"))
}

fn prepared_discord_texts(prepared: &PreparedTelegramDelivery) -> Vec<String> {
    prepared
        .payloads
        .iter()
        .filter_map(|payload| payload.get("text").and_then(Value::as_str))
        .map(|text| {
            text.replace("Telegram's Reply action", "Discord's Reply action")
                .replace(
                    "Use the buttons below, or use Discord's Reply action on this message.",
                    "Reply YES or NO to this message.",
                )
        })
        .collect()
}

fn deliver_prepared_discord_delivery(
    conn: &Connection,
    discord: &DiscordConfig,
    prepared: &PreparedTelegramDelivery,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut message_ids = Vec::new();
    for text in prepared_discord_texts(prepared) {
        let response = discord_send_message(discord, &text, timeout)?;
        let message_id =
            discord_message_id(&response).context("Discord create message response missing id")?;
        if let Some(thread_id) = prepared.thread_id.as_deref() {
            insert_telegram_message_route(
                conn,
                &discord.channel_id,
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
        .context("Discord delivery did not send any messages")?;
    if let Some(thread_id) = prepared.thread_id.as_deref() {
        clear_discord_typing_indicator(conn, discord, thread_id)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "discord",
        "messageId": first_message_id,
        "messageIds": message_ids,
        "chunks": message_ids.len(),
        "threadId": prepared.thread_id
    }))
}

pub(crate) fn deliver_discord_event(
    conn: &Connection,
    discord: &DiscordConfig,
    event: &Value,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut deliveries = Vec::new();
    for channel_id in discord_config_channel_ids(discord) {
        let channel_discord = discord_for_channel(discord, channel_id);
        let prepared = prepare_telegram_delivery(channel_id, event)?;
        let result =
            deliver_prepared_discord_delivery(conn, &channel_discord, &prepared, now, timeout)?;
        deliveries.push(json!({
            "channelId": channel_id,
            "result": result
        }));
    }
    Ok(json!({
        "ok": true,
        "transport": "discord",
        "channels": deliveries
    }))
}

fn send_recent_thread_snapshot(
    conn: &Connection,
    discord: &DiscordConfig,
    snapshot: &BridgeThreadSnapshot,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let prepared = prepare_telegram_thread_snapshot_delivery(&discord.channel_id, snapshot)?;
    deliver_prepared_discord_delivery(conn, discord, &prepared, now, timeout)
}

fn current_project_for_identity<'a>(
    config: &'a DaemonConfig,
    conn: &Connection,
    channel_id: &str,
    user_id: Option<&str>,
) -> Result<Option<&'a RegisteredProject>> {
    if let Some(project_id) = get_telegram_current_project_id(conn, channel_id, user_id)? {
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

fn discord_typing_key(channel_id: &str, thread_id: &str) -> String {
    format!("discord_typing:{channel_id}:{thread_id}")
}

fn register_discord_typing_indicator(
    conn: &Connection,
    discord: &DiscordConfig,
    thread_id: &str,
    now: u64,
) -> Result<()> {
    set_setting_text(
        conn,
        &discord_typing_key(&discord.channel_id, thread_id),
        &json!({
            "channelId": discord.channel_id,
            "threadId": thread_id,
            "until": now + DISCORD_TYPING_TTL_MS,
            "nextAt": now
        })
        .to_string(),
    )
}

pub(crate) fn refresh_discord_typing_indicators(
    conn: &Connection,
    discord: &DiscordConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let rows = crate::state::list_settings_with_prefix(conn, "discord_typing:")?;
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
        let channel_id = value.get("channelId").and_then(Value::as_str);
        if channel_id != Some(discord.channel_id.as_str()) {
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
        match discord_trigger_typing(discord, timeout) {
            Ok(_) => {
                sent += 1;
                set_setting_text(
                    conn,
                    &key,
                    &json!({
                        "channelId": discord.channel_id,
                        "threadId": value.get("threadId").cloned().unwrap_or(Value::Null),
                        "until": until,
                        "nextAt": now + DISCORD_TYPING_REFRESH_MS
                    })
                    .to_string(),
                )?;
            }
            Err(_) => failed += 1,
        }
    }
    Ok(json!({
        "ok": failed == 0,
        "transport": "discord",
        "channelId": discord.channel_id,
        "active": active,
        "sent": sent,
        "expired": expired,
        "failed": failed
    }))
}

fn clear_discord_typing_indicator(
    conn: &Connection,
    discord: &DiscordConfig,
    thread_id: &str,
) -> Result<()> {
    delete_setting(conn, &discord_typing_key(&discord.channel_id, thread_id))
}

fn send_codex_reply_to_thread(
    conn: &Connection,
    config: &DaemonConfig,
    thread_id: &str,
    message: &str,
    now: u64,
) -> Result<Value> {
    let mut client = CodexAppServerClient::connect_configured(config)?;
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
    record_action(
        conn,
        thread_id,
        "discord_reply",
        json!({
            "message": message,
            "resumed": resumed,
            "started": started,
            "sentAt": now
        }),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "discord_reply",
        "threadId": thread_id,
        "message": message,
        "codex": {
            "transport": transport.transport,
            "appServerPid": transport.app_server_pid
        },
        "delivery": {
            "mode": "daemon_sync",
            "status": "turn_started",
            "startedTurnId": started_turn_id.as_deref()
        },
        "sentAt": now
    }))
}

fn start_new_thread_from_discord(
    conn: &Connection,
    config: &DaemonConfig,
    project: &RegisteredProject,
    message: &str,
    now: u64,
) -> Result<Value> {
    let mut client = CodexAppServerClient::connect_configured(config)?;
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
        "discord_new_thread",
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
    Ok(result)
}

fn send_new_thread_confirmation(
    conn: &Connection,
    discord: &DiscordConfig,
    project: &RegisteredProject,
    result: &Value,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("new thread result missing threadId")?;
    let text = telegram_new_thread_confirmation_text(project, result)?
        .replace("Telegram's Reply action", "Discord's Reply action");
    let sent = discord_send_message(discord, &text, timeout)?;
    let message_id = discord_message_id(&sent).context("Discord confirmation missing id")?;
    insert_telegram_message_route(
        conn,
        &discord.channel_id,
        message_id,
        thread_id,
        &format!("discord_new_thread:{thread_id}"),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "discord_new_thread_confirmation",
        "threadId": thread_id,
        "messageId": message_id
    }))
}

fn send_new_thread_prompt_for_project(
    conn: &Connection,
    discord: &DiscordConfig,
    project: &RegisteredProject,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let text = format!(
        "What should Codex work on in {}?\n{}\n\nUse Discord's Reply action on this message with the prompt for the new thread.",
        project.label, project.cwd
    );
    let sent = discord_send_message(discord, &text, timeout)?;
    let message_id = discord_message_id(&sent).context("Discord prompt missing id")?;
    insert_telegram_command_route(
        conn,
        &discord.channel_id,
        message_id,
        TelegramCommandRouteKind::NewThread,
        Some(&json!({ "projectId": project.id })),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "discord_new_thread_prompt",
        "projectId": project.id,
        "messageId": message_id
    }))
}

fn parse_discord_threads_limit(raw: Option<&str>) -> Result<u64> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_DISCORD_THREADS_LIMIT);
    };
    let limit = raw.parse::<u64>().with_context(|| {
        format!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_DISCORD_THREADS_LIMIT}"
        )
    })?;
    if !(1..=MAX_DISCORD_THREADS_LIMIT).contains(&limit) {
        bail!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_DISCORD_THREADS_LIMIT}"
        );
    }
    Ok(limit)
}

fn extract_discord_reply_route(
    conn: &Connection,
    message: &Value,
    discord: &DiscordConfig,
) -> Result<Option<RoutedDiscordReply>> {
    if discord_author_is_bot(message) {
        return Ok(None);
    }
    let user_id = discord_author_user_id(message);
    if !discord_authorized(discord, user_id.as_deref()) {
        return Ok(None);
    }
    let Some(reply_message_id) = discord_referenced_message_id(message) else {
        return Ok(None);
    };
    let Some(text) = discord_message_content(message) else {
        return Ok(None);
    };
    let thread_id = lookup_telegram_message_route(conn, &discord.channel_id, reply_message_id)?;
    Ok(thread_id.map(|thread_id| RoutedDiscordReply {
        thread_id,
        message: text,
    }))
}

fn extract_discord_command_prompt_reply(
    conn: &Connection,
    message: &Value,
    discord: &DiscordConfig,
) -> Result<Option<RoutedDiscordCommandPromptReply>> {
    if discord_author_is_bot(message) {
        return Ok(None);
    }
    let user_id = discord_author_user_id(message);
    if !discord_authorized(discord, user_id.as_deref()) {
        return Ok(None);
    }
    let Some(reply_message_id) = discord_referenced_message_id(message) else {
        return Ok(None);
    };
    let Some(message_text) = discord_message_content(message) else {
        return Ok(None);
    };
    let Some((kind, payload)) =
        lookup_telegram_command_route(conn, &discord.channel_id, reply_message_id)?
    else {
        return Ok(None);
    };
    Ok(Some(RoutedDiscordCommandPromptReply {
        kind,
        message: message_text,
        project_id: payload
            .as_ref()
            .and_then(|value| value.get("projectId"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

fn extract_discord_command(
    message: &Value,
    discord: &DiscordConfig,
) -> Option<DiscordInboundCommand> {
    if discord_author_is_bot(message) || discord_referenced_message_id(message).is_some() {
        return None;
    }
    let user_id = discord_author_user_id(message);
    if !discord_authorized(discord, user_id.as_deref()) {
        return None;
    }
    discord_message_content(message).and_then(|content| parse_discord_command_text(&content))
}

fn execute_threads_command(
    conn: &Connection,
    discord: &DiscordConfig,
    raw_limit: Option<String>,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let limit = parse_discord_threads_limit(raw_limit.as_deref())?;
    let config = load_daemon_config()?;
    let mut client = CodexAppServerClient::connect_configured(&config)?;
    sync_state_from_live(&mut client, conn, now, limit, false)?;
    let snapshots = list_recent_thread_snapshots_from_db(conn, limit)?;
    if snapshots.is_empty() {
        let sent = discord_send_message(
            discord,
            "No recent Codex threads are cached yet. Open Codex locally or try again after the daemon syncs.",
            timeout,
        )?;
        return Ok(json!({
            "ok": true,
            "action": "discord_threads_empty",
            "limit": limit,
            "sent": sent
        }));
    }
    let mut sent = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        sent.push(send_recent_thread_snapshot(
            conn, discord, snapshot, now, timeout,
        )?);
    }
    Ok(json!({
        "ok": true,
        "action": "discord_threads",
        "limit": limit,
        "count": snapshots.len(),
        "sent": sent
    }))
}

fn execute_discord_command(
    conn: &Connection,
    discord: &DiscordConfig,
    message: &Value,
    command: DiscordInboundCommand,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let user_id = discord_author_user_id(message);
    match command {
        DiscordInboundCommand::Start | DiscordInboundCommand::Help => {
            let sent = discord_send_message(discord, &discord_help_text(), timeout)?;
            Ok(json!({ "ok": true, "action": "discord_help", "sent": sent }))
        }
        DiscordInboundCommand::Back => {
            let state = set_away_mode(conn, false, now)?;
            let cleared = state
                .get("clearedPendingNotifications")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let sent = discord_send_message(
                discord,
                &format!("Remote Codex mode is off. Cleared {cleared} pending notification(s)."),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "discord_back", "state": state, "sent": sent }))
        }
        DiscordInboundCommand::Status => {
            let sent = discord_send_message(discord, &discord_status_text(conn)?, timeout)?;
            Ok(json!({ "ok": true, "action": "discord_status", "sent": sent }))
        }
        DiscordInboundCommand::TelegramOn => {
            let state = crate::telegram::telegram_set_enabled_result(true, false)?;
            let sent = discord_send_message(discord, "Telegram channel is enabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "telegram_enable_command", "state": state, "sent": sent }),
            )
        }
        DiscordInboundCommand::TelegramOff => {
            let state = crate::telegram::telegram_set_enabled_result(false, false)?;
            let sent = discord_send_message(discord, "Telegram channel is disabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "telegram_disable_command", "state": state, "sent": sent }),
            )
        }
        DiscordInboundCommand::DiscordOn => {
            let state = discord_set_enabled_result(true, false)?;
            let sent = discord_send_message(discord, "Discord channel is enabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "discord_enable_command", "state": state, "sent": sent }),
            )
        }
        DiscordInboundCommand::DiscordOff => {
            let state = discord_set_enabled_result(false, false)?;
            let sent = discord_send_message(discord, "Discord channel is disabled.", timeout)?;
            Ok(
                json!({ "ok": true, "action": "discord_disable_command", "state": state, "sent": sent }),
            )
        }
        DiscordInboundCommand::Away => {
            let state = set_away_mode(conn, true, now)?;
            let sent = discord_send_message(discord, "Remote Codex mode is on.", timeout)?;
            Ok(json!({ "ok": true, "action": "discord_away", "state": state, "sent": sent }))
        }
        DiscordInboundCommand::Repair => {
            let sent = discord_send_message(
                discord,
                "Use `codex-telegram-bridge remote repair` locally to repair the shared live backend.",
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "discord_repair_hint", "sent": sent }))
        }
        DiscordInboundCommand::Threads(raw_limit) => {
            execute_threads_command(conn, discord, raw_limit, now, timeout)
        }
        DiscordInboundCommand::NewThread(prompt) => {
            let config = load_daemon_config()?;
            let current_project = current_project_for_identity(
                &config,
                conn,
                &discord.channel_id,
                user_id.as_deref(),
            )?;
            match resolve_new_thread_request(&config.projects, current_project, prompt.as_deref()) {
                Ok(request) => {
                    if let Some(prompt) = request.prompt.as_deref() {
                        let result = start_new_thread_from_discord(
                            conn,
                            &config,
                            request.project,
                            prompt,
                            now,
                        )?;
                        if let Some(thread_id) = result.get("threadId").and_then(Value::as_str) {
                            register_discord_typing_indicator(conn, discord, thread_id, now)?;
                            let _ = refresh_discord_typing_indicators(conn, discord, now, timeout);
                        }
                        let confirmation = send_new_thread_confirmation(
                            conn,
                            discord,
                            request.project,
                            &result,
                            timeout,
                            now,
                        )?;
                        Ok(json!({
                            "ok": true,
                            "action": "discord_new_thread",
                            "projectId": request.project.id,
                            "result": result,
                            "confirmation": confirmation
                        }))
                    } else {
                        send_new_thread_prompt_for_project(
                            conn,
                            discord,
                            request.project,
                            timeout,
                            now,
                        )
                    }
                }
                Err(error) => {
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = discord_send_message(
                        discord,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "discord_new_thread_needs_project",
                        "sent": sent
                    }))
                }
            }
        }
        DiscordInboundCommand::Project(Some(query)) => {
            let config = load_daemon_config()?;
            match resolve_project_query(&config.projects, &query) {
                Ok(project) => {
                    set_telegram_current_project_id(
                        conn,
                        &discord.channel_id,
                        user_id.as_deref(),
                        &project.id,
                    )?;
                    let sent = discord_send_message(
                        discord,
                        &telegram_project_text(Some(project))
                            .replace("Telegram threads", "Discord threads"),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "discord_project_set",
                        "projectId": project.id,
                        "sent": sent
                    }))
                }
                Err(error) => {
                    let current_project = current_project_for_identity(
                        &config,
                        conn,
                        &discord.channel_id,
                        user_id.as_deref(),
                    )?;
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = discord_send_message(
                        discord,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "discord_project_not_found",
                        "sent": sent
                    }))
                }
            }
        }
        DiscordInboundCommand::Project(None) => {
            let config = load_daemon_config()?;
            let current_project = current_project_for_identity(
                &config,
                conn,
                &discord.channel_id,
                user_id.as_deref(),
            )?;
            let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
            let sent = discord_send_message(
                discord,
                &telegram_projects_text(&config, current_project, &observed),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "discord_project", "sent": sent }))
        }
        DiscordInboundCommand::Unknown(command) => {
            let sent = discord_send_message(
                discord,
                &format!("I don't know {command} yet.\n\n{}", discord_help_text()),
                timeout,
            )?;
            Ok(json!({
                "ok": true,
                "action": "discord_unknown_command",
                "command": command,
                "sent": sent
            }))
        }
    }
}

fn execute_discord_command_prompt_reply(
    conn: &Connection,
    discord: &DiscordConfig,
    message: &Value,
    route: RoutedDiscordCommandPromptReply,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let user_id = discord_author_user_id(message);
    let reply_message_id = discord_referenced_message_id(message)
        .context("Discord command prompt reply missing referenced message id")?;
    match route.kind {
        TelegramCommandRouteKind::NewThread => {
            let config = load_daemon_config()?;
            let current_project = current_project_for_identity(
                &config,
                conn,
                &discord.channel_id,
                user_id.as_deref(),
            )?;
            let project = match route.project_id.as_deref() {
                Some(project_id) => config
                    .projects
                    .iter()
                    .find(|project| project.id == project_id),
                None => current_project,
            };
            let Some(project) = project else {
                let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                let sent = discord_send_message(
                    discord,
                    &format!(
                        "No project is selected for that prompt. Use /project <id> first, then try /new again.\n\n{}",
                        telegram_projects_text(&config, current_project, &observed)
                    ),
                    timeout,
                )?;
                mark_telegram_command_route_used(conn, &discord.channel_id, reply_message_id, now)?;
                return Ok(json!({
                    "ok": true,
                    "action": "discord_new_thread_prompt_needs_project",
                    "sent": sent
                }));
            };
            let result =
                start_new_thread_from_discord(conn, &config, project, &route.message, now)?;
            if let Some(thread_id) = result.get("threadId").and_then(Value::as_str) {
                register_discord_typing_indicator(conn, discord, thread_id, now)?;
                let _ = refresh_discord_typing_indicators(conn, discord, now, timeout);
            }
            mark_telegram_command_route_used(conn, &discord.channel_id, reply_message_id, now)?;
            let confirmation =
                send_new_thread_confirmation(conn, discord, project, &result, timeout, now)?;
            Ok(json!({
                "ok": true,
                "action": "discord_new_thread_prompt_reply",
                "projectId": project.id,
                "result": result,
                "confirmation": confirmation
            }))
        }
    }
}

fn list_discord_messages_after(
    discord: &DiscordConfig,
    after: Option<u64>,
    timeout: Duration,
) -> Result<Vec<Value>> {
    let mut path = format!("/channels/{}/messages?limit=50", discord.channel_id);
    if let Some(after) = after {
        path.push_str(&format!("&after={after}"));
    }
    let value = discord_api_request(discord, "GET", &path, None, timeout)?;
    value
        .as_array()
        .cloned()
        .context("Discord list channel messages response was not an array")
}

fn process_discord_channel_updates(
    conn: &Connection,
    config: &DaemonConfig,
    discord: &DiscordConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let key = format!("discord_last_message_id:{}", discord.channel_id);
    let after = get_setting_number(conn, &key)?;
    let mut messages = list_discord_messages_after(discord, after, timeout)?;
    messages.sort_by_key(|message| discord_message_id(message).unwrap_or_default());
    process_discord_channel_messages(conn, config, discord, &key, after, &messages, now, timeout)
}

fn process_discord_channel_messages(
    conn: &Connection,
    config: &DaemonConfig,
    discord: &DiscordConfig,
    cursor_key: &str,
    after: Option<u64>,
    messages: &[Value],
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    if after.is_none() {
        let latest = messages
            .iter()
            .filter_map(discord_message_id)
            .map(|message_id| message_id as u64)
            .max();
        if let Some(message_id) = latest {
            set_setting(conn, cursor_key, message_id)?;
        }
        return Ok(json!({
            "ok": true,
            "transport": "discord",
            "channelId": discord.channel_id,
            "seen": messages.len(),
            "replies": 0,
            "commandPromptReplies": 0,
            "commands": 0,
            "ignored": messages.len(),
            "initialized": true
        }));
    }

    let mut seen = 0usize;
    let mut replies = 0usize;
    let mut command_prompt_replies = 0usize;
    let mut commands = 0usize;
    let mut ignored = 0usize;
    let mut failed = 0usize;
    let mut max_message_id = after;
    for message in messages {
        let message_id = discord_message_id(message);
        if let Some(message_id) = message_id {
            max_message_id = Some(max_message_id.unwrap_or(0).max(message_id as u64));
        }
        seen += 1;
        if discord_author_is_bot(message) {
            ignored += 1;
            continue;
        }
        let outcome: Result<()> = (|| {
            if let Some(route) = extract_discord_reply_route(conn, message, discord)? {
                send_codex_reply_to_thread(conn, config, &route.thread_id, &route.message, now)?;
                register_discord_typing_indicator(conn, discord, &route.thread_id, now)?;
                let _ = refresh_discord_typing_indicators(conn, discord, now, timeout);
                replies += 1;
            } else if let Some(route) =
                extract_discord_command_prompt_reply(conn, message, discord)?
            {
                execute_discord_command_prompt_reply(conn, discord, message, route, now, timeout)?;
                command_prompt_replies += 1;
            } else if let Some(command) = extract_discord_command(message, discord) {
                execute_discord_command(conn, discord, message, command, now, timeout)?;
                commands += 1;
            } else {
                ignored += 1;
            }
            Ok(())
        })();
        if let Err(error) = outcome {
            failed += 1;
            // Keep the cursor moving past the failing message so the channel does not
            // wedge on it forever (mirrors the Telegram update batch behavior).
            let _ = discord_send_message(
                discord,
                &format!("Your message could not be processed: {error:#}"),
                timeout,
            );
        }
    }
    if let Some(message_id) = max_message_id {
        set_setting(conn, cursor_key, message_id)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "discord",
        "channelId": discord.channel_id,
        "seen": seen,
        "replies": replies,
        "commandPromptReplies": command_prompt_replies,
        "commands": commands,
        "ignored": ignored,
        "failed": failed
    }))
}

pub(crate) fn process_discord_updates(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let discord = config
        .discord
        .as_ref()
        .context("Discord is not configured. Run discord setup first.")?;
    let mut channels = Vec::new();
    let mut ok = true;
    for channel_id in discord_config_channel_ids(discord) {
        let channel_discord = discord_for_channel(discord, channel_id);
        let typing = refresh_discord_typing_indicators(conn, &channel_discord, now, timeout)
            .unwrap_or_else(|error| {
                ok = false;
                json!({
                    "ok": false,
                    "transport": "discord",
                    "channelId": channel_id,
                    "error": format!("{error:#}")
                })
            });
        match process_discord_channel_updates(conn, config, &channel_discord, now, timeout) {
            Ok(mut result) => {
                if let Some(object) = result.as_object_mut() {
                    object.insert("typing".to_string(), typing);
                }
                channels.push(result);
            }
            Err(error) => {
                ok = false;
                channels.push(json!({
                    "ok": false,
                    "transport": "discord",
                    "channelId": channel_id,
                    "error": format!("{error:#}")
                }));
            }
        }
    }
    Ok(json!({
        "ok": ok,
        "transport": "discord",
        "channels": channels
    }))
}

pub(crate) fn discord_setup_result(options: DiscordSetupOptions<'_>) -> Result<Value> {
    let bot_token = resolve_discord_bot_token(options.bot_token)?;
    let channel_ids = options
        .channel_ids
        .iter()
        .map(|channel_id| channel_id.trim())
        .filter(|channel_id| !channel_id.is_empty())
        .fold(Vec::<String>::new(), |mut ids, channel_id| {
            if !ids.iter().any(|id| id == channel_id) {
                ids.push(channel_id.to_string());
            }
            ids
        });
    let events = options.events.trim();
    let bridge_command = options.bridge_command.trim();
    let websocket_url = options.websocket_url.trim();
    if channel_ids.is_empty() {
        bail!("discord setup channel ids cannot be empty");
    }
    if events.is_empty() {
        bail!("discord setup events cannot be empty");
    }
    if bridge_command.is_empty() {
        bail!("discord setup bridge command cannot be empty");
    }
    validate_shared_websocket_url(websocket_url)
        .context("discord setup websocket url is invalid")?;

    let discord = DiscordConfig {
        bot_token,
        channel_id: channel_ids[0].clone(),
        channel_ids: channel_ids.clone(),
        allowed_user_id: options
            .allowed_user_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        enabled: true,
    };
    let existing = read_daemon_config_raw()?;
    let config = DaemonConfig {
        version: 4,
        bridge_command: bridge_command.to_string(),
        events: events.to_string(),
        telegram: existing.as_ref().and_then(|config| config.telegram.clone()),
        discord: Some(discord.clone()),
        codex: Some(CodexConfig {
            live_mode: CodexLiveMode::Shared,
            websocket_url: websocket_url.to_string(),
        }),
        projects: existing
            .map(|config| config.projects.clone())
            .unwrap_or_default(),
    };
    let config_path = if options.dry_run {
        daemon_config_path()?
    } else {
        let mut tests = Vec::new();
        for channel_id in &channel_ids {
            let channel_discord = discord_for_channel(&discord, channel_id);
            let test = discord_send_message(
                &channel_discord,
                "Codex Discord bridge configured.",
                Duration::from_secs(10),
            )
            .with_context(|| {
                format!("failed to send Discord setup confirmation to channel {channel_id}")
            })?;
            tests.push(json!({
                "channelId": channel_id,
                "testMessageId": test.get("id").cloned().unwrap_or(Value::Null)
            }));
        }
        let path = write_daemon_config(&config)?;
        return Ok(json!({
            "ok": true,
            "action": "discord_setup",
            "dryRun": false,
            "configPath": path.display().to_string(),
            "discord": {
                "configured": true,
                "botToken": "<redacted>",
                "channelId": discord.channel_id,
                "channelIds": channel_ids,
                "allowedUserId": discord.allowed_user_id,
                "tests": tests
            },
            "config": redacted_daemon_config(&config),
            "daemonCommand": crate::daemon_run_command(bridge_command),
            "nextStep": "Install and start the daemon. Use /away in Discord before leaving; replies and new threads use the shared Codex live backend."
        }));
    };
    Ok(json!({
        "ok": true,
        "action": "discord_setup",
        "dryRun": true,
        "configPath": config_path.display().to_string(),
        "discord": {
            "configured": true,
            "botToken": "<redacted>",
            "channelId": discord.channel_id,
            "channelIds": channel_ids,
            "allowedUserId": discord.allowed_user_id
        },
        "config": redacted_daemon_config(&config),
        "daemonCommand": crate::daemon_run_command(bridge_command)
    }))
}

pub(crate) fn discord_status_result() -> Result<Value> {
    let config = read_daemon_config_raw()?;
    let discord = config.as_ref().and_then(|config| config.discord.as_ref());
    Ok(json!({
        "ok": true,
        "action": "discord_status",
        "configPath": daemon_config_path()?.display().to_string(),
        "configured": discord.is_some(),
        "enabled": discord.map(|discord| discord.enabled).unwrap_or(false),
        "config": config.as_ref().map(redacted_daemon_config)
    }))
}

pub(crate) fn discord_set_enabled_result(enabled: bool, dry_run: bool) -> Result<Value> {
    let path = daemon_config_path()?;
    let mut config =
        read_daemon_config_raw()?.context("daemon config is missing. Run discord setup first.")?;
    let discord = config
        .discord
        .as_mut()
        .context("Discord is not configured. Run discord setup first.")?;
    let previous_enabled = discord.enabled;
    discord.enabled = enabled;
    if !dry_run {
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": if enabled { "discord_enable" } else { "discord_disable" },
        "dryRun": dry_run,
        "configured": true,
        "previousEnabled": previous_enabled,
        "enabled": enabled,
        "configPath": path.display().to_string(),
        "config": redacted_daemon_config(&config)
    }))
}

pub(crate) fn discord_test_result(
    message: &str,
    timeout: Duration,
    dry_run: bool,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let discord = config
        .discord
        .as_ref()
        .context("Discord is not configured. Run discord setup first.")?;
    let text = normalized_message(Some(message))
        .unwrap_or_else(|| "Codex Discord bridge test".to_string());
    let payload = json!({
        "content": text,
        "allowed_mentions": { "parse": [] }
    });
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "discord_test",
            "dryRun": true,
            "channelIds": discord_config_channel_ids(discord),
            "payload": payload
        }));
    }
    let mut sent = Vec::new();
    for channel_id in discord_config_channel_ids(discord) {
        let channel_discord = discord_for_channel(discord, channel_id);
        let message = discord_send_message(&channel_discord, &text, timeout)?;
        sent.push(json!({
            "channelId": channel_id,
            "messageId": message.get("id").cloned().unwrap_or(Value::Null)
        }));
    }
    Ok(json!({
        "ok": true,
        "action": "discord_test",
        "dryRun": false,
        "sent": sent
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discord_command_parser_supports_core_commands() {
        assert_eq!(
            parse_discord_command_text("/away"),
            Some(DiscordInboundCommand::Away)
        );
        assert_eq!(
            parse_discord_command_text("/new Fix the formatter"),
            Some(DiscordInboundCommand::NewThread(Some(
                "Fix the formatter".to_string()
            )))
        );
        assert_eq!(
            parse_discord_command_text("/threads 10"),
            Some(DiscordInboundCommand::Threads(Some("10".to_string())))
        );
        assert_eq!(parse_discord_command_text("hello"), None);
    }

    #[test]
    fn discord_setup_dry_run_redacts_token() {
        let result = discord_setup_result(DiscordSetupOptions {
            bot_token: Some("discord-secret"),
            channel_ids: &["123".to_string()],
            allowed_user_id: Some("456"),
            events: crate::DEFAULT_NOTIFICATION_EVENTS,
            bridge_command: "codex-telegram-bridge",
            websocket_url: "ws://127.0.0.1:4500",
            dry_run: true,
        })
        .expect("discord setup dry run");
        assert_eq!(result["action"], "discord_setup");
        assert_eq!(result["discord"]["botToken"], "<redacted>");
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("discord-secret"),
            "setup output must not leak Discord bot token"
        );
    }

    #[test]
    fn failing_discord_message_is_skipped_and_does_not_wedge_the_channel() {
        let _guard = crate::state::test_env_lock().lock().expect("test env lock");
        let root = std::env::temp_dir().join(format!("codex-discord-wedge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create state dir");
        let previous_state_dir = std::env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR").ok();
        std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", &root);

        let conn = crate::state::create_state_db_in_memory().expect("db");
        let discord = DiscordConfig {
            bot_token: "fake-token".to_string(),
            channel_id: "111".to_string(),
            channel_ids: Vec::new(),
            allowed_user_id: Some("789".to_string()),
            enabled: true,
        };
        let config = DaemonConfig {
            version: 4,
            bridge_command: "codex-telegram-bridge".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            discord: Some(discord.clone()),
            codex: Some(crate::CodexConfig {
                live_mode: crate::CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:9".to_string(),
            }),
            projects: Vec::new(),
        };
        let messages = vec![
            json!({
                "id": "101",
                "author": { "id": "789", "bot": false },
                "content": "/threads 10"
            }),
            json!({
                "id": "102",
                "author": { "id": "789", "bot": false },
                "content": "plain message"
            }),
        ];
        let cursor_key = format!("discord_last_message_id:{}", discord.channel_id);

        let result = process_discord_channel_messages(
            &conn,
            &config,
            &discord,
            &cursor_key,
            Some(100),
            &messages,
            0,
            Duration::from_secs(1),
        )
        .expect("discord message batch");

        // /threads fails (daemon config is missing) but the cursor still advances past
        // the failing message and later messages are still processed: no wedge.
        assert_eq!(result["failed"], 1);
        assert_eq!(result["ignored"], 1);
        assert_eq!(
            get_setting_number(&conn, &cursor_key)
                .expect("cursor lookup")
                .expect("cursor set"),
            102
        );

        if let Some(previous_state_dir) = &previous_state_dir {
            std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
        } else {
            std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
        }
        let _ = std::fs::remove_dir_all(&root);
    }
}

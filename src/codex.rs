use anyhow::{bail, Context, Result};
use notify::Watcher as _;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::Write;
#[cfg(test)]
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::process::{Child, ChildStdin, ChildStdout};
use std::process::{Command, Stdio};
#[cfg(test)]
use std::sync::mpsc::{self, Receiver};
#[cfg(test)]
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::{CodexConfig, CodexLiveMode, DaemonConfig};
use crate::now_millis;
use crate::projects::derive_project_label;
use crate::state::{
    clear_pending_outbound_events, derive_thread_display_name, get_setting_number,
    get_setting_text, recent_actions_json, reconcile_thread_snapshots, remote_mode_status_path,
    set_setting, set_setting_text, should_emit_for_away_window, upsert_thread_snapshot,
    BridgeThreadSnapshot, PendingPrompt,
};
use crate::ws::{validate_shared_websocket_url, WsJsonRpcTransport};

#[derive(Debug, Clone)]
pub(crate) struct ResolvedBinary {
    pub(crate) path: PathBuf,
    pub(crate) source: &'static str,
}

fn last_preview_from_thread(thread: &Value) -> Option<String> {
    let turns = thread.get("turns")?.as_array()?;
    let mut fallback: Option<String> = None;
    for turn in turns.iter().rev() {
        let Some(items) = turn.get("items").and_then(|value| value.as_array()) else {
            continue;
        };
        for item in items.iter().rev() {
            if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
                continue;
            }
            let Some(text) = item.get("text").and_then(Value::as_str).map(str::trim) else {
                continue;
            };
            if text.is_empty() {
                continue;
            }
            if item.get("phase").and_then(Value::as_str) == Some("final_answer") {
                return Some(text.to_string());
            }
            if fallback.is_none() {
                fallback = Some(text.to_string());
            }
        }
    }
    fallback
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn last_preview_from_summary(summary: &Value) -> Option<String> {
    string_field(summary, "lastPreview")
        .or_else(|| string_field(summary, "preview"))
        .or_else(|| string_field(summary, "lastMessage"))
}

fn event_type(event: &Value) -> Option<&str> {
    event.get("type").and_then(Value::as_str)
}

fn summary_updated_at(summary: &Value) -> Option<u64> {
    summary.get("updatedAt").and_then(Value::as_u64)
}

fn turn_timestamp(turn: &Value) -> Option<u64> {
    turn.get("completedAt")
        .and_then(Value::as_u64)
        .or_else(|| turn.get("updatedAt").and_then(Value::as_u64))
        .or_else(|| turn.get("startedAt").and_then(Value::as_u64))
}

fn latest_turn_timestamp(thread: &Value) -> Option<u64> {
    thread
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| turns.iter().filter_map(turn_timestamp).max())
}

fn extract_item_text(item: &Value) -> Option<String> {
    if let Some(text) = item.get("text").and_then(Value::as_str).map(str::trim) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }

    let text = item
        .get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn find_last_message_by_type(thread: &Value, item_type: &str) -> Option<String> {
    let turns = thread.get("turns").and_then(Value::as_array)?;
    for turn in turns.iter().rev() {
        let Some(items) = turn.get("items").and_then(Value::as_array) else {
            continue;
        };
        for item in items.iter().rev() {
            if item.get("type").and_then(Value::as_str) == Some(item_type) {
                if let Some(text) = extract_item_text(item) {
                    return Some(text);
                }
            }
        }
    }
    None
}

fn latest_question(thread: &Value) -> Option<String> {
    let last_user_message = find_last_message_by_type(thread, "userMessage")?;
    last_user_message.contains('?').then_some(last_user_message)
}

fn status_flags_waiting_for_approval(status_flags: &[String]) -> bool {
    status_flags.iter().any(|flag| flag == "waitingOnApproval")
}

fn status_flags_waiting_for_input(status_flags: &[String]) -> bool {
    status_flags
        .iter()
        .any(|flag| flag == "waitingOnUserInput" || flag == "waitingOnInput")
}

fn active_flag_is_waiting(flag: &Value) -> bool {
    matches!(
        flag.as_str(),
        Some("waitingOnUserInput") | Some("waitingOnInput") | Some("waitingOnApproval")
    )
}

fn active_flags_are_waiting(value: &Value) -> bool {
    value
        .pointer("/status/activeFlags")
        .and_then(Value::as_array)
        .map(|active_flags| active_flags.iter().any(active_flag_is_waiting))
        .unwrap_or(false)
}

fn show_pending_prompt(status_flags: &[String]) -> Option<Value> {
    if status_flags_waiting_for_approval(status_flags) {
        return Some(json!({
            "kind": "approval",
            "promptStatus": "Needs approval"
        }));
    }
    if status_flags_waiting_for_input(status_flags) {
        return Some(json!({
            "kind": "reply",
            "promptStatus": "Needs input"
        }));
    }
    None
}

fn build_delta_summary(
    pending_prompt: Option<&Value>,
    last_turn_status: Option<&str>,
    last_agent_message: Option<&str>,
) -> Option<String> {
    let agent_suffix = last_agent_message
        .filter(|value| !value.trim().is_empty())
        .map(|message| format!(" · Last agent update: {message}"))
        .unwrap_or_default();

    if let Some(prompt) = pending_prompt {
        let prompt_status = prompt
            .get("promptStatus")
            .and_then(Value::as_str)
            .unwrap_or("Needs input");
        return Some(format!("{prompt_status}{agent_suffix}"));
    }

    if let Some(status) = last_turn_status {
        return Some(format!("Last turn status: {status}{agent_suffix}"));
    }

    last_agent_message.map(str::to_string)
}

fn build_unresolved_summary(
    pending_prompt: Option<&Value>,
    latest_question: Option<&str>,
    last_user_message: Option<&str>,
) -> Option<String> {
    if pending_prompt
        .and_then(|prompt| prompt.get("kind"))
        .and_then(Value::as_str)
        == Some("approval")
    {
        return Some("Approval is still pending.".to_string());
    }

    if let Some(question) = latest_question {
        return Some(format!("Latest unresolved question: {question}"));
    }

    if pending_prompt
        .and_then(|prompt| prompt.get("kind"))
        .and_then(Value::as_str)
        == Some("reply")
    {
        if let Some(message) = last_user_message {
            return Some(format!("Reply still needed: {message}"));
        }
    }

    None
}

pub(crate) fn derive_pending_prompt(
    thread_id: &str,
    status_flags: &[String],
    last_preview: Option<String>,
) -> Option<PendingPrompt> {
    if status_flags_waiting_for_approval(status_flags) {
        return Some(PendingPrompt {
            prompt_id: format!("approval:{thread_id}"),
            kind: "approval".to_string(),
            status: "Needs approval".to_string(),
            question: last_preview.or_else(|| Some("Approval required".to_string())),
        });
    }
    if status_flags_waiting_for_input(status_flags) {
        return Some(PendingPrompt {
            prompt_id: format!("reply:{thread_id}"),
            kind: "reply".to_string(),
            status: "Needs input".to_string(),
            question: last_preview.or_else(|| Some("Input required".to_string())),
        });
    }
    None
}

pub(crate) fn normalize_thread_snapshot(
    summary: &Value,
    thread: &Value,
) -> Result<BridgeThreadSnapshot> {
    let thread_id = thread
        .get("id")
        .or_else(|| summary.get("id"))
        .and_then(Value::as_str)
        .context("thread id missing")?
        .to_string();
    let name = thread
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .or_else(|| {
            summary
                .get("name")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
        });
    let cwd = thread
        .get("cwd")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .or_else(|| {
            summary
                .get("cwd")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
        });
    let summary_or_thread_updated_at = thread
        .get("updatedAt")
        .and_then(Value::as_u64)
        .or_else(|| summary.get("updatedAt").and_then(Value::as_u64));
    let updated_at = match (summary_or_thread_updated_at, latest_turn_timestamp(thread)) {
        (Some(summary_updated_at), Some(turn_updated_at)) => {
            Some(summary_updated_at.max(turn_updated_at))
        }
        (Some(summary_updated_at), None) => Some(summary_updated_at),
        (None, Some(turn_updated_at)) => Some(turn_updated_at),
        (None, None) => None,
    };
    let status_type = thread
        .pointer("/status/type")
        .and_then(Value::as_str)
        .or_else(|| summary.pointer("/status/type").and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    let status_flags = thread
        .pointer("/status/activeFlags")
        .and_then(Value::as_array)
        .or_else(|| {
            summary
                .pointer("/status/activeFlags")
                .and_then(Value::as_array)
        })
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let turns = thread.get("turns").and_then(Value::as_array);
    let last_turn_status = turns
        .and_then(|items| items.last())
        .and_then(|turn| turn.get("status"))
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .or_else(|| string_field(summary, "lastTurnStatus"));
    let last_preview =
        last_preview_from_thread(thread).or_else(|| last_preview_from_summary(summary));
    let pending_prompt = derive_pending_prompt(&thread_id, &status_flags, last_preview.clone());
    Ok(BridgeThreadSnapshot {
        thread_id,
        name,
        cwd,
        updated_at,
        status_type,
        status_flags,
        last_turn_status,
        last_preview,
        pending_prompt,
    })
}

pub(crate) fn start_new_thread_dry_run(cwd: Option<&str>, message: Option<&str>) -> Value {
    json!({
        "ok": true,
        "action": "new",
        "dryRun": true,
        "cwd": cwd,
        "message": message.map(str::trim).filter(|value| !value.is_empty())
    })
}

pub(crate) fn normalized_message(message: Option<&str>) -> Option<String> {
    message
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn text_input_value(text: &str) -> Value {
    json!({ "type": "text", "text": text, "text_elements": [] })
}

pub(crate) fn thread_start_params(cwd: Option<&str>) -> Value {
    let mut params = serde_json::Map::new();
    if let Some(cwd) = cwd.filter(|value| !value.trim().is_empty()) {
        params.insert("cwd".to_string(), json!(cwd));
    }
    Value::Object(params)
}

pub(crate) fn turn_start_params(thread_id: &str, cwd: Option<&str>, message: &str) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("threadId".to_string(), json!(thread_id));
    if let Some(cwd) = cwd.filter(|value| !value.trim().is_empty()) {
        params.insert("cwd".to_string(), json!(cwd));
    }
    params.insert("input".to_string(), json!([text_input_value(message)]));
    Value::Object(params)
}

pub(crate) fn start_thread_in_cwd(
    client: &mut CodexAppServerClient,
    cwd: Option<&str>,
    message: Option<&str>,
) -> Result<Value> {
    let created = client.request("thread/start", thread_start_params(cwd))?;
    let normalized_message = normalized_message(message);
    let thread_id = thread_id_from_response(&created);
    let started = match (thread_id.as_deref(), normalized_message.as_deref()) {
        (Some(thread_id), Some(message)) => {
            Some(client.request("turn/start", turn_start_params(thread_id, cwd, message))?)
        }
        _ => None,
    };
    Ok(new_thread_live_result(
        cwd,
        normalized_message.as_deref(),
        created,
        started,
    ))
}

fn thread_field_string(response: &Value, field: &str) -> Option<String> {
    response
        .get("thread")
        .and_then(|thread| thread.get(field))
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub(crate) fn thread_id_from_response(response: &Value) -> Option<String> {
    thread_field_string(response, "id")
}

pub(crate) fn thread_cwd_from_response(response: &Value, fallback: Option<&str>) -> Option<String> {
    thread_field_string(response, "cwd").or_else(|| fallback.map(str::to_string))
}

fn new_thread_live_result(
    cwd: Option<&str>,
    message: Option<&str>,
    created: Value,
    started: Option<Value>,
) -> Value {
    let thread_id = thread_id_from_response(&created);
    let resolved_cwd = thread_cwd_from_response(&created, cwd);
    json!({
        "ok": true,
        "action": "new",
        "threadId": thread_id,
        "cwd": resolved_cwd,
        "message": normalized_message(message),
        "created": created,
        "started": started
    })
}

pub(crate) fn fork_thread_dry_run(thread_id: &str, message: Option<&str>) -> Value {
    json!({
        "ok": true,
        "action": "fork",
        "dryRun": true,
        "fromThreadId": thread_id,
        "message": message.map(str::trim).filter(|value| !value.is_empty())
    })
}

pub(crate) fn fork_thread_live_result(
    from_thread_id: &str,
    message: Option<&str>,
    forked: Value,
    started: Option<Value>,
) -> Value {
    json!({
        "ok": true,
        "action": "fork",
        "fromThreadId": from_thread_id,
        "threadId": thread_id_from_response(&forked),
        "message": normalized_message(message),
        "forked": forked,
        "started": started
    })
}

pub(crate) fn sync_state_from_live(
    client: &mut CodexAppServerClient,
    conn: &Connection,
    now: u64,
    limit: u64,
    record_deliveries: bool,
) -> Result<Value> {
    let away = get_setting_text(conn, "away")?.unwrap_or_default() == "true";
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    if client.deadline_exceeded() {
        return Ok(json!({
            "synced": 0,
            "threads": [],
            "events": [],
            "away": away
        }));
    }
    let list = client.request(
        "thread/list",
        json!({
            "limit": limit,
            "sortKey": "updated_at"
        }),
    )?;
    let mut snapshots = Vec::new();
    if let Some(summaries) = list.get("data").and_then(Value::as_array) {
        for summary in summaries {
            if client.deadline_exceeded() {
                break;
            }
            let thread_id = summary
                .get("id")
                .and_then(Value::as_str)
                .context("thread id missing in thread/list")?;
            let include_turns =
                away && should_emit_for_away_window(away_started_at, summary_updated_at(summary));
            let read = client.request(
                "thread/read",
                json!({
                    "threadId": thread_id,
                    "includeTurns": include_turns
                }),
            )?;
            let Some(thread) = read.get("thread") else {
                continue;
            };
            let snapshot = normalize_thread_snapshot(summary, thread)?;
            snapshots.push(snapshot);
        }
    }
    let sync_result = reconcile_thread_snapshots(conn, now, snapshots, record_deliveries)?;
    enrich_completed_event_previews(sync_result, |thread_id| {
        if client.deadline_exceeded() {
            Ok(None)
        } else {
            fetch_full_completed_preview(client, thread_id)
        }
    })
}

fn fetch_full_completed_preview(
    client: &mut CodexAppServerClient,
    thread_id: &str,
) -> Result<Option<String>> {
    let read = client.request(
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true
        }),
    )?;
    Ok(read.get("thread").and_then(last_preview_from_thread))
}

fn enrich_completed_event_previews<F>(mut sync_result: Value, mut fetch_preview: F) -> Result<Value>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    let Some(events) = sync_result.get_mut("events").and_then(Value::as_array_mut) else {
        return Ok(sync_result);
    };
    let mut updates = Vec::new();
    for event in events {
        if event_type(event) != Some("thread_completed") {
            continue;
        }
        let Some(thread_id) = event
            .get("threadId")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(preview) = fetch_preview(&thread_id)? else {
            continue;
        };
        event["lastPreview"] = json!(preview.clone());
        updates.push((thread_id, preview));
    }

    if let Some(threads) = sync_result.get_mut("threads").and_then(Value::as_array_mut) {
        for (thread_id, preview) in updates {
            if let Some(thread) = threads
                .iter_mut()
                .find(|thread| thread.get("threadId").and_then(Value::as_str) == Some(&thread_id))
            {
                thread["lastPreview"] = json!(preview);
            }
        }
    }

    Ok(sync_result)
}

pub(crate) fn resolve_codex_binary() -> Result<ResolvedBinary> {
    let cwd = env::current_dir()?;
    let mut seen = BTreeSet::new();
    let mut candidates: Vec<ResolvedBinary> = Vec::new();

    if let Ok(override_path) = env::var("CODEX_BIN") {
        push_candidate(
            &mut candidates,
            &mut seen,
            PathBuf::from(override_path),
            "override",
        );
    }

    for root in workspace_roots(&cwd) {
        push_candidate(
            &mut candidates,
            &mut seen,
            root.join("node_modules").join(".bin").join("codex"),
            "workspace-local",
        );
    }

    if let Some(home) = dirs::home_dir() {
        push_candidate(
            &mut candidates,
            &mut seen,
            home.join("Applications/Codex.app/Contents/Resources/codex"),
            "platform-known",
        );
    }
    push_candidate(
        &mut candidates,
        &mut seen,
        PathBuf::from("/Applications/Codex.app/Contents/Resources/codex"),
        "platform-known",
    );

    if let Ok(path_codex) = which::which("codex") {
        push_candidate(&mut candidates, &mut seen, path_codex, "path");
    }

    for candidate in candidates {
        if codex_candidate_is_usable(&candidate.path) {
            return Ok(candidate);
        }
    }

    bail!(
        "Could not resolve codex executable. Set CODEX_BIN or install Codex so `codex --version` works in this environment"
    )
}

fn codex_candidate_is_usable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(metadata) = fs::metadata(path) else {
            return false;
        };
        if metadata.permissions().mode() & 0o111 == 0 {
            return false;
        }
    }
    codex_candidate_is_usable_with_timeout(path, Duration::from_secs(3))
}

fn codex_candidate_is_usable_with_timeout(path: &Path, timeout: Duration) -> bool {
    let Ok(mut child) = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };

    let started = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

fn push_candidate(
    candidates: &mut Vec<ResolvedBinary>,
    seen: &mut BTreeSet<String>,
    path: PathBuf,
    source: &'static str,
) {
    let key = path.display().to_string();
    if seen.insert(key) {
        candidates.push(ResolvedBinary { path, source });
    }
}

fn workspace_roots(start: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let mut current = start.to_path_buf();
    loop {
        roots.push(current.clone());
        match current.parent() {
            Some(parent) if parent != current => current = parent.to_path_buf(),
            _ => break,
        }
    }
    roots
}

pub(crate) fn get_away_mode(conn: &Connection) -> Result<Value> {
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    if get_setting_text(conn, "away")?.is_none() {
        if let Some(legacy) = get_setting_text(conn, "away_mode")? {
            set_setting_text(conn, "away", &legacy)?;
            conn.execute("DELETE FROM settings WHERE key = ?1", params!["away_mode"])?;
        }
    }
    let existing_session = get_setting_text(conn, "away_session_id")?;
    let away_session_id =
        existing_session.or_else(|| away_started_at.map(|value| value.to_string()));
    if let Some(session_id) = away_session_id.as_deref() {
        if get_setting_text(conn, "away_session_id")?.is_none() {
            conn.execute(
                "INSERT INTO settings(key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params!["away_session_id", session_id],
            )?;
        }
    }
    let state = json!({
        "ok": true,
        "away": get_setting_text(conn, "away")?.unwrap_or_default() == "true",
        "awayStartedAt": away_started_at,
        "awaySessionId": away_session_id
    });
    write_remote_mode_status_marker(&state)?;
    Ok(state)
}

pub(crate) fn set_away_mode(conn: &Connection, away: bool, now: u64) -> Result<Value> {
    set_setting_text(conn, "away", if away { "true" } else { "false" })?;
    conn.execute("DELETE FROM settings WHERE key = ?1", params!["away_mode"])?;

    let state = if away {
        let session_id = now.to_string();
        set_setting(conn, "away_started_at", now)?;
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params!["away_session_id", session_id],
        )?;
        json!({
            "ok": true,
            "away": true,
            "awayStartedAt": now,
            "awaySessionId": now.to_string()
        })
    } else {
        let cleared_pending = clear_pending_outbound_events(conn)?;
        conn.execute(
            "DELETE FROM settings WHERE key = ?1",
            params!["away_started_at"],
        )?;
        conn.execute(
            "DELETE FROM settings WHERE key = ?1",
            params!["away_session_id"],
        )?;
        json!({
            "ok": true,
            "away": false,
            "awayStartedAt": Value::Null,
            "awaySessionId": Value::Null,
            "clearedPendingNotifications": cleared_pending
        })
    };
    write_remote_mode_status_marker(&state)?;
    Ok(state)
}

fn write_remote_mode_status_marker(state: &Value) -> Result<()> {
    #[cfg(test)]
    if env::var_os("CODEX_TELEGRAM_BRIDGE_STATE_DIR").is_none() {
        return Ok(());
    }

    let marker = json!({
        "away": state.get("away").cloned().unwrap_or(Value::Bool(false)),
        "awayStartedAt": state.get("awayStartedAt").cloned().unwrap_or(Value::Null),
        "awaySessionId": state.get("awaySessionId").cloned().unwrap_or(Value::Null)
    });
    let content = serde_json::to_vec(&marker)?;
    let path = remote_mode_status_path()?;
    if fs::read(&path).ok().as_deref() == Some(content.as_slice()) {
        return Ok(());
    }
    fs::write(path, content)?;
    Ok(())
}

pub(crate) fn build_show_thread_result(
    conn: Option<&Connection>,
    requested_thread_id: &str,
    response: Value,
) -> Result<Value> {
    let thread = response.get("thread").cloned().unwrap_or(response);
    let thread_id = thread
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(requested_thread_id)
        .to_string();
    let name = thread
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string);
    let cwd = thread
        .get("cwd")
        .and_then(Value::as_str)
        .map(str::to_string);
    let project = derive_project_label(cwd.as_deref());
    let status_type = thread
        .pointer("/status/type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let status_flags = thread
        .pointer("/status/activeFlags")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let last_turn_status = turns
        .last()
        .and_then(|turn| turn.get("status"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let last_agent_message = find_last_message_by_type(&thread, "agentMessage");
    let last_user_message = find_last_message_by_type(&thread, "userMessage");
    let latest_question = latest_question(&thread);
    let pending_prompt = show_pending_prompt(&status_flags);
    let can_approve = pending_prompt
        .as_ref()
        .and_then(|prompt| prompt.get("kind"))
        .and_then(Value::as_str)
        == Some("approval");
    let can_reply = pending_prompt
        .as_ref()
        .and_then(|prompt| prompt.get("kind"))
        .and_then(Value::as_str)
        == Some("reply")
        || !can_approve;
    let is_waiting = pending_prompt.is_some();
    let is_completed = last_turn_status.as_deref() == Some("completed");
    let display_name = derive_thread_display_name(
        name.as_deref(),
        project.as_deref(),
        latest_question.as_deref().or(last_user_message.as_deref()),
        &thread_id,
    );
    let delta_summary = build_delta_summary(
        pending_prompt.as_ref(),
        last_turn_status.as_deref(),
        last_agent_message.as_deref(),
    );
    let unresolved_summary = build_unresolved_summary(
        pending_prompt.as_ref(),
        latest_question.as_deref(),
        last_user_message.as_deref(),
    );
    let recent_actions = match conn {
        Some(conn) => recent_actions_json(conn, &thread_id, 5)?,
        None => Vec::new(),
    };

    Ok(json!({
        "thread": {
            "threadId": thread_id,
            "name": name,
            "displayName": display_name,
            "project": project,
            "cwd": cwd,
            "statusType": status_type,
            "statusFlags": status_flags,
            "pendingPrompt": pending_prompt,
            "canReply": can_reply,
            "canApprove": can_approve,
            "isWaiting": is_waiting,
            "isCompleted": is_completed,
            "lastTurnStatus": last_turn_status,
            "lastAgentMessage": last_agent_message,
            "lastUserMessage": last_user_message,
            "latestQuestion": latest_question,
            "deltaSummary": delta_summary,
            "unresolvedSummary": unresolved_summary,
            "recentActions": recent_actions,
            "turns": turns
        }
    }))
}

pub(crate) fn classify_app_server_error_message(message: &str) -> Value {
    if message.contains("thread not loaded") {
        return json!({
            "code": "thread_not_loaded",
            "retryable": true,
            "message": message,
            "guidance": "The thread is not visible in this app-server client session. Prefer single-session flows like --follow on new/reply/approve/fork."
        });
    }

    if message.contains("is not materialized yet")
        || message.contains("includeTurns is unavailable before first user message")
    {
        return json!({
            "code": "thread_not_materialized",
            "retryable": true,
            "message": message,
            "guidance": "The thread exists but full turn materialization is not ready yet. Retry shortly or use single-session follow after creating/resuming the thread."
        });
    }

    if message.contains("no rollout found for thread id") {
        return json!({
            "code": "thread_no_rollout",
            "retryable": false,
            "message": message,
            "guidance": "The backend does not consider this thread forkable/rollable in the current context. This is likely an upstream limitation rather than a bridge bug."
        });
    }

    json!({
        "code": "app_server_error",
        "retryable": false,
        "message": message,
        "guidance": Value::Null
    })
}

pub(crate) fn parse_event_filter(input: Option<&str>) -> Option<BTreeSet<String>> {
    input
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<BTreeSet<_>>()
        })
        .filter(|set| !set.is_empty())
}

fn should_emit_event(filter: Option<&BTreeSet<String>>, event: &Value) -> bool {
    match filter {
        None => true,
        Some(filter) => event
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| filter.contains(kind))
            .unwrap_or(false),
    }
}

fn normalize_notification_event(notification: &Value) -> Value {
    match notification.get("method").and_then(Value::as_str) {
        Some("thread/status/changed") => json!({
            "type": "thread_status_changed",
            "threadId": notification.pointer("/params/threadId").cloned().unwrap_or(Value::Null),
            "status": notification.pointer("/params/status").cloned().unwrap_or(Value::Null),
            "raw": notification
        }),
        Some("turn/started") => json!({
            "type": "turn_started",
            "threadId": notification.pointer("/params/threadId").cloned().unwrap_or(Value::Null),
            "turn": notification.pointer("/params/turn").cloned().unwrap_or(Value::Null),
            "raw": notification
        }),
        Some("turn/completed") => json!({
            "type": "turn_completed",
            "threadId": notification.pointer("/params/threadId").cloned().unwrap_or(Value::Null),
            "turn": notification.pointer("/params/turn").cloned().unwrap_or(Value::Null),
            "raw": notification
        }),
        Some("item/started") => json!({
            "type": "item_started",
            "threadId": notification.pointer("/params/threadId").cloned().unwrap_or(Value::Null),
            "turnId": notification.pointer("/params/turnId").cloned().unwrap_or(Value::Null),
            "item": notification.pointer("/params/item").cloned().unwrap_or(Value::Null),
            "raw": notification
        }),
        Some("item/completed") => json!({
            "type": "item_completed",
            "threadId": notification.pointer("/params/threadId").cloned().unwrap_or(Value::Null),
            "turnId": notification.pointer("/params/turnId").cloned().unwrap_or(Value::Null),
            "item": notification.pointer("/params/item").cloned().unwrap_or(Value::Null),
            "raw": notification
        }),
        _ => json!({
            "type": "notification",
            "raw": notification
        }),
    }
}

fn push_follow_event(
    events: &mut Vec<Value>,
    event_filter: Option<&BTreeSet<String>>,
    event: Value,
) {
    if should_emit_event(event_filter, &event) {
        events.push(event);
    }
}

fn read_thread_with_turns_fallback(
    client: &mut CodexAppServerClient,
    thread_id: &str,
) -> Result<Value> {
    match client.request(
        "thread/read",
        json!({ "threadId": thread_id, "includeTurns": true }),
    ) {
        Ok(value) => Ok(value),
        Err(error) => {
            let message = format!("{error:#}");
            if !message.contains("includeTurns is unavailable before first user message")
                && !message.contains("is not materialized yet")
            {
                return Err(error);
            }
            client.request(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": false }),
            )
        }
    }
}

pub(crate) fn collect_follow_events(
    client: &mut CodexAppServerClient,
    thread_id: &str,
    message: Option<&str>,
    duration_ms: u64,
    poll_interval_ms: u64,
    event_filter: Option<&BTreeSet<String>>,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    push_follow_event(
        &mut events,
        event_filter,
        json!({
            "type": "follow_started",
            "threadId": thread_id,
            "durationMs": duration_ms,
            "pollIntervalMs": poll_interval_ms
        }),
    );

    let initial = read_thread_with_turns_fallback(client, thread_id)?;
    push_follow_event(
        &mut events,
        event_filter,
        json!({
            "type": "follow_snapshot",
            "threadId": thread_id,
            "thread": initial.get("thread").cloned().unwrap_or(Value::Null)
        }),
    );

    if let Some(message) = message.map(str::trim).filter(|value| !value.is_empty()) {
        let started = client.request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": message, "text_elements": [] }]
            }),
        )?;
        push_follow_event(
            &mut events,
            event_filter,
            json!({ "type": "follow_turn_started", "threadId": thread_id, "started": started }),
        );
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(duration_ms);
    let mut next_poll_at =
        std::time::Instant::now() + std::time::Duration::from_millis(poll_interval_ms);
    while std::time::Instant::now() < deadline {
        for notification in client.drain_notifications() {
            push_follow_event(
                &mut events,
                event_filter,
                normalize_notification_event(&notification),
            );
        }

        if poll_interval_ms > 0 && std::time::Instant::now() >= next_poll_at {
            let thread = read_thread_with_turns_fallback(client, thread_id)?;
            push_follow_event(
                &mut events,
                event_filter,
                json!({
                    "type": "follow_snapshot",
                    "threadId": thread_id,
                    "thread": thread.get("thread").cloned().unwrap_or(Value::Null)
                }),
            );
            next_poll_at =
                std::time::Instant::now() + std::time::Duration::from_millis(poll_interval_ms);
        }

        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    for notification in client.drain_notifications() {
        push_follow_event(
            &mut events,
            event_filter,
            normalize_notification_event(&notification),
        );
    }

    Ok(events)
}

fn persist_follow_events(
    conn: &Connection,
    thread_id: &str,
    events: &[Value],
    now: u64,
) -> Result<()> {
    let latest_snapshot = events
        .iter()
        .rev()
        .find(|event| event.get("type").and_then(Value::as_str) == Some("follow_snapshot"))
        .and_then(|event| event.get("thread"));

    if let Some(thread) = latest_snapshot {
        let snapshot = normalize_thread_snapshot(thread, thread)?;
        upsert_thread_snapshot(conn, &snapshot, now)?;
    }

    if events.iter().any(|event| {
        matches!(
            event.get("type").and_then(Value::as_str),
            Some("task_complete") | Some("turn.completed")
        )
    }) {
        conn.execute(
            "DELETE FROM pending_prompts WHERE thread_id = ?1",
            params![thread_id],
        )?;
    }

    Ok(())
}

pub(crate) struct FollowRun<'a> {
    pub(crate) thread_id: &'a str,
    pub(crate) duration_ms: u64,
    pub(crate) poll_interval_ms: u64,
    pub(crate) event_filter: Option<&'a BTreeSet<String>>,
    pub(crate) stream: bool,
}

pub(crate) fn follow_result_summary(
    thread_id: &str,
    duration_ms: u64,
    events: &[Value],
    include_events: bool,
) -> Value {
    let mut summary = json!({
        "ok": true,
        "action": "follow",
        "threadId": thread_id,
        "durationMs": duration_ms
    });
    if include_events {
        summary["events"] = json!(events);
    }
    summary
}

fn attach_follow_payload(mut result: Value, follow: Value, stream: bool) -> Value {
    result["follow"] = follow;
    if stream {
        json!({
            "type": "command_result",
            "result": result
        })
    } else {
        result
    }
}

fn composed_settle_duration_ms(duration_ms: u64) -> u64 {
    if duration_ms <= 100 {
        0
    } else {
        20_000
    }
}

fn event_is_terminal(event: &Value) -> bool {
    matches!(
        event.get("type").and_then(Value::as_str),
        Some("item_completed")
            | Some("task_complete")
            | Some("turn.completed")
            | Some("turn.waiting")
            | Some("approval_request")
    )
}

fn thread_snapshot_is_terminal(thread: &Value) -> bool {
    let last_turn_status = thread
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| turns.last())
        .and_then(|turn| turn.get("status"))
        .and_then(Value::as_str);
    if last_turn_status == Some("completed") {
        return true;
    }

    active_flags_are_waiting(thread)
}

fn settle_composed_follow_events(
    client: &mut CodexAppServerClient,
    thread_id: &str,
    events: &mut Vec<Value>,
    duration_ms: u64,
    event_filter: Option<&BTreeSet<String>>,
) -> Result<()> {
    if events.iter().any(event_is_terminal) {
        return Ok(());
    }

    let settle_duration_ms = composed_settle_duration_ms(duration_ms);
    if settle_duration_ms == 0 {
        return Ok(());
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(settle_duration_ms);
    while std::time::Instant::now() < deadline {
        let read = read_thread_with_turns_fallback(client, thread_id)?;
        let thread = read.get("thread").cloned().unwrap_or(Value::Null);
        push_follow_event(
            events,
            event_filter,
            json!({
                "type": "follow_snapshot",
                "threadId": thread_id,
                "thread": thread
            }),
        );
        if read
            .get("thread")
            .map(thread_snapshot_is_terminal)
            .unwrap_or(false)
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }

    Ok(())
}

pub(crate) fn attach_follow_result(
    result: Value,
    client: &mut CodexAppServerClient,
    conn: &Connection,
    follow: FollowRun<'_>,
) -> Result<Value> {
    let mut events = collect_follow_events(
        client,
        follow.thread_id,
        None,
        follow.duration_ms,
        follow.poll_interval_ms,
        follow.event_filter,
    )?;
    if follow.stream {
        for event in &events {
            println!("{}", serde_json::to_string(event)?);
        }
        let follow_summary =
            follow_result_summary(follow.thread_id, follow.duration_ms, &events, false);
        return Ok(attach_follow_payload(result, follow_summary, true));
    }

    settle_composed_follow_events(
        client,
        follow.thread_id,
        &mut events,
        follow.duration_ms,
        follow.event_filter,
    )?;
    persist_follow_events(conn, follow.thread_id, &events, now_millis()?)?;
    let follow_summary = follow_result_summary(follow.thread_id, follow.duration_ms, &events, true);
    Ok(attach_follow_payload(result, follow_summary, false))
}

#[cfg(test)]
struct FollowEventsFixture<'a> {
    pub(crate) thread_id: &'a str,
    pub(crate) duration_ms: u64,
    pub(crate) poll_interval_ms: u64,
    pub(crate) initial_thread: Option<Value>,
    pub(crate) started: Option<Value>,
    pub(crate) notifications: Vec<Value>,
    pub(crate) event_filter: Option<&'a BTreeSet<String>>,
}

#[cfg(test)]
fn build_follow_events(fixture: FollowEventsFixture<'_>) -> Result<Vec<Value>> {
    let mut events = vec![json!({
        "type": "follow_started",
        "threadId": fixture.thread_id,
        "durationMs": fixture.duration_ms,
        "pollIntervalMs": fixture.poll_interval_ms
    })];
    if let Some(thread) = fixture.initial_thread {
        events.push(json!({
            "type": "follow_snapshot",
            "threadId": fixture.thread_id,
            "thread": thread
        }));
    }
    if let Some(started) = fixture.started {
        events.push(json!({
            "type": "follow_turn_started",
            "threadId": fixture.thread_id,
            "started": started
        }));
    }
    events.extend(
        fixture
            .notifications
            .into_iter()
            .map(|notification| normalize_notification_event(&notification)),
    );
    Ok(events
        .into_iter()
        .filter(|event| should_emit_event(fixture.event_filter, event))
        .collect())
}

pub(crate) fn filter_watch_events(
    events: Vec<Value>,
    filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    match filter {
        None => events,
        Some(filter) => events
            .into_iter()
            .filter(|event| {
                event
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|kind| filter.contains(kind))
                    .unwrap_or(false)
            })
            .collect(),
    }
}

pub(crate) fn watch_thread_error_event(error: &anyhow::Error) -> Value {
    json!({
        "type": "thread_error",
        "message": error.to_string()
    })
}

fn enrich_event_with_thread(event: Value, threads: &[Value]) -> Value {
    let Some(thread_id) = event.get("threadId").and_then(Value::as_str) else {
        return event;
    };
    let Some(thread) = threads
        .iter()
        .find(|thread| thread.get("threadId").and_then(Value::as_str) == Some(thread_id))
    else {
        return event;
    };
    let mut enriched = event;
    if let Some(object) = enriched.as_object_mut() {
        object.insert("thread".to_string(), thread.clone());
    }
    enriched
}

pub(crate) fn watch_events_from_sync_result(
    sync_result: &Value,
    notifications: Vec<Value>,
    filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    let threads = sync_result
        .get("threads")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = sync_result
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|event| enrich_event_with_thread(event, &threads))
        .collect::<Vec<_>>();
    events.extend(
        notifications
            .into_iter()
            .map(|notification| normalize_notification_event(&notification))
            .map(|event| enrich_event_with_thread(event, &threads)),
    );
    filter_watch_events(events, filter)
}

pub(crate) fn run_exec_hook(command: &str, event: &Value) -> Result<()> {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to spawn exec hook: {command}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(serde_json::to_string(event)?.as_bytes())?;
    }
    let status = child
        .wait()
        .with_context(|| format!("failed to wait for exec hook: {command}"))?;
    if !status.success() {
        bail!("exec hook `{command}` exited with status {status}");
    }
    Ok(())
}

pub(crate) struct CodexWatchReceiver {
    rx: std::sync::mpsc::Receiver<()>,
    _watcher: notify::RecommendedWatcher,
}

impl CodexWatchReceiver {
    pub(crate) fn recv_timeout(&self, timeout: std::time::Duration) {
        let _ = self.rx.recv_timeout(timeout);
        while self.rx.try_recv().is_ok() {}
    }
}

pub(crate) fn start_codex_watch_receiver() -> Result<CodexWatchReceiver> {
    let home = PathBuf::from(env::var("HOME").context("HOME is not set")?);
    let sessions_root = home.join(".codex").join("sessions");
    let session_index = home.join(".codex").join("session_index.jsonl");
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::RecommendedWatcher::new(
        move |result: notify::Result<notify::Event>| {
            if result.is_ok() {
                let _ = tx.send(());
            }
        },
        notify::Config::default(),
    )?;

    let mut watched_any = false;
    if sessions_root.exists() {
        watcher.watch(&sessions_root, notify::RecursiveMode::Recursive)?;
        watched_any = true;
    }
    if session_index.exists() {
        watcher.watch(&session_index, notify::RecursiveMode::NonRecursive)?;
        watched_any = true;
    }

    if !watched_any {
        bail!("No Codex session paths exist to watch");
    }

    Ok(CodexWatchReceiver {
        rx,
        _watcher: watcher,
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexBackend {
    #[cfg(test)]
    SpawnedStdio,
    SharedWebsocket {
        url: String,
    },
}

pub(crate) fn codex_backend_from_config(config: &DaemonConfig) -> Result<CodexBackend> {
    let codex = config.codex.as_ref().context(
        "shared Codex live backend is not configured; run setup to configure codex.websocketUrl",
    )?;
    codex_backend_from_codex_config(codex)
}

pub(crate) fn codex_backend_from_codex_config(codex: &CodexConfig) -> Result<CodexBackend> {
    match codex.live_mode {
        CodexLiveMode::Shared => {
            validate_shared_websocket_url(&codex.websocket_url)?;
            Ok(CodexBackend::SharedWebsocket {
                url: codex.websocket_url.clone(),
            })
        }
    }
}

pub(crate) struct CodexAppServerClient {
    transport: CodexTransport,
    next_id: u64,
    notifications: Vec<Value>,
    pending_transport_error: Option<String>,
    request_timeout: Duration,
    deadline: Option<Instant>,
}

const SHARED_WEBSOCKET_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(5);
const SHARED_WEBSOCKET_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn shared_websocket_initialize_timeout() -> Duration {
    #[cfg(test)]
    if let Some(timeout) = websocket_test_timeout() {
        return timeout;
    }

    SHARED_WEBSOCKET_INITIALIZE_TIMEOUT
}

fn shared_websocket_request_timeout() -> Duration {
    #[cfg(test)]
    if let Some(timeout) = websocket_test_timeout() {
        return timeout;
    }

    SHARED_WEBSOCKET_REQUEST_TIMEOUT
}

#[cfg(test)]
fn websocket_test_timeout() -> Option<Duration> {
    std::env::var("CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(|ms| Duration::from_millis(ms.max(1)))
}

enum CodexTransport {
    #[cfg(test)]
    SpawnedStdio(SpawnedStdioTransport),
    SharedWebsocket(WsJsonRpcTransport),
}

#[cfg(test)]
struct SpawnedStdioTransport {
    child: Child,
    stdin: ChildStdin,
    messages: Receiver<AppServerReaderMessage>,
    reader: Option<JoinHandle<()>>,
    reader_error: Option<String>,
}

#[cfg(test)]
enum AppServerReaderMessage {
    Json(Value),
    Error(String),
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodexAppServerTransportInfo {
    pub(crate) transport: &'static str,
    pub(crate) app_server_pid: Option<u32>,
}

impl CodexAppServerClient {
    #[cfg(test)]
    pub(crate) fn connect() -> Result<Self> {
        Self::connect_with_backend(CodexBackend::SpawnedStdio)
    }

    pub(crate) fn connect_configured(config: &DaemonConfig) -> Result<Self> {
        Self::connect_with_backend(codex_backend_from_config(config)?)
    }

    pub(crate) fn connect_with_backend(backend: CodexBackend) -> Result<Self> {
        let transport = match backend {
            #[cfg(test)]
            CodexBackend::SpawnedStdio => CodexTransport::SpawnedStdio(connect_spawned_stdio()?),
            CodexBackend::SharedWebsocket { url } => {
                CodexTransport::SharedWebsocket(WsJsonRpcTransport::connect(&url)?)
            }
        };

        let mut client = Self {
            transport,
            next_id: 1,
            notifications: Vec::new(),
            pending_transport_error: None,
            request_timeout: shared_websocket_request_timeout(),
            deadline: None,
        };

        let _ = client.request_with_timeout(
            "initialize",
            initialize_params(),
            shared_websocket_initialize_timeout(),
        )?;
        client.notify("initialized", json!({}))?;
        Ok(client)
    }

    pub(crate) fn transport_info(&self) -> CodexAppServerTransportInfo {
        match &self.transport {
            #[cfg(test)]
            CodexTransport::SpawnedStdio(transport) => CodexAppServerTransportInfo {
                transport: "spawned_stdio",
                app_server_pid: Some(transport.child.id()),
            },
            CodexTransport::SharedWebsocket(_) => CodexAppServerTransportInfo {
                transport: "shared_websocket",
                app_server_pid: None,
            },
        }
    }

    pub(crate) fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub(crate) fn deadline_exceeded(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        let envelope = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_message(&envelope)
    }

    pub(crate) fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.request_with_timeout(method, params, self.request_timeout)
    }

    fn request_with_timeout(
        &mut self,
        method: &str,
        params: Value,
        shared_websocket_timeout: Duration,
    ) -> Result<Value> {
        self.take_pending_transport_error()?;
        let timeout = match self.deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    bail!("Codex request deadline exceeded while waiting for {method}");
                }
                remaining.min(shared_websocket_timeout)
            }
            None => shared_websocket_timeout,
        };

        let id = self.next_id;
        self.next_id += 1;
        let envelope = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.write_message(&envelope)?;

        loop {
            let parsed = self.read_message_blocking(method, timeout)?;
            if let Some(result) = handle_app_server_message(&parsed, id, &mut self.notifications)? {
                return Ok(result);
            }
        }
    }

    pub(crate) fn drain_notifications(&mut self) -> Vec<Value> {
        while let Some(parsed) = self.try_read_message() {
            if parsed.get("id").is_none() && parsed.get("method").and_then(Value::as_str).is_some()
            {
                self.notifications.push(parsed);
            }
        }
        std::mem::take(&mut self.notifications)
    }

    fn write_message(&mut self, value: &Value) -> Result<()> {
        match &mut self.transport {
            #[cfg(test)]
            CodexTransport::SpawnedStdio(transport) => {
                writeln!(transport.stdin, "{}", serde_json::to_string(value)?)?;
                transport.stdin.flush()?;
                Ok(())
            }
            CodexTransport::SharedWebsocket(transport) => transport.write_json(value),
        }
    }

    fn read_message_blocking(
        &mut self,
        _method: &str,
        shared_websocket_timeout: Duration,
    ) -> Result<Value> {
        match &mut self.transport {
            #[cfg(test)]
            CodexTransport::SpawnedStdio(transport) => {
                if let Some(error) = transport.reader_error.take() {
                    bail!("{error}");
                }
                match transport.messages.recv() {
                    Ok(AppServerReaderMessage::Json(parsed)) => Ok(parsed),
                    Ok(AppServerReaderMessage::Error(message)) => bail!("{message}"),
                    Ok(AppServerReaderMessage::Closed) | Err(_) => {
                        bail!("codex app-server closed stdout while waiting for {_method}")
                    }
                }
            }
            CodexTransport::SharedWebsocket(transport) => {
                transport.read_json_with_timeout(shared_websocket_timeout)
            }
        }
    }

    fn try_read_message(&mut self) -> Option<Value> {
        match &mut self.transport {
            #[cfg(test)]
            CodexTransport::SpawnedStdio(transport) => {
                while let Ok(message) = transport.messages.try_recv() {
                    match message {
                        AppServerReaderMessage::Json(parsed) => return Some(parsed),
                        AppServerReaderMessage::Error(message) => {
                            if transport.reader_error.is_none() {
                                transport.reader_error = Some(message);
                            }
                        }
                        AppServerReaderMessage::Closed => {}
                    }
                }
                None
            }
            CodexTransport::SharedWebsocket(transport) => match transport.try_read_json() {
                Ok(message) => message,
                Err(error) => {
                    if self.pending_transport_error.is_none() {
                        self.pending_transport_error = Some(error.to_string());
                    }
                    None
                }
            },
        }
    }

    fn take_pending_transport_error(&mut self) -> Result<()> {
        if let Some(error) = self.pending_transport_error.take() {
            bail!("{error}");
        }
        Ok(())
    }
}

#[cfg(test)]
fn connect_spawned_stdio() -> Result<SpawnedStdioTransport> {
    let resolved = resolve_codex_binary()?;
    let mut child = Command::new(&resolved.path)
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to spawn {} app-server", resolved.path.display()))?;

    let stdin = child
        .stdin
        .take()
        .context("failed to open app-server stdin")?;
    let stdout = child
        .stdout
        .take()
        .context("failed to open app-server stdout")?;

    let (messages, reader) = spawn_app_server_reader(stdout);

    Ok(SpawnedStdioTransport {
        child,
        stdin,
        messages,
        reader: Some(reader),
        reader_error: None,
    })
}

#[cfg(test)]
fn spawn_app_server_reader(
    stdout: ChildStdout,
) -> (Receiver<AppServerReaderMessage>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut stdout = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match stdout.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(AppServerReaderMessage::Closed);
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(parsed) => {
                            let _ = tx.send(AppServerReaderMessage::Json(parsed));
                        }
                        Err(error) => {
                            let _ = tx.send(AppServerReaderMessage::Error(format!(
                                "invalid JSON from app-server: {trimmed}: {error}"
                            )));
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(AppServerReaderMessage::Error(format!(
                        "failed to read app-server stdout: {error}"
                    )));
                    break;
                }
            }
        }
    });
    (rx, reader)
}

fn initialize_params() -> Value {
    json!({
        "protocolVersion": 1,
        "capabilities": {},
        "clientInfo": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn handle_app_server_message(
    parsed: &Value,
    expected_id: u64,
    notifications: &mut Vec<Value>,
) -> Result<Option<Value>> {
    match parsed.get("id") {
        Some(value) if value == &json!(expected_id) => {
            if let Some(error) = parsed.get("error") {
                bail!("{}", error);
            }
            Ok(Some(parsed.get("result").cloned().unwrap_or(Value::Null)))
        }
        Some(_) => Ok(None),
        None => {
            if parsed.get("method").and_then(Value::as_str).is_some() {
                notifications.push(parsed.clone());
            }
            Ok(None)
        }
    }
}

impl Drop for CodexAppServerClient {
    fn drop(&mut self) {
        #[cfg(test)]
        if let CodexTransport::SpawnedStdio(transport) = &mut self.transport {
            let _ = transport.stdin.flush();
            let _ = transport.child.kill();
            let _ = transport.child.wait();
            if let Some(reader) = transport.reader.take() {
                let _ = reader.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        archive_from_db, classify_inbox_item, create_state_db_in_memory, get_thread_history,
        list_inbox_from_db, reconcile_thread_snapshots, record_action, resolve_archive_targets,
        test_env_lock, unarchive_thread_result, upsert_thread_snapshot, watch_once_from_db,
    };
    use crate::{get_away_mode, set_away_mode};
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    struct TempStateDir {
        previous_state_dir: Option<String>,
        root: PathBuf,
    }

    impl TempStateDir {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "codex-telegram-bridge-{name}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).expect("create temp state dir");
            let previous_state_dir = std::env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR").ok();
            std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", &root);
            Self {
                previous_state_dir,
                root,
            }
        }
    }

    impl Drop for TempStateDir {
        fn drop(&mut self) {
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn app_server_initialize_params_use_stable_capabilities() {
        let params = initialize_params();
        assert_eq!(params["capabilities"], json!({}));
        assert_eq!(params["clientInfo"]["name"], "codex-telegram-bridge");
    }

    #[test]
    fn codex_backend_from_config_requires_shared_backend_config() {
        let missing = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: "final_answer".to_string(),
            telegram: None,
            discord: None,
            codex: None,
            projects: Vec::new(),
        };
        let error = codex_backend_from_config(&missing).expect_err("missing codex config");
        assert!(format!("{error:#}").contains("shared Codex live backend is not configured"));

        let configured = DaemonConfig {
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:4500".to_string(),
            }),
            ..missing
        };
        assert_eq!(
            codex_backend_from_config(&configured).expect("configured backend"),
            CodexBackend::SharedWebsocket {
                url: "ws://127.0.0.1:4500".to_string()
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_codex_binary_prefers_platform_binary_before_path() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = test_env_lock().lock().expect("env lock");
        let root = std::env::temp_dir().join(format!("codex-resolve-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let home = root.join("home");
        std::fs::create_dir_all(root.join("path-bin")).expect("mkdir path bin");
        std::fs::create_dir_all(home.join("Applications/Codex.app/Contents/Resources"))
            .expect("mkdir platform bin");
        let bad_override = root.join("bad-codex");
        let path_codex = root.join("path-bin/codex");
        let platform_codex = home.join("Applications/Codex.app/Contents/Resources/codex");
        std::fs::write(
            &bad_override,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo bad; exit 42; fi\nexit 42\n",
        )
        .expect("write bad override");
        std::fs::write(
            &path_codex,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo codex-path-1.0; exit 0; fi\nexit 1\n",
        )
        .expect("write path codex");
        std::fs::write(
            &platform_codex,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo codex-platform-1.0; exit 0; fi\nexit 1\n",
        )
        .expect("write platform codex");
        for path in [&bad_override, &path_codex, &platform_codex] {
            let mut permissions = std::fs::metadata(path).expect("metadata").permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(path, permissions).expect("chmod");
        }

        let previous_codex_bin = std::env::var("CODEX_BIN").ok();
        let previous_path = std::env::var("PATH").ok();
        let previous_home = std::env::var("HOME").ok();
        std::env::set_var("CODEX_BIN", &bad_override);
        std::env::set_var("HOME", &home);
        std::env::set_var(
            "PATH",
            format!(
                "{}:{}",
                root.join("path-bin").display(),
                previous_path.clone().unwrap_or_default()
            ),
        );

        let resolved = resolve_codex_binary().expect("resolve codex");

        if let Some(previous) = previous_codex_bin {
            std::env::set_var("CODEX_BIN", previous);
        } else {
            std::env::remove_var("CODEX_BIN");
        }
        if let Some(previous) = previous_path {
            std::env::set_var("PATH", previous);
        } else {
            std::env::remove_var("PATH");
        }
        if let Some(previous) = previous_home {
            std::env::set_var("HOME", previous);
        } else {
            std::env::remove_var("HOME");
        }

        assert_eq!(resolved.path, platform_codex);
        assert_eq!(resolved.source, "platform-known");
    }

    #[cfg(unix)]
    #[test]
    fn codex_candidate_probe_times_out() {
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("codex-probe-timeout-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir timeout root");
        let codex_path = root.join("codex");
        std::fs::write(
            &codex_path,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then sleep 10; exit 0; fi\nexit 1\n",
        )
        .expect("write hanging codex");
        let mut permissions = std::fs::metadata(&codex_path)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&codex_path, permissions).expect("chmod");

        let started = std::time::Instant::now();
        let usable =
            codex_candidate_is_usable_with_timeout(&codex_path, Duration::from_millis(100));

        assert!(!usable);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn app_server_message_parser_preserves_notifications() {
        let mut notifications = Vec::new();
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": { "threadId": "thr_1" }
        });
        let parsed = handle_app_server_message(&notification, 7, &mut notifications)
            .expect("notification parse");
        assert!(parsed.is_none());
        assert_eq!(notifications, vec![notification]);

        let response = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "result": { "ok": true }
        });
        let parsed =
            handle_app_server_message(&response, 7, &mut notifications).expect("response parse");
        assert_eq!(parsed, Some(json!({ "ok": true })));
    }

    #[cfg(unix)]
    #[test]
    fn app_server_client_collects_notifications_between_requests() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = test_env_lock().lock().expect("env lock");
        let root = std::env::temp_dir().join(format!(
            "codex-async-notification-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir fake codex root");
        let codex_path = root.join("codex");
        std::fs::write(
            &codex_path,
            r#"#!/usr/bin/env python3
import json
import sys
import time

if len(sys.argv) > 1 and sys.argv[1] == "--version":
    print("codex-fake-async")
    raise SystemExit(0)

if len(sys.argv) > 1 and sys.argv[1] == "app-server":
    line = sys.stdin.readline()
    request = json.loads(line)
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}), flush=True)
    time.sleep(0.05)
    print(json.dumps({
        "jsonrpc": "2.0",
        "method": "item/completed",
        "params": {
            "threadId": "thr_async",
            "turnId": "turn_async",
            "item": {"type": "assistantMessage"}
        }
    }), flush=True)
    time.sleep(1)
    raise SystemExit(0)

raise SystemExit(1)
"#,
        )
        .expect("write fake codex");
        let mut permissions = std::fs::metadata(&codex_path)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&codex_path, permissions).expect("chmod fake codex");

        let previous_codex_bin = std::env::var("CODEX_BIN").ok();
        std::env::set_var("CODEX_BIN", &codex_path);
        let mut client = CodexAppServerClient::connect().expect("connect fake codex");
        if let Some(previous) = previous_codex_bin {
            std::env::set_var("CODEX_BIN", previous);
        } else {
            std::env::remove_var("CODEX_BIN");
        }
        assert_eq!(
            client.transport_info(),
            CodexAppServerTransportInfo {
                transport: "spawned_stdio",
                app_server_pid: Some(client.transport_info().app_server_pid.expect("pid")),
            }
        );
        let mut notifications: Vec<Value> = Vec::new();
        for _ in 0..20 {
            notifications.extend(client.drain_notifications());
            if notifications.iter().any(|notification| {
                notification.get("method").and_then(Value::as_str) == Some("item/completed")
            }) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }

        assert!(notifications.iter().any(|notification| {
            notification.get("method").and_then(Value::as_str) == Some("item/completed")
                && notification
                    .pointer("/params/threadId")
                    .and_then(Value::as_str)
                    == Some("thr_async")
        }));
    }

    struct FakeWsAppServer {
        url: String,
        done: Receiver<Vec<Value>>,
        server: JoinHandle<()>,
    }

    impl FakeWsAppServer {
        fn spawn(responses: Vec<Value>) -> Self {
            use std::net::TcpListener;

            let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws app-server");
            let address = listener.local_addr().expect("fake ws app-server addr");
            let url = format!("ws://{address}");
            let (done_tx, done_rx) = mpsc::channel();
            let server = thread::spawn(move || {
                let (stream, _) = listener.accept().expect("accept fake ws client");
                let mut socket = tungstenite::accept(stream).expect("accept websocket");
                let mut requests = Vec::new();
                for response in responses {
                    let message = socket.read().expect("read websocket frame");
                    let text = message.into_text().expect("websocket text frame");
                    let parsed: Value = serde_json::from_str(&text).expect("parse websocket JSON");
                    requests.push(parsed);
                    socket
                        .send(tungstenite::Message::text(
                            serde_json::to_string(&response).expect("serialize websocket response"),
                        ))
                        .expect("send websocket response");
                }
                done_tx.send(requests).expect("send fake ws requests");
            });
            Self {
                url,
                done: done_rx,
                server,
            }
        }

        fn url(&self) -> &str {
            &self.url
        }

        fn finish(self) -> Vec<Value> {
            let requests = self.done.recv().expect("fake ws requests");
            self.server.join().expect("fake ws app-server thread");
            requests
        }
    }

    struct WsTimeoutEnv {
        previous_timeout: Option<String>,
    }

    impl WsTimeoutEnv {
        fn set(timeout_ms: u64) -> Self {
            let previous_timeout = std::env::var("CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS").ok();
            std::env::set_var(
                "CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS",
                timeout_ms.to_string(),
            );
            Self { previous_timeout }
        }
    }

    impl Drop for WsTimeoutEnv {
        fn drop(&mut self) {
            if let Some(previous_timeout) = &self.previous_timeout {
                std::env::set_var("CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS", previous_timeout);
            } else {
                std::env::remove_var("CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS");
            }
        }
    }

    #[test]
    fn websocket_app_server_client_initializes_and_sends_notifications() {
        let server = FakeWsAppServer::spawn(vec![
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "protocolVersion": 1,
                    "serverInfo": { "name": "fake-codex", "version": "test" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {
                    "threadId": "thr_ws",
                    "turnId": "turn_ws",
                    "item": { "type": "assistantMessage" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "threads": []
                }
            }),
        ]);

        let mut client =
            CodexAppServerClient::connect_with_backend(CodexBackend::SharedWebsocket {
                url: server.url().to_string(),
            })
            .expect("connect websocket backend");

        assert_eq!(
            client.transport_info(),
            CodexAppServerTransportInfo {
                transport: "shared_websocket",
                app_server_pid: None,
            }
        );

        let response = client
            .request("thread/list", json!({ "limit": 1 }))
            .expect("thread/list");

        assert_eq!(response["threads"], json!([]));

        let notifications = client.drain_notifications();
        assert!(notifications.iter().any(|notification| {
            notification.get("method").and_then(Value::as_str) == Some("item/completed")
                && notification
                    .pointer("/params/threadId")
                    .and_then(Value::as_str)
                    == Some("thr_ws")
        }));

        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["method"], "initialize");
        assert_eq!(requests[1]["method"], "initialized");
        assert_eq!(requests[2]["method"], "thread/list");
    }

    #[test]
    fn websocket_notification_polling_preserves_transport_error_for_next_request() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws app-server");
        let address = listener.local_addr().expect("fake ws app-server addr");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fake ws client");
            let mut socket = tungstenite::accept(stream).expect("accept websocket");

            let initialize = socket.read().expect("read initialize");
            let initialize = initialize.into_text().expect("initialize text");
            let initialize: Value =
                serde_json::from_str(&initialize).expect("parse initialize request");
            socket
                .send(tungstenite::Message::text(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": initialize["id"],
                    "result": {
                        "protocolVersion": 1,
                            "serverInfo": { "name": "fake-codex", "version": "test" }
                        }
                    }))
                    .expect("serialize initialize response"),
                ))
                .expect("send initialize response");

            let initialized = socket.read().expect("read initialized");
            let initialized = initialized.into_text().expect("initialized text");
            let initialized: Value =
                serde_json::from_str(&initialized).expect("parse initialized notification");
            assert_eq!(initialized["method"], "initialized");

            let request = socket.read().expect("read thread/list");
            let request = request.into_text().expect("thread/list text");
            let request: Value = serde_json::from_str(&request).expect("parse thread/list request");
            socket
                .send(tungstenite::Message::text(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "method": "item/completed",
                    "params": {
                        "threadId": "thr_ws",
                        "turnId": "turn_ws",
                        "item": { "type": "assistantMessage" }
                        }
                    }))
                    .expect("serialize notification"),
                ))
                .expect("send notification");
            socket
                .send(tungstenite::Message::text(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": { "threads": [] }
                    }))
                    .expect("serialize thread/list response"),
                ))
                .expect("send thread/list response");
            socket
                .close(Some(tungstenite::protocol::CloseFrame {
                    code: tungstenite::protocol::frame::coding::CloseCode::Normal,
                    reason: "done".into(),
                }))
                .expect("close websocket");
        });

        let mut client =
            CodexAppServerClient::connect_with_backend(CodexBackend::SharedWebsocket {
                url: format!("ws://{address}"),
            })
            .expect("connect websocket backend");

        let response = client
            .request("thread/list", json!({ "limit": 1 }))
            .expect("thread/list");
        assert_eq!(response["threads"], json!([]));

        let notifications = client.drain_notifications();
        assert!(notifications.iter().any(|notification| {
            notification.get("method").and_then(Value::as_str) == Some("item/completed")
        }));

        let error = client
            .request("thread/read", json!({ "threadId": "thr_ws" }))
            .expect_err("next request should surface websocket close");
        assert!(
            format!("{error:#}").contains("websocket app-server closed connection"),
            "unexpected error: {error:#}"
        );

        server.join().expect("fake ws app-server thread");
    }

    #[test]
    fn websocket_app_server_client_times_out_when_initialize_never_returns() {
        let _guard = test_env_lock().lock().expect("env lock");
        let _timeout = WsTimeoutEnv::set(100);
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws app-server");
        let address = listener.local_addr().expect("fake ws app-server addr");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fake ws client");
            let mut socket = tungstenite::accept(stream).expect("accept websocket");
            let initialize = socket.read().expect("read initialize");
            let initialize = initialize.into_text().expect("initialize text");
            let initialize: Value =
                serde_json::from_str(&initialize).expect("parse initialize request");
            assert_eq!(initialize["method"], "initialize");
            thread::sleep(Duration::from_millis(500));
        });

        let started = Instant::now();
        let error =
            match CodexAppServerClient::connect_with_backend(CodexBackend::SharedWebsocket {
                url: format!("ws://{address}"),
            }) {
                Ok(_) => panic!("stalled initialize should time out"),
                Err(error) => error,
            };

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "initialize timeout took {:?}",
            started.elapsed()
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for websocket JSON-RPC message"),
            "unexpected error: {error:#}"
        );
        server.join().expect("fake ws app-server thread");
    }

    #[test]
    fn websocket_app_server_client_times_out_when_initialize_only_gets_keepalives() {
        let _guard = test_env_lock().lock().expect("env lock");
        let _timeout = WsTimeoutEnv::set(100);
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws app-server");
        let address = listener.local_addr().expect("fake ws app-server addr");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept fake ws client");
            let mut socket = tungstenite::accept(stream).expect("accept websocket");
            let initialize = socket.read().expect("read initialize");
            let initialize = initialize.into_text().expect("initialize text");
            let initialize: Value =
                serde_json::from_str(&initialize).expect("parse initialize request");
            assert_eq!(initialize["method"], "initialize");

            let started = Instant::now();
            while started.elapsed() < Duration::from_millis(500) {
                if socket
                    .send(tungstenite::Message::Ping(vec![1, 2, 3].into()))
                    .is_err()
                {
                    break;
                }
                thread::sleep(Duration::from_millis(25));
            }
        });

        let started = Instant::now();
        let error =
            match CodexAppServerClient::connect_with_backend(CodexBackend::SharedWebsocket {
                url: format!("ws://{address}"),
            }) {
                Ok(_) => panic!("keepalive-only initialize should time out"),
                Err(error) => error,
            };

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "initialize timeout took {:?}",
            started.elapsed()
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for websocket JSON-RPC message"),
            "unexpected error: {error:#}"
        );
        server.join().expect("fake ws app-server thread");
    }

    #[test]
    fn normalize_notification_matches_ts_event_contract() {
        let event = normalize_notification_event(&json!({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "item": { "type": "agentMessage" }
            }
        }));
        assert_eq!(event["type"], "item_completed");
        assert_eq!(event["threadId"], "thr_1");
        assert_eq!(event["turnId"], "turn_1");

        let unknown = normalize_notification_event(&json!({
            "method": "codex/custom",
            "params": { "ok": true }
        }));
        assert_eq!(unknown["type"], "notification");
        assert_eq!(unknown["raw"]["method"], "codex/custom");
    }

    #[test]
    fn follow_events_filter_normalized_notifications() {
        let events = build_follow_events(FollowEventsFixture {
            thread_id: "thr_follow",
            duration_ms: 1000,
            poll_interval_ms: 500,
            initial_thread: Some(json!({"id": "thr_follow"})),
            started: None,
            notifications: vec![json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thr_follow",
                    "turnId": "turn_1",
                    "item": { "type": "agentMessage" }
                }
            })],
            event_filter: Some(&BTreeSet::from(["item_completed".to_string()])),
        })
        .expect("follow events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "item_completed");
    }

    #[test]
    fn classifies_inbox_attention_states() {
        let approval = snapshot_fixture(
            "thr_approve",
            "/tmp/project-a",
            1000,
            "active",
            vec!["waitingOnApproval"],
            None,
        );
        let reply = snapshot_fixture(
            "thr_reply",
            "/tmp/project-b",
            900,
            "active",
            vec!["waitingOnUserInput"],
            None,
        );
        let completed = snapshot_fixture(
            "thr_done",
            "/tmp/project-c",
            800,
            "notLoaded",
            vec![],
            Some("completed"),
        );
        let active = snapshot_fixture("thr_active", "/tmp/project-d", 700, "active", vec![], None);

        let approval_item = classify_inbox_item(&approval, 2000);
        assert_eq!(approval_item.attention_reason, "pending_approval");
        assert_eq!(approval_item.waiting_on, "me");
        assert_eq!(approval_item.suggested_action, "approve");
        assert_eq!(approval_item.priority, "high");

        let reply_item = classify_inbox_item(&reply, 2000);
        assert_eq!(reply_item.attention_reason, "needs_reply");
        assert_eq!(reply_item.waiting_on, "me");
        assert_eq!(reply_item.suggested_action, "reply");

        let completed_item = classify_inbox_item(&completed, 2000);
        assert_eq!(completed_item.attention_reason, "completed");
        assert_eq!(completed_item.waiting_on, "none");
        assert_eq!(completed_item.suggested_action, "archive");

        let active_item = classify_inbox_item(&active, 2000);
        assert_eq!(active_item.attention_reason, "active");
        assert_eq!(active_item.waiting_on, "codex");
        assert_eq!(active_item.suggested_action, "inspect");
    }

    #[test]
    fn sqlite_backed_inbox_matches_cached_attention() {
        let conn = create_state_db_in_memory().expect("db");
        let approval = snapshot_fixture(
            "thr_approve",
            "/tmp/project-a",
            1000,
            "active",
            vec!["waitingOnApproval"],
            Some("in_progress"),
        );
        let completed = snapshot_fixture(
            "thr_done",
            "/tmp/project-b",
            1100,
            "notLoaded",
            vec![],
            Some("completed"),
        );
        upsert_thread_snapshot(&conn, &approval, 2000).expect("upsert approval");
        upsert_thread_snapshot(&conn, &completed, 2000).expect("upsert completed");

        let inbox = list_inbox_from_db(&conn, 3000, None, None, None, None, 10).expect("inbox");
        assert_eq!(inbox.summary.total, 2);
        assert_eq!(inbox.summary.needs_attention, 1);
        assert_eq!(inbox.items[0].attention_reason, "pending_approval");
        assert_eq!(inbox.items[0].suggested_action, "approve");
    }

    #[test]
    fn inbox_items_include_recent_actions() {
        let conn = create_state_db_in_memory().expect("db");
        let reply = snapshot_fixture(
            "thr_reply",
            "/tmp/project-a",
            1000,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        upsert_thread_snapshot(&conn, &reply, 2000).expect("upsert reply");
        record_action(
            &conn,
            "thr_reply",
            "reply",
            json!({"message": "On it"}),
            2100,
        )
        .expect("record action");

        let inbox = list_inbox_from_db(&conn, 3000, None, None, None, None, 10).expect("inbox");
        assert_eq!(
            inbox.items[0].recent_action.as_ref().unwrap()["actionType"],
            "reply"
        );
        assert_eq!(
            inbox.items[0].recent_action.as_ref().unwrap()["payload"]["message"],
            "On it"
        );
    }

    #[test]
    fn normalize_thread_snapshot_uses_summary_fields_without_turns() {
        let snapshot = normalize_thread_snapshot(
            &json!({
                "id": "thr_summary",
                "name": "Summary only",
                "cwd": "/tmp/project-a",
                "updatedAt": 1234,
                "status": { "type": "active", "activeFlags": ["waitingOnUserInput"] },
                "lastTurnStatus": "in_progress",
                "lastPreview": "Need a reply"
            }),
            &json!({
                "id": "thr_summary",
                "status": { "type": "active", "activeFlags": ["waitingOnUserInput"] }
            }),
        )
        .expect("snapshot");

        assert_eq!(snapshot.thread_id, "thr_summary");
        assert_eq!(snapshot.name.as_deref(), Some("Summary only"));
        assert_eq!(snapshot.cwd.as_deref(), Some("/tmp/project-a"));
        assert_eq!(snapshot.last_turn_status.as_deref(), Some("in_progress"));
        assert_eq!(snapshot.last_preview.as_deref(), Some("Need a reply"));
        assert_eq!(
            snapshot
                .pending_prompt
                .as_ref()
                .map(|prompt| prompt.kind.as_str()),
            Some("reply")
        );
    }

    #[test]
    fn normalize_thread_snapshot_uses_latest_turn_timestamp_when_summary_is_stale() {
        let snapshot = normalize_thread_snapshot(
            &json!({
                "id": "thr_done",
                "name": "Done",
                "updatedAt": 1000,
                "lastTurnStatus": "completed",
                "lastPreview": "Old summary"
            }),
            &json!({
                "id": "thr_done",
                "updatedAt": 1000,
                "status": { "type": "idle", "activeFlags": [] },
                "turns": [{
                    "status": "completed",
                    "completedAt": 2500,
                    "items": [{
                        "type": "agentMessage",
                        "phase": "final_answer",
                        "text": "Fresh answer"
                    }]
                }]
            }),
        )
        .expect("snapshot");

        assert_eq!(snapshot.updated_at, Some(2500));
        assert_eq!(snapshot.last_turn_status.as_deref(), Some("completed"));
        assert_eq!(snapshot.last_preview.as_deref(), Some("Fresh answer"));
    }

    #[test]
    fn completed_events_are_enriched_with_full_final_preview() {
        let sync_result = json!({
            "threads": [{
                "threadId": "thr_done",
                "lastPreview": "Summary preview"
            }],
            "events": [{
                "type": "thread_completed",
                "threadId": "thr_done",
                "lastPreview": "Summary preview"
            }]
        });

        let enriched = enrich_completed_event_previews(sync_result, |thread_id| {
            assert_eq!(thread_id, "thr_done");
            Ok(Some(
                "Full final answer\n\nWith all paragraphs.".to_string(),
            ))
        })
        .expect("enrich");

        assert_eq!(
            enriched["events"][0]["lastPreview"],
            "Full final answer\n\nWith all paragraphs."
        );
        assert_eq!(
            enriched["threads"][0]["lastPreview"],
            "Full final answer\n\nWith all paragraphs."
        );
    }

    #[test]
    fn show_thread_result_includes_derived_state_and_recent_actions() {
        let conn = create_state_db_in_memory().expect("db");
        record_action(
            &conn,
            "thr_show",
            "approve",
            json!({"decision": "approve"}),
            2200,
        )
        .expect("record action");
        let result = build_show_thread_result(
            Some(&conn),
            "thr_show",
            json!({
                "thread": {
                    "id": "thr_show",
                    "name": null,
                    "cwd": "/tmp/project-a",
                    "status": { "type": "active", "activeFlags": ["waitingOnUserInput"] },
                    "turns": [
                        {
                            "status": "completed",
                            "items": [
                                { "type": "userMessage", "text": "Can you check this?" },
                                { "type": "agentMessage", "text": "I checked it." }
                            ]
                        },
                        {
                            "status": "interrupted",
                            "items": [
                                { "type": "userMessage", "text": "Which option should I pick?" }
                            ]
                        }
                    ]
                }
            }),
        )
        .expect("show result");

        assert_eq!(result["thread"]["threadId"], "thr_show");
        assert_eq!(
            result["thread"]["displayName"],
            "Which option should I pick?"
        );
        assert_eq!(result["thread"]["project"], "project-a");
        assert_eq!(result["thread"]["pendingPrompt"]["kind"], "reply");
        assert_eq!(result["thread"]["canReply"], true);
        assert_eq!(result["thread"]["canApprove"], false);
        assert_eq!(
            result["thread"]["lastUserMessage"],
            "Which option should I pick?"
        );
        assert_eq!(
            result["thread"]["latestQuestion"],
            "Which option should I pick?"
        );
        assert!(result["thread"]["deltaSummary"]
            .as_str()
            .unwrap()
            .contains("Needs input"));
        assert_eq!(
            result["thread"]["recentActions"][0]["actionType"],
            "approve"
        );
    }

    #[test]
    fn reconcile_snapshots_returns_ts_sync_shape_and_dedupes_away_events() {
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 900).expect("away on");
        let waiting = snapshot_fixture(
            "thr_wait",
            "/tmp/project-a",
            1000,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        let completed = snapshot_fixture(
            "thr_done",
            "/tmp/project-b",
            1100,
            "notLoaded",
            vec![],
            Some("completed"),
        );

        let first =
            reconcile_thread_snapshots(&conn, 1200, vec![waiting.clone(), completed.clone()], true)
                .expect("first reconcile");
        assert_eq!(first["synced"], 2);
        assert_eq!(first["away"], true);
        assert_eq!(first["threads"][0]["threadId"], "thr_wait");
        assert!(first["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["type"] == "thread_waiting"));
        assert!(first["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["type"] == "thread_completed"));

        let second = reconcile_thread_snapshots(&conn, 1300, vec![waiting, completed], true)
            .expect("second reconcile");
        assert_eq!(second["events"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn watch_events_from_sync_result_enriches_events_and_notifications() {
        let sync_result = json!({
            "threads": [{
                "threadId": "thr_wait",
                "name": "Need reply",
                "cwd": "/tmp/project",
                "updatedAt": 1000,
                "statusType": "active",
                "statusFlags": ["waitingOnUserInput"],
                "lastTurnStatus": "in_progress",
                "lastPreview": "Question",
                "pendingPrompt": {
                    "promptId": "reply:thr_wait",
                    "promptKind": "reply",
                    "promptStatus": "Needs input",
                    "question": "Question"
                }
            }],
            "events": [{
                "type": "thread_waiting",
                "threadId": "thr_wait",
                "promptKind": "reply",
                "updatedAt": 1000
            }]
        });
        let events = watch_events_from_sync_result(
            &sync_result,
            vec![json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thr_wait",
                    "turnId": "turn_1",
                    "item": { "type": "agentMessage" }
                }
            })],
            None,
        );
        assert_eq!(events[0]["thread"]["pendingPrompt"]["promptKind"], "reply");
        assert!(events
            .iter()
            .any(|event| event["type"] == "item_completed"
                && event["thread"]["threadId"] == "thr_wait"));
    }

    #[test]
    fn hermes_hook_notifies_on_thread_completed_event() {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/print-hook-event.py");
        let mut child = Command::new("python3")
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn hermes hook");

        let payload = json!({
            "type": "thread_completed",
            "threadId": "thr_done",
            "thread": {
                "name": "Launch checklist",
                "lastPreview": "Finished the launch checklist."
            }
        });
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(serde_json::to_string(&payload).unwrap().as_bytes())
            .expect("write payload");
        drop(child.stdin.take());

        let output = child.wait_with_output().expect("hook output");
        assert!(output.status.success());
        let notification: Value =
            serde_json::from_slice(&output.stdout).expect("hook should emit JSON");

        assert_eq!(notification["notify"], true);
        assert_eq!(notification["threadId"], "thr_done");
        assert_eq!(notification["eventType"], "thread_completed");
        assert_eq!(notification["message"], "Finished the launch checklist.");
    }

    #[test]
    fn print_hook_reports_missing_stdin_cleanly() {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/print-hook-event.py");
        let output = Command::new("python3")
            .arg(script)
            .stderr(Stdio::piped())
            .output()
            .expect("run print hook without stdin");

        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("Expected one JSON event"));
    }

    #[test]
    fn new_and_fork_dry_run_shapes_are_stable() {
        let new_result = start_new_thread_dry_run(Some("/tmp/project"), Some("hello"));
        assert_eq!(new_result["action"], "new");
        assert_eq!(new_result["dryRun"], true);
        assert_eq!(new_result["cwd"], "/tmp/project");

        let fork_result = fork_thread_dry_run("thr_source", Some("follow up"));
        assert_eq!(fork_result["action"], "fork");
        assert_eq!(fork_result["dryRun"], true);
        assert_eq!(fork_result["fromThreadId"], "thr_source");
    }

    #[test]
    fn archive_requires_yes_for_filter_selection_and_supports_dry_run() {
        let conn = create_state_db_in_memory().expect("db");
        let completed = snapshot_fixture(
            "thr_done",
            "/tmp/project-b",
            1100,
            "notLoaded",
            vec![],
            Some("completed"),
        );
        upsert_thread_snapshot(&conn, &completed, 2000).expect("upsert completed");

        let err = archive_from_db(&conn, None, Some("completed"), false, false, 3000)
            .expect_err("should require yes");
        assert!(err.to_string().contains("Refusing bulk archive"));

        let dry_run = archive_from_db(&conn, None, Some("completed"), true, false, 3000)
            .expect("dry run archive");
        assert_eq!(dry_run["dryRun"], true);
        assert_eq!(dry_run["results"][0]["status"], "would_archive");
    }

    #[test]
    fn watch_once_emits_cached_waiting_event_shape() {
        let conn = create_state_db_in_memory().expect("db");
        let waiting = snapshot_fixture(
            "thr_wait",
            "/tmp/project-wait",
            1500,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        upsert_thread_snapshot(&conn, &waiting, 2000).expect("upsert waiting");

        let events = watch_once_from_db(&conn).expect("watch once");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "thread_waiting");
        assert_eq!(events[0]["threadId"], "thr_wait");
        assert_eq!(events[0]["thread"]["pendingPrompt"]["promptKind"], "reply");
    }

    #[test]
    fn away_mode_round_trip_matches_ts_shape() {
        let conn = create_state_db_in_memory().expect("db");

        let initial = get_away_mode(&conn).expect("initial away");
        assert_eq!(initial["away"], false);
        assert_eq!(initial["awayStartedAt"], Value::Null);

        let enabled = set_away_mode(&conn, true, 1234).expect("enable away");
        assert_eq!(enabled["away"], true);
        assert_eq!(enabled["awayStartedAt"], 1234);
        assert_eq!(enabled["awaySessionId"], "1234");

        let status = get_away_mode(&conn).expect("status away");
        assert_eq!(status["away"], true);
        assert_eq!(status["awayStartedAt"], 1234);
        assert_eq!(status["awaySessionId"], "1234");

        let disabled = set_away_mode(&conn, false, 1500).expect("disable away");
        assert_eq!(disabled["away"], false);
        assert_eq!(disabled["awayStartedAt"], Value::Null);
        assert_eq!(disabled["awaySessionId"], Value::Null);
    }

    #[test]
    fn away_mode_writes_remote_mode_status_marker() {
        let _guard = test_env_lock().lock().expect("env lock");
        let state_dir = TempStateDir::new("remote-mode-marker");
        let conn = create_state_db_in_memory().expect("db");

        set_away_mode(&conn, true, 1234).expect("enable away");
        let marker_path = state_dir.root.join("remote-mode.json");
        let enabled_marker: Value =
            serde_json::from_slice(&std::fs::read(&marker_path).expect("read enabled marker"))
                .expect("parse enabled marker");
        assert_eq!(enabled_marker["away"], true);
        assert_eq!(enabled_marker["awayStartedAt"], 1234);
        assert_eq!(enabled_marker["awaySessionId"], "1234");

        set_away_mode(&conn, false, 1500).expect("disable away");
        let disabled_marker: Value =
            serde_json::from_slice(&std::fs::read(&marker_path).expect("read disabled marker"))
                .expect("parse disabled marker");
        assert_eq!(disabled_marker["away"], false);
        assert_eq!(disabled_marker["awayStartedAt"], Value::Null);
        assert_eq!(disabled_marker["awaySessionId"], Value::Null);
    }

    #[test]
    fn unarchive_dry_run_and_live_record_action() {
        let conn = create_state_db_in_memory().expect("db");

        let dry_run =
            unarchive_thread_result(&conn, "thr_archived", true, 1000, None).expect("dry run");
        assert_eq!(dry_run["action"], "unarchive");
        assert_eq!(dry_run["results"][0]["status"], "would_unarchive");

        let live = unarchive_thread_result(
            &conn,
            "thr_archived",
            false,
            1100,
            Some(json!({"threadId": "thr_archived", "ok": true})),
        )
        .expect("live");
        assert_eq!(live["results"][0]["status"], "unarchived");

        let history = get_thread_history(&conn, "thr_archived", 10).expect("history");
        assert_eq!(history[0].action_type, "unarchive");
    }

    #[test]
    fn follow_result_emits_started_and_snapshot_events() {
        let thread = json!({"id": "thr_follow", "name": "Follow me"});
        let events = build_follow_events(FollowEventsFixture {
            thread_id: "thr_follow",
            duration_ms: 1000,
            poll_interval_ms: 500,
            initial_thread: Some(thread.clone()),
            started: None,
            notifications: vec![],
            event_filter: None,
        })
        .expect("follow events");
        assert_eq!(events[0]["type"], "follow_started");
        assert_eq!(events[1]["type"], "follow_snapshot");
        assert_eq!(events[1]["thread"], thread);
    }

    #[test]
    fn follow_summaries_match_direct_and_composed_ts_shapes() {
        let events = vec![json!({"type": "follow_started", "threadId": "thr_follow"})];

        let direct = follow_result_summary("thr_follow", 3000, &events, false);
        assert_eq!(direct["ok"], true);
        assert_eq!(direct["action"], "follow");
        assert_eq!(direct["threadId"], "thr_follow");
        assert!(direct.get("events").is_none());

        let composed = follow_result_summary("thr_follow", 3000, &events, true);
        assert_eq!(composed["events"].as_array().unwrap().len(), 1);

        let command = attach_follow_payload(
            json!({"ok": true, "action": "reply", "threadId": "thr_follow"}),
            direct,
            true,
        );
        assert_eq!(command["type"], "command_result");
        assert_eq!(command["result"]["follow"]["action"], "follow");
    }

    #[test]
    fn composed_follow_settle_duration_matches_ts_default() {
        assert_eq!(composed_settle_duration_ms(100), 0);
        assert_eq!(composed_settle_duration_ms(101), 20_000);
        assert_eq!(composed_settle_duration_ms(3_000), 20_000);
    }

    #[test]
    fn new_thread_request_and_result_shapes_match_ts() {
        assert_eq!(thread_start_params(None), json!({}));
        assert_eq!(
            thread_start_params(Some("/tmp/project")),
            json!({"cwd": "/tmp/project"})
        );
        assert_eq!(
            turn_start_params("thr_new", Some("/tmp/project"), "hello"),
            json!({
                "threadId": "thr_new",
                "cwd": "/tmp/project",
                "input": [{"type": "text", "text": "hello", "text_elements": []}]
            })
        );

        let result = new_thread_live_result(
            Some("/tmp/input"),
            Some(" hello "),
            json!({"thread": {"id": "thr_new", "cwd": "/tmp/from-codex"}}),
            Some(json!({"turn": {"id": "turn_1"}})),
        );
        assert_eq!(result["threadId"], "thr_new");
        assert_eq!(result["cwd"], "/tmp/from-codex");
        assert_eq!(result["message"], "hello");
        assert_eq!(result["started"]["turn"]["id"], "turn_1");
    }

    #[test]
    fn archive_targets_match_ts_explicit_and_filtered_selection() {
        let conn = create_state_db_in_memory().expect("db");
        let project_a = snapshot_fixture(
            "thr_a",
            "/tmp/project-a",
            1000,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        let project_b = snapshot_fixture(
            "thr_b",
            "/tmp/project-b",
            900,
            "idle",
            vec![],
            Some("completed"),
        );
        upsert_thread_snapshot(&conn, &project_a, 1000).expect("upsert a");
        upsert_thread_snapshot(&conn, &project_b, 1000).expect("upsert b");

        let explicit = resolve_archive_targets(
            &conn,
            &["thr_a".into(), "thr_b".into()],
            None,
            None,
            None,
            100,
            1500,
        )
        .expect("explicit targets");
        assert_eq!(explicit.targets, vec!["thr_a", "thr_b"]);
        assert!(!explicit.using_filter_selection);

        let filtered = resolve_archive_targets(
            &conn,
            &[],
            Some("project-a"),
            Some("active"),
            None,
            10,
            1500,
        )
        .expect("filtered targets");
        assert_eq!(filtered.targets, vec!["thr_a"]);
        assert!(filtered.using_filter_selection);

        let implicit_bulk = resolve_archive_targets(&conn, &[], None, None, None, 10, 1500)
            .expect("implicit bulk targets");
        assert!(implicit_bulk.using_filter_selection);
        let bulk_without_yes =
            archive_from_db(&conn, None, None, false, false, 1500).expect_err("bulk refusal");
        assert!(format!("{bulk_without_yes:#}").contains("Refusing bulk archive"));
    }

    #[test]
    fn watch_filter_keeps_only_requested_event_types() {
        let filtered = filter_watch_events(
            vec![
                json!({"type": "thread_waiting", "threadId": "thr_1"}),
                json!({"type": "item_completed", "threadId": "thr_1"}),
            ],
            Some(&std::collections::BTreeSet::from([
                "item_completed".to_string()
            ])),
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["type"], "item_completed");
    }

    #[test]
    fn watch_thread_error_event_matches_ts_stream_shape() {
        let event = watch_thread_error_event(&anyhow::anyhow!("sync failed"));
        assert_eq!(event["type"], "thread_error");
        assert_eq!(event["message"], "sync failed");

        let filtered = filter_watch_events(
            vec![event],
            Some(&BTreeSet::from(["thread_waiting".to_string()])),
        );
        assert!(filtered.is_empty());
    }

    #[test]
    fn watch_exec_hook_writes_event_to_stdout_consumer() {
        let temp = std::env::temp_dir().join(format!("codex-watch-hook-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp);
        std::fs::create_dir_all(&temp).expect("mkdir temp");
        let hook_output = temp.join("hook.json");
        let command = format!(
            "python3 -c \"import sys,pathlib; pathlib.Path(r'{}').write_text(sys.stdin.read())\"",
            hook_output.display()
        );
        run_exec_hook(
            &command,
            &json!({"type": "thread_waiting", "threadId": "thr_1"}),
        )
        .expect("exec hook");
        std::thread::sleep(std::time::Duration::from_millis(200));
        let payload = std::fs::read_to_string(hook_output).expect("hook output");
        assert!(payload.contains("thread_waiting"));
    }

    #[test]
    fn watch_exec_hook_fails_on_nonzero_exit() {
        let err = run_exec_hook(
            "python3 -c \"import sys; sys.exit(7)\"",
            &json!({"type": "thread_waiting", "threadId": "thr_1"}),
        )
        .expect_err("nonzero hook exit should fail");
        assert!(format!("{err:#}").contains("exited with status"));
    }

    fn snapshot_fixture(
        thread_id: &str,
        cwd: &str,
        updated_at: u64,
        status_type: &str,
        status_flags: Vec<&str>,
        last_turn_status: Option<&str>,
    ) -> BridgeThreadSnapshot {
        let status_flags_vec = status_flags
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        BridgeThreadSnapshot {
            thread_id: thread_id.to_string(),
            name: None,
            cwd: Some(cwd.to_string()),
            updated_at: Some(updated_at),
            status_type: status_type.to_string(),
            status_flags: status_flags_vec.clone(),
            last_turn_status: last_turn_status.map(|s| s.to_string()),
            last_preview: Some(format!("preview for {thread_id}")),
            pending_prompt: derive_pending_prompt(
                thread_id,
                &status_flags_vec,
                Some(format!("preview for {thread_id}")),
            ),
        }
    }
}

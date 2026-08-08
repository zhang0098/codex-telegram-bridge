#[cfg(test)]
use anyhow::bail;
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::projects::{canonicalize_project_cwd, derive_project_label};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PendingPrompt {
    pub(crate) prompt_id: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) question: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TelegramInboundLogContext<'a> {
    pub(crate) thread_id: Option<&'a str>,
    pub(crate) route_message_id: Option<i64>,
    pub(crate) result_action: Option<&'a str>,
    pub(crate) codex_transport: Option<&'a str>,
    pub(crate) codex_app_server_pid: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BridgeThreadSnapshot {
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) updated_at: Option<u64>,
    pub(crate) status_type: String,
    pub(crate) status_flags: Vec<String>,
    pub(crate) last_turn_status: Option<String>,
    pub(crate) last_preview: Option<String>,
    pub(crate) pending_prompt: Option<PendingPrompt>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingThread {
    #[serde(rename = "threadId")]
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    #[serde(rename = "displayName")]
    pub(crate) display_name: String,
    pub(crate) project: Option<String>,
    pub(crate) cwd: Option<String>,
    #[serde(rename = "updatedAt")]
    pub(crate) updated_at: Option<u64>,
    #[serde(rename = "statusType")]
    pub(crate) status_type: String,
    #[serde(rename = "statusFlags")]
    pub(crate) status_flags: Vec<String>,
    pub(crate) prompt: PendingPrompt,
    #[serde(rename = "lastPreview")]
    pub(crate) last_preview: Option<String>,
    pub(crate) label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingSummary {
    pub(crate) count: usize,
    #[serde(rename = "threadIds")]
    pub(crate) thread_ids: Vec<String>,
    pub(crate) labels: Vec<String>,
    #[serde(rename = "appliedFilters")]
    pub(crate) applied_filters: Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingResult {
    pub(crate) summary: WaitingSummary,
    pub(crate) threads: Vec<WaitingThread>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxItem {
    #[serde(rename = "threadId")]
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    #[serde(rename = "displayName")]
    pub(crate) display_name: String,
    pub(crate) project: Option<String>,
    pub(crate) cwd: Option<String>,
    #[serde(rename = "updatedAt")]
    pub(crate) updated_at: Option<u64>,
    #[serde(rename = "lastSeenAt")]
    pub(crate) last_seen_at: Option<u64>,
    #[serde(rename = "ageSeconds")]
    pub(crate) age_seconds: Option<u64>,
    #[serde(rename = "statusType")]
    pub(crate) status_type: String,
    #[serde(rename = "statusFlags")]
    pub(crate) status_flags: Vec<String>,
    #[serde(rename = "lastPreview")]
    pub(crate) last_preview: Option<String>,
    #[serde(rename = "promptKind")]
    pub(crate) prompt_kind: Option<String>,
    #[serde(rename = "promptStatus")]
    pub(crate) prompt_status: Option<String>,
    pub(crate) question: Option<String>,
    pub(crate) basis: String,
    #[serde(rename = "attentionReason")]
    pub(crate) attention_reason: String,
    #[serde(rename = "waitingOn")]
    pub(crate) waiting_on: String,
    #[serde(rename = "suggestedAction")]
    pub(crate) suggested_action: String,
    pub(crate) priority: String,
    #[serde(rename = "recentAction")]
    pub(crate) recent_action: Option<Value>,
    pub(crate) label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxSummary {
    pub(crate) total: usize,
    #[serde(rename = "needsAttention")]
    pub(crate) needs_attention: usize,
    #[serde(rename = "countsByReason")]
    pub(crate) counts_by_reason: Value,
    #[serde(rename = "appliedFilters")]
    pub(crate) applied_filters: Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxResult {
    pub(crate) summary: InboxSummary,
    pub(crate) items: Vec<InboxItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedWorkspace {
    pub(crate) cwd: String,
    pub(crate) label: String,
    pub(crate) last_seen_at: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TelegramCommandRouteKind {
    NewThread,
}

impl TelegramCommandRouteKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NewThread => "new_thread",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "new_thread" => Some(Self::NewThread),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelegramCallbackRoute {
    pub(crate) callback_id: String,
    pub(crate) chat_id: String,
    pub(crate) message_id: Option<i64>,
    pub(crate) thread_id: String,
    pub(crate) action: TelegramCallbackAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TelegramCallbackAction {
    Approve,
    Deny,
}

impl TelegramCallbackAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "approve" => Some(Self::Approve),
            "deny" => Some(Self::Deny),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OutboxDeliverySummary {
    pub(crate) attempted: usize,
    pub(crate) delivered: usize,
    pub(crate) failed: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct HistoryAction {
    pub(crate) action_type: String,
    pub(crate) payload: Value,
    pub(crate) created_at: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ArchiveSelection {
    pub(crate) targets: Vec<String>,
    pub(crate) using_filter_selection: bool,
}

fn timestamp_to_millis(value: u64) -> u64 {
    const UNIX_TIMESTAMP_MILLIS_THRESHOLD: u64 = 100_000_000_000;
    if value < UNIX_TIMESTAMP_MILLIS_THRESHOLD {
        value.saturating_mul(1000)
    } else {
        value
    }
}

fn timestamp_age_seconds(now: u64, then: u64) -> u64 {
    timestamp_to_millis(now).saturating_sub(timestamp_to_millis(then)) / 1000
}

fn compact_text_preview(input: Option<String>, limit: usize) -> Option<String> {
    let text = input?.trim().replace(char::is_whitespace, " ");
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    let count = normalized.chars().count();
    if count <= limit {
        return Some(normalized);
    }
    let truncated = normalized.chars().take(limit).collect::<String>();
    Some(format!("{}…", truncated.trim_end()))
}

pub(crate) fn derive_thread_display_name(
    name: Option<&str>,
    project: Option<&str>,
    question: Option<&str>,
    thread_id: &str,
) -> String {
    if let Some(value) = name.map(str::trim).filter(|value| !value.is_empty()) {
        return value.to_string();
    }
    if let Some(value) = question.map(str::trim).filter(|value| !value.is_empty()) {
        return compact_text_preview(Some(value.to_string()), 80)
            .unwrap_or_else(|| value.to_string());
    }
    if let Some(project) = project.filter(|value| !value.is_empty()) {
        return format!("Untitled {project} thread");
    }
    let short_id = thread_id.chars().take(8).collect::<String>();
    format!("Untitled thread {short_id}")
}

fn classify_attention(snapshot: &BridgeThreadSnapshot) -> (&'static str, &'static str) {
    match snapshot
        .pending_prompt
        .as_ref()
        .map(|prompt| prompt.kind.as_str())
    {
        Some("approval") => ("pending_approval", "prompt_kind"),
        Some("reply") => ("needs_reply", "prompt_kind"),
        _ if snapshot.last_turn_status.as_deref() == Some("completed") => {
            ("completed", "last_turn_completed")
        }
        _ if snapshot.last_turn_status.as_deref() == Some("interrupted")
            && snapshot
                .last_preview
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false) =>
        {
            ("updated", "last_turn_interrupted")
        }
        _ => ("active", "fallback_active"),
    }
}

fn classify_waiting_on(reason: &str) -> &'static str {
    match reason {
        "pending_approval" | "needs_reply" => "me",
        "active" => "codex",
        "updated" => "none",
        _ => "none",
    }
}

fn classify_suggested_action(reason: &str) -> &'static str {
    match reason {
        "pending_approval" => "approve",
        "needs_reply" => "reply",
        "completed" => "archive",
        "updated" => "inspect",
        _ => "inspect",
    }
}

fn classify_priority(reason: &str) -> &'static str {
    match reason {
        "pending_approval" => "high",
        "needs_reply" | "active" | "updated" => "medium",
        _ => "low",
    }
}

pub(crate) fn score_inbox_item(item: &InboxItem) -> u128 {
    let priority_score = match item.priority.as_str() {
        "high" => 300_u128,
        "medium" => 200_u128,
        _ => 100_u128,
    };
    priority_score * 1_000_000_000_000 + item.updated_at.unwrap_or(0) as u128
}

pub(crate) fn classify_inbox_item(snapshot: &BridgeThreadSnapshot, now: u64) -> InboxItem {
    let (attention_reason, basis) = classify_attention(snapshot);
    let project = derive_project_label(snapshot.cwd.as_deref());
    let display_name = derive_thread_display_name(
        snapshot.name.as_deref(),
        project.as_deref(),
        snapshot
            .pending_prompt
            .as_ref()
            .and_then(|prompt| prompt.question.as_deref()),
        &snapshot.thread_id,
    );
    InboxItem {
        thread_id: snapshot.thread_id.clone(),
        name: snapshot.name.clone(),
        display_name: display_name.clone(),
        project: project.clone(),
        cwd: snapshot.cwd.clone(),
        updated_at: snapshot.updated_at,
        last_seen_at: None,
        age_seconds: snapshot
            .updated_at
            .map(|updated| timestamp_age_seconds(now, updated)),
        status_type: snapshot.status_type.clone(),
        status_flags: snapshot.status_flags.clone(),
        last_preview: compact_text_preview(snapshot.last_preview.clone(), 220),
        prompt_kind: snapshot
            .pending_prompt
            .as_ref()
            .map(|prompt| prompt.kind.clone()),
        prompt_status: snapshot
            .pending_prompt
            .as_ref()
            .map(|prompt| prompt.status.clone()),
        question: compact_text_preview(
            snapshot
                .pending_prompt
                .as_ref()
                .and_then(|prompt| prompt.question.clone()),
            160,
        ),
        basis: basis.to_string(),
        attention_reason: attention_reason.to_string(),
        waiting_on: classify_waiting_on(attention_reason).to_string(),
        suggested_action: classify_suggested_action(attention_reason).to_string(),
        priority: classify_priority(attention_reason).to_string(),
        recent_action: None,
        label: format!(
            "{} · {}",
            display_name,
            project
                .clone()
                .or_else(|| snapshot.cwd.clone())
                .unwrap_or_else(|| "unknown cwd".to_string())
        ),
    }
}

pub(crate) fn observed_workspaces_from_db(
    conn: &Connection,
    limit: u64,
) -> Result<Vec<ObservedWorkspace>> {
    let mut stmt = conn.prepare(
        "SELECT cwd, MAX(COALESCE(updated_at, last_seen_at, 0)) AS seen_at
         FROM threads_cache
         WHERE cwd IS NOT NULL AND TRIM(cwd) != ''
         GROUP BY cwd
         ORDER BY seen_at DESC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![to_sql_i64(limit)?], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(cwd, seen_at)| {
            let cwd = canonicalize_project_cwd(&cwd)?;
            Ok(ObservedWorkspace {
                label: derive_project_label(Some(&cwd)).unwrap_or_else(|| cwd.clone()),
                cwd,
                last_seen_at: optional_from_sql_i64(seen_at)?,
            })
        })
        .collect()
}

fn to_sql_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("timestamp out of range for sqlite i64")
}

fn from_sql_i64(value: i64) -> Result<u64> {
    u64::try_from(value).context("negative sqlite integer cannot be converted to u64")
}

fn optional_from_sql_i64(value: Option<i64>) -> Result<Option<u64>> {
    value.map(from_sql_i64).transpose()
}

pub(crate) fn create_state_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))
        .context("failed to set SQLite busy timeout")?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;",
    )
    .context("failed to configure SQLite journal mode")?;
    init_state_db(&conn)?;
    Ok(conn)
}

pub(crate) fn prune_state_logs(conn: &Connection, now: u64) -> Result<usize> {
    let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
    let cutoff = now.saturating_sub(retention_ms);
    let sql_cutoff = to_sql_i64(cutoff)?;
    let inbound = conn.execute(
        "DELETE FROM telegram_inbound_log WHERE processed_at < ?1",
        params![sql_cutoff],
    )?;
    let actions = conn.execute(
        "DELETE FROM actions_log WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    Ok(inbound + actions)
}

#[cfg(test)]
pub(crate) fn create_state_db_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    init_state_db(&conn)?;
    Ok(conn)
}

pub(crate) fn init_state_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS threads_cache (
          thread_id TEXT PRIMARY KEY,
          name TEXT,
          cwd TEXT,
          source TEXT,
          status_type TEXT NOT NULL,
          status_flags_json TEXT NOT NULL,
          updated_at INTEGER,
          last_seen_at INTEGER NOT NULL,
          last_turn_status TEXT,
          last_preview TEXT
        );
        CREATE TABLE IF NOT EXISTS pending_prompts (
          thread_id TEXT PRIMARY KEY,
          prompt_id TEXT NOT NULL,
          prompt_kind TEXT NOT NULL,
          prompt_status TEXT NOT NULL,
          question TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS delivery_log (
          event_key TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          delivered_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS thread_events (
          event_key TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbound_events (
          event_id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          thread_id TEXT,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at INTEGER NOT NULL,
          last_error TEXT,
          created_at INTEGER NOT NULL,
          delivered_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS telegram_message_routes (
          chat_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          thread_id TEXT NOT NULL,
          event_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS telegram_callback_routes (
          callback_id TEXT PRIMARY KEY,
          chat_id TEXT NOT NULL,
          message_id INTEGER,
          thread_id TEXT NOT NULL,
          action TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          used_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS telegram_command_routes (
          chat_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          command TEXT NOT NULL,
          payload_json TEXT,
          created_at INTEGER NOT NULL,
          used_at INTEGER,
          PRIMARY KEY(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS transport_delivery_log (
          event_id TEXT NOT NULL,
          transport TEXT NOT NULL,
          result_json TEXT NOT NULL,
          delivered_at INTEGER NOT NULL,
          PRIMARY KEY(event_id, transport)
        );
        CREATE TABLE IF NOT EXISTS telegram_inbound_log (
          bot_id TEXT NOT NULL,
          update_id INTEGER NOT NULL,
          update_kind TEXT NOT NULL,
          result_json TEXT NOT NULL,
          processed_at INTEGER NOT NULL,
          PRIMARY KEY(bot_id, update_id)
        );
        CREATE TABLE IF NOT EXISTS actions_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          thread_id TEXT NOT NULL,
          action_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        ",
    )?;
    ensure_column(
        conn,
        "threads_cache",
        "last_seen_at",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(conn, "threads_cache", "last_turn_status", "TEXT")?;
    ensure_column(conn, "threads_cache", "last_preview", "TEXT")?;
    ensure_column(conn, "telegram_command_routes", "payload_json", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "thread_id", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "route_message_id", "INTEGER")?;
    ensure_column(conn, "telegram_inbound_log", "result_action", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "codex_transport", "TEXT")?;
    ensure_column(
        conn,
        "telegram_inbound_log",
        "codex_app_server_pid",
        "INTEGER",
    )?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns)
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let columns = table_columns(conn, table)?;
    if !columns.iter().any(|current| current == column) {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

pub(crate) fn state_dir_path() -> Result<PathBuf> {
    if let Ok(dir) = env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR") {
        let dir = PathBuf::from(dir);
        fs::create_dir_all(&dir)?;
        return Ok(dir);
    }

    let home = env::var("HOME").context("HOME is not set")?;
    let dir = PathBuf::from(home).join(".codex-telegram-bridge");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub(crate) fn state_db_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("state.db"))
}

pub(crate) fn remote_mode_status_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("remote-mode.json"))
}

#[allow(dead_code)]
pub(crate) fn live_backend_status_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("live-backend.json"))
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn get_setting_number(conn: &Connection, key: &str) -> Result<Option<u64>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM settings WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(raw.and_then(|value| value.parse::<u64>().ok()))
}

pub(crate) fn set_setting(conn: &Connection, key: &str, value: u64) -> Result<()> {
    conn.execute(
        "INSERT INTO settings(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value.to_string()],
    )?;
    Ok(())
}

pub(crate) fn set_setting_text(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO settings(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

pub(crate) fn get_setting_text(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM settings WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn list_settings_with_prefix(
    conn: &Connection,
    prefix: &str,
) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT key, value FROM settings
         WHERE key LIKE ?1
         ORDER BY key",
    )?;
    let pattern = format!("{prefix}%");
    let rows = stmt.query_map(params![pattern], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(crate) fn delete_setting(conn: &Connection, key: &str) -> Result<()> {
    conn.execute("DELETE FROM settings WHERE key = ?1", params![key])?;
    Ok(())
}

pub(crate) fn upsert_thread_snapshot(
    conn: &Connection,
    snapshot: &BridgeThreadSnapshot,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO threads_cache(
            thread_id, name, cwd, source, status_type, status_flags_json,
            updated_at, last_seen_at, last_turn_status, last_preview
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(thread_id) DO UPDATE SET
            name = excluded.name,
            cwd = excluded.cwd,
            source = excluded.source,
            status_type = excluded.status_type,
            status_flags_json = excluded.status_flags_json,
            updated_at = CASE
                WHEN threads_cache.updated_at IS NULL THEN excluded.updated_at
                WHEN excluded.updated_at IS NULL THEN threads_cache.updated_at
                WHEN excluded.updated_at > threads_cache.updated_at THEN excluded.updated_at
                ELSE threads_cache.updated_at
            END,
            last_seen_at = excluded.last_seen_at,
            last_turn_status = excluded.last_turn_status,
            last_preview = excluded.last_preview",
        params![
            snapshot.thread_id,
            snapshot.name,
            snapshot.cwd,
            "codex-app-server",
            snapshot.status_type,
            serde_json::to_string(&snapshot.status_flags)?,
            snapshot.updated_at.map(to_sql_i64).transpose()?,
            to_sql_i64(now)?,
            snapshot.last_turn_status,
            snapshot.last_preview,
        ],
    )?;

    match &snapshot.pending_prompt {
        Some(prompt) => {
            conn.execute(
                "INSERT INTO pending_prompts(thread_id, prompt_id, prompt_kind, prompt_status, question, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(thread_id) DO UPDATE SET
                    prompt_id = excluded.prompt_id,
                    prompt_kind = excluded.prompt_kind,
                    prompt_status = excluded.prompt_status,
                    question = excluded.question,
                    created_at = excluded.created_at",
                params![
                    snapshot.thread_id,
                    prompt.prompt_id,
                    prompt.kind,
                    prompt.status,
                    prompt.question.clone().unwrap_or_default(),
                    to_sql_i64(now)?,
                ],
            )?;
        }
        None => {
            conn.execute(
                "DELETE FROM pending_prompts WHERE thread_id = ?1",
                params![snapshot.thread_id],
            )?;
        }
    }
    Ok(())
}

pub(crate) fn record_action(
    conn: &Connection,
    thread_id: &str,
    action_type: &str,
    payload: Value,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            thread_id,
            action_type,
            serde_json::to_string(&payload)?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

pub(crate) fn get_thread_history(
    conn: &Connection,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<HistoryAction>> {
    let mut stmt = conn.prepare(
        "SELECT action_type, payload_json, created_at
         FROM actions_log
         WHERE thread_id = ?1
         ORDER BY id DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![thread_id, to_sql_i64(limit)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    raw.into_iter()
        .map(|(action_type, payload_json, created_at)| {
            Ok(HistoryAction {
                action_type,
                payload: serde_json::from_str::<Value>(&payload_json).unwrap_or(Value::Null),
                created_at: from_sql_i64(created_at)?,
            })
        })
        .collect()
}

pub(crate) fn recent_actions_json(
    conn: &Connection,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<Value>> {
    Ok(get_thread_history(conn, thread_id, limit)?
        .into_iter()
        .map(|action| {
            json!({
                "actionType": action.action_type,
                "createdAt": action.created_at,
                "payload": action.payload
            })
        })
        .collect())
}

fn thread_snapshot_json(snapshot: &BridgeThreadSnapshot) -> Value {
    json!({
        "threadId": snapshot.thread_id,
        "name": snapshot.name,
        "cwd": snapshot.cwd,
        "updatedAt": snapshot.updated_at,
        "statusType": snapshot.status_type,
        "statusFlags": snapshot.status_flags,
        "lastTurnStatus": snapshot.last_turn_status,
        "lastPreview": snapshot.last_preview,
        "pendingPrompt": snapshot.pending_prompt.as_ref().map(|prompt| json!({
            "promptId": prompt.prompt_id,
            "promptKind": prompt.kind,
            "promptStatus": prompt.status,
            "question": prompt.question
        }))
    })
}

pub(crate) fn should_emit_for_away_window(
    away_started_at: Option<u64>,
    updated_at: Option<u64>,
) -> bool {
    match away_started_at {
        None => true,
        Some(started_at) => updated_at
            .map(|value| timestamp_to_millis(value) >= timestamp_to_millis(started_at))
            .unwrap_or(false),
    }
}

fn record_delivery(
    conn: &Connection,
    event_key: &str,
    thread_id: &str,
    event_type: &str,
    delivered_at: u64,
) -> Result<bool> {
    let changed = conn.execute(
        "INSERT OR IGNORE INTO delivery_log(event_key, thread_id, event_type, delivered_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![event_key, thread_id, event_type, to_sql_i64(delivered_at)?],
    )?;
    Ok(changed > 0)
}

fn record_thread_event(
    conn: &Connection,
    event_key: &str,
    thread_id: &str,
    event_type: &str,
    observed_at: u64,
    payload: &Value,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO thread_events(event_key, thread_id, event_type, observed_at, payload_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event_key,
            thread_id,
            event_type,
            to_sql_i64(observed_at)?,
            serde_json::to_string(payload)?
        ],
    )?;
    Ok(())
}

pub(crate) fn reconcile_thread_snapshots(
    conn: &Connection,
    now: u64,
    snapshots: Vec<BridgeThreadSnapshot>,
    record_deliveries: bool,
) -> Result<Value> {
    let away = get_setting_text(conn, "away")?.unwrap_or_default() == "true";
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    let mut events = Vec::new();
    let mut threads = Vec::new();

    for snapshot in &snapshots {
        upsert_thread_snapshot(conn, snapshot, now)?;
        threads.push(thread_snapshot_json(snapshot));

        if !away || !should_emit_for_away_window(away_started_at, snapshot.updated_at) {
            continue;
        }

        if let Some(prompt) = &snapshot.pending_prompt {
            let event_key = format!(
                "thread_waiting:{}:{}:{}",
                snapshot.thread_id,
                prompt.prompt_id,
                snapshot
                    .updated_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            );
            let should_emit = !record_deliveries
                || record_delivery(conn, &event_key, &snapshot.thread_id, "thread_waiting", now)?;
            if should_emit {
                let event = json!({
                    "type": "thread_waiting",
                    "threadId": snapshot.thread_id,
                    "promptKind": prompt.kind,
                    "updatedAt": snapshot.updated_at,
                    "lastPreview": snapshot.last_preview,
                });
                record_thread_event(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_waiting",
                    now,
                    &event,
                )?;
                events.push(event);
            }
        }

        if snapshot.pending_prompt.is_none()
            && snapshot.last_turn_status.as_deref() == Some("completed")
        {
            let event_key = format!(
                "thread_completed:{}:{}",
                snapshot.thread_id,
                snapshot
                    .updated_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            );
            let should_emit = !record_deliveries
                || record_delivery(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_completed",
                    now,
                )?;
            if should_emit {
                let event = json!({
                    "type": "thread_completed",
                    "threadId": snapshot.thread_id,
                    "updatedAt": snapshot.updated_at,
                    "lastPreview": snapshot.last_preview,
                });
                record_thread_event(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_completed",
                    now,
                    &event,
                )?;
                events.push(event);
            }
        }
    }

    Ok(json!({
        "synced": snapshots.len(),
        "threads": threads,
        "events": events,
        "away": away
    }))
}

pub(crate) fn list_waiting_from_db(
    conn: &Connection,
    project_filter: Option<&str>,
    limit: u64,
) -> Result<WaitingResult> {
    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.status_type, t.status_flags_json,
                t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status, p.question
         FROM pending_prompts p
         INNER JOIN threads_cache t ON t.thread_id = p.thread_id
         ORDER BY COALESCE(t.updated_at, 0) DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, String>(9)?,
            row.get::<_, String>(10)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut threads = raw
        .into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                status_type,
                status_flags_json,
                last_preview,
                prompt_id,
                prompt_kind,
                prompt_status,
                question,
            )| {
                let prompt = PendingPrompt {
                    prompt_id,
                    kind: prompt_kind.clone(),
                    status: prompt_status,
                    question: Some(question.clone()),
                };
                let project = derive_project_label(cwd.as_deref());
                let display_name = derive_thread_display_name(
                    name.as_deref(),
                    project.as_deref(),
                    Some(question.as_str()),
                    &thread_id,
                );
                let label = format!(
                    "{} · {} · {}",
                    display_name,
                    project
                        .clone()
                        .or(cwd.clone())
                        .unwrap_or_else(|| "unknown cwd".to_string()),
                    prompt_kind
                );
                Ok(WaitingThread {
                    thread_id,
                    name,
                    display_name,
                    project,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags: serde_json::from_str(&status_flags_json).unwrap_or_default(),
                    prompt,
                    last_preview,
                    label,
                })
            },
        )
        .collect::<Result<Vec<_>>>()?;
    if let Some(needle) = project_filter {
        let needle = needle.to_lowercase();
        threads.retain(|thread| {
            thread
                .cwd
                .as_deref()
                .unwrap_or_default()
                .to_lowercase()
                .contains(&needle)
        });
    }
    if limit > 0 {
        threads.truncate(limit as usize);
    }
    Ok(WaitingResult {
        summary: WaitingSummary {
            count: threads.len(),
            thread_ids: threads
                .iter()
                .map(|thread| thread.thread_id.clone())
                .collect(),
            labels: threads.iter().map(|thread| thread.label.clone()).collect(),
            applied_filters: json!({ "project": project_filter, "limit": limit }),
        },
        threads,
    })
}

pub(crate) fn list_recent_thread_snapshots_from_db(
    conn: &Connection,
    limit: u64,
) -> Result<Vec<BridgeThreadSnapshot>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.status_type, t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status,
                p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY COALESCE(t.updated_at, t.last_seen_at, 0) DESC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![to_sql_i64(limit)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    raw.into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                status_type,
                status_flags_json,
                last_turn_status,
                last_preview,
                prompt_id,
                prompt_kind,
                prompt_status,
                question,
            )| {
                Ok(BridgeThreadSnapshot {
                    thread_id,
                    name,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags: serde_json::from_str(&status_flags_json).unwrap_or_default(),
                    last_turn_status,
                    last_preview,
                    pending_prompt: prompt_id.map(|prompt_id| PendingPrompt {
                        prompt_id,
                        kind: prompt_kind.unwrap_or_else(|| "reply".to_string()),
                        status: prompt_status.unwrap_or_else(|| "Needs input".to_string()),
                        question,
                    }),
                })
            },
        )
        .collect()
}

pub(crate) fn list_inbox_from_db(
    conn: &Connection,
    now: u64,
    project_filter: Option<&str>,
    status_filter: Option<&str>,
    attention_filter: Option<&str>,
    waiting_on_filter: Option<&str>,
    limit: u64,
) -> Result<InboxResult> {
    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.last_seen_at, t.status_type, t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_kind, p.prompt_status, p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY COALESCE(t.updated_at, 0) DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut items = raw
        .into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                last_seen_at_raw,
                status_type,
                status_flags_json,
                last_turn_status,
                last_preview,
                prompt_kind,
                prompt_status,
                question,
            )| {
                let status_flags =
                    serde_json::from_str::<Vec<String>>(&status_flags_json).unwrap_or_default();
                let pending_prompt = prompt_kind.clone().map(|kind| PendingPrompt {
                    prompt_id: format!("{}:{}", kind, thread_id),
                    kind,
                    status: prompt_status.unwrap_or_else(|| "Needs input".to_string()),
                    question,
                });
                let snapshot = BridgeThreadSnapshot {
                    thread_id,
                    name,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags,
                    last_turn_status,
                    last_preview,
                    pending_prompt,
                };
                let mut item = classify_inbox_item(&snapshot, now);
                item.last_seen_at = Some(from_sql_i64(last_seen_at_raw)?);
                item.recent_action = recent_actions_json(conn, &item.thread_id, 1)?
                    .into_iter()
                    .next();
                Ok(item)
            },
        )
        .collect::<Result<Vec<_>>>()?;

    if let Some(needle) = project_filter {
        let needle = needle.to_lowercase();
        items.retain(|item| {
            item.cwd
                .as_deref()
                .unwrap_or_default()
                .to_lowercase()
                .contains(&needle)
        });
    }
    if let Some(status) = status_filter {
        items.retain(|item| item.status_type == status);
    }
    if let Some(attention) = attention_filter {
        items.retain(|item| item.attention_reason == attention);
    }
    if let Some(waiting_on) = waiting_on_filter {
        items.retain(|item| item.waiting_on == waiting_on);
    }

    items.sort_by_key(|item| std::cmp::Reverse(score_inbox_item(item)));
    if limit > 0 {
        items.truncate(limit as usize);
    }

    let pending_approval = items
        .iter()
        .filter(|item| item.attention_reason == "pending_approval")
        .count();
    let needs_reply = items
        .iter()
        .filter(|item| item.attention_reason == "needs_reply")
        .count();
    let active = items
        .iter()
        .filter(|item| item.attention_reason == "active")
        .count();
    let completed = items
        .iter()
        .filter(|item| item.attention_reason == "completed")
        .count();

    Ok(InboxResult {
        summary: InboxSummary {
            total: items.len(),
            needs_attention: items.iter().filter(|item| item.waiting_on == "me").count(),
            counts_by_reason: json!({
                "pendingApproval": pending_approval,
                "needsReply": needs_reply,
                "active": active,
                "completed": completed
            }),
            applied_filters: json!({
                "project": project_filter,
                "status": status_filter,
                "attention": attention_filter,
                "waitingOn": waiting_on_filter,
                "limit": limit
            }),
        },
        items,
    })
}

pub(crate) fn resolve_archive_targets(
    conn: &Connection,
    thread_ids: &[String],
    project: Option<&str>,
    status: Option<&str>,
    attention: Option<&str>,
    limit: u64,
    now: u64,
) -> Result<ArchiveSelection> {
    if !thread_ids.is_empty() {
        return Ok(ArchiveSelection {
            targets: thread_ids.to_vec(),
            using_filter_selection: false,
        });
    }

    let inbox = list_inbox_from_db(conn, now, project, status, attention, None, limit)?;
    Ok(ArchiveSelection {
        targets: inbox
            .items
            .into_iter()
            .map(|item| item.thread_id)
            .collect::<Vec<_>>(),
        using_filter_selection: true,
    })
}

pub(crate) fn archive_result(dry_run: bool, results: Vec<Value>) -> Value {
    json!({
        "ok": true,
        "action": "archive",
        "dryRun": dry_run,
        "results": results
    })
}

#[cfg(test)]
pub(crate) fn archive_from_db(
    conn: &Connection,
    thread_id: Option<&str>,
    attention: Option<&str>,
    dry_run: bool,
    yes: bool,
    now: u64,
) -> Result<Value> {
    let explicit = thread_id
        .map(|value| vec![value.to_string()])
        .unwrap_or_default();
    let selection = resolve_archive_targets(conn, &explicit, None, None, attention, 100, now)?;
    if !dry_run && selection.using_filter_selection && !yes {
        bail!("Refusing bulk archive without --yes or --dry-run");
    }

    let results = selection
        .targets
        .into_iter()
        .map(|thread_id| {
            json!({
                "threadId": thread_id,
                "status": if dry_run { "would_archive" } else { "archived" }
            })
        })
        .collect::<Vec<_>>();

    Ok(archive_result(dry_run, results))
}

pub(crate) fn telegram_current_project_key(chat_id: &str, user_id: Option<&str>) -> String {
    format!(
        "telegram_current_project:{}:{}",
        chat_id,
        user_id.unwrap_or("*")
    )
}

pub(crate) fn get_telegram_current_project_id(
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
) -> Result<Option<String>> {
    get_setting_text(conn, &telegram_current_project_key(chat_id, user_id))
}

pub(crate) fn set_telegram_current_project_id(
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
    project_id: &str,
) -> Result<()> {
    set_setting_text(
        conn,
        &telegram_current_project_key(chat_id, user_id),
        project_id,
    )
}

pub(crate) fn unarchive_thread_result(
    conn: &Connection,
    thread_id: &str,
    dry_run: bool,
    now: u64,
    live_result: Option<Value>,
) -> Result<Value> {
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "unarchive",
            "dryRun": true,
            "results": [{ "threadId": thread_id, "status": "would_unarchive" }]
        }));
    }

    let result = live_result.unwrap_or_else(|| json!({ "threadId": thread_id, "ok": true }));
    record_action(
        conn,
        thread_id,
        "unarchive",
        json!({ "result": result, "unarchivedAt": now }),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "unarchive",
        "dryRun": false,
        "results": [{ "threadId": thread_id, "status": "unarchived", "result": result }]
    }))
}

#[cfg(test)]
pub(crate) fn watch_once_from_db(conn: &Connection) -> Result<Vec<Value>> {
    let waiting = list_waiting_from_db(conn, None, 100)?;
    Ok(waiting
        .threads
        .into_iter()
        .map(|thread| {
            json!({
                "type": "thread_waiting",
                "threadId": thread.thread_id,
                "thread": {
                    "threadId": thread.thread_id,
                    "name": thread.name,
                    "cwd": thread.cwd,
                    "updatedAt": thread.updated_at,
                    "statusType": thread.status_type,
                    "statusFlags": thread.status_flags,
                    "lastPreview": thread.last_preview,
                    "pendingPrompt": {
                        "promptId": thread.prompt.prompt_id,
                        "promptKind": thread.prompt.kind,
                        "promptStatus": thread.prompt.status,
                        "question": thread.prompt.question
                    }
                }
            })
        })
        .collect())
}

pub(crate) fn telegram_inbound_processed(
    conn: &Connection,
    bot_id: &str,
    update_id: i64,
) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT update_id FROM telegram_inbound_log WHERE bot_id = ?1 AND update_id = ?2",
            params![bot_id, update_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

pub(crate) fn record_telegram_inbound_processed(
    conn: &Connection,
    bot_id: &str,
    update_id: i64,
    update_kind: &str,
    result: &Value,
    context: TelegramInboundLogContext<'_>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO telegram_inbound_log(
            bot_id,
            update_id,
            update_kind,
            result_json,
            processed_at,
            thread_id,
            route_message_id,
            result_action,
            codex_transport,
            codex_app_server_pid
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            bot_id,
            update_id,
            update_kind,
            serde_json::to_string(result)?,
            to_sql_i64(now)?,
            context.thread_id,
            context.route_message_id,
            context.result_action,
            context.codex_transport,
            context.codex_app_server_pid.map(i64::from),
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_message_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    thread_id: &str,
    event_id: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_message_routes(chat_id, message_id, thread_id, event_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![chat_id, message_id, thread_id, event_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_callback_route(
    conn: &Connection,
    route: &TelegramCallbackRoute,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_callback_routes(callback_id, chat_id, message_id, thread_id, action, created_at, used_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
        params![
            route.callback_id,
            route.chat_id,
            route.message_id,
            route.thread_id,
            route.action.as_str(),
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_command_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    kind: TelegramCommandRouteKind,
    payload: Option<&Value>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_command_routes(chat_id, message_id, command, payload_json, created_at, used_at)
         VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
        params![
            chat_id,
            message_id,
            kind.as_str(),
            payload.map(serde_json::to_string).transpose()?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

pub(crate) fn update_telegram_callback_message_id(
    conn: &Connection,
    callback_id: &str,
    message_id: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_callback_routes SET message_id = ?2 WHERE callback_id = ?1",
        params![callback_id, message_id],
    )?;
    Ok(())
}

pub(crate) fn mark_telegram_callback_route_used(
    conn: &Connection,
    callback_id: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_callback_routes SET used_at = ?2 WHERE callback_id = ?1 AND used_at IS NULL",
        params![callback_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

pub(crate) fn lookup_telegram_message_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT thread_id FROM telegram_message_routes WHERE chat_id = ?1 AND message_id = ?2",
        params![chat_id, message_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn lookup_telegram_command_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
) -> Result<Option<(TelegramCommandRouteKind, Option<Value>)>> {
    let command = conn
        .query_row(
            "SELECT command, payload_json FROM telegram_command_routes
             WHERE chat_id = ?1 AND message_id = ?2 AND used_at IS NULL",
            params![chat_id, message_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    match command {
        Some((command, payload_json)) => {
            Ok(TelegramCommandRouteKind::from_str(&command).map(|kind| {
                (
                    kind,
                    payload_json.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
                )
            }))
        }
        None => Ok(None),
    }
}

pub(crate) fn mark_telegram_command_route_used(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    now: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_command_routes SET used_at = ?3 WHERE chat_id = ?1 AND message_id = ?2",
        params![chat_id, message_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

pub(crate) fn enqueue_outbound_event(conn: &Connection, event: &Value, now: u64) -> Result<bool> {
    let event_id = crate::notification_event_id(event);
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("codex_event");
    let thread_id = crate::event_thread_id(event);
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO outbound_events(event_id, event_type, thread_id, payload_json, status, attempts, next_attempt_at, last_error, created_at, delivered_at)
         VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, NULL, ?5, NULL)",
        params![
            event_id,
            event_type,
            thread_id,
            serde_json::to_string(event)?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(inserted > 0)
}

pub(crate) fn pending_outbound_count(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM outbound_events WHERE status != 'delivered'",
        [],
        |row| row.get(0),
    )?;
    from_sql_i64(count)
}

pub(crate) fn clear_pending_outbound_events(conn: &Connection) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM outbound_events WHERE status != 'delivered'",
        [],
    )?;
    Ok(deleted)
}

pub(crate) fn transport_delivery_exists(
    conn: &Connection,
    event_id: &str,
    transport: &str,
) -> Result<bool> {
    let exists: Option<String> = conn
        .query_row(
            "SELECT event_id FROM transport_delivery_log WHERE event_id = ?1 AND transport = ?2",
            params![event_id, transport],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

pub(crate) fn record_transport_delivery(
    conn: &Connection,
    event_id: &str,
    transport: &str,
    result: &Value,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO transport_delivery_log(event_id, transport, result_json, delivered_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event_id,
            transport,
            serde_json::to_string(result)?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

fn retry_delay_ms(attempts: u64) -> u64 {
    let exponent = attempts.saturating_sub(1).min(8);
    (1u64 << exponent) * 1000
}

pub(crate) fn deliver_due_outbound_events<F>(
    conn: &Connection,
    now: u64,
    limit: usize,
    deadline: Option<Instant>,
    mut sender: F,
) -> Result<OutboxDeliverySummary>
where
    F: FnMut(&Value) -> Result<Value>,
{
    let rows = {
        let mut stmt = conn.prepare(
            "SELECT event_id, payload_json, attempts
             FROM outbound_events
             WHERE status != 'delivered' AND next_attempt_at <= ?1
             ORDER BY created_at ASC, event_id ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![to_sql_i64(now)?, limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    let mut summary = OutboxDeliverySummary {
        attempted: 0,
        delivered: 0,
        failed: 0,
    };
    for (event_id, payload_json, attempts) in rows {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        summary.attempted += 1;
        let event: Value = serde_json::from_str(&payload_json)
            .with_context(|| format!("outbound event {event_id} contains invalid JSON"))?;
        match sender(&event) {
            Ok(_) => {
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'delivered', attempts = attempts + 1, delivered_at = ?2, last_error = NULL
                     WHERE event_id = ?1",
                    params![event_id, to_sql_i64(now)?],
                )?;
                summary.delivered += 1;
            }
            Err(error) => {
                let next_attempts = from_sql_i64(attempts)?.saturating_add(1);
                let next_attempt_at = now.saturating_add(retry_delay_ms(next_attempts));
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'failed', attempts = ?2, next_attempt_at = ?3, last_error = ?4
                     WHERE event_id = ?1",
                    params![
                        event_id,
                        to_sql_i64(next_attempts)?,
                        to_sql_i64(next_attempt_at)?,
                        format!("{error:#}")
                    ],
                )?;
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::json;

    use crate::telegram::{
        extract_telegram_callback_route, extract_telegram_command_prompt_reply,
        extract_telegram_reply_route, telegram_bot_id,
    };
    use crate::{importable_projects_from_observed, set_away_mode, TelegramConfig};

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
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let pending_prompt = if status_flags_vec
            .iter()
            .any(|flag| flag == "waitingOnApproval")
        {
            Some(PendingPrompt {
                prompt_id: format!("approval:{thread_id}"),
                kind: "approval".to_string(),
                status: "Needs approval".to_string(),
                question: Some(format!("preview for {thread_id}")),
            })
        } else if status_flags_vec
            .iter()
            .any(|flag| flag == "waitingOnUserInput" || flag == "waitingOnInput")
        {
            Some(PendingPrompt {
                prompt_id: format!("reply:{thread_id}"),
                kind: "reply".to_string(),
                status: "Needs input".to_string(),
                question: Some(format!("preview for {thread_id}")),
            })
        } else {
            None
        };
        BridgeThreadSnapshot {
            thread_id: thread_id.to_string(),
            name: None,
            cwd: Some(cwd.to_string()),
            updated_at: Some(updated_at),
            status_type: status_type.to_string(),
            status_flags: status_flags_vec.clone(),
            last_turn_status: last_turn_status.map(|value| value.to_string()),
            last_preview: Some(format!("preview for {thread_id}")),
            pending_prompt,
        }
    }

    #[test]
    fn state_schema_matches_ts_bridge_contract() {
        let conn = create_state_db_in_memory().expect("db");
        let tables = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .expect("prepare")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("tables");
        assert!(tables.contains(&"delivery_log".to_string()));
        assert!(tables.contains(&"outbound_events".to_string()));

        let enabled = set_away_mode(&conn, true, 1234).expect("enable away");
        assert_eq!(enabled["away"], true);
        assert_eq!(
            get_setting_text(&conn, "away")
                .expect("away setting")
                .as_deref(),
            Some("true")
        );
        assert_eq!(
            get_setting_text(&conn, "away_mode").expect("legacy away setting"),
            None
        );
    }

    #[test]
    fn create_state_db_migrates_legacy_threads_cache_columns() {
        let path =
            std::env::temp_dir().join(format!("codex-state-migrate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "
                CREATE TABLE threads_cache (
                    thread_id TEXT PRIMARY KEY,
                    name TEXT,
                    cwd TEXT,
                    source TEXT,
                    status_type TEXT NOT NULL,
                    status_flags_json TEXT NOT NULL,
                    updated_at INTEGER,
                    last_seen_at INTEGER NOT NULL
                );
                ",
            )
            .expect("create legacy table");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "threads_cache").expect("columns");
        assert!(columns.contains(&"last_turn_status".to_string()));
        assert!(columns.contains(&"last_preview".to_string()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn create_state_db_migrates_legacy_telegram_inbound_log_columns() {
        let path = std::env::temp_dir().join(format!(
            "codex-telegram-inbound-migrate-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "
                CREATE TABLE telegram_inbound_log (
                    bot_id TEXT NOT NULL,
                    update_id INTEGER NOT NULL,
                    update_kind TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    processed_at INTEGER NOT NULL,
                    PRIMARY KEY(bot_id, update_id)
                );
                ",
            )
            .expect("create legacy inbound log");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "telegram_inbound_log").expect("columns");
        assert!(columns.contains(&"thread_id".to_string()));
        assert!(columns.contains(&"route_message_id".to_string()));
        assert!(columns.contains(&"result_action".to_string()));
        assert!(columns.contains(&"codex_transport".to_string()));
        assert!(columns.contains(&"codex_app_server_pid".to_string()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn telegram_command_prompt_routes_map_reply_to_new_thread_prompt() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_command_route(
            &conn,
            "456",
            222,
            TelegramCommandRouteKind::NewThread,
            Some(&json!({ "projectId": "bridge" })),
            1000,
        )
        .expect("insert command route");

        let route = extract_telegram_command_prompt_reply(
            &conn,
            &json!({
                "chat": { "id": 456 },
                "from": { "id": 789 },
                "text": "Build the mobile proof report",
                "reply_to_message": { "message_id": 222 }
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("extract command prompt reply")
        .expect("route");

        assert_eq!(route.kind, TelegramCommandRouteKind::NewThread);
        assert_eq!(route.message, "Build the mobile proof report");
        assert_eq!(route.project_id.as_deref(), Some("bridge"));
    }

    #[test]
    fn telegram_routes_map_message_replies_to_codex_threads() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_message_route(&conn, "456", 111, "thr_1", "event_1", 1000)
            .expect("insert route");

        let routed = extract_telegram_reply_route(
            &conn,
            &json!({
                "message_id": 222,
                "from": { "id": 789 },
                "chat": { "id": 456 },
                "text": "continue with the safer patch",
                "reply_to_message": { "message_id": 111 }
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("route")
        .expect("reply should route");

        assert_eq!(routed.thread_id, "thr_1");
        assert_eq!(routed.message, "continue with the safer patch");
    }

    #[test]
    fn telegram_callback_routes_map_buttons_to_approvals() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_callback_route(
            &conn,
            &TelegramCallbackRoute {
                callback_id: "cb_1".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_approval".to_string(),
                action: TelegramCallbackAction::Deny,
            },
            1000,
        )
        .expect("insert callback");

        let routed = extract_telegram_callback_route(
            &conn,
            &json!({
                "id": "callback-query-id",
                "from": { "id": 789 },
                "message": {
                    "message_id": 111,
                    "chat": { "id": 456 }
                },
                "data": "codex:cb_1"
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("route")
        .expect("callback should route");

        assert_eq!(routed.thread_id, "thr_approval");
        assert_eq!(routed.action, TelegramCallbackAction::Deny);
        assert_eq!(routed.callback_query_id, "callback-query-id");
    }

    #[test]
    fn telegram_inbound_log_dedupes_processed_updates_per_bot() {
        let conn = create_state_db_in_memory().expect("db");
        let bot_id = telegram_bot_id("123:secret");

        assert!(
            !telegram_inbound_processed(&conn, &bot_id, 42).expect("lookup"),
            "update should not start processed"
        );
        record_telegram_inbound_processed(
            &conn,
            &bot_id,
            42,
            "message",
            &json!({ "threadId": "thr_1" }),
            TelegramInboundLogContext {
                thread_id: Some("thr_1"),
                route_message_id: Some(111),
                result_action: Some("telegram_reply"),
                codex_transport: Some("spawned_stdio"),
                codex_app_server_pid: Some(4242),
            },
            1000,
        )
        .expect("record inbound update");

        assert!(
            telegram_inbound_processed(&conn, &bot_id, 42).expect("lookup"),
            "recorded update should not be processed again"
        );
        assert!(
            !telegram_inbound_processed(&conn, &telegram_bot_id("456:other"), 42).expect("lookup"),
            "same update id from a different bot token hash must remain independent"
        );

        let row = conn
            .query_row(
                "SELECT thread_id, route_message_id, result_action, codex_transport, codex_app_server_pid
                 FROM telegram_inbound_log
                 WHERE bot_id = ?1 AND update_id = ?2",
                params![bot_id, 42],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .expect("structured inbound row");
        assert_eq!(row.0.as_deref(), Some("thr_1"));
        assert_eq!(row.1, Some(111));
        assert_eq!(row.2.as_deref(), Some("telegram_reply"));
        assert_eq!(row.3.as_deref(), Some("spawned_stdio"));
        assert_eq!(row.4, Some(4242));
    }

    #[test]
    fn persists_snapshots_and_actions_in_sqlite_state() {
        let conn = create_state_db_in_memory().expect("db");
        let waiting = snapshot_fixture(
            "thr_wait",
            "/tmp/project-wait",
            1200,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        let done = snapshot_fixture(
            "thr_done",
            "/tmp/project-done",
            1300,
            "notLoaded",
            vec![],
            Some("completed"),
        );

        upsert_thread_snapshot(&conn, &waiting, 2000).expect("upsert waiting");
        upsert_thread_snapshot(&conn, &done, 2000).expect("upsert done");
        record_action(
            &conn,
            "thr_wait",
            "reply",
            json!({"message": "On it"}),
            2100,
        )
        .expect("record action");

        let waiting_rows = list_waiting_from_db(&conn, None, 10).expect("waiting rows");
        assert_eq!(waiting_rows.threads.len(), 1);
        assert_eq!(waiting_rows.threads[0].thread_id, "thr_wait");
        assert_eq!(waiting_rows.threads[0].prompt.kind, "reply");

        let history = get_thread_history(&conn, "thr_wait", 10).expect("history");
        assert_eq!(history[0].action_type, "reply");
    }

    #[test]
    fn list_recent_thread_snapshots_orders_by_latest_activity() {
        let conn = create_state_db_in_memory().expect("db");
        let older = snapshot_fixture(
            "thr_old",
            "/tmp/project-old",
            1200,
            "notLoaded",
            vec![],
            Some("completed"),
        );
        let newer = snapshot_fixture(
            "thr_new",
            "/tmp/project-new",
            2400,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );

        upsert_thread_snapshot(&conn, &older, 1200).expect("upsert older");
        upsert_thread_snapshot(&conn, &newer, 2400).expect("upsert newer");

        let recent = list_recent_thread_snapshots_from_db(&conn, 1).expect("recent threads");

        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].thread_id, "thr_new");
        assert_eq!(
            recent[0]
                .pending_prompt
                .as_ref()
                .expect("pending prompt")
                .kind,
            "reply"
        );
    }

    #[test]
    fn project_current_selection_round_trips_per_identity() {
        let conn = create_state_db_in_memory().expect("db");
        set_telegram_current_project_id(&conn, "chat-1", Some("user-1"), "bridge")
            .expect("set current");

        assert_eq!(
            get_telegram_current_project_id(&conn, "chat-1", Some("user-1"))
                .expect("get current")
                .as_deref(),
            Some("bridge")
        );
        assert_eq!(
            get_telegram_current_project_id(&conn, "chat-1", Some("user-2"))
                .expect("get other")
                .as_deref(),
            None
        );
    }

    #[test]
    fn project_import_suggests_unique_ids_from_observed_workspaces() {
        let conn = create_state_db_in_memory().expect("db");
        let alpha = snapshot_fixture(
            "thr_alpha",
            "/Users/hanifcarroll/projects/client-a/app",
            2000,
            "active",
            vec![],
            Some("in_progress"),
        );
        let beta = snapshot_fixture(
            "thr_beta",
            "/Users/hanifcarroll/projects/client-b/app",
            3000,
            "active",
            vec![],
            Some("in_progress"),
        );
        upsert_thread_snapshot(&conn, &alpha, 2000).expect("upsert alpha");
        upsert_thread_snapshot(&conn, &beta, 3000).expect("upsert beta");

        let imported = importable_projects_from_observed(
            &observed_workspaces_from_db(&conn, 10).expect("observed"),
            &[],
        );

        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].id, "app");
        assert_eq!(imported[1].id, "app-2");
    }

    #[test]
    fn live_backend_status_path_uses_bridge_state_directory() {
        let _guard = test_env_lock().lock().expect("test env lock");
        let home =
            std::env::temp_dir().join(format!("codex-live-state-path-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(&home).expect("create temp home");
        let previous_home = std::env::var("HOME").ok();
        let previous_state_dir = std::env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR").ok();
        std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
        std::env::set_var("HOME", &home);

        let path = live_backend_status_path().expect("live backend status path");

        if let Some(previous_home) = previous_home {
            std::env::set_var("HOME", previous_home);
        } else {
            std::env::remove_var("HOME");
        }
        if let Some(previous_state_dir) = previous_state_dir {
            std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
        } else {
            std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
        }
        let _ = fs::remove_dir_all(&home);

        assert!(path.ends_with(".codex-telegram-bridge/live-backend.json"));
    }

    #[test]
    fn prune_state_logs_removes_only_rows_older_than_retention() {
        let conn = create_state_db_in_memory().expect("db");
        let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        let now = retention_ms + 2000;
        record_telegram_inbound_processed(
            &conn,
            "bot",
            1,
            "message_ignored",
            &json!({}),
            TelegramInboundLogContext::default(),
            1000,
        )
        .expect("old inbound");
        record_telegram_inbound_processed(
            &conn,
            "bot",
            2,
            "message_ignored",
            &json!({}),
            TelegramInboundLogContext::default(),
            now,
        )
        .expect("recent inbound");
        conn.execute(
            "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
             VALUES ('thr_1', 'x', '{}', 1000)",
            [],
        )
        .expect("old action");
        conn.execute(
            "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
             VALUES ('thr_1', 'x', '{}', ?1)",
            params![to_sql_i64(now).expect("recent timestamp")],
        )
        .expect("recent action");

        let removed = prune_state_logs(&conn, now).expect("prune");
        assert_eq!(removed, 2);
        assert!(!telegram_inbound_processed(&conn, "bot", 1).expect("old inbound removed"));
        assert!(telegram_inbound_processed(&conn, "bot", 2).expect("recent inbound kept"));
        let recent_actions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM actions_log WHERE created_at >= ?1",
                params![to_sql_i64(now).expect("cutoff")],
                |row| row.get(0),
            )
            .expect("recent actions count");
        assert_eq!(recent_actions, 1);
    }

    #[test]
    fn outbound_delivery_deadline_skips_pending_events() {
        let conn = create_state_db_in_memory().expect("db");
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_waiting",
                "threadId": "thr_1",
                "updatedAt": 42
            }),
            1000,
        )
        .expect("enqueue");

        let summary = deliver_due_outbound_events(
            &conn,
            2000,
            10,
            Some(Instant::now() - Duration::from_secs(1)),
            |_| Ok(json!({ "ok": true })),
        )
        .expect("deadline summary");

        assert_eq!(summary.attempted, 0);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }
}

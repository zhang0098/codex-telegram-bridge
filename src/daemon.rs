use anyhow::{bail, Context, Result};
use fs2::FileExt;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use crate::codex::{
    filter_watch_events, parse_event_filter, start_codex_watch_receiver, sync_state_from_live,
    watch_events_from_sync_result, watch_thread_error_event, CodexAppServerClient,
};
use crate::state::{
    create_state_db, deliver_due_outbound_events, enqueue_outbound_event, pending_outbound_count,
    prune_state_logs, record_transport_delivery, should_emit_for_away_window, state_db_path,
    transport_delivery_exists, OutboxDeliverySummary,
};
use crate::telegram::{
    deliver_telegram_event, process_telegram_updates, refresh_telegram_typing_indicators,
    telegram_set_my_commands,
};
use crate::{
    daemon_config_path, load_daemon_config, notification_event_id, now_millis, shell_quote,
    state_dir_path, DaemonConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct DaemonServiceSpec {
    pub(crate) service_path: PathBuf,
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
    pub(crate) unit_name: String,
    pub(crate) contents: String,
    pub(crate) install_command: String,
    pub(crate) uninstall_command: String,
    pub(crate) start_command: String,
    pub(crate) stop_command: String,
    pub(crate) status_command: String,
}

pub(crate) const DEFAULT_DAEMON_LABEL: &str = "com.hanifcarroll.codex-telegram-bridge";
const DAEMON_PRUNE_INTERVAL_MS: u64 = 10 * 60 * 1000;
const DAEMON_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

struct DaemonLock {
    file: fs::File,
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn daemon_lock_path() -> Result<PathBuf> {
    Ok(state_db_path()?.with_file_name("daemon.lock"))
}

fn acquire_daemon_lock() -> Result<DaemonLock> {
    let path = daemon_lock_path()?;
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to open daemon lock at {}", path.display()))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok(DaemonLock { file }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if started.elapsed() >= DAEMON_LOCK_TIMEOUT {
                    bail!(
                        "another daemon instance is already running (lock: {})",
                        path.display()
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to lock daemon instance at {}", path.display())
                })
            }
        }
    }
}

pub(crate) fn daemon_lock_free() -> Result<bool> {
    let path = daemon_lock_path()?;
    let file = match fs::OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to open daemon lock at {}", path.display()))
        }
    };
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("failed to probe daemon lock at {}", path.display())),
    }
}

fn event_observed_at(event: &Value) -> Option<u64> {
    event
        .get("updatedAt")
        .and_then(Value::as_u64)
        .or_else(|| event.get("observedAt").and_then(Value::as_u64))
        .or_else(|| event.pointer("/thread/updatedAt").and_then(Value::as_u64))
}

fn should_enqueue_daemon_notification(conn: &Connection, event: &Value) -> Result<bool> {
    let away_status = crate::get_away_mode(conn)?;
    if !away_notifications_enabled_from_status(&away_status) {
        return Ok(false);
    }
    let away_started_at = away_status.get("awayStartedAt").and_then(Value::as_u64);
    Ok(should_emit_for_away_window(
        away_started_at,
        event_observed_at(event),
    ))
}

pub(crate) fn enqueue_daemon_notification_events(
    conn: &Connection,
    events: &[Value],
    now: u64,
) -> Result<usize> {
    let mut enqueued = 0usize;
    for event in events {
        if should_enqueue_daemon_notification(conn, event)?
            && enqueue_outbound_event(conn, event, now)?
        {
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

fn away_notifications_enabled_from_status(away_status: &Value) -> bool {
    away_status.get("away").and_then(Value::as_bool) == Some(true)
}

fn away_notifications_enabled(conn: &Connection) -> Result<bool> {
    Ok(away_notifications_enabled_from_status(
        &crate::get_away_mode(conn)?,
    ))
}

fn reconcile_daemon_backend(conn: &Connection, config: &DaemonConfig, now: u64) -> Value {
    if !away_notifications_enabled(conn).unwrap_or(false) {
        return Value::Null;
    }
    let Some(codex) = config.codex.as_ref() else {
        return json!({
            "ok": false,
            "required": true,
            "state": "unhealthy",
            "error": "shared Codex live backend is not configured"
        });
    };
    match crate::reconcile_live_backend(codex, now) {
        Ok(result) => json!({
            "ok": result.status.healthy,
            "action": result.action,
            "status": result.status
        }),
        Err(error) => json!({
            "ok": false,
            "required": true,
            "state": "unhealthy",
            "error": format!("{error:#}")
        }),
    }
}

fn deliver_outbound_events(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    deadline: Instant,
) -> Result<OutboxDeliverySummary> {
    deliver_due_outbound_events(conn, now, 100, Some(deadline), |event| {
        let event_id = notification_event_id(event);
        let telegram = if let Some(telegram) = config.telegram.as_ref() {
                if transport_delivery_exists(conn, &event_id, "telegram")? {
                    json!({ "ok": true, "transport": "telegram", "skipped": "already_delivered" })
                } else {
                    let result = deliver_telegram_event(conn, telegram, event, now, timeout)?;
                    record_transport_delivery(conn, &event_id, "telegram", &result, now)?;
                    result
                }
            } else {
                Value::Null
            };
        Ok(json!({ "telegram": telegram }))
    })
}

fn daemon_cycle_budget(timeout: Duration) -> Duration {
    timeout.max(Duration::from_secs(5)).saturating_mul(3)
}

fn daemon_cycle(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let deadline = Instant::now() + daemon_cycle_budget(timeout);
    let filter = parse_event_filter(Some(&config.events));
    let backend = reconcile_daemon_backend(conn, config, now);
    let telegram_updates = match config.telegram.as_ref() {
        Some(telegram) => {
            let mut result =
                match process_telegram_updates(conn, config, now, timeout, Some(deadline)) {
                    Ok(result) => result,
                    Err(error) => json!({
                        "ok": false,
                        "transport": "telegram",
                        "error": format!("{error:#}")
                    }),
                };
            let typing = if Instant::now() < deadline {
                refresh_telegram_typing_indicators(conn, telegram, now, timeout).unwrap_or_else(
                    |error| {
                        json!({
                            "ok": false,
                            "transport": "telegram",
                            "error": format!("{error:#}")
                        })
                    },
                )
            } else {
                json!({
                    "ok": true,
                    "transport": "telegram",
                    "skipped": "cycle_deadline"
                })
            };
            if let Some(object) = result.as_object_mut() {
                object.insert("typing".to_string(), typing);
            }
            result
        }
        None => Value::Null,
    };
    let events = match CodexAppServerClient::connect_configured(config).and_then(|mut client| {
        client.set_deadline(deadline);
        let sync_result = sync_state_from_live(&mut client, conn, now, 50, true)?;
        Ok(watch_events_from_sync_result(
            &sync_result,
            client.drain_notifications(),
            filter.as_ref(),
        ))
    }) {
        Ok(events) => events,
        Err(error) => filter_watch_events(vec![watch_thread_error_event(&error)], filter.as_ref()),
    };
    let enqueued = enqueue_daemon_notification_events(conn, &events, now)?;
    let delivery = if away_notifications_enabled(conn)? {
        deliver_outbound_events(conn, config, now, timeout, deadline)?
    } else {
        OutboxDeliverySummary::default()
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_cycle",
        "observed": events.len(),
        "enqueued": enqueued,
        "backend": backend,
        "delivery": delivery,
        "telegramUpdates": telegram_updates,
        "pending": pending_outbound_count(conn)?
    }))
}

pub(crate) fn run_daemon(once: bool, poll_interval: u64, timeout: Duration) -> Result<()> {
    let _daemon_lock = acquire_daemon_lock()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let config = load_daemon_config()?;
    if once {
        let result = daemon_cycle(&conn, &config, now_millis()?, timeout)?;
        println!("{}", serde_json::to_string(&result)?);
        return Ok(());
    }

    let telegram_commands = config
        .telegram
        .as_ref()
        .map(|telegram| {
            telegram_set_my_commands(telegram, timeout)
                .map(|_| json!({ "registered": true }))
                .unwrap_or_else(|error| {
                    json!({
                        "registered": false,
                        "error": format!("{error:#}")
                    })
                })
        });

    println!(
        "{}",
        serde_json::to_string(&json!({
            "ok": true,
            "action": "daemon_started",
            "configPath": daemon_config_path()?.display().to_string(),
            "events": config.events,
            "telegramCommands": telegram_commands
        }))?
    );
    let watch_rx = start_codex_watch_receiver().ok();
    let mut last_prune_at = 0u64;
    loop {
        let config = match load_daemon_config() {
            Ok(config) => config,
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "daemon_config_error",
                        "error": format!("{error:#}")
                    })
                );
                thread::sleep(Duration::from_millis(poll_interval));
                continue;
            }
        };
        let now = match now_millis() {
            Ok(now) => now,
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "daemon_clock_error",
                        "error": format!("{error:#}")
                    })
                );
                thread::sleep(Duration::from_millis(poll_interval));
                continue;
            }
        };
        if now.saturating_sub(last_prune_at) >= DAEMON_PRUNE_INTERVAL_MS {
            match prune_state_logs(&conn, now) {
                Ok(removed) => {
                    println!(
                        "{}",
                        json!({
                            "ok": true,
                            "action": "daemon_logs_pruned",
                            "removed": removed
                        })
                    );
                }
                Err(error) => {
                    println!(
                        "{}",
                        json!({
                            "ok": false,
                            "action": "daemon_logs_prune_error",
                            "error": format!("{error:#}")
                        })
                    );
                }
            }
            last_prune_at = now;
        }
        match daemon_cycle(&conn, &config, now, timeout) {
            Ok(result) => println!("{}", result),
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "daemon_cycle_error",
                        "error": format!("{error:#}")
                    })
                );
            }
        }
        if let Some(rx) = watch_rx.as_ref() {
            rx.recv_timeout(Duration::from_millis(poll_interval));
        } else {
            thread::sleep(Duration::from_millis(poll_interval));
        }
    }
}

fn validate_daemon_label(label: &str) -> Result<&str> {
    let trimmed = label.trim();
    if trimmed.is_empty() {
        bail!("daemon label cannot be empty");
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains('\'') {
        bail!("daemon label contains unsupported characters");
    }
    Ok(trimmed)
}

fn push_path_entry(entries: &mut Vec<String>, path: impl Into<String>) {
    let path = path.into();
    if path.trim().is_empty() || entries.iter().any(|entry| entry == &path) {
        return;
    }
    entries.push(path);
}

fn push_home_path(entries: &mut Vec<String>, home: &Path, suffix: &str) {
    push_path_entry(entries, home.join(suffix).display().to_string());
}

fn push_node_version_bins(entries: &mut Vec<String>, versions_dir: PathBuf) {
    let Ok(children) = fs::read_dir(versions_dir) else {
        return;
    };
    let mut bins = children
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("bin"))
        .filter(|path| path.is_dir())
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    bins.sort();
    for bin in bins {
        push_path_entry(entries, bin);
    }
}

fn daemon_runtime_path() -> String {
    let mut entries = Vec::new();
    if let Some(home) = dirs::home_dir() {
        push_home_path(&mut entries, &home, ".bun/bin");
        push_home_path(&mut entries, &home, ".cargo/bin");
        push_home_path(&mut entries, &home, ".local/bin");
        push_home_path(&mut entries, &home, ".deno/bin");
        push_home_path(&mut entries, &home, ".pyenv/shims");
        push_home_path(&mut entries, &home, ".asdf/shims");
        push_home_path(&mut entries, &home, "Library/pnpm");
        push_home_path(&mut entries, &home, "go/bin");
        push_node_version_bins(&mut entries, home.join(".config/nvm/versions/node"));
        push_node_version_bins(&mut entries, home.join(".nvm/versions/node"));
    }

    for path in [
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ] {
        push_path_entry(&mut entries, path);
    }

    if let Some(current) = env::var_os("PATH").and_then(|value| value.into_string().ok()) {
        for path in current.split(':') {
            push_path_entry(&mut entries, path);
        }
    }

    entries.join(":")
}

fn systemd_escape_env(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
}

pub(crate) fn daemon_service_spec(label: &str, bridge_command: &str) -> Result<DaemonServiceSpec> {
    let label = validate_daemon_label(label)?;
    let bridge_command = bridge_command.trim();
    if bridge_command.is_empty() {
        bail!("bridge command cannot be empty");
    }
    let service_bridge_command = resolve_service_bridge_command(bridge_command);
    let state_dir = state_dir_path()?;
    let stdout_log = state_dir.join("daemon.out.log");
    let stderr_log = state_dir.join("daemon.err.log");
    let runtime_path = daemon_runtime_path();
    let run_args = [
        service_bridge_command.clone(),
        "daemon".to_string(),
        "run".to_string(),
    ];
    if cfg!(target_os = "macos") {
        let service_path = dirs::home_dir()
            .context("home directory is not available")?
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist"));
        let args_xml = run_args
            .iter()
            .map(|arg| format!("        <string>{}</string>", xml_escape(arg)))
            .collect::<Vec<_>>()
            .join("\n");
        let contents = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{}</string>
    <key>ProgramArguments</key>
    <array>
{}
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{}</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{}</string>
    <key>StandardErrorPath</key>
    <string>{}</string>
</dict>
</plist>
"#,
            xml_escape(label),
            args_xml,
            xml_escape(&runtime_path),
            xml_escape(&stdout_log.display().to_string()),
            xml_escape(&stderr_log.display().to_string())
        );
        let quoted_path = shell_quote(&service_path.display().to_string());
        let bootstrap_command = format!("launchctl bootstrap gui/$(id -u) {quoted_path}");
        let kickstart_command =
            format!("launchctl kickstart -k gui/$(id -u)/{}", shell_quote(label));
        let bootout_command = format!("launchctl bootout gui/$(id -u)/{}", shell_quote(label));
        Ok(DaemonServiceSpec {
            service_path,
            stdout_log,
            stderr_log,
            unit_name: label.to_string(),
            contents,
            install_command: bootstrap_command.clone(),
            uninstall_command: format!("{bootout_command} 2>/dev/null || true"),
            start_command: format!("{bootstrap_command} 2>/dev/null || {kickstart_command}"),
            stop_command: bootout_command,
            status_command: format!("launchctl print gui/$(id -u)/{}", shell_quote(label)),
        })
    } else if cfg!(target_os = "linux") {
        let unit_name = if label.ends_with(".service") {
            label.to_string()
        } else {
            format!("{label}.service")
        };
        let service_path = dirs::home_dir()
            .context("home directory is not available")?
            .join(".config")
            .join("systemd")
            .join("user")
            .join(&unit_name);
        let contents = format!(
            "[Unit]\nDescription=Codex Telegram Bridge notification daemon\n\n[Service]\nType=simple\nEnvironment=\"PATH={}\"\nExecStart={} daemon run\nRestart=always\nRestartSec=2\nStandardOutput=append:{}\nStandardError=append:{}\n\n[Install]\nWantedBy=default.target\n",
            systemd_escape_env(&runtime_path),
            shell_quote(&service_bridge_command),
            stdout_log.display(),
            stderr_log.display()
        );
        Ok(DaemonServiceSpec {
            service_path,
            stdout_log,
            stderr_log,
            unit_name: unit_name.clone(),
            contents,
            install_command: format!(
                "systemctl --user daemon-reload && systemctl --user enable --now {}",
                shell_quote(&unit_name)
            ),
            uninstall_command: format!(
                "systemctl --user disable --now {} 2>/dev/null || true",
                shell_quote(&unit_name)
            ),
            start_command: format!(
                "systemctl --user daemon-reload && systemctl --user enable --now {}",
                shell_quote(&unit_name)
            ),
            stop_command: format!("systemctl --user stop {}", shell_quote(&unit_name)),
            status_command: format!("systemctl --user status {}", shell_quote(&unit_name)),
        })
    } else {
        bail!("daemon service install is only supported on macOS launchd and Linux systemd")
    }
}

fn resolve_service_bridge_command(bridge_command: &str) -> String {
    let trimmed = bridge_command.trim();
    if trimmed.contains('/') {
        let path = PathBuf::from(trimmed);
        return if path.is_absolute() {
            path.display().to_string()
        } else {
            env::current_dir()
                .map(|cwd| cwd.join(path).display().to_string())
                .unwrap_or_else(|_| trimmed.to_string())
        };
    }
    which::which(trimmed)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| trimmed.to_string())
}

pub(crate) fn install_daemon_service(
    label: &str,
    bridge_command: &str,
    dry_run: bool,
) -> Result<Value> {
    let spec = daemon_service_spec(label, bridge_command)?;
    if !dry_run {
        if let Some(parent) = spec.service_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&spec.service_path, &spec.contents)?;
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_install",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "servicePath": spec.service_path,
        "runCommand": crate::daemon_run_command(bridge_command),
        "installCommand": spec.install_command,
        "startCommand": spec.start_command,
        "stopCommand": spec.stop_command,
        "statusCommand": spec.status_command,
        "logs": {
            "stdout": spec.stdout_log,
            "stderr": spec.stderr_log
        },
        "contents": if dry_run { Some(spec.contents) } else { None }
    }))
}

pub(crate) fn uninstall_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "codex-telegram-bridge")?;
    let output = if dry_run {
        None
    } else {
        Some(run_shell_command(&spec.uninstall_command)?)
    };
    if !dry_run && spec.service_path.exists() {
        fs::remove_file(&spec.service_path)?;
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_uninstall",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "servicePath": spec.service_path,
        "uninstallCommand": spec.uninstall_command,
        "output": output
    }))
}

fn run_shell_command(command: &str) -> Result<Value> {
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .output()
        .with_context(|| format!("failed to run `{command}`"))?;
    Ok(json!({
        "status": output.status.code(),
        "success": output.status.success(),
        "stdout": String::from_utf8_lossy(&output.stdout).trim(),
        "stderr": String::from_utf8_lossy(&output.stderr).trim()
    }))
}

fn macos_service_runtime(output: &Value) -> Value {
    let stdout = output
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stderr = output
        .get("stderr")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let success = output
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !success {
        let not_loaded = stderr.contains("Could not find service")
            || stderr.contains("could not find service")
            || stderr.contains("service not found");
        return json!({
            "loaded": !not_loaded,
            "running": false,
            "state": Value::Null,
            "raw": output
        });
    }

    let state = stdout.lines().find_map(|line| {
        line.trim()
            .strip_prefix("state = ")
            .map(|value| value.trim().to_string())
    });
    let running = matches!(state.as_deref(), Some("running"));
    json!({
        "loaded": true,
        "running": running,
        "state": state,
        "raw": output
    })
}

fn linux_service_runtime(unit_name: &str, status_output: &Value) -> Result<Value> {
    let active_output = run_shell_command(&format!(
        "systemctl --user is-active {}",
        shell_quote(unit_name)
    ))?;
    let active = active_output
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    let loaded = status_output
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || active != "inactive";
    Ok(json!({
        "loaded": loaded,
        "running": active == "active",
        "state": if active.is_empty() { Value::Null } else { json!(active) },
        "raw": {
            "status": status_output,
            "isActive": active_output
        }
    }))
}

fn service_runtime_status(spec: &DaemonServiceSpec) -> Result<Value> {
    if !spec.service_path.exists() {
        return Ok(json!({
            "loaded": false,
            "running": false,
            "state": Value::Null,
            "raw": Value::Null
        }));
    }

    if cfg!(target_os = "macos") {
        let output = run_shell_command(&spec.status_command)?;
        return Ok(macos_service_runtime(&output));
    }

    if cfg!(target_os = "linux") {
        let output = run_shell_command(&spec.status_command)?;
        return linux_service_runtime(&spec.unit_name, &output);
    }

    Ok(json!({
        "loaded": false,
        "running": false,
        "state": Value::Null,
        "raw": Value::Null
    }))
}

pub(crate) fn start_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "codex-telegram-bridge")?;
    let command = spec.start_command.clone();
    let output = if dry_run {
        None
    } else {
        Some(run_shell_command(&command)?)
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_start",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "command": command,
        "output": output
    }))
}

pub(crate) fn stop_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "codex-telegram-bridge")?;
    let command = spec.stop_command.clone();
    let output = if dry_run {
        None
    } else {
        Some(run_shell_command(&command)?)
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_stop",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "command": command,
        "output": output
    }))
}

pub(crate) fn daemon_service_status(label: &str) -> Result<Value> {
    let spec = daemon_service_spec(label, "codex-telegram-bridge")?;
    let config_path = daemon_config_path()?;
    let service_status = service_runtime_status(&spec)?;
    let running = service_status
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let healthy = config_path.exists() && spec.service_path.exists() && running;
    Ok(json!({
        "ok": true,
        "action": "daemon_status",
        "label": spec.unit_name,
        "healthy": healthy,
        "configPath": config_path,
        "configExists": config_path.exists(),
        "servicePath": spec.service_path,
        "serviceExists": spec.service_path.exists(),
        "serviceStatus": service_status,
        "nextStep": if healthy {
            Value::Null
        } else {
            json!("Run `codex-telegram-bridge daemon start` to restore background Telegram delivery.")
        },
        "statusCommand": spec.status_command,
        "logs": {
            "stdout": spec.stdout_log,
            "stderr": spec.stderr_log
        }
    }))
}

pub(crate) fn daemon_service_logs(label: &str) -> Result<Value> {
    let spec = daemon_service_spec(label, "codex-telegram-bridge")?;
    Ok(json!({
        "ok": true,
        "action": "daemon_logs",
        "label": spec.unit_name,
        "stdout": spec.stdout_log,
        "stderr": spec.stderr_log,
        "tailCommand": format!(
            "tail -f {} {}",
            shell_quote(&spec.stdout_log.display().to_string()),
            shell_quote(&spec.stderr_log.display().to_string())
        )
    }))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::set_away_mode;
    use crate::state::{create_state_db_in_memory, pending_outbound_count};
    use std::path::PathBuf;

    struct TempStateDir {
        previous_state_dir: Option<String>,
        root: PathBuf,
    }

    impl TempStateDir {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "codex-telegram-bridge-daemon-{name}-{}",
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

    struct FakeSpawnEnv {
        previous_spawn: Option<String>,
    }

    impl FakeSpawnEnv {
        fn new() -> Self {
            let previous_spawn = std::env::var("CODEX_LIVE_TEST_FAKE_SPAWN").ok();
            std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", "1");
            Self { previous_spawn }
        }
    }

    impl Drop for FakeSpawnEnv {
        fn drop(&mut self) {
            crate::live::terminate_all_test_live_backends();
            if let Some(previous_spawn) = &self.previous_spawn {
                std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", previous_spawn);
            } else {
                std::env::remove_var("CODEX_LIVE_TEST_FAKE_SPAWN");
            }
        }
    }

    #[test]
    fn daemon_install_dry_run_resolves_relative_bridge_command_for_services() {
        let result =
            install_daemon_service(DEFAULT_DAEMON_LABEL, "bin/codex-telegram-bridge", true)
                .expect("daemon install dry run");
        let expected = std::env::current_dir()
            .expect("cwd")
            .join("bin/codex-telegram-bridge")
            .display()
            .to_string();
        assert!(result["contents"]
            .as_str()
            .expect("service contents")
            .contains(&expected));
    }

    #[test]
    fn daemon_service_exports_developer_tool_path() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let previous_home = std::env::var_os("HOME");
        let previous_path = std::env::var_os("PATH");
        let root =
            std::env::temp_dir().join(format!("codex-bridge-daemon-path-{}", std::process::id()));
        let home = root.join("home");
        let nvm_bin = home.join(".config/nvm/versions/node/v24.11.1/bin");
        std::fs::create_dir_all(&nvm_bin).expect("create nvm bin");
        std::env::set_var("HOME", &home);
        std::env::set_var("PATH", "/usr/bin:/bin:/usr/sbin:/sbin");

        let spec =
            daemon_service_spec(DEFAULT_DAEMON_LABEL, "bin/codex-telegram-bridge").expect("spec");

        if let Some(previous) = previous_home {
            std::env::set_var("HOME", previous);
        } else {
            std::env::remove_var("HOME");
        }
        if let Some(previous) = previous_path {
            std::env::set_var("PATH", previous);
        } else {
            std::env::remove_var("PATH");
        }
        let _ = std::fs::remove_dir_all(&root);

        if cfg!(target_os = "macos") {
            assert!(spec.contents.contains("<key>EnvironmentVariables</key>"));
            assert!(spec.contents.contains("<key>PATH</key>"));
            assert!(spec.contents.contains(&nvm_bin.display().to_string()));
            assert!(spec.contents.contains("/opt/homebrew/bin"));
        } else if cfg!(target_os = "linux") {
            assert!(spec.contents.contains("Environment=\"PATH="));
            assert!(spec.contents.contains(&nvm_bin.display().to_string()));
            assert!(spec.contents.contains("/opt/homebrew/bin"));
        }
    }

    #[test]
    fn daemon_notification_policy_only_enqueues_events_while_away() {
        let conn = create_state_db_in_memory().expect("db");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 1500
        });

        let off_count =
            enqueue_daemon_notification_events(&conn, std::slice::from_ref(&event), 2000)
                .expect("away off enqueue");
        assert_eq!(
            off_count, 0,
            "daemon should stay quiet while user is present"
        );

        set_away_mode(&conn, true, 1000).expect("away on");
        let on_count =
            enqueue_daemon_notification_events(&conn, &[event], 2000).expect("away on enqueue");
        assert_eq!(on_count, 1, "daemon should notify while user is away");
    }

    #[test]
    fn daemon_cycle_reports_backend_error_when_shared_backend_is_unreachable() {
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: "thread_error".to_string(),
            telegram: None,
            codex: Some(crate::CodexConfig {
                live_mode: crate::CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:9".to_string(),
            }),
            projects: Vec::new(),
        };

        let result = daemon_cycle(&conn, &config, 1000, Duration::from_millis(10)).expect("cycle");

        assert_eq!(result["action"], "daemon_cycle");
        assert_eq!(result["observed"], 1);
    }

    #[test]
    fn daemon_backend_reconciliation_is_idle_while_remote_mode_is_off() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _state = TempStateDir::new("remote-off");
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: "thread_error".to_string(),
            telegram: None,
            codex: Some(crate::CodexConfig {
                live_mode: crate::CodexLiveMode::Shared,
                websocket_url: "ws://127.0.0.1:9".to_string(),
            }),
            projects: Vec::new(),
        };

        let backend = reconcile_daemon_backend(&conn, &config, 1000);

        assert_eq!(backend, Value::Null);
    }

    #[test]
    fn daemon_backend_reconciliation_starts_backend_while_remote_mode_is_on() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _state = TempStateDir::new("remote-on");
        let _spawn = FakeSpawnEnv::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind random port");
        let websocket_url = format!("ws://{}", listener.local_addr().expect("addr"));
        drop(listener);
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 1000).expect("away on");
        let config = DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: "thread_error".to_string(),
            telegram: None,
            codex: Some(crate::CodexConfig {
                live_mode: crate::CodexLiveMode::Shared,
                websocket_url,
            }),
            projects: Vec::new(),
        };

        let backend = reconcile_daemon_backend(&conn, &config, 2000);

        assert_eq!(backend["ok"], true);
        assert_eq!(backend["status"]["required"], true);
        assert_eq!(backend["status"]["state"], "ready");
    }

    #[test]
    fn daemon_notification_policy_skips_events_before_away_started() {
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 2000).expect("away on");
        let events = vec![
            json!({
                "type": "thread_waiting",
                "threadId": "thr_old",
                "updatedAt": 1500
            }),
            json!({
                "type": "thread_waiting",
                "threadId": "thr_new",
                "updatedAt": 2500
            }),
        ];

        let count = enqueue_daemon_notification_events(&conn, &events, 3000).expect("enqueue");

        assert_eq!(count, 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }

    #[test]
    fn daemon_notification_policy_accepts_codex_second_timestamps() {
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 1_776_219_288_240).expect("away on");
        let events = vec![
            json!({
                "type": "thread_completed",
                "threadId": "thr_old",
                "updatedAt": 1_776_219_200
            }),
            json!({
                "type": "thread_completed",
                "threadId": "thr_new",
                "updatedAt": 1_776_219_396
            }),
        ];

        let count = enqueue_daemon_notification_events(&conn, &events, 1_776_219_397_000)
            .expect("mixed timestamp enqueue");

        assert_eq!(count, 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }

    #[test]
    fn away_off_clears_pending_daemon_notifications() {
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 1000).expect("away on");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 1500
        });
        enqueue_daemon_notification_events(&conn, &[event], 2000).expect("enqueue");
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);

        let disabled = set_away_mode(&conn, false, 2500).expect("away off");

        assert_eq!(disabled["away"], false);
        assert_eq!(disabled["clearedPendingNotifications"], 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 0);
    }

    #[test]
    fn macos_service_runtime_marks_running_services_as_loaded() {
        let runtime = macos_service_runtime(&json!({
            "success": true,
            "stdout": "gui/501/example = {\n\tstate = running\n}\n",
            "stderr": ""
        }));

        assert_eq!(runtime["loaded"], true);
        assert_eq!(runtime["running"], true);
        assert_eq!(runtime["state"], "running");
    }

    #[test]
    fn macos_service_runtime_marks_missing_services_as_not_loaded() {
        let runtime = macos_service_runtime(&json!({
            "success": false,
            "stdout": "",
            "stderr": "Bad request.\nCould not find service \"example\" in domain for user gui: 501"
        }));

        assert_eq!(runtime["loaded"], false);
        assert_eq!(runtime["running"], false);
        assert_eq!(runtime["state"], Value::Null);
    }
}

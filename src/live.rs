use anyhow::{bail, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::codex::{resolve_codex_binary, CodexAppServerClient, CodexBackend};
use crate::config::CodexConfig;
use crate::state::live_backend_status_path;
use crate::ws::validate_shared_websocket_url;

#[allow(dead_code)]
const LIVE_BACKEND_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
#[allow(dead_code)]
const LIVE_BACKEND_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(100);
#[allow(dead_code)]
const LIVE_BACKEND_TERMINATE_TIMEOUT: Duration = Duration::from_secs(3);
#[allow(dead_code)]
const LIVE_BACKEND_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_BACKEND_RECONCILE_BACKOFF_BASE_MS: u64 = 5_000;
const LIVE_BACKEND_RECONCILE_BACKOFF_MAX_MS: u64 = 60_000;

pub(crate) const LIVE_BACKEND_STATE_IDLE: &str = "idle";
pub(crate) const LIVE_BACKEND_STATE_READY: &str = "ready";
pub(crate) const LIVE_BACKEND_STATE_UNHEALTHY: &str = "unhealthy";
pub(crate) const LIVE_BACKEND_STATE_BLOCKED: &str = "blocked";

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LiveBackendStatus {
    pub(crate) websocket_url: String,
    pub(crate) pid: Option<u32>,
    #[serde(default)]
    pub(crate) process_start_key: Option<String>,
    pub(crate) healthy: bool,
    pub(crate) last_error: Option<String>,
    #[serde(default)]
    pub(crate) required: bool,
    #[serde(default)]
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) recoverable: bool,
    #[serde(default)]
    pub(crate) reconcile_attempts: u32,
    #[serde(default)]
    pub(crate) retry_after_ms: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EnsureLiveBackendResult {
    pub(crate) action: String,
    pub(crate) status: LiveBackendStatus,
}

#[allow(dead_code)]
struct LiveBackendLock {
    file: File,
}

impl Drop for LiveBackendLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[allow(dead_code)]
pub(crate) fn live_backend_status(config: &CodexConfig) -> Result<LiveBackendStatus> {
    validate_live_backend_config(config)?;
    let _lock = acquire_live_backend_lock()?;
    live_backend_status_unlocked(config, true)
}

#[allow(dead_code)]
pub(crate) fn live_backend_idle_status(config: &CodexConfig) -> Result<LiveBackendStatus> {
    validate_live_backend_config(config)?;
    let _lock = acquire_live_backend_lock()?;
    live_backend_status_unlocked(config, false)
}

#[allow(dead_code)]
pub(crate) fn ensure_live_backend(config: &CodexConfig) -> Result<EnsureLiveBackendResult> {
    validate_live_backend_config(config)?;
    let _lock = acquire_live_backend_lock()?;
    let status = live_backend_status_unlocked(config, true)?;
    if status.healthy {
        return Ok(EnsureLiveBackendResult {
            action: "reused".to_string(),
            status,
        });
    }

    if let Some(pid) = status.pid {
        terminate_managed_backend_pid(
            pid,
            &config.websocket_url,
            status.process_start_key.as_deref(),
        );
    }

    start_live_backend(config, "started")
}

#[allow(dead_code)]
pub(crate) fn reconcile_live_backend(
    config: &CodexConfig,
    now_ms: u64,
) -> Result<EnsureLiveBackendResult> {
    validate_live_backend_config(config)?;
    let _lock = acquire_live_backend_lock()?;
    let status = live_backend_status_unlocked(config, true)?;
    if status.healthy {
        return Ok(EnsureLiveBackendResult {
            action: "reused".to_string(),
            status,
        });
    }
    if status.state == LIVE_BACKEND_STATE_BLOCKED || !status.recoverable {
        return Ok(EnsureLiveBackendResult {
            action: "blocked".to_string(),
            status,
        });
    }
    if status
        .retry_after_ms
        .is_some_and(|retry_after| retry_after > now_ms)
    {
        return Ok(EnsureLiveBackendResult {
            action: "deferred".to_string(),
            status,
        });
    }

    let attempts = status.reconcile_attempts.saturating_add(1);
    if let Some(pid) = status.pid {
        terminate_managed_backend_pid(
            pid,
            &config.websocket_url,
            status.process_start_key.as_deref(),
        );
    }

    match start_live_backend(config, "started") {
        Ok(mut result) => {
            result.status.reconcile_attempts = 0;
            result.status.retry_after_ms = None;
            write_live_backend_status(&result.status)?;
            Ok(result)
        }
        Err(error) => {
            let retry_after_ms = now_ms.saturating_add(reconcile_backoff_ms(attempts));
            let mut failed =
                read_live_backend_status()?.unwrap_or_else(|| empty_live_backend_status(config));
            set_status_state(
                &mut failed,
                true,
                LIVE_BACKEND_STATE_UNHEALTHY,
                false,
                true,
                Some(format!("{error:#}")),
            );
            failed.reconcile_attempts = attempts;
            failed.retry_after_ms = Some(retry_after_ms);
            write_live_backend_status(&failed)?;
            Ok(EnsureLiveBackendResult {
                action: "deferred".to_string(),
                status: failed,
            })
        }
    }
}

#[allow(dead_code)]
pub(crate) fn reset_live_backend(config: &CodexConfig) -> Result<EnsureLiveBackendResult> {
    validate_live_backend_config(config)?;
    let _lock = acquire_live_backend_lock()?;
    if let Some(mut status) = read_live_backend_status()? {
        backfill_process_start_key(&mut status);
        if let Some(pid) = status.pid {
            terminate_managed_backend_pid(
                pid,
                &status.websocket_url,
                status.process_start_key.as_deref(),
            );
        }
    }
    if let Some(pid) = discover_backend_pid_for_websocket_url(&config.websocket_url) {
        bail!(
            "found codex app-server process {pid} on {} without bridge ownership metadata; refusing to repair it. Use /away to reuse it or stop it locally first.",
            config.websocket_url
        );
    }

    start_live_backend(config, "restarted")
}

pub(crate) fn terminate_recorded_live_backend() -> Result<Option<u32>> {
    let _lock = acquire_live_backend_lock()?;
    let mut recorded_pid = None;
    match read_live_backend_status() {
        Ok(Some(mut status)) => {
            backfill_process_start_key(&mut status);
            recorded_pid = status.pid;
            if let Some(pid) = status.pid {
                terminate_managed_backend_pid(
                    pid,
                    &status.websocket_url,
                    status.process_start_key.as_deref(),
                );
            }
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!(
                "reset: ignoring unreadable live backend status: {error:#}"
            );
        }
    }
    Ok(recorded_pid)
}

#[allow(dead_code)]
fn live_backend_status_unlocked(config: &CodexConfig, required: bool) -> Result<LiveBackendStatus> {
    let previous_status = read_live_backend_status()?;
    let mut status = previous_status
        .clone()
        .unwrap_or_else(|| empty_live_backend_status(config));
    normalize_status_fields(&mut status, required);

    if status.websocket_url != config.websocket_url {
        status = empty_live_backend_status(config);
        normalize_status_fields(&mut status, required);
    }

    backfill_process_start_key(&mut status);

    let recorded_pid_matches = match status.pid {
        Some(pid) => backend_pid_matches(
            pid,
            &config.websocket_url,
            status.process_start_key.as_deref(),
        ),
        None => false,
    };

    if !recorded_pid_matches {
        if let Some(pid) = discover_backend_pid_for_websocket_url(&config.websocket_url) {
            status.pid = Some(pid);
            status.process_start_key = backend_process_start_key(pid);
        } else {
            status.pid = None;
            status.process_start_key = None;
        }
    }

    let managed_pid_matches = match status.pid {
        Some(pid) => backend_pid_matches(
            pid,
            &config.websocket_url,
            status.process_start_key.as_deref(),
        ),
        None => false,
    };
    if !required && !managed_pid_matches {
        set_status_state(
            &mut status,
            false,
            LIVE_BACKEND_STATE_IDLE,
            false,
            true,
            None,
        );
        if previous_status.as_ref() != Some(&status) {
            write_live_backend_status(&status)?;
        }
        return Ok(status);
    }

    match verify_live_backend_health(&config.websocket_url) {
        Ok(()) if managed_pid_matches => {
            set_status_state(
                &mut status,
                required,
                LIVE_BACKEND_STATE_READY,
                true,
                true,
                None,
            );
            status.reconcile_attempts = 0;
            status.retry_after_ms = None;
        }
        Ok(()) => {
            let last_error = Some(match status.pid {
                Some(pid) => format!(
                    "managed live backend process {pid} is not running with the configured websocket URL"
                ),
                None => {
                    "websocket URL answered but no managed codex app-server process was found"
                        .to_string()
                }
            });
            set_status_state(
                &mut status,
                required,
                LIVE_BACKEND_STATE_BLOCKED,
                false,
                false,
                last_error,
            );
        }
        Err(error) => {
            if required {
                set_status_state(
                    &mut status,
                    true,
                    LIVE_BACKEND_STATE_UNHEALTHY,
                    false,
                    true,
                    Some(format!("{error:#}")),
                );
            } else {
                set_status_state(
                    &mut status,
                    false,
                    LIVE_BACKEND_STATE_IDLE,
                    false,
                    true,
                    None,
                );
            }
        }
    }

    if previous_status.as_ref() != Some(&status) {
        write_live_backend_status(&status)?;
    }
    Ok(status)
}

fn empty_live_backend_status(config: &CodexConfig) -> LiveBackendStatus {
    LiveBackendStatus {
        websocket_url: config.websocket_url.clone(),
        pid: None,
        process_start_key: None,
        healthy: false,
        last_error: None,
        required: false,
        state: LIVE_BACKEND_STATE_IDLE.to_string(),
        recoverable: true,
        reconcile_attempts: 0,
        retry_after_ms: None,
    }
}

fn normalize_status_fields(status: &mut LiveBackendStatus, required: bool) {
    status.required = required;
    if status.state.is_empty() {
        status.state = if status.healthy {
            LIVE_BACKEND_STATE_READY
        } else if required {
            LIVE_BACKEND_STATE_UNHEALTHY
        } else {
            LIVE_BACKEND_STATE_IDLE
        }
        .to_string();
    }
    if status.healthy {
        status.recoverable = true;
        status.reconcile_attempts = 0;
        status.retry_after_ms = None;
    }
}

fn set_status_state(
    status: &mut LiveBackendStatus,
    required: bool,
    state: &str,
    healthy: bool,
    recoverable: bool,
    last_error: Option<String>,
) {
    status.required = required;
    status.state = state.to_string();
    status.healthy = healthy;
    status.recoverable = recoverable;
    status.last_error = last_error;
}

fn reconcile_backoff_ms(attempts: u32) -> u64 {
    let exponent = attempts.saturating_sub(1).min(4);
    LIVE_BACKEND_RECONCILE_BACKOFF_BASE_MS
        .saturating_mul(1_u64 << exponent)
        .min(LIVE_BACKEND_RECONCILE_BACKOFF_MAX_MS)
}

fn backfill_process_start_key(status: &mut LiveBackendStatus) {
    if let Some(pid) = status.pid {
        if status.process_start_key.is_none()
            && backend_pid_command_matches(pid, &status.websocket_url)
        {
            status.process_start_key = backend_process_start_key(pid);
        }
    }
}

#[allow(dead_code)]
fn start_live_backend(config: &CodexConfig, action: &str) -> Result<EnsureLiveBackendResult> {
    validate_live_backend_config(config)?;
    let pid = spawn_live_backend_process(config)?;
    let process_start_key = wait_for_backend_process_start_key(pid).with_context(|| {
        format!("managed live backend process {pid} did not expose a stable process start key")
    })?;

    let status = wait_for_live_backend(config, pid, &process_start_key).with_context(|| {
        format!(
            "managed live backend failed to become healthy at {}",
            config.websocket_url
        )
    })?;

    Ok(EnsureLiveBackendResult {
        action: action.to_string(),
        status,
    })
}

#[allow(dead_code)]
fn validate_live_backend_config(config: &CodexConfig) -> Result<()> {
    validate_shared_websocket_url(&config.websocket_url)?;
    Ok(())
}

#[allow(dead_code)]
fn wait_for_live_backend(
    config: &CodexConfig,
    pid: u32,
    process_start_key: &str,
) -> Result<LiveBackendStatus> {
    let started = Instant::now();
    let mut last_error = None;

    while started.elapsed() < LIVE_BACKEND_HEALTH_TIMEOUT {
        if !backend_pid_matches(pid, &config.websocket_url, Some(process_start_key)) {
            if !backend_pid_is_alive(pid) {
                last_error = Some(format!(
                    "managed live backend process {pid} exited before becoming healthy with the configured websocket URL"
                ));
                break;
            }
            last_error = Some(format!(
                "managed live backend process {pid} is still starting and has not exposed the configured websocket command yet"
            ));
            thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
            continue;
        }

        match verify_live_backend_health(&config.websocket_url) {
            Ok(()) => {
                let status = LiveBackendStatus {
                    websocket_url: config.websocket_url.clone(),
                    pid: Some(pid),
                    process_start_key: Some(process_start_key.to_string()),
                    healthy: true,
                    last_error: None,
                    required: true,
                    state: LIVE_BACKEND_STATE_READY.to_string(),
                    recoverable: true,
                    reconcile_attempts: 0,
                    retry_after_ms: None,
                };
                write_live_backend_status(&status)?;
                return Ok(status);
            }
            Err(error) => {
                last_error = Some(format!("{error:#}"));
                thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
            }
        }
    }

    terminate_managed_backend_pid(pid, &config.websocket_url, Some(process_start_key));
    let status = LiveBackendStatus {
        websocket_url: config.websocket_url.clone(),
        pid: Some(pid),
        process_start_key: Some(process_start_key.to_string()),
        healthy: false,
        last_error,
        required: true,
        state: LIVE_BACKEND_STATE_UNHEALTHY.to_string(),
        recoverable: true,
        reconcile_attempts: 0,
        retry_after_ms: None,
    };
    write_live_backend_status(&status)?;

    let message = status
        .last_error
        .clone()
        .unwrap_or_else(|| "unknown live backend startup failure".to_string());
    bail!("{message}");
}

#[allow(dead_code)]
fn live_backend_lock_path() -> Result<PathBuf> {
    Ok(live_backend_status_path()?.with_file_name("live-backend.lock"))
}

#[allow(dead_code)]
fn acquire_live_backend_lock() -> Result<LiveBackendLock> {
    let path = live_backend_lock_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open live backend lock at {}", path.display()))?;
    let started = Instant::now();
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(LiveBackendLock { file }),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LIVE_BACKEND_LOCK_TIMEOUT {
                    bail!(
                        "live backend state is locked at {}; try again shortly",
                        path.display()
                    );
                }
                thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to create live backend lock at {}", path.display())
                });
            }
        }
    }
}

#[allow(dead_code)]
fn verify_live_backend_health(websocket_url: &str) -> Result<()> {
    let _client = CodexAppServerClient::connect_with_backend(CodexBackend::SharedWebsocket {
        url: websocket_url.to_string(),
    })?;
    Ok(())
}

#[allow(dead_code)]
fn read_live_backend_status() -> Result<Option<LiveBackendStatus>> {
    let path = live_backend_status_path()?;
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read live backend status at {}", path.display()))?;
    let status = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse live backend status at {}", path.display()))?;
    Ok(Some(status))
}

#[allow(dead_code)]
fn write_live_backend_status(status: &LiveBackendStatus) -> Result<()> {
    let path = live_backend_status_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("live backend status path missing file name")?;
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));
    fs::write(&tmp_path, serde_json::to_vec_pretty(status)?)?;
    fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "failed to replace live backend status file {}",
            path.display()
        )
    })?;
    Ok(())
}

#[allow(dead_code)]
fn spawn_live_backend_process(config: &CodexConfig) -> Result<u32> {
    #[cfg(test)]
    if let Some(pid) = spawn_test_live_backend(&config.websocket_url)? {
        return Ok(pid);
    }

    let resolved = resolve_codex_binary()?;

    #[cfg(unix)]
    {
        if unix_command_exists("screen") {
            return spawn_live_backend_with_screen(config, &resolved.path);
        }

        let output = Command::new("sh")
            .arg("-c")
            .arg(live_backend_spawn_shell_script())
            .arg("codex-live-backend")
            .arg(&resolved.path)
            .arg(&config.websocket_url)
            .output()
            .with_context(|| format!("failed to spawn {} app-server", resolved.path.display()))?;

        if !output.status.success() {
            bail!(
                "failed to spawn {} app-server: {}",
                resolved.path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }

        let pid = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u32>()
            .with_context(|| {
                format!(
                    "failed to parse spawned app-server pid from {}",
                    String::from_utf8_lossy(&output.stdout).trim()
                )
            })?;
        Ok(pid)
    }

    #[cfg(not(unix))]
    {
        let child = Command::new(&resolved.path)
            .arg("app-server")
            .arg("--listen")
            .arg(&config.websocket_url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to spawn {} app-server", resolved.path.display()))?;

        Ok(child.id())
    }
}

#[cfg(unix)]
fn unix_command_exists(command: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg("command -v \"$1\" >/dev/null 2>&1")
        .arg("codex-command-exists")
        .arg(command)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(unix)]
fn spawn_live_backend_with_screen(config: &CodexConfig, binary: &Path) -> Result<u32> {
    let session_name = live_backend_screen_session_name(&config.websocket_url);
    let status = Command::new("screen")
        .arg("-dmS")
        .arg(&session_name)
        .arg(binary)
        .arg("app-server")
        .arg("--listen")
        .arg(&config.websocket_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("failed to launch screen session {session_name}"))?;

    if !status.success() {
        bail!("screen failed to launch live backend session {session_name}: {status}");
    }

    let started = Instant::now();
    while started.elapsed() < LIVE_BACKEND_HEALTH_TIMEOUT {
        if let Some(pid) = discover_backend_pid_for_websocket_url(&config.websocket_url) {
            return Ok(pid);
        }
        thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
    }

    bail!(
        "screen launched session {session_name}, but no codex app-server process appeared for {}",
        config.websocket_url
    )
}

#[cfg(unix)]
fn live_backend_screen_session_name(websocket_url: &str) -> String {
    let mut safe = websocket_url
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    safe.truncate(48);
    format!("codex-bridge-live-{safe}")
}

#[cfg(unix)]
fn live_backend_spawn_shell_script() -> &'static str {
    r#"
binary=$1
url=$2
# codex app-server exits when stdin is EOF and when its launcher disappears.
# Keep stdin open and leave a tiny supervisor waiting while returning the app-server pid.
(
  tail -f /dev/null | "$binary" app-server --listen "$url" >/dev/null 2>&1 &
  child=$!
  printf '%s\n' "$child" >&3
  exec 3>&-
  wait "$child"
) 3>&1 >/dev/null 2>&1 &
"#
}

#[allow(dead_code)]
fn terminate_managed_backend_pid(pid: u32, websocket_url: &str, process_start_key: Option<&str>) {
    let Some(process_start_key) = process_start_key else {
        return;
    };

    #[cfg(test)]
    if test_fake_spawn_enabled() {
        if backend_pid_matches(pid, websocket_url, Some(process_start_key)) {
            let _ = terminate_test_live_backend(pid);
        }
        return;
    }

    if !backend_pid_matches(pid, websocket_url, Some(process_start_key)) {
        return;
    }

    send_backend_termination_signal(pid, false);
    wait_for_managed_backend_pid_exit(
        pid,
        websocket_url,
        Some(process_start_key),
        LIVE_BACKEND_TERMINATE_TIMEOUT,
    );

    if backend_pid_matches(pid, websocket_url, Some(process_start_key)) {
        send_backend_termination_signal(pid, true);
        wait_for_managed_backend_pid_exit(
            pid,
            websocket_url,
            Some(process_start_key),
            LIVE_BACKEND_TERMINATE_TIMEOUT,
        );
    }
}

#[allow(dead_code)]
fn terminate_backend_pid(pid: u32) {
    #[cfg(test)]
    if terminate_test_live_backend(pid) {
        return;
    }

    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return;
    }

    #[cfg(unix)]
    {
        send_backend_termination_signal(pid, false);
        wait_for_backend_pid_exit(pid, LIVE_BACKEND_TERMINATE_TIMEOUT);
        if backend_pid_is_alive(pid) {
            send_backend_termination_signal(pid, true);
            wait_for_backend_pid_exit(pid, LIVE_BACKEND_TERMINATE_TIMEOUT);
        }
    }

    #[cfg(windows)]
    {
        send_backend_termination_signal(pid, true);
        wait_for_backend_pid_exit(pid, LIVE_BACKEND_TERMINATE_TIMEOUT);
    }
}

#[allow(dead_code)]
fn send_backend_termination_signal(pid: u32, force: bool) {
    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return;
    }

    #[cfg(unix)]
    {
        let signal = if force { "-KILL" } else { "-TERM" };
        let _ = Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    #[cfg(windows)]
    {
        let mut command = Command::new("taskkill");
        command.args(["/PID", &pid.to_string(), "/T"]);
        if force {
            command.arg("/F");
        }
        let _ = command.stdout(Stdio::null()).stderr(Stdio::null()).status();
    }
}

#[allow(dead_code)]
fn discover_backend_pid_for_websocket_url(websocket_url: &str) -> Option<u32> {
    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return test_backend_registry()
            .lock()
            .expect("test backend registry lock")
            .iter()
            .find_map(|(pid, handle)| (handle.websocket_url == websocket_url).then_some(*pid));
    }

    #[cfg(unix)]
    {
        let output = Command::new("ps")
            .args(["ax", "-o", "pid=,comm=,command="])
            .output();
        return output
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| {
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .find_map(|line| {
                        let trimmed = line.trim_start();
                        let mut parts = trimmed.split_whitespace();
                        let pid = parts.next()?;
                        let process_name = parts.next()?.trim();
                        let command = parts.collect::<Vec<_>>().join(" ");
                        if !is_unix_shell_wrapper_process(process_name)
                            && command_line_matches_backend(&command, websocket_url)
                        {
                            pid.parse::<u32>().ok()
                        } else {
                            None
                        }
                    })
            });
    }

    #[cfg(windows)]
    {
        let script = r#"
Get-CimInstance Win32_Process | ForEach-Object {
  if ($_.CommandLine) {
    "$($_.ProcessId)`t$($_.CommandLine)"
  }
}
"#;
        let output = Command::new("powershell")
            .args(["-NoProfile", "-Command", script])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        return String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|line| {
                let (pid, command) = line.split_once('\t')?;
                if command_line_matches_backend(command, websocket_url) {
                    pid.trim().parse::<u32>().ok()
                } else {
                    None
                }
            });
    }

    #[allow(unreachable_code)]
    None
}

#[allow(dead_code)]
fn backend_pid_matches(pid: u32, websocket_url: &str, process_start_key: Option<&str>) -> bool {
    if !backend_pid_is_alive(pid) {
        return false;
    }

    let command_matches = backend_pid_command_matches(pid, websocket_url);
    if !command_matches {
        return false;
    }

    match process_start_key {
        Some(expected) => backend_process_start_key(pid).as_deref() == Some(expected),
        None => true,
    }
}

#[allow(dead_code)]
fn backend_pid_command_matches(pid: u32, websocket_url: &str) -> bool {
    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return test_backend_registry()
            .lock()
            .expect("test backend registry lock")
            .get(&pid)
            .map(|handle| handle.websocket_url == websocket_url)
            .unwrap_or(false);
    }

    #[cfg(unix)]
    {
        if unix_process_name(pid)
            .as_deref()
            .map(is_unix_shell_wrapper_process)
            .unwrap_or(true)
        {
            return false;
        }

        let pid_arg = pid.to_string();
        let output = Command::new("ps")
            .args(["-p", &pid_arg, "-o", "command="])
            .output();
        return output
            .ok()
            .filter(|output| output.status.success())
            .map(|output| {
                let command = String::from_utf8_lossy(&output.stdout);
                command_line_matches_backend(&command, websocket_url)
            })
            .unwrap_or(false);
    }

    #[cfg(windows)]
    {
        return windows_process_command_line(pid)
            .as_deref()
            .map(|command| command_line_matches_backend(command, websocket_url))
            .unwrap_or(false);
    }

    #[allow(unreachable_code)]
    false
}

#[cfg(unix)]
fn unix_process_name(pid: u32) -> Option<String> {
    let pid_arg = pid.to_string();
    let output = Command::new("ps")
        .args(["-p", &pid_arg, "-o", "comm="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let process_name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!process_name.is_empty()).then_some(process_name)
}

#[cfg(unix)]
fn is_unix_shell_wrapper_process(process_name: &str) -> bool {
    matches!(
        process_name,
        "sh" | "bash" | "zsh" | "fish" | "nohup" | "SCREEN" | "screen" | "login"
    )
}

#[allow(dead_code)]
fn backend_process_start_key(pid: u32) -> Option<String> {
    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return test_backend_registry()
            .lock()
            .expect("test backend registry lock")
            .get(&pid)
            .map(|handle| handle.process_start_key.clone());
    }

    #[cfg(unix)]
    {
        let pid_arg = pid.to_string();
        let output = Command::new("ps")
            .args(["-p", &pid_arg, "-o", "lstart="])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let started = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return (!started.is_empty()).then_some(started);
    }

    #[cfg(windows)]
    {
        return windows_process_start_key(pid);
    }

    #[allow(unreachable_code)]
    None
}

#[allow(dead_code)]
fn wait_for_backend_process_start_key(pid: u32) -> Result<String> {
    let started = Instant::now();
    while started.elapsed() < LIVE_BACKEND_HEALTH_TIMEOUT {
        if let Some(process_start_key) = backend_process_start_key(pid) {
            return Ok(process_start_key);
        }
        if !backend_pid_is_alive(pid) {
            bail!("managed live backend process {pid} exited before start key was available");
        }
        thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
    }

    bail!("timed out waiting for managed live backend process {pid} start key")
}

#[allow(dead_code)]
fn command_line_matches_backend(command: &str, websocket_url: &str) -> bool {
    let args = command.split_whitespace().collect::<Vec<_>>();
    let Some(app_server_index) = args.iter().position(|arg| *arg == "app-server") else {
        return false;
    };
    if !command_line_has_codex_executable(&args[..app_server_index]) {
        return false;
    }

    args.windows(2)
        .any(|window| window[0] == "--listen" && window[1] == websocket_url)
        || args.iter().any(|arg| {
            arg.strip_prefix("--listen=")
                .map(|value| value == websocket_url)
                .unwrap_or(false)
        })
}

fn command_line_has_codex_executable(args_before_app_server: &[&str]) -> bool {
    args_before_app_server.iter().any(|arg| {
        let executable = arg.rsplit(['/', '\\']).next().unwrap_or(arg);
        executable == "codex" || executable.starts_with("codex-")
    })
}

#[cfg(windows)]
fn windows_process_command_line(pid: u32) -> Option<String> {
    let command =
        format!("(Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\").CommandLine");
    let output = Command::new("powershell")
        .args(["-NoProfile", "-Command", &command])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let command_line = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!command_line.is_empty()).then_some(command_line)
}

#[cfg(windows)]
fn windows_process_start_key(pid: u32) -> Option<String> {
    let command =
        format!("(Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\").CreationDate");
    let output = Command::new("powershell")
        .args(["-NoProfile", "-Command", &command])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let started = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!started.is_empty()).then_some(started)
}

#[cfg(windows)]
fn windows_tasklist_contains_pid(stdout: &[u8], pid: u32) -> bool {
    let pid = pid.to_string();
    String::from_utf8_lossy(stdout).lines().any(|line| {
        let fields = line
            .trim()
            .trim_matches('"')
            .split("\",\"")
            .collect::<Vec<_>>();
        fields.get(1).copied() == Some(pid.as_str())
    })
}

#[allow(dead_code)]
fn wait_for_backend_pid_exit(pid: u32, timeout: Duration) {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if !backend_pid_is_alive(pid) {
            return;
        }
        thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
    }
}

#[allow(dead_code)]
fn wait_for_managed_backend_pid_exit(
    pid: u32,
    websocket_url: &str,
    process_start_key: Option<&str>,
    timeout: Duration,
) {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if !backend_pid_is_alive(pid) {
            return;
        }
        if let Some(expected) = process_start_key {
            match backend_process_start_key(pid) {
                Some(current) if current != expected => return,
                None => {}
                _ => {}
            }
        }
        if !backend_pid_command_matches(pid, websocket_url) {
            thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
            continue;
        }
        thread::sleep(LIVE_BACKEND_HEALTH_POLL_INTERVAL);
    }
}

#[allow(dead_code)]
fn backend_pid_is_alive(pid: u32) -> bool {
    #[cfg(test)]
    if test_fake_spawn_enabled() {
        return test_backend_registry()
            .lock()
            .expect("test backend registry lock")
            .contains_key(&pid);
    }

    #[cfg(unix)]
    {
        return Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
    }

    #[cfg(windows)]
    {
        let filter = format!("PID eq {pid}");
        return Command::new("tasklist")
            .args(["/FI", &filter, "/FO", "CSV", "/NH"])
            .output()
            .map(|output| {
                output.status.success() && windows_tasklist_contains_pid(&output.stdout, pid)
            })
            .unwrap_or(false);
    }

    #[allow(unreachable_code)]
    false
}

#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::net::{TcpListener, TcpStream};
#[cfg(test)]
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(test)]
use std::sync::mpsc::{self, Sender, TryRecvError};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
#[cfg(test)]
use std::thread::JoinHandle;
#[cfg(test)]
use tungstenite::Message;
#[cfg(test)]
use url::Url;

#[cfg(test)]
struct TestBackendHandle {
    websocket_url: String,
    process_start_key: String,
    address: String,
    stop_tx: Sender<()>,
    join: JoinHandle<()>,
}

#[cfg(test)]
fn test_backend_registry() -> &'static Mutex<BTreeMap<u32, TestBackendHandle>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<u32, TestBackendHandle>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn next_test_backend_pid() -> u32 {
    static NEXT_PID: AtomicU32 = AtomicU32::new(50_000);
    NEXT_PID.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
fn test_fake_spawn_enabled() -> bool {
    std::env::var("CODEX_LIVE_TEST_FAKE_SPAWN").ok().as_deref() == Some("1")
}

#[cfg(test)]
fn spawn_test_live_backend(websocket_url: &str) -> Result<Option<u32>> {
    if !test_fake_spawn_enabled() {
        return Ok(None);
    }

    let parsed = Url::parse(websocket_url)
        .with_context(|| format!("invalid websocket url for test backend: {websocket_url}"))?;
    let host = parsed
        .host_str()
        .context("test websocket url missing host")?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .context("test websocket url missing port")?;
    let address = format!("{host}:{port}");
    let listener = TcpListener::bind(&address)
        .with_context(|| format!("bind fake live backend at {address}"))?;

    let (stop_tx, stop_rx) = mpsc::channel();
    let join = std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            match stop_rx.try_recv() {
                Ok(()) | Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {}
            }

            let mut socket = match tungstenite::accept(stream) {
                Ok(socket) => socket,
                Err(_) => continue,
            };

            let initialize = match socket.read() {
                Ok(message) => message,
                Err(_) => continue,
            };
            let initialize = match initialize.into_text() {
                Ok(text) => text,
                Err(_) => continue,
            };
            let initialize: serde_json::Value = match serde_json::from_str(&initialize) {
                Ok(value) => value,
                Err(_) => continue,
            };

            let _ = socket.send(Message::text(
                serde_json::to_string(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": initialize["id"],
                    "result": {
                        "protocolVersion": 1,
                        "serverInfo": { "name": "fake-codex", "version": "test" }
                    }
                }))
                .expect("serialize initialize response"),
            ));

            let _ = socket
                .get_mut()
                .set_read_timeout(Some(Duration::from_millis(200)));
            let _ = socket.read();
        }
    });

    let pid = next_test_backend_pid();
    test_backend_registry()
        .lock()
        .expect("test backend registry lock")
        .insert(
            pid,
            TestBackendHandle {
                websocket_url: websocket_url.to_string(),
                process_start_key: format!("test-backend-{pid}"),
                address: address.clone(),
                stop_tx,
                join,
            },
        );
    Ok(Some(pid))
}

#[cfg(test)]
fn terminate_test_live_backend(pid: u32) -> bool {
    let handle = test_backend_registry()
        .lock()
        .expect("test backend registry lock")
        .remove(&pid);
    if let Some(handle) = handle {
        let _ = handle.stop_tx.send(());
        let _ = TcpStream::connect(&handle.address);
        let _ = handle.join.join();
        true
    } else {
        false
    }
}

#[cfg(test)]
pub(crate) fn terminate_all_test_live_backends() {
    let pids = test_backend_registry()
        .lock()
        .expect("test backend registry lock")
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for pid in pids {
        let _ = terminate_test_live_backend(pid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct TempHome {
        previous_state_dir: Option<String>,
        root: PathBuf,
    }

    impl TempHome {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "codex-live-test-{name}-{}-{}",
                std::process::id(),
                next_test_backend_pid()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create temp state dir");
            let previous_state_dir = std::env::var("CODEX_TELEGRAM_BRIDGE_STATE_DIR").ok();
            std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", &root);
            Self {
                previous_state_dir,
                root,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    struct LiveTestEnv {
        previous_spawn: Option<String>,
    }

    impl LiveTestEnv {
        fn fake_spawn() -> Self {
            let previous_spawn = std::env::var("CODEX_LIVE_TEST_FAKE_SPAWN").ok();
            std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", "1");
            Self { previous_spawn }
        }
    }

    impl Drop for LiveTestEnv {
        fn drop(&mut self) {
            if let Some(previous_spawn) = &self.previous_spawn {
                std::env::set_var("CODEX_LIVE_TEST_FAKE_SPAWN", previous_spawn);
            } else {
                std::env::remove_var("CODEX_LIVE_TEST_FAKE_SPAWN");
            }
        }
    }

    fn live_test_lock() -> &'static Mutex<()> {
        crate::state::test_env_lock()
    }

    fn shared_codex_config(websocket_url: &str) -> CodexConfig {
        CodexConfig {
            live_mode: crate::CodexLiveMode::Shared,
            websocket_url: websocket_url.to_string(),
        }
    }

    fn random_websocket_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind random port");
        let address = listener.local_addr().expect("local addr");
        drop(listener);
        format!("ws://{address}")
    }

    fn wait_until_healthy(websocket_url: &str) {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            if verify_live_backend_health(websocket_url).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("fake live backend did not become healthy at {websocket_url}");
    }

    #[test]
    fn optional_status_reports_idle_without_health_error_when_backend_is_not_required() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("optional-idle");
        let websocket_url = random_websocket_url();

        let status =
            live_backend_idle_status(&shared_codex_config(&websocket_url)).expect("idle status");

        assert_eq!(status.websocket_url, websocket_url);
        assert_eq!(status.state, LIVE_BACKEND_STATE_IDLE);
        assert!(!status.required);
        assert!(!status.healthy);
        assert!(status.recoverable);
        assert_eq!(status.last_error, None);
    }

    #[test]
    fn optional_status_clears_stale_pid_when_backend_is_not_required() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("optional-stale-pid");
        let websocket_url = random_websocket_url();
        let mut stale = empty_live_backend_status(&shared_codex_config(&websocket_url));
        stale.pid = Some(77_777);
        stale.process_start_key = Some("dead-test-process".to_string());
        write_live_backend_status(&stale).expect("write stale status");

        let status =
            live_backend_idle_status(&shared_codex_config(&websocket_url)).expect("idle status");

        assert_eq!(status.state, LIVE_BACKEND_STATE_IDLE);
        assert_eq!(status.pid, None);
        assert_eq!(status.process_start_key, None);
        assert_eq!(status.last_error, None);
    }

    #[test]
    fn required_status_reports_unhealthy_when_backend_is_missing() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("required-unhealthy");
        let websocket_url = random_websocket_url();

        let status = live_backend_status(&shared_codex_config(&websocket_url)).expect("status");

        assert_eq!(status.state, LIVE_BACKEND_STATE_UNHEALTHY);
        assert!(status.required);
        assert!(!status.healthy);
        assert!(status.recoverable);
        assert!(status.last_error.is_some());
    }

    #[test]
    fn reconcile_live_backend_defers_until_retry_time() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reconcile-defer");
        let websocket_url = random_websocket_url();
        let mut status = empty_live_backend_status(&shared_codex_config(&websocket_url));
        set_status_state(
            &mut status,
            true,
            LIVE_BACKEND_STATE_UNHEALTHY,
            false,
            true,
            Some("previous startup failed".to_string()),
        );
        status.reconcile_attempts = 2;
        status.retry_after_ms = Some(10_000);
        write_live_backend_status(&status).expect("write deferred status");

        let result =
            reconcile_live_backend(&shared_codex_config(&websocket_url), 5_000).expect("reconcile");

        assert_eq!(result.action, "deferred");
        assert_eq!(result.status.retry_after_ms, Some(10_000));
        assert_eq!(result.status.reconcile_attempts, 2);
    }

    #[cfg(unix)]
    #[test]
    fn live_backend_screen_session_name_is_safe() {
        let session_name = live_backend_screen_session_name("ws://127.0.0.1:4500");

        assert!(session_name.starts_with("codex-bridge-live-ws---127-0-0-1-4500"));
        assert!(session_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-'));
        assert!(session_name.len() <= "codex-bridge-live-".len() + 48);
    }

    #[cfg(unix)]
    #[test]
    fn live_backend_fallback_spawn_script_keeps_app_server_alive() {
        let script = live_backend_spawn_shell_script();

        assert!(
            script.contains(r#"tail -f /dev/null | "$binary" app-server --listen "$url""#),
            "app-server stdin must stay open after the launcher shell exits"
        );
        assert!(
            script.contains(r#"wait "$child""#),
            "a supervisor must stay alive while app-server is running"
        );
        assert!(
            !script.contains("</dev/null"),
            "redirecting app-server stdin from /dev/null makes it exit immediately"
        );
    }

    #[test]
    fn ensure_live_backend_reuses_healthy_backend() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reuse");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);
        let initial = LiveBackendStatus {
            websocket_url: websocket_url.clone(),
            pid: Some(pid),
            process_start_key: None,
            healthy: true,
            last_error: None,
            required: true,
            state: LIVE_BACKEND_STATE_READY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        };
        write_live_backend_status(&initial).expect("write initial status");

        let result = ensure_live_backend(&shared_codex_config(&websocket_url)).expect("ensure");

        assert_eq!(result.action, "reused");
        assert_eq!(result.status.pid, Some(pid));
        assert_eq!(
            result.status.process_start_key,
            backend_process_start_key(pid)
        );
        assert!(result.status.healthy);

        terminate_backend_pid(pid);
    }

    #[test]
    fn reset_live_backend_restarts_unhealthy_backend() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reset");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        write_live_backend_status(&LiveBackendStatus {
            websocket_url: websocket_url.clone(),
            pid: Some(41_242),
            process_start_key: Some("stale-test-process".to_string()),
            healthy: false,
            last_error: Some("socket closed".to_string()),
            required: true,
            state: LIVE_BACKEND_STATE_UNHEALTHY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        })
        .expect("write unhealthy status");

        let result = reset_live_backend(&shared_codex_config(&websocket_url)).expect("reset");

        assert_eq!(result.action, "restarted");
        assert!(result.status.healthy);
        assert!(result.status.pid.is_some());
        assert_eq!(result.status.websocket_url, websocket_url);
        assert_eq!(result.status.last_error, None);

        let persisted = read_live_backend_status()
            .expect("read status")
            .expect("persisted status");
        assert_eq!(persisted, result.status);

        terminate_backend_pid(result.status.pid.expect("pid"));
    }

    #[test]
    fn wait_for_live_backend_rejects_dead_managed_pid_even_if_socket_is_healthy() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("dead-pid");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let healthy_pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);
        let dead_pid = healthy_pid + 1;

        let error = wait_for_live_backend(
            &shared_codex_config(&websocket_url),
            dead_pid,
            "dead-process",
        )
        .expect_err("dead managed pid should fail");

        assert!(format!("{error:#}").contains("exited before becoming healthy"));
        terminate_backend_pid(healthy_pid);
    }

    #[test]
    fn live_backend_status_replaces_stale_pid_with_discovered_backend() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("status-dead-pid");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let healthy_pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);
        let dead_pid = healthy_pid + 1;
        write_live_backend_status(&LiveBackendStatus {
            websocket_url: websocket_url.clone(),
            pid: Some(dead_pid),
            process_start_key: Some("dead-test-process".to_string()),
            healthy: true,
            last_error: None,
            required: true,
            state: LIVE_BACKEND_STATE_READY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        })
        .expect("write stale status");

        let status = live_backend_status(&shared_codex_config(&websocket_url)).expect("status");

        assert!(status.healthy);
        assert_eq!(status.pid, Some(healthy_pid));
        assert_eq!(
            status.process_start_key,
            backend_process_start_key(healthy_pid)
        );
        assert_eq!(status.last_error, None);
        terminate_backend_pid(healthy_pid);
    }

    #[test]
    fn live_backend_status_adopts_managed_process_when_status_file_is_missing() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("status-adopt");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);

        let status = live_backend_status(&shared_codex_config(&websocket_url)).expect("status");

        assert!(status.healthy);
        assert_eq!(status.pid, Some(pid));
        assert_eq!(status.process_start_key, backend_process_start_key(pid));
        terminate_backend_pid(pid);
    }

    #[test]
    fn terminate_managed_backend_pid_requires_matching_process_start_key() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("terminate-start-key");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);

        terminate_managed_backend_pid(pid, &websocket_url, Some("wrong-start-key"));
        assert!(backend_pid_matches(pid, &websocket_url, None));

        terminate_managed_backend_pid(pid, &websocket_url, None);
        assert!(backend_pid_matches(pid, &websocket_url, None));
        terminate_backend_pid(pid);
    }

    #[test]
    fn reset_live_backend_backfills_legacy_status_before_terminating() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reset-legacy-start-key");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let old_pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);
        write_live_backend_status(&LiveBackendStatus {
            websocket_url: websocket_url.clone(),
            pid: Some(old_pid),
            process_start_key: None,
            healthy: true,
            last_error: None,
            required: true,
            state: LIVE_BACKEND_STATE_READY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        })
        .expect("write legacy status");

        let result = reset_live_backend(&shared_codex_config(&websocket_url)).expect("reset");

        assert_eq!(result.action, "restarted");
        assert_ne!(result.status.pid, Some(old_pid));
        assert!(!backend_pid_is_alive(old_pid));
        assert!(result.status.process_start_key.is_some());
        if let Some(pid) = result.status.pid {
            terminate_backend_pid(pid);
        }
    }

    #[test]
    fn wait_for_managed_backend_pid_exit_stops_on_process_identity_mismatch() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("wait-start-key");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);

        let started = Instant::now();
        wait_for_managed_backend_pid_exit(
            pid,
            &websocket_url,
            Some("wrong-start-key"),
            Duration::from_secs(1),
        );

        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(backend_pid_matches(pid, &websocket_url, None));
        terminate_backend_pid(pid);
    }

    #[test]
    fn command_line_matching_requires_exact_listen_url_argument() {
        let websocket_url = "ws://127.0.0.1:4500";

        assert!(command_line_matches_backend(
            "/usr/local/bin/codex app-server --listen ws://127.0.0.1:4500",
            websocket_url
        ));
        assert!(command_line_matches_backend(
            "/usr/local/bin/codex app-server --listen=ws://127.0.0.1:4500",
            websocket_url
        ));
        assert!(!command_line_matches_backend(
            "/usr/local/bin/not-codex app-server --listen ws://127.0.0.1:4500",
            websocket_url
        ));
        assert!(!command_line_matches_backend(
            "/usr/local/bin/codex app-server --listen ws://127.0.0.1:45001",
            websocket_url
        ));
        assert!(!command_line_matches_backend(
            "/usr/local/bin/codex app-server --listen ws://127.0.0.1:4500-extra",
            websocket_url
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unix_shell_wrapper_processes_are_not_backend_processes() {
        assert!(is_unix_shell_wrapper_process("sh"));
        assert!(is_unix_shell_wrapper_process("nohup"));
        assert!(is_unix_shell_wrapper_process("SCREEN"));
        assert!(is_unix_shell_wrapper_process("login"));
        assert!(!is_unix_shell_wrapper_process("codex"));
    }

    #[test]
    fn reset_live_backend_does_not_terminate_pid_that_does_not_match_recorded_url() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reset-stale-pid");
        let current_url = random_websocket_url();
        let stale_recorded_url = random_websocket_url();
        let reset_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let current_pid = spawn_test_live_backend(&current_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&current_url);
        write_live_backend_status(&LiveBackendStatus {
            websocket_url: stale_recorded_url,
            pid: Some(current_pid),
            process_start_key: backend_process_start_key(current_pid),
            healthy: false,
            last_error: Some("stale pid".to_string()),
            required: true,
            state: LIVE_BACKEND_STATE_UNHEALTHY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        })
        .expect("write stale status");

        let result = reset_live_backend(&shared_codex_config(&reset_url)).expect("reset");

        assert!(backend_pid_matches(current_pid, &current_url, None));
        assert!(result.status.healthy);
        assert_ne!(result.status.pid, Some(current_pid));
        terminate_backend_pid(current_pid);
        terminate_backend_pid(result.status.pid.expect("reset pid"));
    }

    #[test]
    fn reset_live_backend_refuses_unowned_backend_when_status_file_is_missing() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reset-discover");
        let websocket_url = random_websocket_url();
        let _env = LiveTestEnv::fake_spawn();

        let old_pid = spawn_test_live_backend(&websocket_url)
            .expect("spawn test backend")
            .expect("test backend pid");
        wait_until_healthy(&websocket_url);

        let error =
            reset_live_backend(&shared_codex_config(&websocket_url)).expect_err("reset refusal");

        assert!(format!("{error:#}").contains("without bridge ownership metadata"));
        assert!(backend_pid_is_alive(old_pid));
        terminate_backend_pid(old_pid);
    }

    #[test]
    fn ensure_live_backend_rejects_non_loopback_url_before_spawning() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("reject-non-loopback");
        let _env = LiveTestEnv::fake_spawn();
        let before_count = test_backend_registry()
            .lock()
            .expect("test backend registry lock")
            .len();

        let error = ensure_live_backend(&shared_codex_config("ws://example.com:4500"))
            .expect_err("non-loopback URL should be rejected");

        assert!(
            format!("{error:#}")
                .contains("only loopback ws:// shared websocket URLs are supported"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            test_backend_registry()
                .lock()
                .expect("test backend registry lock")
                .len(),
            before_count
        );
    }

    #[test]
    fn live_backend_lock_is_released_on_drop() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("lock-release");

        let lock = acquire_live_backend_lock().expect("acquire lock");
        let path = live_backend_lock_path().expect("lock path");
        assert!(path.exists());

        drop(lock);

        let _second_lock = acquire_live_backend_lock().expect("reacquire lock");
    }

    #[test]
    fn write_live_backend_status_replaces_existing_status_without_tmp_leftover() {
        let _guard = live_test_lock().lock().expect("live test lock");
        let _home = TempHome::new("atomic-status");
        let websocket_url = random_websocket_url();
        let initial = LiveBackendStatus {
            websocket_url: websocket_url.clone(),
            pid: Some(1),
            process_start_key: None,
            healthy: false,
            last_error: Some("starting".to_string()),
            required: true,
            state: LIVE_BACKEND_STATE_UNHEALTHY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        };
        let updated = LiveBackendStatus {
            websocket_url,
            pid: Some(2),
            process_start_key: None,
            healthy: true,
            last_error: None,
            required: true,
            state: LIVE_BACKEND_STATE_READY.to_string(),
            recoverable: true,
            reconcile_attempts: 0,
            retry_after_ms: None,
        };

        write_live_backend_status(&initial).expect("write initial");
        write_live_backend_status(&updated).expect("write updated");

        let persisted = read_live_backend_status()
            .expect("read status")
            .expect("persisted status");
        let status_path = live_backend_status_path().expect("status path");
        let tmp_leftovers = fs::read_dir(status_path.parent().expect("status parent"))
            .expect("read status dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .contains("live-backend.json.tmp")
            })
            .count();

        assert_eq!(persisted, updated);
        assert_eq!(tmp_leftovers, 0);
    }
}

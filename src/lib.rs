use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod cli;
mod codex;
mod config;
mod daemon;
mod discord;
mod live;
mod mcp;
mod projects;
mod state;
mod telegram;
mod ws;

use crate::cli::{
    AwayCommands, Cli, Commands, DaemonCommands, DiscordCommands, HermesCommands, ProjectCommands,
    RemoteCommands, TelegramCommands,
};
use crate::codex::{
    attach_follow_result, build_show_thread_result, classify_app_server_error_message,
    collect_follow_events, filter_watch_events, follow_result_summary, fork_thread_dry_run,
    fork_thread_live_result, get_away_mode, normalized_message, parse_event_filter,
    resolve_codex_binary, run_exec_hook, set_away_mode, start_codex_watch_receiver,
    start_new_thread_dry_run, start_thread_in_cwd, sync_state_from_live, thread_cwd_from_response,
    thread_id_from_response, turn_start_params, watch_events_from_sync_result,
    watch_thread_error_event, CodexAppServerClient, FollowRun,
};
#[cfg(test)]
use crate::codex::{derive_pending_prompt, normalize_thread_snapshot};
use crate::daemon::{
    daemon_service_logs, daemon_service_spec, daemon_service_status, install_daemon_service,
    run_daemon, start_daemon_service, stop_daemon_service, uninstall_daemon_service,
    DEFAULT_DAEMON_LABEL,
};
use crate::discord::{
    discord_set_enabled_result, discord_setup_result, discord_status_result, discord_test_result,
};
use crate::mcp::run_mcp_server;
use crate::projects::{build_registered_project, ensure_unique_project_id, slugify_project_token};
use crate::state::{
    archive_result, create_state_db, list_inbox_from_db, list_waiting_from_db,
    observed_workspaces_from_db, pending_outbound_count, record_action, resolve_archive_targets,
    state_db_path, unarchive_thread_result, ObservedWorkspace,
};
#[cfg(test)]
use crate::state::{classify_inbox_item, create_state_db_in_memory, BridgeThreadSnapshot};
#[cfg(test)]
use crate::state::{
    deliver_due_outbound_events, enqueue_outbound_event, record_transport_delivery,
    transport_delivery_exists, OutboxDeliverySummary,
};
use crate::telegram::{
    telegram_set_enabled_result, telegram_setup_result, telegram_status_result,
    telegram_test_result,
};
use clap::Parser;
pub(crate) use config::{
    daemon_config_path, discord_config_channel_ids, discord_config_enabled, load_daemon_config,
    merged_daemon_config, read_daemon_config_raw, redacted_daemon_config,
    resolve_discord_bot_token, resolve_telegram_bot_token, telegram_config_enabled,
    write_daemon_config, CodexConfig, CodexLiveMode, DaemonConfig, DiscordConfig,
    DiscordSetupOptions, RegisteredProject, SetupOptions, TelegramConfig, TelegramSetupOptions,
};
#[allow(unused_imports)]
pub(crate) use live::{
    ensure_live_backend, live_backend_idle_status, live_backend_status, reconcile_live_backend,
    reset_live_backend,
};
use rusqlite::Connection;
pub(crate) use state::state_dir_path;

#[derive(Serialize)]
struct ErrorEnvelope {
    ok: bool,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    classified: Value,
}

#[derive(Serialize)]
struct DoctorEnvelope {
    ok: bool,
    codex: DoctorCodex,
    bridge: DoctorBridge,
}

#[derive(Serialize)]
struct DoctorCodex {
    resolved_path: String,
    source: String,
    version_stdout: String,
}

#[derive(Serialize)]
struct DoctorBridge {
    config_path: String,
    config_exists: bool,
    telegram_configured: bool,
    telegram_enabled: bool,
    discord_configured: bool,
    discord_enabled: bool,
    codex_configured: bool,
    codex_live_mode: Option<String>,
    codex_websocket_url: Option<String>,
    live_backend_healthy: Option<bool>,
    live_backend_pid: Option<u32>,
    live_backend_error: Option<String>,
    daemon_service_path: String,
    daemon_service_exists: bool,
    daemon_service_loaded: bool,
    daemon_running: bool,
    daemon_state: Option<String>,
    daemon_healthy: bool,
    next_step: Option<String>,
}

#[derive(Serialize)]
struct ReplyResult<'a> {
    ok: bool,
    action: &'a str,
    dry_run: bool,
    thread_id: &'a str,
    message: &'a str,
    sent_at: u64,
}

#[derive(Serialize)]
struct ApproveResult<'a> {
    ok: bool,
    action: &'a str,
    dry_run: bool,
    thread_id: &'a str,
    decision: &'a str,
    sent_text: &'a str,
    sent_at: u64,
}

pub fn main_entry() -> anyhow::Result<()> {
    run()
}

pub fn render_error_envelope(error: &anyhow::Error) -> String {
    let envelope = ErrorEnvelope {
        ok: false,
        error: ErrorBody {
            code: "internal_error",
            message: format!("{error:#}"),
            classified: classify_app_server_error_message(&format!("{error:#}")),
        },
    };

    serde_json::to_string(&envelope).expect("serialize error envelope")
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Setup {
            bot_token,
            chat_id,
            allowed_user_id,
            events,
            bridge_command,
            websocket_url,
            daemon_label,
            install_daemon,
            start_daemon,
            register_hermes,
            hermes_server_name,
            hermes_command,
            pair_timeout_ms,
            dry_run,
        } => {
            let result = setup_result(SetupOptions {
                bot_token: bot_token.as_deref(),
                chat_id: chat_id.as_deref(),
                allowed_user_id: allowed_user_id.as_deref(),
                events: &events,
                bridge_command: &bridge_command,
                websocket_url: &websocket_url,
                daemon_label: &daemon_label,
                install_daemon,
                start_daemon,
                register_hermes,
                hermes_server_name: &hermes_server_name,
                hermes_command: &hermes_command,
                dry_run,
                pair_timeout_ms,
            })?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Doctor => {
            let resolved = resolve_codex_binary()?;
            let output = Command::new(&resolved.path)
                .arg("--version")
                .output()
                .with_context(|| {
                    format!("failed to execute {} --version", resolved.path.display())
                })?;
            if !output.status.success() {
                bail!(
                    "codex binary {} returned non-zero exit status for --version",
                    resolved.path.display()
                );
            }
            let payload = DoctorEnvelope {
                ok: true,
                codex: DoctorCodex {
                    resolved_path: resolved.path.display().to_string(),
                    source: resolved.source.to_string(),
                    version_stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
                },
                bridge: doctor_bridge()?,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        Commands::Away { command } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let payload = match command {
                AwayCommands::On => set_away_mode(&conn, true, now)?,
                AwayCommands::Off => set_away_mode(&conn, false, now)?,
                AwayCommands::Status => get_away_mode(&conn)?,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        Commands::Remote { command } => {
            let payload = match command {
                RemoteCommands::On => remote_mode_on_result(RemoteModeStartAction::On)?,
                RemoteCommands::Off => remote_mode_off_result()?,
                RemoteCommands::Repair => remote_mode_on_result(RemoteModeStartAction::Repair)?,
                RemoteCommands::Status => remote_mode_status_result()?,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        Commands::Threads { limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let mut client = configured_codex_client()?;
            let result = sync_state_from_live(&mut client, &conn, now, limit, false)?;
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "threads": result["threads"].clone()
                }))?
            );
        }
        Commands::Follow {
            thread_id,
            message,
            duration,
            poll_interval,
            events,
        } => {
            let event_filter = parse_event_filter(events.as_deref());
            let mut client = configured_codex_client()?;
            let events = collect_follow_events(
                &mut client,
                &thread_id,
                message.as_deref(),
                duration,
                poll_interval,
                event_filter.as_ref(),
            )?;
            for event in &events {
                println!("{}", serde_json::to_string(event)?);
            }
            println!(
                "{}",
                serde_json::to_string(&follow_result_summary(
                    &thread_id, duration, &events, false,
                ))?
            );
        }
        Commands::Unarchive { thread_id, dry_run } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let live_result = if dry_run {
                None
            } else {
                let mut client = configured_codex_client()?;
                Some(client.request("thread/unarchive", json!({ "threadId": thread_id }))?)
            };
            println!(
                "{}",
                serde_json::to_string(&unarchive_thread_result(
                    &conn,
                    &thread_id,
                    dry_run,
                    now,
                    live_result
                )?)?
            );
        }
        Commands::Waiting { project, limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let mut client = configured_codex_client()?;
            sync_state_from_live(&mut client, &conn, now, limit.max(25), false)?;
            let result = list_waiting_from_db(&conn, project.as_deref(), limit)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Inbox {
            project,
            status,
            attention,
            waiting_on,
            limit,
        } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let mut client = configured_codex_client()?;
            sync_state_from_live(&mut client, &conn, now, limit.max(25), false)?;
            let result = list_inbox_from_db(
                &conn,
                now,
                project.as_deref(),
                status.as_deref(),
                attention.as_deref(),
                waiting_on.as_deref(),
                limit,
            )?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Watch { once, exec, events } => {
            let filter = parse_event_filter(events.as_deref());
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            if once {
                let now = now_millis()?;
                let mut client = configured_codex_client()?;
                let sync_result = match sync_state_from_live(&mut client, &conn, now, 50, false) {
                    Ok(sync_result) => sync_result,
                    Err(error) => {
                        let filtered = filter_watch_events(
                            vec![watch_thread_error_event(&error)],
                            filter.as_ref(),
                        );
                        for event in &filtered {
                            if let Some(command) = exec.as_deref() {
                                run_exec_hook(command, event)?;
                            }
                        }
                        println!("{}", serde_json::to_string(&json!({ "events": filtered }))?);
                        return Ok(());
                    }
                };
                let filtered = watch_events_from_sync_result(&sync_result, vec![], filter.as_ref());
                for event in &filtered {
                    if let Some(command) = exec.as_deref() {
                        run_exec_hook(command, event)?;
                    }
                }
                println!("{}", serde_json::to_string(&json!({ "events": filtered }))?);
            } else {
                println!(
                    "{}",
                    serde_json::to_string(
                        &json!({ "type": "watch_started", "away": get_away_mode(&conn)?["away"] })
                    )?
                );
                let mut last = String::new();
                let watch_rx = start_codex_watch_receiver().ok();
                loop {
                    let now = now_millis()?;
                    let mut client = configured_codex_client()?;
                    let filtered = match sync_state_from_live(&mut client, &conn, now, 50, false) {
                        Ok(sync_result) => watch_events_from_sync_result(
                            &sync_result,
                            client.drain_notifications(),
                            filter.as_ref(),
                        ),
                        Err(error) => filter_watch_events(
                            vec![watch_thread_error_event(&error)],
                            filter.as_ref(),
                        ),
                    };
                    let serialized = serde_json::to_string(&filtered)?;
                    if serialized != last {
                        last = serialized;
                        for event in filtered {
                            println!("{}", serde_json::to_string(&event)?);
                            if let Some(command) = exec.as_deref() {
                                run_exec_hook(command, &event)?;
                            }
                        }
                    }
                    if let Some(rx) = watch_rx.as_ref() {
                        rx.recv_timeout(std::time::Duration::from_millis(1500));
                    } else {
                        std::thread::sleep(std::time::Duration::from_millis(1500));
                    }
                }
            }
        }
        Commands::Daemon { command } => match command {
            DaemonCommands::Run {
                once,
                poll_interval,
                timeout_ms,
            } => {
                run_daemon(once, poll_interval, Duration::from_millis(timeout_ms))?;
            }
            DaemonCommands::Install {
                dry_run,
                label,
                bridge_command,
            } => {
                let result = install_daemon_service(&label, &bridge_command, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Uninstall { dry_run, label } => {
                let result = uninstall_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Start { dry_run, label } => {
                let result = start_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Stop { dry_run, label } => {
                let result = stop_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Status { label } => {
                let result = daemon_service_status(&label)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Logs { label } => {
                let result = daemon_service_logs(&label)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Telegram { command } => match command {
            TelegramCommands::Setup {
                bot_token,
                chat_id,
                allowed_user_id,
                events,
                bridge_command,
                websocket_url,
                pair_timeout_ms,
                dry_run,
            } => {
                let result = telegram_setup_result(TelegramSetupOptions {
                    bot_token: bot_token.as_deref(),
                    chat_id: chat_id.as_deref(),
                    allowed_user_id: allowed_user_id.as_deref(),
                    events: &events,
                    bridge_command: &bridge_command,
                    websocket_url: &websocket_url,
                    dry_run,
                    pair_timeout_ms,
                })?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Status => {
                let result = telegram_status_result()?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Enable { dry_run } => {
                let result = telegram_set_enabled_result(true, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Test {
                message,
                timeout_ms,
                dry_run,
            } => {
                let result =
                    telegram_test_result(&message, Duration::from_millis(timeout_ms), dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Disable { dry_run } => {
                let result = telegram_set_enabled_result(false, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Discord { command } => match command {
            DiscordCommands::Setup {
                bot_token,
                channel_ids,
                allowed_user_id,
                events,
                bridge_command,
                websocket_url,
                dry_run,
            } => {
                let result = discord_setup_result(DiscordSetupOptions {
                    bot_token: bot_token.as_deref(),
                    channel_ids: &channel_ids,
                    allowed_user_id: allowed_user_id.as_deref(),
                    events: &events,
                    bridge_command: &bridge_command,
                    websocket_url: &websocket_url,
                    dry_run,
                })?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DiscordCommands::Status => {
                let result = discord_status_result()?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DiscordCommands::Enable { dry_run } => {
                let result = discord_set_enabled_result(true, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DiscordCommands::Test {
                message,
                timeout_ms,
                dry_run,
            } => {
                let result =
                    discord_test_result(&message, Duration::from_millis(timeout_ms), dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DiscordCommands::Disable { dry_run } => {
                let result = discord_set_enabled_result(false, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Projects { command } => match command {
            ProjectCommands::List { observed_limit } => {
                let result = projects_list_result(observed_limit)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Add {
                cwd,
                id,
                label,
                aliases,
                dry_run,
            } => {
                let result =
                    project_add_result(&cwd, id.as_deref(), label.as_deref(), &aliases, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Import { limit, dry_run } => {
                let result = project_import_result(limit, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Remove { id, dry_run } => {
                let result = project_remove_result(&id, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Sync { limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let mut client = configured_codex_client()?;
            let result = sync_state_from_live(&mut client, &conn, now, limit, false)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::New {
            cwd,
            message,
            dry_run,
            follow,
            stream,
            duration,
            poll_interval,
            events,
            prompt,
        } => {
            let message = normalized_message(message.as_deref()).or_else(|| {
                let joined = prompt.join(" ").trim().to_string();
                (!joined.is_empty()).then_some(joined)
            });
            if dry_run {
                println!(
                    "{}",
                    serde_json::to_string(&start_new_thread_dry_run(
                        cwd.as_deref(),
                        message.as_deref()
                    ))?
                );
            } else {
                let mut client = configured_codex_client()?;
                let result = start_thread_in_cwd(&mut client, cwd.as_deref(), message.as_deref())?;
                let result = if follow {
                    let db_path = state_db_path()?;
                    let conn = create_state_db(&db_path)?;
                    let filter = parse_event_filter(events.as_deref());
                    if let Some(thread_id) = result.get("threadId").and_then(Value::as_str) {
                        attach_follow_result(
                            result.clone(),
                            &mut client,
                            &conn,
                            FollowRun {
                                thread_id,
                                duration_ms: duration,
                                poll_interval_ms: poll_interval,
                                event_filter: filter.as_ref(),
                                stream,
                            },
                        )?
                    } else {
                        result
                    }
                } else {
                    result
                };
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Fork {
            thread_id,
            message,
            dry_run,
            follow,
            stream,
            duration,
            poll_interval,
            events,
            prompt,
        } => {
            let message = normalized_message(message.as_deref()).or_else(|| {
                let joined = prompt.join(" ").trim().to_string();
                (!joined.is_empty()).then_some(joined)
            });
            if dry_run {
                println!(
                    "{}",
                    serde_json::to_string(&fork_thread_dry_run(&thread_id, message.as_deref()))?
                );
            } else {
                let mut client = configured_codex_client()?;
                let forked = client.request("thread/fork", json!({ "threadId": thread_id }))?;
                let new_thread_id = thread_id_from_response(&forked);
                let forked_cwd = thread_cwd_from_response(&forked, None);
                let started = match (new_thread_id.as_deref(), message.as_deref()) {
                    (Some(new_thread_id), Some(message)) if !message.trim().is_empty() => {
                        Some(client.request(
                            "turn/start",
                            turn_start_params(new_thread_id, forked_cwd.as_deref(), message),
                        )?)
                    }
                    _ => None,
                };
                let result =
                    fork_thread_live_result(&thread_id, message.as_deref(), forked, started);
                let result = if follow {
                    let db_path = state_db_path()?;
                    let conn = create_state_db(&db_path)?;
                    let filter = parse_event_filter(events.as_deref());
                    if let Some(thread_id) = result.get("threadId").and_then(Value::as_str) {
                        attach_follow_result(
                            result.clone(),
                            &mut client,
                            &conn,
                            FollowRun {
                                thread_id,
                                duration_ms: duration,
                                poll_interval_ms: poll_interval,
                                event_filter: filter.as_ref(),
                                stream,
                            },
                        )?
                    } else {
                        result
                    }
                } else {
                    result
                };
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Archive {
            thread_id_option,
            thread_ids,
            project,
            status,
            attention,
            limit,
            dry_run,
            yes,
        } => {
            let mut targets = Vec::new();
            if let Some(raw) = thread_id_option {
                let raw = raw.as_str();
                targets.extend(
                    raw.split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                );
            }
            targets.extend(thread_ids);
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            if !dry_run && targets.is_empty() && !yes {
                bail!("Refusing bulk archive without --yes or --dry-run");
            }
            let mut client = if dry_run {
                None
            } else {
                Some(configured_codex_client()?)
            };
            if !dry_run && targets.is_empty() {
                if let Some(client) = client.as_mut() {
                    sync_state_from_live(client, &conn, now, 50, false)?;
                }
            }
            let selection = resolve_archive_targets(
                &conn,
                &targets,
                project.as_deref(),
                status.as_deref(),
                attention.as_deref(),
                limit,
                now,
            )?;
            if !dry_run && selection.using_filter_selection && !yes {
                bail!("Refusing bulk archive without --yes or --dry-run");
            }
            if dry_run {
                let results = selection
                    .targets
                    .into_iter()
                    .map(|thread_id| json!({ "threadId": thread_id, "status": "would_archive" }))
                    .collect::<Vec<_>>();
                println!("{}", serde_json::to_string(&archive_result(true, results))?);
            } else {
                let mut results = Vec::new();
                for target in selection.targets {
                    let result = client
                        .as_mut()
                        .context("archive client missing")?
                        .request("thread/archive", json!({ "threadId": target }))?;
                    record_action(
                        &conn,
                        &target,
                        "archive",
                        json!({ "result": result, "archivedAt": now }),
                        now,
                    )?;
                    results.push(json!({
                        "threadId": target,
                        "status": "archived",
                        "result": result
                    }));
                }
                println!(
                    "{}",
                    serde_json::to_string(&archive_result(false, results))?
                );
            }
        }
        Commands::Show { thread_id } => {
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let mut client = configured_codex_client()?;
            let result = client.request(
                "thread/read",
                json!({
                    "threadId": thread_id,
                    "includeTurns": true
                }),
            )?;
            println!(
                "{}",
                serde_json::to_string(
                    &build_show_thread_result(Some(&conn), &thread_id, result,)?
                )?
            );
        }
        Commands::Reply {
            thread_id,
            message,
            dry_run,
            follow,
            stream,
            duration,
            poll_interval,
            events,
            prompt,
        } => {
            let message = message
                .or_else(|| {
                    let joined = prompt.join(" ").trim().to_string();
                    (!joined.is_empty()).then_some(joined)
                })
                .unwrap_or_default()
                .trim()
                .to_string();
            if message.is_empty() {
                bail!("Reply message cannot be empty");
            }
            let sent_at = now_millis()?;
            if dry_run {
                let payload = ReplyResult {
                    ok: true,
                    action: "reply",
                    dry_run: true,
                    thread_id: &thread_id,
                    message: &message,
                    sent_at,
                };
                println!("{}", serde_json::to_string(&payload)?);
            } else {
                let mut client = configured_codex_client()?;
                let resumed = client.request("thread/resume", json!({ "threadId": thread_id }))?;
                let started = client.request(
                    "turn/start",
                    json!({
                        "threadId": thread_id,
                        "input": [{
                            "type": "text",
                            "text": message,
                            "text_elements": []
                        }]
                    }),
                )?;
                let db_path = state_db_path()?;
                let conn = create_state_db(&db_path)?;
                record_action(
                    &conn,
                    &thread_id,
                    "reply",
                    json!({
                        "message": message,
                        "resumed": resumed,
                        "started": started,
                        "sentAt": sent_at
                    }),
                    sent_at,
                )?;
                let result = json!({
                    "ok": true,
                    "action": "reply",
                    "threadId": thread_id,
                    "message": message,
                    "sentAt": sent_at,
                    "resumed": resumed,
                    "started": started
                });
                let result = if follow {
                    let filter = parse_event_filter(events.as_deref());
                    attach_follow_result(
                        result,
                        &mut client,
                        &conn,
                        FollowRun {
                            thread_id: &thread_id,
                            duration_ms: duration,
                            poll_interval_ms: poll_interval,
                            event_filter: filter.as_ref(),
                            stream,
                        },
                    )?
                } else {
                    result
                };
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Approve {
            thread_id,
            decision,
            dry_run,
            follow,
            stream,
            duration,
            poll_interval,
            events,
            positional_decision,
        } => {
            let normalized = decision
                .or(positional_decision)
                .unwrap_or_default()
                .trim()
                .to_lowercase();
            let sent_text = match normalized.as_str() {
                "approve" => "YES",
                "deny" => "NO",
                _ => bail!("Approval decision must be approve or deny"),
            };
            let sent_at = now_millis()?;
            if dry_run {
                let payload = ApproveResult {
                    ok: true,
                    action: "approve",
                    dry_run: true,
                    thread_id: &thread_id,
                    decision: &normalized,
                    sent_text,
                    sent_at,
                };
                println!("{}", serde_json::to_string(&payload)?);
            } else {
                let mut client = configured_codex_client()?;
                let resumed = client.request("thread/resume", json!({ "threadId": thread_id }))?;
                let started = client.request(
                    "turn/start",
                    json!({
                        "threadId": thread_id,
                        "input": [{
                            "type": "text",
                            "text": sent_text,
                            "text_elements": []
                        }]
                    }),
                )?;
                let db_path = state_db_path()?;
                let conn = create_state_db(&db_path)?;
                record_action(
                    &conn,
                    &thread_id,
                    "approve",
                    json!({
                        "decision": normalized,
                        "sentText": sent_text,
                        "resumed": resumed,
                        "started": started,
                        "sentAt": sent_at
                    }),
                    sent_at,
                )?;
                let result = json!({
                    "ok": true,
                    "action": "approve",
                    "threadId": thread_id,
                    "decision": normalized,
                    "sentText": sent_text,
                    "sentAt": sent_at,
                    "resumed": resumed,
                    "started": started
                });
                let result = if follow {
                    let filter = parse_event_filter(events.as_deref());
                    attach_follow_result(
                        result,
                        &mut client,
                        &conn,
                        FollowRun {
                            thread_id: &thread_id,
                            duration_ms: duration,
                            poll_interval_ms: poll_interval,
                            event_filter: filter.as_ref(),
                            stream,
                        },
                    )?
                } else {
                    result
                };
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Mcp => {
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            run_mcp_server(stdin.lock(), stdout.lock())?;
        }
        Commands::Hermes { command } => match command {
            HermesCommands::Install {
                server_name,
                hermes_command,
                bridge_command,
                dry_run,
            } => {
                let result = run_hermes_install(HermesInstallOptions {
                    server_name: &server_name,
                    hermes_command: &hermes_command,
                    bridge_command: &bridge_command,
                    dry_run,
                })?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
    }

    Ok(())
}

fn now_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!(e))?
        .as_millis() as u64)
}

fn importable_projects_from_observed(
    observed: &[ObservedWorkspace],
    existing_projects: &[RegisteredProject],
) -> Vec<RegisteredProject> {
    let mut projects = Vec::new();
    let mut existing_ids = existing_projects
        .iter()
        .map(|project| project.id.clone())
        .collect::<BTreeSet<_>>();
    let existing_cwds = existing_projects
        .iter()
        .map(|project| project.cwd.clone())
        .collect::<BTreeSet<_>>();
    for workspace in observed {
        if existing_cwds.contains(&workspace.cwd)
            || projects
                .iter()
                .any(|project: &RegisteredProject| project.cwd == workspace.cwd)
        {
            continue;
        }
        let base_id =
            slugify_project_token(&workspace.label).unwrap_or_else(|| "project".to_string());
        let id = ensure_unique_project_id(&base_id, &existing_ids);
        existing_ids.insert(id.clone());
        projects.push(RegisteredProject {
            id,
            label: workspace.label.clone(),
            cwd: workspace.cwd.clone(),
            aliases: Vec::new(),
        });
    }
    projects
}

fn daemon_run_command(bridge_command: &str) -> String {
    format!("{} daemon run", shell_quote(bridge_command))
}

fn doctor_bridge() -> Result<DoctorBridge> {
    let config_path = daemon_config_path()?;
    let config = read_daemon_config_raw()?;
    let telegram = config.as_ref().and_then(|config| config.telegram.as_ref());
    let discord = config.as_ref().and_then(|config| config.discord.as_ref());
    let codex = config.as_ref().and_then(|config| config.codex.as_ref());
    let (live_backend_healthy, live_backend_pid, live_backend_error) = match codex {
        Some(codex) => match live::live_backend_status(codex) {
            Ok(status) => (Some(status.healthy), status.pid, status.last_error),
            Err(error) => (Some(false), None, Some(format!("{error:#}"))),
        },
        None => (
            None,
            None,
            Some("shared Codex live backend is not configured".to_string()),
        ),
    };
    let service = daemon_service_spec(DEFAULT_DAEMON_LABEL, "codex-telegram-bridge")?;
    let status = daemon_service_status(DEFAULT_DAEMON_LABEL)?;
    Ok(DoctorBridge {
        config_path: config_path.display().to_string(),
        config_exists: config_path.exists(),
        telegram_configured: telegram.is_some(),
        telegram_enabled: telegram_config_enabled(telegram),
        discord_configured: discord.is_some(),
        discord_enabled: discord_config_enabled(discord),
        codex_configured: codex.is_some(),
        codex_live_mode: codex.map(|codex| match codex.live_mode {
            CodexLiveMode::Shared => "shared".to_string(),
        }),
        codex_websocket_url: codex.map(|codex| codex.websocket_url.clone()),
        live_backend_healthy,
        live_backend_pid,
        live_backend_error,
        daemon_service_path: service.service_path.display().to_string(),
        daemon_service_exists: service.service_path.exists(),
        daemon_service_loaded: status
            .pointer("/serviceStatus/loaded")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        daemon_running: status
            .pointer("/serviceStatus/running")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        daemon_state: status
            .pointer("/serviceStatus/state")
            .and_then(Value::as_str)
            .map(str::to_string),
        daemon_healthy: status
            .get("healthy")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        next_step: status
            .get("nextStep")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn configured_codex_client() -> Result<CodexAppServerClient> {
    let config = load_daemon_config()?;
    CodexAppServerClient::connect_configured(&config)
}

fn setup_result(options: SetupOptions<'_>) -> Result<Value> {
    let resolved = resolve_codex_binary()?;
    let telegram = telegram_setup_result(TelegramSetupOptions {
        bot_token: options.bot_token,
        chat_id: options.chat_id,
        allowed_user_id: options.allowed_user_id,
        events: options.events,
        bridge_command: options.bridge_command,
        websocket_url: options.websocket_url,
        dry_run: options.dry_run,
        pair_timeout_ms: options.pair_timeout_ms,
    })?;
    let daemon_install = if options.install_daemon {
        Some(install_daemon_service(
            options.daemon_label,
            options.bridge_command,
            options.dry_run,
        )?)
    } else {
        None
    };
    let daemon_start = if options.start_daemon {
        Some(start_daemon_service(options.daemon_label, options.dry_run)?)
    } else {
        None
    };
    let hermes = if options.register_hermes {
        Some(run_hermes_install(HermesInstallOptions {
            server_name: options.hermes_server_name,
            hermes_command: options.hermes_command,
            bridge_command: options.bridge_command,
            dry_run: options.dry_run,
        })?)
    } else {
        None
    };

    Ok(json!({
        "ok": true,
        "action": "setup",
        "dryRun": options.dry_run,
        "codex": {
            "resolvedPath": resolved.path.display().to_string(),
            "source": resolved.source
        },
        "telegram": telegram,
        "daemon": {
            "install": daemon_install,
            "start": daemon_start
        },
        "hermes": hermes,
        "nextStep": if options.dry_run {
            "Run setup without --dry-run, then send /away to the Telegram bot when leaving your computer."
        } else {
            "Send /away to the Telegram bot when leaving your computer. Reply to Codex Telegram messages to keep working from Telegram; use /repair if the shared backend becomes unhealthy."
        }
    }))
}

fn remote_backend_status_value(config: Option<&DaemonConfig>, required: bool) -> Value {
    match config.and_then(|config| config.codex.as_ref()) {
        Some(codex) => match if required {
            live_backend_status(codex)
        } else {
            live_backend_idle_status(codex)
        } {
            Ok(status) => json!(status),
            Err(error) => json!({
                "websocketUrl": codex.websocket_url,
                "pid": Value::Null,
                "processStartKey": Value::Null,
                "healthy": false,
                "required": required,
                "state": if required { "unhealthy" } else { "idle" },
                "recoverable": required,
                "lastError": format!("{error:#}")
            }),
        },
        None => Value::Null,
    }
}

fn remote_mode_status_payload(
    action: &str,
    config: Option<&DaemonConfig>,
    away: Value,
    backend: Value,
    conn: &Connection,
) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "action": action,
        "configPath": daemon_config_path()?.display().to_string(),
        "stateFolderPath": state_dir_path()?.display().to_string(),
        "configured": config.is_some(),
        "telegramConfigured": config.as_ref().and_then(|config| config.telegram.as_ref()).is_some(),
        "telegramEnabled": telegram_config_enabled(config.as_ref().and_then(|config| config.telegram.as_ref())),
        "discordConfigured": config.as_ref().and_then(|config| config.discord.as_ref()).is_some(),
        "discordEnabled": discord_config_enabled(config.as_ref().and_then(|config| config.discord.as_ref())),
        "codexConfigured": config.as_ref().and_then(|config| config.codex.as_ref()).is_some(),
        "away": away,
        "backend": backend,
        "pending": pending_outbound_count(conn)?
    }))
}

fn remote_mode_status_result() -> Result<Value> {
    let config = read_daemon_config_raw()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let away = get_away_mode(&conn)?;
    let backend_required = away.get("away").and_then(Value::as_bool) == Some(true);
    let backend = remote_backend_status_value(config.as_ref(), backend_required);
    remote_mode_status_payload("remote_status", config.as_ref(), away, backend, &conn)
}

#[derive(Clone, Copy)]
enum RemoteModeStartAction {
    On,
    Repair,
}

impl RemoteModeStartAction {
    fn json_action(self) -> &'static str {
        match self {
            Self::On => "remote_on",
            Self::Repair => "remote_repair",
        }
    }
}

fn remote_mode_on_result(action: RemoteModeStartAction) -> Result<Value> {
    let now = now_millis()?;
    let config = load_daemon_config()?;
    let codex = config
        .codex
        .as_ref()
        .context("shared Codex live backend is not configured; run setup first")?;
    let backend = match action {
        RemoteModeStartAction::On => ensure_live_backend(codex)?,
        RemoteModeStartAction::Repair => reset_live_backend(codex)?,
    };
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let away = set_away_mode(&conn, true, now)?;
    remote_mode_status_payload(
        action.json_action(),
        Some(&config),
        away,
        json!(backend.status),
        &conn,
    )
}

fn remote_mode_off_result() -> Result<Value> {
    let now = now_millis()?;
    let config = read_daemon_config_raw()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let away = set_away_mode(&conn, false, now)?;
    let backend = remote_backend_status_value(config.as_ref(), false);
    remote_mode_status_payload("remote_off", config.as_ref(), away, backend, &conn)
}

fn projects_list_result(observed_limit: u64) -> Result<Value> {
    let config = load_daemon_config()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let observed = observed_workspaces_from_db(&conn, observed_limit)?;
    let importable = importable_projects_from_observed(&observed, &config.projects);
    Ok(json!({
        "ok": true,
        "action": "projects_list",
        "configured": config.projects,
        "observed": observed.into_iter().map(|workspace| json!({
            "label": workspace.label,
            "cwd": workspace.cwd,
            "lastSeenAt": workspace.last_seen_at
        })).collect::<Vec<_>>(),
        "importable": importable
    }))
}

fn project_add_result(
    cwd: &str,
    id: Option<&str>,
    label: Option<&str>,
    aliases: &[String],
    dry_run: bool,
) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let project = build_registered_project(cwd, id, label, aliases, &config.projects)?;
    if config
        .projects
        .iter()
        .any(|existing| existing.cwd == project.cwd)
    {
        bail!("project cwd `{}` is already registered", project.cwd);
    }
    if !dry_run {
        config.projects.push(project.clone());
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_add",
        "dryRun": dry_run,
        "project": project
    }))
}

fn project_import_result(limit: u64, dry_run: bool) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let observed = observed_workspaces_from_db(&conn, limit)?;
    let importable = importable_projects_from_observed(&observed, &config.projects);
    if !dry_run && !importable.is_empty() {
        config.projects.extend(importable.clone());
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_import",
        "dryRun": dry_run,
        "imported": importable,
        "count": importable.len()
    }))
}

fn project_remove_result(id: &str, dry_run: bool) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let Some(project) = config
        .projects
        .iter()
        .find(|project| project.id == id)
        .cloned()
    else {
        bail!("project `{id}` was not found");
    };
    if !dry_run {
        config.projects.retain(|candidate| candidate.id != id);
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_remove",
        "dryRun": dry_run,
        "project": project
    }))
}

const DEFAULT_NOTIFICATION_EVENTS: &str = "thread_waiting,thread_completed";
const DEFAULT_CODEX_WEBSOCKET_URL: &str = "ws://127.0.0.1:4500";

#[derive(Debug, Clone, Copy)]
struct HermesInstallOptions<'a> {
    server_name: &'a str,
    hermes_command: &'a str,
    bridge_command: &'a str,
    dry_run: bool,
}

fn run_hermes_install(options: HermesInstallOptions<'_>) -> Result<Value> {
    let server_name = options.server_name.trim();
    let hermes_command = options.hermes_command.trim();
    let bridge_command = options.bridge_command.trim();

    if server_name.is_empty() {
        bail!("server name cannot be empty");
    }
    if hermes_command.is_empty() {
        bail!("hermes command cannot be empty");
    }
    if bridge_command.is_empty() {
        bail!("bridge command cannot be empty");
    }

    let mcp_args = vec![
        "mcp".to_string(),
        "add".to_string(),
        server_name.to_string(),
        "--command".to_string(),
        bridge_command.to_string(),
        "--args".to_string(),
        "mcp".to_string(),
    ];

    let base = json!({
        "ok": true,
        "action": "hermes_install",
        "dryRun": options.dry_run,
        "serverName": server_name,
        "hermesCommand": hermes_command,
        "bridgeCommand": bridge_command,
        "args": mcp_args.clone(),
        "mcp": {
            "configured": true,
            "args": mcp_args.clone(),
            "nextStep": "Restart Hermes so it reconnects to MCP servers and discovers codex_* tools."
        },
        "nextStep": "Restart Hermes for MCP discovery. Telegram notifications are configured with the top-level setup command, not through Hermes."
    });
    if options.dry_run {
        return Ok(base);
    }

    let mcp_output = Command::new(hermes_command)
        .args(&mcp_args)
        .output()
        .with_context(|| format!("failed to run {hermes_command} mcp add"))?;
    if !mcp_output.status.success() {
        bail!(
            "Hermes MCP registration failed with status {}: {}",
            mcp_output.status,
            String::from_utf8_lossy(&mcp_output.stderr).trim()
        );
    }

    Ok(json!({
        "ok": true,
        "action": "hermes_install",
        "dryRun": false,
        "serverName": server_name,
        "hermesCommand": hermes_command,
        "bridgeCommand": bridge_command,
        "args": mcp_args,
        "mcp": {
            "configured": true,
            "stdout": String::from_utf8_lossy(&mcp_output.stdout).trim(),
            "stderr": String::from_utf8_lossy(&mcp_output.stderr).trim()
        },
        "nextStep": "Restart Hermes for MCP discovery. Telegram notifications are configured with the top-level setup command, not through Hermes."
    }))
}

fn redact_secret_text(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_string()
    } else {
        text.replace(secret, "<redacted>")
    }
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ',' | ':' | '=')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn event_thread_id(event: &Value) -> Option<String> {
    event
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| event.pointer("/thread/threadId").and_then(Value::as_str))
        .or_else(|| event.pointer("/thread/id").and_then(Value::as_str))
        .map(str::to_string)
}

fn notification_event_id(event: &Value) -> String {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("codex_event");
    let thread_id = event_thread_id(event).unwrap_or_else(|| "unknown".to_string());
    let discriminator = event
        .get("eventKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            event
                .get("updatedAt")
                .and_then(Value::as_u64)
                .map(|value| value.to_string())
        })
        .or_else(|| {
            event
                .get("observedAt")
                .and_then(Value::as_u64)
                .map(|value| value.to_string())
        })
        .unwrap_or_else(|| {
            serde_json::to_string(event)
                .map(|raw| sha256_hex(raw.as_bytes()))
                .unwrap_or_else(|_| "event".to_string())
        });
    sanitize_delivery_id(&format!("codex:{event_type}:{thread_id}:{discriminator}"))
}

fn sanitize_delivery_id(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn sha256_hex(message: &[u8]) -> String {
    hex_lower(&Sha256::digest(message))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

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

    impl Drop for TempStateDir {
        fn drop(&mut self) {
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("CODEX_TELEGRAM_BRIDGE_STATE_DIR");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn derives_waiting_prompt_from_status_flags() {
        let summary = json!({
            "id": "thr_reply",
            "name": null,
            "cwd": "/tmp/reply",
            "updatedAt": 123,
            "status": {
                "type": "active",
                "activeFlags": ["waitingOnUserInput"]
            }
        });
        let thread = json!({
            "id": "thr_reply",
            "cwd": "/tmp/reply",
            "status": {
                "type": "active",
                "activeFlags": ["waitingOnUserInput"]
            },
            "turns": [
                {
                    "status": "in_progress",
                    "items": [
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "Can you confirm the plan?"
                        }
                    ]
                }
            ]
        });

        let snapshot = normalize_thread_snapshot(&summary, &thread).expect("snapshot");
        let prompt = snapshot.pending_prompt.expect("pending prompt");
        assert_eq!(prompt.kind, "reply");
        assert_eq!(
            prompt.question.as_deref(),
            Some("Can you confirm the plan?")
        );
        assert_eq!(snapshot.last_turn_status.as_deref(), Some("in_progress"));
    }

    #[test]
    fn remote_status_reports_missing_config_without_failing() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let state = TempStateDir::new("remote-status-missing-config");

        let status = remote_mode_status_result().expect("remote status");

        assert_eq!(status["ok"], true);
        assert_eq!(status["action"], "remote_status");
        assert_eq!(status["configured"], false);
        assert_eq!(status["stateFolderPath"], state.root.display().to_string());
        assert_eq!(status["away"]["away"], false);
        assert_eq!(status["backend"], Value::Null);
        assert_eq!(status["pending"], 0);
    }

    #[test]
    fn remote_status_reports_idle_backend_when_remote_mode_is_off() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _state = TempStateDir::new("remote-status-idle-backend");
        let websocket_url = "ws://127.0.0.1:9";
        write_daemon_config(&DaemonConfig {
            version: 4,
            bridge_command: "bridge".to_string(),
            events: "thread_waiting".to_string(),
            telegram: None,
            discord: None,
            codex: Some(CodexConfig {
                live_mode: CodexLiveMode::Shared,
                websocket_url: websocket_url.to_string(),
            }),
            projects: Vec::new(),
        })
        .expect("write config");

        let status = remote_mode_status_result().expect("remote status");

        assert_eq!(status["configured"], true);
        assert_eq!(status["codexConfigured"], true);
        assert_eq!(status["away"]["away"], false);
        assert_eq!(status["backend"]["websocketUrl"], websocket_url);
        assert_eq!(status["backend"]["required"], false);
        assert_eq!(status["backend"]["state"], "idle");
        assert_eq!(status["backend"]["healthy"], false);
        assert_eq!(status["backend"]["lastError"], Value::Null);
    }

    #[test]
    fn remote_off_clears_pending_notifications() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _state = TempStateDir::new("remote-off-clears-pending");
        let conn = create_state_db(&state_db_path().expect("state db path")).expect("db");
        set_away_mode(&conn, true, 1000).expect("away on");
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_waiting",
                "threadId": "thr_remote",
                "updatedAt": 1500
            }),
            2000,
        )
        .expect("enqueue");
        assert_eq!(pending_outbound_count(&conn).expect("pending before"), 1);

        let result = remote_mode_off_result().expect("remote off");

        assert_eq!(result["action"], "remote_off");
        assert_eq!(result["configured"], false);
        assert_eq!(result["away"]["away"], false);
        assert_eq!(result["away"]["clearedPendingNotifications"], 1);
        assert_eq!(result["backend"], Value::Null);
        assert_eq!(result["pending"], 0);
        assert_eq!(pending_outbound_count(&conn).expect("pending after"), 0);
    }

    #[test]
    fn hermes_install_dry_run_builds_mcp_add_command() {
        let result = run_hermes_install(HermesInstallOptions {
            server_name: "codex",
            hermes_command: "hermes-se",
            bridge_command: "codex-telegram-bridge",
            dry_run: true,
        })
        .expect("dry-run install result");

        assert_eq!(result["action"], "hermes_install");
        assert_eq!(result["dryRun"], true);
        assert_eq!(result["hermesCommand"], "hermes-se");
        assert_eq!(
            result["args"],
            json!([
                "mcp",
                "add",
                "codex",
                "--command",
                "codex-telegram-bridge",
                "--args",
                "mcp"
            ])
        );
        assert_eq!(
            result["mcp"]["nextStep"],
            "Restart Hermes so it reconnects to MCP servers and discovers codex_* tools."
        );
        assert!(
            result.get("notificationLane").is_none(),
            "Hermes install should not configure Telegram notifications"
        );
    }

    #[test]
    fn outbound_events_dedupe_retry_and_deliver_durably() {
        let conn = create_state_db_in_memory().expect("db");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 42
        });

        assert!(enqueue_outbound_event(&conn, &event, 1000).expect("enqueue"));
        assert!(
            !enqueue_outbound_event(&conn, &event, 1001).expect("dedupe"),
            "same delivery id should not enqueue twice"
        );

        let failed =
            deliver_due_outbound_events(&conn, 1000, 10, None, |_| bail!("Hermes offline"))
                .expect("failed delivery summary");
        assert_eq!(
            failed,
            OutboxDeliverySummary {
                attempted: 1,
                delivered: 0,
                failed: 1
            }
        );
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);

        let delayed =
            deliver_due_outbound_events(&conn, 1000, 10, None, |_| Ok(json!({"ok": true})))
                .expect("not due summary");
        assert_eq!(delayed.attempted, 0);

        let delivered =
            deliver_due_outbound_events(&conn, 2000, 10, None, |_| Ok(json!({"ok": true})))
                .expect("delivered summary");
        assert_eq!(
            delivered,
            OutboxDeliverySummary {
                attempted: 1,
                delivered: 1,
                failed: 0
            }
        );
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 0);
    }

    #[test]
    fn transport_delivery_log_tracks_each_transport_once() {
        let conn = create_state_db_in_memory().expect("db");

        assert!(
            !transport_delivery_exists(&conn, "event_1", "telegram").expect("lookup"),
            "transport should not start delivered"
        );
        record_transport_delivery(
            &conn,
            "event_1",
            "telegram",
            &json!({ "messageId": 111 }),
            1000,
        )
        .expect("record delivery");

        assert!(
            transport_delivery_exists(&conn, "event_1", "telegram").expect("lookup"),
            "recorded transport should be treated as delivered"
        );
        assert!(
            !transport_delivery_exists(&conn, "event_1", "hermes").expect("lookup"),
            "other transports for the same event must remain pending"
        );
    }

    #[test]
    fn inbox_age_seconds_handles_mixed_timestamp_units() {
        let snapshot = snapshot_fixture(
            "thr_recent",
            "/tmp/project",
            1_776_219_396,
            "notLoaded",
            vec![],
            Some("completed"),
        );

        let item = classify_inbox_item(&snapshot, 1_776_219_400_000);

        assert_eq!(item.age_seconds, Some(4));
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

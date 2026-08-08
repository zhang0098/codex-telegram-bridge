use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};
use tungstenite::{
    client::client_with_config, protocol::WebSocketConfig, Error as WsError, Message, WebSocket,
};
use url::Url;

const WEBSOCKET_IO_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_APP_SERVER_WEBSOCKET_LIMIT_BYTES: usize = 256 << 20;

#[derive(Debug)]
pub(crate) struct WsJsonRpcTransport {
    socket: WebSocket<TcpStream>,
}

impl WsJsonRpcTransport {
    pub(crate) fn connect(url: &str) -> Result<Self> {
        let url = validate_shared_websocket_url(url)?;
        let stream = connect_loopback_tcp(&url, websocket_io_timeout())?;
        let (socket, _) = client_with_config(url.as_str(), stream, Some(local_app_server_config()))
            .context("failed to connect websocket transport")?;
        Ok(Self { socket })
    }

    pub(crate) fn write_json(&mut self, value: &Value) -> Result<()> {
        self.socket
            .send(Message::text(serde_json::to_string(value)?))
            .context("failed to write websocket JSON-RPC message")?;
        Ok(())
    }

    pub(crate) fn read_json(&mut self) -> Result<Value> {
        self.read_json_with_timeout(websocket_io_timeout())
    }

    pub(crate) fn read_json_with_timeout(&mut self, timeout: Duration) -> Result<Value> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        loop {
            let now = Instant::now();
            if now >= deadline {
                bail!(
                    "timed out waiting for websocket JSON-RPC message after {}ms",
                    timeout.as_millis()
                );
            }
            self.socket
                .get_mut()
                .set_read_timeout(Some(deadline.saturating_duration_since(now)))
                .context("failed to set websocket read timeout")?;

            let message = match self.socket.read() {
                Ok(message) => message,
                Err(WsError::Io(error))
                    if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
                        && Instant::now() >= deadline =>
                {
                    bail!(
                        "timed out waiting for websocket JSON-RPC message after {}ms",
                        timeout.as_millis()
                    );
                }
                Err(error) => {
                    return Err(error).context("failed to read websocket JSON-RPC message");
                }
            };

            match message {
                Message::Text(text) => {
                    return serde_json::from_str(&text).with_context(|| {
                        format!("invalid JSON from websocket app-server: {text}")
                    });
                }
                Message::Binary(bytes) => {
                    return serde_json::from_slice(&bytes).with_context(|| {
                        format!(
                            "invalid binary JSON from websocket app-server ({} bytes)",
                            bytes.len()
                        )
                    });
                }
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .context("failed to answer websocket ping")?;
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Close(frame) => {
                    let reason = frame
                        .as_ref()
                        .map(|value| value.reason.to_string())
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| "no reason provided".to_string());
                    bail!("websocket app-server closed connection: {reason}");
                }
            }
        }
    }

    pub(crate) fn try_read_json(&mut self) -> Result<Option<Value>> {
        self.set_nonblocking(true)?;
        let read = match self.read_json() {
            Ok(message) => Ok(Some(message)),
            Err(error) if is_would_block(&error) => Ok(None),
            Err(error) => Err(error),
        };
        self.set_nonblocking(false)?;
        read
    }

    fn set_nonblocking(&mut self, value: bool) -> Result<()> {
        self.socket
            .get_mut()
            .set_nonblocking(value)
            .with_context(|| format!("failed to set websocket nonblocking={value}"))
    }
}

fn local_app_server_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(LOCAL_APP_SERVER_WEBSOCKET_LIMIT_BYTES);
    config.max_frame_size = Some(LOCAL_APP_SERVER_WEBSOCKET_LIMIT_BYTES);
    config
}

pub(crate) fn validate_shared_websocket_url(url: &str) -> Result<Url> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        bail!("shared Codex live backend websocket URL cannot be empty");
    }
    if trimmed != url {
        bail!("shared Codex live backend websocket URL must not contain surrounding whitespace");
    }

    let parsed = Url::parse(url).with_context(|| format!("invalid websocket URL: {url}"))?;
    if parsed.scheme() != "ws" {
        bail!("only ws:// shared websocket URLs are supported in this release");
    }
    match parsed.host_str() {
        Some("127.0.0.1") | Some("localhost") | Some("::1") => {}
        _ => bail!("only loopback ws:// shared websocket URLs are supported in this release"),
    }
    Ok(parsed)
}

fn connect_loopback_tcp(url: &Url, timeout: Duration) -> Result<TcpStream> {
    let host = url
        .host_str()
        .context("shared websocket URL missing host")?;
    let port = url
        .port_or_known_default()
        .context("shared websocket URL missing port")?;
    let mut saw_address = false;
    let mut saw_loopback = false;
    let mut last_error = None;

    for address in (host, port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve shared websocket host {host}"))?
    {
        saw_address = true;
        if !address.ip().is_loopback() {
            continue;
        }
        saw_loopback = true;
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(timeout))
                    .context("failed to set websocket read timeout")?;
                stream
                    .set_write_timeout(Some(timeout))
                    .context("failed to set websocket write timeout")?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }

    if !saw_address {
        bail!("shared websocket URL {url} did not resolve to a socket address");
    }
    if !saw_loopback {
        bail!("shared websocket URL {url} did not resolve to a loopback socket address");
    }
    if let Some(error) = last_error {
        return Err(error).with_context(|| {
            format!(
                "failed to connect websocket transport to {url} within {}ms",
                timeout.as_millis()
            )
        });
    }
    bail!("failed to connect websocket transport to {url}")
}

fn websocket_io_timeout() -> Duration {
    #[cfg(test)]
    if let Ok(raw) = std::env::var("CODEX_TELEGRAM_BRIDGE_WS_TIMEOUT_MS") {
        if let Ok(ms) = raw.parse::<u64>() {
            return Duration::from_millis(ms.max(1));
        }
    }

    WEBSOCKET_IO_TIMEOUT
}

fn is_would_block(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|source| source.downcast_ref::<WsError>())
        .any(|source| matches!(source, WsError::Io(io) if io.kind() == ErrorKind::WouldBlock))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn websocket_transport_reads_and_writes_json() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws server");
        let address = listener.local_addr().expect("fake ws server addr");
        let (requests_tx, requests_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept websocket client");
            let mut socket = tungstenite::accept(stream).expect("accept websocket");
            let request = socket.read().expect("read websocket request");
            let text = request.into_text().expect("text websocket request");
            requests_tx
                .send(serde_json::from_str::<Value>(&text).expect("parse websocket request"))
                .expect("send parsed request");
            socket
                .send(Message::text(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": { "ok": true }
                    }))
                    .expect("serialize websocket response"),
                ))
                .expect("write websocket response");
        });

        let mut transport =
            WsJsonRpcTransport::connect(&format!("ws://{address}")).expect("connect transport");
        transport
            .write_json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            }))
            .expect("write websocket JSON");
        assert_eq!(
            transport.read_json().expect("read websocket JSON"),
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": { "ok": true }
            })
        );
        assert_eq!(
            requests_rx.recv().expect("captured websocket request"),
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            })
        );
        server.join().expect("fake ws server thread");
    }

    #[test]
    fn websocket_transport_accepts_large_local_app_server_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ws server");
        let address = listener.local_addr().expect("fake ws server addr");
        let oversized_text = "x".repeat((16 << 20) + 1);
        let expected_text = oversized_text.clone();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept websocket client");
            let mut socket = tungstenite::accept(stream).expect("accept websocket");
            let request = socket.read().expect("read websocket request");
            let text = request.into_text().expect("text websocket request");
            serde_json::from_str::<Value>(&text).expect("parse websocket request");
            socket
                .send(Message::text(
                    serde_json::to_string(&json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": { "payload": oversized_text }
                    }))
                    .expect("serialize websocket response"),
                ))
                .expect("write websocket response");
        });

        let mut transport =
            WsJsonRpcTransport::connect(&format!("ws://{address}")).expect("connect transport");
        transport
            .write_json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "thread/list",
                "params": {}
            }))
            .expect("write websocket JSON");
        let response = transport.read_json().expect("read websocket JSON");
        assert_eq!(response["result"]["payload"], expected_text);

        server.join().expect("join fake websocket server");
    }

    #[test]
    fn websocket_transport_rejects_non_loopback_secure_urls_for_now() {
        let error = WsJsonRpcTransport::connect("wss://127.0.0.1:4500").expect_err("reject wss");
        assert!(
            format!("{error:#}").contains("only ws:// shared websocket URLs are supported"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn websocket_transport_rejects_non_loopback_hosts() {
        let error = WsJsonRpcTransport::connect("ws://example.com:4500")
            .expect_err("reject non-loopback host");
        assert!(
            format!("{error:#}")
                .contains("only loopback ws:// shared websocket URLs are supported"),
            "unexpected error: {error:#}"
        );
    }
}

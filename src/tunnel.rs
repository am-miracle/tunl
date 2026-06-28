use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff::Backoff;
use crate::bridge;
use crate::target::Target;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(
    service: String,
    target: Arc<dyn Target>,
    listener: TcpListener,
    token: CancellationToken,
) {
    info!(service = %service, target = %target.describe(), "listening");

    let mut connections: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!(service = %service, error = %e, "accept failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                            _ = token.cancelled() => break,
                        }
                        continue;
                    }
                };

                connections.spawn(connect_and_bridge(
                    service.clone(),
                    Arc::clone(&target),
                    stream,
                    peer,
                    token.child_token(),
                ));
            }
            _ = token.cancelled() => break,
        }
    }

    // Drain: wait for all active connections to finish their bridge drain window.
    while connections.join_next().await.is_some() {}
}

async fn connect_and_bridge(
    service: String,
    target: Arc<dyn Target>,
    stream: TcpStream,
    peer: SocketAddr,
    token: CancellationToken,
) {
    let mut backoff = Backoff::new();

    let remote = loop {
        info!(service = %service, %peer, "connect_attempt_started");

        let result = tokio::time::timeout(CONNECT_TIMEOUT, target.connect()).await;

        let err = match result {
            Ok(Ok(remote)) => {
                backoff.reset();
                break remote;
            }
            Ok(Err(e)) => e,
            Err(_) => anyhow::anyhow!("timed out after {CONNECT_TIMEOUT:?}"),
        };

        let delay = backoff.delay();
        warn!(service = %service, error = %err, "connect_attempt_failed");
        warn!(service = %service, delay_secs = delay.as_secs_f32(), "connect_retry_sleep");

        let mut peek_buf = [0u8; 1];
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = token.cancelled() => return,
            // peek doesn't consume bytes — Ok(0) means the client closed the connection.
            r = stream.peek(&mut peek_buf) => {
                if matches!(r, Ok(0)) {
                    return;
                }
            }
        }
    };

    info!(service = %service, "bridge_started");

    // Pin the bridge future so we can hand it to both the normal path and the
    // drain path without moving it twice.
    let mut bridge = pin!(bridge::run(stream, remote));

    let result = tokio::select! {
        r = bridge.as_mut() => r,
        _ = token.cancelled() => {
            tokio::time::timeout(DRAIN_TIMEOUT, bridge.as_mut())
                .await
                .unwrap_or(Ok(()))
        }
    };

    match result {
        Ok(()) => info!(service = %service, "bridge_closed"),
        Err(e) => warn!(service = %service, error = %e, "bridge_closed"),
    }
}

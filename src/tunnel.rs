use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff::Backoff;
use crate::bridge;
use crate::target::Target;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(
    service: String,
    target: Arc<dyn Target>,
    listener: TcpListener,
    token: CancellationToken,
) {
    info!(service = %service, target = %target.describe(), "listening");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!(service = %service, error = %e, "accept failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                            _ = token.cancelled() => return,
                        }
                        continue;
                    }
                };

                let target = Arc::clone(&target);
                let service = service.clone();
                let child = token.child_token();

                tokio::spawn(async move {
                    connect_and_bridge(&service, &*target, stream, peer, child).await;
                });
            }
            _ = token.cancelled() => return,
        }
    }
}

async fn connect_and_bridge(
    service: &str,
    target: &dyn Target,
    stream: TcpStream,
    peer: std::net::SocketAddr,
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

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = token.cancelled() => return,
            _ = stream.readable() => {
                let mut buf = [0u8; 1];
                if matches!(stream.try_read(&mut buf), Ok(0)) {
                    return; // client disconnected
                }
            }
        }
    };

    info!(service = %service, "bridge_started");

    match bridge::run(stream, remote).await {
        Ok(()) => info!(service = %service, "bridge_closed"),
        Err(e) => warn!(service = %service, error = %e, "bridge_closed"),
    }
}

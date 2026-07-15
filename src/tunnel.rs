use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff::Backoff;
use crate::bridge;
use crate::config::{ConnectionPolicy, HealthPolicy};
use crate::health::{ConnectionHealth, ServiceHealth};
use crate::target::Target;

pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(
    service: String,
    target: Arc<dyn Target>,
    listener: TcpListener,
    policy: watch::Receiver<ConnectionPolicy>,
    health_policy: watch::Receiver<HealthPolicy>,
    health: ServiceHealth,
    token: CancellationToken,
) {
    let connection = *policy.borrow();
    info!(
        service = %service,
        target = %target.describe(),
        connect_timeout_secs = connection.connect_timeout.as_secs_f32(),
        backoff_initial_secs = connection.backoff_initial.as_secs_f32(),
        backoff_max_secs = connection.backoff_max.as_secs_f32(),
        "listening"
    );

    let mut connections: JoinSet<()> = JoinSet::new();
    let probe = tokio::spawn(probe_target(
        service.clone(),
        Arc::clone(&target),
        health_policy,
        health.clone(),
        token.child_token(),
    ));

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!(service = %service, error = %e, "accept_failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                            _ = token.cancelled() => break,
                        }
                        continue;
                    }
                };

                // Sampled per connection, so an edit only affects connections accepted after it.
                let connection = *policy.borrow();

                connections.spawn(connect_and_bridge(
                    service.clone(),
                    Arc::clone(&target),
                    stream,
                    peer,
                    connection,
                    health.connection(),
                    token.child_token(),
                ));
            }
            _ = token.cancelled() => break,
        }
    }

    // Drain: wait for all active connections to finish their bridge drain window.
    while connections.join_next().await.is_some() {}
    let _ = probe.await;
}

async fn probe_target(
    service: String,
    target: Arc<dyn Target>,
    mut policy: watch::Receiver<HealthPolicy>,
    health: ServiceHealth,
    token: CancellationToken,
) {
    let mut backoff = Backoff::with_base(
        policy.borrow().probe_backoff_initial,
        policy.borrow().probe_backoff_max,
    );

    loop {
        let current = *policy.borrow();
        health.mark_target_probing();

        let result = tokio::time::timeout(current.probe_timeout, target.probe()).await;
        match result {
            Ok(Ok(())) => {
                backoff =
                    Backoff::with_base(current.probe_backoff_initial, current.probe_backoff_max);
                health.mark_target_reachable();
                info!(service = %service, "health_probe_succeeded");
                match wait_for_probe_delay(current.probe_interval, &mut policy, &token).await {
                    ProbeDelay::Elapsed => {}
                    ProbeDelay::Changed => {
                        let changed = *policy.borrow();
                        backoff = Backoff::with_base(
                            changed.probe_backoff_initial,
                            changed.probe_backoff_max,
                        );
                    }
                    ProbeDelay::Cancelled => return,
                }
            }
            Ok(Err(e)) => {
                let delay = backoff.delay();
                health.mark_target_unreachable(&e);
                warn!(
                    service = %service,
                    error = %e,
                    retry_secs = delay.as_secs_f32(),
                    "health_probe_failed"
                );
                match wait_for_probe_delay(delay, &mut policy, &token).await {
                    ProbeDelay::Elapsed => {}
                    ProbeDelay::Changed => {
                        let changed = *policy.borrow();
                        backoff = Backoff::with_base(
                            changed.probe_backoff_initial,
                            changed.probe_backoff_max,
                        );
                    }
                    ProbeDelay::Cancelled => return,
                }
            }
            Err(_) => {
                let err = anyhow::anyhow!("timed out after {:?}", current.probe_timeout);
                let delay = backoff.delay();
                health.mark_target_unreachable(&err);
                warn!(
                    service = %service,
                    error = %err,
                    retry_secs = delay.as_secs_f32(),
                    "health_probe_failed"
                );
                match wait_for_probe_delay(delay, &mut policy, &token).await {
                    ProbeDelay::Elapsed => {}
                    ProbeDelay::Changed => {
                        let changed = *policy.borrow();
                        backoff = Backoff::with_base(
                            changed.probe_backoff_initial,
                            changed.probe_backoff_max,
                        );
                    }
                    ProbeDelay::Cancelled => return,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeDelay {
    Elapsed,
    Changed,
    Cancelled,
}

async fn wait_for_probe_delay(
    delay: Duration,
    policy: &mut watch::Receiver<HealthPolicy>,
    token: &CancellationToken,
) -> ProbeDelay {
    tokio::select! {
        _ = tokio::time::sleep(delay) => ProbeDelay::Elapsed,
        _ = token.cancelled() => ProbeDelay::Cancelled,
        changed = policy.changed() => {
            if changed.is_ok() {
                ProbeDelay::Changed
            } else {
                ProbeDelay::Cancelled
            }
        },
    }
}

async fn connect_and_bridge(
    service: String,
    target: Arc<dyn Target>,
    stream: TcpStream,
    peer: SocketAddr,
    connection: ConnectionPolicy,
    mut health: ConnectionHealth,
    token: CancellationToken,
) {
    let mut backoff = Backoff::with_base(connection.backoff_initial, connection.backoff_max);

    let remote = loop {
        health.mark_connecting();
        info!(service = %service, %peer, "connect_attempt_started");

        let result = tokio::time::timeout(connection.connect_timeout, target.connect()).await;

        let err = match result {
            Ok(Ok(remote)) => {
                backoff.reset();
                break remote;
            }
            Ok(Err(e)) => e,
            Err(_) => anyhow::anyhow!("timed out after {:?}", connection.connect_timeout),
        };

        let delay = backoff.delay();
        health.mark_retrying(&err);
        warn!(service = %service, %peer, error = %err, "connect_attempt_failed");
        warn!(service = %service, %peer, delay_secs = delay.as_secs_f32(), "connect_retry_sleep");

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

    health.mark_up();
    info!(service = %service, %peer, "bridge_started");

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
        Ok(()) => info!(service = %service, %peer, "bridge_closed"),
        Err(e) => warn!(service = %service, %peer, error = %e, "bridge_closed"),
    }
}

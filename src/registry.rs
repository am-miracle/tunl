use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::ConnectionPolicy;
use crate::target::Target;

/// Why a task left the registry.
#[derive(Debug, PartialEq, Eq)]
pub enum ExitReason {
    Retired,
    Unexpected,
}

struct ActiveEntry {
    token: CancellationToken,
    address: SocketAddr,
}

/// Tracks running tunnel tasks by service name so one service can be
/// cancelled or restarted without touching the others.
///
/// A service that is stopped does not vanish immediately: its accept loop
/// stops, but its `TcpListener` is not dropped until `tunnel::run` finishes
/// draining any open bridges. Until then it sits in `retiring`, and `start`
/// waits for any retiring entry on the same port before binding, which is
/// what makes reusing a port across a reload safe instead of racy.
pub struct Registry {
    root: CancellationToken,
    active: HashMap<String, ActiveEntry>,
    retiring: HashMap<String, SocketAddr>,
    tasks: JoinSet<String>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            root: CancellationToken::new(),
            active: HashMap::new(),
            retiring: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn adopt(
        &mut self,
        name: String,
        address: SocketAddr,
        target: Arc<dyn Target>,
        listener: TcpListener,
        connection: ConnectionPolicy,
    ) {
        let token = self.root.child_token();
        let run_token = token.clone();
        let run_name = name.clone();

        self.tasks.spawn(async move {
            crate::tunnel::run(run_name.clone(), target, listener, connection, run_token).await;
            run_name
        });

        self.active.insert(name, ActiveEntry { token, address });
    }

    /// Bind `address` and adopt the result. If a just-stopped service is still
    /// draining on the same port, wait for it to finish first rather than
    /// racing its listener's teardown.
    pub async fn start(
        &mut self,
        name: String,
        address: SocketAddr,
        target: Arc<dyn Target>,
        connection: ConnectionPolicy,
    ) -> anyhow::Result<()> {
        self.await_port_free(address.port()).await;

        let listener = crate::listener::bind(address)
            .await
            .map_err(|e| anyhow::anyhow!("[{name}] failed to bind {address}: {e}"))?;

        self.adopt(name, address, target, listener, connection);
        Ok(())
    }

    pub fn stop(&mut self, name: &str) -> bool {
        let Some(entry) = self.active.remove(name) else {
            return false;
        };
        entry.token.cancel();
        self.retiring.insert(name.to_string(), entry.address);
        true
    }

    /// Cancelling the shared root cascades to every child token, including
    /// tasks that are still draining.
    pub fn cancel_all(&mut self) {
        self.root.cancel();
        for (name, entry) in self.active.drain() {
            self.retiring.insert(name, entry.address);
        }
    }

    pub async fn join_next(&mut self) -> Option<(String, ExitReason)> {
        loop {
            let joined = self.tasks.join_next().await?;
            match joined {
                Ok(name) => {
                    let reason = if self.retiring.remove(&name).is_some() {
                        ExitReason::Retired
                    } else {
                        // tunnel::run only returns after its token is
                        // cancelled. Getting here without us having stopped
                        // it means the task ended on its own; drop it rather
                        // than let stale bookkeeping claim a dead service.
                        self.active.remove(&name);
                        ExitReason::Unexpected
                    };
                    return Some((name, reason));
                }
                Err(e) => {
                    // A panic in one tunnel task should not take down the
                    // whole daemon. Log it and keep draining the rest.
                    warn!(error = %e, "registry_task_panicked");
                }
            }
        }
    }

    async fn await_port_free(&mut self, port: u16) {
        while self.retiring.values().any(|address| address.port() == port) {
            if self.join_next().await.is_none() {
                break;
            }
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

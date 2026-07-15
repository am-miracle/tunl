use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::ConnectionPolicy;
use crate::health::{HealthRegistry, ServiceHealth};
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
    health: ServiceHealth,
    // Paired with the Receiver tunnel::run samples per connection; see update_policy.
    policy: watch::Sender<ConnectionPolicy>,
}

struct RetiringEntry {
    name: String,
    address: SocketAddr,
    health: ServiceHealth,
}

struct TaskExit {
    name: String,
    health: ServiceHealth,
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
    health: HealthRegistry,
    active: HashMap<String, ActiveEntry>,
    retiring: Vec<RetiringEntry>,
    tasks: JoinSet<TaskExit>,
}

impl Registry {
    pub fn new() -> Self {
        Self::with_health(HealthRegistry::default())
    }

    pub fn with_health(health: HealthRegistry) -> Self {
        Self {
            root: CancellationToken::new(),
            health,
            active: HashMap::new(),
            retiring: Vec::new(),
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
        let service_health = self
            .health
            .register(name.clone(), address, target.describe());
        let run_health = service_health.clone();
        let task_health = service_health.clone();
        let (policy_tx, policy_rx) = watch::channel(connection);

        self.tasks.spawn(async move {
            crate::tunnel::run(
                run_name.clone(),
                target,
                listener,
                policy_rx,
                run_health,
                run_token,
            )
            .await;
            TaskExit {
                name: run_name,
                health: task_health,
            }
        });

        self.active.insert(
            name,
            ActiveEntry {
                token,
                address,
                health: service_health,
                policy: policy_tx,
            },
        );
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

    /// Push a new policy into a running service without restarting it.
    /// Existing connections keep the policy they started with; new
    /// connections accepted after this call see the update. Returns `false`
    /// if `name` is not currently active.
    pub fn update_policy(&mut self, name: &str, connection: ConnectionPolicy) -> bool {
        let Some(entry) = self.active.get(name) else {
            return false;
        };
        entry.policy.send_replace(connection);
        true
    }

    pub fn stop(&mut self, name: &str) -> bool {
        let Some(entry) = self.active.remove(name) else {
            return false;
        };
        entry.health.mark_draining();
        entry.token.cancel();
        self.retiring.push(RetiringEntry {
            name: name.to_string(),
            address: entry.address,
            health: entry.health,
        });
        true
    }

    /// Cancelling the shared root cascades to every child token, including
    /// tasks that are still draining.
    pub fn cancel_all(&mut self) {
        self.root.cancel();
        for (name, entry) in self.active.drain() {
            entry.health.mark_draining();
            self.retiring.push(RetiringEntry {
                name,
                address: entry.address,
                health: entry.health,
            });
        }
    }

    pub async fn join_next(&mut self) -> Option<(String, ExitReason)> {
        loop {
            let joined = self.tasks.join_next().await?;
            match joined {
                Ok(exit) => {
                    let retiring = self.retiring.iter().position(|entry| {
                        entry.name == exit.name && entry.health.is_same_generation(&exit.health)
                    });
                    let reason = if let Some(index) = retiring {
                        let retiring = self.retiring.remove(index);
                        self.health.remove(&retiring.health);
                        ExitReason::Retired
                    } else {
                        // tunnel::run only returns after its token is
                        // cancelled. Getting here without us having stopped
                        // it means the task ended on its own; drop it rather
                        // than let stale bookkeeping claim a dead service.
                        let is_active_generation = self
                            .active
                            .get(&exit.name)
                            .is_some_and(|entry| entry.health.is_same_generation(&exit.health));
                        if is_active_generation {
                            self.active.remove(&exit.name);
                            self.health.remove(&exit.health);
                        }
                        ExitReason::Unexpected
                    };
                    return Some((exit.name, reason));
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
        while self
            .retiring
            .iter()
            .any(|entry| entry.address.port() == port)
        {
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

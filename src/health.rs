use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// The service-level state shown by the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    Listening,
    Connecting,
    Up,
    Retrying,
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerStatus {
    Listening,
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetStatus {
    Unknown,
    Probing,
    Reachable,
    Unreachable,
}

#[derive(Debug, Clone)]
pub struct ServiceSnapshot {
    pub name: String,
    pub local_address: SocketAddr,
    pub target: String,
    pub status: ServiceStatus,
    pub listener_status: ListenerStatus,
    pub target_status: TargetStatus,
    pub active_connections: usize,
    pub last_error: Option<String>,
    pub status_age: Duration,
    pub target_status_age: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct HealthRegistry {
    services: Arc<Mutex<BTreeMap<String, ServiceHealth>>>,
}

impl HealthRegistry {
    pub fn register(
        &self,
        name: String,
        local_address: SocketAddr,
        target: String,
    ) -> ServiceHealth {
        let health = ServiceHealth {
            inner: Arc::new(Mutex::new(ServiceState {
                name: name.clone(),
                local_address,
                target,
                lifecycle: Lifecycle::Listening,
                connecting: 0,
                retrying: 0,
                active_connections: 0,
                has_connected: false,
                target_status: TargetProbeState::Unknown,
                last_error: None,
                updated_at: Instant::now(),
                target_updated_at: Instant::now(),
            })),
        };
        lock(&self.services).insert(name, health.clone());
        health
    }

    pub fn snapshots(&self) -> Vec<ServiceSnapshot> {
        lock(&self.services)
            .values()
            .map(ServiceHealth::snapshot)
            .collect()
    }

    /// Remove only this generation. A replacement with the same service name
    /// must survive when an older, draining tunnel exits later.
    pub fn remove(&self, health: &ServiceHealth) {
        let name = lock(&health.inner).name.clone();
        let mut services = lock(&self.services);
        if services
            .get(&name)
            .is_some_and(|current| Arc::ptr_eq(&current.inner, &health.inner))
        {
            services.remove(&name);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceHealth {
    inner: Arc<Mutex<ServiceState>>,
}

impl ServiceHealth {
    pub(crate) fn is_same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn connection(&self) -> ConnectionHealth {
        let mut state = lock(&self.inner);
        state.active_connections += 1;
        state.connecting += 1;
        state.updated_at = Instant::now();
        ConnectionHealth {
            service: self.clone(),
            phase: ConnectionPhase::Connecting,
        }
    }

    pub fn mark_draining(&self) {
        let mut state = lock(&self.inner);
        state.lifecycle = Lifecycle::Draining;
        state.updated_at = Instant::now();
    }

    pub fn mark_target_probing(&self) {
        let mut state = lock(&self.inner);
        state.target_status = TargetProbeState::Probing;
        state.target_updated_at = Instant::now();
    }

    pub fn mark_target_reachable(&self) {
        let mut state = lock(&self.inner);
        state.target_status = TargetProbeState::Reachable;
        state.last_error = None;
        state.target_updated_at = Instant::now();
        state.updated_at = Instant::now();
    }

    pub fn mark_target_unreachable(&self, error: &anyhow::Error) {
        let mut state = lock(&self.inner);
        state.target_status = TargetProbeState::Unreachable;
        state.last_error = Some(error.to_string());
        state.target_updated_at = Instant::now();
        state.updated_at = Instant::now();
    }

    pub fn snapshot(&self) -> ServiceSnapshot {
        let state = lock(&self.inner);
        ServiceSnapshot {
            name: state.name.clone(),
            local_address: state.local_address,
            target: state.target.clone(),
            status: state.status(),
            listener_status: state.listener_status(),
            target_status: state.target_status(),
            active_connections: state.active_connections,
            last_error: state.last_error.clone(),
            status_age: state.updated_at.elapsed(),
            target_status_age: state.target_updated_at.elapsed(),
        }
    }
}

/// RAII accounting for one accepted client. Dropping this value on any return
/// path decrements every counter associated with the connection.
#[derive(Debug)]
pub struct ConnectionHealth {
    service: ServiceHealth,
    phase: ConnectionPhase,
}

impl ConnectionHealth {
    pub fn mark_connecting(&mut self) {
        self.transition(ConnectionPhase::Connecting, None);
    }

    pub fn mark_retrying(&mut self, error: &anyhow::Error) {
        self.transition(ConnectionPhase::Retrying, Some(error.to_string()));
    }

    pub fn mark_up(&mut self) {
        self.transition(ConnectionPhase::Up, None);
    }

    fn transition(&mut self, next: ConnectionPhase, error: Option<String>) {
        if self.phase == next && error.is_none() {
            return;
        }

        let mut state = lock(&self.service.inner);
        state.leave(self.phase);
        state.enter(next);
        if let Some(error) = error {
            state.last_error = Some(error);
        } else if next == ConnectionPhase::Up && state.retrying == 0 {
            state.target_status = TargetProbeState::Reachable;
            state.target_updated_at = Instant::now();
            state.last_error = None;
        }
        state.updated_at = Instant::now();
        self.phase = next;
    }
}

impl Drop for ConnectionHealth {
    fn drop(&mut self) {
        let mut state = lock(&self.service.inner);
        state.leave(self.phase);
        state.active_connections = state.active_connections.saturating_sub(1);
        if state.retrying == 0 && state.target_status != TargetProbeState::Unreachable {
            state.last_error = None;
        }
        state.updated_at = Instant::now();
    }
}

#[derive(Debug)]
struct ServiceState {
    name: String,
    local_address: SocketAddr,
    target: String,
    lifecycle: Lifecycle,
    connecting: usize,
    retrying: usize,
    active_connections: usize,
    has_connected: bool,
    target_status: TargetProbeState,
    last_error: Option<String>,
    updated_at: Instant,
    target_updated_at: Instant,
}

impl ServiceState {
    fn status(&self) -> ServiceStatus {
        if self.lifecycle == Lifecycle::Draining {
            ServiceStatus::Draining
        } else if self.retrying > 0 {
            ServiceStatus::Retrying
        } else if self.has_connected {
            ServiceStatus::Up
        } else if self.connecting > 0 {
            ServiceStatus::Connecting
        } else {
            ServiceStatus::Listening
        }
    }

    fn listener_status(&self) -> ListenerStatus {
        match self.lifecycle {
            Lifecycle::Listening => ListenerStatus::Listening,
            Lifecycle::Draining => ListenerStatus::Draining,
        }
    }

    fn target_status(&self) -> TargetStatus {
        match self.target_status {
            TargetProbeState::Unknown => TargetStatus::Unknown,
            TargetProbeState::Probing => TargetStatus::Probing,
            TargetProbeState::Reachable => TargetStatus::Reachable,
            TargetProbeState::Unreachable => TargetStatus::Unreachable,
        }
    }

    fn enter(&mut self, phase: ConnectionPhase) {
        match phase {
            ConnectionPhase::Connecting => self.connecting += 1,
            ConnectionPhase::Retrying => self.retrying += 1,
            ConnectionPhase::Up => self.has_connected = true,
        }
    }

    fn leave(&mut self, phase: ConnectionPhase) {
        let counter = match phase {
            ConnectionPhase::Connecting => &mut self.connecting,
            ConnectionPhase::Retrying => &mut self.retrying,
            ConnectionPhase::Up => return,
        };
        *counter = counter.saturating_sub(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Listening,
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionPhase {
    Connecting,
    Retrying,
    Up,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetProbeState {
    Unknown,
    Probing,
    Reachable,
    Unreachable,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(registry: &HealthRegistry) -> ServiceHealth {
        registry.register(
            "api".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            "remote://api:80".to_string(),
        )
    }

    #[test]
    fn derives_status_from_connection_activity() {
        let registry = HealthRegistry::default();
        let service = service(&registry);
        assert_eq!(service.snapshot().status, ServiceStatus::Listening);
        assert_eq!(
            service.snapshot().listener_status,
            ListenerStatus::Listening
        );
        assert_eq!(service.snapshot().target_status, TargetStatus::Unknown);

        let mut connection = service.connection();
        assert_eq!(service.snapshot().status, ServiceStatus::Connecting);
        assert_eq!(service.snapshot().active_connections, 1);

        connection.mark_retrying(&anyhow::anyhow!("connection refused"));
        assert_eq!(service.snapshot().status, ServiceStatus::Retrying);
        assert_eq!(
            service.snapshot().last_error.as_deref(),
            Some("connection refused")
        );

        connection.mark_connecting();
        connection.mark_up();
        assert_eq!(service.snapshot().status, ServiceStatus::Up);
        assert_eq!(service.snapshot().target_status, TargetStatus::Reachable);
        assert!(service.snapshot().last_error.is_none());

        drop(connection);
        assert_eq!(service.snapshot().active_connections, 0);
        assert_eq!(service.snapshot().status, ServiceStatus::Up);
    }

    #[test]
    fn old_generation_cannot_remove_replacement() {
        let registry = HealthRegistry::default();
        let old = service(&registry);
        let replacement = service(&registry);

        registry.remove(&old);
        assert_eq!(registry.snapshots().len(), 1);
        assert_eq!(registry.snapshots()[0].name, "api");

        registry.remove(&replacement);
        assert!(registry.snapshots().is_empty());
    }

    #[test]
    fn draining_overrides_connection_status() {
        let registry = HealthRegistry::default();
        let service = service(&registry);
        let _connection = service.connection();
        service.mark_draining();
        assert_eq!(service.snapshot().status, ServiceStatus::Draining);
        assert_eq!(service.snapshot().listener_status, ListenerStatus::Draining);
    }

    #[test]
    fn target_reachability_is_tracked_separately_from_listener_status() {
        let registry = HealthRegistry::default();
        let service = service(&registry);

        service.mark_target_probing();
        assert_eq!(
            service.snapshot().listener_status,
            ListenerStatus::Listening
        );
        assert_eq!(service.snapshot().target_status, TargetStatus::Probing);

        service.mark_target_unreachable(&anyhow::anyhow!("connection refused"));
        let snapshot = service.snapshot();
        assert_eq!(snapshot.listener_status, ListenerStatus::Listening);
        assert_eq!(snapshot.target_status, TargetStatus::Unreachable);
        assert_eq!(snapshot.last_error.as_deref(), Some("connection refused"));

        service.mark_target_reachable();
        let snapshot = service.snapshot();
        assert_eq!(snapshot.listener_status, ListenerStatus::Listening);
        assert_eq!(snapshot.target_status, TargetStatus::Reachable);
        assert!(snapshot.last_error.is_none());
    }
}

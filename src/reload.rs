use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{info, warn};

use crate::config::Service;
use crate::registry::Registry;
use crate::target::Target;

/// Service names added, removed, or changed between two configs.
#[derive(Debug, Default, PartialEq)]
pub struct ReloadPlan {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

impl ReloadPlan {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Compare two service maps by name.
pub fn diff(old: &HashMap<String, Service>, new: &HashMap<String, Service>) -> ReloadPlan {
    let mut plan = ReloadPlan::default();

    for (name, new_service) in new {
        match old.get(name) {
            None => plan.added.push(name.clone()),
            Some(old_service) if old_service != new_service => plan.changed.push(name.clone()),
            Some(_) => {}
        }
    }

    for name in old.keys() {
        if !new.contains_key(name) {
            plan.removed.push(name.clone());
        }
    }

    // HashMap iteration order is unspecified; sort so callers (and tests) get
    // a deterministic result regardless of insertion order.
    plan.added.sort();
    plan.removed.sort();
    plan.changed.sort();
    plan
}

/// Reconcile `current` to the services that actually started after applying a
/// reload. Failed starts stay out of `current`, so the next reload retries them.
pub async fn apply(
    registry: &mut Registry,
    current: &mut HashMap<String, Service>,
    mut new_services: HashMap<String, Service>,
) -> ReloadPlan {
    for (name, service) in &new_services {
        if let Err(e) = crate::target::from_uri(name, &service.target) {
            warn!(error = %e, "config_reload_rejected");
            return ReloadPlan::default();
        }
    }

    let plan = diff(current, &new_services);
    if plan.is_empty() {
        return plan;
    }

    let failed = apply_plan(registry, &plan, &new_services).await;
    for name in &failed {
        new_services.remove(name);
        if let Some(old) = current.remove(name) {
            new_services.insert(name.clone(), old);
        }
    }

    *current = new_services;
    plan
}

async fn apply_plan(
    registry: &mut Registry,
    plan: &ReloadPlan,
    services: &HashMap<String, Service>,
) -> Vec<String> {
    let mut failed = Vec::new();

    // Stop all changed services before starting replacements so port swaps
    // do not race an old listener that is still active.
    for name in &plan.removed {
        if registry.stop(name) {
            info!(service = %name, "service_removed");
        }
    }
    for name in &plan.changed {
        registry.stop(name);
    }

    for name in &plan.changed {
        if !start_one(registry, name, services, true).await {
            failed.push(name.clone());
        }
    }
    for name in &plan.added {
        if !start_one(registry, name, services, false).await {
            failed.push(name.clone());
        }
    }

    failed
}

async fn start_one(
    registry: &mut Registry,
    name: &str,
    services: &HashMap<String, Service>,
    is_restart: bool,
) -> bool {
    let Some(service) = services.get(name) else {
        return false;
    };

    let target: Arc<dyn Target> = match crate::target::from_uri(name, &service.target) {
        Ok(t) => Arc::from(t),
        Err(e) => {
            warn!(service = %name, error = %e, "reload_target_invalid");
            return false;
        }
    };

    let address = SocketAddr::new(service.bind_address, service.local_port as u16);
    if !service.bind_address.is_loopback() {
        warn!(service = %name, %address, "remote_listener_enabled");
    }
    match registry
        .start(name.to_string(), address, target, service.connection)
        .await
    {
        Ok(()) if is_restart => {
            info!(service = %name, %address, "service_restarted");
            true
        }
        Ok(()) => {
            info!(service = %name, %address, "service_added");
            true
        }
        Err(e) => {
            warn!(service = %name, error = %e, "reload_bind_failed");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc(port: i64, target: &str) -> Service {
        Service {
            local_port: port,
            bind_address: "127.0.0.1".parse().unwrap(),
            allow_remote_connections: false,
            connection: crate::config::ConnectionPolicy::default(),
            target: target.to_string(),
        }
    }

    // A fixed literal port would flake if anything else on the machine
    // happened to already be listening on it. Ask the OS for a free one and
    // release it immediately instead, the same technique tests/tunnel_test.rs
    // and tests/registry_test.rs use.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn detects_added_service() {
        let old = HashMap::new();
        let mut new = HashMap::new();
        new.insert("cache".to_string(), svc(9000, "docker://redis:6379"));

        let plan = diff(&old, &new);
        assert_eq!(plan.added, vec!["cache".to_string()]);
        assert!(plan.removed.is_empty());
        assert!(plan.changed.is_empty());
    }

    #[test]
    fn detects_removed_service() {
        let mut old = HashMap::new();
        old.insert("cache".to_string(), svc(9000, "docker://redis:6379"));
        let new = HashMap::new();

        let plan = diff(&old, &new);
        assert_eq!(plan.removed, vec!["cache".to_string()]);
        assert!(plan.added.is_empty());
        assert!(plan.changed.is_empty());
    }

    #[test]
    fn detects_changed_service() {
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));
        let mut new = HashMap::new();
        new.insert(
            "api".to_string(),
            svc(8080, "kubectl://default/app=api:8080"),
        );

        let plan = diff(&old, &new);
        assert_eq!(plan.changed, vec!["api".to_string()]);
        assert!(plan.added.is_empty());
        assert!(plan.removed.is_empty());
    }

    #[test]
    fn detects_bind_address_change() {
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "remote://api.internal:8080"));

        let mut changed = svc(8080, "remote://api.internal:8080");
        changed.bind_address = "::1".parse().unwrap();
        let mut new = HashMap::new();
        new.insert("api".to_string(), changed);

        assert_eq!(diff(&old, &new).changed, vec!["api".to_string()]);
    }

    #[test]
    fn detects_connection_policy_change() {
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "remote://api.internal:8080"));

        let mut changed = svc(8080, "remote://api.internal:8080");
        changed.connection.connect_timeout = std::time::Duration::from_secs(3);
        let mut new = HashMap::new();
        new.insert("api".to_string(), changed);

        assert_eq!(diff(&old, &new).changed, vec!["api".to_string()]);
    }

    #[test]
    fn unchanged_service_is_not_in_any_list() {
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));
        let mut new = HashMap::new();
        new.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));

        assert!(diff(&old, &new).is_empty());
    }

    #[test]
    fn map_insertion_order_does_not_affect_result() {
        // Same two services, built in different insertion order. HashMap
        // gives no iteration-order guarantee, so the diff must not depend on
        // it, only on the (name, value) pairs.
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));
        old.insert("cache".to_string(), svc(9000, "docker://redis:6379"));

        let mut new = HashMap::new();
        new.insert("cache".to_string(), svc(9000, "docker://redis:6379"));
        new.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));

        assert!(diff(&old, &new).is_empty());
    }

    #[test]
    fn mixed_add_remove_and_change_together() {
        let mut old = HashMap::new();
        old.insert("api".to_string(), svc(8080, "kubectl://default/api-0:8080"));
        old.insert("gone".to_string(), svc(7000, "remote://old-host:7000"));

        let mut new = HashMap::new();
        new.insert(
            "api".to_string(),
            svc(8080, "kubectl://default/app=api:8080"),
        );
        new.insert("cache".to_string(), svc(9000, "docker://redis:6379"));

        let plan = diff(&old, &new);
        assert_eq!(plan.added, vec!["cache".to_string()]);
        assert_eq!(plan.removed, vec!["gone".to_string()]);
        assert_eq!(plan.changed, vec!["api".to_string()]);
    }

    #[tokio::test]
    async fn apply_commits_current_on_full_success() {
        let mut registry = Registry::new();
        let mut current = HashMap::new();

        let port = free_port();
        let mut new_services = HashMap::new();
        new_services.insert("echo".to_string(), svc(port as i64, "remote://127.0.0.1:1"));

        let plan = apply(&mut registry, &mut current, new_services).await;
        assert_eq!(plan.added, vec!["echo".to_string()]);
        assert_eq!(current.get("echo").unwrap().local_port, port as i64);
    }

    #[tokio::test]
    async fn failed_add_is_not_committed_and_is_retried_next_time() {
        let port = free_port();
        let _blocker = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();

        let mut registry = Registry::new();
        let mut current = HashMap::new();

        let mut new_services = HashMap::new();
        new_services.insert("echo".to_string(), svc(port as i64, "remote://127.0.0.1:1"));

        let plan = apply(&mut registry, &mut current, new_services).await;
        assert_eq!(plan.added, vec!["echo".to_string()]);
        assert!(!current.contains_key("echo"));

        // A later reload of the same file must retry the failed add.
        let mut retry_services = HashMap::new();
        retry_services.insert("echo".to_string(), svc(port as i64, "remote://127.0.0.1:1"));
        assert_eq!(
            diff(&current, &retry_services).added,
            vec!["echo".to_string()]
        );
    }

    #[tokio::test]
    async fn failed_change_is_not_committed_and_is_retried_next_time() {
        let old_port = free_port();
        let new_port = free_port();

        let mut registry = Registry::new();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", old_port))
            .await
            .unwrap();
        let target: Arc<dyn Target> =
            Arc::from(crate::target::from_uri("echo", "remote://127.0.0.1:1").unwrap());
        registry.adopt(
            "echo".to_string(),
            SocketAddr::from(([127, 0, 0, 1], old_port)),
            target,
            listener,
            crate::config::ConnectionPolicy::default(),
        );

        let mut current = HashMap::new();
        current.insert(
            "echo".to_string(),
            svc(old_port as i64, "remote://127.0.0.1:1"),
        );

        let _blocker = tokio::net::TcpListener::bind(("127.0.0.1", new_port))
            .await
            .unwrap();

        let mut new_services = HashMap::new();
        new_services.insert(
            "echo".to_string(),
            svc(new_port as i64, "remote://127.0.0.1:1"),
        );

        let plan = apply(&mut registry, &mut current, new_services).await;
        assert_eq!(plan.changed, vec!["echo".to_string()]);

        assert_eq!(current.get("echo").unwrap().local_port, old_port as i64);

        // A later reload of the same file must retry the failed change.
        let mut retry_services = HashMap::new();
        retry_services.insert(
            "echo".to_string(),
            svc(new_port as i64, "remote://127.0.0.1:1"),
        );
        assert_eq!(
            diff(&current, &retry_services).changed,
            vec!["echo".to_string()]
        );
    }

    #[tokio::test]
    async fn swapping_ports_between_two_changed_services_succeeds() {
        // Regression test for review feedback: two active services trade
        // ports in a single edit. Before the fix, apply_plan stopped and
        // started each changed service one at a time, so the
        // alphabetically-first one would try to bind the port the other one
        // was still actively holding, and fail. Both must now succeed
        // because every changed service is stopped before any of them start.
        let port_a = free_port();
        let port_b = free_port();

        let mut registry = Registry::new();

        let listener_a = tokio::net::TcpListener::bind(("127.0.0.1", port_a))
            .await
            .unwrap();
        let target_a: Arc<dyn Target> =
            Arc::from(crate::target::from_uri("svc-a", "remote://127.0.0.1:1").unwrap());
        registry.adopt(
            "svc-a".to_string(),
            SocketAddr::from(([127, 0, 0, 1], port_a)),
            target_a,
            listener_a,
            crate::config::ConnectionPolicy::default(),
        );

        let listener_b = tokio::net::TcpListener::bind(("127.0.0.1", port_b))
            .await
            .unwrap();
        let target_b: Arc<dyn Target> =
            Arc::from(crate::target::from_uri("svc-b", "remote://127.0.0.1:1").unwrap());
        registry.adopt(
            "svc-b".to_string(),
            SocketAddr::from(([127, 0, 0, 1], port_b)),
            target_b,
            listener_b,
            crate::config::ConnectionPolicy::default(),
        );

        let mut current = HashMap::new();
        current.insert(
            "svc-a".to_string(),
            svc(port_a as i64, "remote://127.0.0.1:1"),
        );
        current.insert(
            "svc-b".to_string(),
            svc(port_b as i64, "remote://127.0.0.1:1"),
        );

        // Swap: svc-a takes svc-b's port, svc-b takes svc-a's port.
        let mut new_services = HashMap::new();
        new_services.insert(
            "svc-a".to_string(),
            svc(port_b as i64, "remote://127.0.0.1:1"),
        );
        new_services.insert(
            "svc-b".to_string(),
            svc(port_a as i64, "remote://127.0.0.1:1"),
        );

        let plan = apply(&mut registry, &mut current, new_services).await;
        assert_eq!(plan.changed, vec!["svc-a".to_string(), "svc-b".to_string()]);

        // Both must have actually landed on their new ports, not been
        // silently left on the old one after a bind failure.
        assert_eq!(current.get("svc-a").unwrap().local_port, port_b as i64);
        assert_eq!(current.get("svc-b").unwrap().local_port, port_a as i64);
    }
}

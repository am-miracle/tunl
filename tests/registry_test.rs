use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tunl::config::{ConnectionPolicy, HealthPolicy};
use tunl::health::HealthRegistry;
use tunl::io::AsyncReadWrite;
use tunl::registry::{ExitReason, Registry};
use tunl::target::Target;

// Registry::start takes a port as input rather than handing back whichever
// one it bound, unlike a bare TcpListener, so tests can't just bind ":0" and
// read the assigned port back afterward. Ask the OS for a free one up front
// instead, then release it immediately so Registry can rebind it. Fixed
// literal ports would flake if anything else on the machine happened to be
// listening on one already.
fn free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

// A target that always connects and echoes bytes back, so tests can prove a
// registered service is genuinely serving traffic, not just bound.
#[derive(Debug)]
struct EchoTarget;

#[async_trait]
impl Target for EchoTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let (local, remote) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let (mut r, mut w) = tokio::io::split(local);
            tokio::io::copy(&mut r, &mut w).await.ok();
        });
        Ok(Box::new(remote))
    }

    fn describe(&self) -> String {
        "fake://echo".to_string()
    }

    async fn probe(&self) -> Option<anyhow::Result<()>> {
        Some(Ok(()))
    }
}

fn echo() -> Arc<dyn Target> {
    Arc::new(EchoTarget)
}

// Fails `failures` times, then connects like EchoTarget. Backoff-sensitive:
// how long a connection takes to succeed depends on the retry policy in
// effect when the connection is accepted.
#[derive(Debug)]
struct FlakyTarget {
    failures_left: Mutex<usize>,
}

impl FlakyTarget {
    fn new(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            failures_left: Mutex::new(failures),
        })
    }
}

#[async_trait]
impl Target for FlakyTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        {
            let mut left = self.failures_left.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                anyhow::bail!("fake connect failure");
            }
        }
        let (local, remote) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let (mut r, mut w) = tokio::io::split(local);
            tokio::io::copy(&mut r, &mut w).await.ok();
        });
        Ok(Box::new(remote))
    }

    fn describe(&self) -> String {
        "fake://flaky".to_string()
    }

    async fn probe(&self) -> Option<anyhow::Result<()>> {
        Some(Ok(()))
    }
}

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

async fn round_trip(port: u16) {
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}

#[tokio::test]
async fn start_serves_traffic_and_stop_drains_it() {
    let port = free_port();
    let mut registry = Registry::new();
    registry
        .start(
            "svc".to_string(),
            localhost(port),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();

    round_trip(port).await;
    assert_eq!(registry.task_count(), 1);

    registry.stop("svc");
    let (name, reason) = registry.join_next().await.unwrap();
    assert_eq!(name, "svc");
    assert_eq!(reason, ExitReason::Retired);
    assert_eq!(registry.task_count(), 0);
}

#[tokio::test]
async fn restarting_a_service_on_the_same_port_does_not_race_the_old_listener() {
    // Regression test for the port re-bind race described in CONTRIBUTING:
    // stopping a service does not free its port immediately (tunnel::run
    // drains before returning), so starting a replacement on the same port
    // must wait rather than fail with "address in use."
    let port = free_port();
    let mut registry = Registry::new();
    registry
        .start(
            "old".to_string(),
            localhost(port),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();
    round_trip(port).await;

    registry.stop("old");
    // No sleep here on purpose: start() itself must wait for "old" to finish
    // draining before it binds the same port for "new".
    registry
        .start(
            "new".to_string(),
            localhost(port),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();

    round_trip(port).await;
    assert_eq!(registry.task_count(), 1);
}

#[tokio::test]
async fn independent_services_do_not_affect_each_other() {
    let port_a = free_port();
    let port_b = free_port();
    let mut registry = Registry::new();
    registry
        .start(
            "a".to_string(),
            localhost(port_a),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();
    registry
        .start(
            "b".to_string(),
            localhost(port_b),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();

    registry.stop("a");
    let (name, reason) = registry.join_next().await.unwrap();
    assert_eq!(name, "a");
    assert_eq!(reason, ExitReason::Retired);

    // "b" was never touched and is still serving traffic.
    round_trip(port_b).await;
    assert_eq!(registry.task_count(), 1);
}

#[tokio::test]
async fn cancel_all_drains_every_service() {
    let port_a = free_port();
    let port_b = free_port();
    let mut registry = Registry::new();
    registry
        .start(
            "a".to_string(),
            localhost(port_a),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();
    registry
        .start(
            "b".to_string(),
            localhost(port_b),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();

    registry.cancel_all();

    let mut seen = Vec::new();
    while let Some((name, reason)) = registry.join_next().await {
        assert_eq!(reason, ExitReason::Retired);
        seen.push(name);
    }
    seen.sort();
    assert_eq!(seen, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(registry.task_count(), 0);
}

#[tokio::test]
async fn update_policy_applies_to_new_connections_without_restarting() {
    // Adopt with a policy that is deliberately too slow for the test
    // deadline below, then update it before ever connecting a client. If
    // update_policy did nothing and the connection still used the slow
    // policy sampled at adopt time, this would time out.
    let port = free_port();
    let mut registry = Registry::new();
    let slow = ConnectionPolicy {
        connect_timeout: Duration::from_secs(5),
        backoff_initial: Duration::from_secs(2),
        backoff_max: Duration::from_secs(2),
    };
    registry
        .start(
            "svc".to_string(),
            localhost(port),
            FlakyTarget::new(1),
            slow,
            HealthPolicy::default(),
        )
        .await
        .unwrap();

    let fast = ConnectionPolicy {
        connect_timeout: Duration::from_secs(5),
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_millis(10),
    };
    assert!(registry.update_policy("svc", fast, HealthPolicy::default()));

    tokio::time::timeout(Duration::from_millis(500), round_trip(port))
        .await
        .expect("connection did not use the updated (fast) backoff policy");

    // No stop/start happened: it is still the one original task.
    assert_eq!(registry.task_count(), 1);
}

#[tokio::test]
async fn update_policy_returns_false_for_an_unknown_service() {
    let mut registry = Registry::new();
    assert!(!registry.update_policy(
        "does-not-exist",
        ConnectionPolicy::default(),
        HealthPolicy::default()
    ));
}

#[tokio::test]
async fn rapid_same_name_restarts_track_each_retiring_generation() {
    let first_port = free_port();
    let second_port = free_port();
    let health = HealthRegistry::default();
    let mut registry = Registry::with_health_probes(health.clone(), true);

    registry
        .start(
            "svc".to_string(),
            localhost(first_port),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();
    assert!(registry.stop("svc"));

    registry
        .start(
            "svc".to_string(),
            localhost(second_port),
            echo(),
            ConnectionPolicy::default(),
            HealthPolicy::default(),
        )
        .await
        .unwrap();
    assert!(registry.stop("svc"));

    for _ in 0..2 {
        let (name, reason) = registry.join_next().await.unwrap();
        assert_eq!(name, "svc");
        assert_eq!(reason, ExitReason::Retired);
    }
    assert!(health.snapshots().is_empty());
}

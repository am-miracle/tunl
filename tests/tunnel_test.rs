use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tunl::backoff::Backoff;
use tunl::config::ConnectionPolicy;
use tunl::io::AsyncReadWrite;
use tunl::target::Target;

#[derive(Debug)]
struct FakeTarget {
    failures_left: Mutex<usize>,
}

impl FakeTarget {
    fn new(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            failures_left: Mutex::new(failures),
        })
    }
}

#[async_trait]
impl Target for FakeTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        {
            let mut left = self.failures_left.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                anyhow::bail!("fake connect failure");
            }
        }
        // Return an in-memory echo stream: bytes written to one end come back
        // from the other, which lets client↔tunnel↔fake round-trips work.
        let (local, remote) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let (mut r, mut w) = tokio::io::split(local);
            tokio::io::copy(&mut r, &mut w).await.ok();
        });
        Ok(Box::new(remote))
    }

    fn describe(&self) -> String {
        "fake://target".to_string()
    }
}

async fn spawn_tunnel_with_policy(
    target: Arc<FakeTarget>,
    connection: ConnectionPolicy,
) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token = CancellationToken::new();
    let target: Arc<dyn Target> = target;
    tokio::spawn(tunl::tunnel::run(
        "test".to_string(),
        target,
        listener,
        connection,
        token.child_token(),
    ));
    (port, token)
}

async fn spawn_tunnel(target: Arc<FakeTarget>) -> (u16, CancellationToken) {
    spawn_tunnel_with_policy(target, ConnectionPolicy::default()).await
}

#[test]
fn backoff_sequence_and_reset() {
    let mut b = Backoff::with_base(Duration::from_secs(1), Duration::from_secs(15));
    assert_eq!(b.delay(), Duration::from_secs(1));
    assert_eq!(b.delay(), Duration::from_secs(2));
    assert_eq!(b.delay(), Duration::from_secs(4));
    assert_eq!(b.delay(), Duration::from_secs(8));
    assert_eq!(b.delay(), Duration::from_secs(15));
    assert_eq!(b.delay(), Duration::from_secs(15)); // stays capped

    b.reset();
    assert_eq!(b.delay(), Duration::from_secs(1)); // back to base
}

// ── tunnel integration tests ──────────────────────────────────────────────────

#[tokio::test]
async fn bytes_flow_through_fake_target() {
    let (port, _token) = spawn_tunnel(FakeTarget::new(0)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        client.write_all(b"hello").await.unwrap();
        let mut buf = vec![0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    })
    .await
    .expect("timed out");
}

#[tokio::test]
async fn retries_on_connect_failure_then_succeeds() {
    // FakeTarget fails once (triggers 1s backoff sleep) then succeeds.
    let (port, _token) = spawn_tunnel(FakeTarget::new(1)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Allow up to 8 seconds — 1 failure means 1s real sleep before success.
    tokio::time::timeout(Duration::from_secs(8), async {
        client.write_all(b"retry").await.unwrap();
        let mut buf = vec![0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"retry");
    })
    .await
    .expect("timed out — retry loop did not connect after one failure");
}

#[tokio::test]
async fn tunnel_accept_loop_stops_on_cancel() {
    // Spawn a tunnel with a target that always fails (so no bridges are open).
    // Cancel the token and verify that the tunnel task finishes quickly.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token = CancellationToken::new();
    let target: Arc<dyn Target> = FakeTarget::new(usize::MAX);

    let handle = tokio::spawn(tunl::tunnel::run(
        "test".to_string(),
        target,
        listener,
        ConnectionPolicy::default(),
        token.child_token(),
    ));

    token.cancel();

    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("tunnel task did not exit within 1s after cancellation")
        .expect("tunnel task panicked");
}

#[tokio::test]
async fn custom_backoff_policy_is_used_for_retries() {
    let (port, _token) = spawn_tunnel_with_policy(
        FakeTarget::new(1),
        ConnectionPolicy {
            connect_timeout: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(10),
        },
    )
    .await;

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        client.write_all(b"retry").await.unwrap();
        let mut buf = vec![0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"retry");
    })
    .await
    .expect("custom retry policy was not applied");
}

#[tokio::test]
async fn shutdown_closes_active_bridge() {
    // Establish a live bridge, then cancel. The client should receive EOF within
    // DRAIN_TIMEOUT + a small buffer.
    let (port, token) = spawn_tunnel(FakeTarget::new(0)).await;
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Write something so the bridge is definitely active.
    client.write_all(b"ping").await.unwrap();
    let mut buf = vec![0u8; 4];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    token.cancel();

    let deadline = tunl::tunnel::DRAIN_TIMEOUT + Duration::from_secs(1);
    tokio::time::timeout(deadline, async {
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "expected EOF after shutdown");
    })
    .await
    .expect("bridge was not closed within drain deadline");
}

#[tokio::test]
async fn cancelling_token_stops_retry_loop() {
    // FakeTarget always fails — tunnel will keep retrying until cancelled.
    let (port, token) = spawn_tunnel(FakeTarget::new(usize::MAX)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Give the tunnel a moment to attempt and fail at least once, then cancel.
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    // After cancellation the client connection should close.
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut buf = vec![0u8; 1];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "expected EOF after cancellation");
    })
    .await
    .expect("timed out — connection was not closed after cancellation");
}

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tunl::backoff::Backoff;
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

async fn spawn_tunnel(target: Arc<FakeTarget>) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token = CancellationToken::new();
    let target: Arc<dyn Target> = target;
    tokio::spawn(tunl::tunnel::run(
        "test".to_string(),
        target,
        listener,
        token.child_token(),
    ));
    (port, token)
}

#[test]
fn backoff_sequence_and_reset() {
    let mut b = Backoff::new();
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

    // Allow up to 5 seconds — 1 failure means 1s real sleep before success.
    tokio::time::timeout(Duration::from_secs(5), async {
        client.write_all(b"retry").await.unwrap();
        let mut buf = vec![0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"retry");
    })
    .await
    .expect("timed out — retry loop did not connect after one failure");
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

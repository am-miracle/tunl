use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

// Boots a TCP echo server that reflects every byte back to the sender.
async fn spawn_echo_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut r, mut w) = stream.split();
                tokio::io::copy(&mut r, &mut w).await.ok();
            });
        }
    });
    port
}

#[tokio::test]
async fn echo_round_trip() {
    let echo_port = spawn_echo_server().await;

    // Pre-bind the tunnel listener before spawning the tunnel task so
    // kernel queues incoming connections even before accept() is called.
    let tunnel_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel_port = tunnel_listener.local_addr().unwrap().port();

    let target: Arc<dyn tunl::target::Target> = Arc::from(
        tunl::target::from_uri("test", &format!("remote://127.0.0.1:{echo_port}")).unwrap(),
    );

    let (_policy_tx, policy_rx) =
        tokio::sync::watch::channel(tunl::config::ConnectionPolicy::default());
    tokio::spawn(tunl::tunnel::run(
        "test".to_string(),
        target,
        tunnel_listener,
        policy_rx,
        CancellationToken::new(),
    ));

    let mut client = TcpStream::connect(("127.0.0.1", tunnel_port))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        client.write_all(b"hello tunl").await.unwrap();

        let mut buf = vec![0u8; 10];
        client.read_exact(&mut buf).await.unwrap();

        assert_eq!(&buf, b"hello tunl");
    })
    .await
    .expect("echo round-trip timed out");
}

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tunl::bridge;

// tokio::io::duplex(n) creates two in-memory streams linked together:
// bytes written to one side can be read from the other. No sockets, no OS
// involvement. All tests here use this to stay completely off the network.

#[tokio::test]
async fn bytes_written_to_local_arrive_at_target() {
    let (local, mut local_peer) = tokio::io::duplex(1024);
    let (target, mut target_peer) = tokio::io::duplex(1024);

    tokio::spawn(bridge::run(local, target));

    local_peer.write_all(b"hello").await.unwrap();

    let mut buf = [0u8; 5];
    target_peer.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
}

#[tokio::test]
async fn bytes_written_to_target_arrive_at_local() {
    let (local, mut local_peer) = tokio::io::duplex(1024);
    let (target, mut target_peer) = tokio::io::duplex(1024);

    tokio::spawn(bridge::run(local, target));

    target_peer.write_all(b"world").await.unwrap();

    let mut buf = [0u8; 5];
    local_peer.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"world");
}

#[tokio::test]
async fn closing_one_side_terminates_the_bridge() {
    let (local, local_peer) = tokio::io::duplex(1024);
    let (target, target_peer) = tokio::io::duplex(1024);

    let handle = tokio::spawn(bridge::run(local, target));

    // Dropping both peers closes the connections. The bridge should notice
    // and exit rather than hang waiting for data that will never come.
    drop(local_peer);
    drop(target_peer);

    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("bridge should stop when peers close, not hang");
    assert!(result.is_ok(), "bridge task panicked");
}

#[tokio::test]
async fn large_payload_copies_intact() {
    // 256 KB through a 1 KB internal duplex buffer forces copy_bidirectional
    // to loop many times, proving it handles payloads larger than any single
    // buffer correctly.
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let (local, mut local_peer) = tokio::io::duplex(1024);
    let (target, mut target_peer) = tokio::io::duplex(1024);

    tokio::spawn(bridge::run(local, target));

    // Writer runs in its own task so it doesn't deadlock against the reader
    // below. Without this, write_all would block waiting for the buffer to
    // drain while read_exact is never reached.
    tokio::spawn(async move {
        local_peer.write_all(&payload).await.unwrap();
    });

    let mut received = vec![0u8; expected.len()];
    target_peer.read_exact(&mut received).await.unwrap();
    assert_eq!(received, expected);
}

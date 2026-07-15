use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tunl::config::ConnectionPolicy;
use tunl::health::HealthRegistry;
use tunl::target::Target;

fn real_backends_enabled() -> bool {
    std::env::var_os("TUNL_REAL_BACKENDS").is_some()
}

fn test_policy() -> ConnectionPolicy {
    ConnectionPolicy {
        connect_timeout: Duration::from_secs(10),
        backoff_initial: Duration::from_millis(100),
        backoff_max: Duration::from_secs(1),
    }
}

async fn assert_target_response(
    service: &str,
    target_uri: &str,
    request: &[u8],
    expected: &str,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let target: Arc<dyn Target> = Arc::from(tunl::target::from_uri(service, target_uri)?);
    let token = CancellationToken::new();
    let (_policy_tx, policy_rx) = tokio::sync::watch::channel(test_policy());
    let health = HealthRegistry::default().register(
        service.to_string(),
        ([127, 0, 0, 1], port).into(),
        target.describe(),
    );
    let handle = tokio::spawn(tunl::tunnel::run(
        service.to_string(),
        target,
        listener,
        policy_rx,
        None,
        health,
        token.child_token(),
    ));

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let mut client = TcpStream::connect(("127.0.0.1", port)).await?;
        if !request.is_empty() {
            client.write_all(request).await?;
        }

        let mut response = String::new();
        client.read_to_string(&mut response).await?;
        anyhow::ensure!(
            response.contains(expected),
            "response did not contain {expected:?}: {response:?}"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await;

    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle).await??;
    result??;

    Ok(())
}

#[tokio::test]
async fn docker_demo_container_returns_bytes() -> anyhow::Result<()> {
    if !real_backends_enabled() {
        return Ok(());
    }

    assert_target_response(
        "docker-demo",
        "docker://tunl-demo:8000",
        b"",
        "hello from container",
    )
    .await
}

#[tokio::test]
async fn kubernetes_demo_pod_name_returns_bytes() -> anyhow::Result<()> {
    if !real_backends_enabled() {
        return Ok(());
    }

    assert_target_response(
        "web-pod",
        "kubectl://default/web-0:8080",
        b"GET / HTTP/1.1\r\nHost: web-0\r\nConnection: close\r\n\r\n",
        "hello from web-0",
    )
    .await
}

#[tokio::test]
async fn kubernetes_demo_label_selector_returns_bytes() -> anyhow::Result<()> {
    if !real_backends_enabled() {
        return Ok(());
    }

    assert_target_response(
        "web-label",
        "kubectl://default/app=web:8080",
        b"GET / HTTP/1.1\r\nHost: web\r\nConnection: close\r\n\r\n",
        "hello from web-0",
    )
    .await
}

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::bridge;
use crate::target::Target;

pub async fn run(service: String, target: Arc<dyn Target>, listener: TcpListener) {
    info!(
        service = %service,
        target = %target.describe(),
        "listening"
    );

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!(service = %service, error = %e, "accept failed");
                continue;
            }
        };

        let target = Arc::clone(&target);
        let service = service.clone();

        tokio::spawn(async move {
            info!(service = %service, "connect_attempt_started");

            let remote = match target.connect().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(service = %service, error = %e, "connect failed");
                    return;
                }
            };

            info!(service = %service, "bridge_started");

            match bridge::run(stream, remote).await {
                Ok(()) => info!(service = %service, "bridge_closed"),
                Err(e) => warn!(service = %service, error = %e, "bridge_closed"),
            }
        });
    }
}

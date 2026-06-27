use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e:#}");
        process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config_path = parse_config_path()?;
    let config = tunl::config::Config::load(&config_path)?;

    info!(count = config.services.len(), "loaded services");

    let n = config.services.len();

    // parse all targets before touching any ports so a bad URI exits cleanly
    let mut parsed = Vec::with_capacity(n);
    for (name, service) in &config.services {
        let target = tunl::target::from_uri(name, &service.target)?;
        parsed.push((name.clone(), service.local_port as u16, target));
    }

    // pre-bind every port before spawning any tasks
    // a bind failure here exits before any tunnel starts — no partial startup
    let mut ready = Vec::with_capacity(n);
    for (name, port, target) in parsed {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| anyhow::anyhow!("[{name}] failed to bind port {port}: {e}"))?;
        let target: Arc<dyn tunl::target::Target> = Arc::from(target);
        ready.push((name, target, listener));
    }

    let token = CancellationToken::new();

    let mut handles = Vec::with_capacity(n);
    for (name, target, listener) in ready {
        handles.push(tokio::spawn(tunl::tunnel::run(
            name,
            target,
            listener,
            token.child_token(),
        )));
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

fn parse_config_path() -> anyhow::Result<PathBuf> {
    let mut args = std::env::args().skip(1);

    match args.next().as_deref() {
        Some("--config") => {}
        Some(unknown) => anyhow::bail!("unknown argument: {unknown}\nusage: tunl --config <path>"),
        None => anyhow::bail!("usage: tunl --config <path>"),
    }

    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("--config requires a path"))?;

    Ok(PathBuf::from(path))
}

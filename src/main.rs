use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// How long main waits for all tunnel tasks to drain before giving up.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);

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

    let mut set: JoinSet<()> = JoinSet::new();
    for (name, target, listener) in ready {
        set.spawn(tunl::tunnel::run(
            name,
            target,
            listener,
            token.child_token(),
        ));
    }

    tokio::signal::ctrl_c().await?;
    info!("shutdown_started");
    token.cancel();

    // Give all tunnel tasks (and their in-flight bridges) time to drain.
    tokio::time::timeout(SHUTDOWN_DRAIN, async {
        while set.join_next().await.is_some() {}
    })
    .await
    .ok();

    info!("shutdown_complete");
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

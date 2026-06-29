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
    // Parse args before init so the first log line already honors --json.
    let args = parse_args()?;
    init_tracing(args.json);

    let config = tunl::config::Config::load(&args.config)?;

    info!(count = config.services.len(), "loaded_services");

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

struct Args {
    config: PathBuf,
    json: bool,
}

fn init_tracing(json: bool) {
    if json {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt::init();
    }
}

const USAGE: &str = "usage: tunl --config <path> [--json]";

fn parse_args() -> anyhow::Result<Args> {
    let mut config = None;
    let mut json = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--config" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--config requires a path\n{USAGE}"))?;
                config = Some(PathBuf::from(path));
            }
            other => anyhow::bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    let config = config.ok_or_else(|| anyhow::anyhow!("--config is required\n{USAGE}"))?;
    Ok(Args { config, json })
}

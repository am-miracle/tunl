use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};
use tunl::config::Service;
use tunl::health::HealthRegistry;
use tunl::registry::{ExitReason, Registry};
use tunl::target::Target;

/// How long main waits for all tunnel tasks to drain before giving up.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);
/// How long to wait after a config file event before treating it as settled.
/// Editors commonly fire more than one event per save (write, then rename).
const RELOAD_DEBOUNCE: Duration = Duration::from_millis(300);

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
    init_tracing(args.json, args.dashboard);

    let config = tunl::config::Config::load(&args.config)?;
    info!(count = config.services.len(), "loaded_services");

    let health = HealthRegistry::default();
    let mut registry = Registry::with_health(health.clone());
    initial_start(&config.services, &mut registry).await?;
    let mut current_services = config.services;

    let dashboard_stop = Arc::new(AtomicBool::new(false));
    let (dashboard_quit_tx, mut dashboard_quit_rx) = mpsc::unbounded_channel();
    let dashboard_task = if args.dashboard {
        let stop = Arc::clone(&dashboard_stop);
        let error_tx = dashboard_quit_tx.clone();
        Some(tokio::task::spawn_blocking(move || {
            let result = tunl::dashboard::run(health, dashboard_quit_tx, stop);
            if result.is_err() {
                let _ = error_tx.send(());
            }
            result
        }))
    } else {
        None
    };

    let (_watcher, mut reload_rx) = watch_config(&args.config)?;
    let mut watcher_alive = true;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,

            _ = dashboard_quit_rx.recv(), if args.dashboard => break,

            event = reload_rx.recv(), if watcher_alive => {
                match event {
                    Some(()) => reload(&args.config, &mut current_services, &mut registry).await,
                    None => watcher_alive = false, // watcher task ended; nothing more to watch
                }
            }

            joined = registry.join_next(), if registry.task_count() > 0 => {
                if let Some((name, ExitReason::Unexpected)) = joined {
                    warn!(service = %name, "service_exited_unexpectedly");
                }
            }
        }
    }

    info!("shutdown_started");
    registry.cancel_all();

    tokio::time::timeout(SHUTDOWN_DRAIN, async {
        while registry.join_next().await.is_some() {}
    })
    .await
    .ok();

    dashboard_stop.store(true, Ordering::Release);
    if let Some(task) = dashboard_task {
        task.await??;
    }

    info!("shutdown_complete");
    Ok(())
}

/// Initial startup is all-or-nothing: no tunnel is spawned until every target
/// parses and every local port binds.
async fn initial_start(
    services: &HashMap<String, Service>,
    registry: &mut Registry,
) -> anyhow::Result<()> {
    struct ParsedService {
        name: String,
        address: SocketAddr,
        target: Box<dyn Target>,
        connection: tunl::config::ConnectionPolicy,
    }

    struct ReadyService {
        name: String,
        address: SocketAddr,
        target: Arc<dyn Target>,
        listener: tokio::net::TcpListener,
        connection: tunl::config::ConnectionPolicy,
    }

    let mut parsed = Vec::with_capacity(services.len());
    for (name, service) in services {
        let target = tunl::target::from_uri(name, &service.target)?;
        let address = SocketAddr::new(service.bind_address, service.local_port as u16);
        if !service.bind_address.is_loopback() {
            warn!(service = %name, %address, "remote_listener_enabled");
        }
        parsed.push(ParsedService {
            name: name.clone(),
            address,
            target,
            connection: service.connection,
        });
    }

    let mut ready = Vec::with_capacity(parsed.len());
    for service in parsed {
        let listener = tunl::listener::bind(service.address).await.map_err(|e| {
            anyhow::anyhow!("[{}] failed to bind {}: {e}", service.name, service.address)
        })?;
        let target: Arc<dyn Target> = Arc::from(service.target);
        ready.push(ReadyService {
            name: service.name,
            address: service.address,
            target,
            listener,
            connection: service.connection,
        });
    }

    for service in ready {
        registry.adopt(
            service.name,
            service.address,
            service.target,
            service.listener,
            service.connection,
        );
    }
    Ok(())
}

async fn reload(
    config_path: &Path,
    current: &mut HashMap<String, Service>,
    registry: &mut Registry,
) {
    let config = match tunl::config::Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "config_reload_rejected");
            return;
        }
    };

    let plan = tunl::reload::apply(registry, current, config.services).await;
    if plan.is_empty() {
        return;
    }

    info!(
        added = plan.added.len(),
        removed = plan.removed.len(),
        changed = plan.changed.len(),
        "config_reloaded"
    );
}

/// Watch the config file's parent directory (not the file itself: editors
/// commonly save via a temp file plus rename, which can drop a watch on the
/// original inode) and debounce bursts of events into a single tick per
/// settled change.
fn watch_config(
    config_path: &Path,
) -> anyhow::Result<(RecommendedWatcher, mpsc::UnboundedReceiver<()>)> {
    let watch_dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let file_name = config_path.file_name().map(|n| n.to_owned());

    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<()>();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
        let Ok(event) = res else { return };
        let matches = event
            .paths
            .iter()
            .any(|p| p.file_name() == file_name.as_deref());
        if matches {
            let _ = raw_tx.send(());
        }
    })?;
    watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

    let (debounced_tx, debounced_rx) = mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            if raw_rx.recv().await.is_none() {
                return;
            }
            // Coalesce any further events that arrive within the debounce
            // window into this same reload attempt.
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(RELOAD_DEBOUNCE) => break,
                    next = raw_rx.recv() => if next.is_none() { return },
                }
            }
            if debounced_tx.send(()).is_err() {
                return;
            }
        }
    });

    Ok((watcher, debounced_rx))
}

struct Args {
    config: PathBuf,
    json: bool,
    dashboard: bool,
}

fn init_tracing(json: bool, dashboard: bool) {
    if json {
        tracing_subscriber::fmt().json().init();
    } else if dashboard {
        // Terminal output would corrupt the alternate-screen UI. Runtime
        // events still fire; dashboard mode simply gives them a sink.
        tracing_subscriber::fmt().with_writer(std::io::sink).init();
    } else {
        tracing_subscriber::fmt::init();
    }
}

const USAGE: &str = "usage: tunl --config <path> [--json | --dashboard]";

fn parse_args() -> anyhow::Result<Args> {
    let mut config = None;
    let mut json = false;
    let mut dashboard = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--dashboard" => dashboard = true,
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
    if json && dashboard {
        anyhow::bail!("--json and --dashboard cannot be used together\n{USAGE}");
    }
    Ok(Args {
        config,
        json,
        dashboard,
    })
}

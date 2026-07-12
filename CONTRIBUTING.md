# Contributing to tunl

Thanks for taking a look. This guide covers how to set up the project, the standards a change needs to meet, and the V2 work that is open for someone to pick up.

## Getting set up

You need a Rust toolchain (edition 2024). Clone the repo and build:

```sh
git clone https://github.com/am-miracle/tunl
cd tunl
cargo build
```

Before you open a pull request, run the same three checks CI runs. All three must pass:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## How the code is laid out

The logic lives in a library and the binary is a thin shell on top, which is what makes the behavior testable without a network or a cluster.

```
src/
  main.rs          # arg parsing, startup, reload loop, signal handling
  lib.rs           # module declarations
  config.rs        # TOML config structs, loading, validation
  error.rs         # typed error enum (thiserror)
  io.rs            # AsyncReadWrite trait
  bridge.rs        # copies bytes between client and target
  backoff.rs       # exponential backoff policy
  listener.rs      # IPv4, IPv6, and dual-stack listener binding
  tunnel.rs        # accept loop, retry loop, drain on shutdown
  registry.rs      # running service tasks and per-service shutdown
  reload.rs        # config diffing and reload application
  target/
    mod.rs         # Target trait + from_uri factory
    remote.rs      # remote:// target
    docker.rs      # docker:// target
    kubectl.rs     # kubectl:// target
    ssh.rs         # ssh:// target
tests/             # integration tests, one file per area
```

The one idea to understand before changing anything: a target is anything that returns a read-write stream.

```rust
#[async_trait]
pub trait Target: Send + Sync + std::fmt::Debug {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>>;
    fn describe(&self) -> String;
}
```

Everything else (the accept loop, reconnection, shutdown) is written once against this trait and does not know which target type it holds. Adding a new target means writing one file and one branch in `from_uri`. It should not mean touching `tunnel.rs`.

## Standards a change needs to meet

- **Tests.** New behavior needs a test. For target types that need real infrastructure (Docker, Kubernetes), unit-test the parsing and use a fake target for the logic, the same way `tests/tunnel_test.rs` does. Infrastructure-dependent end-to-end checks stay out of CI and go in a manual recipe, as the existing targets do.
- **Errors point somewhere.** When something fails, the message should tell the user what to do next. Look at `src/target/docker.rs` and `src/target/kubectl.rs` for the pattern: read the underlying error, map it to a clear sentence with the command to run.
- **Match the surrounding style.** Comments explain why, not what. Keep them sparse. Run `cargo fmt`.
- **Keep dependencies minimal.** Add a crate only when it earns its place, and turn off default features you do not use.

## Writing style for docs

If your change touches the README, or any prose, keep the voice plain and natural. Do not use em dashes. Avoid filler words like "simply", "just", "seamlessly", and "powerful". Short and direct beats long and impressive.

## V2 roadmap

V2 includes label-based Kubernetes targeting, hot config reload, and SSH bastion targets. The remaining items are roughly ordered by value. Each one is self-contained, so you can take a single item without touching the others.

If you want to work on one, open an issue first so we can agree on the approach before you write code.

### 1. Configurable timeouts and backoff

**Problem.** The connect timeout (10s) and the backoff (1s growing to 15s) are hardcoded in `src/tunnel.rs` and `src/backoff.rs`.

**Goal.** Allow a service to override them, keeping the current values as defaults.

**Where.** `src/config.rs`, `src/tunnel.rs`, `src/backoff.rs`.

**Done when.** A service can set its own connect timeout and backoff bounds, and a service that sets nothing behaves exactly as it does now.

**Size.** Small. Good first issue.

### 2. Docker and Kubernetes integration tests in CI

**Problem.** The Docker and Kubernetes paths are verified by hand. V2 adds label resolution and reload, which touch the riskiest code, so this is the moment to automate those checks.

**Goal.** A CI job that spins up a kind cluster and a throwaway container, then runs the real connect path against both. Keep it separate from the fast unit test job so the common case stays quick.

**Where.** A new CI workflow plus a test harness. `docs/demo-setup.sh` already shows how to bring up both backends and is a good starting point.

**Done when.** CI exercises a real Docker exec and a real pod port-forward, and fails if either breaks.

**Size.** Medium.

### 3. Health dashboard

**Problem.** There is no at-a-glance view of what is up. To see tunnel state you read the logs.

**Goal.** A terminal UI showing every service: its local port, its target, its current status (connecting, up, or retrying), and the number of active connections. The view updates live as connections open and close and as targets go down and come back.

**Where.** A new module and a `--dashboard` flag in `src/main.rs`. Use `ratatui` with `crossterm` for the UI. This item has a prerequisite: the tunnel layer needs to report state. Today `src/tunnel.rs` logs events but holds no shared state. You will add a small shared structure (for example an `Arc` of per-service status that the tunnel tasks update) that the dashboard reads. Keep the logging path intact so `--json` still works.

**Done when.** Running with the dashboard flag shows per-service status and connection counts that change in real time. Stopping a backend flips its service to retrying, and bringing it back flips it to up.

**Size.** Large, mostly because of the state-reporting plumbing rather than the UI itself.

### 4. `tunl init`

**Problem.** Writing the first config by hand means looking up pod names, container names, and ports before you can start.

**Goal.** A subcommand that introspects a running Kubernetes namespace (and optionally Docker) and writes a starter `config.toml` with one service per discovered port, ready to edit.

```sh
tunl init --namespace default > config.toml
```

**Where.** A new subcommand in the `src/main.rs` arg parser. Use the `kube` crate's `Api::list` to enumerate pods and their container ports, and assign each one a free local port. The Docker side can use the same `bollard` client the target already uses.

**Done when.** `tunl init` against a namespace produces a valid config that loads without edits, with a sensible local port per discovered service.

**Size.** Medium.

### 5. Authenticated listeners

**Problem.** A service bound to a non-loopback address accepts any client that can reach its port. The `allow_remote_connections` setting acknowledges that exposure but does not protect the listener.

**Goal.** Authenticate incoming clients before forwarding traffic. Support TLS with client certificates or integration with an authenticated proxy without weakening the current loopback default.

**Where.** A new listener authentication layer between `src/listener.rs` and `src/tunnel.rs`, plus certificate and trust configuration in `src/config.rs`.

**Done when.** A protected listener rejects clients without a trusted identity, accepts configured clients, and reloads certificate configuration without restarting unrelated services.

**Size.** Large. Authentication, certificate lifecycle, and proxy trust boundaries need a design before implementation.

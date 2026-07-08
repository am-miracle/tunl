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
  main.rs          # arg parsing, startup, signal handling
  lib.rs           # module declarations
  config.rs        # TOML config structs, loading, validation
  error.rs         # typed error enum (thiserror)
  io.rs            # AsyncReadWrite trait
  bridge.rs        # copies bytes between client and target
  backoff.rs       # exponential backoff policy
  tunnel.rs        # accept loop, retry loop, drain on shutdown
  target/
    mod.rs         # Target trait + from_uri factory
    remote.rs      # remote:// target
    docker.rs      # docker:// target
    kubectl.rs     # kubectl:// target
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

V1 ships static tunnels to three target types. V2 is about targets that follow what is actually running, and a few quality-of-life wins. Items are roughly ordered by value. Each one is self-contained, so you can take a single item without touching the others.

If you want to work on one, open an issue first so we can agree on the approach before you write code.

### 1. Label-based Kubernetes targeting (flagship)

**Problem.** V1 targets an explicit pod name, so Deployments do not work. When a Deployment rolls, the pod name changes and tunl keeps retrying a name that is gone.

**Goal.** Support a label selector in place of a pod name:

```toml
target = "kubectl://default/app=api:8080"
```

Resolve the selector to a current pod at connect time, and re-resolve on the next connection rather than retrying a dead name. The existing pod-name form keeps working.

**Where.** `src/target/mod.rs` (parse the selector form in `from_uri`), `src/target/kubectl.rs` (list pods by label, pick a ready one, then port-forward). The `kube` crate's `Api::list` with a label selector does the lookup.

**Done when.** A label target connects to a running pod, and after that pod is replaced under a new name, a new client connection reaches the replacement without a restart. Document the pick rule (for example, first ready pod) and the parsing in the README.

**Size.** Medium.

### 2. Hot config reload

**Problem.** Adding or removing a service means restarting the whole process and dropping every active tunnel.

**Goal.** Watch the config file. When it changes, start tunnels for new services and stop tunnels for removed ones, leaving untouched services running.

```toml
# add a [services.cache] block and save. tunl brings up the new tunnel
# without touching postgres or api, and without a restart.
```

**Why this fits, and where it does not (read before starting).** Each service already runs as its own task with its own cancellation token, so the shutdown half of this is close to free: removing a service is cancelling its token, adding one is spawning a task, and `Service` already derives `PartialEq` so diffing old and new config is a plain `HashMap` comparison. But three things are missing today and need real design, not just wiring:

- **No name-to-task registry.** `main.rs` currently spawns everything into one anonymous `JoinSet` and blocks until Ctrl+C. To cancel or restart *one* service, `main.rs` needs a `HashMap<String, RunningService>` keyed by service name, and a loop that reacts to more than one event (Ctrl+C, a task exiting, a config change), not a single linear startup.
- **Port re-bind race.** Cancelling a service's token stops its accept loop, but the bound `TcpListener` is not dropped until `tunnel::run` returns, which is after its drain window (up to `DRAIN_TIMEOUT`, plus any open bridges). If a reload removes a service and adds a different one on the same port, the new bind can race the old listener's teardown and fail with "address in use." Editing a service's target on the same port hits the same race, since that is a remove-then-add under the hood. This needs an explicit rule: wait for the old task to fully exit before binding the reused port, and accept that a rapid edit on a busy port takes a moment to settle rather than being instant.
- **Reload must never apply a bad or partial config.** File watchers fire more than once per save (temp file plus rename, editors that write in chunks), so a raw `notify` event can catch a half-written file. A reload that fails to parse or fails validation must log and leave every running service untouched, not tear anything down. Validation also needs to check the new config against currently live and still-draining ports, not just for duplicates within itself.

**Where.** `src/main.rs` (registry, event loop, applying a diff), a new pure module for the diff itself (see steps), `src/tunnel.rs` (no changes expected; it already returns cleanly on cancel).

**Steps.**

| # | Task | Notes |
|---|------|-------|
| 2.1 | Diff function | A pure function `diff(old: &HashMap<String, Service>, new: &HashMap<String, Service>) -> ReloadPlan` returning added, removed, and changed service names. No file I/O, no `notify`, unit-tested the way `Backoff` is: plain values in, plain values out. |
| 2.2 | Service registry | Replace the flat `JoinSet` in `main.rs` with a `HashMap<String, RunningService>` (name, child token, join handle, bound port). Startup builds this map instead of a `Vec`. |
| 2.3 | Apply a plan | Cancel tokens for removed and changed services and move their handles into a separate "retiring" pool rather than awaiting them inline. Bind and spawn added and changed services, waiting for a retiring handle on the same port to finish first. |
| 2.4 | Watch and debounce | Wire `notify` to a channel, debounce bursts of events into one reload attempt, reject a config that fails to parse or validate and log why, and never apply a partial diff. |
| 2.5 | Shutdown accounting | Ctrl+C must drain both the active registry and the retiring pool. `shutdown_complete` should not log until both are empty. |
| 2.6 | Reload events | Log `config_reloaded`, and per service `service_added`, `service_removed`, `service_restarted`, matching the existing snake_case event style and carrying the `service` field. |
| 2.7 | Tests | Unit tests on `diff` for every combination (added, removed, changed, unchanged, and unchanged-but-reordered map iteration). An integration test that calls the apply-plan step directly against a running set of fake services, the same way `tests/tunnel_test.rs` avoids real infrastructure. The `notify` glue itself stays thin and is verified by hand, the same way Docker and Kubernetes connect paths are. |

**Done when.** Editing `config.toml` to add a service brings its port up without a restart. Removing one drains and frees its port. Changing a service's target drains and restarts only that service. A service whose definition did not change keeps its active connections through a reload of an unrelated service. A config edit that fails to parse or validate changes nothing.

**Size.** Medium-large. The reload trigger is the easy part; the registry rework in `main.rs` and the port re-bind race are the real work, and both are correctness issues, not polish.

### 3. SSH target

**Goal.** Reach a service behind a bastion:

```toml
target = "ssh://user@host:22/db.internal:5432"
```

Open an SSH connection and forward to the inner host and port, then return that stream as an `AsyncReadWrite`. This is a new file under `src/target/` and one branch in `from_uri`, the same shape as the existing targets.

**Where.** New `src/target/ssh.rs`. Look at an async SSH crate such as `russh`. The work here is auth and host-key handling, so think through how keys and known-hosts are resolved before writing the connect path.

**Done when.** A service reachable only through a bastion is forwarded to a local port, with clear errors for auth failure and an unknown host key.

**Size.** Medium to large, mostly because of auth.

### 4. IPv6 and a bind address

**Problem.** Local listeners bind to `127.0.0.1` only.

**Goal.** Let a service or a global flag choose the bind address, so `::1` or dual-stack works.

**Where.** `src/config.rs` (a `bind_address` field with a sensible default) and `src/main.rs` (use it in `TcpListener::bind`).

**Done when.** A configured bind address is honored and the default stays `127.0.0.1`.

**Size.** Small. Good first issue.

### 5. Configurable timeouts and backoff

**Problem.** The connect timeout (10s) and the backoff (1s growing to 15s) are hardcoded in `src/tunnel.rs` and `src/backoff.rs`.

**Goal.** Allow a service to override them, keeping the current values as defaults.

**Where.** `src/config.rs`, `src/tunnel.rs`, `src/backoff.rs`.

**Done when.** A service can set its own connect timeout and backoff bounds, and a service that sets nothing behaves exactly as it does now.

**Size.** Small. Good first issue.

### 6. Docker and Kubernetes integration tests in CI

**Problem.** The Docker and Kubernetes paths are verified by hand. V2 adds label resolution and reload, which touch the riskiest code, so this is the moment to automate those checks.

**Goal.** A CI job that spins up a kind cluster and a throwaway container, then runs the real connect path against both. Keep it separate from the fast unit test job so the common case stays quick.

**Where.** A new CI workflow plus a test harness. `docs/demo-setup.sh` already shows how to bring up both backends and is a good starting point.

**Done when.** CI exercises a real Docker exec and a real pod port-forward, and fails if either breaks.

**Size.** Medium.

### 7. Health dashboard

**Problem.** There is no at-a-glance view of what is up. To see tunnel state you read the logs.

**Goal.** A terminal UI showing every service: its local port, its target, its current status (connecting, up, or retrying), and the number of active connections. The view updates live as connections open and close and as targets go down and come back.

**Where.** A new module and a `--dashboard` flag in `src/main.rs`. Use `ratatui` with `crossterm` for the UI. This item has a prerequisite: the tunnel layer needs to report state. Today `src/tunnel.rs` logs events but holds no shared state. You will add a small shared structure (for example an `Arc` of per-service status that the tunnel tasks update) that the dashboard reads. Keep the logging path intact so `--json` still works.

**Done when.** Running with the dashboard flag shows per-service status and connection counts that change in real time. Stopping a backend flips its service to retrying, and bringing it back flips it to up.

**Size.** Large, mostly because of the state-reporting plumbing rather than the UI itself.

### 8. `tunl init`

**Problem.** Writing the first config by hand means looking up pod names, container names, and ports before you can start.

**Goal.** A subcommand that introspects a running Kubernetes namespace (and optionally Docker) and writes a starter `config.toml` with one service per discovered port, ready to edit.

```sh
tunl init --namespace default > config.toml
```

**Where.** A new subcommand in the `src/main.rs` arg parser. Use the `kube` crate's `Api::list` to enumerate pods and their container ports, and assign each one a free local port. The Docker side can use the same `bollard` client the target already uses.

**Done when.** `tunl init` against a namespace produces a valid config that loads without edits, with a sensible local port per discovered service.

**Size.** Medium.

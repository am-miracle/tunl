# tunl

Open all your service tunnels with one command.

`tunl` reads a config file that lists the services you depend on and forwards a local port to each one. Your app connects to `localhost:5432` and reaches Postgres, whether that Postgres runs in a Kubernetes pod, a Docker container, or on a server somewhere else. One process, one config, every port forwarded.

![tunl forwarding three services at once](docs/demo.gif)

## Why

Working on one service often means reaching several others. Some live in a Kubernetes cluster, some in local containers, some on a remote host. The usual answer is a handful of terminal tabs running `kubectl port-forward` and `docker` and `ssh -L`, each with its own flags, each dying on its own when a pod restarts or a connection drops.

`tunl` puts all of that in one file. Start it once and every port is up. If a target goes away, `tunl` reconnects when it comes back instead of leaving you with a dead tunnel.

## Install

You need a Rust toolchain. Build from source:

```sh
git clone https://github.com/am-miracle/tunl
cd tunl
cargo install --path .
```

That puts a `tunl` binary on your `PATH`. You can also run `cargo build --release` and use `target/release/tunl` directly.

## Configure

Create a `config.toml`. Each block defines one tunnel:

```toml
[services.postgres]
local_port = 15432
target = "remote://127.0.0.1:5432"

[services.cache]
local_port = 9000
target = "docker://redis:6379"

[services.api]
local_port = 8080
bind_address = "::1"
target = "kubectl://default/api-0:8080"

[services.internal_db]
local_port = 25432
target = "ssh://deploy@bastion.example.com/db.internal:5432"

[services.internal_db.connection]
connect_timeout = "5s"
backoff_initial = "500ms"
backoff_max = "10s"

[services.internal_db.health]
probe_interval = "5s"
probe_timeout = "2s"
probe_backoff_initial = "1s"
probe_backoff_max = "30s"
```

- `local_port` is the port on your machine that `tunl` listens on.
- `bind_address` is optional and defaults to `127.0.0.1`. Use `::1` for IPv6 loopback.
- `connection` is optional. Use it when a service needs a different connect timeout or retry backoff.
- `health` is optional. Use it to tune dashboard target probes independently from client retry behavior.
- `target` is where that traffic goes, written as a URI.

`0.0.0.0`, `::`, and other non-loopback addresses can expose a tunnel to other machines. Tunl rejects these addresses unless the service acknowledges the exposure:

```toml
[services.shared_api]
local_port = 8080
bind_address = "::"
allow_remote_connections = true
target = "remote://api.internal:8080"
```

`::` creates one dual-stack listener that accepts IPv4 and IPv6 connections. The opt-in does not authenticate incoming clients. Use firewall rules to restrict access.

Four target types are supported:

| Scheme | Format | Forwards to |
|--------|--------|-------------|
| `remote` | `remote://host:port` | a TCP host and port |
| `docker` | `docker://container:port` | a port inside a running container |
| `kubectl` | `kubectl://namespace/pod:port` | a port on a named pod |
| `kubectl` | `kubectl://namespace/label=value:port` | a port on a pod matched by label |
| `ssh` | `ssh://user@bastion[:port]/host:port[?identity=fingerprint]` | a host reachable through an SSH bastion |

The `kubectl` target takes either an explicit pod name or a label selector. A selector is anything with an `=` in it, such as `app=api` or `app=api,tier=web`. Use a selector when the pod name is not stable, which is the case for Deployments.

## Run

```sh
tunl --config config.toml
```

`tunl` binds every local port up front. If any port is taken, it reports which one and exits without starting a partial set. Once it is running, point your clients at the local ports:

```sh
psql -h localhost -p 15432      # reaches the remote Postgres
redis-cli -p 9000               # reaches the container
curl localhost:8080             # reaches the pod
```

Press Ctrl+C to stop. `tunl` lets active connections finish, then exits.

Add `--dashboard` for a live terminal view of every service, its listener,
target reachability, and active connection count. Press `q`, Escape, or Ctrl+C
to stop:

```sh
tunl --config config.toml --dashboard
```

`Listening` in the listener column means the local port is bound and accepting
clients. `Reachable`, `Unreachable`, `Probing`, and `Unknown` in the
reachability column come from the background health probe loop. This makes a
down backend visible even when no client is currently trying to connect.

Add `--json` for structured logs you can pipe into `jq` or a log collector:

```sh
tunl --config config.toml --json
```

## Editing the config while it runs

`tunl` watches the config file and picks up changes without a restart. Add a service block and save, and its tunnel comes up on its own. Remove one and its port is freed. Change a service's target or port and only that service restarts, everything else keeps its connections.

If a save leaves the file broken (invalid TOML, a bad target URI, a duplicate port), `tunl` logs why and leaves every running service exactly as it was. Nothing gets torn down over a typo.

## How each target works

**remote** opens a plain TCP connection to the host and port. Use it for anything reachable over the network, including a bastion or an SSH tunnel you already have open.

**docker** runs `nc` inside the container and streams its input and output over the Docker socket. This reaches the container's port without publishing it and works the same on macOS and Linux. The container image needs a `nc` (netcat) binary, so minimal images like `distroless` and `scratch` will not work.

**kubectl** uses the Kubernetes API server's port-forward, the same path `kubectl port-forward` takes. It reads your current kubeconfig context.

You can target a pod two ways. An explicit pod name (`kubectl://default/api-0:8080`) forwards to that exact pod. If it is recreated under a new name, as a Deployment does on rollout, `tunl` keeps trying the configured name and logs that it cannot find it. A label selector (`kubectl://default/app=api:8080`) resolves to a matching pod on every new connection and picks the first one that is ready, so it follows the current pod behind a Deployment. Use an explicit name for StatefulSet pods, which keep stable names like `api-0`, and a selector for Deployments.

**ssh** connects to a bastion and opens a direct TCP channel to the destination. Connections for the same service share one authenticated SSH transport, which is recreated if it closes. The bastion port defaults to `22`. Each host key must already be trusted in `~/.ssh/known_hosts`. Authentication tries identities from `ssh-agent`, then unencrypted `~/.ssh/id_ed25519`, `id_ecdsa`, and `id_rsa` files. Add encrypted keys to `ssh-agent`; passwords are not accepted in target URIs.

If your agent contains several keys, append `?identity=<fingerprint>` to select one public key. Use `ssh-add -l -E sha256` to list fingerprints. When a fingerprint is configured, tunl offers only that agent identity or a matching default identity file to the bastion.

## Reconnection

When a target is down, `tunl` retries with a backoff that grows from one second up to fifteen, then connects as soon as the target is back. The retry covers connection setup. An open connection that drops is closed, and the next client connection sets up a fresh tunnel. Restart a pod or a container and the next request goes through once it is ready.

Each service can tune the connection setup policy:

```toml
[services.api.connection]
connect_timeout = "5s"
backoff_initial = "500ms"
backoff_max = "10s"
```

The defaults are `10s`, `1s`, and `15s`. Durations must be greater than zero, and `backoff_initial` must not be higher than `backoff_max`. Very small retry values can put pressure on a failing backend, so use them for local tests rather than shared infrastructure.

Each service can also tune dashboard health probes:

```toml
[services.api.health]
probe_interval = "5s"
probe_timeout = "2s"
probe_backoff_initial = "1s"
probe_backoff_max = "30s"
```

Successful probes wait `probe_interval` before checking again. Failed probes
retry with exponential backoff from `probe_backoff_initial` to
`probe_backoff_max`. These values only affect the dashboard's target
reachability signal; client connection retries still use `[services.api.connection]`.

## Limitations

- Non-loopback listeners do not authenticate incoming clients.
- Docker targets need `nc` in the container image.
- A label selector picks the first ready pod, so it does not spread connections across replicas.
- SSH targets do not read aliases, identity paths, or proxy rules from `~/.ssh/config`.

## License

`tunl` is available under the MIT License. See [LICENSE](LICENSE) for the full text.

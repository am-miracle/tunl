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
target = "kubectl://default/api-0:8080"
```

- `local_port` is the port on your machine (`127.0.0.1`) that `tunl` listens on.
- `target` is where that traffic goes, written as a URI.

Three target types are supported:

| Scheme | Format | Forwards to |
|--------|--------|-------------|
| `remote` | `remote://host:port` | a TCP host and port |
| `docker` | `docker://container:port` | a port inside a running container |
| `kubectl` | `kubectl://namespace/pod:port` | a port on a named pod |

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

Add `--json` for structured logs you can pipe into `jq` or a log collector:

```sh
tunl --config config.toml --json
```

## How each target works

**remote** opens a plain TCP connection to the host and port. Use it for anything reachable over the network, including a bastion or an SSH tunnel you already have open.

**docker** runs `nc` inside the container and streams its input and output over the Docker socket. This reaches the container's port without publishing it and works the same on macOS and Linux. The container image needs a `nc` (netcat) binary, so minimal images like `distroless` and `scratch` will not work.

**kubectl** uses the Kubernetes API server's port-forward, the same path `kubectl port-forward` takes. It reads your current kubeconfig context. `tunl` forwards to an explicit pod name. If a pod is recreated under a new name, as a Deployment does on rollout, `tunl` keeps trying the configured name and logs that it cannot find it. Use StatefulSet pods, which keep stable names like `api-0`, or fall back to `kubectl port-forward` for Deployment routing.

## Reconnection

When a target is down, `tunl` retries with a backoff that grows from one second up to fifteen, then connects as soon as the target is back. The retry covers connection setup. An open connection that drops is closed, and the next client connection sets up a fresh tunnel. Restart a pod or a container and the next request goes through once it is ready.

## Limitations

- Local listeners bind to IPv4 loopback (`127.0.0.1`) only.
- Docker targets need `nc` in the container image.
- Kubernetes targets follow a fixed pod name, not a label or service.

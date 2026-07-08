#!/usr/bin/env bash
# Bring up the three backends the demo proxies to, then regenerate docs/demo.gif.
#
# Requires: docker, kind, kubectl, vhs, and a local Postgres listening on :5432.
# The three services in ../config.toml map to these backends:
#   remote://127.0.0.1:5432   -> your local Postgres        (local port 15432)
#   docker://tunl-demo:8000   -> busybox container below    (local port 9000)
#   kubectl://default/web-0:8080 -> kind StatefulSet pod web-0 (local port 8080)
set -euo pipefail
cd "$(dirname "$0")/.."

# 1. Docker backend: busybox (ships nc, which the exec target needs) serving a
#    line on :8000.
docker rm -f tunl-demo >/dev/null 2>&1 || true
docker run -d --name tunl-demo busybox \
  sh -c 'while true; do echo "hello from container" | nc -l -p 8000; done' >/dev/null

# 2. Kubernetes backend: a StatefulSet pod (stable name web-0) on :8080.
kind create cluster --name tunl-test >/dev/null 2>&1 || true
kubectl --context kind-tunl-test apply -f - <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: web }
spec:
  serviceName: web
  replicas: 1
  selector: { matchLabels: { app: web } }
  template:
    metadata: { labels: { app: web } }
    spec:
      containers:
        - name: web
          image: hashicorp/http-echo
          args: ["-text=hello from web-0", "-listen=:8080"]
          ports: [{ containerPort: 8080 }]
YAML
kubectl --context kind-tunl-test wait --for=condition=ready pod/web-0 --timeout=120s

# 3. Remote backend: your local Postgres on :5432 (start it however you normally do).

# Build the binary the tape invokes, then record.
cargo build
vhs docs/demo.tape
echo "wrote docs/demo.gif"

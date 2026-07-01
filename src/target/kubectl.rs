use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Error as KubeError;
use kube::api::ListParams;
use kube::{Api, Client};

use crate::io::AsyncReadWrite;

use super::Target;

#[derive(Debug)]
pub(super) enum PodSelector {
    Name(String),
    Labels(String),
}

/// Proxies to a TCP port on a Kubernetes pod via the API server's port-forward
/// subresource, the same mechanism `kubectl port-forward` uses.
///
/// The pod is chosen either by an explicit name or by a label selector. A name
/// that is deleted and recreated under a different name (as a Deployment does
/// on rollout) is not followed. A label selector is resolved to a ready pod on
/// every new connection, so it does follow the current pod behind a Deployment.
#[derive(Debug)]
pub struct KubectlTarget {
    pub(super) namespace: String,
    pub(super) selector: PodSelector,
    pub(super) port: u16,
}

#[async_trait]
impl Target for KubectlTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let client = Client::try_default().await.map_err(|e| self.explain(e))?;
        let pods: Api<Pod> = Api::namespaced(client, &self.namespace);

        let pod = self.resolve_pod(&pods).await?;

        let mut forwarder = pods
            .portforward(&pod, &[self.port])
            .await
            .map_err(|e| self.explain(e))?;

        // take_stream yields the bidirectional stream for this port exactly
        // once. None would mean the port wasn't in the negotiated set, which
        // shouldn't happen since we asked for exactly this one.
        let stream = forwarder.take_stream(self.port).ok_or_else(|| {
            anyhow::anyhow!(
                "port-forward to {}/{} opened but port {} was not negotiated",
                self.namespace,
                pod,
                self.port
            )
        })?;

        Ok(Box::new(stream))
    }

    fn describe(&self) -> String {
        format!(
            "kubectl://{}/{}:{}",
            self.namespace,
            self.selector_display(),
            self.port
        )
    }
}

impl KubectlTarget {
    /// Resolve the selector to a concrete pod name. A name is returned as-is; a
    /// label selector is listed and the first ready pod wins.
    async fn resolve_pod(&self, pods: &Api<Pod>) -> anyhow::Result<String> {
        let labels = match &self.selector {
            PodSelector::Name(name) => return Ok(name.clone()),
            PodSelector::Labels(labels) => labels,
        };

        let list = pods
            .list(&ListParams::default().labels(labels))
            .await
            .map_err(|e| self.explain(e))?;

        let pod = list.items.iter().find(|p| is_ready(p)).ok_or_else(|| {
            let ns = &self.namespace;
            if list.items.is_empty() {
                anyhow::anyhow!(
                    "no pod matches selector {labels:?} in namespace {ns} — check: kubectl -n {ns} get pods -l {labels}"
                )
            } else {
                anyhow::anyhow!(
                    "{} pod(s) match selector {labels:?} in namespace {ns} but none are ready",
                    list.items.len()
                )
            }
        })?;

        pod.metadata
            .name
            .clone()
            .ok_or_else(|| anyhow::anyhow!("pod matching {labels:?} has no name"))
    }

    fn selector_display(&self) -> &str {
        match &self.selector {
            PodSelector::Name(name) => name,
            PodSelector::Labels(labels) => labels,
        }
    }

    /// Turn a kube error into an actionable message. The `Api` variant carries
    /// the API server's HTTP status, which distinguishes a missing pod (404)
    /// from an RBAC denial (403); everything else is a config or transport
    /// problem. All of these propagate to the retry loop, so a transient
    /// failure (pod restarting, API blip) reconnects on its own.
    fn explain(&self, err: KubeError) -> anyhow::Error {
        let ns = &self.namespace;
        let who = self.selector_display();
        match &err {
            KubeError::Api(status) if status.code == 404 => {
                anyhow::anyhow!("no pod found for {ns}/{who} — check: kubectl -n {ns} get pods")
            }
            KubeError::Api(status) if status.code == 403 => anyhow::anyhow!(
                "forbidden to port-forward {ns}/{who}: {} — your kubeconfig user needs the pods/portforward permission",
                status.message
            ),
            KubeError::Api(status) => anyhow::anyhow!(
                "kubernetes rejected port-forward to {ns}/{who} (HTTP {}): {}",
                status.code,
                status.message
            ),
            KubeError::InferConfig(_) | KubeError::InferKubeconfig(_) => anyhow::Error::new(err)
                .context(format!(
                    "cannot load kubeconfig for {ns}/{who} — is KUBECONFIG set and a context selected?"
                )),
            KubeError::HyperError(_) | KubeError::Service(_) => anyhow::Error::new(err).context(
                format!(
                    "cannot reach the Kubernetes API server for {ns}/{who} — is the cluster up? check: kubectl cluster-info"
                ),
            ),
            KubeError::UpgradeConnection(_) => anyhow::Error::new(err).context(format!(
                "failed to establish port-forward to {ns}/{who} — the pod may be starting, or RBAC denies pods/portforward"
            )),
            _ => anyhow::Error::new(err).context(format!("port-forward to {ns}/{who} failed")),
        }
    }
}

fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
}

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Error as KubeError;
use kube::{Api, Client};

use crate::io::AsyncReadWrite;

use super::Target;

/// Proxies to a TCP port on a named Kubernetes pod via the API server's
/// port-forward subresource — the same mechanism `kubectl port-forward` uses.
///
/// If that pod is deleted and recreated
/// under a different name (as a Deployment does on rollout), tunl keeps
/// retrying the configured name and logs a clear error rather than chasing the
/// replacement. Use StatefulSet pods (stable names) or `kubectl port-forward`
/// for Deployment-level routing.
#[derive(Debug)]
pub struct KubectlTarget {
    pub(super) namespace: String,
    pub(super) pod: String,
    pub(super) port: u16,
}

#[async_trait]
impl Target for KubectlTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let client = Client::try_default().await.map_err(|e| self.explain(e))?;
        let pods: Api<Pod> = Api::namespaced(client, &self.namespace);

        let mut forwarder = pods
            .portforward(&self.pod, &[self.port])
            .await
            .map_err(|e| self.explain(e))?;

        // take_stream yields the bidirectional stream for this port exactly
        // once. None would mean the port wasn't in the negotiated set, which
        // shouldn't happen since we asked for exactly this one.
        let stream = forwarder.take_stream(self.port).ok_or_else(|| {
            anyhow::anyhow!(
                "port-forward to {}/{} opened but port {} was not negotiated",
                self.namespace,
                self.pod,
                self.port
            )
        })?;

        Ok(Box::new(stream))
    }

    fn describe(&self) -> String {
        format!("kubectl://{}/{}:{}", self.namespace, self.pod, self.port)
    }
}

impl KubectlTarget {
    fn explain(&self, err: KubeError) -> anyhow::Error {
        let KubectlTarget { namespace, pod, .. } = self;
        match &err {
            KubeError::Api(status) if status.code == 404 => anyhow::anyhow!(
                "pod {namespace}/{pod} not found — check: kubectl -n {namespace} get pods"
            ),
            KubeError::Api(status) if status.code == 403 => anyhow::anyhow!(
                "forbidden to port-forward {namespace}/{pod}: {} — your kubeconfig user needs the pods/portforward permission",
                status.message
            ),
            KubeError::Api(status) => anyhow::anyhow!(
                "kubernetes rejected port-forward to {namespace}/{pod} (HTTP {}): {}",
                status.code,
                status.message
            ),
            KubeError::InferConfig(_) | KubeError::InferKubeconfig(_) => anyhow::Error::new(err)
                .context(format!(
                    "cannot load kubeconfig for {namespace}/{pod} — is KUBECONFIG set and a context selected?"
                )),
            KubeError::HyperError(_) | KubeError::Service(_) => anyhow::Error::new(err).context(
                format!(
                    "cannot reach the Kubernetes API server for {namespace}/{pod} — is the cluster up? check: kubectl cluster-info"
                ),
            ),
            KubeError::UpgradeConnection(_) => anyhow::Error::new(err).context(format!(
                "failed to establish port-forward to {namespace}/{pod} — the pod may be starting, or RBAC denies pods/portforward"
            )),
            _ => anyhow::Error::new(err)
                .context(format!("port-forward to {namespace}/{pod} failed")),
        }
    }
}

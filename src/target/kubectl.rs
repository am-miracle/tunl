use async_trait::async_trait;

use crate::io::AsyncReadWrite;

use super::Target;

#[derive(Debug)]
pub struct KubectlTarget {
    pub(super) namespace: String,
    pub(super) pod: String,
    pub(super) port: u16,
}

#[async_trait]
impl Target for KubectlTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        // will use kube::Api::portforward to open a SPDY/WebSocket stream.
        anyhow::bail!("kubectl target not yet implemented")
    }

    fn describe(&self) -> String {
        format!("kubectl://{}/{}:{}", self.namespace, self.pod, self.port)
    }
}

use async_trait::async_trait;

use crate::io::AsyncReadWrite;

use super::Target;

#[derive(Debug)]
pub struct DockerTarget {
    pub(super) container: String,
    pub(super) port: u16,
}

#[async_trait]
impl Target for DockerTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        // will resolve container IP via Docker API, open TcpStream.
        anyhow::bail!("docker target not yet implemented")
    }

    fn describe(&self) -> String {
        format!("docker://{}:{}", self.container, self.port)
    }
}

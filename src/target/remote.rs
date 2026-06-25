use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::io::AsyncReadWrite;

use super::Target;

#[derive(Debug)]
pub struct RemoteTarget {
    /// "host:port" — stored pre-joined so TcpStream::connect receives it
    /// directly without any formatting at connect time.
    pub(super) address: String,
}

#[async_trait]
impl Target for RemoteTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let stream = TcpStream::connect(&self.address).await?;
        Ok(Box::new(stream))
    }

    fn describe(&self) -> String {
        format!("remote://{}", self.address)
    }
}

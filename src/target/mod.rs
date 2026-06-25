mod docker;
mod kubectl;
mod remote;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::io::AsyncReadWrite;

#[async_trait]
pub trait Target: Send + Sync + std::fmt::Debug {
    /// Open a bidirectional byte stream to the real service
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>>;

    /// Human-readable description used in log lines.
    fn describe(&self) -> String;
}

/// Parse a target URI into the right boxed Target implementation
pub fn from_uri(service: &str, uri: &str) -> Result<Box<dyn Target>> {
    if let Some(rest) = uri.strip_prefix("kubectl://") {
        let (namespace, pod_port) = rest.split_once('/').ok_or_else(|| Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: "expected kubectl://<namespace>/<pod>:<port>".to_string(),
        })?;
        let (pod, port_str) = pod_port
            .rsplit_once(':')
            .ok_or_else(|| Error::InvalidTarget {
                service: service.to_string(),
                target: uri.to_string(),
                reason: "expected kubectl://<namespace>/<pod>:<port>".to_string(),
            })?;
        let port = parse_port(service, uri, port_str)?;
        require_nonempty(service, uri, "namespace", namespace)?;
        require_nonempty(service, uri, "pod name", pod)?;
        Ok(Box::new(kubectl::KubectlTarget {
            namespace: namespace.to_string(),
            pod: pod.to_string(),
            port,
        }))
    } else if let Some(rest) = uri.strip_prefix("docker://") {
        let (container, port_str) = rest.rsplit_once(':').ok_or_else(|| Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: "expected docker://<container>:<port>".to_string(),
        })?;
        let port = parse_port(service, uri, port_str)?;
        require_nonempty(service, uri, "container name", container)?;
        Ok(Box::new(docker::DockerTarget {
            container: container.to_string(),
            port,
        }))
    } else if let Some(rest) = uri.strip_prefix("remote://") {
        let (host, port_str) = rest.rsplit_once(':').ok_or_else(|| Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: "expected remote://<host>:<port>".to_string(),
        })?;
        let port = parse_port(service, uri, port_str)?;
        require_nonempty(service, uri, "host", host)?;
        Ok(Box::new(remote::RemoteTarget {
            // Store pre-joined host:port so TcpStream::connect gets a single
            // string it can resolve directly without any formatting at call time
            address: format!("{host}:{port}"),
        }))
    } else {
        Err(Error::UnknownScheme {
            service: service.to_string(),
            target: uri.to_string(),
        })
    }
}

fn parse_port(service: &str, uri: &str, port_str: &str) -> Result<u16> {
    port_str
        .parse::<u16>()
        .ok()
        .filter(|&p| p >= 1)
        .ok_or_else(|| Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: format!("{port_str:?} is not a valid port (1-65535)"),
        })
}

fn require_nonempty(service: &str, uri: &str, field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

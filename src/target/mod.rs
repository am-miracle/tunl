mod docker;
mod kubectl;
mod remote;
mod ssh;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::io::AsyncReadWrite;

#[async_trait]
pub trait Target: Send + Sync + std::fmt::Debug {
    /// open a bidirectional byte stream to the real service
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>>;
    fn describe(&self) -> String;
}

/// parse a target URI into the right boxed Target implementation
pub fn from_uri(service: &str, uri: &str) -> Result<Box<dyn Target>> {
    if let Some(rest) = uri.strip_prefix("kubectl://") {
        let (namespace, pod_port) = rest.split_once('/').ok_or_else(|| Error::InvalidTarget {
            service: service.to_string(),
            target: uri.to_string(),
            reason: "expected kubectl://<namespace>/<pod-or-selector>:<port>".to_string(),
        })?;
        let (selector, port_str) =
            pod_port
                .rsplit_once(':')
                .ok_or_else(|| Error::InvalidTarget {
                    service: service.to_string(),
                    target: uri.to_string(),
                    reason: "expected kubectl://<namespace>/<pod-or-selector>:<port>".to_string(),
                })?;
        let port = parse_port(service, uri, port_str)?;
        require_nonempty(service, uri, "namespace", namespace)?;
        require_nonempty(service, uri, "pod name or label selector", selector)?;
        // '=' means a label query (app=api); otherwise it's a pod name, since
        // pod names never contain '='. Existence selectors like `tier` (no
        // '=') aren't supported and are treated as pod names instead.
        let selector = if selector.contains('=') {
            kubectl::PodSelector::Labels(selector.to_string())
        } else {
            kubectl::PodSelector::Name(selector.to_string())
        };
        Ok(Box::new(kubectl::KubectlTarget {
            namespace: namespace.to_string(),
            selector,
            port,
        }))
    } else if let Some(rest) = uri.strip_prefix("ssh://") {
        let (authority, destination_and_query) =
            rest.split_once('/').ok_or_else(|| Error::InvalidTarget {
                service: service.to_string(),
                target: uri.to_string(),
                reason: "expected ssh://<user>@<bastion>[:port]/<host>:<port>".to_string(),
            })?;
        let (destination, identity) = parse_ssh_query(service, uri, destination_and_query)?;
        let (user, bastion) = authority
            .split_once('@')
            .ok_or_else(|| Error::InvalidTarget {
                service: service.to_string(),
                target: uri.to_string(),
                reason: "expected ssh://<user>@<bastion>[:port]/<host>:<port>".to_string(),
            })?;
        require_nonempty(service, uri, "SSH user", user)?;
        // URI passwords would be exposed through config files and logs.
        if user.contains(':') {
            return Err(Error::InvalidTarget {
                service: service.to_string(),
                target: uri.to_string(),
                reason: "passwords are not allowed in SSH target URIs".to_string(),
            });
        }
        let (bastion_host, bastion_port) = parse_host_port(service, uri, bastion, Some(22))?;
        let (destination_host, destination_port) =
            parse_host_port(service, uri, destination, None)?;
        Ok(Box::new(ssh::SshTarget::new(
            user.to_string(),
            bastion_host,
            bastion_port,
            destination_host,
            destination_port,
            identity,
        )))
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
            // store pre-joined host:port so TcpStream::connect gets a single
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

fn parse_ssh_query<'a>(
    service: &str,
    uri: &str,
    destination: &'a str,
) -> Result<(&'a str, Option<russh::keys::ssh_key::Fingerprint>)> {
    let Some((destination, query)) = destination.split_once('?') else {
        return Ok((destination, None));
    };
    let Some(fingerprint) = query.strip_prefix("identity=") else {
        return Err(invalid_ssh_query(service, uri));
    };
    if fingerprint.is_empty() || fingerprint.contains(['&', '?']) {
        return Err(invalid_ssh_query(service, uri));
    }
    let identity = fingerprint
        .parse()
        .map_err(|_| invalid_ssh_query(service, uri))?;
    Ok((destination, Some(identity)))
}

fn invalid_ssh_query(service: &str, uri: &str) -> Error {
    Error::InvalidTarget {
        service: service.to_string(),
        target: uri.to_string(),
        reason: "expected SSH query identity=<SHA256-or-SHA512-fingerprint>".to_string(),
    }
}

fn parse_host_port(
    service: &str,
    uri: &str,
    value: &str,
    default_port: Option<u16>,
) -> Result<(String, u16)> {
    let (host, port) = if let Some(bracketed) = value.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or_else(|| invalid_endpoint(service, uri, value))?;
        let port = match suffix.strip_prefix(':') {
            Some(port) => parse_port(service, uri, port)?,
            None if suffix.is_empty() => {
                default_port.ok_or_else(|| invalid_endpoint(service, uri, value))?
            }
            _ => return Err(invalid_endpoint(service, uri, value)),
        };
        (host, port)
    } else if let Some((host, port)) = value.rsplit_once(':') {
        if host.contains(':') {
            return Err(invalid_endpoint(service, uri, value));
        }
        (host, parse_port(service, uri, port)?)
    } else {
        (
            value,
            default_port.ok_or_else(|| invalid_endpoint(service, uri, value))?,
        )
    };
    require_nonempty(service, uri, "host", host)?;
    Ok((host.to_string(), port))
}

fn invalid_endpoint(service: &str, uri: &str, value: &str) -> Error {
    Error::InvalidTarget {
        service: service.to_string(),
        target: uri.to_string(),
        reason: format!("{value:?} is not a valid host and port"),
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

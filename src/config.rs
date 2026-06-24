use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, PartialEq, Deserialize)]
pub struct Service {
    pub local_port: i64,
    pub target: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    pub services: HashMap<String, Service>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();

        let contents = std::fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;

        let config: Config = toml::from_str(&contents).map_err(|source| Error::ConfigParse {
            path: path.to_path_buf(),
            source,
        })?;

        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.services.is_empty() {
            return Err(Error::NoServices);
        }

        for (name, service) in &self.services {
            validate_port(name, service.local_port)?;
        }

        check_duplicate_ports(&self.services)?;

        for (name, service) in &self.services {
            validate_target(name, &service.target)?;
        }

        Ok(())
    }
}

fn validate_port(service: &str, port: i64) -> Result<()> {
    if !(1..=65535).contains(&port) {
        return Err(Error::InvalidPort {
            service: service.to_string(),
            port,
        });
    }
    Ok(())
}

fn check_duplicate_ports(services: &HashMap<String, Service>) -> Result<()> {
    // HashMap iteration order isn't stable, so without sorting, which name
    // gets reported as "first" vs "second" would vary between runs.
    let mut names: Vec<&String> = services.keys().collect();
    names.sort();

    let mut seen: HashMap<i64, &String> = HashMap::new();
    for name in names {
        let port = services[name].local_port;
        if let Some(first) = seen.get(&port) {
            return Err(Error::DuplicatePort {
                port,
                first: first.to_string(),
                second: name.to_string(),
            });
        }
        seen.insert(port, name);
    }
    Ok(())
}

fn validate_target(service: &str, target: &str) -> Result<()> {
    if let Some(rest) = target.strip_prefix("kubectl://") {
        let (namespace, pod_port) = rest.split_once('/').ok_or_else(|| {
            malformed_target(
                service,
                target,
                "expected kubectl://<namespace>/<pod>:<port>",
            )
        })?;
        let (pod, port) = pod_port.rsplit_once(':').ok_or_else(|| {
            malformed_target(
                service,
                target,
                "expected kubectl://<namespace>/<pod>:<port>",
            )
        })?;
        require_nonempty(service, target, "namespace", namespace)?;
        require_nonempty(service, target, "pod name", pod)?;
        require_valid_port_str(service, target, port)
    } else if let Some(rest) = target.strip_prefix("docker://") {
        let (container, port) = rest.rsplit_once(':').ok_or_else(|| {
            malformed_target(service, target, "expected docker://<container>:<port>")
        })?;
        require_nonempty(service, target, "container name", container)?;
        require_valid_port_str(service, target, port)
    } else if let Some(rest) = target.strip_prefix("remote://") {
        let (host, port) = rest
            .rsplit_once(':')
            .ok_or_else(|| malformed_target(service, target, "expected remote://<host>:<port>"))?;
        require_nonempty(service, target, "host", host)?;
        require_valid_port_str(service, target, port)
    } else {
        Err(Error::UnknownScheme {
            service: service.to_string(),
            target: target.to_string(),
        })
    }
}

fn malformed_target(service: &str, target: &str, reason: &str) -> Error {
    Error::InvalidTarget {
        service: service.to_string(),
        target: target.to_string(),
        reason: reason.to_string(),
    }
}

fn require_nonempty(service: &str, target: &str, field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(malformed_target(
            service,
            target,
            &format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

fn require_valid_port_str(service: &str, target: &str, port: &str) -> Result<()> {
    match port.parse::<u16>() {
        Ok(1..=u16::MAX) => Ok(()),
        _ => Err(malformed_target(
            service,
            target,
            &format!("{port:?} is not a valid port (1-65535)"),
        )),
    }
}

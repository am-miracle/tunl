use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub local_port: i64,
    #[serde(default = "default_bind_address")]
    pub bind_address: IpAddr,
    #[serde(default)]
    pub allow_remote_connections: bool,
    #[serde(default)]
    pub connection: ConnectionPolicy,
    #[serde(default)]
    pub health: HealthPolicy,
    pub target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionPolicy {
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(default = "default_backoff_initial", with = "humantime_serde")]
    pub backoff_initial: Duration,
    #[serde(default = "default_backoff_max", with = "humantime_serde")]
    pub backoff_max: Duration,
}

impl Default for ConnectionPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: default_connect_timeout(),
            backoff_initial: default_backoff_initial(),
            backoff_max: default_backoff_max(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthPolicy {
    #[serde(default = "default_probe_interval", with = "humantime_serde")]
    pub probe_interval: Duration,
    #[serde(default = "default_probe_timeout", with = "humantime_serde")]
    pub probe_timeout: Duration,
    #[serde(default = "default_probe_backoff_initial", with = "humantime_serde")]
    pub probe_backoff_initial: Duration,
    #[serde(default = "default_probe_backoff_max", with = "humantime_serde")]
    pub probe_backoff_max: Duration,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            probe_interval: default_probe_interval(),
            probe_timeout: default_probe_timeout(),
            probe_backoff_initial: default_probe_backoff_initial(),
            probe_backoff_max: default_probe_backoff_max(),
        }
    }
}

fn default_bind_address() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_backoff_initial() -> Duration {
    Duration::from_secs(1)
}

fn default_backoff_max() -> Duration {
    Duration::from_secs(15)
}

fn default_probe_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_probe_timeout() -> Duration {
    Duration::from_secs(2)
}

fn default_probe_backoff_initial() -> Duration {
    Duration::from_secs(1)
}

fn default_probe_backoff_max() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
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
            if !service.bind_address.is_loopback() && !service.allow_remote_connections {
                return Err(Error::RemoteBindingNotAllowed {
                    service: name.clone(),
                    address: service.bind_address,
                });
            }
            validate_connection_policy(name, service.connection)?;
            validate_health_policy(name, service.health)?;
        }

        check_duplicate_ports(&self.services)?;

        Ok(())
    }
}

fn validate_health_policy(service: &str, policy: HealthPolicy) -> Result<()> {
    if policy.probe_interval.is_zero() {
        return Err(Error::InvalidHealthPolicy {
            service: service.to_string(),
            reason: "probe_interval must be greater than 0".to_string(),
        });
    }
    if policy.probe_timeout.is_zero() {
        return Err(Error::InvalidHealthPolicy {
            service: service.to_string(),
            reason: "probe_timeout must be greater than 0".to_string(),
        });
    }
    if policy.probe_backoff_initial.is_zero() {
        return Err(Error::InvalidHealthPolicy {
            service: service.to_string(),
            reason: "probe_backoff_initial must be greater than 0".to_string(),
        });
    }
    if policy.probe_backoff_max.is_zero() {
        return Err(Error::InvalidHealthPolicy {
            service: service.to_string(),
            reason: "probe_backoff_max must be greater than 0".to_string(),
        });
    }
    if policy.probe_backoff_initial > policy.probe_backoff_max {
        return Err(Error::InvalidHealthPolicy {
            service: service.to_string(),
            reason: "probe_backoff_initial must be less than or equal to probe_backoff_max"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_connection_policy(service: &str, policy: ConnectionPolicy) -> Result<()> {
    if policy.connect_timeout.is_zero() {
        return Err(Error::InvalidConnectionPolicy {
            service: service.to_string(),
            reason: "connect_timeout must be greater than 0".to_string(),
        });
    }
    if policy.backoff_initial.is_zero() {
        return Err(Error::InvalidConnectionPolicy {
            service: service.to_string(),
            reason: "backoff_initial must be greater than 0".to_string(),
        });
    }
    if policy.backoff_max.is_zero() {
        return Err(Error::InvalidConnectionPolicy {
            service: service.to_string(),
            reason: "backoff_max must be greater than 0".to_string(),
        });
    }
    if policy.backoff_initial > policy.backoff_max {
        return Err(Error::InvalidConnectionPolicy {
            service: service.to_string(),
            reason: "backoff_initial must be less than or equal to backoff_max".to_string(),
        });
    }
    Ok(())
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

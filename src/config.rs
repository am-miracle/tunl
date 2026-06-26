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

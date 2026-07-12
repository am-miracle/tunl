use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // PathBuf doesn't implement Display (paths aren't guaranteed valid UTF-8),
    // so we call .display() explicitly instead of writing {path}.
    #[error("failed to read config file {}: {source}", path.display())]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config file {}: {source}", path.display())]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("config must define at least one service")]
    NoServices,

    #[error("[{service}] local_port {port} is invalid: must be between 1 and 65535")]
    InvalidPort { service: String, port: i64 },

    #[error(
        "local_port {port} is used by both [{first}] and [{second}] — each service needs a unique local_port"
    )]
    DuplicatePort {
        port: i64,
        first: String,
        second: String,
    },

    #[error(
        "[{service}] bind_address {address} accepts remote connections; set allow_remote_connections = true to permit network exposure"
    )]
    RemoteBindingNotAllowed { service: String, address: IpAddr },

    // {target:?} instead of {target} so the value gets wrapped in quotes,
    // making it clear where the URI starts and ends in the message.
    #[error(
        "[{service}] target {target:?} has an unrecognized scheme: expected kubectl://, docker://, remote://, or ssh://"
    )]
    UnknownScheme { service: String, target: String },

    // One variant covers every malformed-target shape (missing namespace,
    // missing port, empty host, etc) instead of a variant per shape. We give
    // up matching on the specific problem, but nothing downstream needs to.
    #[error("[{service}] target {target:?} is malformed: {reason}")]
    InvalidTarget {
        service: String,
        target: String,
        reason: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

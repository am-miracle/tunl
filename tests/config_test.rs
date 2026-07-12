use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use tempfile::NamedTempFile;
use tunl::config::Config;

// Returns the NamedTempFile itself, not just its path. The file is deleted
// as soon as the NamedTempFile is dropped, so the caller has to hold onto it
// for as long as the path is in use.
fn write_config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("failed to create temp file");
    file.write_all(contents.as_bytes())
        .expect("failed to write temp config");
    file
}

#[test]
fn valid_config_parses_into_expected_structs() {
    let file = write_config(
        r#"
        [services.postgres]
        local_port = 5432
        target = "kubectl://default/postgres-0:5432"

        [services.redis]
        local_port = 6379
        target = "docker://redis:6379"
        "#,
    );

    let config = Config::load(file.path()).expect("valid config should load");

    assert_eq!(config.services.len(), 2);

    let postgres = &config.services["postgres"];
    assert_eq!(postgres.local_port, 5432);
    assert_eq!(postgres.bind_address, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert!(!postgres.allow_remote_connections);
    assert_eq!(postgres.target, "kubectl://default/postgres-0:5432");

    let redis = &config.services["redis"];
    assert_eq!(redis.local_port, 6379);
    assert_eq!(redis.target, "docker://redis:6379");
}

#[test]
fn accepts_ipv6_loopback_without_remote_opt_in() {
    let file = write_config(
        r#"
        [services.api]
        local_port = 8080
        bind_address = "::1"
        target = "remote://api.internal:8080"
        "#,
    );

    let config = Config::load(file.path()).expect("IPv6 loopback should load");
    assert_eq!(
        config.services["api"].bind_address,
        IpAddr::V6(Ipv6Addr::LOCALHOST)
    );
}

#[test]
fn rejects_remote_bind_without_explicit_opt_in() {
    let file = write_config(
        r#"
        [services.api]
        local_port = 8080
        bind_address = "::"
        target = "remote://api.internal:8080"
        "#,
    );

    let err = Config::load(file.path()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "[api] bind_address :: accepts remote connections; set allow_remote_connections = true to permit network exposure"
    );
}

#[test]
fn accepts_remote_bind_with_explicit_opt_in() {
    let file = write_config(
        r#"
        [services.api]
        local_port = 8080
        bind_address = "0.0.0.0"
        allow_remote_connections = true
        target = "remote://api.internal:8080"
        "#,
    );

    let config = Config::load(file.path()).expect("explicit remote bind should load");
    assert!(config.services["api"].allow_remote_connections);
}

#[test]
fn rejects_malformed_bind_address() {
    let file = write_config(
        r#"
        [services.api]
        local_port = 8080
        bind_address = "localhost"
        target = "remote://api.internal:8080"
        "#,
    );

    let err = Config::load(file.path()).unwrap_err();
    assert!(
        err.to_string().starts_with("failed to parse config file"),
        "unexpected error message: {err}"
    );
}

#[test]
fn rejects_local_port_below_range() {
    let file = write_config(
        r#"
        [services.postgres]
        local_port = 0
        target = "remote://db.internal:5432"
        "#,
    );

    let err = Config::load(file.path()).unwrap_err();

    assert_eq!(
        err.to_string(),
        "[postgres] local_port 0 is invalid: must be between 1 and 65535"
    );
}

#[test]
fn rejects_local_port_above_range() {
    let file = write_config(
        r#"
        [services.postgres]
        local_port = 70000
        target = "remote://db.internal:5432"
        "#,
    );

    let err = Config::load(file.path()).unwrap_err();

    assert_eq!(
        err.to_string(),
        "[postgres] local_port 70000 is invalid: must be between 1 and 65535"
    );
}

#[test]
fn rejects_duplicate_local_port() {
    let file = write_config(
        r#"
        [services.service-a]
        local_port = 5432
        target = "remote://a.internal:5432"

        [services.service-b]
        local_port = 5432
        target = "remote://b.internal:5432"
        "#,
    );

    let err = Config::load(file.path()).unwrap_err();

    assert_eq!(
        err.to_string(),
        "local_port 5432 is used by both [service-a] and [service-b] \
         — each service needs a unique local_port"
    );
}

#[test]
fn rejects_empty_services() {
    let file = write_config("[services]\n");

    let err = Config::load(file.path()).unwrap_err();

    assert_eq!(err.to_string(), "config must define at least one service");
}

#[test]
fn rejects_malformed_toml() {
    let file = write_config("this is not valid toml {{{");

    let err = Config::load(file.path()).unwrap_err();

    assert!(
        err.to_string().starts_with("failed to parse config file"),
        "unexpected error message: {err}"
    );
}

#[test]
fn rejects_nonexistent_file() {
    let err = Config::load("/nonexistent/path/to/config.toml").unwrap_err();

    assert!(
        err.to_string().starts_with("failed to read config file"),
        "unexpected error message: {err}"
    );
}

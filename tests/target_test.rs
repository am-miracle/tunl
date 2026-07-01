use tunl::target::from_uri;

#[test]
fn remote_parses_and_describes_correctly() {
    let t = from_uri("auth", "remote://staging.company.com:8080").unwrap();
    assert_eq!(t.describe(), "remote://staging.company.com:8080");
}

#[test]
fn remote_localhost_is_accepted() {
    let t = from_uri("db", "remote://localhost:5432").unwrap();
    assert_eq!(t.describe(), "remote://localhost:5432");
}

#[test]
fn docker_parses_and_describes_correctly() {
    let t = from_uri("redis", "docker://redis:6379").unwrap();
    assert_eq!(t.describe(), "docker://redis:6379");
}

#[test]
fn kubectl_parses_and_describes_correctly() {
    let t = from_uri("postgres", "kubectl://default/postgres-0:5432").unwrap();
    assert_eq!(t.describe(), "kubectl://default/postgres-0:5432");
}

#[test]
fn kubectl_label_selector_parses_and_describes() {
    // A selector containing '=' is a label query and round-trips through describe.
    let t = from_uri("api", "kubectl://default/app=api:8080").unwrap();
    assert_eq!(t.describe(), "kubectl://default/app=api:8080");

    // Multiple comma-separated labels are still one selector.
    let t = from_uri("api", "kubectl://prod/app=api,tier=web:8080").unwrap();
    assert_eq!(t.describe(), "kubectl://prod/app=api,tier=web:8080");
}

#[test]
fn rejects_unknown_scheme() {
    let err = from_uri("x", "ftp://x:21").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[x] target \"ftp://x:21\" has an unrecognized scheme \
         — expected kubectl://, docker://, or remote://"
    );
}

#[test]
fn rejects_remote_missing_port() {
    let err = from_uri("auth", "remote://staging.company.com").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[auth] target \"remote://staging.company.com\" is malformed: \
         expected remote://<host>:<port>"
    );
}

#[test]
fn rejects_remote_invalid_port() {
    let err = from_uri("auth", "remote://staging.company.com:notaport").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[auth] target \"remote://staging.company.com:notaport\" is malformed: \
         \"notaport\" is not a valid port (1-65535)"
    );
}

#[test]
fn rejects_docker_missing_port() {
    let err = from_uri("redis", "docker://redis").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[redis] target \"docker://redis\" is malformed: expected docker://<container>:<port>"
    );
}

#[test]
fn rejects_kubectl_missing_namespace() {
    let err = from_uri("postgres", "kubectl://postgres-0:5432").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[postgres] target \"kubectl://postgres-0:5432\" is malformed: \
         expected kubectl://<namespace>/<pod-or-selector>:<port>"
    );
}

#[test]
fn rejects_kubectl_invalid_port() {
    let err = from_uri("postgres", "kubectl://default/postgres-0:badport").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[postgres] target \"kubectl://default/postgres-0:badport\" is malformed: \
         \"badport\" is not a valid port (1-65535)"
    );
}

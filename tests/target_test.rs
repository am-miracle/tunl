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

#[tokio::test]
async fn non_remote_targets_do_not_enable_active_probes_by_default() {
    for uri in [
        "docker://redis:6379",
        "kubectl://default/postgres-0:5432",
        "ssh://deploy@bastion.example.com/db.internal:5432",
    ] {
        let target = from_uri("svc", uri).unwrap();
        assert!(
            target.probe().await.is_none(),
            "{uri} unexpectedly opted into active probing"
        );
    }
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
fn ssh_parses_and_describes_with_default_bastion_port() {
    let target = from_uri("db", "ssh://deploy@bastion.example.com/db.internal:5432").unwrap();

    assert_eq!(
        target.describe(),
        "ssh://deploy@bastion.example.com:22/db.internal:5432"
    );
}

#[test]
fn ssh_rejects_passwords_in_the_uri() {
    let err = from_uri(
        "db",
        "ssh://deploy:secret@bastion.example.com/db.internal:5432",
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "[db] target \"ssh://deploy:secret@bastion.example.com/db.internal:5432\" is malformed: \
         passwords are not allowed in SSH target URIs"
    );
}

#[test]
fn ssh_supports_explicit_ports_and_ipv6_hosts() {
    let target = from_uri("db", "ssh://deploy@[2001:db8::10]:2222/[2001:db8::20]:5432").unwrap();

    assert_eq!(
        target.describe(),
        "ssh://deploy@[2001:db8::10]:2222/[2001:db8::20]:5432"
    );
}

#[test]
fn ssh_identity_fingerprint_parses_and_describes() {
    let fingerprint = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let uri = format!("ssh://deploy@bastion.example.com/db.internal:5432?identity={fingerprint}");

    let target = from_uri("db", &uri).unwrap();

    assert_eq!(
        target.describe(),
        format!("ssh://deploy@bastion.example.com:22/db.internal:5432?identity={fingerprint}")
    );
}

#[test]
fn ssh_rejects_invalid_identity_queries() {
    for query in [
        "identity=",
        "identity=not-a-fingerprint",
        "other=value",
        "identity=SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&other=value",
    ] {
        let uri = format!("ssh://deploy@bastion.example.com/db.internal:5432?{query}");
        assert!(from_uri("db", &uri).is_err(), "accepted invalid URI {uri}");
    }
}

#[test]
fn ssh_rejects_missing_connection_fields() {
    for uri in [
        "ssh://bastion.example.com/db.internal:5432",
        "ssh://deploy@bastion.example.com",
        "ssh://deploy@bastion.example.com/db.internal",
        "ssh://deploy@bastion.example.com:0/db.internal:5432",
        "ssh://deploy@bastion.example.com/db.internal:70000",
    ] {
        assert!(from_uri("db", uri).is_err(), "accepted malformed URI {uri}");
    }
}

#[test]
fn kubectl_rejects_empty_selector() {
    // Same require_nonempty guard as namespace/host/container — an empty
    // segment is a parse error, not an empty-string pod name or selector.
    let err = from_uri("api", "kubectl://default/:8080").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[api] target \"kubectl://default/:8080\" is malformed: \
         pod name or label selector must not be empty"
    );
}

#[test]
fn kubectl_malformed_label_like_strings_still_classify_as_selectors() {
    // from_uri only decides Name vs Labels on whether '=' is present. It does
    // not validate label-selector grammar — that grammar is Kubernetes' own
    // (it also supports `key`, `!key`, and `key in (a, b)` forms), so
    // validating it here would just re-implement the API server's own check,
    // incompletely. Anything containing '=' parses fine at this layer and any
    // real syntax error surfaces from the Kubernetes API at connect time.
    for selector in ["=", "=value", "key=", "app=api,", "a==b"] {
        let uri = format!("kubectl://default/{selector}:8080");
        let t = from_uri("api", &uri).unwrap_or_else(|e| panic!("{selector:?}: {e}"));
        assert_eq!(t.describe(), format!("kubectl://default/{selector}:8080"));
    }
}

#[test]
fn kubectl_existence_selector_without_equals_is_a_known_limitation() {
    // Kubernetes also supports existence selectors with no '=' at all, such as
    // `tier` (has the label) or `!tier` (does not). tunl classifies purely on
    // '=' presence, so a bare existence selector is indistinguishable from a
    // pod name and is treated as one. Label selectors that assert a key/value
    // pair (the common case) are unaffected. This is a documented v1
    // limitation, not a bug: fixing it would need real cluster context to
    // know whether a given string names a pod or a label.
    let t = from_uri("api", "kubectl://default/tier:8080").unwrap();
    assert_eq!(t.describe(), "kubectl://default/tier:8080");
}

#[test]
fn rejects_unknown_scheme() {
    let err = from_uri("x", "ftp://x:21").unwrap_err();
    assert_eq!(
        err.to_string(),
        "[x] target \"ftp://x:21\" has an unrecognized scheme: \
         expected kubectl://, docker://, remote://, or ssh://"
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

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use russh::client;
use russh::keys::PrivateKey;
use russh::keys::agent::AgentIdentity;
use russh::keys::key::PrivateKeyWithHashAlg;
use russh::keys::ssh_key::Fingerprint;
use tokio::sync::Mutex;

use crate::io::AsyncReadWrite;

use super::Target;

/// Proxies to a host through an SSH bastion using a direct TCP channel.
/// Bastion keys must already be trusted in `known_hosts`; authentication uses
/// the SSH agent first, then unencrypted default identity files.
pub struct SshTarget {
    pub(super) user: String,
    pub(super) bastion_host: String,
    pub(super) bastion_port: u16,
    pub(super) destination_host: String,
    pub(super) destination_port: u16,
    identity: Option<Fingerprint>,
    // One authenticated transport can carry independent channels for many clients.
    session: Mutex<Option<client::Handle<HostKeyVerifier>>>,
    #[cfg(test)]
    known_hosts: Option<PathBuf>,
    #[cfg(test)]
    test_identity: Option<PrivateKey>,
}

impl fmt::Debug for SshTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SshTarget")
            .field("user", &self.user)
            .field("bastion_host", &self.bastion_host)
            .field("bastion_port", &self.bastion_port)
            .field("destination_host", &self.destination_host)
            .field("destination_port", &self.destination_port)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
enum SshClientError {
    #[error(transparent)]
    Protocol(#[from] russh::Error),

    #[error(
        "SSH host {host}:{port} is not in ~/.ssh/known_hosts; connect with ssh once or add its trusted host key"
    )]
    UnknownHost { host: String, port: u16 },

    #[error("failed to verify SSH host {host}:{port} against ~/.ssh/known_hosts: {source}")]
    HostKey {
        host: String,
        port: u16,
        #[source]
        source: russh::keys::Error,
    },
}

#[derive(Debug)]
struct HostKeyVerifier {
    host: String,
    port: u16,
    // Production uses the standard file; tests provide an isolated path.
    known_hosts: Option<PathBuf>,
}

impl client::Handler for HostKeyVerifier {
    type Error = SshClientError;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        let result = match &self.known_hosts {
            Some(path) => {
                russh::keys::check_known_hosts_path(&self.host, self.port, server_public_key, path)
            }
            None => russh::keys::check_known_hosts(&self.host, self.port, server_public_key),
        };

        match result {
            Ok(true) => Ok(true),
            Ok(false) => Err(SshClientError::UnknownHost {
                host: self.host.clone(),
                port: self.port,
            }),
            Err(source) => Err(SshClientError::HostKey {
                host: self.host.clone(),
                port: self.port,
                source,
            }),
        }
    }
}

#[async_trait]
impl Target for SshTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let mut cached = self.session.lock().await;

        for attempt in 0..2 {
            if cached.as_ref().is_none_or(client::Handle::is_closed) {
                *cached = Some(self.connect_authenticated().await?);
            }

            let session = cached.as_ref().expect("session was initialized");
            match self.open_channel(session).await {
                Ok(stream) => return Ok(stream),
                Err(_) if attempt == 0 && session.is_closed() => {
                    *cached = None;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("closed SSH sessions are retried once")
    }

    fn describe(&self) -> String {
        let mut description = format!(
            "ssh://{}@{}:{}/{}:{}",
            self.user,
            display_host(&self.bastion_host),
            self.bastion_port,
            display_host(&self.destination_host),
            self.destination_port
        );
        if let Some(identity) = self.identity {
            description.push_str(&format!("?identity={identity}"));
        }
        description
    }
}

impl SshTarget {
    pub(super) fn new(
        user: String,
        bastion_host: String,
        bastion_port: u16,
        destination_host: String,
        destination_port: u16,
        identity: Option<Fingerprint>,
    ) -> Self {
        Self {
            user,
            bastion_host,
            bastion_port,
            destination_host,
            destination_port,
            identity,
            session: Mutex::new(None),
            #[cfg(test)]
            known_hosts: None,
            #[cfg(test)]
            test_identity: None,
        }
    }

    async fn connect_authenticated(&self) -> anyhow::Result<client::Handle<HostKeyVerifier>> {
        let mut session = self.connect_bastion(self.known_hosts_override()).await?;
        #[cfg(test)]
        if let Some(identity) = &self.test_identity {
            if authenticate_with_key(&mut session, &self.user, identity.clone()).await? {
                return Ok(session);
            }
            bail!("test SSH identity was rejected")
        }
        authenticate(&mut session, &self.user, self.identity.as_ref())
            .await
            .with_context(|| {
                format!(
                    "SSH authentication failed for {}@{}:{}",
                    self.user, self.bastion_host, self.bastion_port
                )
            })?;
        Ok(session)
    }

    fn known_hosts_override(&self) -> Option<PathBuf> {
        #[cfg(test)]
        {
            self.known_hosts.clone()
        }
        #[cfg(not(test))]
        {
            None
        }
    }

    async fn connect_bastion(
        &self,
        known_hosts: Option<PathBuf>,
    ) -> Result<client::Handle<HostKeyVerifier>, SshClientError> {
        let config = client::Config {
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_max: 3,
            ..Default::default()
        };
        let verifier = HostKeyVerifier {
            host: self.bastion_host.clone(),
            port: self.bastion_port,
            known_hosts,
        };
        client::connect(
            Arc::new(config),
            (self.bastion_host.as_str(), self.bastion_port),
            verifier,
        )
        .await
    }

    async fn open_channel(
        &self,
        session: &client::Handle<HostKeyVerifier>,
    ) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let channel = session
            .channel_open_direct_tcpip(
                &self.destination_host,
                u32::from(self.destination_port),
                "127.0.0.1",
                0,
            )
            .await
            .with_context(|| {
                format!(
                    "SSH bastion could not connect to {}:{}",
                    self.destination_host, self.destination_port
                )
            })?;

        Ok(Box::new(channel.into_stream()))
    }
}

async fn authenticate<H>(
    session: &mut client::Handle<H>,
    user: &str,
    requested_identity: Option<&Fingerprint>,
) -> anyhow::Result<()>
where
    H: client::Handler,
    H::Error: std::error::Error + Send + Sync + 'static,
{
    #[cfg(unix)]
    // Prefer the agent so encrypted private keys never enter this process.
    if authenticate_with_agent(session, user, requested_identity).await? {
        return Ok(());
    }

    for path in default_identity_paths() {
        if !path.is_file() {
            continue;
        }

        let key = match russh::keys::load_secret_key(&path, None) {
            Ok(key) => key,
            // Tunnel setup cannot safely stop for an interactive passphrase.
            Err(russh::keys::Error::KeyIsEncrypted) => continue,
            Err(source) => {
                return Err(source)
                    .with_context(|| format!("failed to load SSH identity {}", path.display()));
            }
        };
        if !matches_requested_identity(key.public_key(), requested_identity) {
            continue;
        }
        if authenticate_with_key(session, user, key).await? {
            return Ok(());
        }
    }

    if let Some(identity) = requested_identity {
        bail!(
            "SSH identity {identity} was not available or accepted; load it into ssh-agent or install it as a default identity file"
        )
    }
    bail!("no SSH agent identity or unencrypted default key was accepted; add the key to ssh-agent")
}

#[cfg(unix)]
async fn authenticate_with_agent<H>(
    session: &mut client::Handle<H>,
    user: &str,
    requested_identity: Option<&Fingerprint>,
) -> anyhow::Result<bool>
where
    H: client::Handler,
    H::Error: std::error::Error + Send + Sync + 'static,
{
    let mut agent = match russh::keys::agent::client::AgentClient::connect_env().await {
        Ok(agent) => agent,
        Err(_) => return Ok(false),
    };
    let identities = agent
        .request_identities()
        .await
        .context("failed to read identities from ssh-agent")?;
    let rsa_hash = rsa_hash_algorithm(session).await?;

    for identity in identities {
        if !matches_requested_identity(&identity.public_key(), requested_identity) {
            continue;
        }
        let result = match identity {
            AgentIdentity::PublicKey { key, .. } => {
                session
                    .authenticate_publickey_with(user, key, rsa_hash, &mut agent)
                    .await?
            }
            AgentIdentity::Certificate { certificate, .. } => {
                session
                    .authenticate_certificate_with(user, certificate, rsa_hash, &mut agent)
                    .await?
            }
        };
        if result.success() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn matches_requested_identity(
    public_key: &russh::keys::PublicKey,
    requested_identity: Option<&Fingerprint>,
) -> bool {
    requested_identity
        .is_none_or(|requested| public_key.fingerprint(requested.algorithm()) == *requested)
}

async fn authenticate_with_key<H>(
    session: &mut client::Handle<H>,
    user: &str,
    key: PrivateKey,
) -> anyhow::Result<bool>
where
    H: client::Handler,
    H::Error: std::error::Error + Send + Sync + 'static,
{
    let rsa_hash = rsa_hash_algorithm(session).await?;
    let result = session
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), rsa_hash))
        .await?;
    Ok(result.success())
}

async fn rsa_hash_algorithm<H>(
    session: &client::Handle<H>,
) -> Result<Option<russh::keys::HashAlg>, russh::Error>
where
    H: client::Handler,
{
    Ok(match session.best_supported_rsa_hash().await? {
        Some(hash) => hash,
        // Modern servers without extension metadata usually accept RSA-SHA2-512.
        None => Some(russh::keys::HashAlg::Sha512),
    })
}

fn default_identity_paths() -> Vec<PathBuf> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    ["id_ed25519", "id_ecdsa", "id_rsa"]
        .into_iter()
        .map(|name| home.join(".ssh").join(name))
        .collect()
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn display_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use russh::server;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    const HOST_KEY: &str = "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";

    #[tokio::test]
    async fn trusted_host_key_is_accepted() {
        let mut known_hosts = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            known_hosts,
            "[bastion.example.com]:2222 ssh-ed25519 {HOST_KEY}"
        )
        .unwrap();
        let key = russh::keys::parse_public_key_base64(HOST_KEY).unwrap();
        let mut verifier = HostKeyVerifier {
            host: "bastion.example.com".to_string(),
            port: 2222,
            known_hosts: Some(known_hosts.path().to_path_buf()),
        };

        let accepted = client::Handler::check_server_key(&mut verifier, &key)
            .await
            .unwrap();

        assert!(accepted);
    }

    #[tokio::test]
    async fn unknown_host_key_has_actionable_error() {
        let known_hosts = tempfile::NamedTempFile::new().unwrap();
        let key = russh::keys::parse_public_key_base64(HOST_KEY).unwrap();
        let mut verifier = HostKeyVerifier {
            host: "bastion.example.com".to_string(),
            port: 22,
            known_hosts: Some(known_hosts.path().to_path_buf()),
        };

        let err = client::Handler::check_server_key(&mut verifier, &key)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "SSH host bastion.example.com:22 is not in ~/.ssh/known_hosts; \
             connect with ssh once or add its trusted host key"
        );
    }

    #[test]
    fn explicit_identity_skips_unrelated_agent_keys() {
        let wrong =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let selected =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let fingerprint = selected
            .public_key()
            .fingerprint(russh::keys::ssh_key::HashAlg::Sha256);

        assert!(!matches_requested_identity(
            wrong.public_key(),
            Some(&fingerprint)
        ));
        assert!(matches_requested_identity(
            selected.public_key(),
            Some(&fingerprint)
        ));
        assert!(matches_requested_identity(wrong.public_key(), None));
    }

    struct EchoServer {
        client_key: russh::keys::PublicKey,
    }

    impl server::Handler for EchoServer {
        type Error = russh::Error;

        async fn auth_publickey(
            &mut self,
            user: &str,
            public_key: &russh::keys::PublicKey,
        ) -> Result<server::Auth, Self::Error> {
            if user == "deploy" && public_key == &self.client_key {
                Ok(server::Auth::Accept)
            } else {
                Ok(server::Auth::reject())
            }
        }

        async fn channel_open_direct_tcpip(
            &mut self,
            channel: russh::Channel<server::Msg>,
            host_to_connect: &str,
            port_to_connect: u32,
            _originator_address: &str,
            _originator_port: u32,
            reply: server::ChannelOpenHandle,
            _session: &mut server::Session,
        ) -> Result<(), Self::Error> {
            if host_to_connect != "db.internal" || port_to_connect != 5432 {
                reply.reject(russh::ChannelOpenFailure::ConnectFailed).await;
                return Ok(());
            }

            reply.accept().await;
            tokio::spawn(async move {
                let mut stream = channel.into_stream();
                let mut bytes = [0; 4];
                stream.read_exact(&mut bytes).await?;
                stream.write_all(&bytes).await?;
                stream.shutdown().await
            });
            Ok(())
        }
    }

    #[derive(Clone)]
    struct DropFirstChannelServer {
        client_key: russh::keys::PublicKey,
        channel_requests: Arc<AtomicUsize>,
    }

    impl server::Handler for DropFirstChannelServer {
        type Error = russh::Error;

        async fn auth_publickey(
            &mut self,
            user: &str,
            public_key: &russh::keys::PublicKey,
        ) -> Result<server::Auth, Self::Error> {
            if user == "deploy" && public_key == &self.client_key {
                Ok(server::Auth::Accept)
            } else {
                Ok(server::Auth::reject())
            }
        }

        async fn channel_open_direct_tcpip(
            &mut self,
            channel: russh::Channel<server::Msg>,
            _host_to_connect: &str,
            _port_to_connect: u32,
            _originator_address: &str,
            _originator_port: u32,
            reply: server::ChannelOpenHandle,
            _session: &mut server::Session,
        ) -> Result<(), Self::Error> {
            if self.channel_requests.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(russh::Error::Disconnect);
            }

            reply.accept().await;
            tokio::spawn(async move {
                let mut stream = channel.into_stream();
                let mut bytes = [0; 4];
                stream.read_exact(&mut bytes).await?;
                stream.write_all(&bytes).await?;
                stream.shutdown().await
            });
            Ok(())
        }
    }

    #[tokio::test]
    async fn closed_session_during_channel_open_is_replaced_and_retried() {
        let server_key =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let client_key =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let channel_requests = Arc::new(AtomicUsize::new(0));
        let handler = DropFirstChannelServer {
            client_key: client_key.public_key().clone(),
            channel_requests: Arc::clone(&channel_requests),
        };
        let mut config = server::Config {
            auth_rejection_time: Duration::ZERO,
            ..Default::default()
        };
        config.keys.push(server_key.clone());
        let config = Arc::new(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let known_hosts = tempfile::NamedTempFile::new().unwrap();
        russh::keys::known_hosts::learn_known_hosts_path(
            "127.0.0.1",
            address.port(),
            server_key.public_key(),
            known_hosts.path(),
        )
        .unwrap();

        let server_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                server::run_stream(Arc::clone(&config), socket, handler.clone())
                    .await
                    .unwrap();
            }
        });
        let mut target = SshTarget::new(
            "deploy".to_string(),
            "127.0.0.1".to_string(),
            address.port(),
            "db.internal".to_string(),
            5432,
            None,
        );
        target.known_hosts = Some(known_hosts.path().to_path_buf());
        target.test_identity = Some(client_key);

        let mut stream = tokio::time::timeout(Duration::from_secs(2), target.connect())
            .await
            .unwrap()
            .unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut response = [0; 4];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(&response, b"ping");
        assert_eq!(channel_requests.load(Ordering::SeqCst), 2);
        server_task.abort();
    }

    #[tokio::test]
    async fn multiple_channels_share_one_authenticated_session() {
        let server_key =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let client_key =
            PrivateKey::random(&mut rand::rng(), russh::keys::ssh_key::Algorithm::Ed25519).unwrap();
        let handler = EchoServer {
            client_key: client_key.public_key().clone(),
        };
        let mut config = server::Config {
            auth_rejection_time: Duration::ZERO,
            ..Default::default()
        };
        config.keys.push(server_key.clone());
        let config = Arc::new(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let known_hosts = tempfile::NamedTempFile::new().unwrap();
        russh::keys::known_hosts::learn_known_hosts_path(
            "127.0.0.1",
            address.port(),
            server_key.public_key(),
            known_hosts.path(),
        )
        .unwrap();

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            server::run_stream(config, socket, handler).await.unwrap();
        });
        let target = SshTarget::new(
            "deploy".to_string(),
            "127.0.0.1".to_string(),
            address.port(),
            "db.internal".to_string(),
            5432,
            None,
        );
        let mut session = target
            .connect_bastion(Some(known_hosts.path().to_path_buf()))
            .await
            .unwrap();
        assert!(
            authenticate_with_key(&mut session, "deploy", client_key)
                .await
                .unwrap()
        );
        target.session.lock().await.replace(session);

        for request in [b"ping", b"pong"] {
            let mut stream = tokio::time::timeout(Duration::from_secs(1), target.connect())
                .await
                .unwrap()
                .unwrap();
            stream.write_all(request).await.unwrap();
            let mut response = [0; 4];
            stream.read_exact(&mut response).await.unwrap();

            assert_eq!(&response, request);
        }
        server_task.abort();
    }
}

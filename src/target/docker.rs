use std::io;

use async_trait::async_trait;
use bollard::Docker;
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, StartExecResults};
use futures_util::StreamExt;
use tokio_util::io::StreamReader;
use tracing::debug;

use crate::io::AsyncReadWrite;

use super::Target;

/// Proxies to a TCP port inside a running Docker container.
///
/// We do **not** dial the container's IP directly: on macOS the container
/// network lives inside the Docker Desktop VM and those IPs are unreachable
/// from the host. Instead we exec `nc` inside the container and stream its
/// stdio over the daemon socket, which bypasses routing entirely and works
/// identically on macOS and Linux.
///
/// Requirement: the container image must ship a `nc` (netcat) binary. Minimal
/// images such as `distroless` or `scratch` will not work.
#[derive(Debug)]
pub struct DockerTarget {
    pub(super) container: String,
    pub(super) port: u16,
}

#[async_trait]
impl Target for DockerTarget {
    async fn connect(&self) -> anyhow::Result<Box<dyn AsyncReadWrite>> {
        let container = &self.container;
        debug!(container = %container, "docker_connect_with_local_defaults_started");
        let docker = Docker::connect_with_local_defaults().map_err(|e| self.explain(e))?;
        debug!(container = %container, "docker_connect_with_local_defaults_done");

        let port = self.port.to_string();
        debug!(container = %container, port = %port, "docker_create_exec_started");
        let exec = docker
            .create_exec(
                &self.container,
                CreateExecOptions {
                    cmd: Some(vec!["nc", "localhost", port.as_str()]),
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| self.explain(e))?;
        debug!(container = %container, exec_id = %exec.id, "docker_create_exec_done");

        debug!(container = %container, exec_id = %exec.id, "docker_start_exec_started");
        let started = docker
            .start_exec(&exec.id, None)
            .await
            .map_err(|e| self.explain(e))?;
        debug!(container = %container, exec_id = %exec.id, "docker_start_exec_done");

        match started {
            StartExecResults::Attached { output, input } => {
                let frame_container = container.clone();
                let reader = StreamReader::new(output.map(move |frame| {
                    frame
                        .map(|log_output| {
                            let bytes = log_output.into_bytes();
                            debug!(
                                container = %frame_container,
                                bytes = bytes.len(),
                                "docker_exec_output_frame"
                            );
                            bytes
                        })
                        .map_err(io::Error::other)
                }));

                Ok(Box::new(tokio::io::join(reader, input)))
            }
            StartExecResults::Detached => {
                anyhow::bail!("docker exec unexpectedly started in detached mode")
            }
        }
    }

    fn describe(&self) -> String {
        format!("docker://{}:{}", self.container, self.port)
    }
}

impl DockerTarget {
    /// Turn a bollard error into an actionable message. A `DockerResponseServerError`
    /// means the daemon answered — so it's reachable — and the HTTP status tells
    /// us what's wrong with the container. Anything else means we never reached
    /// the daemon at all.
    fn explain(&self, err: BollardError) -> anyhow::Error {
        let container = &self.container;
        match err {
            BollardError::DockerResponseServerError {
                status_code: 404, ..
            } => anyhow::anyhow!(
                "container {container:?} not found — check the name with: docker ps"
            ),
            BollardError::DockerResponseServerError {
                status_code: 409, ..
            } => anyhow::anyhow!(
                "container {container:?} is not running — start it with: docker start {container}"
            ),
            BollardError::DockerResponseServerError {
                status_code,
                message,
            } => anyhow::anyhow!(
                "docker refused exec into {container:?} (HTTP {status_code}): {message}"
            ),
            other => anyhow::Error::new(other).context(
                "cannot reach the Docker daemon — is it running? (on macOS, start Docker Desktop)",
            ),
        }
    }
}

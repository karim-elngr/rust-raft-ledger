use ledger_core::LedgerId;
use ledger_node::{CheckpointPolicy, ClientTls, Peer, PeerConfig};
use ledger_replica::{CacheBudget, ReplicaConfig, StaticReplicaConfig};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tonic::transport::Endpoint;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    cluster_id: String,
    ledger_id: String,
    node_id: u64,
    learner_id: Option<u64>,
    export: Option<ExportConfig>,
    asset: String,
    scale: u8,
    listen: SocketAddr,
    data_dir: PathBuf,
    recovery_dir: Option<PathBuf>,
    #[serde(default)]
    cache: CacheBudget,
    #[serde(default)]
    checkpoint: CheckpointPolicy,
    client: Option<FileClient>,
    monitor: Option<FileClient>,
    tls: TlsFiles,
    peers: Vec<FilePeer>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsFiles {
    ca_pem: PathBuf,
    cert_pem: PathBuf,
    key_pem: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileClient {
    admin_cert_der: Option<Vec<PathBuf>>,
    listen: SocketAddr,
    ca_pem: PathBuf,
    cert_pem: PathBuf,
    key_pem: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePeer {
    node_id: u64,
    endpoint: String,
    domain: String,
    cert_der: PathBuf,
}

mod exports;
pub(crate) use exports::{ArchiveConfig, ExportConfig};

pub(crate) struct Config {
    pub(crate) data_dir: PathBuf,
    pub(crate) recovery_dir: Option<PathBuf>,
    pub(crate) cache: CacheBudget,
    pub(crate) checkpoint: CheckpointPolicy,
    pub(crate) replica: StaticReplicaConfig,
    pub(crate) export: Option<ExportConfig>,
    pub(crate) listen: SocketAddr,
    base: PathBuf,
    client: Option<FileClient>,
    monitor: Option<FileClient>,
    tls: TlsFiles,
    peers: Vec<FilePeer>,
}

impl Config {
    pub(crate) fn load(path: &Path) -> crate::Result<Self> {
        let source =
            fs::read_to_string(path).map_err(|e| format!("cannot read configuration: {e}"))?;
        // TOML diagnostics can quote the input line. Report its location without
        // echoing any user-supplied contents into logs.
        let raw: FileConfig =
            toml::from_str(&source).map_err(|e: toml::de::Error| match e.span() {
                Some(span) => {
                    let line = 1 + source.as_bytes()[..span.start]
                        .iter()
                        .filter(|&&c| c == b'\n')
                        .count();
                    format!("invalid configuration near line {line}; check fields and types")
                }
                None => "invalid configuration; check required fields and types".to_owned(),
            })?;
        let cluster = parse_id(&raw.cluster_id, "cluster_id")?;
        let ledger = LedgerId(parse_id(&raw.ledger_id, "ledger_id")?);
        if cluster == 0 || raw.node_id == 0 || raw.asset.trim().is_empty() {
            return Err("cluster_id and node_id must be nonzero and asset must be nonempty".into());
        }
        let remote_count = if raw.learner_id.is_some() { 3 } else { 2 };
        if raw.data_dir.as_os_str().is_empty() || raw.peers.len() != remote_count {
            return Err(
                format!("a data_dir and exactly {remote_count} remote peers are required").into(),
            );
        }

        let base = path.parent().unwrap_or(Path::new("."));
        let learner = raw.learner_id == Some(raw.node_id);
        if learner && raw.client.is_some() {
            return Err("a learner cannot serve the client API".into());
        }
        if let Some(monitor) = &raw.monitor
            && monitor.admin_cert_der.is_some()
        {
            return Err("monitor listener does not accept payment permissions".into());
        }
        if raw.export.is_some() && !learner {
            return Err("only the configured learner may export".into());
        }
        if raw.client.is_some() && raw.recovery_dir.is_none() && raw.learner_id.is_none() {
            return Err("the client listener requires recovery_dir".into());
        }
        let data_dir = base.join(raw.data_dir);
        let recovery_dir = raw
            .recovery_dir
            .as_deref()
            .map(|dir| archive_path(base, dir))
            .transpose()?;
        let mut peers = raw.peers;
        let mut members = BTreeSet::from([raw.node_id]);
        let mut domains = BTreeSet::new();
        for peer in &mut peers {
            peer.domain.make_ascii_lowercase();
            if peer.node_id == 0 || !members.insert(peer.node_id) {
                return Err(
                    "peer node_ids must be nonzero and distinct from all other members".into(),
                );
            }
            if peer.domain.is_empty()
                || peer.domain.ends_with('.')
                || peer.domain.contains('*')
                || !domains.insert(peer.domain.clone())
            {
                return Err("each remote peer needs a distinct TLS DNS name".into());
            }
            let endpoint = Endpoint::from_shared(peer.endpoint.clone())
                .map_err(|_| "invalid peer endpoint URI")?;
            if endpoint.uri().scheme_str() != Some("https") || endpoint.uri().host().is_none() {
                return Err("peer endpoint must be an absolute HTTPS URI".into());
            }
        }
        if let Some(learner) = raw.learner_id
            && !members.remove(&learner)
        {
            return Err("learner_id must identify a configured member".into());
        }
        let mut export = learner.then(|| raw.export.unwrap_or_default());
        #[cfg(feature = "kafka")]
        if let Some(settings) = &mut export {
            settings.resolve_kafka_paths(base)?;
        }
        if let Some(ExportConfig {
            archive: ArchiveConfig::Posix { path },
            ..
        }) = &mut export
        {
            if recovery_dir.is_some() {
                return Err("configure one explicit POSIX archive path".into());
            }
            *path = archive_path(base, path)?;
            ledger_export_adapters::PosixArchive::check_configured_paths(path, &data_dir)?;
        }
        let replica = ReplicaConfig {
            cluster,
            node: raw.node_id,
            voters: members.into_iter().collect(),
            ledger,
            asset: raw.asset,
            scale: raw.scale,
        };
        let replica = match raw.learner_id {
            Some(id) => replica.with_learner(id),
            None => replica.into(),
        };
        Ok(Self {
            data_dir,
            export,
            replica,
            recovery_dir,
            cache: raw.cache,
            checkpoint: raw.checkpoint,
            base: base.to_owned(),
            client: raw.client,
            monitor: raw.monitor,
            tls: raw.tls,
            peers,
            listen: raw.listen,
        })
    }

    /// Only call for actions that must access the recovery mount.
    pub(crate) fn journal_path(&self) -> crate::Result<Option<PathBuf>> {
        let path = self
            .recovery_dir
            .as_deref()
            .or_else(|| match &self.export.as_ref()?.archive {
                ArchiveConfig::Posix { path } => Some(path.as_path()),
                ArchiveConfig::Noop => None,
            });
        Ok(path
            .map(|p| ledger_export_adapters::PosixArchive::resolve_separate_path(p, &self.data_dir))
            .transpose()?)
    }

    fn credential(&self, relative: &Path, field: &str) -> crate::Result<Vec<u8>> {
        if relative.as_os_str().is_empty() {
            return Err(format!("{field} requires a credential file path").into());
        }
        let contents =
            fs::read(self.base.join(relative)).map_err(|e| format!("cannot read {field}: {e}"))?;
        if contents.is_empty() {
            return Err(format!("{field} is empty").into());
        }
        Ok(contents)
    }

    pub(crate) fn load_client(&self) -> crate::Result<Option<(SocketAddr, ClientTls)>> {
        self.load_listener(self.client.as_ref())
    }

    pub(crate) fn load_monitor(&self) -> crate::Result<Option<(SocketAddr, ClientTls)>> {
        self.load_listener(self.monitor.as_ref())
    }

    fn load_listener(
        &self,
        settings: Option<&FileClient>,
    ) -> crate::Result<Option<(SocketAddr, ClientTls)>> {
        settings
            .map(|listener| {
                Ok((
                    listener.listen,
                    ClientTls {
                        ca_pem: self.credential(&listener.ca_pem, "listener.ca_pem")?,
                        cert_pem: self.credential(&listener.cert_pem, "listener.cert_pem")?,
                        key_pem: self.credential(&listener.key_pem, "listener.key_pem")?,
                    },
                ))
            })
            .transpose()
    }

    pub(crate) fn load_admin_pins(&self) -> crate::Result<Option<Vec<Vec<u8>>>> {
        self.client
            .as_ref()
            .and_then(|client| client.admin_cert_der.as_ref())
            .map(|paths| {
                paths
                    .iter()
                    .map(|path| self.credential(path, "client.admin_cert_der"))
                    .collect()
            })
            .transpose()
    }

    /// Only a member being initialized or started needs its transport credentials.
    pub(crate) fn load_transport(&self) -> crate::Result<PeerConfig> {
        let mut peers = Vec::new();
        for remote in &self.peers {
            let cert_der = self.credential(&remote.cert_der, "peers.cert_der")?;
            if peers.iter().any(|p: &Peer| p.cert_der == cert_der) {
                return Err("remote peers must have distinct certificate pins".into());
            }
            peers.push(Peer {
                node: remote.node_id,
                endpoint: remote.endpoint.clone(),
                domain: remote.domain.clone(),
                cert_der,
            });
        }
        Ok(PeerConfig {
            cluster: self.replica.cluster,
            node: self.replica.node,
            peers,
            ca_pem: self.credential(&self.tls.ca_pem, "tls.ca_pem")?,
            cert_pem: self.credential(&self.tls.cert_pem, "tls.cert_pem")?,
            key_pem: self.credential(&self.tls.key_pem, "tls.key_pem")?,
        })
    }
}

fn archive_path(base: &Path, path: &Path) -> crate::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err("empty recovery directory path".into());
    }
    Ok(base.join(path))
}

fn parse_id(input: &str, field: &str) -> crate::Result<u128> {
    if input.is_empty()
        || !input.bytes().all(|b| b.is_ascii_digit())
        || (input.len() > 1 && input.starts_with('0'))
    {
        return Err(format!("{field} must be a canonical unsigned decimal string").into());
    }
    input
        .parse()
        .map_err(|_| format!("{field} exceeds the 128-bit range").into())
}

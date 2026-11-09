mod config;

use ledger_node::{ClientServer, Node};
use ledger_recovery::{Head, Journal, Spec};
use ledger_replica::Replica;
use std::error::Error;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::net::TcpListener;

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Init,
    Run,
    JournalInit,
    JournalHead,
    JournalScrub,
    JournalCheckpoint,
    JournalCheckpointFromJournal,
    Fence,
    Restore,
    RestoreFromJournal,
    Activate,
    Checkpoint,
    CheckpointScrub,
    Rebuild,
    Reclaim,
}

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const USAGE: &str = "usage: ledger-node <init|run|checkpoint|checkpoint-scrub|rebuild|reclaim|journal-init|journal-head|journal-scrub|journal-checkpoint|journal-checkpoint-from-journal|fence|restore|restore-from-journal|activate> CONFIG.toml";

fn arguments(args: impl IntoIterator<Item = OsString>) -> Result<(Action, PathBuf)> {
    let mut args = args.into_iter();
    let action = args.next();
    let config = args.next();
    let action = match action.as_deref().and_then(|s| s.to_str()) {
        Some("init") => Action::Init,
        Some("run") => Action::Run,
        Some("journal-init") => Action::JournalInit,
        Some("journal-head") => Action::JournalHead,
        Some("journal-scrub") => Action::JournalScrub,
        Some("journal-checkpoint") => Action::JournalCheckpoint,
        Some("journal-checkpoint-from-journal") => Action::JournalCheckpointFromJournal,
        Some("fence") => Action::Fence,
        Some("restore") => Action::Restore,
        Some("restore-from-journal") => Action::RestoreFromJournal,
        Some("activate") => Action::Activate,
        Some("checkpoint") => Action::Checkpoint,
        Some("checkpoint-scrub") => Action::CheckpointScrub,
        Some("rebuild") => Action::Rebuild,
        Some("reclaim") => Action::Reclaim,
        _ => return Err(USAGE.into()),
    };
    let config = config.filter(|s| !s.is_empty()).ok_or(USAGE)?;
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    Ok((action, PathBuf::from(config)))
}

fn open(config: &config::Config, action: Action) -> Result<Replica> {
    Ok(match action {
        Action::Init => {
            Replica::bootstrap_with_cache(&config.data_dir, config.replica.clone(), config.cache)?
        }
        Action::Run => {
            Replica::reopen_with_cache(&config.data_dir, config.replica.clone(), config.cache)?
        }
        _ => return Err("expected init or run".into()),
    })
}

#[cfg(unix)]
fn shutdown_signal() -> std::io::Result<impl Future<Output = std::io::Result<()>>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
        Ok(())
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> std::io::Result<impl Future<Output = std::io::Result<()>>> {
    Ok(tokio::signal::ctrl_c())
}

fn show_head(head: &Head) {
    use std::fmt::Write;
    let mut digest = String::new();
    for byte in head.digest {
        write!(digest, "{byte:02x}").expect("writing to a string");
    }
    println!(
        "cluster={} index={} term={} fenced={} sha256={digest}",
        head.cluster, head.index, head.term, head.fenced
    );
}

async fn execute() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!("{USAGE}");
        return Ok(());
    }
    let (action, file) = arguments(args)?;
    let mut config = config::Config::load(&file)?;
    // These operations need exclusive access to one voter disk. The voter
    // can be offline, including its transport identities and recovery mount.
    match action {
        Action::Rebuild => {
            let voter = Replica::rebuild_from_log(&config.data_dir, config.replica, config.cache)?;
            println!(
                "Rebuilt voter {} from its Raft log through index {}.",
                voter.node_id(),
                voter.applied_index()
            );
            return Ok(());
        }
        Action::Checkpoint | Action::CheckpointScrub => {
            let reference = if action == Action::CheckpointScrub {
                Replica::scrub_stored_checkpoint(&config.data_dir, config.replica)?
            } else {
                let voter = open(&config, Action::Run)?;
                match voter.begin_checkpoint()? {
                    Some(task) => task.finish()?,
                    None => voter.current_checkpoint()?.ok_or("no local checkpoint")?,
                }
            };
            println!(
                "Local checkpoint index={} term={}.",
                reference.index, reference.term
            );
            return Ok(());
        }
        _ => {}
    }
    let spec = Spec {
        ledger: config.replica.ledger.0,
        asset: config.replica.asset.clone(),
        scale: config.replica.scale,
    };
    if action == Action::JournalInit {
        let path = config
            .journal_path()?
            .ok_or("configure a POSIX archive directory")?;
        let journal = Journal::init(&path, &spec, config.replica.cluster)?;
        show_head(&journal.head()?);
        return Ok(());
    }
    let learner_topology = config.replica.learner_id().is_some();
    let live = matches!(action, Action::Init | Action::Run);
    if live && learner_topology && config.recovery_dir.is_some() {
        return Err("recovery_dir is only for offline administration in a learner topology".into());
    }
    let journal = if live && learner_topology {
        None
    } else {
        config
            .journal_path()?
            .map(|dir| Journal::open(&dir, &spec))
            .transpose()?
    };
    match action {
        Action::Init => {
            config.load_transport()?;
            config.load_client()?;
            config.load_admin_pins()?;
            config.load_monitor()?;
            if let Some(journal) = &journal {
                let head = journal.head()?;
                if head.cluster != config.replica.cluster || head.fenced || head.index != 0 {
                    return Err("genesis initialization requires the active, empty journal for this cluster".into());
                }
            }
            let replica = open(&config, Action::Init)?;
            println!(
                "Initialized {} {}.",
                if replica.is_learner() {
                    "learner"
                } else {
                    "voter"
                },
                replica.node_id()
            );
            return Ok(());
        }
        Action::Run => {}
        recovery_action => {
            let journal = journal.as_ref().ok_or("recovery_dir is required")?;
            let head = journal.head()?;
            match recovery_action {
                Action::JournalHead => show_head(&head),
                Action::Reclaim => {
                    let voter = open(&config, Action::Run)?;
                    let stats = voter.reclaim_log(journal)?;
                    println!(
                        "Local Raft reclaim through={} reclaimed_entries={} reclaimed_bytes={} live_sst_bytes_before={} live_sst_bytes_after={}",
                        stats.through,
                        stats.reclaimed_entries,
                        stats.reclaimed_bytes,
                        stats.live_sst_bytes_before,
                        stats.live_sst_bytes_after
                    );
                }
                Action::JournalCheckpoint | Action::JournalCheckpointFromJournal => {
                    if head.cluster != config.replica.cluster {
                        return Err("source generation does not match configuration".into());
                    }
                    let build = if recovery_action == Action::JournalCheckpointFromJournal {
                        Replica::rebuild_recovery_base
                    } else {
                        Replica::create_recovery_base
                    };
                    let stats = build(journal, &head, config.cache)?;
                    println!(
                        "Built recovery checkpoint through index {}. base_index={} replayed_entries={}",
                        head.index, stats.base_index, stats.replayed_entries
                    );
                    show_head(&head);
                }
                Action::JournalScrub => {
                    journal.verify_since(None, &head, |_| Ok(()))?;
                    show_head(&head);
                }
                Action::Fence => {
                    if head.cluster != config.replica.cluster {
                        return Err("source generation does not match configuration".into());
                    }
                    show_head(&journal.fence(&head)?);
                }
                Action::Restore | Action::RestoreFromJournal => {
                    let restore = if recovery_action == Action::RestoreFromJournal {
                        Replica::restore_journal_only
                    } else {
                        Replica::restore_with_stats
                    };
                    let (voter, stats) = restore(
                        &config.data_dir,
                        config.replica.clone(),
                        journal,
                        &head,
                        config.cache,
                    )?;
                    println!(
                        "Restored voter {} through index {}.",
                        voter.node_id(),
                        voter.applied_index()
                    );
                    println!(
                        "Recovery work base_index={} replayed_entries={}",
                        stats.base_index, stats.replayed_entries
                    );
                }
                Action::Activate => {
                    if !head.fenced || config.replica.cluster == head.cluster {
                        return Err(
                            "expected a fenced predecessor of the replacement cluster".into()
                        );
                    }
                    let voter = open(&config, Action::Run)?;
                    let (active, stats) = voter.activate_replacement(journal, &head)?;
                    eprintln!(
                        "Activation work base_index={} compared_entries={}",
                        stats.base_index, stats.replayed_entries
                    );
                    show_head(&active);
                }
                _ => unreachable!("recovery action"),
            }
            return Ok(());
        }
    }
    let peers = config.load_transport()?;
    let backend = match config.load_client()? {
        Some((address, tls)) => Some((TcpListener::bind(address).await?, tls)),
        None => None,
    };
    let admin_pins = config.load_admin_pins()?;
    let monitor_listener = match config.load_monitor()? {
        Some((address, tls)) => Some((TcpListener::bind(address).await?, tls)),
        None => None,
    };
    let signal = shutdown_signal()?;
    let replica = open(&config, Action::Run)?;
    let id = replica.node_id();
    let listener = TcpListener::bind(config.listen).await?;
    let address = listener.local_addr()?;
    let node = Node::start_configured(replica, peers, listener, journal, config.checkpoint).await?;
    let exporter = match (config.export.take(), node.learner_feed()) {
        (Some(settings), Some(feed)) => {
            let data_dir = config.data_dir.clone();
            Some(tokio::task::spawn_blocking(move || settings.start(&data_dir, feed)).await??)
        }
        (None, None) => None,
        _ => return Err("export configuration and durable learner role disagree".into()),
    };
    let role = if exporter.is_some() {
        "Learner"
    } else {
        "Voter"
    };
    eprintln!("{role} {id} listening on {address}.");
    let monitor = async {
        let Some((workers, _)) = &exporter else {
            return std::future::pending::<()>().await;
        };
        let mut previous = None;
        loop {
            let status = workers.status();
            if previous.as_ref() != Some(&status) {
                eprintln!("Learner exports {status:?}");
                previous = Some(status);
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    };
    let mut api = None;
    let mut status = None;
    let serve: Result<()> = async {
        if let Some((listener, tls)) = backend {
            let address = listener.local_addr()?;
            api = Some(
                ClientServer::start_authorized(node.handle(), listener, tls, admin_pins).await?,
            );
            eprintln!("Ledger API listening on {address}.");
        }
        if let Some((listener, tls)) = monitor_listener {
            let address = listener.local_addr()?;
            status = Some(match &exporter {
                Some((workers, _)) => {
                    ClientServer::start_monitor(node.handle(), listener, tls, workers.clone())
                        .await?
                }
                None => ClientServer::start_voter_monitor(node.handle(), listener, tls).await?,
            });
            eprintln!("{role} monitor listening on {address}.");
        }
        tokio::select! {
            () = monitor => unreachable!(),
            result = signal => result?,
            _ = node.wait_stopped() => (),
            _ = wait_listener(api.as_ref()) => (),
            _ = wait_listener(status.as_ref()) => (),
        }
        Ok(())
    }
    .await;
    let stop_exports: Result<()> = async {
        if let Some((_, running)) = exporter {
            tokio::task::spawn_blocking(move || running.shutdown()).await??;
        }
        Ok(())
    }
    .await;
    let (node_result, api_result, status_result) =
        tokio::join!(node.shutdown(), stop_listener(api), stop_listener(status));
    serve?;
    stop_exports?;
    node_result?;
    api_result?;
    status_result?;
    Ok(())
}

async fn wait_listener(server: Option<&ClientServer>) {
    match server {
        Some(server) => server.wait_stopped().await,
        None => std::future::pending().await,
    }
}

async fn stop_listener(
    server: Option<ClientServer>,
) -> std::result::Result<(), ledger_node::NodeError> {
    if let Some(server) = server {
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ledger-node: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    const CONFIG: &str = r#"
cluster_id = "340282366920938463463374607431768211455"
ledger_id = "1"
node_id = 1
asset = "USD"
scale = 2
listen = "127.0.0.1:7101"
data_dir = "voter"
[tls]
ca_pem = "ca.pem"
cert_pem = "local.pem"
key_pem = "local.key"
[[peers]]
node_id = 2
endpoint = "https://127.0.0.1:7102"
domain = "voter2.example"
cert_der = "peer2.der"
[[peers]]
node_id = 3
endpoint = "https://127.0.0.1:7103"
domain = "voter3.example"
cert_der = "peer3.der"
"#;

    fn fixture(root: &std::path::Path) -> PathBuf {
        for file in ["ca.pem", "local.pem", "local.key", "peer2.der", "peer3.der"] {
            fs::write(root.join(file), file.as_bytes()).unwrap();
        }
        let path = root.join("node.toml");
        fs::write(&path, CONFIG).unwrap();
        path
    }

    #[test]
    fn bootstrap_is_explicit_and_reopen_uses_the_config_directory() {
        let dir = tempdir().unwrap();
        let path = fixture(dir.path());
        let config = config::Config::load(&path).unwrap();
        assert_eq!(config.data_dir, dir.path().join("voter"));
        assert_eq!(config.replica.cluster, u128::MAX);
        let transport = config.load_transport().unwrap();
        assert_eq!(transport.key_pem, b"local.key");
        assert_eq!(transport.peers[0].cert_der, b"peer2.der");
        assert!(open(&config, Action::Run).is_err());
        assert!(!config.data_dir.exists());
        drop(open(&config, Action::Init).unwrap());
        assert!(open(&config, Action::Init).is_err());
        assert_eq!(open(&config, Action::Run).unwrap().node_id(), 1);
    }

    #[test]
    fn strict_fields_and_membership_are_checked_without_creating_state() {
        let dir = tempdir().unwrap();
        let path = fixture(dir.path());
        for erroneous in [
            CONFIG.replace("scale = 2", "scale = 2\nmisspelled = 3"),
            CONFIG.replace(
                "cert_der = \"peer2.der\"",
                "cert_der = \"peer2.der\"\nextra = 3",
            ),
            CONFIG.replace("key_pem = \"local.key\"", ""),
            CONFIG.replace("node_id = 3", "node_id = 2"),
            CONFIG.replace("https://127.0.0.1:7102", "http://127.0.0.1:7102"),
            CONFIG.replace("ledger_id = \"1\"", "ledger_id = \"+1\""),
        ] {
            fs::write(&path, erroneous).unwrap();
            assert!(config::Config::load(&path).is_err());
        }
        fs::write(&path, CONFIG).unwrap();
        fs::write(dir.path().join("local.key"), []).unwrap();
        assert!(
            config::Config::load(&path)
                .unwrap()
                .load_transport()
                .is_err()
        );
        assert!(!dir.path().join("voter").exists());
    }

    #[test]
    fn voter_can_reopen_with_a_different_cache_budget() {
        let dir = tempdir().unwrap();
        let path = fixture(dir.path());
        let default = config::Config::load(&path).unwrap();
        drop(open(&default, Action::Init).unwrap());
        fs::write(&path, format!("{CONFIG}\n[cache]\naccount_bytes = 0\n")).unwrap();
        let cold = config::Config::load(&path).unwrap();
        assert_eq!(cold.cache.block_bytes, default.cache.block_bytes);
        let replica = open(&cold, Action::Run).unwrap();
        assert_eq!(replica.cache_stats().slot_capacity, 0);
    }

    #[test]
    fn public_listener_requires_recovery_and_loads_credentials_only_for_a_voter() {
        let dir = tempdir().unwrap();
        let path = fixture(dir.path());
        let client = r#"
[client]
listen = "127.0.0.1:7201"
ca_pem = "backend-ca.pem"
cert_pem = "backend-cert.pem"
key_pem = "backend-key.pem"
"#;
        fs::write(&path, format!("{CONFIG}{client}")).unwrap();
        assert!(config::Config::load(&path).is_err());
        fs::write(
            &path,
            format!("recovery_dir = \"journal\"\n{CONFIG}{client}"),
        )
        .unwrap();
        let config = config::Config::load(&path).unwrap();
        assert!(config.load_client().is_err());
        for file in ["backend-ca.pem", "backend-cert.pem", "backend-key.pem"] {
            fs::write(dir.path().join(file), file).unwrap();
        }
        let (address, tls) = config.load_client().unwrap().unwrap();
        assert_eq!(address.port(), 7201);
        assert_eq!(tls.ca_pem, b"backend-ca.pem");
        assert_eq!(tls.cert_pem, b"backend-cert.pem");
        assert_eq!(tls.key_pem, b"backend-key.pem");
        assert!(config.load_admin_pins().unwrap().is_none());
        let with_pins = |value: &str| {
            format!("recovery_dir = \"journal\"\n{CONFIG}{client}admin_cert_der = {value}\n")
        };
        fs::write(&path, with_pins("[]")).unwrap();
        assert_eq!(
            config::Config::load(&path)
                .unwrap()
                .load_admin_pins()
                .unwrap(),
            Some(vec![])
        );
        fs::write(&path, with_pins("[\"admin.der\"]")).unwrap();
        let restricted = config::Config::load(&path).unwrap();
        assert!(restricted.load_admin_pins().is_err());
        fs::write(dir.path().join("admin.der"), b"admin certificate bytes").unwrap();
        assert_eq!(
            restricted.load_admin_pins().unwrap(),
            Some(vec![b"admin certificate bytes".to_vec()])
        );
    }

    #[test]
    fn static_learner_topology_places_exports_only_on_the_nonvoter() {
        let root = tempdir().unwrap();
        let path = root.path().join("node.toml");
        let fourth = r#"
[[peers]]
node_id = 4
endpoint = "https://127.0.0.1:7104"
domain = "learner.example"
cert_der = "learner.der"
"#;
        let voter = format!("learner_id = 4\n{CONFIG}{fourth}");
        fs::write(&path, &voter).unwrap();
        let config = config::Config::load(&path).unwrap();
        assert_eq!(config.replica.learner_id(), Some(4));
        assert_eq!(config.replica.voters, vec![1, 2, 3]);
        let noop = "\n[export.archive]\nkind = \"noop\"\n[export.publisher]\nkind = \"noop\"\n";
        fs::write(&path, format!("{voter}{noop}")).unwrap();
        assert!(config::Config::load(&path).is_err());
        let learner = voter
            .replace("node_id = 1", "node_id = 4")
            .replace("node_id = 4\nendpoint", "node_id = 1\nendpoint");
        fs::write(&path, format!("{learner}{noop}")).unwrap();
        let config = config::Config::load(&path).unwrap();
        assert!(config.replica.is_learner());
        assert!(config.export.is_some());
        fs::write(&path, format!("{learner}{noop}\n[client]\nlisten = \"127.0.0.1:9000\"\nca_pem = \"c\"\ncert_pem = \"p\"\nkey_pem = \"k\"")).unwrap();
        assert!(config::Config::load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn learner_configuration_is_independent_of_a_broken_archive_mount() {
        use std::os::unix::fs::symlink;
        let root = tempdir().unwrap();
        let path = root.path().join("node.toml");
        let archive = root.path().join("mount");
        symlink("mount", &archive).unwrap();
        let config = format!(
            r#"learner_id = 1
{CONFIG}
[[peers]]
node_id = 4
endpoint = "https://127.0.0.1:7104"
domain = "peer-4.example"
cert_der = "peer-4.der"
[export.archive]
kind = "posix"
path = "mount/journal"
"#
        );
        fs::write(&path, config).unwrap();
        let config = config::Config::load(&path).unwrap();
        assert!(config.replica.is_learner());
        assert!(config.journal_path().is_err());
        fs::remove_file(&archive).unwrap();
        fs::create_dir_all(&config.data_dir).unwrap();
        symlink(&config.data_dir, &archive).unwrap();
        assert!(config.journal_path().is_err());
        assert!(!config.data_dir.join("journal").exists());
    }

    #[test]
    fn only_init_or_run_with_one_path_is_valid() {
        let parse = |args: &[&str]| arguments(args.iter().map(OsString::from));
        assert_eq!(
            parse(&["run", "x.toml"]).unwrap(),
            (Action::Run, "x.toml".into())
        );
        for bad in [
            &[][..],
            &["x.toml"],
            &["reset", "x.toml"],
            &["init", "x.toml", "extra"],
        ] {
            assert!(parse(bad).is_err());
        }
    }
}

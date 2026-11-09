use std::fmt::Write;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, ExitStatus};

use super::*;

/// The guard also reaps a failed or timed-out test's subprocess.
struct Process(Child);

impl Process {
    fn spawn(verb: &str, config: &Path) -> Self {
        Self(
            ProcessCommand::new(env!("CARGO_BIN_EXE_ledger-node"))
                .arg(verb)
                .arg(config)
                .spawn()
                .unwrap(),
        )
    }

    async fn wait_exit(&mut self) -> ExitStatus {
        timeout(WAIT, async {
            loop {
                if let Some(exit) = self.0.try_wait().unwrap() {
                    return exit;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("ledger-node did not exit in time")
    }

    async fn wait_success(&mut self) {
        let exit = self.wait_exit().await;
        assert!(exit.success(), "ledger-node exited with {exit}");
    }

    async fn stop(mut self) {
        assert!(self.0.try_wait().unwrap().is_none(), "voter exited early");
        let sent = ProcessCommand::new("/bin/kill")
            .arg("-TERM")
            .arg(self.0.id().to_string())
            .status()
            .unwrap();
        assert!(sent.success(), "failed to signal the child voter");
        self.wait_success().await;
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn write_configs(cluster: &Cluster, public: Option<(&Cluster, &Path)>) -> BTreeMap<u64, PathBuf> {
    write_topology(cluster, public, None)
}

fn write_topology(
    cluster: &Cluster,
    public: Option<(&Cluster, &Path)>,
    learner: Option<u64>,
) -> BTreeMap<u64, PathBuf> {
    write_generation(cluster, public, learner, CLUSTER)
}

fn write_generation(
    cluster: &Cluster,
    public: Option<(&Cluster, &Path)>,
    learner: Option<u64>,
    generation: u128,
) -> BTreeMap<u64, PathBuf> {
    let dir = cluster.directory.path();
    fs::write(dir.join("ca.pem"), cluster.ca.pem()).unwrap();
    if let Some((service, _)) = public {
        fs::write(dir.join("client-ca.pem"), service.ca.pem()).unwrap();
    }
    let mut configs = BTreeMap::new();
    for (&id, member) in &cluster.members {
        fs::write(dir.join(format!("n{id}.pem")), &member.identity.cert_pem).unwrap();
        fs::write(dir.join(format!("n{id}.key")), &member.identity.key_pem).unwrap();
        fs::write(dir.join(format!("n{id}.der")), &member.identity.cert_der).unwrap();
        let mut config = format!(
            r#"
cluster_id = "{generation}"
ledger_id = "{}"
node_id = {id}
asset = "USD"
scale = 2
listen = "{}"
data_dir = "n{id}"
"#,
            BOOK.0, member.addr
        );
        if let Some(id) = learner {
            writeln!(config, "learner_id = {id}").unwrap();
        } else if let Some((_, journal)) = public {
            writeln!(config, "recovery_dir = \"{}\"", journal.display()).unwrap();
        }
        writeln!(
            config,
            r#"
[tls]
ca_pem = "ca.pem"
cert_pem = "n{id}.pem"
key_pem = "n{id}.key""#
        )
        .unwrap();
        for (&peer, remote) in &cluster.members {
            if peer != id {
                writeln!(
                    config,
                    r#"
[[peers]]
node_id = {peer}
endpoint = "https://{}"
domain = "{}"
cert_der = "n{peer}.der""#,
                    remote.addr, remote.domain
                )
                .unwrap();
            }
        }
        if let Some((service, _)) = public
            && learner != Some(id)
        {
            let member = &service.members[&id];
            fs::write(dir.join(format!("api-{id}.pem")), &member.identity.cert_pem).unwrap();
            fs::write(dir.join(format!("api-{id}.key")), &member.identity.key_pem).unwrap();
            writeln!(
                config,
                r#"
[client]
listen = "{}"
ca_pem = "client-ca.pem"
cert_pem = "api-{id}.pem"
key_pem = "api-{id}.key""#,
                member.addr
            )
            .unwrap();
            writeln!(config, "\n[checkpoint]\ninterval_entries = 1").unwrap();
        }
        let path = dir.join(format!("n{id}.toml"));
        fs::write(&path, config).unwrap();
        configs.insert(id, path);
    }
    configs
}

async fn committed_after(handle: &NodeHandle, index: u64, term: u64) -> (u64, u64) {
    timeout(WAIT, async {
        loop {
            let state = handle.status().await.unwrap();
            if state.applied_index > index && state.term > term && state.leader_id.is_some() {
                return (state.applied_index, state.term);
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("voters did not commit a new election entry")
}

/// A live voter has sealed files once a final manifest is visible. An offline
/// scrub after shutdown separately proves the Raft pointer was published.
async fn await_seal(voter_dir: &Path) {
    let directory = voter_dir.join("checkpoints");
    timeout(WAIT, async {
        loop {
            match fs::read_dir(&directory) {
                Ok(mut entries) => {
                    if entries.any(|e| {
                        let e = e.unwrap();
                        e.file_name().to_string_lossy().starts_with("gen-")
                            && e.path().join("manifest").is_file()
                    }) {
                        return;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => panic!("could not inspect checkpoint directory: {e}"),
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("no automatically sealed checkpoint appeared");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_voter_processes_commit_again_after_restart() {
    let mut cluster = Cluster::new().await;
    let configs = write_configs(&cluster, None);
    for id in [1, 2] {
        Process::spawn("init", &configs[&id]).wait_success().await;
        assert!(
            cluster
                .directory
                .path()
                .join(format!("n{id}/raft/CURRENT"))
                .is_file()
        );
        drop(
            cluster
                .members
                .get_mut(&id)
                .unwrap()
                .listener
                .take()
                .unwrap(),
        );
    }

    // Voters 1 and 2 execute the built command-line binary. Voter 3 exposes
    // only its local observation, so progress here requires a real child voter.
    let first = Process::spawn("run", &configs[&1]);
    let second = Process::spawn("run", &configs[&2]);
    cluster.start(3, cluster.config(3)).await;
    let (index, term) = committed_after(&cluster.handle(3), 0, 0).await;
    cluster.stop(3).await;
    first.stop().await;
    second.stop().await;

    // With voter 2 still offline, the restarted child and reopened observer
    // must establish a fresh majority from their previous Raft databases.
    let restarted = Process::spawn("run", &configs[&1]);
    cluster.start(3, cluster.config(3)).await;
    committed_after(&cluster.handle(3), index, term).await;
    cluster.stop(3).await;
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_cli_voters_serve_protected_account_after_process_restart() {
    use ledger_api::client::{ClientSeed, LedgerClient};
    use ledger_api::v1;

    let mut peers = Cluster::new().await;
    let mut public = Cluster::new().await;
    let external = TempDir::new().unwrap();
    let journal = external.path().join("journal");
    let configs = write_configs(&peers, Some((&public, &journal)));
    let client_identity = leaf("backend.test", &public.ca, &public.ca_key);
    let seeds = public.members.iter().map(|(&node, member)| ClientSeed {
        node,
        endpoint: format!("https://{}", member.addr),
        domain: member.domain.clone(),
    });
    let client = LedgerClient::connect(
        seeds.collect(),
        public.ca.pem().into_bytes(),
        client_identity.cert_pem,
        client_identity.key_pem,
    )
    .unwrap();
    Process::spawn("journal-init", &configs[&1])
        .wait_success()
        .await;
    for config in configs.values() {
        Process::spawn("init", config).wait_success().await;
    }
    let mut processes = Vec::new();
    for (&id, config) in &configs {
        drop(peers.members.get_mut(&id).unwrap().listener.take().unwrap());
        drop(
            public
                .members
                .get_mut(&id)
                .unwrap()
                .listener
                .take()
                .unwrap(),
        );
        processes.push(Process::spawn("run", config));
    }
    let create = v1::CreateAccountRequest {
        ledger_id: BOOK.0.to_string(),
        request_id: "100".into(),
        account_id: "52".into(),
        code: 27,
        normal: v1::Side::Credit.into(),
        bounds: None,
    };
    let receipt = client
        .create_account(create.clone(), Instant::now() + WAIT)
        .await
        .unwrap();
    assert!(matches!(
        receipt.result,
        Some(v1::decision::Result::Accepted(v1::Outcome {kind: Some(v1::outcome::Kind::AccountCreated(ref id))})) if id == &create.account_id
    ));
    let root = peers.directory.path();
    let repaired = root.join("n2");
    await_seal(&repaired).await;
    for voter in processes {
        voter.stop().await;
    }

    // This first scrub runs before any manual checkpoint command.
    Process::spawn("checkpoint-scrub", &configs[&2])
        .wait_success()
        .await;
    for id in [2, 3] {
        Process::spawn("checkpoint", &configs[&id])
            .wait_success()
            .await;
    }
    fs::rename(repaired.join("checkpoints"), root.join("lost-checkpoints")).unwrap();
    assert!(
        !Process::spawn("run", &configs[&2])
            .wait_exit()
            .await
            .success(),
        "ordinary restart accepted a missing checkpoint"
    );

    let disconnected = external.path().join("disconnected");
    fs::rename(&journal, &disconnected).unwrap();
    for id in [2, 3] {
        fs::rename(
            root.join(format!("n{id}.key")),
            root.join(format!("offline-{id}.key")),
        )
        .unwrap();
    }
    let working = root.join("n3/state/data/CURRENT");
    let working_identity = || {
        (
            fs::metadata(&working).unwrap().ino(),
            fs::read(&working).unwrap(),
        )
    };
    let before = working_identity();
    Process::spawn("checkpoint-scrub", &configs[&3])
        .wait_success()
        .await;
    assert_eq!(
        working_identity(),
        before,
        "scrub replaced the writable projection"
    );
    assert!(
        !Process::spawn("checkpoint-scrub", &configs[&2])
            .wait_exit()
            .await
            .success()
    );
    Process::spawn("rebuild", &configs[&2]).wait_success().await;
    fs::rename(disconnected, &journal).unwrap();
    for id in [2, 3] {
        fs::rename(
            root.join(format!("offline-{id}.key")),
            root.join(format!("n{id}.key")),
        )
        .unwrap();
    }

    // One restarted voter used its checkpoint and one rebuilt from its full log.
    // The surviving client retries its original request across the new quorum.
    let second = Process::spawn("run", &configs[&2]);
    let third = Process::spawn("run", &configs[&3]);
    assert_eq!(
        client
            .create_account(create.clone(), Instant::now() + WAIT)
            .await
            .unwrap(),
        receipt
    );
    let account = client
        .get_account(
            v1::GetAccountRequest {
                ledger_id: create.ledger_id.clone(),
                account_id: create.account_id.clone(),
            },
            Instant::now() + WAIT,
        )
        .await
        .unwrap()
        .account
        .expect("created account must survive");
    assert_eq!(account.ledger_id, create.ledger_id);
    assert_eq!(account.account_id, create.account_id);
    assert_eq!(account.code, create.code);
    assert_eq!(account.normal, create.normal);
    assert_eq!(account.available_balance, "0");
    await_seal(&repaired).await;
    second.stop().await;
    third.stop().await;
    Process::spawn("checkpoint-scrub", &configs[&2])
        .wait_success()
        .await;
}

#[path = "cli/learner.rs"]
#[cfg(feature = "kafka")]
mod learner;

#[path = "cli/rolling.rs"]
mod rolling;

#[path = "cli/monitor.rs"]
mod monitor;

#[path = "cli/process_crash.rs"]
mod process_crash;

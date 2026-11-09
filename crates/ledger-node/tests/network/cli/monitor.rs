use super::*;
use ledger_api::v1::{self, ledger_client::LedgerClient, monitor_client::MonitorClient};
use tonic::transport::{
    Certificate as Trust, Channel, ClientTlsConfig, Endpoint, Identity as Credential,
};

fn channel(addr: SocketAddr, domain: &str, ca: &Certificate, client: &Identity) -> Channel {
    Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name(domain)
                .ca_certificate(Trust::from_pem(ca.pem()))
                .identity(Credential::from_pem(&client.cert_pem, &client.key_pem)),
        )
        .unwrap()
        .connect_lazy()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn voter_operator_can_read_status_but_neither_listener_grants_it_payments() {
    let mut peers = Cluster::new().await;
    let mut public = Cluster::new().await;
    let mut operators = Cluster::with_ids([1]).await;
    let external = TempDir::new().unwrap();
    let configs = write_configs(&peers, Some((&public, &external.path().join("journal"))));
    let file = &configs[&1];
    let private = &operators.members[&1];
    let api = &public.members[&1];
    let admin = leaf("operator.test", &operators.ca, &operators.ca_key);
    let app = leaf("backend.test", &public.ca, &public.ca_key);
    let address = format!("https://{}", private.addr);
    let server_name = private.domain.clone();
    let status_channel = channel(private.addr, &private.domain, &operators.ca, &admin);
    let payment_channel = channel(api.addr, &api.domain, &public.ca, &app);
    let unauthorized_payment = channel(api.addr, &api.domain, &public.ca, &admin);
    let dir = peers.directory.path();
    fs::write(dir.join("operator-ca.pem"), operators.ca.pem()).unwrap();
    fs::write(dir.join("monitor.pem"), &private.identity.cert_pem).unwrap();
    fs::write(dir.join("monitor.key"), &private.identity.key_pem).unwrap();
    let config = format!(
        "{}\n[monitor]\nlisten = \"{}\"\nca_pem = \"operator-ca.pem\"\ncert_pem = \"monitor.pem\"\nkey_pem = \"monitor.key\"\n",
        fs::read_to_string(file).unwrap(),
        private.addr
    );
    fs::write(file, config).unwrap();
    Process::spawn("journal-init", file).wait_success().await;
    Process::spawn("init", file).wait_success().await;
    for cluster in [&mut peers, &mut public, &mut operators] {
        drop(cluster.members.get_mut(&1).unwrap().listener.take());
    }
    let voter = Process::spawn("run", file);
    let query = v1::GetStatusRequest {
        ledger_id: BOOK.0.to_string(),
    };
    let mut monitor = MonitorClient::new(status_channel.clone());
    let observed = timeout(WAIT, async {
        loop {
            if let Ok(state) = monitor.get_status(query.clone()).await {
                break state.into_inner();
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("voter monitor did not start");
    assert_eq!(observed.mode(), v1::ServingMode::ProtectedVoter);
    assert_eq!(observed.node_id, 1);
    assert!(observed.exports.is_none());
    assert_eq!(
        MonitorClient::new(payment_channel.clone())
            .get_status(query)
            .await
            .unwrap()
            .into_inner()
            .generation,
        observed.generation
    );

    // Set the path to the independently built ledger-api example to exercise
    // its argument parser, TLS identity, member check, output and exit status.
    if let Some(command) = std::env::var_os("LEDGER_MEMBER_STATUS_BIN") {
        let dir = peers.directory.path();
        fs::write(dir.join("operator.pem"), &admin.cert_pem).unwrap();
        fs::write(dir.join("operator.key"), &admin.key_pem).unwrap();
        let invoke = |id: &str| {
            ProcessCommand::new(&command)
                .current_dir(dir)
                .args([
                    &BOOK.0.to_string(),
                    id,
                    &address,
                    &server_name,
                    "operator-ca.pem",
                    "operator.pem",
                    "operator.key",
                ])
                .output()
                .unwrap()
        };
        let ok = invoke("1");
        assert!(
            ok.status.success(),
            "{}",
            String::from_utf8_lossy(&ok.stderr)
        );
        let sample = String::from_utf8(ok.stdout).unwrap();
        print!("{sample}");
        assert!(sample.contains("node=1") && sample.contains("mode=ProtectedVoter"));
        assert!(sample.contains("role=follower") && sample.contains("verified_journal_index="));
        let mismatch = invoke("2");
        assert!(!mismatch.status.success());
        let reason = String::from_utf8(mismatch.stderr).unwrap();
        assert!(reason.contains("member identity mismatch"), "{reason}");
        println!("wrong expected member exited with {}", mismatch.status);
    }

    // This invalid mutation reaches validation on the authenticated payment port.
    // The dedicated monitor does not register the ledger service at all.
    let mutation = v1::CreateAccountRequest::default();
    assert_eq!(
        LedgerClient::new(payment_channel)
            .create_account(mutation.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        LedgerClient::new(status_channel)
            .create_account(mutation.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    let denied = LedgerClient::new(unauthorized_payment)
        .create_account(mutation)
        .await
        .unwrap_err();
    assert!(
        matches!(
            denied.code(),
            tonic::Code::Unavailable | tonic::Code::Unknown
        ),
        "{denied:?}"
    );
    voter.stop().await;
}

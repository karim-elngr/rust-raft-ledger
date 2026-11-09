//! Query the specified member's mTLS monitor listener without leader routing.
use ledger_api::v1::{
    GetStatusRequest, GetStatusResponse, ServingMode, monitor_client::MonitorClient,
};
use std::{env, fmt::Display, fs, process::ExitCode, time::Duration};
use tokio::time::{Instant, timeout_at};
use tonic::{
    Request,
    transport::{Certificate, ClientTlsConfig, Endpoint, Identity},
};

const USAGE: &str = "member_status LEDGER_ID EXPECTED_NODE_ID HTTPS_ORIGIN TLS_NAME SERVER_CA_PEM OPERATOR_CERT_PEM OPERATOR_KEY_PEM";
const BUDGET: Duration = Duration::from_secs(5);

fn shown<T: Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".into(), |x| x.to_string())
}

fn report(state: &GetStatusResponse) -> bool {
    let role = if state.mode() == ServingMode::Learner {
        "learner"
    } else if state.is_leader {
        "leader"
    } else {
        "follower"
    };
    println!(
        "ledger={} generation={} node={} mode={:?} role={role} term={} leader={}",
        state.ledger_id,
        state.generation,
        state.node_id,
        state.mode(),
        state.term,
        shown(state.leader_id)
    );
    println!(
        "applied_index={} checkpoint_index={} verified_journal_index={}",
        state.applied_index,
        shown(state.checkpoint_index),
        shown(state.verified_journal_index)
    );
    for peer in &state.peers {
        let role = if peer.learner { "learner" } else { "voter" };
        println!(
            "peer={} role={role} matched_index={}",
            peer.node_id, peer.matched_index
        );
    }
    let exports = state.exports.as_ref();
    if let Some(x) = exports {
        println!(
            "saved_archive_index={} verified_archive_index={} archive_lag_entries={}",
            shown(x.saved_archive_index),
            shown(x.verified_archive_index),
            shown(x.archive_lag_entries)
        );
        println!(
            "event_index={} event_slot={} event_lag_entries={}",
            shown(x.event_index),
            shown(x.event_slot),
            shown(x.event_lag_entries)
        );
    }
    let mut healthy = true;
    for (name, error) in [
        ("protection", state.protection_error.as_ref()),
        ("checkpoint", state.checkpoint_error.as_ref()),
        (
            "snapshot_maintenance",
            state.snapshot_maintenance_error.as_ref(),
        ),
        ("archive", exports.and_then(|x| x.archive_error.as_ref())),
        ("events", exports.and_then(|x| x.event_error.as_ref())),
    ] {
        if let Some(error) = error {
            println!("{name}_error={error:?}");
            healthy = false;
        }
    }
    healthy
}

async fn run(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let [ledger, expected, origin, name, ca, cert, key] = args else {
        return Err(format!("usage: {USAGE}").into());
    };
    let expected: u64 = expected.parse()?;
    if expected == 0 {
        return Err("expected node ID must be nonzero".into());
    }
    let endpoint = Endpoint::from_shared(origin.clone())?;
    let uri = endpoint.uri();
    if uri.scheme_str() != Some("https")
        || uri.authority().is_none_or(|a| a.as_str().contains('@'))
        || !matches!(uri.path(), "" | "/")
        || uri.query().is_some()
        || name.trim().is_empty()
    {
        return Err("supply one HTTPS origin and its TLS server name".into());
    }
    let tls = ClientTlsConfig::new()
        .domain_name(name)
        .ca_certificate(Certificate::from_pem(fs::read(ca)?))
        .identity(Identity::from_pem(fs::read(cert)?, fs::read(key)?));
    let until = Instant::now() + BUDGET;
    let channel = timeout_at(until, endpoint.tls_config(tls)?.connect()).await??;
    let mut client =
        MonitorClient::new(channel).max_decoding_message_size(ledger_api::MAX_RPC_BYTES);
    let mut request = Request::new(GetStatusRequest {
        ledger_id: ledger.clone(),
    });
    request.set_timeout(until.saturating_duration_since(Instant::now()));
    let state = timeout_at(until, client.get_status(request))
        .await??
        .into_inner();
    if state.node_id != expected || &state.ledger_id != ledger {
        return Err(format!("member identity mismatch: expected ledger {ledger} node {expected}, received ledger {} node {}", state.ledger_id, state.node_id).into());
    }
    if !report(&state) {
        return Err("member reported an operational error".into());
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args == ["--help"] || args == ["-h"] {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("member_status: {error}");
            ExitCode::FAILURE
        }
    }
}

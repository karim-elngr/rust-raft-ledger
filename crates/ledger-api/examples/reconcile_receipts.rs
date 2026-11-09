//! Read-only comparison of external client receipts with quorum-confirmed decisions.
//! See docs/recovery-operations.md for the evidence format and invocation.
#[path = "reconcile_receipts/receipts.rs"]
mod receipts;

use ledger_api::{
    adapter::parse_id,
    client::{ClientSeed, LedgerClient},
    v1,
};
use std::{env, error::Error, fs, path::Path, process::ExitCode, time::Duration};
use tokio::time::Instant;

const USAGE: &str = "reconcile_receipts LEDGER_ID RECEIPT_ROOT SERVER_CA_PEM CLIENT_CERT_PEM CLIENT_KEY_PEM NODE_ID,HTTPS_ORIGIN,TLS_NAME ...";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<_> = env::args().skip(1).collect();
    if args == ["--help"] {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match reconcile(&args).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("cannot reconcile: {error}");
            ExitCode::from(2)
        }
    }
}

async fn reconcile(args: &[String]) -> Result<bool, Box<dyn Error>> {
    let [ledger, root, ca, cert, key, origins @ ..] = args else {
        return Err(USAGE.into());
    };
    parse_id(ledger)?;
    let dir = Path::new(root).join(ledger);
    let ids = receipts::inventory(&dir)
        .map_err(|error| format!("invalid evidence at {}: {error}", dir.display()))?;
    let mut seeds = Vec::new();
    for seed in origins {
        let fields: Vec<_> = seed.split(',').collect();
        let [node, endpoint, domain] = fields.as_slice() else {
            return Err("each seed requires NODE_ID,HTTPS_ORIGIN,TLS_NAME".into());
        };
        seeds.push(ClientSeed {
            node: node.parse()?,
            endpoint: (*endpoint).into(),
            domain: (*domain).into(),
        });
    }
    let client = LedgerClient::connect(seeds, fs::read(ca)?, fs::read(cert)?, fs::read(key)?)?;
    let expected_count = ids.len();
    let mut matched = 0;
    let mut missing_acceptances = 0usize;
    let mut missing_rejections = 0usize;
    let mut mismatches = 0usize;
    let mut rpc_errors = 0usize;
    let mut invalid_evidence = 0usize;
    for id in ids {
        let expected = match receipts::load(&dir, id) {
            Ok(receipt) => receipt,
            Err(error) => {
                invalid_evidence += 1;
                println!("ledger={ledger} request={id} invalid_evidence={error}");
                continue;
            }
        };
        let request = v1::GetRequestOutcomeRequest {
            ledger_id: ledger.clone(),
            request_id: id.to_string(),
        };
        match client
            .get_request_outcome(request, Instant::now() + Duration::from_secs(10))
            .await
        {
            Ok(reply) => match reply.decision {
                Some(actual) if actual == expected => matched += 1,
                Some(_) => {
                    mismatches += 1;
                    println!("ledger={ledger} request={id} decision_mismatch");
                }
                None => match expected.result {
                    Some(v1::decision::Result::Accepted(_)) => {
                        missing_acceptances += 1;
                        println!("ledger={ledger} request={id} missing_acceptance");
                    }
                    Some(v1::decision::Result::Rejected(_)) => {
                        missing_rejections += 1;
                        println!("ledger={ledger} request={id} missing_rejection");
                    }
                    None => unreachable!("receipt validated during load"),
                },
            },
            Err(status) => {
                rpc_errors += 1;
                println!("ledger={ledger} request={id} unverified_rpc_error={status}");
            }
        }
    }
    println!(
        "ledger={ledger} expected={expected_count} matched={matched} missing_acceptances={missing_acceptances} missing_rejections={missing_rejections} mismatches={mismatches} rpc_errors={rpc_errors} invalid_evidence={invalid_evidence}"
    );
    Ok(matched == expected_count)
}

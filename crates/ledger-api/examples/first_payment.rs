//! See docs/backend-integration.md for a runnable invocation.
#[path = "first_payment/workflow.rs"]
mod workflow;

use ledger_api::client::{ClientSeed, LedgerClient};
use std::{env, fs};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let [ledger, ca, cert, key, origins @ ..] = args.as_slice() else {
        return Err("usage: first_payment LEDGER_ID SERVER_CA_PEM CLIENT_CERT_PEM CLIENT_KEY_PEM NODE_ID,HTTPS_ORIGIN,TLS_NAME ...".into());
    };
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
    let entry = workflow::run(&client, ledger).await?;
    println!(
        "Payment request 5 posted 25 minor units, from account 2 to account 3. Original, retry and lookup all returned entry {}.",
        entry.entry_id
    );
    Ok(())
}

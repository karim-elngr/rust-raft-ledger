use ledger_api::{client::LedgerClient, v1};
use std::time::Duration;
use tokio::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(15)
}

fn acceptance(decision: v1::Decision) -> Result<v1::outcome::Kind> {
    match decision.result {
        Some(v1::decision::Result::Accepted(v1::Outcome { kind: Some(kind) })) => Ok(kind),
        other => Err(format!("unexpected decision: {other:?}").into()),
    }
}

fn transfer(
    ledger: &str,
    id: &str,
    debit: &str,
    credit: &str,
    amount: &str,
) -> v1::PostEntryRequest {
    let lines =
        [(debit, v1::Side::Debit), (credit, v1::Side::Credit)].map(|(account, side)| v1::Line {
            account_id: account.into(),
            side: side.into(),
            amount: amount.into(),
        });
    v1::PostEntryRequest {
        ledger_id: ledger.into(),
        request_id: id.into(),
        funding: Some(v1::Funding {
            kind: Some(v1::funding::Kind::Explicit(v1::ExplicitFunding {
                lines: lines.into(),
            })),
        }),
        ..Default::default()
    }
}

/// Run on a dedicated demonstration ledger. The fixed IDs make restarts replay
/// each decision even when the previous process exited immediately after send.
pub async fn run(client: &LedgerClient, ledger: &str) -> Result<v1::JournalEntry> {
    for (id, normal) in [
        ("1", v1::Side::Debit),
        ("2", v1::Side::Credit),
        ("3", v1::Side::Credit),
    ] {
        let request = v1::CreateAccountRequest {
            ledger_id: ledger.into(),
            request_id: id.into(),
            account_id: id.into(),
            code: 1,
            normal: normal.into(),
            bounds: Some(v1::Bounds {
                min: Some("0".into()),
                max: None,
            }),
        };
        acceptance(client.create_account(request, deadline()).await?)?;
    }
    let funding = transfer(ledger, "4", "1", "2", "100");
    acceptance(client.post_entry(funding, deadline()).await?)?;
    let payment = transfer(ledger, "5", "2", "3", "25");
    let original = client.post_entry(payment.clone(), deadline()).await?;
    let retry = client.post_entry(payment.clone(), deadline()).await?;
    let lookup = client
        .get_request_outcome(
            v1::GetRequestOutcomeRequest {
                ledger_id: ledger.into(),
                request_id: payment.request_id,
            },
            deadline(),
        )
        .await?;
    if retry != original || lookup.decision.as_ref() != Some(&original) {
        return Err("payment retry or lookup returned a different decision".into());
    }
    match acceptance(original)? {
        v1::outcome::Kind::Posted(entry) => Ok(entry),
        other => Err(format!("expected a posted payment, got {other:?}").into()),
    }
}

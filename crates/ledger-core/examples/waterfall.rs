use ledger_core::{
    AccountId, Balance, Bounds, Command, Error, Funding, Ledger, LedgerId, Line, Outcome,
    RequestId, Shortfall, Side, Source, SourceFloor, Waterfall,
};

fn main() -> Result<(), Error> {
    let book = LedgerId(1);
    let mut ledger = Ledger::new(book, "USD", 2);
    let [bank, rewards, cash, merchant] = [1, 2, 3, 4].map(|n| AccountId::new(book, n));
    for (account, normal) in [
        (bank, Side::Debit),
        (rewards, Side::Credit),
        (cash, Side::Credit),
        (merchant, Side::Credit),
    ] {
        ledger.apply(
            RequestId(account.number),
            Command::CreateAccount {
                account,
                normal,
                code: 1,
                bounds: Bounds {
                    min: Some(Balance::positive(0)),
                    max: None,
                },
            },
        )?;
    }

    ledger.apply(
        RequestId(5),
        Command::Post {
            funding: Funding::Explicit(vec![
                Line::debit(bank, 12_000),
                Line::credit(rewards, 3_000),
                Line::credit(cash, 9_000),
            ]),
            conditions: vec![],
            corrects: None,
        },
    )?;

    let purchase = Command::Post {
        funding: Funding::Waterfall(Waterfall {
            amount: 8_000,
            sources: [rewards, cash]
                .map(|account| Source {
                    account,
                    floor: SourceFloor::Configured,
                    cap: None,
                })
                .into(),
            destinations: vec![Line::credit(merchant, 8_000)],
            shortfall: Shortfall::RequireFull,
        }),
        conditions: vec![],
        corrects: None,
    };
    let receipt = ledger.apply(RequestId(6), purchase.clone())?;
    if let Outcome::Posted(entry) = &receipt {
        println!("Journal entry {} (USD cents)", entry.id.0);
        for line in entry.lines.iter() {
            println!(
                "  {:?} account {}: {}",
                line.side, line.account.number, line.amount
            );
        }
    }
    let retry = ledger.apply(RequestId(6), purchase)?;
    assert_eq!(receipt, retry);
    println!(
        "Retry returned the original receipt. Journal entries: {}",
        ledger.journal().len()
    );
    Ok(())
}

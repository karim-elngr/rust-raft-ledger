//! V1 Raft payloads use fixed-width little-endian integers, byte tags and u16 list
//! counts. Variant tags and field order below are part of the durable format.
//! Domain validation belongs to apply so recorded rejections replay identically.
use crate::ReplicaError;
use ledger_core::{
    AccountId, Allocation, Balance, Bounds, CaptureAmount, Command, Condition, EntryId, Funding,
    LedgerId, Line, Remainder, RequestId, ReservationId, Shortfall, Side, Source, SourceFloor,
    Waterfall,
};

pub(crate) const MAX_BATCH_BYTES: usize = 1 << 20;
pub(crate) const MAX_COMMANDS: usize = 256;
const MAX_ITEMS: usize = 1024;
type Result<T> = std::result::Result<T, ReplicaError>;

const MAGIC_V1: &[u8; 5] = b"RLCB\x01";
pub(crate) const HEADER_BYTES: usize = MAGIC_V1.len() + size_of::<u128>() + size_of::<u16>();

fn invalid(message: &str) -> ReplicaError {
    ReplicaError::Invalid(message.to_owned())
}

pub(crate) fn batch_header(ledger: LedgerId, count: u16) -> [u8; HEADER_BYTES] {
    let mut header = [0; HEADER_BYTES];
    let count_offset = HEADER_BYTES - size_of::<u16>();
    header[..MAGIC_V1.len()].copy_from_slice(MAGIC_V1);
    header[MAGIC_V1.len()..count_offset].copy_from_slice(&ledger.0.to_le_bytes());
    header[count_offset..].copy_from_slice(&count.to_le_bytes());
    header
}

pub(crate) fn encode_request(id: RequestId, command: &Command) -> Result<Vec<u8>> {
    let mut writer = Writer(Vec::new());
    writer.request(id, command)?;
    if writer.0.len() > MAX_BATCH_BYTES - HEADER_BYTES {
        return Err(ReplicaError::SizeLimit(
            "command exceeds single-entry byte limit",
        ));
    }
    Ok(writer.0)
}

pub(crate) fn encode_batch(ledger: LedgerId, commands: &[(RequestId, Command)]) -> Result<Vec<u8>> {
    if commands.is_empty() || commands.len() > MAX_COMMANDS {
        return Err(invalid("invalid batch command count"));
    }
    let mut w = Writer(batch_header(ledger, commands.len() as u16).to_vec());
    for (id, command) in commands {
        w.request(*id, command)?;
    }
    Ok(w.0)
}

pub(crate) fn decode_batch(expected: LedgerId, bytes: &[u8]) -> Result<Vec<(RequestId, Command)>> {
    if bytes.len() > MAX_BATCH_BYTES {
        return Err(invalid("batch exceeds byte limit"));
    }
    let mut r = Reader(bytes);
    if &r.take::<5>()? != MAGIC_V1 {
        return Err(invalid("unknown batch format"));
    }
    if LedgerId(r.amount()?) != expected {
        return Err(invalid("wrong batch ledger"));
    }
    let count = usize::from(u16::from_le_bytes(r.take()?));
    if count == 0 || count > MAX_COMMANDS {
        return Err(invalid("invalid batch command count"));
    }
    let mut commands = Vec::new();
    for _ in 0..count {
        commands.push((RequestId(r.amount()?), r.command()?));
    }
    if !r.0.is_empty() {
        return Err(invalid("trailing batch bytes"));
    }
    Ok(commands)
}

struct Writer(Vec<u8>);

impl Writer {
    fn request(&mut self, id: RequestId, command: &Command) -> Result<()> {
        self.amount(id.0)?;
        self.command(command)
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_BATCH_BYTES - self.0.len() {
            return Err(ReplicaError::SizeLimit("batch exceeds byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    fn tag(&mut self, tag: u8) -> Result<()> {
        self.put(&[tag])
    }

    fn amount(&mut self, n: u128) -> Result<()> {
        self.put(&n.to_le_bytes())
    }

    fn items<T>(
        &mut self,
        items: &[T],
        mut write: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        if items.len() > MAX_ITEMS {
            return Err(ReplicaError::SizeLimit("collection exceeds item limit"));
        }
        self.put(&(items.len() as u16).to_le_bytes())?;
        for item in items {
            write(self, item)?;
        }
        Ok(())
    }

    fn optional<T>(
        &mut self,
        value: Option<T>,
        write: impl FnOnce(&mut Self, T) -> Result<()>,
    ) -> Result<()> {
        self.tag(u8::from(value.is_some()))?;
        if let Some(value) = value {
            write(self, value)?;
        }
        Ok(())
    }

    fn balance(&mut self, balance: Balance) -> Result<()> {
        self.tag(u8::from(balance.is_negative()))?;
        self.amount(balance.magnitude())
    }

    fn bounds(&mut self, bounds: Bounds) -> Result<()> {
        self.optional(bounds.min, Self::balance)?;
        self.optional(bounds.max, Self::balance)
    }

    fn account(&mut self, account: AccountId) -> Result<()> {
        self.amount(account.ledger.0)?;
        self.amount(account.number)
    }

    fn side(&mut self, side: Side) -> Result<()> {
        self.tag(match side {
            Side::Debit => 0,
            Side::Credit => 1,
        })
    }

    fn line(&mut self, line: &Line) -> Result<()> {
        self.account(line.account)?;
        self.side(line.side)?;
        self.amount(line.amount)
    }

    fn source(&mut self, source: &Source) -> Result<()> {
        self.account(source.account)?;
        match source.floor {
            SourceFloor::Configured => self.tag(0)?,
            SourceFloor::At(balance) => {
                self.tag(1)?;
                self.balance(balance)?;
            }
        }
        self.optional(source.cap, Self::amount)
    }

    fn funding(&mut self, funding: &Funding) -> Result<()> {
        match funding {
            Funding::Explicit(lines) => {
                self.tag(0)?;
                self.items(lines, Self::line)
            }
            Funding::Waterfall(waterfall) => {
                self.tag(1)?;
                self.amount(waterfall.amount)?;
                self.items(&waterfall.sources, Self::source)?;
                self.items(&waterfall.destinations, Self::line)?;
                self.tag(match waterfall.shortfall {
                    Shortfall::RequireFull => 0,
                })
            }
        }
    }

    fn condition(&mut self, condition: &Condition) -> Result<()> {
        match *condition {
            Condition::AvailableAtLeast { account, amount } => {
                self.tag(0)?;
                self.account(account)?;
                self.balance(amount)
            }
        }
    }

    fn capture(&mut self, capture: &CaptureAmount) -> Result<()> {
        match capture {
            CaptureAmount::Full => self.tag(0),
            CaptureAmount::Amount(n) => {
                self.tag(1)?;
                self.amount(*n)
            }
            CaptureAmount::Lines(allocations) => {
                self.tag(2)?;
                self.items(allocations, |w, allocation| {
                    w.put(&allocation.line.to_le_bytes())?;
                    w.amount(allocation.amount)
                })
            }
        }
    }

    fn command(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::CreateAccount {
                account,
                code,
                normal,
                bounds,
            } => {
                self.tag(0)?;
                self.account(*account)?;
                self.put(&code.to_le_bytes())?;
                self.side(*normal)?;
                self.bounds(*bounds)
            }
            Command::SetAccountBounds { account, bounds } => {
                self.tag(1)?;
                self.account(*account)?;
                self.bounds(*bounds)
            }
            Command::Post {
                funding,
                conditions,
                corrects,
            } => {
                self.tag(2)?;
                self.funding(funding)?;
                self.items(conditions, Self::condition)?;
                self.optional(*corrects, |w, id| w.put(&id.0.to_le_bytes()))
            }
            Command::Reserve {
                reservation,
                funding,
                conditions,
            } => {
                self.tag(3)?;
                self.amount(reservation.0)?;
                self.funding(funding)?;
                self.items(conditions, Self::condition)
            }
            Command::Capture {
                reservation,
                amount,
                remainder,
            } => {
                self.tag(4)?;
                self.amount(reservation.0)?;
                self.capture(amount)?;
                self.optional(*remainder, |w, r| {
                    w.tag(match r {
                        Remainder::KeepReserved => 0,
                        Remainder::Release => 1,
                    })
                })
            }
            Command::Void { reservation } => {
                self.tag(5)?;
                self.amount(reservation.0)
            }
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (bytes, tail) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| invalid("truncated batch"))?;
        self.0 = tail;
        Ok(*bytes)
    }

    fn tag(&mut self) -> Result<u8> {
        Ok(self.take::<1>()?[0])
    }

    fn amount(&mut self) -> Result<u128> {
        Ok(u128::from_le_bytes(self.take()?))
    }

    fn items<T>(&mut self, mut read: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let count = usize::from(u16::from_le_bytes(self.take()?));
        if count > MAX_ITEMS {
            return Err(invalid("collection exceeds item limit"));
        }
        // Only allocate when an item has been completely decoded.
        let mut items = Vec::new();
        for _ in 0..count {
            items.push(read(self)?);
        }
        Ok(items)
    }

    fn optional<T>(&mut self, read: impl FnOnce(&mut Self) -> Result<T>) -> Result<Option<T>> {
        match self.tag()? {
            0 => Ok(None),
            1 => Ok(Some(read(self)?)),
            _ => Err(invalid("invalid option tag")),
        }
    }

    fn balance(&mut self) -> Result<Balance> {
        let sign = self.tag()?;
        let magnitude = self.amount()?;
        match sign {
            0 => Ok(Balance::positive(magnitude)),
            1 if magnitude != 0 => Ok(Balance::negative(magnitude)),
            _ => Err(invalid("invalid signed magnitude")),
        }
    }

    fn bounds(&mut self) -> Result<Bounds> {
        Ok(Bounds {
            min: self.optional(Self::balance)?,
            max: self.optional(Self::balance)?,
        })
    }

    fn account(&mut self) -> Result<AccountId> {
        Ok(AccountId::new(LedgerId(self.amount()?), self.amount()?))
    }

    fn side(&mut self) -> Result<Side> {
        match self.tag()? {
            0 => Ok(Side::Debit),
            1 => Ok(Side::Credit),
            _ => Err(invalid("unknown posting side")),
        }
    }

    fn line(&mut self) -> Result<Line> {
        Ok(Line {
            account: self.account()?,
            side: self.side()?,
            amount: self.amount()?,
        })
    }

    fn source(&mut self) -> Result<Source> {
        let account = self.account()?;
        let floor = match self.tag()? {
            0 => SourceFloor::Configured,
            1 => SourceFloor::At(self.balance()?),
            _ => return Err(invalid("unknown source floor")),
        };
        Ok(Source {
            account,
            floor,
            cap: self.optional(Self::amount)?,
        })
    }

    fn funding(&mut self) -> Result<Funding> {
        match self.tag()? {
            0 => Ok(Funding::Explicit(self.items(Self::line)?)),
            1 => {
                let amount = self.amount()?;
                let sources = self.items(Self::source)?;
                let destinations = self.items(Self::line)?;
                let shortfall = match self.tag()? {
                    0 => Shortfall::RequireFull,
                    _ => return Err(invalid("unknown shortfall policy")),
                };
                Ok(Funding::Waterfall(Waterfall {
                    amount,
                    sources,
                    destinations,
                    shortfall,
                }))
            }
            _ => Err(invalid("unknown funding kind")),
        }
    }

    fn condition(&mut self) -> Result<Condition> {
        match self.tag()? {
            0 => Ok(Condition::AvailableAtLeast {
                account: self.account()?,
                amount: self.balance()?,
            }),
            _ => Err(invalid("unknown condition")),
        }
    }

    fn capture(&mut self) -> Result<CaptureAmount> {
        match self.tag()? {
            0 => Ok(CaptureAmount::Full),
            1 => Ok(CaptureAmount::Amount(self.amount()?)),
            2 => Ok(CaptureAmount::Lines(self.items(|r| {
                Ok(Allocation {
                    line: u32::from_le_bytes(r.take()?),
                    amount: r.amount()?,
                })
            })?)),
            _ => Err(invalid("unknown capture kind")),
        }
    }

    fn command(&mut self) -> Result<Command> {
        match self.tag()? {
            0 => Ok(Command::CreateAccount {
                account: self.account()?,
                code: u16::from_le_bytes(self.take()?),
                normal: self.side()?,
                bounds: self.bounds()?,
            }),
            1 => Ok(Command::SetAccountBounds {
                account: self.account()?,
                bounds: self.bounds()?,
            }),
            2 => Ok(Command::Post {
                funding: self.funding()?,
                conditions: self.items(Self::condition)?,
                corrects: self.optional(|r| Ok(EntryId(u64::from_le_bytes(r.take()?))))?,
            }),
            3 => Ok(Command::Reserve {
                reservation: ReservationId(self.amount()?),
                funding: self.funding()?,
                conditions: self.items(Self::condition)?,
            }),
            4 => Ok(Command::Capture {
                reservation: ReservationId(self.amount()?),
                amount: self.capture()?,
                remainder: self.optional(|r| match r.tag()? {
                    0 => Ok(Remainder::KeepReserved),
                    1 => Ok(Remainder::Release),
                    _ => Err(invalid("unknown remainder disposition")),
                })?,
            }),
            5 => Ok(Command::Void {
                reservation: ReservationId(self.amount()?),
            }),
            _ => Err(invalid("unknown command kind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::Ledger;

    const BOOK: LedgerId = LedgerId(1);

    fn void() -> (RequestId, Command) {
        (
            RequestId(2),
            Command::Void {
                reservation: ReservationId(3),
            },
        )
    }

    #[test]
    fn void_matches_the_v1_golden_bytes() {
        let golden = [
            b'R', b'L', b'C', b'B', 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0,
        ];
        assert_eq!(encode_batch(BOOK, &[void()]).unwrap(), golden);
        assert_eq!(decode_batch(BOOK, &golden).unwrap(), [void()]);
        for end in 0..golden.len() {
            assert!(decode_batch(BOOK, &golden[..end]).is_err());
        }
        assert!(decode_batch(LedgerId(9), &golden).is_err());
        let mut trailing = golden.to_vec();
        trailing.push(0);
        assert!(decode_batch(BOOK, &trailing).is_err());
        for byte in [0, 4, 39] {
            let mut invalid = golden;
            invalid[byte] = 0xff;
            assert!(decode_batch(BOOK, &invalid).is_err());
        }
    }

    #[test]
    fn preserves_complete_ordered_payloads_and_replay() {
        let wallet = AccountId::new(BOOK, u128::MAX);
        let merchant = AccountId::new(BOOK, 8);
        let bounds = Bounds {
            min: Some(Balance::negative(u128::MAX)),
            max: Some(Balance::positive(u128::MAX)),
        };
        let explicit = Funding::Explicit(vec![Line::debit(wallet, 5), Line::credit(merchant, 5)]);
        let waterfall = Funding::Waterfall(Waterfall {
            amount: 5,
            sources: vec![
                Source {
                    account: wallet,
                    floor: SourceFloor::At(Balance::negative(5)),
                    cap: Some(4),
                },
                Source {
                    account: AccountId::new(LedgerId(9), 1),
                    floor: SourceFloor::Configured,
                    cap: None,
                },
            ],
            destinations: vec![Line::credit(merchant, 5)],
            shortfall: Shortfall::RequireFull,
        });
        let hold = ReservationId(u128::MAX);
        let commands = vec![
            Command::CreateAccount {
                account: wallet,
                code: u16::MAX,
                normal: Side::Credit,
                bounds,
            },
            Command::CreateAccount {
                account: merchant,
                code: 0,
                normal: Side::Debit,
                bounds: Bounds::default(),
            },
            Command::SetAccountBounds {
                account: merchant,
                bounds,
            },
            Command::Post {
                funding: explicit.clone(),
                conditions: vec![],
                corrects: None,
            },
            Command::Reserve {
                reservation: hold,
                funding: explicit,
                conditions: vec![Condition::AvailableAtLeast {
                    account: wallet,
                    amount: Balance::negative(u128::MAX),
                }],
            },
            Command::Capture {
                reservation: hold,
                amount: CaptureAmount::Lines(vec![
                    Allocation { line: 1, amount: 1 },
                    Allocation { line: 0, amount: 1 },
                ]),
                remainder: Some(Remainder::KeepReserved),
            },
            Command::Capture {
                reservation: hold,
                amount: CaptureAmount::Amount(1),
                remainder: Some(Remainder::Release),
            },
            Command::Capture {
                reservation: hold,
                amount: CaptureAmount::Full,
                remainder: None,
            },
            Command::Post {
                funding: waterfall,
                conditions: vec![],
                corrects: Some(EntryId(u64::MAX)),
            },
            Command::Void { reservation: hold },
        ];
        let requests: Vec<_> = commands
            .into_iter()
            .enumerate()
            .map(|(i, cmd)| (RequestId(i as u128), cmd))
            .collect();
        let bytes = encode_batch(BOOK, &requests).unwrap();
        let decoded = decode_batch(BOOK, &bytes).unwrap();
        assert_eq!(decoded, requests);
        assert_eq!(encode_batch(BOOK, &decoded).unwrap(), bytes);
        let mut live = Ledger::new(BOOK, "USD", 2);
        let mut recovered = Ledger::new(BOOK, "USD", 2);
        for ((id, original), (_, replay)) in requests.into_iter().zip(decoded) {
            assert_eq!(live.apply(id, original), recovered.apply(id, replay));
        }
        assert_eq!(live, recovered);
    }

    #[test]
    fn rejects_noncanonical_negative_zero_and_invalid_option_tags() {
        let cmd = Command::SetAccountBounds {
            account: AccountId::new(BOOK, 2),
            bounds: Bounds {
                min: Some(Balance::default()),
                max: None,
            },
        };
        let mut bytes = encode_batch(BOOK, &[(RequestId(0), cmd)]).unwrap();
        // Last 18 bytes are minimum's sign and magnitude, then the absent maximum.
        let sign = bytes.len() - 18;
        bytes[sign] = 1;
        assert!(decode_batch(BOOK, &bytes).is_err());
        bytes[sign] = 0;
        *bytes.last_mut().unwrap() = 2;
        assert!(decode_batch(BOOK, &bytes).is_err());
    }

    #[test]
    fn bounds_encoded_bytes_and_all_collections() {
        assert!(encode_batch(BOOK, &[]).is_err());
        assert!(encode_batch(BOOK, &vec![void(); MAX_COMMANDS + 1]).is_err());
        assert!(decode_batch(BOOK, &vec![0; MAX_BATCH_BYTES + 1]).is_err());
        let mut header = encode_batch(BOOK, &[void()]).unwrap();
        header[21..23].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(decode_batch(BOOK, &header).is_err());
        let post = |len| {
            (
                RequestId(7),
                Command::Post {
                    funding: Funding::Explicit(vec![Line::debit(AccountId::new(BOOK, 8), 1); len]),
                    conditions: vec![],
                    corrects: None,
                },
            )
        };
        assert!(encode_batch(BOOK, &[post(MAX_ITEMS + 1)]).is_err());
        let mut forged = encode_batch(BOOK, &[post(1)]).unwrap();
        forged[41..43].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(decode_batch(BOOK, &forged).is_err());
        assert!(encode_batch(BOOK, &vec![post(MAX_ITEMS); 32]).is_err());
    }
}

use crate::{Journal, JournalError, Spec};
use tempfile::tempdir;

#[test]
fn encoded_entries_survive_overlap_and_partial_segment_seek_without_reencoding() {
    let dir = tempdir().unwrap();
    let j = Journal::init(
        dir.path(),
        &Spec {
            ledger: 1,
            asset: "A".into(),
            scale: 0,
        },
        2,
    )
    .unwrap();
    // The legal protobuf serializes index before term, opposite the generated encoder.
    let row = |index: u8| vec![0x18, index, 0x10, 1];
    let first = j
        .append_encoded(&j.head().unwrap(), &[&row(1), &row(2)])
        .unwrap();
    let tip = j.append_encoded(&first, &[&row(3), &row(4)]).unwrap();
    let mut cursor = j.encoded_cursor(&tip, 2).unwrap();
    for i in 2..=4 {
        assert_eq!(cursor.next_entry().unwrap().unwrap(), row(i));
    }
    assert!(cursor.next_entry().unwrap().is_none());
    assert!(matches!(
        j.append_encoded(&first, &[&row(3)]),
        Err(JournalError::Conflict)
    ));
    assert!(j.append_encoded(&tip, &[&row(6)]).is_err());
    assert_eq!(j.head().unwrap(), tip);
}

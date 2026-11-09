//! Bounded sequence decoding for rows derived from the V1 Raft command format.
use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};
use std::{fmt, marker::PhantomData, sync::Arc};

const INPUT_ITEMS: usize = 1024;
// A resolved waterfall can contain every source and every destination.
const RESOLVED_LINES: usize = 2 * INPUT_ITEMS;

fn bounded<'de, D, T, const MAX: usize>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Sequence<T, const N: usize>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for Sequence<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(out, "up to {N} items")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let hint = seq.size_hint().unwrap_or(0);
            if hint > N {
                return Err(de::Error::custom("stored sequence exceeds item limit"));
            }
            let mut items = Vec::with_capacity(hint);
            while let Some(item) = seq.next_element()? {
                if items.len() == N {
                    return Err(de::Error::custom("stored sequence exceeds item limit"));
                }
                items.push(item);
            }
            Ok(items)
        }
    }
    d.deserialize_seq(Sequence::<T, MAX>(PhantomData))
}

pub(crate) fn inputs<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    bounded::<D, T, INPUT_ITEMS>(d)
}

pub(crate) fn amounts<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u128>, D::Error> {
    bounded::<D, u128, RESOLVED_LINES>(d)
}

pub(crate) fn lines<'de, D: Deserializer<'de>>(d: D) -> Result<Arc<[crate::Line]>, D::Error> {
    Ok(bounded::<D, crate::Line, RESOLVED_LINES>(d)?.into())
}

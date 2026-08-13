//! A hash map whose layout does not depend on the process it is running in.
//!
//! T5a is the reason this file exists. The Python solver's answer turned out to
//! depend on CPython's hash-table layout: several searches iterate a `set`, and
//! that order reaches the plan because it decides which application is appended
//! to the frontier first and the selection rules break ties by index. Salting
//! `Node.__hash__` moved four of the eight corpus fingerprints.
//!
//! Rust's default `HashMap` has the same hazard and hides it better: `RandomState`
//! is seeded per process, so a map iterated anywhere would give a different
//! answer on every *run* rather than merely on every interpreter. The port's
//! rule is that no iteration order is ever taken from a hash map -- ranks and
//! declaration order are the only orders -- but a rule that is only in a comment
//! is one refactor from being broken. A fixed seed makes the failure
//! reproducible instead of intermittent, which is the difference between a bug
//! that is found and a bug that is filed as flaky.
//!
//! The hash itself is the FxHash mixing step: one multiply and one rotate per
//! word. It is not a strong hash and does not need to be -- these keys are
//! arena indices and interned bit patterns, not adversarial input -- and it is
//! several times cheaper than SipHash, which matters on maps rebuilt once per
//! expanded node.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

#[derive(Default, Clone, Copy)]
pub struct FxHasher {
    hash: u64,
}

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().unwrap()));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(buf));
        }
    }
    #[inline]
    fn write_u32(&mut self, v: u32) { self.add(v as u64); }
    #[inline]
    fn write_u64(&mut self, v: u64) { self.add(v); }
    #[inline]
    fn write_usize(&mut self, v: usize) { self.add(v as u64); }
    #[inline]
    fn finish(&self) -> u64 { self.hash }
}

pub type DetHasher = BuildHasherDefault<FxHasher>;
pub type Map<K, V> = HashMap<K, V, DetHasher>;
pub type Set<K> = HashSet<K, DetHasher>;

pub fn map<K, V>() -> Map<K, V> { Map::default() }
pub fn set<K>() -> Set<K> { Set::default() }

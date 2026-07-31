//! The Rust half of the random-decision contract.
//!
//! `src/metasmith/models/solver_rng.py` is the other half, and the two are one
//! specification written twice. Everything here is a transcription of that
//! file, deliberately literal: same rules, same order of operations, same
//! shortcuts. Where a line looks like it could be simplified, the simplification
//! is what would make the two streams diverge.
//!
//! The stream is ChaCha8 in its reference form -- key = the seed as eight
//! little-endian bytes followed by 24 zero bytes, 96-bit zero nonce, block
//! counter from 0, the sixteen words of each block consumed in order.
//! `ChaCha8Rng::from_seed` accepts exactly that 32-byte key.
//! `ChaCha8Rng::seed_from_u64` does **not** -- it runs the seed through PCG
//! first -- and must never appear in this file.

use rand_chacha::ChaCha8Rng;
// Through `rand_chacha`'s own re-export, never a separate `rand_core` entry in
// Cargo.toml: two paths to the trait means two resolvable versions of it, and
// the stream this contract rests on is the one *this* crate implements.
use rand_chacha::rand_core::{Rng, SeedableRng};

/// Bump in lockstep with `SOLVER_RNG_VERSION` in `solver_rng.py`. A plan solved
/// under one version is not reproducible under another; the two sides exchange
/// this so a desync is loud rather than a quiet divergence in results.
pub const SOLVER_RNG_VERSION: u32 = 1;

const TWO_32: u64 = 1 << 32;

// A decision trace, for localising a differential failure to the draw it
// happened on. Off unless `MSM_SOLVER_TRACE` is set, and it writes to stderr so
// it can never be mistaken for the reply.
thread_local! {
    static TRACE: std::cell::Cell<bool> =
        std::cell::Cell::new(std::env::var_os("MSM_SOLVER_TRACE").is_some());
}

/// The seed-to-key rule. Both implementations must use this one.
pub fn seed_to_key(seed: u64) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&seed.to_le_bytes());
    key
}

// A NaN would make the orderings below non-total, and a non-total order is a
// divergence waiting to happen rather than a crash. The rule is that NaN is
// always *worst*, which means it sorts in opposite directions depending on which
// way "best" points -- one shared mapping quietly makes NaN the winner of every
// `argmin_index`.
fn rank_high(score: f64) -> f64 {
    if score.is_nan() { f64::NEG_INFINITY } else { score }
}

/// The k highest-scoring indices, best first, ties broken by lower index.
pub fn top_k_indices(scores: &[f64], k: usize) -> Vec<usize> {
    let k = k.min(scores.len());
    if k == 0 { return Vec::new(); }
    // The refiner's frontier reaches tens of thousands of states and is
    // re-ranked on every iteration, so the general sort is worth skipping when
    // k is 1 -- which is what both call sites actually ask for. Same rule, one
    // pass instead of n log n.
    if k == 1 { return vec![argmax_index(scores)]; }
    let mut idx: Vec<usize> = (0..scores.len()).collect();
    // `partial_cmp`, deliberately, not `total_cmp`. Two reasons, and only the
    // first is obvious. `-rank_high(..)` is never NaN -- NEG_INFINITY negates to
    // INFINITY, so a NaN score sorts last -- which makes the unwrap safe. But
    // `total_cmp` also orders `-0.0` *before* `0.0`, and Python's sort on the
    // `(-rank, i)` tuple calls them equal and falls through to the index. A
    // score of `0.0` and a score of `-0.0` in one list is all it would take to
    // hand the two implementations different plans.
    idx.sort_by(|&a, &b| {
        (-rank_high(scores[a]))
            .partial_cmp(&-rank_high(scores[b]))
            .expect("rank_high removes every NaN")
            .then(a.cmp(&b))
    });
    idx.truncate(k);
    idx
}

/// Index of the maximum; the *first* one on a tie.
pub fn argmax_index(scores: &[f64]) -> usize {
    let (mut best, mut best_v) = (0usize, rank_high(scores[0]));
    for i in 1..scores.len() {
        let v = scores[i];
        if v.is_nan() { continue; } // NaN is never a maximum
        if v > best_v { best = i; best_v = v; }
    }
    best
}

/// Index of the minimum; the *first* one on a tie.
pub fn argmin_index(values: &[f64]) -> usize {
    let (mut best, mut best_v) = (
        0usize,
        if values[0].is_nan() { f64::INFINITY } else { values[0] },
    );
    for i in 1..values.len() {
        let v = values[i];
        if v.is_nan() { continue; } // nor a minimum
        if v < best_v { best = i; best_v = v; }
    }
    best
}

/// Every random decision the solver makes, and no way to make another.
pub struct DecisionStream {
    words: ChaCha8Rng,
    /// Consumed words; a cheap desync tell in differential tests.
    pub draws: u64,
}

impl DecisionStream {
    pub fn new(seed: u64) -> Self {
        Self { words: ChaCha8Rng::from_seed(seed_to_key(seed)), draws: 0 }
    }

    /// Raw stream words. Diagnostic only -- it exists so a differential failure
    /// can be attributed to the stream rather than to a decision rule, and no
    /// decision may be built on it.
    pub fn raw_words(&mut self, n: usize) -> Vec<u32> {
        (0..n)
            .map(|_| {
                self.draws += 1;
                self.words.next_u32()
            })
            .collect()
    }

    /// Uniform in `[0, n)`, by rejection -- and *no draw at all* when n <= 1.
    ///
    /// Rejection rather than a plain modulo because the bias of a modulo is a
    /// behaviour the other side would have to reproduce bit-for-bit instead of
    /// implementing the stated rule. The n <= 1 shortcut is part of the
    /// contract, not an optimisation: it fixes how many words a degenerate
    /// choice consumes, and a stream that drifts by one word diverges completely
    /// from there on.
    pub fn bounded_int(&mut self, n: u64) -> u64 {
        let out = self.bounded_int_inner(n);
        if TRACE.with(|t| t.get()) {
            eprintln!("bounded_int n={n} -> {out} draws={}", self.draws);
        }
        out
    }

    fn bounded_int_inner(&mut self, n: u64) -> u64 {
        if n <= 1 { return 0; }
        // Python computes `2**32 - (2**32 % n)` in unbounded integers. For
        // n > 2**32 that is 0 and the loop never terminates on either side, so
        // the bound is a precondition of the contract rather than a Rust-only
        // limit.
        assert!(n <= TWO_32, "bounded_int is defined for n <= 2^32, got {n}");
        let limit = TWO_32 - (TWO_32 % n); // largest multiple of n that fits
        loop {
            let v = self.words.next_u32() as u64;
            self.draws += 1;
            if v < limit { return v % n; }
        }
    }

    /// Pick an index in proportion to *integer* weights.
    pub fn weighted_index(&mut self, weights: &[i64]) -> usize {
        let out = self.weighted_index_inner(weights);
        if TRACE.with(|t| t.get()) {
            eprintln!("weighted_index -> {out} draws={}", self.draws);
        }
        out
    }

    fn weighted_index_inner(&mut self, weights: &[i64]) -> usize {
        assert!(!weights.is_empty(), "cannot choose from no options");
        if weights.len() == 1 { return 0; } // degenerate choices consume nothing
        let total: i64 = weights.iter().sum();
        assert!(total > 0, "weights must not sum to zero");
        let r = self.bounded_int(total as u64) as i64;
        let mut acc: i64 = 0;
        for (i, w) in weights.iter().enumerate() {
            acc += w;
            if r < acc { return i; }
        }
        weights.len() - 1 // unreachable while weights are non-negative
    }

    /// Uniformly among the k highest-scoring indices.
    ///
    /// One decision, not two, so that "which are the best k" and "which of them"
    /// cannot be answered by different rules on the two sides.
    pub fn pick_top_k(&mut self, scores: &[f64], k: usize) -> usize {
        let out = self.pick_top_k_inner(scores, k);
        if TRACE.with(|t| t.get()) {
            eprintln!("pick_top_k n={} -> {out} draws={}", scores.len(), self.draws);
        }
        out
    }

    fn pick_top_k_inner(&mut self, scores: &[f64], k: usize) -> usize {
        let candidates = top_k_indices(scores, k);
        assert!(!candidates.is_empty(), "cannot pick from an empty frontier");
        let i = self.bounded_int(candidates.len() as u64) as usize;
        candidates[i]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The eight words the Python side pins for seed 42. If this test fails,
    /// `rand_chacha` changed its stream and the port's whole premise is gone --
    /// which is the failure this pin exists to make loud.
    #[test]
    fn the_stream_matches_the_python_pin() {
        let mut s = DecisionStream::new(42);
        assert_eq!(
            s.raw_words(8),
            vec![
                0x198fa887, 0x59273471, 0x169df72b, 0x49238aa4,
                0x64fc90f6, 0x7e54361f, 0x96cef6e2, 0x5e2e306a,
            ]
        );
    }

    #[test]
    fn a_degenerate_choice_consumes_nothing() {
        let mut s = DecisionStream::new(42);
        assert_eq!(s.bounded_int(1), 0);
        assert_eq!(s.bounded_int(0), 0);
        assert_eq!(s.weighted_index(&[7]), 0);
        assert_eq!(s.draws, 0);
    }

    #[test]
    fn nan_is_worst_in_both_directions() {
        let v = [f64::NAN, 1.0, f64::NAN, 3.0, 3.0];
        assert_eq!(argmax_index(&v), 3); // first of the tied maxima
        assert_eq!(argmin_index(&v), 1);
        assert_eq!(top_k_indices(&v, 5), vec![3, 4, 1, 0, 2]);
    }

    /// `-0.0` and `0.0` are equal, and the tie goes to the lower index. This is
    /// the one place a `total_cmp` would silently disagree with the Python side.
    #[test]
    fn negative_zero_ties_with_zero() {
        assert_eq!(top_k_indices(&[0.0, -0.0, 0.0], 3), vec![0, 1, 2]);
        assert_eq!(top_k_indices(&[-0.0, 0.0, -0.0], 3), vec![0, 1, 2]);
        assert_eq!(argmax_index(&[-0.0, 0.0]), 0);
        assert_eq!(argmin_index(&[0.0, -0.0]), 0);
    }

    #[test]
    fn argmax_of_all_nan_is_the_first_index() {
        assert_eq!(argmax_index(&[f64::NAN, f64::NAN]), 0);
        assert_eq!(argmin_index(&[f64::NAN, f64::NAN]), 0);
    }
}

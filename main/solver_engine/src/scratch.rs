//! Dense, reusable scratch space for the refiner's per-state score.
//!
//! `Refiner::score` runs once per expanded state -- 174,804 times in sixteen
//! refiner iterations of `sink-24`, 81,486 times in *one* iteration of
//! `sink-178` -- and every lookup table it needs is built from nothing at the
//! start of the call and dropped at the end of it. A callgrind profile of the
//! first of those put roughly half the engine's instructions inside
//! `malloc`/`free` and `hashbrown`'s rehash, and almost none of them in the
//! graph work those tables exist to do.
//!
//! Two facts make that avoidable, and the second is the one that makes it safe.
//! `EpSig` is an index interned densely from zero, and the whole universe of
//! them is 221 signatures on `sink-24` and 284 on `sink-178` -- small enough
//! that a flat array indexed by signature is cheaper than a hash of it. And not
//! one of these tables is ever *iterated*: they are insert-and-look-up only. A
//! hash map that is never iterated cannot leak its layout into the plan, and
//! neither can the array that replaces it, so this is invisible to the
//! iteration-order contract in `tests/solver/test_iteration_order.py`.
//!
//! Emptying is the part that has to stay O(1) -- clearing an array sized to the
//! whole signature universe once per state would give back what the map cost.
//! Each table carries a generation counter and each slot a stamp, and a slot
//! holds a value only while its stamp is the current generation, so `clear` is
//! an increment.

use crate::model::EpSig;
use crate::search::ApplId;

/// A `Set<EpSig>` as a stamp array.
///
/// `clear` must be called before first use: generation zero is what a fresh
/// array is full of, so an unclear'd set reports every signature present.
#[derive(Default)]
pub struct SigSet {
    stamp: Vec<u32>,
    epoch: u32,
}

impl SigSet {
    /// Empty the set, and make room for signatures below `n`.
    pub fn clear(&mut self, n: usize) {
        if self.stamp.len() < n {
            self.stamp.resize(n, 0);
        }
        // Wrapping past zero would make every stale slot look live, so the
        // overflow case rewrites the array. That is one pass every four billion
        // clears, against a branch on each.
        match self.epoch.checked_add(1) {
            Some(g) => self.epoch = g,
            None => {
                self.stamp.fill(0);
                self.epoch = 1;
            }
        }
    }

    #[inline]
    pub fn contains(&self, k: EpSig) -> bool {
        self.stamp[k as usize] == self.epoch
    }

    /// `true` when `k` was not already present -- `HashSet::insert`'s answer.
    #[inline]
    pub fn insert(&mut self, k: EpSig) -> bool {
        let s = &mut self.stamp[k as usize];
        if *s == self.epoch {
            false
        } else {
            *s = self.epoch;
            true
        }
    }
}

/// A `Map<EpSig, V>` as a stamp array beside a value array. Same contract as
/// `SigSet`, including that `clear` comes first.
pub struct SigMap<V> {
    /// Stamp beside value in one slot rather than in two parallel arrays. Every
    /// `get` and `insert` in the depth walk reads or writes both halves, so
    /// splitting them costs two bounds checks and two cache lines for one
    /// logical access.
    slot: Vec<(u32, V)>,
    epoch: u32,
}

// Hand-written so that `V` need not be `Default` to construct an empty map.
impl<V> Default for SigMap<V> {
    fn default() -> Self {
        Self { slot: Vec::new(), epoch: 0 }
    }
}

impl<V: Copy + Default> SigMap<V> {
    pub fn clear(&mut self, n: usize) {
        if self.slot.len() < n {
            self.slot.resize(n, (0, V::default()));
        }
        match self.epoch.checked_add(1) {
            Some(g) => self.epoch = g,
            None => {
                for s in &mut self.slot { s.0 = 0; }
                self.epoch = 1;
            }
        }
    }

    #[inline]
    pub fn contains_key(&self, k: EpSig) -> bool {
        self.slot[k as usize].0 == self.epoch
    }

    #[inline]
    pub fn get(&self, k: EpSig) -> Option<V> {
        let s = &self.slot[k as usize];
        if s.0 == self.epoch { Some(s.1) } else { None }
    }

    /// Last write wins, as `HashMap::insert` does -- which matters: two steps
    /// can produce the same signature, and the later one's inputs are the ones
    /// `produced_from` ends up holding.
    #[inline]
    pub fn insert(&mut self, k: EpSig, v: V) {
        self.slot[k as usize] = (self.epoch, v);
    }
}

/// Every table `score` builds, allocated once for the whole refine and reused.
///
/// Field-by-field rather than behind accessors because the borrow checker has
/// to see the pieces as disjoint: `has_ancestor` reads `produced_from` while
/// writing its own `seen` set, and that is one `&mut Scratch` split two ways.
#[derive(Default)]
pub struct Scratch {
    /// `produced_from` -- for a produced signature, the inputs of the step that
    /// produced it, as a range into `pf_flat`. The map this replaces stored one
    /// *clone* of the producing step's input list per product.
    pub pf: SigMap<(u32, u32)>,
    pub pf_flat: Vec<EpSig>,
    /// `have` -- what is schedulable so far, in `is_valid`.
    pub have: SigSet,
    /// `seen` and the stack of one `has_ancestor` walk.
    pub anc_seen: SigSet,
    pub anc_todo: Vec<EpSig>,
    /// `used_as_lineage`.
    pub lin_used: SigSet,
    /// `depth_maps` -- one depth table per distinct walk source, pooled by
    /// arrival order. Sources per state peak at 23 on `sink-24` and 46 on
    /// `sink-178`, so the pool stops growing within the first few states and
    /// costs a few hundred kilobytes at the signature counts these problems
    /// reach.
    pub depth_slot: SigMap<u32>,
    pub depth_pool: Vec<SigMap<i32>>,
    /// Whether the pooled map beside it was walked to exhaustion.
    pub depth_full: Vec<bool>,
    pub depth_todo: Vec<(EpSig, i32)>,
    /// Kahn's layers, in `is_valid`.
    pub pending: Vec<ApplId>,
    pub ready: Vec<ApplId>,
    pub rest: Vec<ApplId>,
    /// The ordered lists the score itself is computed from. `counts` is
    /// `lin_usage`'s values, which is what `entropy` reads.
    pub lin_usage: Vec<(EpSig, i64)>,
    pub counts: Vec<i64>,
    pub lin_distances: Vec<f64>,
}

impl Scratch {
    /// Ready the tables `score`'s own body fills, over a signature universe of
    /// `n`. `pf` and `have` are not here: `validate` and `is_valid` each clear
    /// what they own, so they stay callable without a preceding `begin`.
    pub fn begin(&mut self, n: usize) {
        self.lin_used.clear(n);
        self.depth_slot.clear(n);
        self.lin_usage.clear();
        self.counts.clear();
        self.lin_distances.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole contract in one test: a cleared table is empty, `insert`
    /// reports novelty the way `HashSet::insert` does, and clearing again wipes
    /// it without touching a single slot.
    #[test]
    fn a_stamp_table_behaves_like_the_map_it_replaces() {
        let mut s = SigSet::default();
        s.clear(8);
        assert!(!s.contains(3));
        assert!(s.insert(3));
        assert!(!s.insert(3));
        assert!(s.contains(3));
        s.clear(8);
        assert!(!s.contains(3));

        let mut m: SigMap<u32> = SigMap::default();
        m.clear(8);
        assert_eq!(m.get(5), None);
        m.insert(5, 11);
        assert_eq!(m.get(5), Some(11));
        assert!(m.contains_key(5));
        // Last write wins, which is what `produced_from` relies on when two
        // steps produce the same signature.
        m.insert(5, 12);
        assert_eq!(m.get(5), Some(12));
        m.clear(8);
        assert_eq!(m.get(5), None);
    }

    /// Growing between states must not resurrect what an earlier state wrote.
    #[test]
    fn growing_the_universe_does_not_revive_stale_slots() {
        let mut s = SigSet::default();
        s.clear(4);
        s.insert(3);
        s.clear(64);
        assert!(!s.contains(3));
        assert!(!s.contains(50));
        assert!(s.insert(50));
    }

    /// The same wrap, on the map. `SigMap` resets its stamps by walking the
    /// slots rather than filling a separate array, so this is a different line
    /// of code from the set's and fails independently.
    #[test]
    fn the_map_epoch_wrapping_does_not_make_empty_slots_look_full() {
        let mut m: SigMap<u32> = SigMap::default();
        m.clear(4);
        m.insert(1, 7);
        m.epoch = u32::MAX;
        m.slot[2] = (u32::MAX, 99); // a slot written in the epoch about to be left
        m.clear(4);
        assert_eq!(m.get(0), None);
        assert_eq!(m.get(1), None);
        assert_eq!(m.get(2), None, "a stale slot survived the wrap");
        m.insert(2, 5);
        assert_eq!(m.get(2), Some(5));
    }

    /// The epoch is a `u32`, so a long enough run wraps. Wrapping *past zero*
    /// would make every never-written slot read as present, which is the one
    /// way a stamp table fails silently -- so the counter resets the array
    /// instead. Reached here by hand, since four billion clears is not a test.
    #[test]
    fn the_epoch_wrapping_does_not_make_empty_slots_look_full() {
        let mut s = SigSet::default();
        s.clear(4);
        s.insert(1);
        s.epoch = u32::MAX;
        s.stamp[2] = u32::MAX; // a slot written in the epoch about to be left
        s.clear(4);
        assert!(!s.contains(0));
        assert!(!s.contains(1));
        assert!(!s.contains(2), "a stale slot survived the wrap");
        assert!(s.insert(2));
    }
}

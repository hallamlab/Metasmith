//! The node-selection seam.
//!
//! Both phases pop one node from a frontier per iteration, by the same rule. The
//! rule is isolated here so an alternative can be measured against it without
//! either phase's code moving, and so the two cannot drift apart.
//!
//! `select` takes accessors rather than slices because the default rule reads one
//! score channel and only in the 95% of iterations that do not take the explore
//! arm. Passing a materialised `[f64; 2]` per node would double the memory
//! traffic of the hot loop to serve a policy that may not be selected. `key_of`
//! is here for a *stateful* policy: nothing is revisited -- the frontier is a
//! plain vector popped by swap-remove -- so per-node statistics are impossible
//! and statistics have to accumulate against something coarser.

use crate::rng::DecisionStream;

/// Both phases weight the same three moves: two exploit arms and one explore
/// arm. Shared so the refiner and the mcts phase cannot drift apart.
pub const SELECTION_WEIGHTS: [i64; 3] = [75, 20, 5];
pub const SELECTION_TOP_K: usize = 1;

pub const POLICY_ENV: &str = "MSM_SOLVER_POLICY";

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Weighted,
}

/// Which phase a policy instance is driving. A stateful policy keys its
/// statistics differently in each, so it is told rather than left to infer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Phase {
    Mcts,
    Refine,
}

pub struct Policy {
    kind: Kind,
}

impl Policy {
    /// Resolve the policy for one phase from the environment.
    ///
    /// An unset variable is the shipped rule, and an unrecognised one is refused
    /// rather than defaulted: a typo that silently selects the incumbent would
    /// report a measurement of the incumbent under the challenger's name.
    pub fn from_env(_phase: Phase) -> Result<Self, String> {
        let kind = match std::env::var(POLICY_ENV) {
            Err(_) => Kind::Weighted,
            Ok(s) if s.is_empty() || s == "weighted" => Kind::Weighted,
            Ok(s) => {
                return Err(format!(
                    "unknown {POLICY_ENV}={s:?}; known policies: weighted"
                ));
            }
        };
        Ok(Self { kind })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Does this policy consume `observe`? A policy that does not lets the caller
    /// skip computing a reward it would throw away.
    pub fn wants_rewards(&self) -> bool {
        match self.kind {
            Kind::Weighted => false,
        }
    }

    /// Choose one index in `0..len`.
    ///
    /// `score_of` yields a node's two score channels; `key_of` yields the coarse
    /// identity a stateful policy accumulates against.
    pub fn select<S, K>(
        &mut self,
        rng: &mut DecisionStream,
        len: usize,
        score_of: S,
        key_of: K,
    ) -> usize
    where
        S: Fn(usize) -> [f64; 2],
        K: Fn(usize) -> u32,
    {
        match self.kind {
            Kind::Weighted => {
                let _ = &key_of;
                let arm = rng.weighted_index(&SELECTION_WEIGHTS);
                if arm < SELECTION_WEIGHTS.len() - 1 {
                    let scores: Vec<f64> = (0..len).map(|i| score_of(i)[arm]).collect();
                    rng.pick_top_k(&scores, SELECTION_TOP_K)
                } else {
                    rng.bounded_int(len as u64) as usize
                }
            }
        }
    }

    /// Report the outcome of a selection. A stateless policy discards it.
    pub fn observe(&mut self, _key: u32, _reward: f64) {
        match self.kind {
            Kind::Weighted => {}
        }
    }
}

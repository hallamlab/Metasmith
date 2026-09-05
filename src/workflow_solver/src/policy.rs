//! The node-selection seam, and the policies that fill it.
//!
//! Both phases pop one node from a frontier per iteration, by the same rule. The
//! rule is isolated here so an alternative can be measured against it without
//! either phase's code moving, and so the two cannot drift apart.
//!
//! `select` takes accessors rather than slices because the default rule reads one
//! score channel and only in the 95% of iterations that do not take the explore
//! arm. Passing a materialised [f64; 2] per node would double the memory traffic
//! of the hot loop to serve a policy that may not be selected.
//!
//! Nothing is revisited -- the frontier is a plain vector popped by swap-remove --
//! so per-node statistics are impossible. `key_of` is the coarser identity a
//! stateful policy accumulates against: the transform, in both phases, which is
//! what `solver_policy.PuctSelection` keys on too.

use crate::det::{self, Map};
use crate::rng::DecisionStream;

/// Both phases weight the same three moves: two exploit arms and one explore
/// arm. Shared so the refiner and the mcts phase cannot drift apart.
pub const SELECTION_WEIGHTS: [i64; 3] = [75, 20, 5];
pub const SELECTION_TOP_K: usize = 1;

pub const POLICY_ENV: &str = "MSM_SOLVER_POLICY";
pub const PUCT_ENV: &str = "MSM_SOLVER_PUCT";

#[derive(Clone, Copy, Debug)]
pub struct PuctConfig {
    pub c_puct: f64,
    /// Prior weight per score channel. The defaults keep the shipped rule's
    /// relative emphasis on its two greedy arms (75:20) rather than inventing a
    /// new one.
    pub channel_weights: [f64; 2],
    pub temperature: f64,
    /// Value assigned to a key with no observations yet.
    pub fpu: f64,
    pub top_k: usize,
    /// Probability in units of 1/1000 of ignoring the index and drawing
    /// uniformly -- the role the shipped rule's third arm plays.
    pub epsilon_milli: u64,
    /// Exponential recency weight for Q. 1.0 is a plain running mean.
    pub decay: f64,
    /// Whether Q is estimated at all. With this off the visit counts still
    /// accumulate but every arm's value stays at `fpu`, so selection is the
    /// prior under a visit-count penalty and nothing is learned.
    pub use_value: bool,
    /// How a caller's before/after progress pair becomes a reward. `Delta`
    /// credits the improvement an action made; `Absolute` credits the state it
    /// left behind. They are not equivalent: a state's satisfied-requirement
    /// count only ever grows along a path, so absolute progress ranks a
    /// transform by how late it tends to be applied.
    pub reward_mode: RewardMode,
    /// Gain applied to the progress reward before it is clamped into [0, 1].
    ///
    /// `progress_of` divides by `requires.len() + 1`, so one satisfied
    /// requirement is worth 1/69 on the widest ladder rung while an unvisited
    /// key sits at `fpu`. At that scale Q is a novelty bonus rather than a
    /// ranking: every observed key falls to ~0 after one observation. Scaling
    /// separates the two hypotheses.
    pub reward_scale: f64,
}

/// How a before/after progress pair becomes a reward.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RewardMode {
    Delta,
    Absolute,
}

impl Default for PuctConfig {
    fn default() -> Self {
        Self {
            c_puct: 1.5,
            channel_weights: [75.0 / 95.0, 20.0 / 95.0],
            temperature: 0.5,
            fpu: 0.5,
            top_k: 1,
            epsilon_milli: 0,
            decay: 1.0,
            use_value: true,
            reward_mode: RewardMode::Delta,
            reward_scale: 1.0,
        }
    }
}

impl PuctConfig {
    /// Parse `k=v,k=v` from the environment. An unknown key or an unparseable
    /// value is refused: a silently ignored knob reports a measurement of the
    /// default under the name of the setting that was asked for.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut c = Self::default();
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (k, v) = part
                .split_once('=')
                .ok_or_else(|| format!("{PUCT_ENV}: expected k=v, got {part:?}"))?;
            let num = |v: &str| v.parse::<f64>().map_err(|e| format!("{PUCT_ENV}: {k}: {e}"));
            match k.trim() {
                "c_puct" => c.c_puct = num(v)?,
                "temperature" => c.temperature = num(v)?,
                "fpu" => c.fpu = num(v)?,
                "top_k" => c.top_k = num(v)?.max(1.0) as usize,
                "epsilon_milli" => c.epsilon_milli = num(v)?.max(0.0) as u64,
                "decay" => c.decay = num(v)?,
                "use_value" => c.use_value = matches!(v.trim(), "1" | "true" | "yes"),
                "reward_mode" => {
                    c.reward_mode = match v.trim() {
                        "delta" => RewardMode::Delta,
                        "absolute" => RewardMode::Absolute,
                        other => {
                            return Err(format!(
                                "{PUCT_ENV}: reward_mode: expected delta or absolute, got {other:?}"
                            ));
                        }
                    }
                }
                "reward_scale" => c.reward_scale = num(v)?,
                "w0" => c.channel_weights[0] = num(v)?,
                "w1" => c.channel_weights[1] = num(v)?,
                other => {
                    return Err(format!(
                        "{PUCT_ENV}: unknown key {other:?}; known: c_puct, temperature, fpu, \
                         top_k, epsilon_milli, decay, use_value, reward_mode, \
                         reward_scale, w0, w1"
                    ));
                }
            }
        }
        Ok(c)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Weighted,
    Puct(PuctConfig),
}

/// Which phase a policy instance is driving. A stateful policy keys its
/// statistics per phase, so it is told rather than left to infer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Phase {
    Mcts,
    Refine,
}

pub struct Policy {
    kind: Kind,
    w: Map<u32, f64>,
    n: Map<u32, f64>,
    total: u64,
    scratch: Vec<f64>,
}

impl Policy {
    /// Resolve the policy for one phase from the environment.
    ///
    /// An unset variable is the shipped rule, and an unrecognised one is refused
    /// rather than defaulted: a typo that silently selected the incumbent would
    /// report a measurement of the incumbent under the challenger's name.
    pub fn from_env(_phase: Phase) -> Result<Self, String> {
        let kind = match std::env::var(POLICY_ENV) {
            Err(_) => Kind::Weighted,
            Ok(s) if s.is_empty() || s == "weighted" => Kind::Weighted,
            Ok(s) if s == "puct" => {
                let cfg = match std::env::var(PUCT_ENV) {
                    Ok(spec) => PuctConfig::parse(&spec)?,
                    Err(_) => PuctConfig::default(),
                };
                Kind::Puct(cfg)
            }
            Ok(s) => {
                return Err(format!(
                    "unknown {POLICY_ENV}={s:?}; known policies: weighted, puct"
                ));
            }
        };
        Ok(Self { kind, w: det::map(), n: det::map(), total: 0, scratch: Vec::new() })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Does this policy consume `observe`? A policy that does not lets the caller
    /// skip computing a reward it would throw away.
    pub fn wants_observations(&self) -> bool {
        matches!(self.kind, Kind::Puct(_))
    }

    /// Whether the caller should compute the progress pair `observe` needs.
    /// Separate from `wants_observations`: counting visits is free, the progress
    /// walk that produces the reward is not.
    pub fn wants_rewards(&self) -> bool {
        match self.kind {
            Kind::Weighted => false,
            Kind::Puct(c) => c.use_value,
        }
    }

    /// Turn a caller's outcome into a reward. `delta` credits the improvement an
    /// action made rather than the state it left behind: an unlocked-transform
    /// set only ever grows, so absolute progress rises along every path and Q
    /// would rank transforms by how late they are applied.
    pub fn reward_for(&self, solved: bool, before: f64, after: f64) -> f64 {
        match self.kind {
            Kind::Weighted => 0.0,
            Kind::Puct(c) => {
                if !c.use_value {
                    c.fpu
                } else if solved {
                    1.0
                } else {
                    let raw = match c.reward_mode {
                        RewardMode::Delta => after - before,
                        RewardMode::Absolute => after,
                    };
                    (raw * c.reward_scale).clamp(0.0, 1.0)
                }
            }
        }
    }

    /// Choose one index in `0..len`.
    ///
    /// `score_of` yields the two channels the shipped rule ranks on, and those
    /// are not negotiable -- they are exactly what the decision contract names.
    /// `prior_of` yields the channels a *new* policy should build a prior from,
    /// which are not always the same: the refiner's second channel is
    /// `score * valid` over a score that is never positive, so an invalid state's
    /// 0.0 outranks every valid one. The shipped rule keeps that, bug and all,
    /// because reproducing it is the point; nothing new should inherit it by
    /// accident.
    pub fn select<S, P, K>(
        &mut self,
        rng: &mut DecisionStream,
        len: usize,
        score_of: S,
        prior_of: P,
        key_of: K,
    ) -> usize
    where
        S: Fn(usize) -> [f64; 2],
        P: Fn(usize) -> [f64; 2],
        K: Fn(usize) -> u32,
    {
        match self.kind {
            Kind::Weighted => {
                let _ = (&prior_of, &key_of);
                let arm = rng.weighted_index(&SELECTION_WEIGHTS);
                if arm < SELECTION_WEIGHTS.len() - 1 {
                    let scores: Vec<f64> = (0..len).map(|i| score_of(i)[arm]).collect();
                    rng.pick_top_k(&scores, SELECTION_TOP_K)
                } else {
                    rng.bounded_int(len as u64) as usize
                }
            }
            Kind::Puct(c) => {
                let _ = &score_of;
                if c.epsilon_milli > 0 && rng.bounded_int(1000) < c.epsilon_milli {
                    return rng.bounded_int(len as u64) as usize;
                }
                self.puct_index(len, &prior_of, &key_of, c);
                rng.pick_top_k(&self.scratch, c.top_k)
            }
        }
    }

    /// Min-max each prior channel across the frontier, weight, then softmax.
    ///
    /// Normalising per frontier rather than globally is what makes one prior
    /// comparable to the next: the channels are unnormalised scores whose range
    /// moves with the problem, and a softmax over raw values would be a different
    /// temperature on every instance.
    fn puct_index<P, K>(&mut self, len: usize, prior_of: &P, key_of: &K, c: PuctConfig)
    where
        P: Fn(usize) -> [f64; 2],
        K: Fn(usize) -> u32,
    {
        self.scratch.clear();
        self.scratch.resize(len, 0.0);
        for ch in 0..2 {
            let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
            for i in 0..len {
                let v = prior_of(i)[ch];
                if v < lo { lo = v; }
                if v > hi { hi = v; }
            }
            let span = hi - lo;
            let w = c.channel_weights[ch];
            for i in 0..len {
                let v = prior_of(i)[ch];
                self.scratch[i] += w * if span > 0.0 { (v - lo) / span } else { 0.5 };
            }
        }
        let tau = if c.temperature > 0.0 { c.temperature } else { 1e-9 };
        let mut peak = f64::NEG_INFINITY;
        for &v in &self.scratch {
            if v > peak { peak = v; }
        }
        let mut total = 0.0;
        for v in self.scratch.iter_mut() {
            *v = ((*v - peak) / tau).exp();
            total += *v;
        }
        let uniform = 1.0 / len as f64;
        // sqrt(1 + total) rather than sqrt(total): at the first selection of a
        // phase the bonus would otherwise be identically zero for every arm,
        // collapsing the index onto the constant first-play value.
        let explore = (1.0 + self.total as f64).sqrt();
        for i in 0..len {
            let p = if total > 0.0 { self.scratch[i] / total } else { uniform };
            let k = key_of(i);
            let n = self.n.get(&k).copied().unwrap_or(0.0);
            let q = if n > 0.0 { self.w[&k] / n } else { c.fpu };
            self.scratch[i] = q + c.c_puct * p * explore / (1.0 + n);
        }
    }

    /// Report the outcome of a selection. A stateless policy discards it.
    pub fn observe(&mut self, key: u32, reward: f64) {
        let c = match self.kind {
            Kind::Weighted => return,
            Kind::Puct(c) => c,
        };
        if c.decay >= 1.0 {
            *self.w.entry(key).or_insert(0.0) += reward;
            *self.n.entry(key).or_insert(0.0) += 1.0;
        } else {
            let w = self.w.entry(key).or_insert(0.0);
            *w = *w * c.decay + reward;
            let n = self.n.entry(key).or_insert(0.0);
            *n = *n * c.decay + 1.0;
        }
        self.total += 1;
    }
}

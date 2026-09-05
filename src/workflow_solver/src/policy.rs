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
    /// How hard to split a key's prior across its own copies in the frontier.
    ///
    /// A self-feeding transform -- one that consumes a property it also produces
    /// -- re-enters the frontier every time it is applied, so its copies grow
    /// without bound while the chain that actually reaches the target sits at one
    /// copy. The prior is a distribution over *transforms*, but the softmax runs
    /// over frontier slots, so 700 copies of one transform carry 700x the mass of
    /// a rival with one. Dividing each copy's prior by `(1 + ln count)^dup`
    /// restores the per-transform reading. 0.0 is off.
    ///
    /// The log is not cosmetic. A plain split by `count` -- `dup_lin` -- also
    /// breaks the loop, but it cannot tell a runaway from a chain that
    /// legitimately offers the same transform two or three times, and it costs
    /// four late-solving cases to buy one. The log is nearly flat at small counts
    /// and still worth ~7x at 700 copies.
    pub dup: f64,
    /// The plain-split form of `dup`: divide by `count^dup_lin`. Kept only so the
    /// harsher curve stays reachable; it measured as a LOSS at every setting.
    pub dup_lin: f64,
    /// Progressive widening: how many frontier slots per transform are eligible.
    ///
    /// The runaway this exists for is not a transform that is slightly too
    /// popular, it is one that re-enters the frontier every time it fires; by the
    /// iteration cap 714 of 818 slots on `sink26-103` are the same transform. A
    /// penalty proportional to the count has to be steep enough to beat 714,
    /// which also punishes the chain that honestly offers three. A cap does not:
    /// the first `cap` slots of every key compete on their merits and the rest
    /// are simply not looked at. 0 is off.
    ///
    /// At least one slot of every key survives, so this can never empty the
    /// frontier.
    pub cap: usize,
    /// Weight of the structural prior channel: how much a self-feeding transform
    /// is discounted before the softmax.
    ///
    /// `dup` and `cap` read the frontier's *population*, so they act only once
    /// the runaway has already grown, and they cannot tell a runaway from a chain
    /// that honestly offers the same transform three times. `Problem::self_feed`
    /// is the same fact read off the transform's own signature, before the search
    /// starts. The channel is subtracted in the same normalised space as the two
    /// score channels, so the suppression a copy gets is `exp(-w2/temperature)`;
    /// beating N copies of one transform takes `w2 > temperature * ln N`.
    ///
    /// 0.0 is off, and the channel is identically zero on every real workflow in
    /// the corpus, so it is inert there at any weight.
    pub w2: f64,
    /// Magnitude of the reward charged to an expansion that made no progress.
    ///
    /// `reward_for` clamps at 0, so a self-feeding transform -- one that consumes
    /// a property it also produces, and so re-enters the frontier every time it
    /// fires -- earns exactly what a merely unlucky transform earns. Once every
    /// key has been visited once, every Q is 0 and selection degenerates to the
    /// prior, which is the distribution the runaway dominates. A negative reward
    /// is the only channel by which "this transform actively wastes iterations"
    /// can be said. 0.0 is off, and off is the default.
    pub no_progress: f64,
    /// Added to `no_progress` per decayed prior no-progress observation of the
    /// same key, so a repeat offender is charged more than a first offender.
    ///
    /// The count decays with `decay` like `w` and `n` do, so it saturates at
    /// `1/(1 - decay)` -- 67 at the default 0.985. Useful settings are therefore
    /// two orders of magnitude below `no_progress`.
    pub np_ramp: f64,
    /// Floor on the penalised reward. Q is a decayed mean, so with a flat penalty
    /// it saturates at `-no_progress`; this bounds how far below the `fpu` gap a
    /// key can be driven however the ramp accumulates.
    pub np_floor: f64,
    /// Which phase charges the penalty. The two phases mean different things by
    /// no progress: in mcts it is a wasted expansion, in the refiner it is merely
    /// a candidate that did not beat the incumbent -- which is the common case
    /// for a healthy search, and the refiner is where real plan length is set.
    pub np_phase: NpPhase,
    /// The inverse of `dup`: multiply a key's per-slot prior by
    /// `(1 + ln count)^widen`.
    ///
    /// `dup` exists because a self-feeding transform floods the frontier, on the
    /// reading that the flood is what starves the chain to the target. Tracing
    /// `sink26-103` says otherwise: with PUCT the flooding transform takes 17 of
    /// 256 selections, not 714 -- the statistics are keyed per transform, so 774
    /// distinct frontier slots share one visit count and one `1/(1 + n)` penalty
    /// with a rival that offers one slot, and selection round-robins over
    /// transform identity. The unsolved cases need the same transform applied
    /// many times under different bindings, which is exactly what that
    /// round-robin forbids. `epsilon_milli` reaches them by accident: a uniform
    /// draw over a frontier that is 87% one transform is a draw for that
    /// transform. This is the targeted form of the same move, and it costs no
    /// randomness anywhere else. 0.0 is off.
    pub widen: f64,
}

/// Which phase charges the no-progress penalty.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NpPhase {
    Both,
    Mcts,
    Refine,
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
            // Above every attainable reward, on purpose. `progress_of` divides by
            // `requires.len() + 1`, so one satisfied requirement is worth 1/69 on
            // the widest ladder rung: Q is a novelty signal, not a ranking, and an
            // unvisited key has to outrank a visited one for that signal to exist.
            // Scaling the reward into a real ranking instead was measured and is
            // worse -- x5 costs three solves, x69 costs five.
            fpu: 1.5,
            top_k: 1,
            epsilon_milli: 0,
            // Stale evidence decays out. The win is a plateau over 0.982-0.99 and
            // falls away below 0.98, so it is the recency that matters rather than
            // this particular number.
            decay: 0.985,
            use_value: true,
            reward_mode: RewardMode::Delta,
            reward_scale: 2.0,
            dup: 0.0,
            dup_lin: 0.0,
            cap: 0,
            // Solves the three cases nothing else reaches, and cannot cost a real
            // workflow anything: the flag is identically 0 on all 17 real payloads
            // in the corpus and 1-5 on every generated one. A broad plateau --
            // 0.9 through 1.2 all reach 81/81 at the same real-plan length -- so
            // this is not a tuned ridge.
            w2: 0.95,
            no_progress: 0.0,
            np_ramp: 0.0,
            np_floor: -1.0,
            np_phase: NpPhase::Both,
            widen: 0.0,
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
                "dup" => c.dup = num(v)?,
                "dup_lin" => c.dup_lin = num(v)?,
                "cap" => c.cap = num(v)?.max(0.0) as usize,
                "w2" => c.w2 = num(v)?,
                "no_progress" => c.no_progress = num(v)?,
                "np_ramp" => c.np_ramp = num(v)?,
                "widen" => c.widen = num(v)?,
                "np_floor" => c.np_floor = num(v)?,
                "np_phase" => {
                    c.np_phase = match v.trim() {
                        "both" => NpPhase::Both,
                        "mcts" => NpPhase::Mcts,
                        "refine" => NpPhase::Refine,
                        other => {
                            return Err(format!(
                                "{PUCT_ENV}: np_phase: expected both, mcts or refine, got {other:?}"
                            ));
                        }
                    }
                }
                "w0" => c.channel_weights[0] = num(v)?,
                "w1" => c.channel_weights[1] = num(v)?,
                other => {
                    return Err(format!(
                        "{PUCT_ENV}: unknown key {other:?}; known: c_puct, temperature, fpu, \
                         top_k, epsilon_milli, decay, use_value, reward_mode, \
                         reward_scale, dup, dup_lin, cap, w0, w1, w2, \
                         no_progress, np_ramp, np_floor, np_phase, widen"
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
    phase: Phase,
    w: Map<u32, f64>,
    n: Map<u32, f64>,
    total: u64,
    scratch: Vec<f64>,
    /// How many frontier slots carry each key, for the `dup` split. Held on the
    /// policy so the hot loop reuses one allocation across iterations.
    dups: Map<u32, f64>,
    /// Running per-key count as `cap` walks the frontier. Separate from `dups`,
    /// which holds whole-frontier totals and must not be mutated underneath the
    /// `dup` split when both knobs are on.
    seen: Map<u32, f64>,
    /// The `w2` channel, indexed by transform. Empty until the caller supplies
    /// it, and read only where a key indexes into it -- the refiner's key for the
    /// plan it was handed is `u32::MAX`, which never does.
    structure: Vec<f64>,
    /// Decayed count of no-progress observations per key, for `np_ramp`. Held
    /// apart from `n` because `n` counts every observation, and the ramp has to
    /// read how often *this* key wasted an iteration.
    fails: Map<u32, f64>,
}

impl Policy {
    /// Resolve the policy for one phase from the environment.
    ///
    /// An unset variable is the shipped rule, and an unrecognised one is refused
    /// rather than defaulted: a typo that silently selected the incumbent would
    /// report a measurement of the incumbent under the challenger's name.
    pub fn from_env(phase: Phase) -> Result<Self, String> {
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
        Ok(Self {
            kind, phase, w: det::map(), n: det::map(), total: 0,
            scratch: Vec::new(), dups: det::map(), seen: det::map(),
            structure: Vec::new(), fails: det::map(),
        })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Whether this policy reads a structural channel, so a caller can skip
    /// handing over a table nothing will look at.
    pub fn wants_structure(&self) -> bool {
        matches!(self.kind, Kind::Puct(c) if c.w2 != 0.0)
    }

    /// Supply the per-transform structural channel. Indexed by the same key
    /// `select` is given.
    pub fn set_structure(&mut self, table: &[f64]) {
        self.structure.clear();
        self.structure.extend_from_slice(table);
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
        // The structural channel enters here, in the same normalised space as
        // the two score channels and before the softmax, so it is a prior over
        // transforms rather than a correction applied to the frontier afterwards.
        if c.w2 != 0.0 && !self.structure.is_empty() {
            for i in 0..len {
                let k = key_of(i) as usize;
                if let Some(&s) = self.structure.get(k) {
                    self.scratch[i] -= c.w2 * s;
                }
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
        let split = c.dup > 0.0 || c.dup_lin > 0.0 || c.widen > 0.0;
        if split {
            self.dups.clear();
            for i in 0..len {
                *self.dups.entry(key_of(i)).or_insert(0.0) += 1.0;
            }
        }
        if c.cap > 0 {
            self.seen.clear();
        }
        let uniform = 1.0 / len as f64;
        // sqrt(1 + total) rather than sqrt(total): at the first selection of a
        // phase the bonus would otherwise be identically zero for every arm,
        // collapsing the index onto the constant first-play value.
        let explore = (1.0 + self.total as f64).sqrt();
        for i in 0..len {
            let mut p = if total > 0.0 { self.scratch[i] / total } else { uniform };
            let k = key_of(i);
            if split {
                let m = self.dups.get(&k).copied().unwrap_or(1.0);
                if m > 1.0 {
                    if c.dup_lin > 0.0 { p /= m.powf(c.dup_lin); }
                    if c.dup > 0.0 { p /= (1.0 + m.ln()).powf(c.dup); }
                    if c.widen > 0.0 { p *= (1.0 + m.ln()).powf(c.widen); }
                }
            }
            if c.cap > 0 {
                let seen = self.seen.entry(k).or_insert(0.0);
                *seen += 1.0;
                if *seen > c.cap as f64 {
                    self.scratch[i] = f64::NEG_INFINITY;
                    continue;
                }
            }
            let n = self.n.get(&k).copied().unwrap_or(0.0);
            let q = if n > 0.0 { self.w[&k] / n } else { c.fpu };
            self.scratch[i] = q + c.c_puct * p * explore / (1.0 + n);
        }
    }

    /// Does this phase charge the no-progress penalty?
    fn np_here(&self, c: PuctConfig) -> bool {
        match c.np_phase {
            NpPhase::Both => true,
            NpPhase::Mcts => self.phase == Phase::Mcts,
            NpPhase::Refine => self.phase == Phase::Refine,
        }
    }

    /// Report the outcome of a selection. A stateless policy discards it.
    ///
    /// A reward of 0 is not merely a low reward: `reward_for` clamps there, and
    /// both call sites reach it only when the expansion satisfied nothing new. So
    /// zero is where the no-progress penalty is applied, and it is applied here
    /// rather than in `reward_for` because the charge is a function of the key's
    /// own history, which the caller does not have.
    pub fn observe(&mut self, key: u32, reward: f64) {
        let c = match self.kind {
            Kind::Weighted => return,
            Kind::Puct(c) => c,
        };
        let mut reward = reward;
        if c.no_progress > 0.0 && c.use_value && reward <= 0.0 && self.np_here(c) {
            let f = self.fails.get(&key).copied().unwrap_or(0.0);
            reward = (-(c.no_progress + c.np_ramp * f)).max(c.np_floor);
            let e = self.fails.entry(key).or_insert(0.0);
            *e = if c.decay >= 1.0 { *e + 1.0 } else { *e * c.decay + 1.0 };
        }
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

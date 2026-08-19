# r9, one bake — the honest width, the fast lane, and the settled table

## Context

r9's chemistry is built, measured and staged-ready. It is not promotable, because the substitution
mechanism reports one member's uncertainty under both members' names. Fixing that means re-running
the member lanes, and a members run currently costs ~38 minutes because the eQuilibrator lane never
got sharded. The principal's instruction is to make the run cheap, settle everything the run must
contain, and then **run it once**.

The intended outcome is one r9 bake — built, staged beside r8, verified at that path — where every
substituted reaction carries a width that names the member it describes.

## Issues

**I1.** `sigma_sub` is computed without a member parameter and written identically into both member
tables, so dGbyG's 9.602 kJ/mol flavin drift is disclosed downstream as eQuilibrator's 0.315.

**I2.** The `congeners` column conflates curator width (member-independent) with member–tabulation
disagreement (member-specific), and carries no record of which member produced its numbers.

**I3.** The eQuilibrator member lane runs at `cpus=1`, unsharded, making it the ~38-minute critical
path of every members run; dGbyG is sharded at 20 and takes ~20 minutes.

**I4.** Three curation questions that change what the single run must contain are unresolved: the
acyl carriers, branching glycogen's acceptor, and calibrate's stale balance gate.

**I5.** `MNXR175494` and `MNXR145036` are the same enzyme on two accessions and receive opposite
directions after r9; the scope that reported the defect has not been told.

**I6.** The `direction` part has failed twice on host flakiness, and the background wrapper's
trailing `echo` masked the driver's real exit code as success.

## What you said

> #12 seems like it obviously needs to be per member and points to a deeper table join or provenance
> issue. need to think about the purpose of each table and understand what each row is representing.
> see if there's a deeper issue or likely slop that needs fixing that has caused #12.

> For the 1hr run, shard more. use sockeye, get it down to 5 mins or so.

> the remaining decisions affect the downstream direction lanes right? shouldn't you do them now
> instead of re-running the bake after all?

> Do what you have to, but run the bake once.

## High-level goals

**G1.** Make the substitution mechanism state which member each uncertainty belongs to, rather than
asserting one member's number for both.

**G2.** Make a full members run cheap enough that getting it right is not rationed by wall-clock.

**G3.** Settle everything the single run must contain before it starts, so it does not need repeating.

**G4.** Produce one r9 bake, staged beside r8 and verified at that path, with each fix separately
attributable.

**G5.** Leave the glycogen finding with the scope that needs it, rather than inside this run log.

## Acceptance criteria

1. `Substitutions.sigma_sub` cannot be called without naming a member; the old signature is gone
   rather than defaulted.
2. `congeners` carries, per row, which member scored it — and a table scored by one member and read
   by another is a refusal, not a silent reuse.
3. Curator width and member disagreement are separate quantities with separate names; the
   `DIR_DECADE` refusal is driven by the member-specific one.
4. A row admitted for one member and refused for another produces an abstention for the refusing
   member, and the refusal appears in `decisions.tsv` naming the member.
5. The flavin couples' dGbyG arm is refused or widened on its own measured 9.602, never on 0.315.
6. `substitute check` and the bake suite pass; `DIRVER` moves, and the vendored tree matches source.
7. Both member lanes are sharded; a full members run completes in ≤ ~6 minutes wall-clock.
8. `dir_drive merge` still refuses unless the shards reconstitute the universe exactly, at the new
   counts, for both members.
9. Shard count changes what runs concurrently and not what any reaction is asked — verified by
   reproducing a known member table at a different count.
10. The members run happens **once**. A second full run is a plan failure, not an iteration.
11. r9 is staged as `metabolism_bake_r9` sharing zero inodes with the deployed tree, exactly four
    files swapped, `logs/` and `aam_cache/` byte-identical.
12. The three verifiers pass at the staged path; criterion 17's sha256 holds.
13. `test_deployed_bake.py` pins are computed and reported, not committed.

## Tasks

**T1 — Split the substitution's width from the member's disagreement.** Root-fix I1 and I2: give
`sigma_sub` a member, give `congeners` provenance, and separate the two quantities it conflates.

**T1.c — Compact.**

**T2 — Shard both member lanes to the five-minute mark.** Mirror dGbyG's fan-out into the
eQuilibrator lane, raise both counts to what Sockeye will grant, and prove the partition is inert.

**T3 — Settle the table's contents before the run.** Price and decide the acyl carriers, branching
glycogen, and the calibrate gate — everything that changes what the one run contains.

**T3.c — Compact.**

**T4 — Run the bake once, stage it beside r8, and verify it there.** Members, then direction, then
direction_bake; stage, gate, verify; compute the deployed pins without committing them.

**T5 — Report the glycogen accession split to bench-eydallin.**

**T6 — Debrief this scope.**

## Approach by task

### T1 — Split the substitution's width from the member's disagreement

The defect is that the member dimension exists at both ends of the pipeline and is lost in the
middle. `substitute anchor --member {eq,dgbyg}` computes a genuine per-member spread; it writes that
to a report TSV, and the numbers in `substitutions.tsv` were transcribed from one run into a column
with no member stamp. From there down nothing has a member: neither `_congener_spread` nor
`Substitutions.sigma_sub` takes one, so `drive.py` calls the same function in both member runs.

Two quantities are tangled in that column, and the code says so at `substitute.py:931`. **Curator
width** is how far apart defensible alternative stand-ins are — a property of the curation, genuinely
member-independent. **Member disagreement** is how far a given member places the stand-in from the
potential the row cites — irreducibly member-specific. T3 folded the second into the column for a
sound reason: a couple with only one defensible model would otherwise report width 0.0, which is
exactly the "asserted structure looks like a measurement" failure the column exists to prevent. The
fix keeps that protection while giving the two quantities separate names.

The consequence to face rather than paper over: `_congener_spread` **refuses** a row whose spread
exceeds `DIR_DECADE`, and dGbyG's honest flavin spread is 9.6 against a 5.71 bound. So per-member
width implies **per-member admission** — flavin admitted for eQuilibrator and refused for dGbyG. That
is the correct semantics rather than an unfortunate one: the ensemble is built for members to abstain
independently, and a member that cannot place the stand-in should not vote on it. It is nonetheless a
different bake from widening an error bar, and it costs coverage where dGbyG is the only speaker.

Price that cost before choosing, because it is the whole argument: count the reactions where dGbyG
speaks and eQuilibrator does not, among those touching a drifting couple. Report it, then take
per-member admission unless the number is large enough to make r9 worse than r8.

**Gotchas:** do not let the new width reach the member's own `sigma` column — `eq_vote` reads sigma
against the floor to detect a group cancellation, and lifting a cancelling zero re-promotes it to
tier 1, the defect r8 was baked to remove. `combine.py:159` gates tier 1 on `sigma_sub == 0`; a
per-member column must keep that gate meaning the same thing. The `congeners` numbers currently on
disk are eQuilibrator's — re-score the dGbyG arm rather than reusing them, and make reuse impossible
rather than merely discouraged.

### T2 — Shard both member lanes to the five-minute mark

The machinery exists and is already member-generic. `drive.py`'s `cmd_eval` takes `--shard i/n` using
`aam.shard`'s crc32 partition — the same one the mapper lanes use — and `cmd_merge` takes
`--shard-file`/`--expect`/`--universe` and refuses unless the shards reconstitute the universe
exactly. Nothing in it is dGbyG-specific. So the eQuilibrator lane needs its transform rewritten
against `dgbyg.py`'s loop, not new sharding code.

Sizing from the measured run rather than from guesses: both lanes submitted 18:06 and finished 18:44;
dGbyG at 20 shards is ~20 minutes at a measured 0.286 s/reaction, and the unsharded eQuilibrator lane
is the ~38-minute critical path. eQuilibrator at 16 shards clears the target comfortably at 4 GB
each. dGbyG is the harder half: its footprint is flat at 2.15 GB per process and does **not** fall as
the partition narrows, so 64 shards is ~138 GB on one node. Check what Sockeye will actually grant
before committing a number, and prefer a count a node will schedule promptly over a count that is
theoretically faster and queues.

**Gotchas:** raise the shard count and change the substitution semantics in separate commits, so a
surprise in the run has one candidate cause. The partition is content-addressed, so the count cannot
change what any reaction is asked — assert that rather than assume it, by reproducing an existing
member table at a different count. `merge`'s `--expect` must move with the count or the run fails at
the end rather than the start, which is the expensive place to fail.

### T3 — Settle the table's contents before the run

Running once means every question that changes the tables is answered first.

**The acyl carriers** are in scope by the principal's standing decision and have never been priced
against the gates. Reach is real — 2,261 tier-0 reactions touch an ACP accession — though that is an
upper bound, because the count does not separate "no wildcard" from "no structure at all". The
obstacle is structural: an acyl carrier's two states differ by the whole acyl group, so the
zero-heavy-atom couple rule refuses every row by construction, and the anchor needs a sibling written
with a matched thioester pair whose chain length agrees. Build that, run the anchors, and admit only
what passes. **If they fail the anchor or congener gates, that is a stop-and-ask, not a quiet
exclusion** — and the principal is present to ask.

**Branching glycogen** costs exactly three tier-0 reactions and cannot be adjudicated: 411 MetaNetX
compounds carry the required acceptor formula, at least three of them defensible branched α-glucans,
and no gate separates them. Recommend leaving it out and recording the cost.

**Calibrate's stale gate** discards 479 reactions the member balanced after restaging. σ₀ is 23.489
either way because the committed fit is the unsubstituted subset, so the constant is settled
regardless; only the bins move. Recommend documenting and deferring, because fixing it now makes r9's
ratios a mix of chemistry and calibration change and costs the attribution the four-way pricing was
built to give.

**Gotchas:** anything admitted here must be re-priced through the forecast before the run, or the
accounting describes a chemistry the bake does not have. A new `kind` must go through the same
refusal ledger as the others rather than around it.

### T4 — Run the bake once, stage it beside r8, and verify it there

Sequence, and it resolves the σ₀/fingerprint circularity: members on the cluster → retrieve →
`curated` and `calibrate` locally → fit σ₀ from r9's own points → commit both canon copies if it
moved → re-vendor → `direction` → `direction_bake`. Re-vendor after the last code edit and before
submission, or the cluster runs the old chemistry with nothing saying so.

Then stage: copy the deployed tree to a suffixed sibling sharing zero inodes, swap the four files,
leave `logs/` and `aam_cache/` byte-identical, run `check_stage.py` before `dvc add`, and verify at
that path with the three verifiers that can be aimed there. Compute the `test_deployed_bake.py` pins
and report them without committing — r8 remains deployed, and a pin file describing a chunk nothing
reads would fail against the one that is read.

**Gotchas:** the driver's real exit code is masked when a background wrapper ends in `echo`; read the
log tail, never the task notification's exit code. Sockeye has dropped twice tonight — on a broken
rsync and on a relay-start timeout — so check the connection before submitting rather than after
failing, and treat a retry as a single deliberate attempt, never a loop. The retrieval must *print*
its refusal to rebuild `logs/`; read it rather than assuming it. Promote is irreversible and stays
the principal's call after verification.

### T5 — Report the glycogen accession split to bench-eydallin

They reported glycogen over-fed and hand-overrode with τ=100. r9 moves the accession they walk,
`MNXR145036`, from 1.0 to a confident 0.204 — *further* in the feeding direction — while the correct
catabolic call lands on `MNXR175494`, which carries zero atom-pair edges and cannot move their number.
Same enzyme, two accessions, opposite calls. Send them the measurement and the reasoning, not just
the number.

**Gotchas:** a cross-scope message posted to this scope's channel is silently never read — post to the
recipient's channel. Do not write into their worktree.

### T6 — Debrief this scope

Rewrite `REBAKE.md` as r9's protocol, including the per-member width semantics and the control-set
screening rule. Re-measure `direction_rescue/README.md`'s mechanism table. Record the calibrate gate
and the acyl-carrier outcome. Register the plan copy in the scope. Journal, commit, refresh.

## Still unsure about

**Whether per-member admission costs more coverage than it buys honesty.** T1 prices it before
choosing; a large number is a stop-and-ask.

**Whether the acyl carriers can pass their gates at all.** In scope by decision, unpriced against the
mechanism until T3 builds it.

## Callouts

**Promote is not in this plan.** r9 is staged and verified; the rename onto the deployed path stays
the principal's explicit call.

**dGbyG's shard count is bounded by memory, not by method** — 2.15 GB per process, flat, so the
five-minute target on that lane costs ~138 GB on one node.

---

## Handoff — 2026-08-17 late

### Where the run stands

`T1` and `T2` are **committed and green**. `T3` is priced but undecided. `T4`, `T5`, `T6`
are untouched. **The members run has not happened.** Nothing is staged and nothing is
promoted; r8 is still the deployed bake and the working tree is clean at `a54a8dc`.

    a54a8dc  T2: the eQuilibrator lane was the critical path because it never sharded
    b37cfeb  T1: the width belongs to a member, and so does the refusal

- `DIRVER` = **`lib-direction-19b4bfeff6b2`** (was `89bcc4651c84`); vendored tree matches source.
- Bake suite **330 passed, 2 skipped**. Non-bake suite must be run with `PYTHONPATH="$PWD/src"`
  pinned — the `ecspr` env has `ecspr` installed from a SIBLING worktree and will import that
  tree otherwise.
- `substitutions.tsv` is migrated: `congeners` is gone, replaced by `congeners_eq`, `gap_eq`,
  `congeners_dgbyg`, `gap_dgbyg`. 26 rows, both arms scored.
- Predicted r9 ledger under per-member admission, measured by splicing rather than estimated:
  **tier 0 37,404 → 36,212 · tier 1 unchanged 2,171 · tier 2 +1,722 · tier 3 −530.**

### The remaining tasks, explicitly

**T3 — Settle the table's contents. BLOCKS T4, because the run happens once.**

- **T3a. Decide the acyl carriers (#8).** Priced this session: 1,428 ACP accessions, ALL
  unreadable (773 no SMILES, 655 wildcard). 2,261 tier-0 reactions touch one; **1,209 are
  blocked ONLY by ACP** and could be unblocked. The route found while pricing: ACP's business
  end is 4′-phosphopantetheine, the identical thioester environment to CoA's, so an acyl-CoA
  model is chemically principled rather than convenient — and MetaNetX writes many of these
  reactions BOTH ways, giving natural siblings with a predicted offset of ~0, the same shape
  as the polymer anchors that landed at 1e-05. Work required: model rows for matched
  chain lengths, then `substitute anchor --member eq` and `--member dgbyg`. `kind='thioester'`
  already exists and is untouched; the couple and potential gates do not apply to it, so the
  ANCHOR is the entire safety argument. **A gate failure is a stop-and-ask, per the deviation
  protocol.**
- **T3b. Decide branching glycogen (#10).** Cost is exactly 3 tier-0 reactions —
  `MNXR136341`, `MNXR145038`, `MNXR145039`. 411 MetaNetX compounds carry the acceptor formula
  `C18H32O16` and at least three are defensible branched α-glucans (panose `MNXM1104683`,
  isomaltotriose `MNXM1104226`/`MNXM1106015`, 6-O-glucosylmaltose `MNXM1107398`). No gate
  separates them. **Recommendation: leave out and record the cost.**
- **T3c. Decide the calibrate gate (#13).** `calibrate.py:85` returns `unbalanced` from raw
  `reac_prop` BEFORE consulting the member, discarding 479 reactions the member balanced after
  restaging. σ₀ is 23.489 either way (the committed fit is the unsubstituted subset), so the
  constant is settled; only the bins move (`PHYSIOL-LEFT-TO-RIGHT` τ 98.27 → 112.18).
  **Recommendation: document in REBAKE.md, defer to r10** — fixing it now makes r9's ratios a
  mix of chemistry and calibration change and costs the attribution.
- **T3d. Re-price whatever T3a admits through the forecast** before submitting, or the
  accounting describes a chemistry the bake does not have.

**T4 — Run the bake once, stage it, verify it there.** In order:

1. Check the ssh connection BEFORE submitting: `ssh(verb="connect", args={host:"sockeye"})`.
   A `status:"unavailable"` means the circuit breaker holds the host and recovery is a
   Discord `/approve` — it is NOT something to retry around.
2. `PYTHONPATH=src mamba run -n msm python tests/fabfos/build_references_bake_parts_on_hpc.py members --user txyliu`
   — plan first WITHOUT `--run`, confirm `check_plan` passes, then add `--run`.
   Expect ~12 min: eQuilibrator 16-wide (~4 min), dGbyG 32-wide (~12 min, the critical path).
3. Retrieve seams, then run `curated` and `calibrate` LOCALLY off the retrieved eq seam.
4. Re-derive σ₀ with `instruments/sigma0.py` from r9's OWN points table. Commit both canon
   copies only if it moved, then re-vendor.
5. `direction`, then `direction_bake` (needs `ref::metabolism_vocab` from the AAM host as a
   declared import).
6. Stage: `cp -a data/fabfos/processed/metabolism_bake data/fabfos/processed/metabolism_bake_r9`,
   swap exactly four files (`direction.parquet` and the three `seams/direction_*`), leave
   `logs/` and `aam_cache/` byte-identical, and CONFIRM the retrieval printed its refusal to
   rebuild `logs/` rather than assuming it.
7. **Run `instruments/check_stage.py <deployed> <staged>` and require exit 0 BEFORE `dvc add`.**
8. Verify at the staged path with all three verifiers: `check_references.py --results`,
   `aam_v3_nostoc.py metabolism_bake metabolism_bake_r9`, `measure_rescue.py --bake metabolism_bake_r9`.
9. Compute the `test_deployed_bake.py` pins and REPORT them; **do not commit that edit** — r8
   remains deployed, and r8 re-derived those pins in its PROMOTE commit `7792651`, not its
   stage `ed9331d`.
10. **STOP. Promote is the principal's explicit call and is not in this plan.**

**T5 — Report the glycogen accession split to bench-eydallin.** `MNXR175494` and `MNXR145036`
are the same enzyme on two accessions and get OPPOSITE directions after r9: the polymer arm
puts `MNXR145036` at 0.204 (favouring synthesis — *further* in the direction they complained
about), while the crosswalk puts `MNXR175494` at 1.369 (catabolic, correct) on an accession
carrying ZERO atom-pair edges, which therefore cannot move their delivery split. Post to
**their** channel — a cross-scope message posted to this scope's channel is silently never
read. Do not write into their worktree.

**T6 — Debrief.** Rewrite `REBAKE.md` for r9 including the per-member width semantics and the
control-set screening rule; re-measure `direction_rescue/README.md`'s mechanism table; register
the plan copy in the scope.

### Open question the principal has not answered

**The five-minute target was not met and cannot be with a bigger number.** dGbyG's shards are a
shell `&`/`wait` loop in one allocation, and Sockeye's skylake nodes carry 32 cores (cascade 40)
against 190 GB — cores run out ~6× sooner than memory. 32 shards is ~12 min. Reaching ~5 min
needs an `srun` fan-out across nodes, or the forward pass on a GPU. Both are real changes.

### Traps this session actually hit

- **A background wrapper ending in `echo` masks the driver's exit code.** The harness reported
  `exit code 0` for a `direction` run that had raised. Read the log tail; never trust the task
  notification for that driver.
- **Sockeye dropped twice** — an rsync broken pipe mid dev-overlay push, then `TimeoutError:
  [relay start] produced no output for 60s` after a clean deploy. Probing a dead connection
  trips the ssh circuit breaker, which then needs a human `/approve`.
- **`reassemble.py --curated` wants the per-MNXR table**, not `_curated_per_reaction.parquet`.
  Passing the latter silently duplicates 345 rows and the comparison then dies with
  "Can only compare identically-labeled Series objects".
- **The `msm` solver binary is not executable**, so every plan falls back to the ~15× slower
  Python solver. Same plans; not r9's to fix.

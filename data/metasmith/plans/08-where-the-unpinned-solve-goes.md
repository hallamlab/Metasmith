<!-- Investigation report. Not a plan: one question, asked and answered by measurement. -->

# Where the unpinned metagenomics solve goes

## The question

`metagenomics_from_paired_reads` pins sixteen of its twenty-five targets to the megahit assembly.
Unpinned, the same workflow was measured at 4 min 6 s and 16.7 GB against 1.56 s and 0.11 GB pinned.
The suspicion put to this session was that the cost is the template's setup rather than the solver.
It is — but not where the template's own comment says, and not in a component whose output is used.

## The answer in three numbers

**The search is not involved.** With the refiner switched off, the unpinned target set solves in
**0.83 s and 30 MB**, in 50 of its 256 MCTS iterations, and returns the 35-step plan with both
assemblers that the template comment has described all along. Pinned, the same figures are 0.06 s and
7 MB. Everything above that is the refiner.

**One refiner iteration on the unpinned plan scores 110,866 candidate plans. On the pinned plan it
scores 755.** That is the entire 150× — measured with `cProfile` on the Python solver, which is a
line-for-line port of the engine and therefore has the engine's call counts.

**All of it is discarded.** `found_on` is 1 in every arm at every budget: the winner is always the
plan the refiner was handed.

## The measurements

`msm_solver` is driven directly on a wire payload dumped once per arm, so nothing below includes
library load or plan assembly. Peak RSS is `/usr/bin/time -v`; each run is capped with
`systemd-run --user --scope -p MemoryMax=20G`.

### The search, isolated

| arm | max_refine | wall | peak RSS | mcts iterations | steps |
|---|---|---|---|---|---|
| unpinned | 0 | 0.83 s | 30 MB | 50 of 256 | 37 |
| shipped | 0 | 0.06 s | 7 MB | 50 of 256 | 31 |

Both searches stop at the same iteration count. The unpinned one is 14× slower and returns six more
steps, and that is the whole of the search's contribution.

### The refiner, laddered

| max_refine | unpinned wall | unpinned peak RSS | shipped wall | shipped peak RSS |
|---|---|---|---|---|
| 0 | 0.83 s | 30 MB | 0.06 s | 7 MB |
| 1 | 2.57 s | 187 MB | 0.08 s | 7 MB |
| 4 | 6.44 s | 493 MB | 0.13 s | 7 MB |
| 16 | 18.92 s | 1.37 GB | 0.21 s | 11 MB |
| 64 | 58.28 s | 4.27 GB | 0.50 s | 30 MB |
| 256 | **4 min 6 s** | **16.7 GB** | **1.56 s** | **0.11 GB** |

Memory is a straight line in the iteration count — about **103 MB per iteration** unpinned against
**0.4 MB** pinned — because nothing is released. `states`, `frontier`, `seen` and the application
arena all grow monotonically for the life of the call. That is why the failure mode is an OOM kill
rather than a slow answer: at 103 MB an iteration, an 8 GB cap runs out around iteration 80 of 256.

### What one iteration does

`cProfile`, same `max_refine`, both arms:

| | shipped | unpinned | ratio |
|---|---|---|---|
| candidate plans scored, one iteration (`score_node`) | 755 | 110,866 | **147×** |
| candidate plans scored, four iterations | 2,939 | 332,862 | 113× |
| ancestry walks (`_depth_between`), four iterations | 129,316 | 10,984,446 | 85× |
| ancestry walks per candidate | 44 | 33 | 0.75× |
| total python calls, four iterations | 12.3 M | 797 M | 65× |

The last row of the middle block is the one that settles it: the unpinned plan does **fewer**
ancestry walks per candidate, because pinning *adds* lineage anchors and each anchor is a walk. The
cost is not that each candidate is dearer. It is that there are 147× as many of them.

Extrapolated over the shipped budget, one unpinned solve scores of the order of **20 million
candidate plans** and runs roughly **700 million ancestry walks**, retains all of it, and returns the
plan it started with.

### Pinning is a cliff, not a gradient

Refiner cost against how many of the target requirements carry a lineage anchor, at `max_refine=8`:

| arm | anchored requirements | peak RSS | MB per refiner iteration |
|---|---|---|---|
| unpinned | 6 | 860 MB | 103 |
| orfs_only | 7 | 847 MB | 104 |
| bins_only | 9 | 369 MB | 45 |
| assembly_only | 10 | 830 MB | 102 |
| shipped | 22 | 7 MB | 0 |

Pinning some of the assembly-derived targets buys nothing: while any of them can still be answered
from either assembler, both legs stay in the plan and every step downstream of them stays
interchangeable. This is the cost half of the rule the previous session found on plan shape — every
target that could diverge needs its own anchor — and it says the same thing more sharply, because a
partial pin costs full price.

## Does the refiner ever earn its budget?

**Yes, and it needs three iterations to do it.** A first pass at this question compared
`max_refine=256` against `0` at seed 42 only, found no difference on 11 templates and 11 generated
cases, and concluded the refiner never does anything. That was a single-seed answer and it was wrong;
the scope's own record already held a counterexample at another seed.

Across four seeds:

| case | seed | steps at `max_refine=0` | at `256` | `found_on` | frontier empty at |
|---|---|---|---|---|---|
| `isolate_assembly_from_long_reads` | 7 | 13 | **11** | 3 | 16 |
| `isolate_assembly_from_long_reads` | 99 | 13 | **9** | 3 | 16 |
| every other template x seed (42 pairs) | | unchanged | | 1 | |
| every generated case (11) | | unchanged | | 1 | |

Two things fall out, and the second is the useful one.

**The refiner is real.** It removes four steps from a thirteen-step plan at seed 99. Switching it off
by default would cost that.

**It finds its winner at iteration 3, and its frontier is empty by 16.** Asked for 256 it still stops
at 16, because on a pinned plan the state space is small enough to exhaust. The shipped budget of 256
is therefore not what makes the refiner work. It is only what lets it run away on a plan whose state
space does not exhaust.

Tested directly: **budgets of 4, 8 and 16 all give byte-identical plan fingerprints to 256 on all 55
case-seed pairs** -- 11 templates by 4 seeds, plus 11 generated cases. Zero differences.

On the metagenomics ladder `found_on` is 1 at every budget from 1 to 256.

**CORRECTION, added after this report was published.** This paragraph originally attributed the
rejections to `mock_produced` leaving stale parents on the candidates' produced endpoints. That is
wrong. `validate_node` does not read `Endpoint.parents` at all. It walks a step graph rebuilt per
state from each step's actual bindings, so a swapped step's outputs already carry the new ones.

What rejects is that same step graph's blind spot. A branched given application carries `used == {}`,
so a given endpoint has no parents in the step graph, and every requirement whose lineage anchor
binds to a given fails. Measured on all eleven templates and both arms here, that is **every**
rejection, with no other cause observed. Judged by the specification's relation -- the reflexive
closure over declared parents -- **40** of the unpinned workflow's 110,866 candidates are admissible
and **1** of the shipped workflow's 755. The stale-parents defect is real and narrow: `rectify`
re-derives every product's parents before a plan leaves the refiner, so it reaches an emitted plan
only through the `inherent_parents` term.

## What to do about it

**The template is fine.** Its pins are load-bearing and its comment now says what they buy. Nothing
about the shipped configuration should change.

**The fix is upstream of the pins, and it is the user's call because it moves the decision
contract.** Three options, in the order the evidence supports them:

1. **Cut the default `max_refine` from 256 to 8.** This is the whole fix and it costs nothing. Every
   improvement the refiner has been observed to make is found by iteration 3, every frontier that
   exhausts is empty by 16, and 4, 8 and 16 are byte-identical to 256 across 55 case-seed pairs. It
   takes the unpinned template from **4 min 6 s / 16.7 GB to 11.5 s / 0.84 GB** and the shipped one
   from 1.56 s to 0.17 s, while keeping the 13-step-to-9 improvement a default of 0 would throw away.
   **It is still not a one-line change**: `refine` draws from the same `DecisionStream` as the search,
   so a shorter budget shifts the stream, and a default change of this kind needs
   `SOLVER_RNG_VERSION` considered and `fingerprints.json` re-pinned deliberately rather than found
   to be unchanged.
2. **Budget the refiner in candidates, not iterations.** `max_refine=256` means 190,000 candidate
   plans on one workflow and 20 million on another. A cap on states scored, or on bytes retained,
   would make the knob mean the same thing everywhere, and would turn the OOM into a worse answer.
3. **Repoint `validate_node` at the specification's relation, and turn the generator prune back
   on.** *Superseded reading: this item originally called for repairing the generator's lineage
   handling.* The measurement above says the validator's relation is the defect and the prune at
   `solver.py:620` is the cost. Together they take one iteration from 110,866 candidates to 40, and
   they recover the +209 and +207 improvements on `fosmid_inserts_from_pooled_reads` and
   `ecspr_survey_from_pooled_reads` that this scope recorded as generated and discarded.

## What this does not say

**It no longer says why the candidates are rejected.** The paragraph that did has been corrected
above. Read the cost findings here as sound and the cause finding as superseded.

A budget of 8 is indistinguishable from 256 *on the cases measured here* -- 55 case-seed pairs over
one library and one generator. That is not a proof that no workflow needs a deeper search; the honest
reading is that nothing in this corpus exhausts a frontier later than iteration 16, not that nothing
could. A budget is also not a repair: `fosmid_inserts_from_pooled_reads` still scores a candidate 209
above its input and still rejects it, and nothing here touches that.

## Reproducing

    python research/metasmith/witness_sweep/duplicate_work.py --arm minimal   # the duplication
    bash <ladder>                                                             # see below

The wire payloads, the ladder script and the profiling harness are under
`research/metasmith/witness_sweep/profiling/`. `perf` is unusable on this host
(`perf_event_paranoid=4`) and there is no `cargo`, which is why the call counts come from the Python
port rather than from the engine; the engine's own wall-clock and RSS ladders are measured directly
on the binary.

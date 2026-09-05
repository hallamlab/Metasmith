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

No case has been found where it does.

- **Eleven shipped templates**: solved at `max_refine=256` and at `0`, plan fingerprints compared.
  **0 of 11 change.** Total solve time 8.43 s against 5.46 s.
- **Eleven generated cases** (`CORPUS` + `STRESS_CORPUS`): **0 of 11 change.**
- `found_on = 1` on every arm of the metagenomics ladder, at every budget from 1 to 256.

That is consistent with what the scope already recorded from a different direction: the candidate
generator is handed `mock_produced`, which switches its lineage check off, so the candidates it
builds reuse produced endpoints whose declared parents describe inputs the candidate no longer
consumes — and `Derived` is exactly the clause that rejects those. The refiner is not choosing not to
improve the plan. It is generating candidates that cannot be accepted, and paying full price to score
each one.

## What to do about it

**The template is fine.** Its pins are load-bearing and its comment now says what they buy. Nothing
about the shipped configuration should change.

**The fix is upstream of the pins, and it is the user's call because it moves the decision
contract.** Three options, in the order the evidence supports them:

1. **Default `max_refine` to 0** until the generator's lineage handling is repaired. On the evidence
   here it moves no plan on 22 cases, and it takes the unpinned template from 4 min 6 s / 16.7 GB to
   0.83 s / 30 MB and the shipped one from 1.56 s to 0.06 s. **It is not a one-line change**: `refine`
   draws from the same `DecisionStream` as the search, so skipping it shifts the stream. The plans
   are identical on all 22 cases measured, but a default change of this kind needs
   `SOLVER_RNG_VERSION` considered and `fingerprints.json` re-pinned deliberately rather than found
   to be unchanged.
2. **Budget the refiner in candidates, not iterations.** `max_refine=256` means 190,000 candidate
   plans on one workflow and 20 million on another. A cap on states scored, or on bytes retained,
   would make the knob mean the same thing everywhere, and would turn the OOM into a worse answer.
3. **Repair the generator's lineage handling** — `expand_node` passing `mock_produced` — which is
   what would make the refiner able to accept a candidate at all. Already the top of this scope's
   list, and this report is another argument for it: until it lands, every cycle the refiner spends
   is spent producing candidates its own checker rejects.

## What this does not say

The refiner has never been observed to improve a plan *here*. That is not the same as proving it
cannot, and the case that motivated it — `fosmid_inserts_from_pooled_reads`, where a candidate scores
209 above the input and is rejected — is still on the record as a case where a working refiner would
have something to do. Switching the default off is a statement about the component as it stands, not
a decision to delete it.

## Reproducing

    python research/metasmith/witness_sweep/duplicate_work.py --arm minimal   # the duplication
    bash <ladder>                                                             # see below

The wire payloads, the ladder script and the profiling harness are under
`research/metasmith/witness_sweep/profiling/`. `perf` is unusable on this host
(`perf_event_paranoid=4`) and there is no `cargo`, which is why the call counts come from the Python
port rather than from the engine; the engine's own wall-clock and RSS ladders are measured directly
on the binary.

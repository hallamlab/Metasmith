# What prompt iteration actually moved

Four revisions on the frozen 400-reaction dev split, one model (Qwen3-32B Q5_K_M), scored
by element recount and never by the model's own account of its work. `scoreboard.tsv` is
the machine-readable version; this file is why the numbers came out that way.

| revision | yield | balanced | unbalanced | unusable | refused | controls |
|---|---:|---:|---:|---:|---:|---|
| r0 baseline | 5.2% | 21 | 98 | 255 | 26 | not run |
| r1 structure annotations | 14.2% | 57 | 247 | 59 | 37 | not run |
| **r2 count and close** | **15.0%** | 60 | 233 | 71 | 36 | **0 regressions** |
| r3 `working` scratchpad | 12.2% | 49 | 226 | 70 | 55 | 21 regressions, FAIL |

**r2 is the frozen prompt.** r2 over r1 is +0.8 points against a standard error near 1.8 at
n=400, so it is flat; r3 is worse on both axes. Two consecutive revisions without
improvement is the stopping rule, and it fired.

## The one revision that worked, and why

r1 tripled the baseline by fixing a single input-design defect: r0 never told the model
which accessions MetaNetX has no structure for, so it rebuilt answers out of the very
blockers it was asked to remove. 236 of r0's 255 `unusable` verdicts were that one bug.
Annotating each participant with HAS STRUCTURE / NO STRUCTURE collapsed `unusable` from 255
to 59.

**The failures did not disappear, they moved.** `unbalanced` went 98 -> 247 as the model
started using real structures and then failed to make them cancel. That is a better class
of failure to have — it is chemistry attempted rather than chemistry evaded — but it is
where the remaining coverage is stuck.

## The two that did not work, and what they rule out

r2 told the model to tally C/N/O/P/S and close the gap with water or CO2, targeting the 64
near-misses within two atoms and the 29 that were oxygen-only. It moved nothing. r3 went
further and gave the tally somewhere to happen: a `working` string emitted **first**, since
a constrained decode emits properties in schema order and field order is the only scratchpad
a non-thinking model gets. It measured worse, and it broke 21 controls.

Together those two say something more useful than either alone. **The unbalanced residue is
not an arithmetic failure and cannot be prompted away.** The model is not failing to count;
it is failing to know what the reaction does. Instructing it to check its work produces a
check, not a correction — and on controls, instructing it to actively balance made it edit
reactions that were already right, which is exactly the regression the gate exists to catch.

The next lever is not wording. It is either a stronger model, or the reasoning budget r3
tried to fake with a string field, and the honest way to test that is to turn thinking on
and pay for the tokens.

## What the gate caught that the scoreboard would not have

r2's control gate first reported one regression. It was not chemistry: a control whose
prompt came to 4,105 tokens against a 4,096-token slot, whose harness error the arbiter
scored as a regression. Harness errors are `unscorable` now. A gate that fails a revision
because a prompt overran its slot is not measuring chemistry, and zero-regressions is a hard
gate, so a false positive there blocks a revision that deserved to ship.

## The crosswalk is a mapping aid, not a structure claim

Harvested from r2's dev run: 83 metabolites through all four `admit()` gates, 177 element
rows, 12.2% of the panel's 683 blockers. Per-metabolite and per-reaction coverage coincide
here only because substitutions are taken from balanced rewrites alone, and a rewrite that
balanced had already covered every blocker in its reaction.

Oxidized and reduced coenzyme F420 harvest to the **same** skeleton. That is correct for
this pipeline — hydrogen is excluded from the recount on purpose, and a redox couple does
share its heavy atoms — but it means a harvested row asserts a carbon skeleton for mapping
and nothing about oxidation state. Anything reading these rows as structures rather than as
mapping stand-ins will be wrong about redox partners specifically.

## The direction lane, measured on both splits

The prompt was never iterated, so dev and held-out are two samples of one measurement
rather than a tuning set and a test set.

| split | baseline | ungated | gated accuracy | gated coverage |
|---|---:|---:|---:|---:|
| dev, n=200 | 50.0% | 59.4% | 81.8% | 5.5% |
| held-out, n=400 | 50.0% | 57.9% | 86.7% | 3.8% |

The pilot's bias reproduces at four times the sample size and is, if anything, sharper:
**99.4% correct on equations MetaNetX writes left-to-right, 6.5% on those it writes
right-to-left.** The model ratifies the layout. A single ungated pass beats guessing by 8
points, and every additional independent opinion would share the bias rather than cancel
it — which is why this lane's ensemble had to shrink to two orientations of one opinion
instead of growing to three opinions of one orientation.

The gate works and it is expensive. Abstaining wherever the two orientations agree throws
away 248 of 400 reactions and converts an untrustworthy 58% into a defensible 87%. What
survives is 15 reactions. Scaled to the 36,151 reactions with no direction evidence that is
roughly 1,400 calls at ~87%, which is below every thermodynamic tier and belongs — if
anywhere — as a low tier that never overrides a member vote.

**This is an argument against running the direct-opinion direction lane at universe scale.**
Two orientations of 83,795 reactions buys a four-figure number of tie-breaks at an accuracy
the existing tiers already beat. The indirect path is the one worth taking: a simplified
balanced equation with real structures is what dGbyG and eQuilibrator need, and those
members are calibrated against measurement in a way this is not.

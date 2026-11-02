# What the stoichiometric collapse moves

`measure_collapse.py` re-adjudicates the whole MNXref reaction universe under both size
measures and joins the result to the deployed bake. `measure_partial.py` sizes what the
partial lane would reach, using the worklist the first script writes. `results/` holds the tables it wrote,
committed so the next session re-runs the script and diffs rather than re-deriving.

Everything they need is local: `data/fabfos/originals/metanetx/4.5`, the materialised
`data/fabfos/processed/metabolism_bake` trio, and a built `lookup::` directory —
`buildlib::mnx_lookups build` makes that from the MetaNetX drop-in in a few minutes, and
it is a build product nothing tracks, so put it somewhere disposable. The adjudication is
about eight minutes of rdkit and is cached; everything after it is seconds, so the
reporting half can be iterated on without `--rebuild`.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_collapse/measure_collapse.py \
        --lookups <built lookups dir> \
        --bake data/fabfos/processed/metabolism_bake \
        --outdir research/fabfos/benchmarks/aam_collapse/results

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_collapse/measure_partial.py \
        --lookups <built lookups dir> \
        --worklist research/fabfos/benchmarks/aam_collapse/results/worklist_both_measures.parquet \
        --outdir research/fabfos/benchmarks/aam_collapse/results

## The tables

| file | what it answers |
|---|---|
| `verdict_movement.tsv` | every before → after transition, with counts. Read the first line: `mappable → mappable` at 57,061 is the invariant, not a statistic. |
| `yield_curve.tsv` | banking rate by atom count under both measures. The threshold's warrant. |
| `recovered.tsv` | every reaction the collapse admits, with both counts and what refused it before. |
| `nitrogen.tsv` | every reaction carrying an N2 species, by id and equation. The scope exists for these. |
| `deployed_bake_debt.tsv` | reactions the deployed bake holds that the 600-atom cut refuses. A pre-existing regression, not one this change introduces. |
| `populations.tsv` | what is still refused, and how much of it. |
| `multiplicity.tsv` | how much stoichiometric repetition the recovered reactions carry — the one thing collapse changes about a row's *shape*. |
| `partial_lane_yield.tsv` | what the partial lane reaches, per target population. Written by `measure_partial.py`. |

## Reading them

**The measure was wrong, not the threshold.** Under the collapsed measure the banking
rate holds at 81–94% up to 600 and falls to 26.9% at 600–650, 16.7% at 650–700, 10% at
700–800. The knee is as sharp as the expanded one and it is in the same place, so the
collapsed cap is 600 — re-derived rather than carried across, and landing on the same
number is the finding rather than a shortcut.

**THE CURVE IS CENSORED ABOVE THE THRESHOLD.** Both measures' rows past 600 describe
MetaCyc, not a mapper: every mapper-derived method in the deployed table stops dead at the
cap because the same cut sat upstream of all three lanes, and `curated` is the only thing
banked above it. So the bins up to 600 warrant the constant and the bins past it warrant
nothing. `../aam_cap/` tests the threshold from the other side.

**`atom_pairs` is keyed by vocabulary CODE, not by MNXR.** Every join to the deployed
bake goes through `vocab.parquet`; reading the code as an id yields an empty join and a
curve of zeros, which reads as a result rather than as a bug.

**A recovered reaction is ADMITTED, not banked.** This measures which reactions the
mappers are allowed to see. Whether they produce pairs is what a rebake answers, and
nothing local stands in for it. Every row of `partial_lane_yield.tsv` past the first is
likewise a PROXY: the lane's real target set is "admitted and still ended with nothing",
which cannot be known without having run the mappers, so the table sweeps reaction length
rather than picking a threshold.

**`n_atoms` counts atom-index triples, so a collapsed reaction emits one where an
expanded one emits sixteen.** No existing row moves — collapse only touches reactions
that were refused — but the new rows sit on a different footing from a comparable
expanded reaction's. `multiplicity.tsv` sizes it: median 4.7x, max 89x, over an estimated
0.7% of the table. Whether those weights should be re-expanded by coefficient is a
question about edge weights, and it is the user's.

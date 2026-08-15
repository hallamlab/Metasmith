# What the stoichiometric collapse moves

`measure_collapse.py` re-adjudicates the whole MNXref reaction universe under both size
measures and joins the result to the deployed bake. `results/` holds the tables it wrote,
committed so the next session re-runs the script and diffs rather than re-deriving.

Everything it needs is local: `data/fabfos/originals/metanetx/4.5` plus a built
`lookup::` directory (minutes, from `ecspr.bake` is not needed — `buildlib::mnx_lookups`
builds it), and the materialised `data/fabfos/processed/metabolism_bake` trio. The
adjudication is about eight minutes of rdkit and is cached; everything after it is
seconds, so the reporting half can be iterated on with `--rebuild` omitted.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_collapse/measure_collapse.py \
        --lookups <built lookups dir> \
        --bake data/fabfos/processed/metabolism_bake \
        --outdir research/fabfos/benchmarks/aam_collapse/results

## The tables

| file | what it answers |
|---|---|
| `verdict_movement.tsv` | every before → after transition, with counts. Read the first line: `mappable → mappable` at 57,061 is the invariant, not a statistic. |
| `yield_curve.tsv` | banking rate by atom count under both measures. The threshold's warrant. |
| `recovered.tsv` | every reaction the collapse admits, with both counts and what refused it before. |
| `nitrogen.tsv` | every reaction carrying an N2 species, by id and equation. The scope exists for these. |
| `deployed_bake_debt.tsv` | reactions the deployed bake holds that the 600-atom cut refuses. A pre-existing regression, not one this change introduces. |
| `populations.tsv` | what is still refused, and how much of it. The partial lane's target sizes. |

## Reading them

**The measure was wrong, not the threshold.** Under the collapsed measure the banking
rate holds at 81–94% up to 600 and falls to 26.9% at 600–650, 16.7% at 650–700, 10% at
700–800. The knee is as sharp as the expanded one and it is in the same place, so the
collapsed cap is 600 — re-derived rather than carried across, and landing on the same
number is the finding rather than a shortcut.

**`atom_pairs` is keyed by vocabulary CODE, not by MNXR.** Every join to the deployed
bake goes through `vocab.parquet`; reading the code as an id yields an empty join and a
curve of zeros, which reads as a result rather than as a bug.

**A recovered reaction is ADMITTED, not banked.** This measures which reactions the
mappers are allowed to see. Whether they produce pairs is what a rebake answers, and
nothing local stands in for it.

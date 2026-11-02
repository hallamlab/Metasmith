# The frozen panel

Built by `panel.py` from `data/fabfos/processed/metabolism_bake` with `--seed 20260818 --frac 0.1 --dev 400
--controls 200`. **Re-running with a different seed invalidates every scoreboard
row taken before it.** Regenerate only with these arguments.

| split | file | n | movable | no_lever | one_sided |
|---|---|---:|---:|---:|---:|
| dev | `dev.jsonl` | 400 | 383 | 11 | 6 |
| held-out | `heldout.jsonl` | 842 | 784 | 35 | 23 |
| controls | `controls.jsonl` | 200 | — | — | — |

Sampled from 12417 residual reactions — every reaction whose ledger outcome is
one of `rescue_declined`, `mapped_nothing`, `partial_declined`, `rescued_nothing`.

Controls are drawn from the 42794 banked reactions that carry at least
4 participants and a structure for every one of them, **and that balance
on heavy atoms exactly as MetaNetX writes them** — 200 kept from 219
screened, so roughly 9% of the fully-structured
banked population does not balance as written. Banking does not require balance; the AAM
lane banks the elements that mapped. A control that never balanced cannot be left balanced
by anything the model does, and without this screen the gate reports regressions that are
properties of MetaNetX rather than of the lane.

Two strata are ceilings on coverage rather than failures of the prompt, and both stay in
the denominator so the ceiling stays visible. `no_lever` reactions carry no blockers: the
mapper was handed a fully structured equation and returned nothing anyway, so there is
nothing for a rewrite to remove. `one_sided` reactions have an empty side, which no
rewrite balances. Iterating against either is wasted GPU; hiding either flatters the lane.
Report per stratum.

Each record annotates every participant with `has_structure` and, where one exists,
`smiles`. That is not decoration: the pilot's prompt withheld it, and the agent rebuilt
its answers out of the very blockers it was asked to remove in ten of twelve failures.

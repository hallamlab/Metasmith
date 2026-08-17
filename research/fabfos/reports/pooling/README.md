# Log-odds pooling — before and after

Raw output from the two harnesses, kept so the change can be re-argued without re-running
anything. `*_before.*` were taken on the pre-pooling code and cannot be reproduced on the
current tree — replacing `belief` in place is what makes them worth keeping.

| file | what it is |
| --- | --- |
| `k12_before.csv` / `k12_after.csv` | `benchmarks/pooling/rank_eval.py`, K-12 de novo scored against iML1515 |
| `k12_sweep.csv` | the same, over the `(lam0, lam1, tau)` grid |
| `eydallin_before.json` / `eydallin_after.json` | `benchmarks/eydallin/pooling_sanity.py`, ag1 vs ag1 + glgC |
| `eydallin_cohort_before.json` / `_after.json` | the same over all 85 eydallin conditions |

## K-12 ranking

| | before | after |
| --- | --- | --- |
| max `E` | 33.46 | 0.99999999999986 |
| spread (max/min) | 8.1e4 | 21.1 |
| AUROC | 0.762 | 0.771 |
| P@100 | 0.49 | 0.66 |
| P@500 | 0.52 | 0.62 |
| top 100 resting on ONE assertion | 25% | 0% |
| top 100 backed by >1 channel | 63% | 93% |
| `MNXR172198` | rank 6, E 12.28 | rank 881, E 0.119 |

`MNXR172198` is the case that motivated the change: 65 ORFs, one channel, one EC number
(2.7.13.3), no corroboration. It now sits exactly where one assertion puts it.

AUROC is reported, not gated — see `benchmarks/pooling/README.md` for why the truth set
works against this change. It went up anyway.

## eydallin end-to-end, ag1 host

Glucose (`MNXM1364061`) → glycogen (`MNXM738130`), carbon slice of the bake, weights from
the de novo `e_coli_ag1` proteome through `condition_weights(..., weighting="belief")`.

| | before | after |
| --- | --- | --- |
| nodes / edges | 168,565 / 193,922 | 168,565 / 193,922 |
| reactions in the weight dict / used | 14,180 / 10,998 | 14,180 / 10,998 |
| `sum(E)` | 4363.0 | 1325.5 |
| base conductance | 0.356411 | 0.399791 |
| + glgC conductance | 0.356555 | 0.400054 |
| glgC relative delta | +0.0403% | +0.0658% |

The graph is bit-identical in shape: pooling changes what each reaction is worth, never
which reactions exist. glgC's delta grew by 1.6x, which is the direction pooling predicts —
its `MNXR145051` is carried by three distinct channels in the clone rows, so it gains from
agreement counting for more than repetition.

## The cohort ordering, which is what the benchmark scores

All 85 eydallin conditions, ranked by relative delta. The top nine are in exactly the same
order before and after, and the top five are the same five glycogen genes:

    glgA > glgB > glgP = malP > glgC

Spearman over all 85 is 0.895; 75 conditions change rank, all of them below `mlc` where
the deltas are 1e-6 and smaller and the ordering is numerical noise rather than signal.
glgC holds rank 5 with a 1.6x larger relative delta.

glgA's relative delta *shrinks*, 0.848 → 0.144. That is the compressed range showing up as
expected, not a regression: the base network conducts more (every `E` moved off a
long-tailed sum onto a bounded probability), so one added gene is a smaller fraction of it.
Ordering, which is what the panel scores, is untouched.

## Where E now ties

`sigma` reaches exactly 1.0 in float64 once the pooled log-odds pass ~37. On K-12 nothing
does (the top is 1 - 1.4e-13); on the four-channel ag1 lane 4 reactions of 14,180 do, and
tie. That is 40 independent assertions apiece — the model is saying it cannot rank them,
which is true.

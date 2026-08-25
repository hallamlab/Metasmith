# Fang 2025 — the four-axis LPS ratio arm

Fang et al., *Metab. Eng.* **92**:13–21 (2025) screened the ASKA overexpression library
inside a free-fatty-acid producer and found `rfaY` — the LPS core heptose II kinase — as its
strongest single target. This lane asks whether ECSPr's **two-point conductance ratio**
nominates that class of gene, with the sinks chosen to ask the paper's question (membrane
homeostasis) rather than the assay's (free fatty acid).

It is the second lane on this study. `research/fabfos/benchmarks/aska/` already asked
whether a **one-probe** glycerol → FFA conductance tracks the titer over the 59 genes Fang
rebuilt, and answered no. Its digitised titers are the phenotype column here and are not
re-measured. What is new is the ratio, the LPS sinks, and the `gof.csv` reaction-chemistry
schema the eydallin arm settled.

## The chain

Each step's own module docstring is the argument for it; this is only the order and the
environment, which differs per step.

| | env | |
|---|---|---|
| `parse/build_gof_table.py` | `msm-fabfos` | the clones, keyed on W3110, with their GC titers |
| `parse/build_gof_reactions.py` | `msm-fabfos` | one MetaNetX reaction per gene, its equation, its carbon-bond call |
| `gpr_build/build_fang_gpr.py` | `msm` | the whole 4,102-clone ASKA roster against iML1515, both channels |
| `sinks/probe_axes.py` | `ecspr` | is each declared axis reachable at all, per channel |
| `sweeps/sweep_fang_ratio.py` | `ecspr` | the library sweep, one row per (clone, reachable axis) |
| `sweeps/analyse_fang_ratio_sweep.py` | `msm` | within-hit ranking, roster-wide AUC, and where `rfaY` lands |

`sinks/probe_axes.py` must run before the sweep, on both channels. The sweep reads its
output to decide what to sweep and refuses to start without it.

## Things that would go wrong silently

- **Sweeping an unreachable sink.** A sink that is not a node, or is a node with no
  atom-resolved route, returns exactly `0` for every clone — a flat column that reads like a
  real null. Two of the four axes are dead on one channel each, and on *different* channels:
  the LPS inner core reaches only on curated, Kdo₂-lipid A only on de-novo. Neither channel
  alone can answer this study.
- **Unioning the fold instead of multiplying it.** Every ASKA clone is a chromosomal *E.
  coli* ORF the host already carries, so a set-union "overexpression" is a no-op for the
  whole library. `--fold-check` is the gate and it is not optional.
- **Joining the cohort on gene names.** The ASKA roster carries 2005 spellings and the paper
  carries 2024 ones. Eleven of the fifty-nine disagree — every `waa*` gene is `rfa*`,
  `lptA/B/D/E` are `yhbN`/`yhbG`/`imp`/`rlpB`, `gppA` is `gpp`, `opgD` is `mdoD`. A name join
  writes "no reaction" for the entire LPS core module, which is the half of the study the
  primary axis is about. Everything joins through the b-number.
- **Filtering the de-novo background on `in_atom_universe`.** `runs/e_coli_k12/gpr/
  gpr_denovo.parquet` leaves that column null. Filtering on it yields an empty graph and a
  run in which nothing is reachable.
- **Mixing the two baselines.** Round 1 was assayed against F0 = 799.6 mg/L and round 2
  against RF = 2240.3 mg/L, so `gof.csv` and `gof_round2.csv` are separate tables. A
  round-2 clone at 2,200 mg/L is exactly its own control, not a triumph over F0.
- **Reading a de-novo tie block as a result.** Eydallin's arm found one promiscuous ProtBERT
  call assigning a committed step to 27 unrelated ORFs. `--min-lanes 2` filters the clone
  side; applying it to the background disconnects the target instead.

## What the paper does not supply

No per-gene enrichment score for the library. The screen sequenced only the sorted top-0.1%
pool, Figs. 1b and 4b show that read-count scatter with the top 24 marked, no table stands
behind it, and the data statement is "Data will be made available on request". Every
roster-wide statistic in this lane therefore rests on a binary label rather than on a
measured null, and the within-hit ranking over the 58 assayed genes is the result that does
not.

The findings are in `REPORT.md`.

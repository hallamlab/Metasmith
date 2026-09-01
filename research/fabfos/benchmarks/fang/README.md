# Fang 2025 — the ASKA / free-fatty-acid lane

Fang et al., *Metab. Eng.* **92**:13–21 (2025) screened the genome-scale ASKA overexpression
library inside a free-fatty-acid producer, then **individually rebuilt and assayed 59 ORFs by
GC**. That is what makes it usable as a benchmark and what LASER could not offer: genes that
were built, measured, and did *not* move the phenotype. Its strongest single target is
`rfaY`, the LPS core heptose II phosphokinase.

One lane, two arms, one verdict in `REPORT.md`:

- **The glycerol → FFA panel arm.** One two-point probe from glycerol to the merged
  fatty-acid readout, over the 87 measured conditions, scored against a size-matched null.
  The first GOF cohort in this tree with real measured negatives, and the first benchmark
  driven by the packaged `src/ecspr` command line rather than `docker/fabfos/bin/ecspr_cli.py`.
- **The ratio arm.** A two-point conductance **ratio** between competing fates, over the whole
  4,102-clone roster, with the numerator grounded at the LPS core rather than at the fatty
  acid — the mechanism the paper claims rather than the readout the assay measured. Follows
  the protocol the eydallin lane settled.

This lane was assembled from two that were built separately, under the library's name
(`aska` / `aska_ffa`) and under the paper's. It is now one lane per paper. The ASKA library
ROSTER keeps the library's name and belongs to no lane:
`data/fabfos/originals/benchmarks/aska/library/` and `data/fabfos/runs/aska/gpr/` are the
4,123-clone library that the eydallin lane also consumes.

## The chain

Each step's own module docstring is the argument for it; this is only the order and the
environment, which differs per step.

### Shared

| | env | |
|---|---|---|
| `parse/digitise_ffa.py` | `figure-net` | the titers, read off the figure bars against the paper's stated anchors |
| `parse/build_extraction.py` | `msm-fabfos` | those bars onto the study schema — **the lane's primary artifact** |

`digitise_ffa.py` needs `pypdf` and Pillow; the rest need only pandas.
`data/fabfos/benchmarks/fang/extraction.tsv` is what both arms read, and nothing regenerates
it — see that directory's own README.

### The glycerol → FFA panel arm

| | env | |
|---|---|---|
| `gpr_build/build_ecspr_tables.py` | `msm-fabfos` | the clones, the plasmid, the null pool, the conditions |
| `panels/run_panel.py` | `ecspr` | four `ecspr` calls: `draw`, both probes on both arms, `score` |
| `sweeps/triage_ffa_panel.py` / `sweeps/analyse_ffa_panel.py` | `msm-fabfos` | what the method can see, and whether the score tracks the titer |

Between the extraction and the tables the study tier is built and published:

    python build_references/run_benchmark_conditions_local.py --only study_tier
    cp -r data/scratch/bench_conditions_local/study_tier/fang data/fabfos/benchmarks/

`run_panel.py` must run under the `ecspr` env — `dev/ecspr.sh --help` builds it.
Its result tables are under `out/`.

### The ratio arm

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
- **Matching ASKA gene names against the model.** The roster and the paper use the old *rfa*
  nomenclature and iML1515 uses *waa*, so the headline hit `rfaY` is `waaY`/`b3625`. Eleven of
  the fifty-nine ORFs — every `waa*`, every `lpt*`, and `gppA` and `opgD` — miss by name and
  resolve by synonym. Every join in this lane goes through the b-number, never through names;
  a name join writes "no reaction" for the entire LPS core module, which is the half of the
  study the ratio arm's primary axis is about.
- **Reading reach off `gpr_gem.parquet`'s `in_atom_universe`.** The study tier excludes
  TRANSPORT from the atom universe and the host GEM's column does not. The ASKA winners are
  largely transporters, so the host column counts exactly those as visible and overstates the
  ceiling by roughly a factor of two.
- **Filtering the de-novo background on `in_atom_universe`.** `runs/e_coli_k12/gpr/
  gpr_denovo.parquet` leaves that column null. Filtering on it yields an empty graph and a run
  in which nothing is reachable.
- **Counting units differently in the two arms.** `--weighting uniform` scores a reaction by
  how many distinct `unit_id`s nominate it. The study tier writes one `unit_id` per *study*,
  so a two-clone strain would add one unit while a size-2 null draw adds two.
  `gpr_build/build_ecspr_tables.py` rewrites `unit_id` to the clone for exactly this reason.
- **Dropping a deletion by `mnxr`.** The ΔrfaY-complemented strain deletes the chromosomal
  copy and carries a plasmid one. A drop keyed on the reaction removes both; the drop is keyed
  on `intermediate_id`, which separates the host's row from the clone's. Both deleted genes
  are sole-gene reactions in iML1515, so it is exact.
- **A clone missing from the null pool.** `ecspr draw` samples the pool's `orf` values, so a
  clone whose ORF resolves to no reaction must still get a row with a null `mnxr`. Without it
  the null is made only of clones the model can see, which is a null for a different question.
- **Mixing the two baselines.** Round 1 was assayed against F0 = 799.6 mg/L and round 2
  against RF = 2240.3 mg/L, so `gof.csv` and `gof_round2.csv` are separate tables. A round-2
  clone at 2,200 mg/L is exactly its own control, not a triumph over F0.
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

## The tier is now one universe — it was two

This study was published on its own out of a **bake v2** run while the other seven folders
were still the **v1** ones, from before transport left the atom universe, so their
`in_atom_universe` columns were not comparable and a cross-study count read two universes as
one. `BUILD_studies.json`, which is a whole-tier record, said v1 and was true of seven
eighths of the tree.

Rebuilding the seven was deliberately not done here — until moving `eydallin` onto its real
host forced a tier rebuild, which is all-or-nothing. That run put every study on v2 and made
the record true. **What moved is only the universe flags**: the edges are identical study for
study — same `(condition_id, orf, mnxr, action)` multiset in every one — while
`in_atom_universe` flipped on 34 of keio's 230 rows, 488 of laser's 3,257 and 2 of aromatic's
7. A number in a committed report that was read off a v1 `in_atom_universe` is therefore
stale; one read off an edge set is not.

The findings are in `REPORT.md`.

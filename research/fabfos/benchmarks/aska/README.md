# ASKA / FFA — does ECSPr predict the phenotype?

Fang et al. (*Metab. Eng.* 92:13–21, 2025) screened the whole ASKA overexpression
library inside a free-fatty-acid producer, then **individually rebuilt and assayed
60 ORFs by GC**. That is what makes it usable as a benchmark and what LASER could
not offer: genes that were built, measured, and did *not* move the phenotype.

The first GOF cohort in this tree with real measured negatives, and the first
benchmark driven by the packaged `src/ecspr` command line rather than
`docker/fabfos/bin/ecspr_cli.py`.

## The chain

Each step's own module docstring is the argument for it; this is only the order.

| | |
|---|---|
| `digitise_ffa.py` | the titers, read off the figure bars against the paper's stated anchors |
| `build_extraction.py` | ORF → b-number → iML1515 reactions, onto the study schema |
| `build_ecspr_tables.py` | the clones, the plasmid, the null pool, the conditions |
| `run_panel.py` | four `ecspr` calls: `draw`, both probes on both arms, `score` |
| `triage.py` / `analyse.py` | what the method can see, and whether the score tracks the titer |

Between the second and third steps the study tier is built and published:

    python build_references/run_benchmark_conditions_local.py --only study_tier
    cp -r data/scratch/bench_conditions_local/study_tier/aska_ffa data/fabfos/benchmarks/

`digitise_ffa.py` needs `pypdf` and Pillow (`figure-net` here); the rest need only
pandas. `run_panel.py` must run under the `ecspr` env — `./dev.sh --iecspr` builds
it, and `./dev.sh -e --where` says which package it imported.

## Things that would go wrong silently

- **Matching ASKA gene names against the model.** The roster and the paper use the
  old *rfa* nomenclature and iML1515 uses *waa*, so the headline hit `rfaY` is
  `waaY`/`b3625`. Twelve of the sixty ORFs — every *waa*, every *lpt*, and `gppA`,
  `nepI`, `opgD` — miss by name and resolve by synonym. Resolution goes through
  `NC_000913.3.gbk`'s locus tags, never through names.
- **Reading reach off `gpr_gem.parquet`'s `in_atom_universe`.** The study tier
  excludes TRANSPORT from the atom universe and the host GEM's column does not.
  The ASKA winners are largely transporters, so the host column counts exactly
  those as visible and overstates the ceiling by roughly a factor of two.
- **Counting units differently in the two arms.** `--weighting uniform` scores a
  reaction by how many distinct `unit_id`s nominate it. The study tier writes one
  `unit_id` per *study*, so a two-clone strain would add one unit while a size-2
  null draw adds two. `build_ecspr_tables.py` rewrites `unit_id` to the clone for
  exactly this reason.
- **Dropping a deletion by `mnxr`.** The ΔrfaY-complemented strain deletes the
  chromosomal copy and carries a plasmid one. A drop keyed on the reaction removes
  both; the drop is keyed on `intermediate_id`, which separates the host's row from
  the clone's. Both deleted genes are sole-gene reactions in iML1515, so it is exact.
- **A clone missing from the null pool.** `ecspr draw` samples the pool's `orf`
  values, so a clone whose ORF resolves to no reaction must still get
  a row with a null `mnxr`. Without it the null is made only of clones the model
  can see, which is a null for a different question.

## The tier is now one universe — it was two

`aska_ffa` was published on its own out of a **bake v2** run while the other seven
folders were still the **v1** ones, from before transport left the atom universe,
so their `in_atom_universe` columns were not comparable and a cross-study count read
two universes as one. `BUILD_studies.json`, which is a whole-tier record, said v1
and was true of seven eighths of the tree.

Rebuilding the seven was deliberately not done here — until moving `eydallin` onto
its real host forced a tier rebuild, which is all-or-nothing. That run put every
study on v2 and made the record true. **What moved is only the universe flags**: the
edges are identical study for study — same `(condition_id, orf, mnxr, action)`
multiset in every one — while `in_atom_universe` flipped on 34 of keio's 230 rows,
488 of laser's 3,257 and 2 of aromatic's 7. A number in a committed report that was
read off a v1 `in_atom_universe` is therefore stale; one read off an edge set is not.

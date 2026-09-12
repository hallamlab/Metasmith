# Curation round 5 — the bodies land, and every one of them is run

Round 4 landed 21 transforms under `src/metasmith_libraries/transforms/kbase/` with real
signatures and stub bodies: each touched its declared outputs and ran no tool. A plan
routing through one of them was a plan that produced empty files. This round writes those
bodies, settles the environment each runs inside, and — the part that makes the claim worth
anything — executes them.

## What this round did

- **Eighteen bodies written, twenty of twenty-one executed.** `runs.md` is the record: one
  row per transform, with what was checked in the product rather than whether the command
  exited zero. `build_tree/gtdbtk_tree.py` is the one not run, and why is in its own body.
  The three that already shipped were run too, because each depends on a binary that is not
  the tool it names — `gzip`, `unzip`, `tar` — and none of those had been confirmed present.
- **Eight of those bodies are an algorithm**, so the algorithm went under `resources/lib/`
  and the protocol became one `python <script> <args>` line, per the library's own rule.
  `lib::modelling/` is the largest: the MetaNetX and cobra helpers lifted out of
  `research/fabfos/benchmarks/laser/vs_gem/`, with their module-level absolute paths turned
  into arguments.
- **`cobra.env` gets an image.** It was the only one of ninety environments with no
  `container:` line at all, so the four modelling transforms could not run under DOCKER or
  APPTAINER at any point in rounds 3 and 4. `docker/cobra/` is built and verified locally
  and is not pushed; the env keeps `conda:` as its working route and the `container:` line
  goes in after the push.
- **The container inventory** is `inventory.md`, from `inventory.py`: ninety environments,
  eighty resolvable, 80 GB compressed, and a verdict on each merge candidate.

## What is in this directory

| file | what it holds |
|---|---|
| `runs.md` | every transform, what it produced, and what was checked in it |
| `fixtures.md` | the recipe for the inputs those runs were made against |
| `inventory.md` | the ninety environments, their images and sizes, and the merge analysis |
| `inventory.py` | regenerates `inventory.json` from the registry manifests |
| `plan_all.py` | solves the eleven shipped templates and prints each plan step by step |
| `run_one.sh` | runs one transform locally: `metasmith run` with `METASMITH_WORK_ROOT` pinned |
| `_plans_r4.txt`, `_plans_r5.txt` | the eleven templates' plans on both sides. `diff` them. |

## The regression, and what it caught

Both sides of the template comparison were solved with the same `plan_all.py`; the r4 side
from a `git archive` extract of round 4's commit, whose plans reproduce round 4's own
committed record exactly. The solver was confirmed deterministic first — two solves of one
tree agree — so a difference is a real change and not search noise.

That mattered, because there was one. **`provides` is a matched property set, not
documentation.** Adding `py/polars` to `python_for_data_science.env` — which is TRUE, polars
is in the 1.4.0 image — made that env a property-superset of `polars.env`, so every polars
requirement became satisfiable by it. Five of the eleven templates changed plan:
`isolate_assembly_from_long_reads` lost filtlong, flye and seqkit_reads; four others gained
annotation steps. Taking the line back out makes all eleven identical to round 4's again.

With that reverted: eleven templates step-for-step identical, eleven analyses solving with
the same steps, 40/40 on `check_transform_compatibility.py`, 27/27 on `test_type_hierarchy`,
32/32 on `test_env_portability`, and 54/54 over the three files that cover the one engine
change this round made (`test_direct_run.py`, `test_api_exit_codes.py`, `test_paths.py`).
The full `tests/metasmith` suite was NOT run to completion — it was still going at 25
minutes on a loaded host and is outside what this round touches.

The two analyses that had to be re-supplied are worth naming, because they are the same
failure twice: a mandatory requirement nothing produces takes the WHOLE plan down, not just
the target that needed it. `a1` dropped all five of its targets when the three MetaNetX
reference types arrived, and `a6` dropped all five when `clustering_params` did. Both solve
again once the probe defers those inputs, at exactly the step counts round 4 recorded.

## What running them found that nothing static would

Six defects, each of which passed every gate in the repository:

1. **`metasmith run` could not run any transform that calls `context.Output()`.** The
   lineage entry it built carried no `KEY`, and `member_token` refuses one without it, so
   every direct run died before the protocol produced anything. Nothing had exercised that
   path with a real body.
2. **`stringtie.env` pinned a tag quay no longer serves.** Not a stale image — a pull that
   fails outright.
3. **`interproscan.py`'s parse dropped InterProScan's own `Ontology_term` column**, which is
   the entire input to the enrichment transform round 4 declared.
4. **`HierarchicalCluster(metric="precomputed")` rejects `np.corrcoef` output**, which is
   symmetric only to floating-point rounding, and `squareform` tolerates no asymmetry at all.
5. **cobra builds its `Configuration` at import and that constructor makes a cache
   directory**, which fails as `$HOME=/` under a uid with no passwd entry — before a line of
   the entry point runs.
6. **`slim_optimize` returns `nan` for an infeasible model**, and `nan` compares False
   against every threshold, so an infeasible model read as one needing no gapfill.

And one result that is not a defect but is the most interesting thing here: **a gapfill over
an unfiltered MetaNetX universe restores growth with reactions no organism runs.** The MILP
minimises reaction COUNT, so one aggregate is always cheaper than the pathway it stands for;
the first run added `12 NADH + 3 succinate = 4 pyruvate + 12 NAD+`, a SABIO-RK lump MetaNetX
makes no balance claim about. Restricting the universe to the 44,168 reactions flagged `B`
(of 83,796) returned 2-oxoglutarate:ferredoxin oxidoreductase and citrate lyase instead, at a
higher objective.

## Decisions this round made that round 4 left open

**`cobra_fba` stays one transform.** Round 3 wanted `ecspr::conditions` optional; the
language has no optional requirement and round 4 made it mandatory, leaving the trade here.
Two transforms differing only in whether a table is read is worse than a template deferring a
one-row table. What the body does not do is invent a meaning for the ecspr mask columns:
`media` and `drop_*` carry over to a cobra model as a medium and a set of gene knockouts,
`background_*` and `mask_*` say how a model is BUILT rather than how a built one is solved,
and the body names the columns it ignored in its own output.

**`expression_clusters` takes a method knob as an input.** KBase ships four apps here —
hierarchical, k-means, WGCNA, and an estimate of k. That is one transform with a knob, so
`transcriptomics::clustering_params` is a requirement like any other. Hierarchical and
k-means are ported and `k: null` estimates k by silhouette; WGCNA is not ported, it is R, and
its output is a module assignment the two ported methods already produce.

**`bowtie2_align` and `polypolish` each take a second environment** rather than a mulled
container: two containers cannot share a pipe, and a mulled hash is a pin nobody can audit.
`env::bwa.env` is new.

**Three MetaNetX reference types are new.** `gem_from_gpr` had the id→MNXR bridge and no
stoichiometry to go with it — `mnxr_lookup` carries none — so `ref::mnx_reac_prop`,
`ref::mnx_chem_prop` and `ref::mnx_chem_xref` are declared, one per upstream file, the
granularity `fabfos::raw` already argues for.

## What this round did not close

- `transcriptomics::bowtie2_bam` does not extend `alignment::bam`, so the bam
  `align_reads/bowtie2_align` produces cannot be consumed by `qc_alignment/samtools_stats`
  or `call_variants/bcftools_variants`. Both were run against a bam supplied directly. This
  is a type question round 4 created and this round did not touch.
- `kraken_abundance` names its rows by the staged file's stem. Nothing in this library names
  a sample above a kraken2 report — `amplicon::survey` groups them but carries no per-sample
  identity the way `ncbi::genome_name` does above an assembly. This is round 4's open
  "no sample-level grouping type" finding wearing different clothes.
- Round 4's other two findings stand: `modelling::` and `metabolomics::` still do not meet,
  and `binning::derep_mag_ref` is still required by two transforms and produced by nothing.
- No `ifVirtualEnvDo` arm was added to any new body. Every run here was under DOCKER, and an
  arm that has never been run answers "can this tool run without a container?" wrongly.

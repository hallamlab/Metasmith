# Benchmark plan

## Context

Tony is designing five benchmark experiments for metasmith, and the design page records every decision so far. The design is mostly settled. The next phase writes the drivers for the runs in this folder, solves them without launching, and puts the solved DAGs on the page. Nothing launches until Tony signs off.

## What you said

> make a folder in ./research/metasmith_benchmark. this will be your workspace folder for driving this session, including the 5 runs

> get organized, then compact and write the drivers in metasmith_benchmark after with a fresh context

> use the write-docs skill to make a pass at tightening each card to only state what is relavent. make a pass to see what is decided and remove the issues that are solved and fill in or update teh DAGs

> for pratama, lets pre interleave the reads.

> dont add phiX. this can be a manual check if we need it. dont place it in the pipeline.

> E2 should match E1. E3 should match pratama. indicate which requires new transforms.

> we can fill in the reasons later, we wont know until E5 fully runs. these are just the initial perspective.

## Issues

- I1. The page cards carry history, solved issues and repeated detail, so a reader cannot see what is decided and what is open.
- I2. No driver exists for the experiments as now designed. The existing drivers (`research/cami/run_cami_metag.py`, `research/metagem/run_metagem.py`) encode the older shapes, and the research-support session edits them on another branch.
- I3. The E2, E3 and E4 DAGs show the as-launched shapes, not the decided designs, and E5 has no DAG.
- I4. Pratama reads are registered as zipped pairs, so every plan carries an interleave step.

## High-level goals

- G1. Make the page state only what is decided, what is open, and what blocks a run.
- G2. Have one driver per experiment in this folder that solves the decided design.
- G3. Show each experiment's solved workflow on the page.

## Acceptance criteria

- Each card lists no resolved item, and each open item names what it blocks.
- E5 carries no phiX step. The page mentions phiX only as a manual check.
- The page states the GTDB r232 package as 56.6 GiB, not 179 GB.
- `research/metasmith_benchmark/drivers/` holds a driver per metasmith experiment (E2, E3, E4, E5 pilot). Each solves with `task.ok` true, or reports its dropped targets and the transforms still missing.
- No driver launches or stages to fir without Tony's sign-off.
- The E3 driver registers the pre-interleaved reads directly, with no interleave step in the plan.
- The E4 driver starts from metaGEM's published MAGs, with Prodigal as the first step.
- Each solvable driver's DAG is rendered to `page/dags/` and shown on its card. Each card whose design needs missing transforms says which ones.
- The page is published to the existing artifact URL, and the folder is committed on `feat/engine/bench-run`.

## Tasks

- T1. Tighten every card and remove solved issues (G1)
- T2. Write the E3 Pratama driver on pre-interleaved reads (G2, G3)
- T3. Write the E4 metaGEM driver from published MAGs (G2, G3)
- T4. Write the E2 CAMI parity driver, short and long arms (G2, G3)
- T5. Compact
- T6. Write the E5 pilot driver (G2, G3)
- T7. Render the DAGs onto the page and publish (G3)
- T8. Commit the workspace
- T9. Debrief

## Approach by task

### T1. Tighten every card

Edit `page/experiments.src.html` under the write-docs rules: one topic per bullet, dense declarative sentences, no session history. Per card, keep goal, datasets, driver, targets, then a status block split into proven, open and blocking. Drop items the decisions below resolved. Move measurements that only support a decision into `findings/`.

Gotchas: `page/tool_table.py` owns the E5 table, so edit tool rows there, not in the HTML. The table intro and the E3 answer box also carry decisions, so check them too.

### T2. E3 driver

Build `drivers/e3_pratama.py` from `build_inputs_pratama`, `build_targets_pratama` and `build_globals_pratama` in `research/cami/run_cami_metag.py`. Import what is reusable rather than copying. Register each run's interleaved file as the short-read type CAMI uses, parented to its `read_metadata`, under the shared `viromics::contig_study` root. Solve with a local dry-run agent and render `task.plan.RenderDAG`. Keep the six Nanopore runs out until the hybrid metaSPAdes transform exists, and say so on the card.

Gotchas: the research agent produces the interleaved files. Take their paths from its report, not from a guess. `enumerate_pratama_runs` needs ssh to fir even for a dry run. Stable instance ids must not embed a checkout path, or plan keys differ between checkouts. Never pass the whole inputs library as a resource, since that silently collapses solver cases.

### T3. E4 driver

Build `drivers/e4_metagem.py`. Register the published MAG FASTAs as the bin type that `prodigal_from_bin` consumes, then target CarveMe models (CPLEX lane) and MEMOTE scores. SMETANA has no transform, so the card lists it as missing. The CarveMe gapfill duration must be 12 h (B11) before any launch.

Gotchas: Tara's archives use their own file names, so read `research/metagem/cluster/published_manifest.tsv` rather than globbing. `run_metagem.py`'s `MEDIUM_TSV` embeds an absolute path, which breaks key portability.

### T4. E2 driver

Build `drivers/e2_cami.py` with a short arm and a long arm, each solved separately, because mixing read shapes in one solve collapses cases. Solve what the library supports today. List the transforms that parity with E1 still needs: fastp in place of bbduk, FastQC, bowtie2 for binning coverage, Porechop ABI, Chopper, and the Flye preset fix (B12). phiX removal stays in this list for parity only.

Gotchas: the long arm needs `build_targets_long_read`'s separate resource library for reads and metadata. SemiBin2 needs `--sequencing-type` from `length_class` (B9).

### T5. Compact

Handoff: the drivers from T2–T4 are on disk in `drivers/`, with their DAG SVGs in `page/dags/`. The todo list shows which tasks are done. Carry the peer's latest reports (interleave paths, Flye `--nano-raw` result, GTDB staging) in the follow-up.

### T6. E5 pilot driver

Build `drivers/e5_pilot.py` over 9 samples: 3 CAMI, 3 Pratama, 3 metaGEM. Use one plan per corpus, because one plan takes one target list for every sample. Solve with the tools that exist and list the missing transforms. The design includes two MAG refiners (DAS Tool, MAGScoT) and two dereplicators (dRep, skANI dedup). Each dereplicator runs on each refiner's bins, per study, which gives 4 MAG sets. AMBER and skANI recovery run after the run, outside the plan.

Gotchas: each dereplication target needs its refiner as a target parent, or the solver binds one refiner to both. A study grouping type does not exist yet. `viromics::contig_study` covers only the viral merge.

### T7. DAGs and publish

Copy each solved DAG SVG into `page/dags/`, reference it from its card, then build and publish.

Gotchas: pass the artifact URL as `url`, because the file path changed from the job temp folder.

### T8. Commit

Commit `research/metasmith_benchmark/` on `feat/engine/bench-run`. Do not commit `page/experiments.html`.

### T9. Debrief

Run the `debrief` skill.

## Decisions taken

- **E1:** unchanged. nf-core/mag 5.5.0 on 249 CAMI samples, using long reads where CAMI has real Nanopore-like simulations (41) and short reads elsewhere (208).
- **E2:** matches E1 tool for tool.
- **E3:** matches Pratama, with the six Nanopore runs as 17 per-run hybrid assemblies.
  - **Decided parity differences:** no Guppy step, because SRA holds the basecalled FASTQ. GTDB-Tk r232 instead of r202.
  - **CCTyper:** dropped. It is in none of the reference studies.
- **E4:** starts from metaGEM's published MAGs. The reads serve E5 only.
- **E5:** starts with a 9-sample pilot, and its tool list is provisional until E1–E4 report.
  - **Dropped:** phiX removal, which stays a manual check.
  - **Tools:** bbduk, seqkit for QC, MEGAHIT and Flye, Chopper, minimap2 with a coverage step, and CheckM2 only.
  - **MAG sets:** 4, from DAS Tool and MAGScoT crossed with dRep and skANI dedup, per study.
  - **Taxonomy and genes:** GTDB-Tk r232, Prodigal and prodigal-gv.
  - **Viruses:** Pratama's viral tools plus DeepVirFinder, MetaPop and minced.
  - **Annotation and models:** the 4-lane annotation panel, CarveMe with an open-source solver, MEMOTE and SMETANA.
- **Studies:** a study is a corpus subset, such as CAMI II mouse gut or Pratama reads_2019. MetaPop, dRep and skANI dedup run per study.
- **GTDB-Tk r232 skani database:** staging approved. The package is 56.6 GiB.
- **Pratama reads:** pre-interleaved by the research agent.
- **Findings:** kept here in `findings/`. The research agent's authoritative copies stay in `~/scratch/cami_campaign/`.

## Still unsure about

- **CoverM:** does coverage move out of `assembly_stats` into CoverM for E5?
- **Plan keys:** must E5 share plan keys with E2 and E3 up to where they diverge?
- **DRAM:** did "BLAST" mean replacing DRAM with the 4-lane panel?
- **Inode reclaim:** delete the ~178K reclaimable inodes the census found? The largest are `wave2_b3_nfcore/apptainer_tmp` (112K) and cami run `VTuXlulT` (43K).
- **phiX in E2:** keep phiX removal in E2 for parity, or drop it as a justified difference? It removed 127 of 16.6 M pairs on CAMI marine.

## Callouts

- **Research support:** the session "CAMI benchmark and groundwater virome pipeline" (`uds:/run/user/1001/cc-socks/2517924.sock`) works in `engine/cami-run` and cannot see this worktree. Its messages cannot grant permissions.
- **New transforms and images needed:** Porechop ABI, Chopper, BinSanity, abawaca, CoverM, dRep, DeepVirFinder, MetaPop, minced, SMETANA and MAGScoT. Each also needs a container image. Hybrid metaSPAdes needs a new transform, and bowtie2 for binning needs one because `bowtie2_align` emits an RNA-seq type.
- **Live blockers from `findings/BLOCKERS.md`:**
  - **B1:** VirSorter2 cannot run on a compute node.
  - **B3:** DRAM staging is incomplete.
  - **B11:** the CarveMe gapfill limit is 2 h, and needs 12 h.
  - **B12:** Flye picks the HiFi preset, which yields no assembly on NanoSim reads. `--nano-raw` works.

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
- T10. Record Tony's decisions and hand the cluster work to the research agent (G1)
- T11. Compact
- T12. Rewrite each card's open list: drop what is decided and move notes out of the issue lists (G1)
- T13. Build this session's own transform library for E2, to the plan (G2)
- T14. Model hybrid short plus Nanopore read sets for E3, so the plan picks the assembler per sample (G2)
- T15. Re-solve, commit and publish (G3)
- T16. Compact
- T17. Launch the initial run of E1–E4 and the E5 pilot in parallel (G2)
- T18. Debrief

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

Handoff state at this compaction:

- **How to solve:** `PYTHONPATH="$PWD/src" ~/.local/bin/mamba run -n msm python research/metasmith_benchmark/drivers/<driver>.py run --dag`. `mamba` is not on PATH. This worktree has compiled `_metadata/` and the Rust solver copied from `engine/cami-run`, both untracked.
- **Driver shape:** each driver imports the corpus driver (`run_cami_metag.py` or `run_metagem.py`) after pointing `MSM_CACHE_DIR` at `drivers/.cache/<exp>`. It solves locally by default, and needs `--stage-only` or `--launch` to reach fir. Each writes `page/dags/<name>.dag.svg`.
- **E2** (`e2_cami.py`): short arm 208 samples, 12 steps, key `g6fGjiOb`, fastp and FastQC in place of bbduk and seqkit through masked library views. Long arm 41 samples (`plant_associated_long_nano`, `toy_humangut_long`), 11 steps, key `2gIF6Dea`, `flye_raw` at 128 GB. Still missing for parity: bowtie2 BAM transform, Porechop ABI, Chopper, the Flye preset fix in `flye_raw.py` (it picks HiFi when mean quality ≥ 20), and the phiX decision.
- **E3** (`e3_pratama.py`): 66 runs pooled under one study root, 21 steps, key `rG10GEFE`. Reads register as study → read_metadata → read_pair → `short_reads_pe`, which keeps `merge_candidate_calls`' read_pair lineage and removes the interleave step. Targets drop the SemiBin2/COMEBin/skANI chain and `spacer_host_links`, because only CCTyper produces `crispr_spacers` until minced exists. MAG recovery is out of the plan: dRep has no transform, and the B7 MetaWRAP recovery transforms exist only uncommitted in `engine/cami-run`. The plan still carries `downloadVibrantDB` and `downloadCheckvDB`, which hang on a compute-node driver. Staging refuses until every `interleaved/<dataset>/<run>.ok` stamp exists.
- **E4** (`e4_metagem.py`): 14,108 published MAGs registered as `sequences::bin_fasta`, 3 steps (prodigal_from_bin, carveme_from_orfs_cplex, memote_score), key `5tRUYUnb`. `drivers/e4_published_mags.tsv` lists the archive members, read from fir. `e4_extract_mags.sh` unpacks them to `published/<study>/mags/<mag>.fa` and has not run. Published GEMs pair with MAGs by name, and Tara names bins `ERR….bin.N.strict` against GEM `ERR…_bin.N.s`. SMETANA is missing, and karlsson2013 publishes `SMETANA.tar.gz`.
- **Peer reports:** interleave array 59607969 writes `/scratch/phyberos/pratama2026/interleaved/<dataset>/<run>.fastq.gz`, gated on read count, with a final table to follow. GTDB r232 download stopped on Tony's instruction. The skani database is on neither fir scratch nor project space, so look on Globus (chinook). phiX read counts (job 59607067) are pending.
- **Page:** v26 published with the tightened cards. The cards still name the old DAG keys (C1IM6IG3, XNG5FppS, HQ5SrqFe) and say "no driver yet" for E5. T7 swaps in the new DAGs and driver facts.

### T6. E5 pilot driver

Build `drivers/e5_pilot.py` over 9 samples: 3 CAMI, 3 Pratama, 3 metaGEM. Use one plan per corpus, because one plan takes one target list for every sample. Solve with the tools that exist and list the missing transforms. The design includes two MAG refiners (DAS Tool, MAGScoT) and two dereplicators (dRep, skANI dedup). Each dereplicator runs on each refiner's bins, per study, which gives 4 MAG sets. AMBER and skANI recovery run after the run, outside the plan.

Gotchas: each dereplication target needs its refiner as a target parent, or the solver binds one refiner to both. A study grouping type does not exist yet. `viromics::contig_study` covers only the viral merge.

Result: pilots are toy_mousegut, Pratama reads_2019 and metaGEM li2019, each registered as study → read_metadata → read_pair → reads. CAMI solves to 45 steps (`ajfZ1MhZ`), Pratama to 45 (`jxG1gehK`), metaGEM to 46 (`FwvpcoFM`). MetaWRAP is masked out of the metagenomics library, because GTDB-Tk de novo's bare `putative_genome` slot otherwise pulls it in. `skani_dedup` runs on the aggregator's pool, which iPHoP needs. The 4-lane panel requires whole-assembly `sequences::orfs`, so it cannot read `bin_orfs` yet.

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
- **GTDB-Tk r232 skani database:** no re-download. Take it from project space or Globus. Found on chinook's Globus collection (`/Resources/reference_databases_for_tools/GTDB/gtdbtk_r232_data.tar.gz`, no .md5 sibling). The research agent is transferring the tarball to fir, unextracted, because `skani/` is about 113K inodes.
- **Pratama reads:** pre-interleaved by the research agent.
- **iPHoP in E5:** kept. The database cost is minimal.
- **phiX:** disable it in E1 and E2 if nf-core/mag allows (`--keep_phix`). No phiX step anywhere.
- **Coverage:** no CoverM. `assembly_stats` (minimap2 plus genomecov) gives per-base coverage.
- **Plan keys:** E5 need not share keys with E2 and E3 for the design. Shared keys are still wanted, to cut runtime.
- **E5 annotation:** the 4-lane panel replaces DRAM.
- **E5 spacers:** BLASTn spacers-to-contigs is dropped. minced in E5 only fed that step, so T12 confirms whether minced goes too.
- **Inode reclaim:** the research agent cleans up what its runs left.
- **E4 extraction:** approved, about 14K inodes.
- **E1:** Flye is tested. Drivers run on compute nodes where possible (`METASMITH_DRIVER_SLURM=1` for metasmith drivers).
- **Initial run:** E1–E4 plus the E5 pilot, in parallel, chunked only if inodes force it.
- **E2 library:** `library/transforms/e2` holds 16 transforms in its own `e2` namespace, so no standard-library transform binds. Commands, arguments and image tags come from nf-core/mag 5.5.0's module files. `library/build.sh` compiles it. E1's QUAST, NanoPlot and standalone Prodigal stay out, because nothing scored reads them.
- **E1 phiX:** `keep_phix = true` in `research/cami/nfcore/control.config`, mirrored from the research branch's commit 38068556. E2 has no phiX step, so the change is one-sided.
- **E1 head:** `sbatch` a script running nextflow, 10 cpus / 48 GB, 7-day wall. Tear down by killing the nextflow PID, never `scancel`, which orphans the grid jobs.
- **E4 chunks:** about 2,000 MAGs per chunk (7 chunks), pruning each chunk's `nxf_work` after promotion. No E4 transform batches, so the corpus as one run needs ~635K inodes against ~590K free. The other four fit together at ~212K.
- **Findings:** kept here in `findings/`. The research agent's authoritative copies stay in `~/scratch/cami_campaign/`.

## Next phase (T10–T18)

- **T12:** an open item names something that blocks a run or a result. A fact to remember is a note, not an issue. Remove each item a decision below settles. Leave E5's open decisions for after the initial runs.
- **T13:** write the transforms the E2 plan names, in a library under this folder. Do not bind a library transform just because its types fit. The list: fastp, FastQC, MEGAHIT, bowtie2 binning BAM, MetaBAT2, SemiBin2, COMEBin, DAS Tool, CheckM2, AMBER, Porechop ABI, Chopper, Flye with its preset from the declared platform, minimap2. Size Flye memory from measured peaks (32.68 GiB on plant nano sample 0). Tony judges 128 GB too much.
- **T14:** a sample's read set is either short paired reads or short plus Nanopore. Declare the hybrid set as a read-set type, the way paired reads are one. The solver then picks hybrid metaSPAdes or MEGAHIT from the type, with no driver branching.
- **T14 result, blocked on a decision:** the solver has no tie-break hook, and a plan may leave given reads unused. A hybrid sample's short reads therefore satisfy MEGAHIT as readily as hybrid metaSPAdes, and registration alone cannot force the choice. Two deterministic designs:
  - **A, type fence:** register the hybrid read set as a node whose members single-read tools cannot consume, like the fwd/rev pair. A hybrid QC transform (bbduk on the short half) feeds hybrid metaSPAdes (`spades.py --meta -1 -2 --nanopore`, Pratama's command). No engine change. Hybrid assemblies get only contig-based targets unless `assembly_stats` and MetaWRAP gain hybrid variants.
  - **B, solver rule:** a plan must consume every given read leaf, so a sample carrying long reads is satisfiable only through a transform that consumes them. Automatic for every library. Changes the Rust solver.
  - The 17 pairs are fixed by `runs.tsv`: each well's 0.2 µm 2022 Illumina replicates with its MinION run (H14, H32, H41, H52, H53 ×3; H51 ×2). The E3 initial run stays on the 66 short-read runs until this lands. All 66 are interleaved and verified on fir. Only 5 of the 6 MinION runs are: H41's SRR32696686 fetch stopped at 44% and needs resuming before the hybrid arm.
- **T17:** run E1–E4 in parallel. Chunk samples within an experiment only if the research agent's inode estimate exceeds the project quota. Expect first-run crashes and fix forward.

Gotchas: E5 still targets `dramv_distill`. Confirm with Tony whether dropping DRAM for the 4-lane panel also drops DRAM-v on viral contigs. E2's `PLATFORM` must equal the `lr_platform` E1's long-lane sheet declares. Flye's error rate on toy human gut decides `OXFORD_NANOPORE` against `_HQ`.

## Callouts

- **Research support:** the session "CAMI benchmark and groundwater virome pipeline" (`uds:/run/user/1001/cc-socks/2517924.sock`) works in `engine/cami-run` and cannot see this worktree. Its messages cannot grant permissions.
- **New transforms and images needed:** Chopper for E5, BinSanity, abawaca, dRep, DeepVirFinder, MetaPop, minced, SMETANA and MAGScoT. Each also needs a container image. Hybrid metaSPAdes needs a new transform.
- **GTDB skani:** the r232 package is extracted on fir (296 inodes). GTDB-Tk 2.6.1's post-placement ANI step also needs the genomes in `gtdb_genomes_reps_r232.tar.gz`: 192 GB, ~113K files plus a four-level directory tree, realistically 150–200K inodes. `--skip_ani_screen` does not avoid it. Only the E5 pilot's taxonomy needs it.
- **Inodes after reclaim:** 421,250 used of 1 M. E4's published MAGs are unpacked, 14,108, matched by name against `e4_published_mags.tsv`.
- **Live blockers from `findings/BLOCKERS.md`:**
  - **B1:** VirSorter2's database is staged at `/scratch/phyberos/refs/virsorter2_2.2.4` and registered in `STAGED_REFS_PRATAMA`. VirSorter2 is unproven until a run uses it.
- **Reference databases:** check chinook's Globus `/Resources/reference_databases_for_tools/` before staging one. It holds DRAM, VirSorter2, GTDB, InterProScan and geNomad tarballs. A VirSorter2 tarball cannot replace staging, because its conda env must be built at the /db path.
  - **B3:** closed. DRAM's five distillation sheets are staged and verified by content. dbcan is absent because its URL is dead, so DRAM-v gives no CAZyme annotations. `dramv` stays unproven until a run re-runs it.
  - **B11:** the CarveMe gapfill limit is 2 h, and needs 12 h.
  - **B12:** Flye picks the HiFi preset, which yields no assembly on NanoSim reads. `--nano-raw` works.
    - **Measured `--nano-raw` run** on plant nano sample 0: 1 h 06 m, 3,186 contigs, N50 96,428, peak memory 32.68 GiB.
    - **Memory limit:** the E2 library's `flye.py` declares 64 GB until three more peaks land (array 59610638).
- **Quota:** only the project quota (`lfs quota -p 83115734 /scratch`, or `diskusage_report`) enforces the 1 M inode limit. The group and user figures count files anywhere on `/scratch` and carry no limit.

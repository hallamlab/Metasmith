# CAPABILITY INVENTORY — what is proven, by what evidence

Research-support handoff for the agent writing the full-run drivers. The campaign pivoted
2026-09-12 from launching full-corpus runs to proving transforms, installs and
implementations first.

**Read the status column literally.** This campaign has been burned four times by treating
"we never saw it fail" as evidence, so there are three distinct statuses and they are not
interchangeable:

| status | means |
|---|---|
| **OBSERVED** | executed on real data and its product was inspected |
| **SOURCE** | read from the tool's or pipeline's own source/config; never executed |
| **UNPROVEN** | neither — do not build a driver that assumes it |

A tool that exits 0 having produced nothing is the normal failure here, so OBSERVED always
means the product was checked, never the exit code.

---

## metasmith side — transforms

| step | status | evidence |
|---|---|---|
| `seqkit_reads`, `bbduk`, `megahit`, `prodigal`, `assembly_stats` | **OBSERVED** | real CAMI sample; megahit 34m31s, per-task work dirs 12-15 files |
| `interleave_zipped_short_reads` | **OBSERVED** | Pratama + metaGEM arms |
| `cami_contig_truth` (the gold-standard bridge) | **OBSERVED** | 128,677-byte table produced; absent from `results/` BY DESIGN — see the caution below |
| `metabat2` | **OBSERVED, two scales** | CAMI marine_sample_0: 51 bins at `--minContig 1500`, exit 0, 1m26s. **Pratama groundwater run: 146 bins, exit 0** — a real-soil sample recovers ~3x the bins of a CAMI one |
| `semibin2` | **OBSERVED, two scales** | CAMI marine_sample_0: 57 bins, exit 0. **Pratama groundwater run: 209 bins, exit 0**, ~10m per task |
| `comebin` | **UNPROVEN** | never completed. Job 59548383 has been `PENDING (Resources)` at 48 cpus since 16:35 — queued, not stalled. Criterion 9's peak RSS has no number. A Pratama-arm COMEBin IS running healthily at 48 cpus in a different agent home, which will give a 48-cpu data point but NOT criterion 9's, since that names a CAMI sample |
| `das_tool` | **UNPROVEN** | waits on comebin |
| `amber` / `amber_das_tool` | **OBSERVED** (per-binner) | MetaBAT2 row: `precision_avg_bp 0.8984`, `recall_avg_bp 0.0775`, `f1_score_bp 0.1428`, `ARI_bp 0.4581`. MAG-level row unproven |
| `checkm` (CheckM2) | **OBSERVED** | ran TWICE in rung 1, `CheckM2 finished successfully`, real per-bin CSVs inspected: 96.8/9.54, 11.0/0.0, 35.75/5.13 completeness/contamination, `Completeness_Model_Used = Neural Network (Specific Model)`. Product inspected, not the exit code — `checkm.py` ends in `|| true` so it cannot fail its step |
| `vibrant` | **OBSERVED** | Pratama run, exit 0, two instances, four TSVs each at 75 KB–370 KB. Products inspected, not counted |
| `genomad` | **OBSERVED** | Pratama run, exit 0, two instances, five TSVs each up to 5.0 MB |
| `splitContigsForAmr` | **OBSERVED** | exit 0, two 260 MB `.fna` shards |
| `metawrap` | **OBSERVED** | Pratama run, exit 0, **92 bin FASTAs** + 2 tables; largest bin 5.6 MB with 1,151 contigs. That is a real refined bin set for one of 66 runs — it was E3's stated blocker on the MAG comparison |
| `merge_candidate_calls` | **OBSERVED** | exit 0, 2 products, largest 58.8 MB |
| `prodigal_gv` | **OBSERVED** | exit 0, 2 products, 35.3 MB |
| `mmseqs_votu`, `mmseqs_precluster` | **OBSERVED** | exit 0, ~1.97 MB each |
| `contig_length_table` | **OBSERVED** | exit 0, 1.13 MB |
| `pratama_votu_recovery` | **OBSERVED** | exit 0, **65,640-row skANI table**, header `Ref_file Query_file ANI Align_fraction_ref Align_fraction_query Ref_name Query_name`. This is criterion 2's vOTU recovery comparison against Pratama's published catalogue — the campaign's FIRST real comparison output. CAVEAT: one rung-1 sample, and the catalogue behind it is missing VirSorter2's contribution (that step failed), so it is a two-caller merge rather than the intended three |
| `dramv` | **BROKEN, cause confirmed** | exit 1 on three attempts, zero products. `Error: Invalid file path or buffer object type: <class 'NoneType'>` — DRAM-v handing a null config entry to a reader. This is the all-null `DRAM.config` manifesting ONE STEP LATER than its cause, which is exactly why that config must be verified by content. Unblocks when B3's staging completes |
| `checkv` | **OBSERVED** | Pratama run, exit 0, 7m08s, 4 TSVs; `quality_summary.tsv` 13,752 rows with the real header (`contig_id, checkv_quality, miuvig_quality, completeness, ...`) |
| `cctyper`, `blast_spacers_to_contigs`, `skani_dedup`, `aggregator`, `derep_mag_reference`, `pratama_mag_recovery` | **UNPROVEN** | not yet reached — all downstream of the per-binner CheckM2 fan-out still running |
| `vcontact3` | **UNPROVEN, and unreachable IN THIS RUN** | its database twin failed and was swallowed, so the step has no task dir and never will here. The database is now staged and pruned, so a relaunch reaches it |
| `virsorter2` | **BROKEN, diagnosed** | see Installs; fails on a compute node for a structural reason |
| `vcontact3` | **UNPROVEN** | was unreachable because its database step failed; now unblocked |
| `carveme_from_orfs_cplex` | **OBSERVED AT CORPUS SCALE** | **131 models, ZERO failures**, 17 still running, in the live li2019 run. 129 carry an `.xml` >100 KB; a sampled model is 6.1 MB SBML with **1,968 reactions and 1,376 species** — a genuine genome-scale model, not a stub. Earlier single-bin probe: draft 2m25s, gap fill 32m00s. The OPEN solver does NOT finish gap fill on the smallest bin (1h49m, killed), so CPLEX is a feasibility constraint rather than a preference. **CAUTION the declared `Duration(hours=2)` is TOO SHORT: measured over 100 gapfill tasks, min 131s / p50 380s / p90 1187s, and 5 of 100 hit the 2h wall (`ExitCode 140:0`, `Elapsed 01:59:1x`). The distribution is right-skewed with an unbounded tail; the two earlier sampled points (smallest bin 32m, largest bin 39s) bracketed nothing. Recommend 12h (fir band 2). The loss is BIASED — it drops whichever MAGs are hardest to gapfill, under retry-then-ignore, while the run reports complete.** |
| `memote_score` | **OBSERVED, fix PROVEN** | Failed ~100 tasks because cobrapy writes `$HOME/.cache/cobrapy` on first use and `$HOME` is READ-ONLY in the container (`[Errno 30] Read-only file system: '/home/phyberos'`). Fixed with `export HOME="$PWD"` — the third instance of a fix already in `downloadVirsorter2DB.py` and `virsorter2.py`. **Proven on a real 6.1 MB model**: job 59593952, 1m03s, all three products written, `score.json` carrying genuine section scores (consistency 0.894, annotation_met 0.797, annotation_rxn 0.766). Scoring is CHEAP — about a minute per model |
| `prodigal_from_bin` | **OBSERVED** | 44 ORFs from a bin FASTA, via direct run |
| `metawrap_skani_dedup` | **OBSERVED 2026-09-12, direct run, three cases** | executed via `metasmith run` direct mode against the live Pratama run's **92 real MetaWRAP bins**, 33s, skani exit 0, well-formed 92-row cluster table. **Case 1 (92 real bins)** proves the skani call, the header-based TSV parse, the schema and the singleton path — but merged NOTHING and could not: only 4 skani pairs, max ANI 88.13, all below the 95 threshold. That is correct (MetaWRAP's bin_refinement already dereplicates internally) and it means the merge path was untested. **Case 2 (92 + 3 exact duplicates)** proves union-find structurally: 95 bins -> 92 clusters, each pair exactly 2 members and 1 centroid. Still could not prove the medoid arithmetic, because an exact duplicate's computed mean ANI is 100.000, which COLLIDES with the singleton default. **Case 3 (one bin + 0.5% and 2.0% diverged copies)** closes it: skani measured A-B 99.48, A-C 97.41, B-C 96.72; the transform wrote 98.445 / 98.100 / 97.065, each exactly the mean of that member's two pairwise ANIs, medoid = highest mean = A. The 95 and 99 thresholds behave independently — at 99 the cluster correctly SPLITS into {A,B} and {C}. Since this transform is `skani_dedup.py`'s protocol unchanged but for the requirement and product types, the same clustering logic is validated for that transform too |
| `metawrap_derep_mag_reference` | **OBSERVED 2026-09-12, direct run** | executed against the real cluster table and the 92 real MetaWRAP bins, 1m41s, verified by product: `mag_ref.fna` 99.4 MB with **15,709** headers matching its own reported count and namespaced `bin_id~contig`; `mag_ref.stb` 15,709 rows over **92 distinct bin_ids** = the 92 centroids; the fna and stb scaffold sets are **IDENTICAL**, so the namespacing round-trips; `mag_ref.mmi` 307 MB with magic bytes `MMI\x02`, a real minimap2 index. 97,297,182 bases. Like skani_dedup it never opens the assembly, so a placeholder input is legitimate |
| `pratama_metawrap_mag_recovery` | **OBSERVED 2026-09-12 — THE CAMPAIGN'S FIRST MAG COMPARISON AGAINST PUBLISHED DATA** | 34s, skani exit 0, against the **real 1,275-MAG Zenodo archive** (17897233) and the real `metawrap_derep_mag_ref` above — which closes the transform's own docstring caveat that it had never been run against either. **132 alignments: 71 of our 92 MAGs matched, 122 of Pratama's 1,275, and 65 of the 132 are high-confidence (ANI>=99, align fraction >=50 both sides).** ANI range 85.52-100.00. **Sanity signal that a broken join could not produce:** our bins are from ERR3858110 = well **H52_1**, and the matched published MAGs are led by **H52 (49)**, ahead of H41 29, H32 19, H53 18, H51 15, H43 2 — the comparison preferentially recovers the same well's genomes. CAVEAT this is ONE run of 66; it is not the study-level recovery figure |

---

## nf-core/mag 5.5.0 side

| process | status | evidence |
|---|---|---|
| FASTQC (raw + trimmed), FASTP, PhiX removal | **OBSERVED** | phiX measured a NO-OP on CAMI: 127 of 16,647,376 pairs |
| MEGAHIT | **OBSERVED** | 2h28m58s on marine_sample_0 |
| QUAST | **OBSERVED** | per assembly, two tasks for two assemblies — not per bin |
| BOWTIE2 binning prep | **OBSERVED** | produces the self-mapped BAM; `binning_map_mode = 'own'` confirmed by 400,744 of 400,744 BAM refs matching the assembly |
| PRODIGAL | **OBSERVED** | |
| MetaBAT2 | **OBSERVED** | 50 bins on marine_sample_0 at floor 1500 |
| SemiBin2 | **OBSERVED** | exit 0 |
| COMEBin | **partially** | ran 36 min healthily, then cancelled by a wrapper timeout. Did NOT reproduce the bundled-test-data crash on real data |
| contig-to-bin map | **SOURCE** | `mag.nf:467`, `collectFile` + `storeDir`, unconditional, one four-column table for every binner incl. DAS Tool. Directory exists, file has not gathered |
| DAS Tool refinement | **OBSERVED (failure path)** | `No bins with bin-score >0.5 found`, exit 1, and the run failed LOUDLY rather than emptying downstream |
| CheckM2 | **SOURCE** | pinned in place of BUSCO; never executed ON THIS SIDE. It IS now observed in the metasmith arm, but that is a different invocation and does not transfer |
| **Flye** | **EXECUTED 2026-09-12, first time ever** | image `docker://staphb/flye:2.9.6` pulled to the shared cache (80,162,816 B, 2m03s on login3; it was NOT among the 136 cached images). Prior art is a CONFIRMED negative on both hosts: zero `.command.sh`, zero staged `workflow.nf`, zero `sacct` rows. **DEFECT FOUND ON FIRST RUN: `flye_raw.py` picks its preset from `seqkit AvgQual`, and CAMI's NanoSim reads carry a FLAT SYNTHETIC Q40 — every quality character is `I` — so the `q>=20` branch fires and the whole CAMI long arm would assemble NANOPORE data with `--pacbio-hifi --read-error 0.000501`.** **A/B MEASURED: the transform's own preset choice produces NO ASSEMBLY.** `asis` (`--pacbio-hifi --read-error 0.000501`) FAILED at 9m43s with Flye's own `No disjointigs were assembled - please check if the read type and genome size parameters are correct`; `--nano-raw` ran healthily past the overlap stage. So the CAMI long arm would have failed every Flye step and reported complete. Caveat: the arm varied preset AND `--read-error` together; job 59601518 separates them. Plant nano sample_0 = 3,105,598 reads / 5.01 Gbp / read N50 2,308, at 8 cpus (the transform's own declared count) / 128 GB. Argument ORDER is load-bearing: `--pacbio-hifi` is `nargs='+'` and greedily eats the reads path, so `--read-error` must precede the preset — the transform gets this right, my first test script did not and flye exited 2 in 27s |
| CAT, GTDB-Tk, Prokka, ALE, BUSCO | **confirmed NOT running** | CAT is gated at `mag.nf:517` on `cat_db \|\| cat_db_generate`, both unset — it appeared in an earlier process list only as a `-profile test` artifact |

---

## Installs, staging and infrastructure

| thing | status | notes |
|---|---|---|
| Nextflow 26.04.6 | **OBSERVED** | private at `~/bin/nextflow26`, `NXF_HOME=$HOME/.nextflow26`. mag 5.5.0 declares `!>=26.04.0`; fir's newest module is 25.04.6 and dies on a missing nf-schema 2.7.2 |
| container cache | **OBSERVED** | 14/14 CAMI, 21/21 Pratama, 11/11 metaGEM images present AND `.verified`. Derive the per-arm list from a staged run's own `workflow.env.json`, never from a hand-maintained list |
| CPLEX 22.2.0 | **OBSERVED** | uncapped solve, 2000 vars / 1999 constraints, in-container, on a compute node, from durable project space. `docplex config --upgrade` is what copies the full runtime — the packaged module alone caps at 1000/1000 with error 1016 and no warning |
| GTDB r232 | staged, **lane severed** | `--skip_ani_screen` does NOT skip the post-placement ANI step in 2.6.1 (the guard is commented out upstream), so it dies at 33m44s AFTER all the expensive work while the Slurm job exits 0:0 |
| DRAM 1.5.0 | **staging, unverified** | **verify BY CONTENT**: `prepare_databases` copies an all-null `DRAM.config` as its FIRST action, so an all-null config is the signature of a step that ran and achieved nothing. The five `*_form`/`*_database` sheets are what `DRAM-v distill` cannot run without |
| VirSorter2 DB | **BROKEN, fix written** | snakemake names its conda env `md5(realpath(conda_prefix)+yaml)[:8]`, so the env name depends on the MOUNT PATH: setup builds it in a per-run work dir, the consumer always binds `/db`, and they can never agree. Build the env under a `/db` bind. `--use-conda-off` is not an escape hatch — the image's own env has no sklearn/pandas/numpy/screed/prodigal/hmmsearch |
| vConTACT3 DB | **OBSERVED, staged** | `/scratch/phyberos/refs/vcontact3_v230`, pruned of upstream mmseqs build scratch: 6,925 → 1,057 inodes, 404 → 0 dangling symlinks, 5.1 → 3.2 GB |
| CheckM2 DB | staged | 2.9 GB; the pipeline's own download step runs as a DISPATCHED TASK and would hang forever on a network-less compute node |
| compute-node drivers | **OBSERVED** | `METASMITH_DRIVER_SLURM=1` is an engine built-in; pass `_CPUS/_MEM/_TIME/_ACCOUNT`. 2 cpus/8 GB died OUT_OF_MEMORY in 65 s because local-executor steps are charged to the driver's job — use 10 cpus / 48 G |
| Slurm band routing | **OBSERVED** | a 16 h request ran; tasks DO auto-route past the 3 h band. Never name a partition — `sbatch` rejects it and redirects automatically |

---

## Environment facts a driver must respect

- **Compute nodes cannot fetch.** 0.14 MB/s to github, a hard stall against `ftp.genome.jp` at 19 MB, while a zenodo HEAD returns 200 in 0.61 s. Small requests succeed and large transfers HANG, so a download step neither errors nor progresses — it looks exactly like a driver that is working. Every `labels=["local"]` download step runs wherever the DRIVER runs.
- **login1 and login2 are dead for any memory-claiming process.** Their 16 GiB per-user cgroup is saturated with page cache; a deliberate 256 MB allocation is killed. `memory.reclaim` is root-only, so it cannot be drained from user space. login3 is the only usable one and it is shared.
- **`ssh fir` round-robins across the three login nodes.** Any node-specific measurement taken through a bare `ssh fir` has an unknown node; hop explicitly.
- **Quota is inodes, not bytes.** 457K of 1,000,000 used against 11 of 20 TB. A `find -type f` census under-reports the Lustre project quota by ~12% because directories are inodes. An ordinary task work dir is **9 inodes**. The per-bin law is now CONFIRMED EXACTLY, not estimated: a `checkm` task is **bins + 12** inodes — 51 bins gave 62 files + 1 dir = 63, and 57 bins gave 68 + 1 = 69. So it scales with RECOVERY, which varies by sample; do not multiply one sample by 249 without a range. COMEBin's own task dir is still unmeasured (3 files while queued). Reference-database staging is 16.5K-31.7K per task dir but **once per agent home**.
- **`results/` is a TARGET census, not a product census.** A healthy intermediate is absent from it by design.
- **`dev/libraries.sh -bm` moves EVERY plan key.** It runs `msm build all`, rebuilding leaf instance-ids (path + **mtime**) for every library, not the one you changed. Rebuild scoped: `python -m metasmith build transforms -t <data_types> -r <the one library>`.
- **`_stable_id` is only stable if the path STRING is.** `run_metagem.py`'s `MEDIUM_TSV` default embeds an absolute checkout path, so **the metaGEM keys are not portable across checkouts** — they reproduce exactly in the original tree and differ in any copy, regardless of content.
- **Transform sources are staged PER RUN; the engine arrives through a per-node dev-overlay tarball.** So an engine fix reaches a run that was staged before the fix existed, and a transform fix does not. That asymmetry decides which of a live run's numbers are usable.

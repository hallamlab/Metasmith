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
| `comebin` | **OBSERVED, and criterion 9 HAS numbers** | 4 of 10 tasks of array 59583112 (CAMI rung 10, 48 cpus) terminal, MaxRSS read from the `.batch` rows because the array-task rows carry a blank column: `_0` 06:34:43 / 58,863,944K = **56.14 GiB**; `_1` 09:36:57 / **60.16 GiB**; `_3` 07:18:08 / **51.53 GiB**; `_8` 08:04:40 / **61.85 GiB**. Six still running past 9 h, so both ends may move. **Range so far: wall 6h35m-9h37m, MaxRSS 51.53-61.85 GiB, peak = 32% of the driver derived 192 GB grant** — no OOM exposure, ~3x over-provisioned. CAUTION the figures previously recorded for criterion 9 (6:34:43/56.14 and 7:18:08/51.53) are `_0` and `_3`, **which happen to be the two FASTEST tasks**; adding `_1` and `_8` moved the wall ceiling from 7h18m to 9h37m and the memory ceiling from 56.14 to 61.85 GiB. Do not quote a COMEBin figure as settled until the array is terminal |
| `das_tool` | **UNPROVEN** | waits on comebin |
| `spades_pratama` (metaSPAdes) | **OBSERVED at execution 2026-09-13, first success** | `Son2YJiI` taskdir `bd/bf206af8920e9f024b966a816a6702`, exit 0, **`--mem 196608M` — ATTEMPT 1**: product `1-1-1.a6d96b3f6d15331f-uuCMr0ru.fna` **565,999,960 B, 1,077,503 contigs, 516,915,782 bases** (mean contig ~480 bp), `.command.cache` present so it promoted. `ok=1 running=64 failed=3`. **So 192 GB IS sufficient for some samples** — the OOMs are sample-size-dependent, not a blanket shortfall, and B18's ladder covers the rest. CAUTION my own watcher reported a second success on `58/201705480afd…`, which is **exit 1 with nothing but an 8,383-byte `.sbatch.log`** — it trusted exit 0 read at an arbitrary instant and never checked for a product. Gen 2 requires exit 0 AND a non-log file over 1 MB |

> **PRE-PIN BASELINE, recorded 2026-09-13 before run `5vqR1dv8` is reclaimed.** That run is
> `marine_short_read / sample_0` — **the same sample** the pinned figures above come from — and its
> metabat2 banner reads `using minContig 2500`, i.e. it predates both the `--minContig 1500` pin and
> the SemiBin2 seed pin. Counted from its own `results/` bin fastas, so the same sample at two
> parameter settings:
>
> | binner | unpinned (`5vqR1dv8`, Sep 9) | pinned | delta |
> |---|---|---|---|
> | metabat2 | **43 bins** (minContig 2500) | 51 bins (minContig 1500) | **+8, +18.6%** |
> | semibin2 | **56 bins** (random seed) | 57 bins (seed 1) | +1 |
> | comebin | **104 bins** | *never recorded anywhere* | — |
>
> **The metabat2 row quantifies the confound the campaign named and never measured**: nf-core pins a
> 1500 bp floor and our binner used the tool's own 2500, and on this sample that is eight bins —
> which is why it could move an AMBER score rather than merely being untidy. One sample, so it is a
> magnitude and not a coefficient.
>
> **The comebin figure is the only bin count this campaign has for COMEBin at all.** Criterion 9's
> numbers are wall and MaxRSS from array 59583112; nothing recorded how many bins it recovers.
>
> That run also holds `annotation-diamond_uniref50_results`, a real 49,151,863-byte TSV dated Sep 9,
> and `taxonomy-checkm_stats` at 203 files. Recording these is what makes the tree safe to delete:
> the numbers, not the directory, are the evidence.
| `amber` / `amber_das_tool` | **OBSERVED** (per-binner) | MetaBAT2 row: `precision_avg_bp 0.8984`, `recall_avg_bp 0.0775`, `f1_score_bp 0.1428`, `ARI_bp 0.4581`. MAG-level row unproven |
| **reference-arm AMBER (post-hoc scorer)** | **OBSERVED at execution 2026-09-13, all four labels** | Job 59654244, 1m06s, `marine_sample_0`: DASTool precision 0.9737 / recall 0.1262 / f1 0.2235 / ARI 0.9670 / misclass 0.0280 / 31 bins; COMEBin 0.7653/0.4161/0.5391/0.8561/0.1165/105 bins; MetaBAT2 0.8993/0.0843/0.1542/0.4239/0.2642/50; SemiBin2 0.8411/0.0776/0.1421/0.8625/0.1075/77. Bridge: 398,702/400,744 contigs voted, 28,686,328/28,686,328 mapped reads matched. **Criterion 12's REFERENCE half is delivered**; the metasmith half for this sample still has to come from C1IM6IG3 or WfOlaqLT. CAUTION the first run scored only three labels — DAS Tool writes the full FASTA header as its contig id and all 65,569 rows were dropped silently; see TOOL_FOR_TOOL.md |
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
| `dramv` | **BROKEN, cause confirmed** | exit 1 on three attempts, zero products. `Error: Invalid file path or buffer object type: <class 'NoneType'>` — DRAM-v handing a null config entry to a reader. This is the all-null `DRAM.config` manifesting ONE STEP LATER than its cause, which is exactly why that config must be verified by content. **B3 CLEARED 22:09** — the null config entry it choked on is now populated and all five distillation sheets verify by content, so `DRAM-v distill` can run. Still UNPROVEN until a relaunch re-runs it: transform sources are staged per run, but the DRAM config is read at run time from the staged tree, so no relaunch of the *transform* is needed — only of the step |
| `checkv` | **OBSERVED** | Pratama run, exit 0, 7m08s, 4 TSVs; `quality_summary.tsv` 13,752 rows with the real header (`contig_id, checkv_quality, miuvig_quality, completeness, ...`) |
| `cctyper` | **OBSERVED** | direct run against a real Pratama MetaWRAP bin, exit 0 in 3m35s: prodigal ORFs, **HMMER against all 705 Cas profiles**, minced, then BLAST for arrays near operons. `--db /usr/local/cct_data` is confirmed load-bearing and working — the 705-profile search IS the bundled database being read, which `$CCTYPER_DB` alone would not have given, since that variable is set by a conda `activate.d` hook only a LOGIN shell sources. The bin reported `No CRISPRs found`, which is a RESULT and not a failure, and the protocol's empty-table branch emitted both tables plus a 0-record spacer FASTA exactly as designed. **NOT yet exercised: the spacer header-tagging path**, which needs a bin that actually carries an array — sweep 59608537 closed that (see below), and a follow-up direct run on the five positive bins exercises the tagging |
| `blast_spacers_to_contigs` | **UNPROVEN** | its `viromics::dereplicated_candidate_virus` subject exists (58.8 MB, published by `d6UJuZgF`), so the only missing input is a non-empty spacer set. Its `n == 0` branch writes a header-only table and returns success, and on MAG data that may well be the PRODUCTION path rather than an edge case — CRISPR arrays are repetitive and are commonly lost in assembly and binning |
| `skani_dedup`, `aggregator`, `derep_mag_reference`, `pratama_mag_recovery` | **UNPROVEN** | not yet reached — downstream of the per-binner CheckM2 fan-out still running. NOTE `skani_dedup`'s clustering logic IS validated: `metawrap_skani_dedup` is its protocol unchanged but for the requirement and product types, and that was proven in three escalating cases |
| `vcontact3` | **UNPROVEN, and unreachable IN THIS RUN** | its database twin failed and was swallowed, so the step has no task dir and never will here. The database is now staged and pruned, so a relaunch reaches it |
| `virsorter2` | **BROKEN, diagnosed** | see Installs; fails on a compute node for a structural reason |
| `vcontact3` | **UNPROVEN** | was unreachable because its database step failed; now unblocked |
| `carveme_from_orfs_cplex` | **OBSERVED AT CORPUS SCALE** | **131 models, ZERO failures**, 17 still running, in the live li2019 run. 129 carry an `.xml` >100 KB; a sampled model is 6.1 MB SBML with **1,968 reactions and 1,376 species** — a genuine genome-scale model, not a stub. Earlier single-bin probe: draft 2m25s, gap fill 32m00s. The OPEN solver does NOT finish gap fill on the smallest bin (1h49m, killed), so CPLEX is a feasibility constraint rather than a preference. **CAUTION the declared `Duration(hours=2)` is TOO SHORT: measured over 100 gapfill tasks, min 131s / p50 380s / p90 1187s, and 5 of 100 hit the 2h wall (`ExitCode 140:0`, `Elapsed 01:59:1x`). The distribution is right-skewed with an unbounded tail; the two earlier sampled points (smallest bin 32m, largest bin 39s) bracketed nothing. Recommend 12h (fir band 2). The loss is BIASED — it drops whichever MAGs are hardest to gapfill, under retry-then-ignore, while the run reports complete.** |
| **E4 chunk 1's whole model lane, at scale** | **OBSERVED 2026-09-13 — 1,995 models and 1,900 scores with ZERO failures** | `lE94xbfH` (e4_c1, launched from `19609c45`), 2,000 MAGs: `prodigal_from_bin` fanned out 392-wide at 1 h / 8 G / 2 cpu, then **`carveme_from_orfs_cplex` ok=1995 running=5 failed=0**, then **`memote_score` ok=1900 running=0 failed=0** (19 array stubs excluded). First memote product: `.json.gz` 83,991 B + `.html` 4,024,276 B + `.json` 467 B. **This closes two blockers at once at corpus scale.** (a) The memote `$HOME` pin works: the staged `task/transforms/.../memote_score.py` carries `HOME=4` and not one of 1,900 tasks hit the read-only-`$HOME` cobrapy failure. (b) **B11's retry ladder holds**: 1,995 gapfills completed at a rendered 12 h / 16 GB / 4 cpu with `memory` and `time` both doubling on `task.attempt`, against the 5-of-100 **biased** loss the old 2 h wall produced — and the bias mattered because it dropped whichever MAGs were hardest to gapfill while the run reported complete. Criterion 6's reconstruction-and-scoring lane is therefore proven at chunk scale, not on a single-bin probe. CAUTION `carveme` at 1,995 of 2,000 and `memote` at 1,900 means the lane is still finishing; the zero-failure claim is over what has terminated |
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
| **CLEAN, and the whole GPU path** | **OBSERVED 2026-09-13, first GPU step of the campaign and CLEAN's first execution anywhere on fir** | Job array 59642405 task `_0`: **COMPLETED 00:43:01, MaxRSS 22,662,928K = 21.6 GiB**, product `1-1-1.5a6f04c3bc6868b9-lqXfiZIS.tsv` at 4,042,725 B holding **139,768 rows** of `Query ID / Predicted EC number / clean_score` (e.g. `k141_8004_1  2.1.2.2  0.2099`). **The MIG slice was ACTUALLY ALLOCATED, not merely requested** — `AllocTRES` carries `gres/gpu:nvidia_h100_80gb_hbm3_3g.40gb=1, gres/gpu=1, cpu=4, mem=32G` — and the job ran under **`def-shallam_gpu`** while every other step of the same run stayed on `rrg-shallam-ab`, so `slurmGpuAccount`'s per-step routing is proven at the level of use. Prior art was a confirmed negative: zero of 1,791 `.command.sh` files in every run of every agent home mentioned it. **There is NO GPU allocation under `rrg-shallam-ab`** — `sacctmgr show assoc` gives only `def-shallam_cpu`, `def-shallam_gpu`, `rpp-shallam_cpu`, `rrg-shallam-ab_cpu` — and a `3g.40gb` MIG slice (40 GB, 60 nodes per band) schedules ~15 min sooner than a full H100 (80 GB, 30 nodes) on `--test-only`, while being ample for CLEAN's declared 16 GB. CAUTION **utilisation is NOT confirmed**: no cuda/device/nvidia string appears anywhere in `.command.log`, so the allocation is proven and whether the tool used the card is not. 43 min for 139,768 predictions on 4 cpus is suggestive of GPU inference, but that is an inference — a CPU fallback would produce correct output more slowly and we have no baseline. CAUTION `sacct --name=nf-p08__clean` returns NOTHING; Slurm stores the array suffix, so the name is `nf-p08__clean_(1)` and `sacct -X \| grep -i clean` is what finds it |
| **Flye, in production** | **OBSERVED 2026-09-13 in E2 long (`33hlLu8Q`), and B12's fix is proven AT EXECUTION** | The pinned `e2/flye.py` runs, not the standard transform: a `MODE` table keyed on the declared platform (`OXFORD_NANOPORE → --nano-raw`), **no `mean_quality` branch**, `Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24))` — verified in the executing copy under `task/transforms/`, not the `task/data/` library payload. A live task's own log at 6 minutes in: `Starting Flye 2.9.5-b1801` · `Total read length: 3931523370` · `Reads N50/N90: 3144 / 1228` · `Minimum overlap set to 1000` · `>>>STAGE: assembly` · `Assembling disjointigs` · `Counting k-mers 0%..100%` · `Filling index table (1/2) 0%..100%` · `(2/2)`. **That is past the exact stage the HiFi preset died at** (`No disjointigs were assembled`, ~9m43s). 41 tasks staged, 9 with log content, 42 in the queue, `--mem 65536M`. **CLOSED 2026-09-13 05:20 — a COMPLETED production assembly**, exit 0, task dir `33hlLu8Q/nxf_work/0d/19c675d0…`: **157,048,136 B · 3,311 contigs · 154,430,709 bp · N50 145,970 · largest 2,802,068 · mean coverage 27**, from 4,514,872,323 bp of reads at N50/N90 4,679/2,693. **Two independent sources agree**: my own `grep -c '^>'` and `awk` length sum give 3,311 and 154,430,709, and Flye's own banner reports `Fragments: 3311` / `Total length: 154430709`. 26 of 41 tasks terminal, **zero failed**. So the pinned platform-keyed preset does not merely get past the disjointig stage — it finishes. CAUTION the production image is **2.9.5-b1801**; my A/B that established B12 ran on hand-pulled **2.9.6-b1802**, a different build — the preset logic lives in the transform so the finding holds, but wall times are not comparable. And **there is no `flye` invocation line to grep for**: metasmith's protocol does not echo its command, so `.command.log` carries only the tool's own stdout. The banner `Starting Flye 2.9.5-b1801` is the signal, exactly as metabat2's `using minContig 1500` banner was |
| **Flye** | **EXECUTED 2026-09-12, first time ever** | image `docker://staphb/flye:2.9.6` pulled to the shared cache (80,162,816 B, 2m03s on login3; it was NOT among the 136 cached images). Prior art is a CONFIRMED negative on both hosts: zero `.command.sh`, zero staged `workflow.nf`, zero `sacct` rows. **DEFECT FOUND ON FIRST RUN: `flye_raw.py` picks its preset from `seqkit AvgQual`, and CAMI's NanoSim reads carry a FLAT SYNTHETIC Q40 — every quality character is `I` — so the `q>=20` branch fires and the whole CAMI long arm would assemble NANOPORE data with `--pacbio-hifi --read-error 0.000501`.** **A/B MEASURED: the transform's own preset choice produces NO ASSEMBLY.** `asis` (`--pacbio-hifi --read-error 0.000501`) FAILED at 9m43s with Flye's own `No disjointigs were assembled - please check if the read type and genome size parameters are correct`; `--nano-raw` ran healthily past the overlap stage. So the CAMI long arm would have failed every Flye step and reported complete. Caveat: the arm varied preset AND `--read-error` together; job 59601518 separates them. Plant nano sample_0 = 3,105,598 reads / 5.01 Gbp / read N50 2,308, at 8 cpus (the transform's own declared count) / 128 GB. Argument ORDER is load-bearing: `--pacbio-hifi` is `nargs='+'` and greedily eats the reads path, so `--read-error` must precede the preset — the transform gets this right, my first test script did not and flye exited 2 in 27s |
| CAT, GTDB-Tk, Prokka, ALE, BUSCO | **confirmed NOT running** | CAT is gated at `mag.nf:517` on `cat_db \|\| cat_db_generate`, both unset — it appeared in an earlier process list only as a `-profile test` artifact |

---

## phiX removal — is it needed on non-human samples?

Asked by the principal. Measured on the live Pratama run `d6UJuZgF`, which assembled ERR3858110
with bbduk only — metasmith has **no phiX removal anywhere** — so its contigs and viral calls ARE
the no-filter condition, against real products rather than a new run.

| question | answer |
|---|---|
| does a phiX contig assemble without the filter? | **YES, but FRAGMENTED into five**, not one 5.4 kb contig |
| identity to phiX174 `NC_001422` | **99.0-100.0%** — the Illumina spike-in, not environmental Microviridae |
| genome covered | 5,021 of 5,386 bp = **93.2%** |
| geNomad | **CALLS IT**: virus_score 0.9600 / 0.9379, taxonomy `...Petitvirales;Microviridae` at agreement 1.0000, and one gene annotated **`Sinsheimervirus phiX174`** by name (bitscore 419, e-value 4.3e-132) |
| VIBRANT | **calls none of the five** |
| enters the vOTU catalogue? | **YES — 2 of 5** (`k141_362813`, `k141_395225`) as their own cluster representatives |
| catchable post hoc? | **trivially** — minimap2/BLAST vs `NC_001422` finds all five in seconds |

The five fragments: `k141_96811` 40-727, `k141_395225` 730-2316, `k141_362813` 2299-3709,
`k141_323311` 4004-4589, `k141_351208` 4620-5386.

**CAUTION a naive "drop the ~5,386 bp contig" rule fails** — these are 586-1,587 bp fragments. It
has to be an alignment check against `NC_001422`. And a VIBRANT-only pipeline would neither remove
nor flag it.

**REFERENCE FINDING: nf-core/mag's bundled "phiX" reference is NOT phiX174.** It is
`assets/data/GCA_002596845.1_ASM259684v1_genomic.fna.gz` = `>DQ079895.1 Coliphage WA11`, 5,387 bp,
against phiX174's 5,386. nf-core's own commented URL says `Enterobacteria_phage_phiX174_sensu_lato`,
which is how the sensu lato grouping put WA11 there. Measured distance: **skani ANI 93.72%**,
**minimap2 95.1%** over a whole-genome block. At ~95% a 150 bp phiX174 read carries ~7 mismatches,
inside bowtie2's default end-to-end tolerance (~15), so the filter still catches true spike-in —
imprecision, not a functional failure. `BOWTIE2_PHIX_REMOVAL_ALIGN` carries **no `ext.args`**, so it
runs at pure bowtie2 defaults.

### How many reads the filter would actually remove — MEASURED on real groundwater

`ERR3858110` (H52_1), nf-core's exact command against its own WA11 reference, defaults, mapped
reads kept instead of deleted:

    56,783,841 pairs; 17 aligned concordantly exactly 1 time; 0 discordantly
    99 further mates aligned singly   ->  ~133 reads total
    0.00% overall alignment rate
    removed pairs, counted from the fastq rather than the log: 17

**17 pairs in 56.8 million — 3 in ten million.** With CAMI's 127 of 16,647,376, the filter is a
no-op on a simulated corpus AND on a real groundwater metagenome. The second is the stronger of
the two, since CAMI could be dismissed as having no spike-in to find by construction.

**And the removed reads match phiX174 BETTER THAN THE REFERENCE THAT CAUGHT THEM.** Percent
identity of the same 34 mapped reads, by NM tag over aligned length:

    vs phiX174 NC_001422   >=99%: 10 (29.4%)   95-99: 14   90-95: 8   80-90: 2
    vs WA11    DQ079895     >=99%:  0 ( 0.0%)   95-99: 14   90-95: 12  80-90: 8

So there IS genuine phiX174 spike-in in this library, at trace level, and nf-core catches it only
because ~5% divergence stays inside bowtie2's default tolerance. Genome coverage 3,048 of 5,386 bp
(56.6%), mean depth 0.95, max 4.

**That depth reconciles the two halves of this section, which look contradictory and are not.**
~133 reads over 5.4 kb is roughly 3.7x — and a 3.7x assembly is exactly what produces **five
fragments covering 93.2%** rather than one complete 5,386 bp contig. The trace read count and the
fragmented contig are the same observation seen twice.

`SRR32696677` (0.2 um viral fraction, 2022), same command:

    76,298,172 pairs; 0 aligned concordantly; 0.00% overall alignment rate
    removed pairs, counted from the fastq: 0

**ZERO.** So across the two libraries the filter removes **17 pairs out of 133,082,013** — and the
two differ in a way worth keeping: the 2019 NextSeq run carries trace phiX174, the 2022 run carries
none at all. Whether a library has spike-in is a property of its sequencing batch, so "no-op" is a
claim about these libraries rather than about groundwater in general.

    VERDICT for the deviations table: phiX removal is a measured no-op on CAMI (127 of
    16,647,376 pairs) and on real Pratama groundwater (17 of 133,082,013). It is a genuine
    methodological difference between the arms and it moves no number that this campaign reports.
    The successor check still matters: 2 of the 5 assembled phiX fragments DO enter the vOTU
    catalogue, so removing the filter without an alignment check against NC_001422 inflates it.

## CRISPR recovery on real MAGs — how much does E3's within-survey host lane actually get?

Job 59608537, cctyper's exact transform command over **all 92 published MetaWRAP MAGs** of the
live Pratama run, 276 tasks COMPLETED, **zero database failures**:

| | |
|---|---|
| bins reporting | **92 of 92** |
| bins carrying a CRISPR array | **5 (5.4%)** |
| total arrays | 5 |
| total cas operons | 7 |
| **total spacers** | **40** |

The five: `1-87-1` 15 spacers (1 array, 2 operons), `1-90-1` 10, `1-8-1` 7, `1-61-1` 5, `1-2-1` 3.
Two more bins carry a cas operon with no array (`1-84-1`, `1-37-1`, `1-15-1`).

**So one whole sample's MAG set yields 40 spacer queries.** That is expected biology rather than a
defect — CRISPR arrays are repetitive, and repeats are exactly what an assembler collapses and a
binner then loses — but it sizes the lane honestly: `blast_spacers_to_contigs` searches tens of
spacers against the frozen viral set, not thousands, so the within-survey half of host prediction
contributes very little here. Worth knowing before paying for it.

    CAUTION do NOT read 5.4% as a property of cctyper or of the pipeline. It is a property of
    MAG-based CRISPR detection on a fragmented groundwater assembly. A spacer set built from the
    ASSEMBLY rather than from binned MAGs would be larger, and that is a different experiment.

## metaGEM's published MAGs — unpacked for E4, verified by NAME

Job 59610574, `e4_extract_mags.sh`, into `/scratch/phyberos/metagem/published/<study>/mags/<name>.fa`:

| study | MAGs |
|---|---|
| li2019 | 172 |
| korem2015 | 154 |
| bissett_base | 277 |
| karlsson2013 | 4,134 |
| sunagawa2015 (Tara) | 9,371 |
| **total** | **14,108** |

Diffed as `(study, mag)` pairs against the driver's `e4_published_mags.tsv`: **14,108 shared, 0 in the
list but absent from disk, 0 on disk but absent from the list.** Counts matching is not names
matching, and this campaign has already had two sets agree on size while disagreeing on membership.

    CAUTION the first diff reported 1,225 names missing AND the SAME 1,225 extra, with identical
    first lines on both sides. That is `comm` refusing two files sorted under different locales
    (the TSV locally, the listing on fir), not a difference. `LC_ALL=C sort` both sides. A set
    difference whose two halves are the same size and start with the same line is an ordering
    artifact.

**E4 does not fit the inode quota unbatched.** None of `prodigal_from_bin`,
`carveme_from_orfs_cplex` or `memote_score` declares `batch_size`, so each MAG is its own task in
all three steps: 14,108 x 3 = **42,324 tasks**, ~27 transient inodes plus ~18 permanent cache
inodes per MAG, so **~635K** of which ~254K is cache that cannot be pruned without losing resume.
Chunk at ~2,000 MAGs and prune each chunk's `nxf_work` after its products are promoted.

## Pratama pre-interleave — all 66 paired runs, verified by content

Array 59607969, 198 tasks all COMPLETED, **zero failures**. Command is
`interleave_zipped_short_reads.py`'s VERBATIM in `docker..staphb_bbtools..39.49.sif`, so the
product is what the transform would have made:
`reformat.sh unbgzip=f in1= in2= out=stdout.fq | pigz -p 16 >`.

    /scratch/phyberos/pratama2026/interleaved/<dataset>/<run>.fastq.gz     originals KEPT

| dataset | runs | bytes | interleaved records |
|---|---|---|---|
| `reads_2019` | 32 | 377,129,154,977 | 4,145,108,866 |
| `reads_2022` | 34 | 366,910,756,889 | 5,376,621,774 |
| **total** | **66** | **744,039,911,866** | **9,521,730,640** |

66 files, 66 `.ok` stamps, **0 leftover `.partial`**, 0 FAIL lines. Input was 763,924,306,715 bytes.

**Verified three ways, the third independent of the first two.** Each task gated promotion on
(a) a line count that is a multiple of 4, (b) records == 2 x that run's `runs.tsv read_count`, and
(c) the first two records being the two mates of one fragment. Then, after the fact and from a
different source: `2 x sum(read_count)` taken straight from `runs.tsv` over every PAIRED run =
**9,521,730,640**, matching the sum of the stamps exactly.

**Full-stream gzip integrity is proven BY CONSTRUCTION**, not assumed: the record count is taken
by decompressing the whole file with `pigz -dc`, so a truncated or corrupt stream cannot reach a
`.ok` stamp. A failed check leaves the `.partial` unpromoted, so a bad run is **absent** from the
tree rather than present-and-wrong — which matters because the consumer globs the tree.

Both datasets carry the same header convention, so the `/1` `/2` alternation holds corpus-wide:

    reads_2019   @ERR3858110.1 NB501242:101:HLYN3BGX9:1:11101:11969:1049/1   and /2
    reads_2022   @SRR32696677.1 A00872:178:HGHN7DMXY:1:1101:2284:1000/1      and /2

Throughput, for sizing a re-run: ~157-267 s to interleave plus ~79-139 s to verify per run at 16
cpus, 8 concurrent; the whole corpus in about 75 minutes. Declared `Size.GB(8)` was not tested —
the array ran at 16 GB.

    CAUTION discovered while accounting for the ORIGINAL file count, which came to 137 rather
    than the 138 a full corpus implies: **SRR32696686 (H41_02um_2022_Nanopore) has no reads on
    disk.** Its directory is empty and a 5,362,917,376-byte partial of an expected 12,125,303,198
    sits in `.ena_staging/` -- 44%, fetched and never resumed. So the Pratama long-read set is
    **5 of 6 MinION runs**, not 6. The PAIRED corpus is unaffected and complete at 66. Also in
    `.ena_staging/`: a stranded `SRR32696714_1`, harmless (that run was completed by the SRA
    route) and reclaimable.

## Installs, staging and infrastructure

| thing | status | notes |
|---|---|---|
| Nextflow 26.04.6 | **OBSERVED** | private at `~/bin/nextflow26`, `NXF_HOME=$HOME/.nextflow26`. mag 5.5.0 declares `!>=26.04.0`; fir's newest module is 25.04.6 and dies on a missing nf-schema 2.7.2 |
| container cache | **OBSERVED** | 14/14 CAMI, 21/21 Pratama, 11/11 metaGEM images present AND `.verified`. Derive the per-arm list from a staged run's own `workflow.env.json`, never from a hand-maintained list |
| CPLEX 22.2.0 | **OBSERVED** | uncapped solve, 2000 vars / 1999 constraints, in-container, on a compute node, from durable project space. `docplex config --upgrade` is what copies the full runtime — the packaged module alone caps at 1000/1000 with error 1016 and no warning |
| GTDB r232 | **SERVED FROM A SQUASHFS IMAGE. The genome tree is DELETED** | **The tree is gone: job 59638488 removed it at 03:22 after gtdbtest passed through the image, FREEING 416,713 inodes (750,788 -> 334,075).** `skani/database/GCF/` and `GCA/` now exist as **empty mount points** — kept deliberately, because that is where the bench GTDB-Tk transform binds the image; remove them and the bind has nowhere to land. The 199,923 genomes (GCF 40,255 + GCA 159,668) are served from **`/scratch/phyberos/staging/gtdb/release232_skani_genomes.sqfs`, 192,112,726,016 bytes, ONE inode**, built by job 59625981 in 56m02s with `mksquashfs -noD -noF` (99.98% of uncompressed size, correctly declining to recompress already-gzipped `.fna.gz`). The four sketch files are UNTOUCHED beside the mount points: `sketches.db` 75,270,701,831 · `markers.bin` 9,864,161,739 · `index.db` 39,984,608 · `metadata.tsv` 390. Both source tarballs were then deleted (253 GB) and are re-fetchable from chinook. **Verified at the level of use, not merely mounted:** gtdbtest classified `SRR7664615_bin.1.s` to `g__Castellaniella` with `classification_method` = "taxonomic classification defined by topology **AND ANI**", ANI 88.44 against a **GCF** genome and 86.91 against a **GCA** one — so both `image-src` binds were read, and skani genuinely compared reference genomes out of the image. 17m59s, MaxRSS 90.6 GiB, of which ~93 GB is the pplacer reference-tree cache and therefore a fixed cost rather than per-genome |

    The image is bound as TWO sources of one file, which is what preserves the sketch files:
        --bind .../release232:/ref,
               .../release232_skani_genomes.sqfs:/ref/skani/database/GCF:image-src=/GCF,
               .../release232_skani_genomes.sqfs:/ref/skani/database/GCA:image-src=/GCA
    The transform's own assertion is the best check in the chain, because it names the exact
    file whose absence killed classify_wf at 33m44s before the image existed:
        test -e /ref/skani/database/GCF/000/367/345/GCF_000367345.1_genomic.fna.gz
    CAUTION `--skip_ani_screen` does NOT avoid needing these genomes. The guard that once
    gated the post-placement ANI step is commented out upstream in 2.6.1, so classify_wf dies
    AFTER every expensive stage while its Slurm job exits 0:0.

| DRAM 1.5.0 | **OBSERVED, staged, 5/5 sheets** | Its first attempt DIED SILENTLY with 0/5 sheets: `prepare_databases` processes `--select_db` IN ORDER and raises on the first failure, and dbcan's download returned **19,313 bytes of HTML** (`Format tag is '<!DOCTYPE'`), so kofam and pfam landed and everything after dbcan — including all five sheets, which are written at the END of that same call — was never reached. Repaired with `DRAM-setup.py update_dram_forms --output_dir /db`, NOT by re-running `stage_dram.sh`, whose first act copies a blank `CONFIG` over the config and would have erased the 24 GB of kofam/pfam work. All five verified as real TSVs with real headers (580,242 / 579,664 / 2,378 / 11,199 / 21,569 B). **DEVIATION: dbcan is absent, so no CAZyme annotations.** CAUTION the config value is `etc_mdoule_database...tsv` — an upstream TYPO — so a glob for `etc_module_database*` matches nothing |
| VirSorter2 DB | **OBSERVED, staged** | `/scratch/phyberos/refs/virsorter2_2.2.4`, 11 GB, 28,732 inodes, built in 8 min. snakemake names its conda env `md5(realpath(conda_prefix)+yaml)[:8]`, so the env name depends on the MOUNT PATH: setup builds it in a per-run work dir, the consumer always binds `/db`, and they can never agree for ANY path. Fixed by BUILDING the env under a `/db` bind — envs are not relocatable, so staging a prebuilt one cannot work. Verified by CAPABILITY not presence: `conda_envs/671930f2` exists AND its python imports sklearn 0.22.1 / pandas 1.2.5 / numpy 1.23.5, matching vs2.yaml's pins. `--use-conda-off` is not an escape hatch — the image's own env has no sklearn/pandas/numpy/screed/prodigal/hmmsearch |
| VIBRANT DB | **OBSERVED, staged** | `/scratch/phyberos/refs/vibrant_1.2.1`, 24 files / 11 GB, `databases/` + `files/`, **0 dangling symlinks**. Promoted with `cp -a` from the Pratama task cache rather than re-downloaded — it had been sitting there complete since the first Pratama run |
| CheckV DB | **OBSERVED, staged** | `/scratch/phyberos/refs/checkv_db`, 91 files / 6.83 GB, `genome_db/` + `hmm_db/`, **0 dangling symlinks**. Same origin. CAUTION `du -sh` reported 6.4 G at the source and 5.7 G at the copy; `du -sb` and a size sum both give **6,834,680,161 on each side** and the per-file size+path listings are identical. `du -sh` disagrees with itself across Lustre paths with different stripe behaviour — this is the second time in this campaign (the CPLEX runtime read 38 M vs 17 M). Compare BYTES across two paths here, never `du -sh` |
| iPHoP DB | **OBSERVED, staged, md5-verified** | `/scratch/phyberos/viromics_refs/iphop_db/Aug_2023_pub_rw`, **318 GB in only 263 inodes**, complete including `db/wish_data` (the component `add_to_db` is known to drop). Staged by an earlier session in Sept 2023 and **verified against its own shipped `md5checkfile.txt`: 254 of 254 OK, 0 FAILED** — the 255th file on disk is the manifest, which does not list itself. **It needed no fetch at all**, and I wrongly told the experimenter it was "a genuine fetch, the largest staging cost in the campaign" after listing chinook and never searching fir. Fourth instance of "absent from where I looked" written up as "absent" |
| vConTACT3 DB | **OBSERVED, staged** | `/scratch/phyberos/refs/vcontact3_v230`, pruned of upstream mmseqs build scratch: 6,925 → 1,057 inodes, 404 → 0 dangling symlinks, 5.1 → 3.2 GB |
| CheckM2 DB | staged | 2.9 GB; the pipeline's own download step runs as a DISPATCHED TASK and would hang forever on a network-less compute node |
| compute-node drivers | **OBSERVED** | `METASMITH_DRIVER_SLURM=1` is an engine built-in; pass `_CPUS/_MEM/_TIME/_ACCOUNT`. 2 cpus/8 GB died OUT_OF_MEMORY in 65 s because local-executor steps are charged to the driver's job — use 10 cpus / 48 G |
| Slurm band routing | **OBSERVED** | a 16 h request ran; tasks DO auto-route past the 3 h band. Never name a partition — `sbatch` rejects it and redirects automatically |

---

## Environment facts a driver must respect

- **Compute nodes cannot fetch.** 0.14 MB/s to github, a hard stall against `ftp.genome.jp` at 19 MB, while a zenodo HEAD returns 200 in 0.61 s. Small requests succeed and large transfers HANG, so a download step neither errors nor progresses — it looks exactly like a driver that is working. Every `labels=["local"]` download step runs wherever the DRIVER runs.
- **login1 and login2 are dead for any memory-claiming process.** Their 16 GiB per-user cgroup is saturated with page cache; a deliberate 256 MB allocation is killed. `memory.reclaim` is root-only, so it cannot be drained from user space. login3 is the only usable one and it is shared.
- **`ssh fir` round-robins across the three login nodes.** Any node-specific measurement taken through a bare `ssh fir` has an unknown node; hop explicitly.
- **BOTH axes bind now, and the inode constants are MEASURED EXACTLY — superseding the estimates in
  the line below.** At 05:00 on 2026-09-13: **16.650 TiB of 18.63 (89.4%)** and **573,091 inodes of
  1,000,000 (426,909 free)**. Bytes fit ~0.226 TiB/h; inodes ~50K/h on a five-point 90-minute series
  (my own two-point 110K/h repeated the too-short-window error I had already been corrected for on
  bytes — a rate needs a fitted slope over >=12 min on this filesystem). WfOlaqLT's by-step sweep,
  job 59648954:

      step                  state   tasks   inodes       GB   inodes/task
      checkm2                ok        53   10,933       0.13   **206.3**
      semibin2               ok       208    9,145      10.64     44.0
      metabat2               ok       208    6,797      10.58     32.7
      megahit / fastqc_trimmed / fastqc_raw / gold_standard
                             ok    193-208    2,496       —      **12.0**
      bowtie2_binning_bam    ok       208    2,496   **649.10**   12.0
      comebin                open     208      624       0.00      3.0
      TOTAL                         1,764   40,450     693.99

  **An ordinary task dir is EXACTLY 12 inodes** -- not "9 to 12", twelve on the nose across five
  different steps. An open dir is 3; a failed one is 10.8 because it never wrote products.
  **checkm2 is 206.3/task** (the `bins + 12` law, ~194 bins average) and is already the largest
  single consumer at only 53 of 208 tasks -- so it will reach ~43,000 inodes alone, more than the
  rest of that run combined.

  **WfOlaqLT's whole work tree is only 40,450 inodes, ~7% of the quota used** -- so it is NOT the
  inode lever. E4 chunk 1's is: **5,900 task dirs for just 18.18 GB**, roughly 65-71K inodes. The
  two axes want opposite candidates -- **bowtie2 for bytes (649 GB in 2,496 inodes), checkm2 and
  E4's many small dirs for inodes** -- so the candidate list must be re-sorted whenever the binding
  axis changes.

  **And a mid-run prune must EMPTY each dir but KEEP `.command.cache`**: `caching/promote.py:record_run`
  globs `nxf_work/**/.command.cache` at run END to index each shard, copy task logs into it, and
  write the member's `InvocationEvent` to `trace.jsonl`. Removing it costs the index and the trace
  while products stay reachable via `invocation.probe` -- a silent loss of bookkeeping, not data.
  Keeping it is 2 inodes per task instead of ~13, so ~85% of the saving for none of the risk.

- **Quota is inodes, not bytes.** 457K of 1,000,000 used against 11 of 20 TB. A `find -type f` census under-reports the Lustre project quota by ~12% because directories are inodes. An ordinary task work dir is **9 inodes**. The per-bin law is now CONFIRMED EXACTLY, not estimated: a `checkm` task is **bins + 12** inodes — 51 bins gave 62 files + 1 dir = 63, and 57 bins gave 68 + 1 = 69. So it scales with RECOVERY, which varies by sample; do not multiply one sample by 249 without a range. COMEBin's own task dir is still unmeasured (3 files while queued). Reference-database staging is 16.5K-31.7K per task dir but **once per agent home**.
- **The lab already mirrors most reference databases on Globus — CHECK THERE BEFORE STAGING ANYTHING.** Collection `2602486c-1e0f-47a0-be15-eec1b0ff0f96` (Projects / `ubcarc#chinook`), `/Resources/reference_databases_for_tools/`: `GTDB/` (gtdbtk r207/r220/r226/r232 packages, `gtdb_genomes_reps_r232.tar.gz`, a `gtdbtk.sif`), `dram/dram_data.tar.gz` 31.2 GB, `virsorter2/virsorter2_data.tar.gz` 3.5 GB, `interproscan/`, `metabuli/`, `MPDB_231223/`, plus genomad, card, vfdb, amrfinderplus, megares, bacmet, tcdb, magref, metaphlan and pathofact tarballs — nearly all with sibling `.md5` files. A chinook→fir transfer moved 1.263 TB at 1.14 GB/s, so this is by far the cheapest source on the network. **CAVEAT a prebuilt tarball does NOT solve VirSorter2**, whose conda env must be BUILT at the `/db` path it will be used from; and `gtdbtk_r232_data.tar.gz` is the one file there with no sibling `.md5`.
- **A direct run of a transform with `group_by` on its batched requirement processes ONE group.** Eight bins bound with repeated `-i bin=` all staged (eight `✓` lines) and the run then reported `branch [1] of [1]`, with `context.AsBatch()` yielding only the first. Repeated `-i` DOES fan out for `context.InputGroup(...)` — `metawrap_skani_dedup` took 92 bins that way — so the discriminator is `AsBatch` plus `group_by`, not the flag. Loop one invocation per item to test such a transform, and read the `branch [N] of [M]` line rather than assuming the bindings fanned out.
- **The documented COMEBin deadlock check — "watch for zero `cluster_res` files" — CANNOT be applied from the work dir while `scratch` is set, and three separate instruments lie about a running job.** All three of these read as a deadlock and none of them is evidence: (1) `sacct ... TotalCPU` reports **00:00:00 for a RUNNING job**, because it is accumulated at step end, not live; (2) `sstat` returns a bare header here for every job including a known-active positive control, so it is not a usable instrument on this cluster at all; (3) the work dir holds **no products and no `cluster_res`**, because a real step's outputs live in node-local `NXF_SCRATCH` until the unstage — so they cannot appear there until the task finishes, deadlock or not. `.command.err` does not exist either; the file is `.command.log`. **The one reliable in-flight signal is `.command.log`'s size and mtime plus its tail**, which streams back live. I was one step from reporting three deadlocked COMEBins and criterion 9 unreachable.
- **ENA's `sample_accession` is NOT the analysis unit for the Pratama corpus, and deduplicating on it would silently pool two different filter fractions.** 2019 puts a well's **0.1 um and 0.2 um** metagenomes under ONE BioSample, so 32 PAIRED runs show only 17 distinct `sample_accession` values. The paper's unit is well x year x fraction x replicate = one ENA run, and the field that says so is **`submitted_names`**, which carries the authors' own filenames: `ERR3858123 -> H14_0_1_1_R1.fastq.gz` (0.1 um) and `ERR3858139 -> H14_0_2_1_R1.fastq.gz` (0.2 um), same BioSample. The 32 stems are all distinct. So E3 is **65 runs** = 31 (2019, after excluding `PNK108_H32_0_2`) + 34 (2022) = 65 metagenomes; counting distinct `sample_accession` gives 51 and is wrong. **I got this wrong by reading the column that looks like an identity instead of the one that states the unit** — and a dedup on `sample_accession` would have merged a 0.1 um and a 0.2 um metagenome into one while looking correct in every count. Note also that `PNK108_H32_0_2` and `H32_0_2_1` carry DIFFERENT BioSample accessions but are the same metagenome, which is why the exclusion is a dedup rather than dropping data.
- **`results/` is a TARGET census, not a product census.** A healthy intermediate is absent from it by design.
- **`dev/libraries.sh -bm` moves EVERY plan key.** It runs `msm build all`, rebuilding leaf instance-ids (path + **mtime**) for every library, not the one you changed. Rebuild scoped: `python -m metasmith build transforms -t <data_types> -r <the one library>`.
- **`_stable_id` is only stable if the path STRING is.** `run_metagem.py`'s `MEDIUM_TSV` default embeds an absolute checkout path, so **the metaGEM keys are not portable across checkouts** — they reproduce exactly in the original tree and differ in any copy, regardless of content.
- **Transform sources are staged PER RUN; the engine arrives through a per-node dev-overlay tarball.** So an engine fix reaches a run that was staged before the fix existed, and a transform fix does not. That asymmetry decides which of a live run's numbers are usable.

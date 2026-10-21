# CLEAN on CPU: results

## Purpose & Contents

This file records how fast CLEAN's ESM-1b embedding step runs on fir CPU nodes and on fir's H100 MIG slices, and how accurate each precision and quantisation is. It also gives the CPU setup to use and its cost compared with the GPU route. All runs took place on 2026-10-20 on fir `turin` nodes. Each node has 2× AMD EPYC 9655 (Zen 5), 192 cores, 8 NUMA domains of 24 cores, 24 L3 domains of 8 cores, and AVX-512 BF16 and VNNI.

Every number in this file is from a run on fir unless it carries the label **UNVERIFIED**. A projection or a length rescale is UNVERIFIED. `summary.tsv` holds every embed run, one row each, written by `summarize.py`. The scripts are in this directory, and the fir copy is at `/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/`. Logs are in its `logs/`, named `<jobname>_<jobid>.log`, or `<arrayjob>_<index>.log` for the array rows.

## Recommendation

- **Mode:** bf16 (`torch.autocast("cpu", dtype=torch.bfloat16)`) on the stock image (torch 2.0.0, MKL 2022.2, oneDNN 2.7.3).
- **Layout:** 6 worker processes × 8 threads in a 48-cpu task. Pin each worker to its own block of 8 allotted cpus (`sched_setaffinity` before `import torch`, plus `OMP_PROC_BIND=close OMP_PLACES=cores`). Use length-sorted batches capped at **1,024 tokens**.
- **Throughput:** 28.2-35.8 ORF/s per 48-cpu task, which is 0.59-0.75 ORF/s per core (149-189 residues/s/core). These are two by-core runs of 20,000 Cyanoverse ORFs on shared nodes (`prod/p_bf16_6x8_p_20k_t1024`, `prod/p_6x8_t1024_slim_20k`). On an idle node the same layout gives 0.69 ORF/s/core.
- **Wall:** the 20,000-ORF tasks (mean 253 aa) took 559-708 s to embed. A 100,000-ORF shard at 298 aa takes about 1.3 h at the conservative rate below. **UNVERIFIED** (linear extrapolation).
- **Memory:** a sacct MaxRSS of 47 GB with the slim checkpoint (job 66151629). It is 74 GB with the shipped 7.8 GB checkpoint (job 66148395).
- **maxsep:** run in memory with the GMM list loaded once (`calls.py`'s `fast` path). It gives the same rows as CLEAN's own code, 24,608 of 24,608 on 20,000 ORFs, and is 9-13x faster.
- **Accuracy:** on the DH10B truth set, bf16 has the same precision, recall and F1 as fp32 at the lane's 0.02 score floor. fp32 on CPU gives the same calls as the GPU production run.

The tuned task is about 9-11x faster than the transform's own code path on the same 48 cpus. It takes 1.24-1.57x the wall of one 3g.40gb slice running `clean.py`'s fp32 path, and 4.5-5.6x the wall of a slice running bf16.

## Accuracy

The truth set is the 1,288 adjudicable DH10B ORFs (journal `11e1ffcb`, built by `research/fabfos/annotation_lanes/pbert/build_cohort.py`), copied to `ref/cohort_dh10b.tsv`. The DH10B rows embedded the e4_mm RefSeq proteome (4,468 proteins, now `inputs/dh10b.faa`). Its sequences map by md5 onto 1,273 of the 1,288 cohort ORFs, so every row is scored on those 1,273. The GPU reference is the production CLEAN output `ref/dh10b.clean.tsv`, copied from `data/fabfos/benchmarks/lane_dh10b/annotations/`.

DH10B, scored against truth (`score.py`). F1 is ORF-level, and P/R are precision and recall:

| calls | AUC | F1, no floor | P / R / F1 at score ≥ 0.02 | top EC = GPU | EC set = GPU |
|---|---|---|---|---|---|
| GPU production | 0.9010 | 0.9174 | 0.9537 / 0.9065 / 0.9295 | — | — |
| CPU fp32 4×12 | 0.9010 | 0.9174 | 0.9537 / 0.9065 / 0.9295 | 100% | 100% |
| CPU bf16 4×12 | 0.8991 | 0.9182 | 0.9537 / 0.9065 / 0.9295 | 98.6% | 97.4% |
| CPU int8 4×12 | 0.8673 | 0.8348 | 0.9214 / 0.7824 / 0.8462 | 58.3% | 50.0% |

The GPU agreement columns cover the 3,826 proteins in both proteomes. The command was `python score.py <dir> out/dh_fp32_4x12.calls.tsv out/dh_bf16_4x12.calls.tsv out/dh_int8_4x12.calls.tsv`, run in the image. The embeddings came from `bench.sbatch` array 66141447 rows 8-10 (int8: 66141447_10).

Each mode against fp32 on the same metagenome ORFs (`compare.py` for the round-1 rows, `compare2.py` for the rest). The "kept set" is the EC set after the lane's 0.02 floor, which is what the GPR mapper keeps. "Top EC, confident" counts only the ORFs whose fp32 top call scores ≥ 0.02, which is 690 of 2,400.

| run (vs fp32) | n | cos min | cos mean | top EC | EC set | kept set | top EC, confident |
|---|---|---|---|---|---|---|---|
| bf16 6×8 t4096 (`c_base_6x8`) | 2400 | 0.9788 | 0.999986 | 98.3% | 95.5% | 99.3% | 99.7% |
| bf16 6×8 t1024 (`d_6x8_t1024`) | 2400 | 0.9788 | 0.999985 | 98.1% | 95.5% | 99.3% | 99.9% |
| fp32 6×8 t1024 vs fp32 t4096 | 2400 | 1.0 | 1.0 | 100% | 100% | 100% | 100% |
| torch 2.5 bf16 | 2400 | 0.9741 | 0.999983 | 98.2% | 95.4% | 99.3% | 99.6% |
| torch 2.5 bf16 + SDPA | 2400 | 0.9965 | 0.999993 | 98.0% | 95.5% | 99.2% | 99.3% |
| IPEX 2.5 bf16 | 2400 | 0.9751 | 0.999985 | 98.0% | 95.8% | 99.4% | 99.6% |
| int8, all Linear | 2400 | 0.8934 | 0.99288 | 46.9% | 40.0% | 80.7% | 73.0% |
| int8, FFN only | 2400 | 0.8939 | 0.9937 | 49.0% | 41.5% | 80.2% | 74.9% |
| bf16 4×12, DH10B (`dh_bf16_4x12`) | 4468 | 0.9995 | 0.999998 | 98.6% | 97.2% | | |

The command was `python compare2.py out/scale c_fp32_6x8 <names...>` in the image. Every run in this table embedded the same 2,400-ORF seeded sample of `SRR27911466.faa` (seed 7).

- **bf16 costs nothing measurable.** The flips sit in calls the lane already drops. The token budget does not change the numerics: fp32 at 1,024 and at 4,096 tokens per batch are bit-identical.
- **int8 is rejected.** Dynamic int8 quantisation gives up about half the top-EC calls and 0.08 F1 on DH10B. It is also slower than bf16 (0.33 against 0.50 ORF/s/core at 6×8).
- **embed.py reproduces the transform.** Its fp32 embeddings, put through CLEAN's stock maxsep, give the same 600 of 600 call rows, scores included, as `/app/CLEAN_infer_fasta.py` on the same sample (`out/stock600_maxsep.csv` against `out/mg_fp32_4x12.calls.tsv`).

## Scaling with cores

These are whole-node exclusive jobs (`scale.sbatch`, `--exclusive`, 192 cpus), so no other job shares the node. A run on N cores uses cpus `0..N-1`. Throughput uses the slowest worker's wall, so a row with many workers is conservative. Unless the row says otherwise, the mode is bf16, the workers are pinned, and the sample is `n = 600 / 1,200 / 2,400 / 4,800` for 12 / 24 / 48 / ≥96 cores, the same seeded sample prefix-consistently. Run-to-run noise on an idle node was under 1% (`d_6x8_t1024` 0.688 against `e_base_6x8_t1024_rep` 0.696).

ORF/s per core at a 4,096-token budget (the fair-esm default). Rows are procs × threads:

| cores | 1×T | 2×T/2 | many × 12 | many × 8 | many × ≤6 |
|---|---|---|---|---|---|
| 12 | 0.569 | — | 1×12: 0.569 | — | — |
| 24 | 0.373 | 2×12: 0.450 | 2×12: 0.450 | — | — |
| 48 | 0.250 | 2×24: 0.380 | 4×12: 0.442 (unpinned 0.405) | 6×8: 0.497 | 12×4: 0.525 |
| 96 | 0.127 | — | 8×12: 0.371, 4×24: 0.314 | — | — |
| 192 | 0.051 | 8×24: 0.300 | 16×12: 0.345 (unpinned 0.239) | 24×8: 0.386 | 32×6: 0.393, 48×4: 0.393 |

The same at the recommended small budgets (512-1,024 tokens):

| cores | layout | toks | ORF/s | ORF/s/core | residues/s/core |
|---|---|---|---|---|---|
| 12 | 1×12 | 1024 | 8.95 | 0.746 | 185 |
| 48 | 1×48 | 1024 | 16.6 | 0.346 | 87 |
| 48 | 6×8 | 1024 | 33.0 | 0.688 | 173 |
| 48 | 12×4 | 512 | 34.6 | 0.721 | 181 |
| 192 | 8×24 | 1024 | 77.8 | 0.405 | 102 |
| 192 | 16×12 | 1024 | 93.0 | 0.485 | 122 |
| 192 | 24×8 | 512 | 104.3 | 0.543 | 137 |
| 192 | 64×3 | 512 | 106.3 | 0.554 | 140 |
| 192 | 96×2 | 512 | 106.4 | 0.554 | 140 |

- **One process does not scale.** A single PyTorch process stops gaining at 48 threads (12.0 ORF/s at 48, 12.2 at 96, 9.8 at 192). Above 12 threads, per-core efficiency falls by a third to a half with each doubling.
- **Many narrow processes do scale.** At 12 or fewer threads per worker and a small token budget, per-core throughput stays within 0.69-0.75 up to 48 cores. A fully packed 192-core node reaches 106 ORF/s (0.55/core), which is 77% of the 48-core per-core rate. That last loss is shared memory bandwidth and package power, and no layout recovered it.
- **Pinning matters more as the node fills.** At 4×12 on 48 cores, pinning gives +9%. At 16×12 on 192 cores it gives +44%.
- **By-core allocations are scattered.** A 48-cpu by-core allocation spans several NUMA domains. For example, job 66146033 got 48 cpus between 12 and 167. Pinning the workers inside it still helped: 0.477 pinned against 0.430 unpinned at 20,000 ORFs, on two different nodes (`prod/p_bf16_6x8_{p,u}_20k`). Shared-node runs of the same config differ by up to 25% between nodes (0.588 and 0.746 at 6×8 t1024).

The commands were `sbatch scale.sbatch plan_a.tsv` (66143324), `plan_b.tsv` (66143366), `plan_c.tsv` (66146026), `plan_d.tsv` (66148394) and `plan_e.tsv` (66152731). Each plan row lists its own config.

## Token budget per batch

| toks | 4×12 (48 cores) | 6×8 (48 cores) |
|---|---|---|
| 256 | — | 0.679 |
| 512 | — | 0.696 |
| 1024 | 0.628 | 0.688 |
| 2048 | 0.525 | 0.594 |
| 4096 | 0.442 | 0.498 |
| 8192 | 0.383 | — |
| 16384 | 0.323 | — |

On CPU, small batches are faster, which is the opposite of the GPU. The GPU was flat from 4k to 32k tokens (journal `eefa4f4e`). A 1,024-token batch's FFN activation (1,024 × 5,120 fp32) is about 21 MB, which fits a worker's L3. At 4,096 tokens it is 84 MB and spills to DRAM. Use 512-1,024. Below 512 there is no further gain. Sequences longer than the budget are batched alone either way.

## The maxsep step on CPU

`calls.py` times CLEAN's own stages, then reruns them with the GMM list loaded once (`cachedgmm`), then runs an in-memory path (`fast`): one MLP pass, a batched distance matrix, a stable top 10, CLEAN's own `maximum_separation`, and one `predict_proba` per GMM over every chosen distance.

| ORFs | stock total (s) | of which writer | writer, GMM cached | fast (s) | stock ms/ORF | fast ms/ORF | rows identical |
|---|---|---|---|---|---|---|---|
| 600 | 3.2-15.7 | 2.1-2.3 | 1.7-1.9 | 0.38-0.57 | 5-26 | 0.6-1.0 | 746/746 |
| 4,468 (DH10B) | 20-26 | 15.0-15.7 | 12.7-13.0 | 2.5-3.1 | 4.5-5.9 | 0.6-0.7 | yes |
| 5,000 | 23.1-24.1 | 17.1-17.5 | 14.3 | 1.6-2.5 | 4.6-4.8 | 0.3-0.5 | 6,156/6,156 |
| 20,000 | 90-113 | 67-85 | 57-64 | 7.0-8.9 | 4.5-5.7 | 0.35-0.44 | 24,608/24,608 |

Stock total is the sum of the `.pt` round trip, the dict-of-dicts distance map, the DataFrame, and the writer.

- **The pickle-in-loop costs about 15% of the writer.** The writer reloads `gmm_ensumble.pkl` for every EC call. Caching it saves 10-20 s per 20,000 ORFs. The rest of the cost is 40 single-row `predict_proba` calls per EC call, plus pandas `nsmallest` per ORF. A cached-GMM patch alone is therefore not worth shipping. The fast path removes both costs.
- **The fast path pays for itself.** Stock maxsep runs serially while the task holds all 48 cpus, so it adds 0.22-0.27 core-s per ORF, which is 60-76 core-hours per million ORFs (12-15% on top of embedding). The fast path costs 0.017 core-s per ORF. It also drops the one-`.pt`-per-ORF round trip, which is the reason the transform batches 20,000 ORFs at a time.

The commands were `sbatch --cpus-per-task=48 --mem=96G prod.sbatch <name> bf16 <P> <T> 20000 1 [toks] [slim]`, jobs 66146033-35, 66148395-96 and 66151628-30. The maxsep timings are in `out/prod/<name>.json` under `maxsep_timings`.

## Stacks and kernels tried (option 4)

Every row is 6×8 pinned on 48 cores of an idle node. All values are ORF/s/core.

| stack | toks 4096 | toks 1024 |
|---|---|---|
| stock torch 2.0 (AVX2 ATen dispatch) | 0.498 | 0.688 |
| stock, `ATEN_CPU_CAPABILITY=avx512` | 0.377 | — |
| torch 2.5.1 CPU wheel | 0.443 | — |
| torch 2.5.1 + SDPA (drop ESM's unused attention weights) | 0.546 | 0.679 |
| IPEX 2.5.0 `ipex.optimize(bf16)` | — | 0.526 (12×4 t512: 0.572) |
| stock, fp32 | 0.194 | 0.217 |
| torch 2.5.1, fp32 | 0.193 | — |

- **A newer stack does not help.** torch 2.5 plus SDPA wins 10% at 4,096 tokens, where attention traffic is large. At 1,024 tokens it is level with the stock image. IPEX is slower. Forcing AVX-512 ATen kernels on Zen 5 is 24% slower. So the image's torch 2.0 is not the bottleneck once the batches fit in cache, and no overlay is recommended.
- **SDPA needs torch 2.5.** On torch 2.0 it fails under bf16 autocast (`Expected attn_mask dtype to be bool or to match query dtype`, `logs/clean_scale_66146026.log`, row `c_sdpa_6x8`).
- **IPEX needs a workaround.** It fails on ESM's fused attention call (`expecting query weights shape of (1280, 1280), but got torch.Size([20, 80, 8, 64, 2])`, same log, rows `c_ipex*`). The `e_ipex*` rows route attention through ESM's per-projection path (`onnx_trace = True`).
- **fp32 on MKL-on-AMD is the slow path.** bf16 is 3.2x fp32 at 1,024 tokens. The newer MKL in torch 2.5 does not change fp32 (0.193 against 0.194).

The overlay wheels sit in `wheels/`. `scale.sbatch` installs them per job onto `$SLURM_TMPDIR` with `pip install --no-deps --target`, so the overlay costs no scratch inodes.

## The transform's own path on CPU

`stock.sbatch` (job 66143784) runs `/app/CLEAN_infer_fasta.py` exactly as `clean.py`'s wrapper does on a 48-cpu node with `OMP_NUM_THREADS=48`, which is what metasmith's apptainer launch sets (`src/metasmith/env/environment.py`). That path is fp32, one process and 4,096 tokens. It took 183.8 s for the 600-ORF sample, including model load and maxsep: 3.3 ORF/s per task, or 0.068 ORF/s/core. The tuned task does 28-36 ORF/s on the same 48 cpus. Removing `gpus=Gpus.REQUIRED` from `clean.py` without changing its wrapper would therefore cost about 9x the core-hours of a tuned CPU variant.

## Memory

| job | layout | checkpoint | sacct MaxRSS |
|---|---|---|---|
| 66143784 stock | 1×48 | shipped | 19 GB |
| 66146035 | 1×12 t4096 | shipped | 21 GB |
| 66151630 | 3×4 t512 | slim | 22 GB |
| 66151629 | 6×8 t1024 | slim | 47 GB |
| 66148395 | 6×8 t1024 | shipped | 74 GB |
| 66151628 | 12×4 t512 | slim | 76 GB |

The shipped `esm1b_t33_650M_UR50S.pt` is 7.8 GB because it carries `optimizer_history` and `last_optimizer_state`. Every worker `torch.load`s all of it. `slim_ckpt.py` writes the 2.6 GB `args` + `model` copy that fair-esm needs, and it lowers the 6×8 peak from 74 GB to 47 GB at no cost in throughput (`e_slim_6x8_t1024` 0.690 against 0.688). Both values fit inside fir's 4 GB/core entitlement (192 GB at 48 cpus).

## ESM-1b on the GPU

`gpu.sbatch` and `gpu_plan.sbatch` run `embed.py --device cuda` with the image's own torch (2.0.0+cu118) on the same 20,000-ORF sample (mean 253.5 aa) and the same DH10B set. Account `def-shallam_gpu`.

- **Native kernels:** the image's arch list includes `sm_90`. Inside the job, `torch.cuda.get_device_name` reported `NVIDIA H100 80GB HBM3 MIG 3g.40gb`, capability (9, 0). So no PTX JIT is involved, unlike COMEBin's image.
- **The MIG handle:** the job forwards its `CUDA_VISIBLE_DEVICES=MIG-<uuid>` through `--cleanenv`. `nvidia-smi --query-compute-apps` showed the process resident on the slice (2,446 MiB, `logs/gpu_smi_mig3g_66162190.log`).
- **Accuracy:** GPU fp32 embeddings are bit-identical to CPU fp32 (cosine 1.0, 2,400 of 2,400 calls identical), and the DH10B scores equal the production run's.

| GPU, 20,000 ORFs | toks | embed wall (s) | ORF/s | peak GPU memory |
|---|---|---|---|---|
| 3g.40gb fp32 (as `clean.py` runs it) | 4096 | 451.3 | 44.3 | 3.5 GB |
| 3g.40gb fp32 | 8192 | 435.5 | 45.9 | 4.3 GB |
| 3g.40gb fp32 | 16384 | 406.4 | 49.2 | 6.0 GB |
| 3g.40gb bf16 autocast | 1024 | 143.3 | 139.6 | 4.3 GB |
| 3g.40gb bf16 autocast | 4096 | 125.4 | 159.5 | 5.0 GB |
| 3g.40gb bf16 autocast | 16384 | 133.3 | 150.0 | 7.8 GB |
| 1g.10gb fp32 | 4096 | 1607.0 | 12.4 | 3.5 GB |
| 1g.10gb bf16 autocast | 4096 | 416.9 | 48.0 | |
| 3g.40gb, `CLEAN_infer_fasta.py` end to end (model load, one `.pt` per ORF, stock maxsep) | 4096 | 627.5 total | 31.9 | |

The jobs were 66162190 (3g), 66168869 (3g, `gpu_plan_a.tsv`), 66164799 (1g fp32, cancelled after its first row) and 66172961 (1g bf16, `gpu_plan_b.tsv`). A full-H100 job (66162191, `--gpus=h100:1`) sat in `ReqNodeNotAvail` from 15:37 to 17:44 and was cancelled unrun, so the full card is not measured. `sbatch --test-only` had offered it a 17:16 start, which did not happen.

- **The ESM-C proxy was optimistic.** The real ESM-1b fp32 rate is 11.2k residues/s per 3g slice. The proxy measured 14.3k (48 ORF/s at 298 aa). Every GPU cost in round 1 was therefore 27% too low.
- **bf16 is a free 3.6x on the GPU too.** On DH10B, GPU bf16 scores F1 0.9291 at the 0.02 floor (fp32: 0.9295), with AUC 0.9031. On the metagenome sample it keeps 99.25% of the lane-kept EC sets. `clean.py` runs fp32 today.
- **A 1g.10gb slice with bf16 matches a 3g.40gb slice with fp32,** at 48 ORF/s for a seventh of a card. bf16 peaks at 5 GB of GPU memory on the 3g slice, so it fits a 1g.10gb slice.

## Wall-time multiplier at equal ORFs

The same 20,000 ORFs, embed wall only. A multiplier above 1 means the CPU shape is slower than one 3g.40gb slice.

| CPU shape (bf16, pinned) | embed wall (s) | ORF/s | × vs slice fp32 (451 s) | × vs slice bf16 (125 s) | cores matching one slice, fp32 / bf16 |
|---|---|---|---|---|---|
| 48-cpu by-core task, 6×8 t1024 (3 runs) | 559-708 | 28.2-35.8 | 1.24-1.57 | 4.5-5.6 | 59-75 / 214-271 |
| 48 cores of an idle node | 585.3 | 34.2 | 1.30 | 4.7 | 62 / 224 |
| 96-cpu by-core task, 12×8 t1024 | 382.9 | 52.2 | 0.85 | 3.1 | 81 / 293 |
| 96 cores of an idle node | 368.3 | 54.3 | 0.82 | 2.9 | 78 / 282 |
| full 192-core node, 24×8 t512 | 189.3 | 105.6 | 0.42 | 1.51 | 80 / 290 |
| full 192-core node, 64×3 t512 | 179.0 | 111.7 | 0.40 | 1.43 | 76 / 274 |

So one 3g.40gb slice-hour equals 59-81 CPU core-hours against `clean.py` as it runs today (fp32), or 214-293 core-hours against a bf16 GPU lane. A full node outruns a slice running fp32 by 2.4-2.5x. It runs at two thirds of the speed of a slice running bf16.

The jobs were 66148395, 66151629 and 66162199 (48-cpu), 66162198 (96-cpu) and 66166449 (`plan_f.tsv`, idle node; its first attempt, 66162197, died with NODE_FAIL).

## Cost against the GPU route

All figures are measured on the 253.5 aa sample, then rescaled linearly to Cyanoverse's mean of 298 aa. The rescale is **UNVERIFIED**.

| per million ORFs at 298 aa | |
|---|---|
| GPU 3g.40gb, fp32 (today's `clean.py`) | 7.4 slice-hours |
| GPU 3g.40gb, bf16 | 2.0 slice-hours |
| GPU 1g.10gb, bf16 | 6.8 1g-slice-hours (about 2.3 3g-slice-hour equivalents) |
| CPU tuned, 137-161 residues/s/core | 514-604 core-hours |
| CPU, the transform's stock path | about 4,900 core-hours |

The CPU planning rates are a node fully packed with our own workers (137 residues/s/core) and the median of the 20,000-ORF by-core runs (161).

Count the cpus a GPU task also holds (`clean.py` asks for 4). Then the fp32 GPU lane costs 30 core-hours per million ORFs against 514-604 for CPU, a factor of 17-20. The bf16 GPU lane costs 8 core-hours, a factor of 63-74. COMEBin's ratio was 23x (864 against 37 core-hours a sample, journal `298eb577`), and Tony moved COMEBin to the CPU lane anyway (precedence `3c8efc8176dca078`).

### Engcyc projection **UNVERIFIED**

These figures assume 1,200 non-Cyanoverse metagenome samples at 300,000 ORFs each, which is 360 M ORFs, with Cyanoverse's mean ORF length of 298 aa. Centroid dedup assumes Cyanoverse's 6.5x ratio (99.4 M ORFs to 15.35 M centroids). Engcyc's own ratio is not measured.

| load | ORFs | GPU 3g slice-hours, fp32 | GPU 3g slice-hours, bf16 | CPU tuned core-hours | CPU stock-path core-hours |
|---|---|---|---|---|---|
| all ORFs | 360 M | 2,650 | 740 | 185,000-217,000 | about 1.75 M |
| centroids (6.5x) | 55.4 M | 410 | 115 | 28,500-33,500 | about 270,000 |

At 2,000 concurrently running cores, the CPU wall is 3.9-4.5 days for all ORFs and 14-17 hours for centroids. Throughput is set by the account's cpu share rather than by step width, so this wall is a planning figure, not a promise. The centroid route also needs the CPU propagation of calls to cluster members, which is not costed here.

## Quantisation sweep

Every row is 6×8 pinned at 1,024 tokens on 48 cores of an idle node. Speed comes from the 2,400-ORF metagenome sample (MG) and from DH10B (DH, 4,468 proteins, mean 305 aa).

Agreement is measured against fp32 on the same inputs:
- "top EC" is the share of ORFs whose top EC call matches fp32's.
- "kept set" is the share whose EC set after the lane's 0.02 floor matches fp32's.

F1 and AUC are against the 1,273-ORF DH10B truth set (fp32: F1 0.9295 at 0.02, AUC 0.901). The modes live in `quant.py` (torch) and `ort_export.py` (ONNX Runtime).

| mode | MG ORF/s/core | vs bf16 | DH ORF/s/core | MG top EC | MG kept set | DH top EC | DH F1 @0.02 | DH AUC |
|---|---|---|---|---|---|---|---|---|
| fp32 | 0.229 | 0.33 | 0.182 | 100% | 100% | 100% | 0.9295 | 0.9010 |
| **bf16 autocast (baseline)** | 0.703 | 1.00 | 0.576 | 98.1% | 99.3% | 98.6% | 0.9295 | 0.8998 |
| bf16 weights, LayerNorm fp32, no autocast | 0.720 | 1.02 | 0.585 | 97.4% | 99.0% | 97.6% | 0.9287 | 0.9036 |
| torch 2.5 bf16 autocast | 0.647 | 0.92 | — | 97.8% | 99.1% | — | — | — |
| a. torch dynamic int8, per-channel, every Linear | 0.504 | 0.72 | stopped | 48.4% | 80.8% | — | — | — |
| b. per-channel int8 on fc1 only, rest bf16 | 0.570 | 0.81 | stopped | 45.1% | 78.3% | — | — | — |
| b. per-channel int8 on fc1+fc2, rest bf16 | 0.628 | 0.89 | stopped | 49.8% | 81.0% | — | — | — |
| c. torchao int8 weight-only, bf16 activations | 0.444 | 0.63 | 0.351 | 95.2% | 98.3% | 95.9% | 0.9295 | 0.9038 |
| c. torchao int4 weight-only (group 128) | 0.231 | 0.33 | stopped | 62.2% | 88.3% | — | — | — |
| c. torchao int8 dynamic (per-token act.) | < 0.041 | — | stopped | — | — | — | — | — |
| d. ORT fp32 | 0.413 | 0.59 | — | — | — | — | — | — |
| d. ORT dynamic int8, per-channel | 0.737 | 1.05 | 0.590 | 83.3% | 94.8% | 89.0% | 0.9290 | 0.9040 |
| d. ORT dynamic int8, per-tensor | 0.754 | 1.07 | 0.593 | 80.9% | 93.4% | 86.2% | 0.9290 | 0.9063 |
| e. SmoothQuant + torch per-channel int8 (q/k/v/fc1) | 0.446 | 0.63 | 0.357 | 91.7% | 97.6% | 93.8% | 0.9287 | 0.9036 |
| e. SmoothQuant + ORT int8, every MatMul | 0.749 | 1.07 | 0.595 | 87.9% | 95.9% | 90.9% | 0.9287 | 0.9054 |
| e. SmoothQuant + ORT int8, out_proj and fc2 left fp32 | 0.573 | 0.82 | 0.447 | 95.2% | 98.4% | 96.5% | 0.9288 | 0.9040 |
| round 1: torch dynamic int8, per-tensor, every Linear (t4096) | 0.333 | — | — | 46.9% | 80.7% | 56.5% | 0.8462 (4×12) | 0.8673 |

The commands were `sbatch scale.sbatch plan_g.tsv` (66166448) for the MG rows and the fp32/bf16 DH rows, and `sbatch scale.sbatch plan_h.tsv` (66176249) for ORT fp32, the smoothed ORT models and the other DH rows. Each was scored with `python score.py <dir> out/scale/<name>.fast.calls.tsv` and `python compare2.py out/scale {g_fp32|gd_fp32} <names>`.

- **Torch's dynamic int8 fails on accuracy, and per-channel weights do not fix it.** Branches a and b keep under half the top-EC calls, so they stopped before DH. The loss is in the activations. Torch quantises them per tensor at 7 bits (`reduce_range`), and ESM-1b's LayerNorm outputs carry outlier channels. SmoothQuant moves those outliers into the weights and lifts top-EC agreement from 48% to 92%. Every torch int8 variant is also slower than bf16, because fbgemm's dynamic path runs the rest of the model in fp32.
- **torchao gains nothing here.** On torch 2.5 without `torch.compile`, weight-only int8 dequantises per call and runs at 0.63x bf16. int4 is slower still and inaccurate. int8 dynamic activation ran at about 1.3 cores per worker and had embedded nothing after 1,221 s. It was stopped by process group (`logs/clean_scale_66166448.log`, row `g_ao_int8dyn`).
- **ONNX Runtime's int8 is the only thing faster than bf16, and only by 3-7%.** Its MatMulInteger uses AVX512-VNNI, and ORT fp32 is already 1.8x torch fp32. ORT's intra-op pool inherited the single cpu that `OMP_PROC_BIND` binds the main thread to when torch loads. The first ORT fp32 row (66166448, `g_ort_fp32`) therefore ran on one core per worker and was stopped. `embed.py` now restores the worker's cpu block before it creates the session.
- **DH10B F1 is not a sufficient gate.** Every int8 variant scored on DH10B, except round 1's, stays within 0.001 F1 of fp32 on DH10B, including ones that change the top EC on 8-19% of metagenome ORFs. DH10B's adjudicable proteins are well characterised, and CLEAN calls them with high confidence. The metagenome ORFs, mostly dark, are where precision loss shows.

**Recommendation: keep bf16.** By the letter of the criterion, ORT dynamic int8 (per-tensor, or SmoothQuant on every MatMul) beats bf16 on ORF/s per core by 3-7%, with DH10B F1 within 0.0008 of fp32. But it changes the lane-kept EC set on 4-7% of metagenome ORFs, against 0.7% for bf16. It also needs an ONNX export and a second runtime. Neither cost is worth 3-7%. No quantisation variant both beats bf16 and keeps bf16's agreement with fp32.

## Recommended `Resources(...)` for a CPU variant of clean.py

Make a second transform, for example `clean_cpu.py`, with the same `model` and products:

    Resources(cpus=48, memory=Size.GB(64), duration=Duration(hours=3))

This assumes the ORF input is chunked to 100,000 ORFs or fewer per task. Its expected wall is about 1.3 h at the conservative rate, so 3 h is the shortest band at more than 2x margin. **UNVERIFIED** (extrapolated from 20,000-ORF runs). For an unchunked 300,000-ORF sample, use `Duration(hours=12)`, which is about 3.8 h expected. Use `Size.GB(96)` instead of 64 if the wrapper keeps the shipped 7.8 GB checkpoint.

The wrapper must differ from `clean.py`'s in these ways. Each measurement above is why.

1. Embed in-process with `cpus / 8` spawned workers of 8 threads each (spawn, not fork). Pin each worker to its block before `import torch`. Set `OMP_NUM_THREADS=8`, `OMP_PROC_BIND=close` and `OMP_PLACES=cores` per worker. **CAUTION** metasmith exports `OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK` into the container, and that gives the single-process 48-thread case, the slowest layout measured.
2. Run under bf16 autocast with length-sorted batches of at most 1,024 tokens.
3. Load a slim checkpoint written once per task to `$SLURM_TMPDIR`, as `slim_ckpt.py` does.
4. Replace `CLEAN_infer_fasta.py` with the in-memory maxsep (`calls.py`, the `fast` block). That drops the per-ORF `.pt` files and the 20,000-ORF batch loop, and keeps the output format and the coverage check.

### The GPU lane, if it stays

Two changes to `clean.py` would cut its GPU cost by about 3.6x and 3x again. Both are measured above.

1. Run ESM-1b under `torch.autocast("cuda", dtype=torch.bfloat16)`. DH10B F1 moves from 0.9295 to 0.9291.
2. Ask for `gpu_memory=Size.GB(10)` instead of 16, so it resolves to a 1g.10gb slice. bf16 peaks at 5 GB, and a small slice backfills where a large one waits (`fir-mig-beats-full-h100-queue`).

## Lessons carried over from COMEBin

- **Measure the curve on one sample.** COMEBin's 48-core choice rested on two points from different samples, which mixed sample size with core count (journal `5200c979`). Every scaling row here draws from one seeded, prefix-consistent sample and also reports residues/s.
- **Use spawn, not fork.** COMEBin's sweep deadlocked because it forked a pool from a threaded parent (`comebin-leiden-sweep-deadlocks-at-width`). The embed workers here are started with `spawn` before torch loads.
- **Cost the lane in core-hours.** COMEBin's decision weighed 864 against 37 core-hours a sample (journal `298eb577`) and took the CPU lane (precedence `3c8efc8176dca078`). The cost section here uses the same terms.
- **Keep the task count down.** fir counts every array element against a queue of about 1,000 (`fir-queue-cap-counts-array-elements`). That favours 48-cpu tasks over 100,000-ORF shards (3,600 tasks for Engcyc) rather than 12-cpu tasks over 25,000-ORF shards (14,400), although 12-cpu tasks were as efficient per core.
- **Throughput is the account's share.** A wall projection is a planning figure only (`throughput-is-account-share-not-step-width`).
- **Check the GPU really runs native kernels.** COMEBin's image had no sm_90 cubin and silently JIT-compiled (`comebin-image-has-no-sm90-cubin`). The CLEAN image lists sm_90, and each GPU job prints the arch list and device name.
- **Forward the MIG handle.** COMEBin reached its slice only when the MIG handle was exported. `gpu.sbatch` forwards a `MIG-` `CUDA_VISIBLE_DEVICES` through `--cleanenv`, and only a MIG handle.
- **Size the slice to the model.** COMEBin's 8 GB fitted a 1g.10gb slice. ESM-1b peaks at 3.5-5 GB, so the 1g.10gb slice was measured too.
- **Keep small files node-local.** The per-ORF `.pt` files went to `$SLURM_TMPDIR`, and so did the torch overlay. The fir tree holds about 200 files.

## Commands

Every run used `/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif`. CPU runs used account `rrg-shallam-ab`, and GPU runs used `def-shallam_gpu`.

- Round 1, by-core 48 cpus, `configs.tsv` rows 1-11: `sbatch bench.sbatch` (66141447), with rows 3, 6 and 11 rerun after the int8 fix (`sbatch --array=3,6,11 bench.sbatch`, 66143181). Rows 1-7 and 11 are on shared nodes at 600 ORFs and are noisier than the later rounds.
- Rounds 2-5, whole node: `sbatch scale.sbatch plan_{a,b,c,d,e}.tsv`.
- By-core production size: `sbatch --cpus-per-task=48 --mem=96G prod.sbatch <name> bf16 6 8 20000 1 1024 1`, with the other jobs varying the arguments as their names show.
- The transform's own path: `sbatch stock.sbatch`.
- GPU: `sbatch --gpus=nvidia_h100_80gb_hbm3_3g.40gb:1 gpu.sbatch mig3g`, and `sbatch --gpus=<gres>:1 gpu_plan.sbatch gpu_plan_{a,b}.tsv`, both on account `def-shallam_gpu`.
- Quantisation and equal-ORF CPU rounds: `sbatch scale.sbatch plan_{f,g,h}.tsv`. `smoke.sbatch` checks that each mode loads and embeds 64 ORFs before a whole-node round.
- Tables: `python3 summarize.py . > summary.tsv`, then `python score.py`, `python compare.py` and `python compare2.py` inside the image, as above.

## Files left on fir

`wheels/` (293 MB, 11 files) holds the overlay wheels: torch 2.5.1 CPU, IPEX 2.5.0, torchao 0.7.0, onnx 1.17.0, onnxruntime 1.20.1 and their small dependencies. Every overlay and ONNX export went to node-local `$SLURM_TMPDIR`, so none survives a job. `out/scale/g_fp32.npy` and `out/scale/gd_fp32.npy` are the fp32 reference embeddings. Every other `.npy` file was deleted after scoring.

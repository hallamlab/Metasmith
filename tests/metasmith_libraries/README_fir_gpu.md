# Running the deep-learning transforms on fir

Verified end-to-end on 2026-05-19 using `/scratch/phyberos/dl_testing_claude/`.
Six embedding models confirmed running standalone on H100 MIG slices, plus
a Nextflow -> SLURM pipeline submitting GPU jobs from the login node.

## Cluster facts (per the Alliance Fir doc + observed)

- Compute Canada / DRAC site, SLURM 24.11.6
- Container runtime: **Apptainer 1.3.5** (`singularity` symlinked)
- Compute nodes have **internet access** — HF downloads work inside any allocation
- Min job duration: **5 min** (test) / 1 hr (regular); max **7 days**
- GPU nodes: 1× AMD EPYC 9454 (48 cores, NPS=4), 1125 GB RAM, 4× H100 80GB SXM5 + NVLink
- MIG slices on H100: `1g.10gb`, `2g.20gb`, `3g.40gb`; full card via `--gpus=h100:1`

| Slice flag | VRAM | Covers |
|---|---|---|
| `--gpus=nvidia_h100_80gb_hbm3_1g.10gb:1` | 10 GB | ESM-C 300M, Ankh-base, ProtT5-XL (small batch) |
| `--gpus=nvidia_h100_80gb_hbm3_2g.20gb:1` | 20 GB | ESM-C 600M, ProtT5-XL (larger batch) |
| `--gpus=nvidia_h100_80gb_hbm3_3g.40gb:1` | 40 GB | + SaProt 650M, ESMFold (short-to-moderate seqs) |
| `--gpus=h100:1`                          | 80 GB | + Boltz-2, long ESMFold targets |

- GPU account on fir for this project: **`def-shallam_gpu`**
- Partitions:
  - **`gpubase_interac`** (3 h limit) — for `salloc` interactive sessions
  - **`gpubase_bygpu_b1..b5`** — for `sbatch` / Nextflow-submitted jobs

`salloc` and `sbatch` use different partitions on fir; `salloc --partition=gpubase_interac`
and `sbatch --partition=gpubase_bygpu_b1` are the working combos. Omit `--partition`
on sbatch and the scheduler picks one automatically.

## Interactive node pattern (Stage 1+2: standalone scripts)

Preferred for iterative debugging — one allocation, many `srun` dispatches:

```bash
# Smallest viable slice (default for most embedding models):
salloc --no-shell \
  --account=def-shallam_gpu --partition=gpubase_interac \
  --gpus=nvidia_h100_80gb_hbm3_1g.10gb:1 \
  --cpus-per-task=4 --mem=16000M --time=1:00:00 \
  --job-name=dl-interactive
# salloc prints "Granted job allocation <JOBID>"; capture it
JOBID=$(squeue -u $USER -h -n dl-interactive -o %i | head -1)

# Dispatch each test as a separate srun to the allocated node:
srun --jobid=$JOBID apptainer exec --nv \
  --bind /scratch/phyberos/dl_testing_claude:/work \
  --bind /etc/pki:/etc/pki:ro \
  /scratch/phyberos/dl_testing_claude/scaffold/pytorch_240_cu124.sif \
  python /work/manual_scripts/infer_esmc.py --weights /work/weights/esmc_300m ...

# When done:
scancel $JOBID
```

Why `--bind /etc/pki:/etc/pki:ro`: the host CA bundle is needed for HTTPS to HF/quay.
Why `PYTHONUSERBASE=/work/pyuser`: container fs is read-only; pip --user lands in scratch.

## Verified model invocations (Stage 2)

All run inside the scaffold container `pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime`.
Test FASTA: 5 short bacterial proteins (50-150 aa) at `/scratch/phyberos/dl_testing_claude/fixtures/tiny_orfs.faa`.

| Model | Slice | Output shape | Notes |
|---|---|---|---|
| ESM-C 300M | 1g.10gb | (5, 960) | `INFRA_PROVIDER=local` + `chdir(weights_dir)` so `data_root()` returns "" and the relative path resolves |
| Ankh-base  | 1g.10gb | (5, 768) | T5EncoderModel via HF transformers, fp16 |
| ProtT5-XL  | 1g.10gb | (5, 1024) | `pytorch_model.bin` only needed (~11 GB); skip TF/Flax variants |
| ESMFold v1 | 3g.40gb | 5 PDB files | `EsmForProteinFolding`, fp16 on the `esm` trunk |
| Foldseek 3Di | CPU | 5x (aa, di3) | `foldseek structureto3didescriptor file1.pdb file2.pdb ... out.tsv` (positional args, NOT a list file) |
| SaProt 650M | 3g.40gb | (5, 1280) | `EsmModel` with structure-aware token concat (aa + lowercase 3Di) |
| Boltz-2 | CPU | import-only | Single-sequence inference path scoped but not run |

### Bugs in the original transform sources (fixed)

- **`esm_c.py`**: `ESMC.from_pretrained(path)` is wrong — the SDK only accepts
  registered model names (`esmc_300m` / `esmc_600m`). Fix: set
  `INFRA_PROVIDER=local`, chdir into the weights bundle, then call
  `ESMC.from_pretrained("esmc_300m")`. The relative path
  `data/weights/<file>.pth` resolves to the local file.
- **`foldseek_3di.py`**: (a) `structureto3didescriptor` takes PDB files as
  positional args, not a list-file. (b) `sid = name.split("_")[0]` lost any
  underscore in the sequence ID — preserve the full stem, only strip a
  trailing single-letter chain suffix.
- **Downloaders** (`download*.py`): they still call deprecated `huggingface-cli download`;
  works for now but should migrate to `hf download` (the new CLI).

## HPC pattern (Stage 4: Nextflow -> SLURM from login node)

Validated end-to-end. Deliverables:

- `tests/manual/fir_slurm/fir_slurm.config` — Nextflow SLURM executor config
  with `cpu_small`, `cpu_large`, `gpu_small`, `gpu_med`, `gpu_full` labels mapping
  to MIG slices and accounts.
- `tests/manual/fir_slurm/stage4_test.nf` — minimal 2-process pipeline (CPU + GPU)
  used to verify the wiring.

Run from the login node (no `salloc` — Nextflow does its own submission):

```bash
module load java/21.0.1
W=/scratch/phyberos/dl_testing_claude
$W/nextflow_bin/nextflow run $W/manual_scripts/stage4_test.nf \
  -c $W/manual_scripts/fir_slurm.config \
  -work-dir ./work
```

Confirmed observations (job IDs 40541215 + 40541298):

- `cpu_demo` (label `cpu_small`) submitted to `cpubase_bycore_b1`, cpus=4, exit 0
- `gpu_demo` (label `gpu_small`) submitted to `gpubase_bygpu_b1`, MIG `nvidia_h100_80gb_hbm3_1g.10gb`,
  `apptainer exec --nv` saw the H100 inside the container

The GPU process spec confirmed via `sacct`:
```
ReqTRES = billing=...,cpu=4,gres/gpu:nvidia_h100_80gb_hbm3_1g.10gb=1,mem=16000M,node=1
```

### Mapping transform `Resources(...)` to SLURM labels

For metasmith integration, the bridge from transform `Resources(cpus, memory, duration)`
+ a `gpus` hint to the Nextflow label is:

| Transform | `gpus` hint | Suggested label |
|---|---|---|
| ESM-C 300M / Ankh-base | 1 | `gpu_small` |
| ProtT5-XL (small batch) | 1 | `gpu_small` |
| ESMFold (short seqs) | 1 | `gpu_med` |
| SaProt 650M | 1 | `gpu_med` |
| Boltz-2 single-seq | 1 | `gpu_full` |
| Downloaders | 0 | `cpu_small` |

This is the wire-up that metasmith's generated workflow needs to emit
once the upstream "container args" feature (for `--nv`) lands. Until then,
the `--nv` flag is passed by editing `metasmith/coms/containers.py`
locally to inject `--nv` in `MakeRunCommand` for the apptainer runtime.

## Filesystem layout used

| Asset | Path |
|---|---|
| Workspace root | `/scratch/phyberos/dl_testing_claude/` |
| Scaffold container | `scaffold/pytorch_240_cu124.sif` |
| Per-model weights | `weights/{esmc_300m,esmc_600m,ankh_base,ankh_large,prott5_xl,esmfold_v1,saprot_650m,boltz_2}/` |
| HF cache | `hf_cache/` (`HF_HOME=...`) |
| pip user packages | `pyuser/` (`PYTHONUSERBASE=...`) |
| Manual inference scripts | `manual_scripts/infer_*.py` |
| Foldseek static binary | `foldseek_bin/foldseek/bin/foldseek` |
| Nextflow binary | `nextflow_bin/nextflow` |
| Test FASTA | `fixtures/tiny_orfs.faa` |
| Run outputs | `runs/c1_esmc/`, `runs/c2_ankh/`, ..., `runs/stage4/` |

Roughly 54 GB of weights total across all 7 models (PyTorch + alt formats).

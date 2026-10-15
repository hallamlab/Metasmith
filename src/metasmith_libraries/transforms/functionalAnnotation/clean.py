# CLEAN contrastive EC prediction for the ORFs -> clean_predictions.
#
# CLEAN (Yu et al., "Enzyme function prediction using contrastive learning",
# Science 2023) places a query enzyme by *function*: its supervised-contrastive
# objective pulls convergent-function / divergent-sequence enzymes together, so it
# reaches enzymes the homology channels (kofam KO, UniRef DIAMOND) miss. It is
# sequence-input but homology-defeating by construction.
#
# Runs the baked `external_clean` image (CUDA torch + ESM-1b weights + CLEAN
# maxsep assets under /app). ESM-1b (650M) mean-embeds the ORFs on the GPU, then
# CLEAN max-separation inference (`CLEAN_infer_fasta.py`) compares each query to the
# EC-cluster-center embeddings and emits, per ORF, the selected EC set with a
# GMM-calibrated confidence per call. Output is standardized to the 3-col TSV the
# GPR mapper consumes:
#
#     Query ID <TAB> Predicted EC number <TAB> clean_score
#
# `clean_score` IS A CONFIDENCE -- higher is better, bounded by 1 -- not the distance
# to the EC cluster centre. The 8.06 in CLEAN's own worked example is its un-calibrated
# path, and citing it is how this file once concluded the opposite; the calibrated one
# is what runs here, because the workspace below links `data/pretrained/gmm_ensumble.pkl`
# into place. The direction was settled by measurement, not by that reading. Against
# the 1,288-ORF DH10B truth set: correct calls sit at a
# median clean_score of 0.9973 and wrong ones at 0.1328, AUC 0.897 in the
# higher-is-better direction, and the ORFs with no known EC at all average 0.044 against
# 0.884 for those that have one. The mapper stored it through a 1/(1+d) inversion until
# that was measured, which ranked every CLEAN call backwards.
#
# CLEAN never abstains -- a full level-4 EC for ~99% of ORFs -- so the mapper drops
# calls below `fabfos_evidence.CLEAN_MIN_SCORE` at parse time, where kofam drops a hit
# below its family threshold. Do not threshold here: this transform emits what CLEAN
# said, and the lane decides what to keep. CLEAN writes relative to CWD and /app is read-only under apptainer, so we
# run from a writable, bind-mounted /clean_ws that symlinks the baked read-only
# assets. Retyped from cyanoverse functionalAnnotation/clean_lane.py onto the dev2
# sequences::orfs -> annotation::clean_predictions scheme.
#
# GATED: needs the external_clean image (~ships ESM-1b weights + pretrained bundle)
# and a GPU to be practical.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::clean.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
pred  = model.AddProduct(lib.GetType("annotation::clean_predictions"))

WRAPPER = r'''
import argparse, os, subprocess, sys

p = argparse.ArgumentParser()
p.add_argument("--fasta", required=True, help="input ORF FASTA (container path)")
p.add_argument("--out", required=True, help="output standardized TSV (container path)")
p.add_argument("--app", default="/app", help="baked CLEAN install root")
p.add_argument("--workdir", default="/clean_ws", help="writable working dir (bind-mounted)")
p.add_argument("--batch", type=int, default=20000,
               help="ORFs per inference pass. CLEAN's extractor writes ONE .pt per "
                    "sequence and never removes them, so an unbatched 100,000-ORF "
                    "shard leaves 100,000 files behind; at the concurrency a corpus "
                    "campaign runs that exhausts a cluster's file quota, which fails "
                    "every OTHER job on the filesystem, not just this one. Batching "
                    "bounds the directory to one batch at a time.")
a = p.parse_args()

APP, WORK = a.app, a.workdir

# ESM-1b's alphabet. `*` is NOT in it -- and prodigal ends every protein with one.
ALPHA = set("ACDEFGHIKLMNPQRSTVWYXBUZO")

def sanitise(src, dst_dir, batch):
    """Rewrite the ORF FASTA into what CLEAN can actually consume.

    Two independent failures, both from feeding prodigal's output through unchanged:

    1. `KeyError: '*'` inside `esm.data.Alphabet.encode`. Prodigal terminates each
       protein with a stop-codon `*`, which ESM-1b has no token for. It dies during
       embedding, AFTER the model has loaded -- so it costs the whole model load first.
       Anything outside the alphabet becomes `X`; `*` is dropped outright, since it is
       a terminator rather than a residue.

    2. `FileNotFoundError: ./data/esm_data/<the entire header>.pt`. CLEAN's extractor
       names each per-sequence tensor after the FULL header line, and prodigal's
       carries its gene call -- including `rbs_motif=GGA/GAG/AGG`. Those slashes make
       the name a path into directories that do not exist. Keeping only the first
       whitespace token fixes it and is also what every other lane keys on, so the
       GPR mapper's join across lanes works by construction rather than by luck.
    """
    n = n_star = n_other = 0
    names, fout = [], None
    for line in open(src):
        if line.startswith(">"):
            if n % batch == 0:
                if fout is not None:
                    fout.close()
                names.append(f"orfs_{len(names):04d}")
                fout = open(os.path.join(dst_dir, names[-1] + ".fasta"), "w")
            n += 1
            fout.write(">" + (line[1:].split() or [f"orf_{n}"])[0] + "\n")
            continue
        s = line.strip().upper()
        n_star += s.count("*")
        out = []
        for c in s:
            if c == "*":
                continue
            if c in ALPHA:
                out.append(c)
            else:
                out.append("X")
                n_other += 1
        fout.write("".join(out) + "\n")
    if fout is not None:
        fout.close()
    print(f"[clean] {n} ORFs in {len(names)} batch(es) of <= {batch}; dropped "
          f"{n_star} stop codons, recoded {n_other} out-of-alphabet residues to X",
          flush=True)
    if n == 0:
        sys.exit("[clean] the ORF FASTA is empty")
    return names

os.makedirs(os.path.join(WORK, "data", "inputs"), exist_ok=True)
os.makedirs(os.path.join(WORK, "data", "esm_data"), exist_ok=True)
os.makedirs(os.path.join(WORK, "results", "inputs"), exist_ok=True)

def link(src, dst):
    if not os.path.lexists(dst):
        os.symlink(src, dst)

# CLEAN reads ./data/pretrained/{split100.pth,100.pt,gmm_ensumble.pkl},
# ./data/split100.csv (EC label map), and runs ./esm/scripts/extract.py -- all
# relative to CWD.
link(os.path.join(APP, "data", "pretrained"), os.path.join(WORK, "data", "pretrained"))
link(os.path.join(APP, "data", "split100.csv"), os.path.join(WORK, "data", "split100.csv"))
link(os.path.join(APP, "esm"), os.path.join(WORK, "esm"))
names = sanitise(a.fasta, os.path.join(WORK, "data", "inputs"), a.batch)

env = dict(os.environ)
env.setdefault("TORCH_HOME", "/opt/torch_cache")

ESM_DATA = os.path.join(WORK, "data", "esm_data")

def clear_esm_data():
    """One .pt per sequence, and CLEAN never removes them.

    Cleared BETWEEN batches, so the directory holds one batch's tensors rather
    than the shard's. This is the whole point of batching: on a shared cluster
    the binding limit is the filesystem's INODE quota, not its bytes, and
    exhausting it takes down every job on that filesystem.
    """
    n = 0
    for f in os.listdir(ESM_DATA):
        os.remove(os.path.join(ESM_DATA, f))
        n += 1
    return n

# Append per batch, flushing as we go: an 8-hour wall on the last batch of a
# long shard should cost that batch, not the whole shard's GPU time.
n_rows = n_orfs = 0
with open(a.out, "w") as fout:
    fout.write("Query ID\tPredicted EC number\tclean_score\n")
    for bi, NAME in enumerate(names):
        print(f"[clean] batch {bi + 1}/{len(names)}: maxsep on {NAME} (cwd={WORK})",
              flush=True)
        r = subprocess.run(
            [sys.executable, os.path.join(APP, "CLEAN_infer_fasta.py"),
             "--fasta_data", NAME],
            cwd=WORK, env=env,
        )
        if r.returncode != 0:
            sys.exit(f"[clean] CLEAN_infer_fasta.py failed on {NAME} (rc={r.returncode})")

        # Parse CLEAN's ragged maxsep CSV -> standardized 3-col TSV.
        # Format (no header): <seq_id>,EC:<ec>/<score>,EC:<ec>/<score>,...
        res_csv = os.path.join(WORK, "results", "inputs", f"{NAME}_maxsep.csv")
        if not os.path.exists(res_csv):
            sys.exit(f"[clean] expected maxsep output missing: {res_csv}")
        with open(res_csv) as fin:
            for line in fin:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split(",")
                # take the first whitespace token of the FASTA header so the ORF
                # id matches the kofam/uniref lanes (which key on `>`-header
                # field 0).
                sid = parts[0].strip().split()[0] if parts[0].strip() else parts[0].strip()
                n_orfs += 1
                for tok in parts[1:]:
                    tok = tok.strip()
                    if not tok:
                        continue
                    # "EC:3.6.1.43/8.06" -> "3.6.1.43/8.06"
                    body = tok[3:] if tok.startswith("EC:") else tok
                    ec, score = (body.rsplit("/", 1) if "/" in body else (body, "NA"))
                    fout.write(f"{sid}\t{ec.strip()}\t{score.strip()}\n")
                    n_rows += 1
        fout.flush()
        os.remove(res_csv)
        freed = clear_esm_data()
        print(f"[clean] batch {bi + 1}/{len(names)} done: {n_orfs} ORFs so far, "
              f"{freed} per-sequence tensors removed", flush=True)

print(f"[clean] wrote {n_rows} EC calls across {n_orfs} ORFs -> {a.out}", flush=True)

# COVERAGE, NOT NON-EMPTINESS. Before batching, this run either produced the
# whole table or none of it, so the caller's `n_rows > 0` was a sound success
# test. It is not any more: a crash after batch 3 of 5 leaves a TSV covering
# 60% of the shard, `n_rows > 0` calls that success, and the workflow's
# errorStrategy records no failure -- so the GPR mapper joins a lane that is
# quietly missing 40% of its ORFs and `validate_gpr`'s per-channel completeness
# check passes, because the channel is non-empty. CLEAN never abstains, so every
# input sequence must appear.
n_in = sum(1 for line in open(a.fasta) if line.startswith(">"))
if n_orfs != n_in:
    sys.exit(f"[clean] INCOMPLETE: {n_orfs} of {n_in} input ORFs carry a call. "
             f"CLEAN never abstains, so a shortfall means a batch died and its "
             f"sequences are simply absent -- which downstream reads as a "
             f"smaller proteome, not as a failure")
'''


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    opred = context.Output(pred)

    # Writable workspace bound into the container (CLEAN writes relative to CWD;
    # /app is read-only under apptainer).
    #
    # ON NODE-LOCAL DISK WHEN THE SCHEDULER OFFERS ONE. The workspace holds one
    # tensor file per sequence, and the task directory lives on the shared
    # parallel filesystem, where hundreds of thousands of tiny files are both
    # slow and charged against an inode quota shared with every other job. A
    # symlink is used rather than a different bind path so the container side is
    # unchanged and the fallback is the old behaviour exactly, on any host that
    # sets no SLURM_TMPDIR.
    context.LocalShell(
        'if [ -n "${SLURM_TMPDIR:-}" ] && [ -d "${SLURM_TMPDIR}" ]; then '
        '  rm -rf clean_ws; mkdir -p "${SLURM_TMPDIR}/clean_ws"; '
        '  ln -sfn "${SLURM_TMPDIR}/clean_ws" clean_ws; '
        'else mkdir -p clean_ws; fi'
    )
    script = Path("run_clean.py")
    with open(script, "w") as f:
        f.write(WRAPPER)

    context.ExecWithEnv(
        env=image,
        binds=[
            (context.external_cwd / "clean_ws", "/clean_ws"),
            (context.external_cwd / script.name, f"/work/{script.name}"),
        ],
        args=["--env", "TORCH_HOME=/opt/torch_cache"],
        cmd=f"""
            python /work/{script.name} \
                --fasta {iorfs.container} \
                --out {opred.container} \
                --app /app \
                --workdir /clean_ws
        """,
    )
    n_rows = sum(1 for _ in open(opred.local)) - 1 if opred.local.exists() else 0
    print(f"[clean] {n_rows:,} EC calls", flush=True)
    return ExecutionResult(
        manifest=[{pred: opred.local}],
        success=n_rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    # ESM-1b 650M dominates; embeds a fosmid-scale ORF set in minutes on a V100.
    #
    # DECLARING the GPU is what gets one allocated. Hand-writing `--nv` into the
    # container args -- which this used to do -- exposes whatever devices the node
    # already has, and under a batch scheduler that is *none*: without `gpus` the
    # step is a plain CPU process, so it lands on a CPU partition and torch reports
    # no devices with nothing raised. `Resources.gpus` is the toggle and
    # `gpu_memory` the size; how many devices that resolves to, what a device is
    # called and which sbatch flag asks for it are host facts, declared once per run
    # via `Agent.RunWorkflow(gpus=Gpu(...))`. The engine then injects the runtime's
    # own exposure flag (`--nv` under apptainer) plus whatever `Agent.gpu_args`
    # carries, so nothing runtime- or site-specific belongs in this file.
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=4),
                        gpus=Gpus.REQUIRED, gpu_memory=Size.GB(16)),
)

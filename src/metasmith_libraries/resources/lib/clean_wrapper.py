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

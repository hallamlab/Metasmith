import os, sys, time
import numpy as np
import pandas as pd

# /work/EZpred is the bind-mounted model bundle (vendored EZpred tree + MLP heads).
EZPRED_DIR = "/work/EZpred"
EMB_LAYERS = os.environ["EZPRED_LAYERS"]
EMB_INDEX  = os.environ["EZPRED_INDEX"]
ORFS       = os.environ["EZPRED_ORFS"]
OUT_CSV    = os.environ["EZPRED_OUT_CSV"]
TOP_TERMS  = int(os.environ.get("EZPRED_TOP_TERMS", "500"))
WORK       = os.environ.get("EZPRED_WORK", "/work/ezpred_run")

# `training_config["repr_layers"]`. The heads are an MLP over the three layer means
# concatenated with the three homolog means -- 6 x 1152 -- so this list is a contract
# with the trained weights, not a tuning knob.
REPR_LAYERS = [34, 35, 36]

os.makedirs(WORK, exist_ok=True)
feature_dir = os.path.join(WORK, "embed_feature")
os.makedirs(feature_dir, exist_ok=True)

# ---------------------------------------------------------------- features
# EZpred's on-disk feature format, read off `fasta2plm.homolog2esm` and
# `model.InterlabelGODataset.load_feature`: one `<name>.npy` per sequence holding a
# pickled {"name": str, "mean": {layer: vec}, "homo": {layer: vec}} of 1152-dim
# float32 vectors, which the loader concatenates in `repr_layers` order.
#
# `homo` is a copy of `mean`: the DL-only fork disables MMseqs2 homolog augmentation,
# and upstream's no-homolog branch copies the query's own embedding into that field,
# which is the input pattern the heads already see for any sequence with zero hits.
#
# THIS USED TO PASS `mean: {0: final_embedding}` -- one vector, under a layer key that
# does not exist, taken from the normalised final output rather than the raw per-block
# hidden states. It does not raise; it predicts, and the numbers look like EZpred's
# without being EZpred's.
idx = pd.read_csv(EMB_INDEX)
layers = np.load(EMB_LAYERS)
if layers.ndim != 3 or layers.shape[1] != len(REPR_LAYERS):
    raise SystemExit(
        f"esm_c_layer_means has shape {layers.shape}; expected "
        f"(n_sequences, {len(REPR_LAYERS)}, 1152) for layers {REPR_LAYERS}")
if len(idx) != len(layers):
    raise SystemExit(
        f"the index has {len(idx)} rows and the layer stack {len(layers)} -- "
        f"pairing them would attribute every prediction to the wrong ORF")

wanted = set()
for _, r in idx.iterrows():
    sid = str(r["sequence_id"])
    row = layers[int(r["index"])].astype(np.float32)
    mean = {L: row[j] for j, L in enumerate(REPR_LAYERS)}
    np.save(os.path.join(feature_dir, sid + ".npy"),
            {"name": sid, "mean": dict(mean), "homo": dict(mean)},
            allow_pickle=True)
    wanted.add(sid)
print(f"[ezpred] staged {len(wanted)} precomputed ESM-C features -> {feature_dir}",
      flush=True)

# ---------------------------------------------------------------- query FASTA
# `predict()` builds names.npy from `parse_fasta`, which keys on the WHOLE header
# line, while `load_feature` opens `<feature_dir>/<name>.npy` and the features above
# are written under the first whitespace token. Those two agree only when the header
# has no whitespace -- and prodigal's carries its full gene call. So the query FASTA
# is rewritten to bare ids here. The sequences themselves are never read: we enter
# below the embed step.
n_orf = n_kept = 0
fasta = os.path.join(WORK, "query.fasta")
with open(ORFS) as fin, open(fasta, "w") as fout:
    keep = False
    for line in fin:
        if line.startswith(">"):
            n_orf += 1
            body = line[1:].split()
            sid = body[0] if body else ""
            keep = sid in wanted
            if keep:
                n_kept += 1
                fout.write(f">{sid}\n")
            continue
        if keep:
            fout.write(line)
if n_kept == 0:
    raise SystemExit(
        f"[ezpred] none of the {n_orf} ORF ids appear in the ESM-C index -- the two "
        f"are keyed on different things and every prediction would be unattributable")
if n_kept != len(wanted):
    raise SystemExit(
        f"[ezpred] {n_kept} of {len(wanted)} embedded sequences have an ORF record "
        f"({n_orf} ORFs read). names.npy drives the DataLoader, so a short FASTA "
        f"quietly predicts for a subset")
print(f"[ezpred] query FASTA: {n_kept} sequences", flush=True)

# ---------------------------------------------------------------- inference
# Import rather than shell out: `predict.py` has no --features flag, and its
# __main__ runs main() = get_embed_features() + predict(). See the module docstring
# for why the first half has to be skipped rather than short-circuited.
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.path.insert(0, EZPRED_DIR)
os.chdir(EZPRED_DIR)
import torch
from predict import InterLabelGO_pipeline
from settings import settings_dict as settings

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[ezpred] EC heads on {device}", flush=True)

HEADS = [("enzyme", "MODEL_CHECKPOINT_DIR1", "DL1.tsv"),
         ("nonenzyme", "MODEL_CHECKPOINT_DIR2", "DL2.tsv")]

for kind, key, tsv_name in HEADS:
    result_file = os.path.join(WORK, tsv_name)
    t0 = time.time()
    InterLabelGO_pipeline(
        working_dir=WORK,
        fasta_file=fasta,
        device=device,
        top_terms=TOP_TERMS,
        aspects=["EC"],
        model_dir=settings[key],
        result_file=result_file,
    ).predict(feature_dir)
    if not os.path.exists(result_file):
        raise SystemExit(f"[ezpred] the {kind} head wrote no {tsv_name}")
    print(f"[ezpred] {kind} head -> {tsv_name} in {time.time()-t0:.1f}s", flush=True)

# ---------------------------------------------------------------- reshape
# DL1/DL2 are ALREADY LONG -- EntryID, term, score, one row per call. `predict()`
# writes a header and then hands the file to `parse_isa`, which rewrites it without
# one, so these are read headerless by position. Reading them with a header would
# silently consume the first prediction as column names; melting them as if they were
# wide (which this used to do) turns every row into nonsense.
out_rows = []
for kind, _, tsv_name in HEADS:
    path = os.path.join(WORK, tsv_name)
    df = pd.read_csv(path, sep="\t", header=None,
                     names=["sequence_id", "ec_number", "score"], dtype=str)
    df = df[df["sequence_id"].astype(str) != "EntryID"]   # in case parse_isa kept it
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df.dropna(subset=["score"])
    df["head_kind"] = kind
    print(f"[ezpred] {tsv_name}: {len(df):,} calls over "
          f"{df['sequence_id'].nunique():,} sequences", flush=True)
    out_rows.append(df[["sequence_id", "ec_number", "score", "head_kind"]])

out = pd.concat(out_rows, ignore_index=True)
if len(out) == 0:
    raise SystemExit("[ezpred] both heads produced zero calls")
out.to_csv(OUT_CSV, index=False)
print(f"[ezpred] wrote {len(out):,} rows to {OUT_CSV}", flush=True)

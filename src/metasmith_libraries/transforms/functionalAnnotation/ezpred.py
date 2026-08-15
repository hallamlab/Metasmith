"""EZpred (DL-only fork) EC-number prediction transform -> ezpred_predictions.

Runs the EZpred deep-learning EC heads (kad-ecoli/EZpred, MIT) on ESM-C 600M
embeddings. EZpred's own path re-embeds each FASTA with its internal fasta2plm.py;
here we feed it the per-layer means the `esm_c` transform already produced, so the
dataflow is esm_c -> ezpred, the expensive 600M pass runs once for both lanes, and
the two lanes are compared on the *same* embedding rather than on two passes that
merely used the same weights.

Inputs:
  - annotation::esm_c_layer_means : the layer-34/35/36 hidden-state means EZpred's
                                    heads were trained on (from esm_c)
  - annotation::esm_c_index       : sequence_id -> row index for that stack
  - sequences::orfs               : the ORFs, for the query FASTA -- see the note on
                                    header agreement in the wrapper
  - ref::ezpred_model             : the EZpred bundle -- the vendored DL-only tree
                                    plus the enzyme / non-enzyme MLP ensembles. No
                                    ESM-C weights: we never reach the embed step.
  - env::ezpred.env               : the ESM-C image (esm SDK + torch + numpy/pandas
                                    /sklearn/scipy/tqdm); EZpred has no image of its
                                    own and needs nothing the image lacks.

Output:
  - annotation::ezpred_predictions : CSV with columns
        sequence_id, ec_number, score, head_kind
    head_kind in {enzyme, nonenzyme} -- DL1.tsv + DL2.tsv concatenated. The GPR
    mapper keeps the enzyme head, level-4 ECs above the DL_EC_SCORE_FLOOR.

WE CALL `predict()`, NOT `predict.py`. Its `main()` is `get_embed_features()` then
`predict()`, and the first calls `homolog2esm`, which loads the 2.3 GB ESM-C
checkpoint *before* its skip-if-the-.npy-already-exists loop -- so precomputing the
features would not spare the load, it would only make it pointless, and it would drag
`ref::esm_c_600m_weights` in as a requirement of a step that embeds nothing.
Importing the module and entering at the second half is the whole fix.
"""
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::ezpred.env"))
ezmodel  = model.AddRequirement(lib.GetType("ref::ezpred_model"))
lay      = model.AddRequirement(lib.GetType("annotation::esm_c_layer_means"))
idx      = model.AddRequirement(lib.GetType("annotation::esm_c_index"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
out_pred = model.AddProduct(lib.GetType("annotation::ezpred_predictions"))


# Module-level constants -- same convention as esm_c.py.
TOP_TERMS = 500   # passed through as InterLabelGO_pipeline(top_terms=...)

# THERE IS NO pip install HERE, and its absence is load-bearing. This used to install
# `iterative-stratification` at task start -- which no file in the bundle imports (it is
# a cross-validation splitter for TRAINING), and which a compute node could not have
# fetched anyway: fir's nodes have no outbound network, so the step would have died on
# a dependency it does not use. Verified by importing `predict` inside
# external_esmc:2026.05.19 against the staged bundle.


WRAPPER = r'''
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
'''


def protocol(context: ExecutionContext):
    ilay  = context.Input(lay)
    iidx  = context.Input(idx)
    iorfs = context.Input(orfs)
    iez   = context.Input(ezmodel)
    ipred = context.Output(out_pred)

    wrapper = Path("ezpred_wrapper.py")
    with open(wrapper, "w") as f:
        f.write(WRAPPER)
    # predict() writes names.npy, the two TSVs and the per-sequence feature cache;
    # the bundle is bind-mounted and may be read-only, so everything mutable lives
    # in a workspace of ours rather than under the bundle's root_dir.
    context.LocalShell("mkdir -p ezpred_run")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/wrapper.name, f"/work/{wrapper.name}"),
            (context.external_cwd/"ezpred_run", "/work/ezpred_run"),
            (iez.local, "/work/EZpred"),
        ],
        cmd=f"""
            EZPRED_LAYERS={ilay.container} \
            EZPRED_INDEX={iidx.container} \
            EZPRED_ORFS={iorfs.container} \
            EZPRED_OUT_CSV={ipred.container} \
            EZPRED_TOP_TERMS={TOP_TERMS} \
            EZPRED_WORK=/work/ezpred_run \
            python /work/{wrapper.name}
        """,
    )

    # The wrapper writes the CSV only once both heads returned rows, so existence is
    # meaningful here -- but a header-only file is not.
    n_rows = sum(1 for _ in open(ipred.local)) - 1 if ipred.local.exists() else 0
    print(f"[ezpred] {n_rows:,} EC calls", flush=True)
    return ExecutionResult(
        manifest=[{out_pred: ipred.local}],
        success=n_rows > 0,
    )


# The EC-head MLPs only (5 ensemble members x 2 heads = 10), over features that are
# already on disk. NO GPU IS DECLARED, deliberately: the 600M pass happens upstream in
# esm_c, and what is left is small enough that queueing for a device would cost more
# than it saves. `predict()` uses cuda if torch sees one and cpu otherwise, so this is
# correct either way.
TransformInstance(
    protocol=protocol,
    model=model,
    group_by=lay,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

# R10 -- the ESM-C half of the labelled landmarks.
#
# The same landmarks as `label_transfer_landmarks.py` -- the same bridge cut, the same
# Swiss-Prot release, the same accessions -- embedded with ESM-C 600M instead of
# ProteinBERT, so `gpr_7lane`'s ESM-C kNN lane has something to vote against.
#
# WHY A SECOND ARTIFACT RATHER THAN A SECOND FILE IN THE FIRST ONE. The two embedders
# live in two images (`env::proteinbert.env` carries no ESM-C SDK, and `env::esmc.env`
# carries no ProteinBERT), and the ESM-C pass needs a GPU while the ProteinBERT one
# does not. Folding both into one transform would make every ProteinBERT rebuild
# queue for a device and would put the two stacks' fate in one exit code. They are
# written separately, and each carries its accessions in the same rows as its own
# embeddings -- so neither can be paired with the other's vectors by accident.
#
# SAME MODEL AS THE QUERY, and this is the whole point of the file. Cosine distance
# between two embedding spaces is a number with no referent, so the pool is only
# meaningful if it was produced by the same function as
# `functionalAnnotation/esm_c.py`'s query embeddings. The inference block below is
# that transform's, copied: same weights (`ref::esm_c_600m_weights`), same
# `max_len`/`chunk_overlap`, same sliding-window aggregation with the same length
# weights, same `out.embeddings` mean over non-special tokens. **The two must be
# changed together.** What is deliberately dropped is the per-layer means: EZpred's
# heads are not in this path, and a (222k, 3, 1152) float32 stack is 3 GB of nothing.
#
# NO RESIDUE RECODING, unlike the ProteinBERT pool. That transform recodes because
# its encoder has an off-by-one that indexes past the end of its lookup array on
# ordinal 90 ('Z') and kills the run after the model has loaded. ESM-C's tokenizer
# maps anything it does not know to its unknown token, and -- more to the point -- the
# query lane does not recode either, so recoding here would make the pool and the
# query disagree about what a rare residue is.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::esmc.env"))
source  = model.AddRequirement(lib.GetType("fabfos_data::swissprot"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
weights = model.AddRequirement(lib.GetType("ref::esm_c_600m_weights"))
pool    = model.AddProduct(lib.GetType("ref::label_transfer_landmarks_esmc"))

POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"

FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"

TABLE_NAME = "landmarks.parquet"
SOURCE_NAME = "source.txt"

MODEL_NAME = "esmc_600m"
BATCH_SIZE = 32
MAX_LEN = 2048
CHUNK_OVERLAP = 128

SELECT = r'''
import gzip
from pathlib import Path
import pandas as pd

BRIDGE = "{bridge}"
SP_ROOT = "{swissprot}"
FASTA_OUT = "_pool.faa"
LABELS_OUT = "_pool_labels.parquet"
SOURCE_OUT = "_pool_source.txt"

subs = sorted(p for p in Path(SP_ROOT).iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(f"[pool-esmc] expected exactly one Swiss-Prot release under "
                     f"{{SP_ROOT}}, found {{len(subs)}} ({{[p.name for p in subs]}}) -- "
                     f"which release the pool was built from is not recoverable from "
                     f"the embeddings")
SP = subs[0]
FASTA = str(SP / "{fasta_file}")
print(f"[pool-esmc] Swiss-Prot release {{SP.name}}", flush=True)

b = pd.read_parquet(BRIDGE, columns=["id", "id_source", "mnxr", "evidence_quality"])
sel = b[(b["id_source"] == "{id_source}") & (b["evidence_quality"] == "{evidence}")]
labels = (sel.groupby("id")["mnxr"].apply(lambda s: ";".join(sorted(set(s))))
             .rename("mnxr_list").reset_index().rename(columns={{"id": "accession"}}))
print(f"[pool-esmc] bridge slice: {{len(sel):,}} rows -> {{len(labels):,}} labelled "
      f"accessions", flush=True)
if labels.empty:
    raise SystemExit("[pool-esmc] the bridge carries no reviewed uniprot rows -- the "
                     "cut that defines the pool selected nothing")

wanted = dict(zip(labels["accession"], labels["mnxr_list"]))

written = 0
seen = set()
keep = False
with gzip.open(FASTA, "rt") as fh, open(FASTA_OUT, "w") as out:
    for line in fh:
        if line.startswith(">"):
            parts = line[1:].split("|")
            acc = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
            keep = acc in wanted and acc not in seen
            if keep:
                seen.add(acc)
                # Bare accession: the embedder echoes the first header token into its
                # index, and that is the key the labels are re-joined on.
                out.write(">" + acc + "\n")
                written += 1
        elif keep:
            out.write(line.strip().upper() + "\n")

missing = len(wanted) - written
print(f"[pool-esmc] {{written:,}} of {{len(wanted):,}} labelled accessions have a "
      f"Swiss-Prot sequence; {{missing:,}} do not", flush=True)
if written == 0:
    raise SystemExit("[pool-esmc] no pool sequences found -- the accession join broke. "
                     "The bridge's uniprot ids and Swiss-Prot's `sp|ACC|` field are the "
                     "same id space, so zero overlap is a parse bug, not a coverage fact")
if missing > 0.02 * len(wanted):
    raise SystemExit(f"[pool-esmc] {{missing:,}} of {{len(wanted):,}} reviewed accessions "
                     f"({{100.0*missing/len(wanted):.1f}}%) are absent from Swiss-Prot "
                     f"{{SP.name}}. The reviewed cut is meant to BE this release")

labels[labels["accession"].isin(seen)].to_parquet(LABELS_OUT, index=False)

reldate = SP / "{reldate_file}"
with open(SOURCE_OUT, "w") as fh:
    fh.write("sequences\tswissprot " + SP.name + "\n")
    fh.write("labels\tmnxr_lookup id_source={id_source} evidence_quality={evidence}\n")
    fh.write("embedder\tesmc_600m (ref::esm_c_600m_weights)\n")
    fh.write("sequences_written\t" + str(written) + "\n")
    fh.write("labelled_accessions\t" + str(len(wanted)) + "\n")
    if reldate.exists():
        fh.write("reldate\t" + reldate.read_text().strip().replace("\n", " | ") + "\n")
'''

# functionalAnnotation/esm_c.py's inference block, minus the per-layer means. See the
# module header: these two must change together.
EMBED = r'''
import argparse, os, time
import numpy as np
import pandas as pd
import torch

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True, help="dir containing data/weights/<file>.pth")
p.add_argument("--model-name", default="esmc_600m")
p.add_argument("--fasta", required=True)
p.add_argument("--out-npy", required=True)
p.add_argument("--out-index", required=True)
p.add_argument("--batch-size", type=int, default=32)
p.add_argument("--max-len", type=int, default=2048)
p.add_argument("--chunk-overlap", type=int, default=128)
a = p.parse_args()

# The ESM SDK's from_pretrained takes registered model NAMES, not paths, and resolves
# data/weights/<file>.pth relative to the process cwd -- hence INFRA_PROVIDER=local
# plus a chdir into the unpacked bundle.
os.environ["INFRA_PROVIDER"] = "local"
weights_dir = os.path.abspath(a.weights)
fasta = os.path.abspath(a.fasta)
out_npy = os.path.abspath(a.out_npy)
out_index = os.path.abspath(a.out_index)
os.chdir(weights_dir)

from esm.models.esmc import ESMC

if not torch.cuda.is_available():
    raise SystemExit(
        "[pool-esmc] torch sees no GPU. 222k sequences through a 600M model on CPU is "
        "not a slower run, it is a run that does not finish inside any walltime -- and "
        "a pool that is a subset of what it claims misindexes nothing but answers "
        "fewer queries, silently. Resources(gpus=REQUIRED) is what allocates one.")
device = "cuda"
client = ESMC.from_pretrained(a.model_name).to(device).eval().to(dtype=torch.bfloat16)
tok = client.tokenizer
pad_id = tok.pad_token_id
print(f"[pool-esmc] loaded {a.model_name} on {device}; batch_size={a.batch_size}; "
      f"max_len={a.max_len}; overlap={a.chunk_overlap}", flush=True)

ids, seqs = [], []
with open(fasta) as f:
    sid, buf = None, []
    for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            if sid is not None:
                ids.append(sid); seqs.append("".join(buf))
            sid = line[1:].split()[0]; buf = []
        else:
            buf.append(line)
    if sid is not None:
        ids.append(sid); seqs.append("".join(buf))
print(f"[pool-esmc] {len(seqs):,} sequences", flush=True)
if not seqs:
    raise SystemExit("[pool-esmc] the pool FASTA is empty")


def chunkify(seq, cap, overlap):
    if len(seq) <= cap:
        return [(seq, len(seq))]
    stride = cap - overlap
    out = []
    for start in range(0, len(seq), stride):
        end = min(start + cap, len(seq))
        weight = (end - start) if start == 0 else max(end - start - overlap, 1)
        out.append((seq[start:end], weight))
        if end == len(seq):
            break
    return out


flat = []
n_long = 0
for si, s in enumerate(seqs):
    parts = chunkify(s, a.max_len, a.chunk_overlap)
    if len(parts) > 1:
        n_long += 1
    for chunk, w in parts:
        flat.append((si, chunk, w))
print(f"[pool-esmc] {n_long:,}/{len(seqs):,} sequences exceeded max_len; "
      f"{len(flat):,} chunks", flush=True)

t0 = time.time()
acc_vec = [None] * len(seqs)
acc_w = [0.0] * len(seqs)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [c for _, c, _ in batch_entries]
        enc = tok(batch, return_tensors="pt", padding=True, add_special_tokens=True)
        ids_t = enc.input_ids.to(device)
        mask = (ids_t != pad_id)
        out = client(sequence_tokens=ids_t, sequence_id=mask)
        for k, (si, _, w) in enumerate(batch_entries):
            idxs = mask[k].nonzero(as_tuple=True)[0]
            if len(idxs) <= 2:
                vec = out.embeddings[k, idxs].mean(dim=0)
            else:
                vec = out.embeddings[k, idxs[1:-1]].mean(dim=0)
            vec = vec.float().cpu().numpy() * w
            if acc_vec[si] is None:
                acc_vec[si] = vec
            else:
                acc_vec[si] = acc_vec[si] + vec
            acc_w[si] += w
        if (i // a.batch_size) % 200 == 0:
            done = i + len(batch)
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(flat) - done) / rate if rate > 0 else 0
            print(f"[pool-esmc]   {done:,}/{len(flat):,} chunks "
                  f"({rate:.1f} chunk/s, ETA {eta/60:.1f} min)", flush=True)

emb = np.vstack([acc_vec[i] / acc_w[i] for i in range(len(seqs))]).astype(np.float32)
np.save(out_npy, emb)
pd.DataFrame({"sequence_id": ids, "index": list(range(len(ids)))}).to_csv(
    out_index, index=False)
print(f"[pool-esmc] {emb.shape} -> {out_npy} in {time.time()-t0:.1f}s", flush=True)
'''

ASSEMBLE = r"""
import shutil
import numpy as np
import pandas as pd
from pathlib import Path

POOL = Path("{pool}")
POOL.mkdir(parents=True, exist_ok=True)

stack = np.load("_esmc_emb.npy")
idx = pd.read_csv("_esmc_index.csv")
if len(idx) != len(stack):
    raise SystemExit(f"[pool-esmc] the index has {{len(idx)}} rows and the stack has "
                     f"{{len(stack)}}")

labels = pd.read_parquet("_pool_labels.parquet")
table = (idx.rename(columns={{"sequence_id": "accession"}})[["accession"]]
            .merge(labels, on="accession", how="left"))
n_unlabelled = int(table["mnxr_list"].isna().sum())
# Every sequence embedded was selected BECAUSE it had labels, so an unlabelled row here
# is the embedder having dropped or renamed an id, which misaligns the merge rather than
# merely thinning the set.
if n_unlabelled:
    raise SystemExit(f"[pool-esmc] {{n_unlabelled:,}} embedded sequences carry no label, "
                     f"but the landmarks were selected on having one")

table = pd.concat([table, pd.DataFrame(
    stack.astype(np.float32),
    columns=[f"dim_{{i}}" for i in range(stack.shape[1])])], axis=1)
table.to_parquet(POOL / "{table_name}", index=False)
shutil.copy("_pool_source.txt", POOL / "{source_name}")
print(f"[pool-esmc] {{len(table):,}} landmarks, "
      f"{{table['mnxr_list'].str.split(';').explode().nunique():,}} distinct MNXR, "
      f"{{stack.shape[1]}} dims", flush=True)
"""


def protocol(context: ExecutionContext):
    ibridge = context.Input(bridge)
    isrc    = context.Input(source)
    iw      = context.Input(weights)
    ipool   = context.Output(pool)

    select = SELECT.format(bridge=ibridge.container, swissprot=isrc.container,
                           fasta_file=FASTA_FILE, reldate_file=RELDATE_FILE,
                           id_source=POOL_ID_SOURCE, evidence=POOL_EVIDENCE)
    context.LocalShell("cat > _pool_select.py << 'PYEOF'\n" + select + "\nPYEOF\n")
    context.ExecWithEnv(env=image, cmd="python3 _pool_select.py")

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    with open("_pool_embed.py", "w") as f:
        f.write(EMBED)
    context.ExecWithEnv(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
            (context.external_cwd/"_pool_embed.py", "/work/_pool_embed.py"),
        ],
        cmd=f"""
            python /work/_pool_embed.py \
                --weights /weights \
                --model-name {MODEL_NAME} \
                --fasta _pool.faa \
                --out-npy _esmc_emb.npy \
                --out-index _esmc_index.csv \
                --batch-size {BATCH_SIZE} \
                --max-len {MAX_LEN} \
                --chunk-overlap {CHUNK_OVERLAP}
        """,
    )

    assemble = ASSEMBLE.format(pool=ipool.container, table_name=TABLE_NAME,
                               source_name=SOURCE_NAME)
    context.LocalShell("cat > _pool_assemble.py << 'PYEOF'\n" + assemble + "\nPYEOF\n")
    context.ExecWithEnv(env=image, cmd="python3 _pool_assemble.py")

    ok = all((ipool.local / n).exists() for n in (TABLE_NAME, SOURCE_NAME))
    return ExecutionResult(
        manifest=[{pool: ipool.local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # DECLARING the GPU is what gets one allocated -- see the note in
    # functionalAnnotation/clean.py. The embed step refuses on a CPU-only node rather
    # than running for a walltime it cannot finish in.
    resources=Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=4),
                        gpus=Gpus.REQUIRED, gpu_memory=Size.GB(24)),
)

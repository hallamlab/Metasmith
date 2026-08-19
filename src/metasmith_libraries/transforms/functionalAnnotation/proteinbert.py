from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image_pbert = model.AddRequirement(lib.GetType("env::proteinbert.env"))
image_polars = model.AddRequirement(lib.GetType("env::polars.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
out_embeddings = model.AddProduct(lib.GetType("annotation::proteinbert_embeddings_chunk"))
out_index = model.AddProduct(lib.GetType("annotation::proteinbert_index_chunk"))

POOL_ALPHABET = "ACDEFGHIKLMNPQRSTUVWXY"

SANITIZE = f'''
import sys
ALPHA = set("{POOL_ALPHABET}")
src, dst = sys.argv[1], sys.argv[2]
n = recoded = 0
with open(src) as fh, open(dst, "w") as out:
    for line in fh:
        if line.startswith(">"):
            out.write(line); n += 1
        else:
            s = line.strip()
            t = "".join(c if c in ALPHA else "X" for c in s.upper())
            if t != s:
                recoded += 1
            out.write(t + "\\n")
if n == 0:
    raise SystemExit("[pbert] the input ORF FASTA has no records")
print(f"[pbert] {{n:,}} sequences, {{recoded:,}} lines recoded to the embedder's alphabet",
      flush=True)
'''

COMBINE = '''
import sys
from pathlib import Path
import numpy as np
import polars as pl

in_dir, emb_out, idx_out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])

npys = sorted(in_dir.glob("*.npy"))
csvs = sorted(in_dir.glob("*.csv"))
if not npys or not csvs:
    raise SystemExit(f"[pbert] embedder produced no output under {in_dir}")

# Both read in the SAME sorted order, so row i of the stack is sequence i of the
# index by construction rather than by coincidence.
stack = np.vstack([np.load(f) for f in npys])[:, -512:]
idx = pl.concat([pl.read_csv(f) for f in csvs])
for cand in ("sequence_id", "id"):
    if cand in idx.columns:
        idx = idx.rename({cand: "sequence_id"})
        break
else:
    raise SystemExit(f"[pbert] the embedder index has no id column: {idx.columns}")

if len(idx) != len(stack):
    raise SystemExit(
        f"[pbert] the index has {len(idx)} rows and the stack has {len(stack)}. "
        f"The consumer addresses the stack by row, so pairing them would misindex "
        f"every row silently")

pl.DataFrame(stack, schema=[f"dim_{i}" for i in range(512)]).write_parquet(emb_out)
idx.select("sequence_id").write_csv(idx_out)
print(f"[pbert] {len(idx):,} embeddings x {stack.shape[1]} dims", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iemb = context.Output(out_embeddings)
    iidx = context.Output(out_index)

    threads = context.params.get("cpus", 8)

    sanitize = Path("sanitize_orfs.py")
    sanitize.write_text(SANITIZE)
    combiner = Path("combine_embeddings.py")
    combiner.write_text(COMBINE)

    context.ExecWithEnv().ifContainerDo(
        env=image_pbert,
        cmd=f"""
            python3 {sanitize.name} {iorfs.container} _orfs_tokenisable.faa && \
            pbert run \
                -i _orfs_tokenisable.faa \
                -o pbert_output \
                --threads {threads} \
                --protein_size 512 \
                --model_batch 1024 \
                -x 1
        """,
    )

    context.ExecWithEnv().ifContainerDo(
        env=image_polars,
        cmd=f"python {combiner.name} pbert_output {iemb.container} {iidx.container}",
    )

    return ExecutionResult(
        manifest=[
            {
                out_embeddings: iemb.local,
                out_index: iidx.local,
            },
        ],
        success=(iemb.local.exists() and iemb.local.stat().st_size > 0
                 and iidx.local.exists() and iidx.local.stat().st_size > 0),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)

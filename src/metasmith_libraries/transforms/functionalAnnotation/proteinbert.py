# ProteinBERT embeddings for the ORFs -> one self-addressing parquet per chunk.
#
# `sequence_id` sits in the same row as its 512 floats. That is the whole design: an
# embedding table that names its own rows cannot be misindexed, and misindexing is
# what this transform used to do. It emitted an id list and a stack as two products,
# paired by row, and the pairing was wrong for every input larger than one embedder
# chunk -- see the chunk-order note below.
#
# THE EMBEDDER'S CHUNK FILES DO NOT SORT INTO THE ORDER THEY WERE WRITTEN. `pbert`
# writes fixed 1,024-sequence chunks in FASTA order, named `<stem>.1.embedding.npy`,
# `<stem>.2.embedding.npy`, ... with no zero padding, so `sorted(glob("*.npy"))` gives
# `.1, .10, .11, ... .19, .2, .20, ...` -- chunk 10 stacked before chunk 2. Measured
# 2026-08-05 by re-embedding
# four assemblies through this same pinned image: single-chunk samples matched on the
# diagonal at cosine 0.999, and a 49,522-ORF sample matched at 0.480 against a best of
# 0.998. So the chunks are stacked by their integer suffix, never lexicographically,
# and the result is checked against the FASTA this transform wrote before an id is
# attached to it.
#
# THE ALPHABET IS NARROWED BEFORE THE EMBEDDER SEES IT. ProteinBERT tokenises exactly
# ACDEFGHIKLMNPQRSTUVWXY, and the image's encoder sizes its lookup array to the
# largest of those ordinals ('Y', 89) while guarding it with `c > len(arrayed_map)` --
# off by one, so a residue at ordinal exactly 90 indexes past the end and the run dies
# with `IndexError: getitem out of range` after the model has loaded. 'Z' is 90.
# Prodigal does not emit it, but this transform also runs on proteomes that are not
# prodigal's, and the same recoding is what makes the landmark set
# (build_references/compile/label_transfer_landmarks.py) comparable to this query in
# the first place -- two different alphabets are two different embedding spaces.
#
# THE INDEX COLUMN IS RENAMED HERE, ONCE. `pbert` writes `id`; every consumer reads
# `sequence_id`. Normalising at the producer means the type has one schema rather than
# each consumer guessing.
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image_pbert = model.AddRequirement(lib.GetType("env::proteinbert.env"))
image_polars = model.AddRequirement(lib.GetType("env::polars.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
out_embeddings = model.AddProduct(lib.GetType("annotation::proteinbert_embeddings_chunk"))

# Shared with label_transfer_landmarks.py -- see the header on why they must agree.
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

COMBINE = r"""
import re
import sys
from pathlib import Path
import numpy as np
import polars as pl

in_dir, faa, out_path = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])


# `<stem>.<k>.embedding.npy` with k unpadded, so the chunks order by k as an INTEGER.
# The number is INSIDE the name, not at its end -- the image announces the pattern it
# writes as `<stem>.#.embedding.npy`, and matching only a trailing number rejects every
# file the embedder produces.
def chunk_no(path):
    m = re.search(r"\.(\d+)(?:\.embedding)?$", path.stem)
    if m is None:
        raise SystemExit(
            f"[pbert] {path.name} does not end in a chunk number, so the order the "
            f"embedder wrote its chunks in is not recoverable. Guessing one would "
            f"attribute every embedding to another sequence and raise nothing")
    return int(m.group(1))


npys = sorted(in_dir.glob("*.npy"), key=chunk_no)
csvs = sorted(in_dir.glob("*.csv"))
if len(csvs) > 1:
    csvs = sorted(csvs, key=chunk_no)
if not npys or not csvs:
    raise SystemExit(f"[pbert] embedder produced no output under {in_dir}")

stack = np.vstack([np.load(f) for f in npys])[:, -512:]


def ids_of(frame):
    for cand in ("sequence_id", "id"):
        if cand in frame.columns:
            return frame.rename({cand: "sequence_id"}).select("sequence_id")
    raise SystemExit(f"[pbert] the embedder index has no id column: {frame.columns}")


idx = pl.concat([ids_of(pl.read_csv(f)) for f in csvs], how="vertical")
if idx.height != len(stack):
    raise SystemExit(
        f"[pbert] the embedder index has {idx.height} rows and its chunks stack to "
        f"{len(stack)}")

# THE ORDER IS CHECKED, NOT ASSUMED. Both the stack and the index claim to be in the
# order of the FASTA this transform wrote, and that FASTA is the one thing here that
# neither the embedder nor a glob can reorder. Disagreement means the embedder changed
# how it names or splits its output, which produces a wrong table rather than a missing
# one -- so it stops the run instead of being repaired in place.
want = [ln[1:].split()[0] for ln in faa.read_text().splitlines() if ln.startswith(">")]
got = idx["sequence_id"].to_list()
if got != want:
    bad = next((i for i, (a, b) in enumerate(zip(got, want)) if a != b),
               min(len(got), len(want)))
    raise SystemExit(
        f"[pbert] the embedder index is not in the order of the FASTA it was given: "
        f"row {bad} is {got[bad:bad+1]}, the FASTA has {want[bad:bad+1]}. Every "
        f"embedding would be attributed to another sequence")

idx.hstack(pl.DataFrame(stack, schema=[f"dim_{i}" for i in range(512)])) \
   .write_parquet(out_path)
print(f"[pbert] {len(stack):,} embeddings x 512 dims over {len(npys)} chunks",
      flush=True)
"""


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iemb = context.Output(out_embeddings)

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
        cmd=f"python {combiner.name} pbert_output _orfs_tokenisable.faa {iemb.container}",
    )

    return ExecutionResult(
        manifest=[
            {
                out_embeddings: iemb.local,
            },
        ],
        success=(iemb.local.exists() and iemb.local.stat().st_size > 0),
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

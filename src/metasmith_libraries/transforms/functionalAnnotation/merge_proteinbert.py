"""merge_proteinbert — gather per-chunk ProteinBERT outputs (parquet + index CSV).

proteinbert.py emits two products per chunk:
  - annotation::proteinbert_embeddings (parquet, 512-dim float matrix)
  - annotation::proteinbert_index (CSV mapping sequence_id -> index)

Strategy: polars vertical concat for the parquet; rewrite the `index` column
in the merged index so it is globally monotonic across chunks (chunk-local
indices would otherwise collide).
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image_polars = model.AddRequirement(lib.GetType("env::polars.env"))
parent_orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_emb    = model.AddRequirement(lib.GetType("annotation::proteinbert_embeddings_chunk"), parents={parent_orfs})
chunk_idx    = model.AddRequirement(lib.GetType("annotation::proteinbert_index_chunk"), parents={parent_orfs})
merged_emb   = model.AddProduct(lib.GetType("annotation::proteinbert_embeddings"))
merged_idx   = model.AddProduct(lib.GetType("annotation::proteinbert_index"))


MERGER = r'''
import sys, json
import polars as pl
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
out_emb = Path(sys.argv[2])
out_idx = Path(sys.argv[3])

# Concat the parquet vertically (ORF id is the stable downstream key,
# encoded in the index csv). For the index csv, preserve original
# columns verbatim and append a `global_row` column equal to the row's
# position in the merged parquet — chunk-local batch values would
# otherwise be ambiguous after the merge.
emb_frames = []
idx_frames = []
row_offset = 0
for emb_path, idx_path in manifest:
    emb = pl.read_parquet(emb_path)
    idx = pl.read_csv(idx_path)
    n = emb.height
    if idx.height != n:
        print(f"WARN: idx rows {idx.height} != emb rows {n} for {emb_path}", flush=True)
    idx = idx.with_columns(
        (pl.arange(0, idx.height) + row_offset).alias("global_row")
    )
    row_offset += n
    emb_frames.append(emb)
    idx_frames.append(idx)

if emb_frames:
    pl.concat(emb_frames, how="vertical").write_parquet(out_emb)
else:
    pl.DataFrame().write_parquet(out_emb)

if idx_frames:
    pl.concat(idx_frames, how="vertical_relaxed").write_csv(out_idx)
else:
    out_idx.write_text("id,batch,global_row\n")
'''


def protocol(context: ExecutionContext):
    import os, json
    from pathlib import Path
    emb_chunks = sorted(context.InputGroup(chunk_emb), key=lambda p: str(p.local))
    idx_chunks = sorted(context.InputGroup(chunk_idx), key=lambda p: str(p.local))
    oemb = context.Output(merged_emb)
    oidx = context.Output(merged_idx)

    # Pair embeddings with their corresponding index by sort order. The
    # splitter assigns chunks monotonic indices, and both products are
    # emitted side-by-side per chunk, so sorted-by-name pairs align.
    pairs = list(zip(emb_chunks, idx_chunks))

    script = Path("_merge_pbert.py")
    script.write_text(MERGER)
    manifest_path = Path("_pbert_manifest.json")
    manifest_path.write_text(json.dumps(
        [[str(e.container), str(i.container)] for e, i in pairs]
    ))

    context.ExecWithEnv().ifContainerDo(
        env=image_polars,
        cmd=f"python {script} {manifest_path} {oemb.container} {oidx.container}",
    )

    for cf in emb_chunks + idx_chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged_emb: oemb.local, merged_idx: oidx.local}],
        success=oemb.local.exists() and oidx.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=parent_orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

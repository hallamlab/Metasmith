from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image_polars = model.AddRequirement(lib.GetType("env::polars.env"))
parent_orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_emb    = model.AddRequirement(lib.GetType("annotation::proteinbert_embeddings_chunk"), parents={parent_orfs})
merged_emb   = model.AddProduct(lib.GetType("annotation::proteinbert_embeddings"))


MERGER = r'''
import sys, json
import polars as pl
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
out_emb = Path(sys.argv[2])

merged = pl.concat([pl.read_parquet(p) for p in manifest], how="vertical")
dupes = merged.height - merged["sequence_id"].n_unique()
if dupes:
    raise SystemExit(
        f"[pbert-merge] {dupes:,} sequence_id appear in more than one chunk. The "
        f"chunks partition the ORFs, so an overlap means the splitter and this "
        f"merge disagree about what a chunk is")
merged.write_parquet(out_emb)
print(f"[pbert-merge] {merged.height:,} embeddings from {len(manifest)} chunks",
      flush=True)
'''


def protocol(context: ExecutionContext):
    import os, json
    from pathlib import Path
    emb_chunks = context.InputGroup(chunk_emb)
    oemb = context.Output(merged_emb)

    script = Path("_merge_pbert.py")
    script.write_text(MERGER)
    manifest_path = Path("_pbert_manifest.json")
    manifest_path.write_text(json.dumps([str(e.container) for e in emb_chunks]))

    context.ExecWithEnv().ifContainerDo(
        env=image_polars,
        cmd=f"python {script} {manifest_path} {oemb.container}",
    )

    for cf in emb_chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged_emb: oemb.local}],
        success=oemb.local.exists(),
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

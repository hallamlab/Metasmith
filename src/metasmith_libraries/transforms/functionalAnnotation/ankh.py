import os
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::ankh.env"))
weights  = model.AddRequirement(lib.GetType("ref::ankh_base_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
plm      = model.AddRequirement(lib.GetType("lib::plm"))
out_emb  = model.AddProduct(lib.GetType("annotation::ankh_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::ankh_index"))


BATCH_SIZE   = 8
MAX_LEN      = 1024
CHUNK_OVERLAP = 128

def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    iplm    = context.Input(plm)
    iw      = context.Input(weights)
    iemb    = context.Output(out_emb)
    iidx    = context.Output(out_idx)

    device = "cuda"

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    context.ExecWithEnv(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
        ],
        args=["--nv", "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}"],
        cmd=f"""
            python {iplm.container}/ankh.py \
                --weights /weights \
                --fasta {iorfs.container} \
                --out-parquet {iemb.container} \
                --out-index {iidx.container} \
                --device {device} \
                --batch-size {BATCH_SIZE} \
                --max-len {MAX_LEN} \
                --chunk-overlap {CHUNK_OVERLAP}
        """,
    )

    return ExecutionResult(
        manifest=[{out_emb: iemb.local, out_idx: iidx.local}],
        success=iemb.local.exists() and iidx.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(12),
        duration=Duration(hours=3),
    ),
)

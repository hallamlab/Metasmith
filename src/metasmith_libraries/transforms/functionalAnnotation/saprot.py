import os
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::saprot.env"))
weights  = model.AddRequirement(lib.GetType("ref::saprot_650m_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
tokens   = model.AddRequirement(lib.GetType("sequences::structure_3di_tokens"), parents={orfs})
plm      = model.AddRequirement(lib.GetType("lib::plm"))
out_emb  = model.AddProduct(lib.GetType("annotation::saprot_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::saprot_index"))


BATCH_SIZE   = 16
MAX_LEN      = 1024
CHUNK_OVERLAP = 128

def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iplm     = context.Input(plm)
    iw       = context.Input(weights)
    itok     = context.Input(tokens)
    iemb     = context.Output(out_emb)
    iidx     = context.Output(out_idx)

    device = "cuda"

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
        ],
        args=["--nv", "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}"],
        cmd=f"""
            python {iplm.container}/saprot.py \
                --weights /weights \
                --fasta {iorfs.container} \
                --di3 {itok.container} \
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
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)

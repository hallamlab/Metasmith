from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::esmc.env"))
weights  = model.AddRequirement(lib.GetType("ref::esm_c_600m_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
plm      = model.AddRequirement(lib.GetType("lib::plm"))
out_emb  = model.AddProduct(lib.GetType("annotation::esm_c_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::esm_c_index"))
out_lay  = model.AddProduct(lib.GetType("annotation::esm_c_layer_means"))


MODEL_NAME   = "esmc_600m"
BATCH_SIZE   = 32
MAX_LEN      = 2048
CHUNK_OVERLAP = 128

def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    iplm    = context.Input(plm)
    iw      = context.Input(weights)
    iemb    = context.Output(out_emb)
    iidx    = context.Output(out_idx)
    ilay    = context.Output(out_lay)

    device = "cuda"

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
        ],
        cmd=f"""
            python {iplm.container}/esm_c.py \
                --weights /weights \
                --model-name {MODEL_NAME} \
                --fasta {iorfs.container} \
                --out-parquet {iemb.container} \
                --out-index {iidx.container} \
                --out-layers {ilay.container} \
                --device {device} \
                --batch-size {BATCH_SIZE} \
                --max-len {MAX_LEN} \
                --chunk-overlap {CHUNK_OVERLAP}
        """,
    )

    return ExecutionResult(
        manifest=[{out_emb: iemb.local, out_idx: iidx.local, out_lay: ilay.local}],
        success=all(f.exists() and f.stat().st_size > 0
                    for f in (iemb.local, iidx.local, ilay.local)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
        gpus=Gpus.REQUIRED,
        gpu_memory=Size.GB(24),
    ),
)

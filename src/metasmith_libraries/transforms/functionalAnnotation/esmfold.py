from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::esmfold.env"))
weights   = model.AddRequirement(lib.GetType("ref::esmfold_weights"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
plm      = model.AddRequirement(lib.GetType("lib::plm"))
out_struct = model.AddProduct(lib.GetType("sequences::predicted_structures"))


MAX_LEN    = 2048
CHUNK_SIZE = 64

def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iplm     = context.Input(plm)
    iw       = context.Input(weights)
    istruct  = context.Output(out_struct)

    device = "cuda"

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")
    context.LocalShell("mkdir -p structures")

    context.ExecWithEnv(
        env=image,
        exports={"PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True"},
        cmd=f"""
            python {iplm.container}/esmfold.py \
                --weights "$PWD/weights" \
                --fasta {iorfs.container} \
                --out-dir "$PWD/structures" \
                --device {device} \
                --max-len {MAX_LEN} \
                --chunk-size {CHUNK_SIZE}
        """,
    )

    context.LocalShell(f"tar -czf {istruct.local} -C structures .")

    return ExecutionResult(
        manifest=[{out_struct: istruct.local}],
        success=istruct.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=3),
        gpus=Gpus.REQUIRED,
        gpu_memory=Size.GB(32),
    ),
)

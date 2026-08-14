from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image        = model.AddRequirement(lib.GetType("env::predictf.env"))
orfs         = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db           = model.AddRequirement(lib.GetType("annotation::predictf_db"))
out_tf       = model.AddProduct(lib.GetType("annotation::predictf_results_chunk"))
out_potential = model.AddProduct(lib.GetType("annotation::predictf_potential_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb   = context.Input(db)
    otf   = context.Output(out_tf)
    opot  = context.Output(out_potential)

    # PredicTF uses DIAMOND alignment against BacTFDB then DL classification.
    # The deepARG.py --folder flag points to BacTFDB (database + model files).
    # Must use conda run -n predictf for Python 2.7 environment.
    # Subshell to avoid changing CWD (metasmith needs /ws writable for exit code).
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/predictf_db")],
        cmd=f"""
            export THEANO_FLAGS="base_compiledir=/tmp/theano_compile"
            mkdir -p /ws/predictf_out && \
            (cd /predictf/deeparg-largerepo && \
            conda run -n predictf python deepARG.py \
                --align --type prot --genes \
                --input {iorfs.container} \
                --output /ws/predictf_out/file.out \
                --folder /predictf_db/)
        """,
    )

    context.LocalShell(f"cp predictf_out/file.out.mapping.TF {otf.local}")
    context.LocalShell(f"cp predictf_out/file.out.mapping.potential.TF {opot.local}")

    return ExecutionResult(
        manifest=[
            {
                out_tf: otf.local,
                out_potential: opot.local,
            },
        ],
        success=otf.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)

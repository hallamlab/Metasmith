from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::deeptfactor.env"))
orfs        = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
out_results = model.AddProduct(lib.GetType("annotation::deeptfactor_results_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out_results)

    # DeepTFactor can't handle '*' stop codons or proteins > 1000 aa.
    # Strip stop codons before running prediction.
    # Subshell cd to /opt/deeptfactor so tf_running.py finds ./trained_model/
    # without changing CWD (metasmith's exit-code trap needs /ws writable)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            sed '/^[^>]/s/\\*//g' {iorfs.container} > /ws/clean.faa
            (cd /opt/deeptfactor && python tf_running.py -i /ws/clean.faa -o /ws/result -g cpu)
        """,
    )

    context.LocalShell(f"cp result/prediction_result.txt {iout.local}")

    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=6),
    ),
)

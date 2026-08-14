from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::deepec.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
out_predictions = model.AddProduct(lib.GetType("annotation::deepec_predictions_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout = context.Output(out_predictions)

    threads = context.params.get("cpus", 8)

    # Run DeepEC
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            deepec \
                -p {threads} \
                -i {iorfs.container} \
                -o deepec_output
        """,
    )

    # Copy output - DeepEC produces results in the output directory
    output_files = list(Path("deepec_output").glob("*.tsv"))
    if output_files:
        context.LocalShell(f"cp {output_files[0]} {iout.local}")
    else:
        # Check for other common output patterns
        result_files = list(Path("deepec_output").glob("*result*"))
        if result_files:
            context.LocalShell(f"cp {result_files[0]} {iout.local}")
        else:
            # Fallback: copy the entire directory content as tsv
            context.LocalShell(f"find deepec_output -type f -exec cat {{}} \\; > {iout.local}")

    return ExecutionResult(
        manifest=[
            {
                out_predictions: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=4),
    ),
)

from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::rgi.env"))
chunk = model.AddRequirement(lib.GetType("sequences::orf_batch"))
card = model.AddRequirement(lib.GetType("annotation::card_db"))
out_results = model.AddProduct(lib.GetType("annotation::rgi_results_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(chunk)
    icard = context.Input(card)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)

    context.LocalShell(f"cp -r {icard.external} ./localDB")
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            rgi main \
                --input_sequence {iorfs.container} \
                --output_file rgi_out \
                --input_type protein \
                --local \
                -n {threads} \
                --include_loose \
                --clean
        """,
    )

    context.LocalShell(f"cp rgi_out.txt {iout.local}")

    return ExecutionResult(
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=chunk,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)

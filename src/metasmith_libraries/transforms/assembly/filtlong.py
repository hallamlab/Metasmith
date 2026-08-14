from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
im_bb   = model.AddRequirement(lib.GetType("env::bbtools.env"))
image   = model.AddRequirement(lib.GetType("env::filtlong.env"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"))
out     = model.AddProduct(lib.GetType("sequences::clean_long_reads"))
disc    = model.AddProduct(lib.GetType("sequences::discarded_long_reads"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    iout=context.Output(out)
    idisc=context.Output(disc)

    temp_unzipped = "temp_unzipped.fq"
    # --min_length 1000 --keep_percent 90 are default
    # todo: somehow cap at 100x coverage
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            filtlong --min_length 1000 --keep_percent 90 {ireads.container} >{temp_unzipped}
        """,
    )

    context.ExecWithEnv().ifContainerDo(
        env=im_bb,
        cmd=f"""\
            filterbyname.sh in={ireads.container} out={idisc.container} names={temp_unzipped}
        """,
    )

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    context.LocalShell(f"pigz {threads} -c {temp_unzipped} >{iout.local}")

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                disc: idisc.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=2,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
im_bb   = model.AddRequirement(lib.GetType("env::bbtools.env"))
image   = model.AddRequirement(lib.GetType("env::filtlong.env"))
rmeta   = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"), parents={rmeta})
gfa     = model.AddRequirement(lib.GetType("sequences::miniasm_gfa"), parents={rmeta})
out     = model.AddProduct(lib.GetType("sequences::100x_long_reads"))
# disc    = model.AddProduct(lib.GetType("sequences::discarded_long_reads"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    igfa=context.Input(gfa)
    iout=context.Output(out)
    # idisc=context.Output(disc)

    n_bases_file = "temp_n_bases.txt"
    context.LocalShell(
        cmd=f"""\
            GENOME_SIZE=$(awk '/^S/ {{ sum += length($3) }} END {{ print sum }}' {igfa.local}); \
            echo "estimated genome size: $GENOME_SIZE"
            TARGET_BASES=$(( GENOME_SIZE * 100 )); \
            echo $TARGET_BASES > {n_bases_file}
        """
    )

    temp_unzipped = "temp_unzipped.fq"
    # --min_length 1000 --keep_percent 90 are default
    # todo: somehow cap at 100x coverage
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            filtlong --min_length 1000 --target_bases $(< {n_bases_file}) {ireads.container} >{temp_unzipped}
        """,
    )

    # context.ExecWithEnv().ifContainerDo(
    #     image=im_bb,
    #     cmd=f"""\
    #         filterbyname.sh in={ireads.container} out={idisc.container} names={temp_unzipped}
    #     """,
    # )

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    context.LocalShell(f"pigz {threads} -c {temp_unzipped} >{iout.local}")

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                # disc: idisc.local,
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
    ),
)

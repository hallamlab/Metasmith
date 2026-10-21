from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::ganon.env"))
inputs = model.AddRequirement(lib.GetType("ref::ganon2_sources"))
out    = model.AddProduct(lib.GetType("ref::ganon2_db"))


def protocol(context: ExecutionContext):
    iin  = context.Input(inputs)
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    # The build-custom call `ganon build` makes once its download is done
    # (ganon 2.4.1, build_update.py), with the taxonomy and genome sizes it would
    # otherwise fetch from NCBI handed over instead.
    context.ExecWithEnv(env=image, cmd=f"""
        ganon build-custom --db-prefix ganon2 \
            --input {iin.container}/files --input-extension fna.gz --input-recursive \
            --input-target file --ncbi-file-info {iin.container}/assembly_summary.txt \
            --level species --taxonomy ncbi --taxonomy-files {iin.container}/taxdump.tar.gz \
            --genome-size-files {iin.container}/species_genome_size.txt.gz {threads_arg}
        mkdir -p {iout.container}
        mv ganon2.hibf ganon2.tax {iout.container}/
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local/f).is_file() and (iout.local/f).stat().st_size > 0
                    for f in ("ganon2.hibf", "ganon2.tax")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=inputs,
    labels=["noscratch"],
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)

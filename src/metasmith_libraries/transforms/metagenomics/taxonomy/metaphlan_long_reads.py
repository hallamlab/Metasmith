from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::metaphlan.env"))
db      = model.AddRequirement(lib.GetType("ref::metaphlan_db"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"))

profile = model.AddProduct(lib.GetType("taxonomy::metaphlan_profile"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iprof    = context.Output(profile)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--nproc {threads}"

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            metaphlan {ireads.container} \
                --input_type fastq --long_reads --offline \
                --db_dir {idb.container} {threads_arg} \
                -o {iprof.container}
        """,
    )

    return ExecutionResult(
        manifest=[{profile: iprof.local}],
        success=iprof.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=4),
    )
)

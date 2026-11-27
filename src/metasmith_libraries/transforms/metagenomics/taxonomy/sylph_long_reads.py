from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::sylph.env"))
db      = model.AddRequirement(lib.GetType("ref::sylph_db"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"))

profile = model.AddProduct(lib.GetType("taxonomy::sylph_profile"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iprof    = context.Output(profile)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-t {threads}"

    # Positional reads are sketched as single-end; sylph estimates their identity itself.
    seq_id = context.params.get("sylph_read_seq_id")
    seq_id_arg = "" if seq_id is None else f"-I {seq_id}"

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            sylph profile {idb.container} {ireads.container} \
                {seq_id_arg} {threads_arg} \
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
        cpus=4,
        memory=Size.GB(64),
        duration=Duration(hours=1),
    )
)

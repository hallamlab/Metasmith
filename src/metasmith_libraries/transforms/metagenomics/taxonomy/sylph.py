from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::sylph.env"))
img_bb  = model.AddRequirement(lib.GetType("env::bbtools.env"))
db      = model.AddRequirement(lib.GetType("ref::sylph_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

profile = model.AddProduct(lib.GetType("taxonomy::sylph_profile"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iprof    = context.Output(profile)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-t {threads}"

    _cmd = f"""
            reformat.sh in={ireads.container} \
                out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    context.ExecWithEnv(env=img_bb, cmd=_cmd)

    _cmd = f"""
            sylph profile {idb.container} \
                -1 split_r1.fq.gz -2 split_r2.fq.gz \
                {threads_arg} \
                -o {iprof.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{
            profile: iprof.local,
        }],
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

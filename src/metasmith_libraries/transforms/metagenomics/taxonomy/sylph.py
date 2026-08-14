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

    context.ExecWithEnv().ifContainerDo(
        env=img_bb,
        cmd=f"""
            reformat.sh in={ireads.container} \
                out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    )

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            sylph profile {idb.container} \
                -1 split_r1.fq.gz -2 split_r2.fq.gz \
                {threads_arg} \
                -o {iprof.container}
        """
    )

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
        memory=Size.GB(64),  # r220 c200 (Wp5jjOW2) SLURM peak RSS 22-26 GB; r232
                             # c200 sketch is ~1.8x bigger (24 GB on disk vs
                             # 13 GB for r220) -> projected peak ~44 GB.
                             # 64 GB keeps ~20 GB headroom.
        duration=Duration(hours=1),
    )
)

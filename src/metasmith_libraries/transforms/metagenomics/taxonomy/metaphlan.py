from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::metaphlan.env"))
db      = model.AddRequirement(lib.GetType("ref::metaphlan_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

profile = model.AddProduct(lib.GetType("taxonomy::metaphlan_profile"))
sam     = model.AddProduct(lib.GetType("taxonomy::metaphlan_sam"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iprof    = context.Output(profile)
    isam     = context.Output(sam)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--nproc {threads}"

    _cmd = f"""
            metaphlan {ireads.container} \
                --input_type fastq --offline \
                --db_dir {idb.container} {threads_arg} \
                -o {iprof.container} \
                --mapout {isam.container}
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{
            profile: iprof.local,
            sam:     isam.local,
        }],
        success=iprof.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=2),
    )
)

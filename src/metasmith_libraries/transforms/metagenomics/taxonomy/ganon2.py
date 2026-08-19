from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::ganon.env"))
img_bb  = model.AddRequirement(lib.GetType("env::bbtools.env"))
db      = model.AddRequirement(lib.GetType("ref::ganon2_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

classif = model.AddProduct(lib.GetType("taxonomy::ganon2_classifications"))
report  = model.AddProduct(lib.GetType("taxonomy::ganon2_report"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iclass   = context.Output(classif)
    irep     = context.Output(report)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    _cmd = f"""
            reformat.sh in={ireads.container} \
                out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_bb, cmd=_cmd) \
        .ifVirtualEnvDo(env=img_bb, cmd=_cmd)

    _cmd = f"""
            ganon classify --db-prefix {idb.container} \
                --paired-reads split_r1.fq.gz split_r2.fq.gz \
                --output-prefix ganon2_out {threads_arg}
            mv ganon2_out.rep {iclass.container}
            mv ganon2_out.tre {irep.container}
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{
            classif: iclass.local,
            report:  irep.local,
        }],
        success=irep.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=2),
    )
)

from metasmith.python_api import *
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

img_hfa = model.AddRequirement(lib.GetType("env::hifiasm-meta.env"))
img_gft = model.AddRequirement(lib.GetType("env::gfatools.env"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"))
out     = model.AddProduct(lib.GetType("sequences::hifiasm_meta_assembly"))

def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-t {threads}"
    assembly_prefix = "the_assembly"
    context.ExecWithEnv().ifContainerDo(
        env = img_hfa,
        cmd = f"""
        hifiasm_meta  -o {assembly_prefix} {threads} {ireads.container}
        """
    )

    primary_gfa = f"{assembly_prefix}.p_ctg.gfa"
    assert Path(primary_gfa).exists(), "failed to find the primary gfa"
    context.ExecWithEnv().ifContainerDo(
        env = img_gft,
        cmd = f"""
        /gfatools-final-gt/gfatools gfa2fa {primary_gfa} >{iout.container}
        """
    )
    
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)

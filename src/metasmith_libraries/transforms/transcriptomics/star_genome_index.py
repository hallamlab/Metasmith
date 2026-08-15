from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
image   = model.AddRequirement(lib.GetType("env::star.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::star_index"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads
    # Same command either way: this tool is a plain CLI in both worlds.
    _cmd = f"""\
            STAR --runMode genomeGenerate \
                --genomeDir star_idx \
                --genomeFastaFiles {iasm.container} \
                --runThreadN {threads}
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)
    context.LocalShell(f"mv star_idx {iout.local}")
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
    group_by=asm,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=2),
    )
)

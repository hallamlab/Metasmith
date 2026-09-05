from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bcftools.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
bam     = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
out     = model.AddProduct(lib.GetType("sequences::variant_calls"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    ibam=context.Input(bam)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    # mpileup faidx-es its reference and writes the .fai beside it. The reference
    # here is a staged input, shared with every other task reading it, so the copy
    # is what keeps two concurrent callers from racing on one index file.
    _cmd = f"""\
            cp {iasm.container} ref.fa
            bcftools mpileup {threads} -Ou -f ref.fa {ibam.container} \
                | bcftools call {threads} -mv -Ov -o {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bam,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)

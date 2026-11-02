from metasmith.python_api import *
import math

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
    with open(iasm.local) as f:
        genome_length = sum(len(l.strip()) for l in f if not l.startswith(">"))
    assert genome_length > 0, f"empty assembly [{iasm.local}]"
    # STAR's default of 14 is sized for a mammal; its manual scales a small genome down to
    # min(14, log2(GenomeLength)/2 - 1).
    sa_index_bases = min(14, int(math.log2(genome_length)/2 - 1))
    _cmd = f"""\
            STAR --runMode genomeGenerate \
                --genomeDir star_idx \
                --genomeFastaFiles {iasm.container} \
                --genomeSAindexNbases {sa_index_bases} \
                --runThreadN {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)
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

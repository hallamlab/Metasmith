# Pratama's MAG-lane assembly (reproduction_map A3): `spades.py --meta -k 21,33,55,77 -m 190`.
# Not the standard spades.py, which follows the JGI protocol (bbcms, --only-assembler, other k).
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::spades.env"))
meta   = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads  = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
out    = model.AddProduct(lib.GetType("sequences::spades_assembly"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)
    threads = context.params.get("cpus", 1)
    # -m is a hard setrlimit inside SPAdes, so leave only the container's overhead outside it.
    mem_gb = context.params.get("memory")
    mem_arg = f"-m {max(1, int(mem_gb * 0.95))}" if mem_gb else ""

    # CAUTION every metasmith runtime pins OMP_NUM_THREADS=1, and SPAdes then honours 1 thread, not -t.
    context.ExecWithEnv(env=image, cmd=f"""
        export OMP_NUM_THREADS={threads}
        spades.py --meta -k 21,33,55,77 -t {threads} {mem_arg} --12 {ireads.container} -o spades_ws
        [[ $(head -c 1 spades_ws/contigs.fasta) == ">" ]] && cp spades_ws/contigs.fasta {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=48, memory=Size.GB(192), duration=Duration(hours=24)),
)

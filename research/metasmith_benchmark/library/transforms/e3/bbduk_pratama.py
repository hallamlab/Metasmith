# Pratama's QC (reproduction_map A1): `bbduk.sh ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1`.
# int=t and unbgzip=f describe the input, not the filter: the reads are one interleaved pigz file,
# and bbduk's BGZF reader hangs on a multi-member gzip.
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::bbtools.env"))
meta   = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads  = model.AddRequirement(lib.GetType("sequences::short_reads"), parents={meta})
out    = model.AddProduct(lib.GetType("sequences::clean_short_reads"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)
    threads = context.params.get("cpus", 1)
    mem_gb = context.params.get("memory")
    xmx = f"-Xmx{int(mem_gb * 0.85)}g" if mem_gb else ""

    context.ExecWithEnv(env=image, cmd=f"""
        bbduk.sh {xmx} threads={threads} unbgzip=f int=t \
            ref=/bbmap/resources/adapters.fa \
            ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1 \
            in={ireads.container} out={iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}],
                           success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=12)),
)

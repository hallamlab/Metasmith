# Flye on a sample's filtered long reads, in the mode its declared platform names. The standard flye picks
# the mode from mean quality, which sends CAMI's PacBio CLR (mean Q 8.4) to --nano-raw.
from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::flye.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_long_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("sequences::flye_assembly"))

MODE = {
    "OXFORD_NANOPORE": "--nano-raw",
    "PACBIO_CLR": "--pacbio-raw",
    "PACBIO_HIFI": "--pacbio-hifi",
}

def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)
    with open(context.Input(meta).local) as j:
        mode = MODE[json.load(j)["platform"]]

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    _cmd = f"""
            flye --meta {threads} {mode} {ireads.container} --out-dir flye_ws
            mv flye_ws/assembly.fasta {iout.container}
            rm -rf flye_ws
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    # The hybrid pilot's Flye peaked at 51 GB on a marine PacBio sample and 25 GB on strain, at 16 cpus.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)

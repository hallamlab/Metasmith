import json
from pathlib import Path
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::flye.env"))
meta     = model.AddRequirement(lib.GetType("sequences::read_metadata"))
pair     = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={meta})
reads    = model.AddRequirement(lib.GetType("e3::nanopore_reads"), parents={pair})
platform = model.AddRequirement(lib.GetType("e5::long_read_platform"), parents={pair})
out      = model.AddProduct(lib.GetType("e5::flye_assembly"))

# The mode comes from the declared platform, never from read quality: NanoSim writes a flat Q40 over
# reads that align at ~15% error (E2's B12).
MODE = {
    "OXFORD_NANOPORE": "--nano-raw",
    "OXFORD_NANOPORE_HQ": "--nano-hq",
    "PACBIO_CLR": "--pacbio-raw",
    "PACBIO_HIFI": "--pacbio-hifi",
}


def protocol(context: ExecutionContext):
    ireads, iout = context.Input(reads), context.Output(out)
    mode = MODE[json.loads(Path(context.Input(platform).local).read_text())["platform"]]
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""
        flye {mode} {ireads.container} --meta --out-dir flye --threads {cpus}
        cp flye/assembly.fasta {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    # E2 measured 32.5 GiB at 16 cpus on CAMI plant nano sample 0.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)

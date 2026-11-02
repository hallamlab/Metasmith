import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::flye.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::filtered_long_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::flye_assembly"))

# LONGREAD_ASSEMBLY in nf-core/mag picks the mode from the declared platform. Never infer it from
# read quality: NanoSim writes a flat Q40 over reads that align at ~15% error (B12).
MODE = {
    "OXFORD_NANOPORE": "--nano-raw",
    "OXFORD_NANOPORE_HQ": "--nano-hq",
    "PACBIO_HIFI": "--pacbio-hifi",
    "PACBIO_CLR": "--pacbio-raw",
}


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    ireads = context.Input(reads)
    iout = context.Output(out)
    mode = MODE[json.loads(Path(imeta.local).read_text())["platform"]]
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""
        flye {mode} {ireads.container} --out-dir . --threads {cpus} --meta
        cp assembly.fasta {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    # One measured peak so far: 32.68 GiB on plant nano sample 0. Resize once more peaks are in.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)

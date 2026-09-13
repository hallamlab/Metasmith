import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::fastqc.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
mate1   = model.AddRequirement(lib.GetType("e2::short_reads_1"), parents={meta})
mate2   = model.AddRequirement(lib.GetType("e2::short_reads_2"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::fastqc_raw_reports"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    im1 = context.Input(mate1)
    im2 = context.Input(mate2)
    iout = context.Output(out)
    sample = json.loads(Path(imeta.local).read_text())["sample"]
    cpus = context.params.get("cpus") or 1
    mem_mb = int((context.params.get("memory") or 4) * 1024 / cpus)

    # nf-core/mag runs FastQC once per mate file, named for the sample, and FastQC names its reports for the file.
    r1, r2 = f"{sample}_raw_1.fastq.gz", f"{sample}_raw_2.fastq.gz"
    context.ExecWithEnv(env=image, cmd=f"""
        ln -sf {im1.container} {r1}
        ln -sf {im2.container} {r2}
        fastqc --quiet --threads {cpus} --memory {min(10000, max(100, mem_mb))} {r1} {r2}
        tar -czf {iout.container} *_fastqc.html *_fastqc.zip
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
)

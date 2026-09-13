import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::fastqc.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::fastqc_raw_reports"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    ireads = context.Input(reads)
    iout = context.Output(out)
    sample = json.loads(Path(imeta.local).read_text())["sample"]
    cpus = context.params.get("cpus") or 1
    mem_mb = int((context.params.get("memory") or 4) * 1024 / cpus)

    # nf-core/mag runs FastQC once per mate file. Split the interleaved file back into mates.
    r1, r2 = f"{sample}_raw_1.fastq.gz", f"{sample}_raw_2.fastq.gz"
    context.LocalShell(
        f"""zcat -f {ireads.local} | awk -v r1="gzip > {r1}" -v r2="gzip > {r2}" """
        """'{ if ((NR - 1) % 8 < 4) print | r1; else print | r2 }'"""
    )
    context.ExecWithEnv(env=image, cmd=f"""
        fastqc --quiet --threads {cpus} --memory {min(10000, max(100, mem_mb))} {r1} {r2}
        tar -czf {iout.container} *_fastqc.html *_fastqc.zip
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
)

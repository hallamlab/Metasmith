# Pratama's read QC report (reproduction_map A2): `fastp -R -j -h`, a report only. No reads are written.
import json
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::fastp.env"))
meta  = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads = model.AddRequirement(lib.GetType("sequences::short_reads"), parents={meta})
rjson = model.AddProduct(lib.GetType("e3::fastp_report_json"))
rhtml = model.AddProduct(lib.GetType("e3::fastp_report_html"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    ojson = context.Output(rjson)
    ohtml = context.Output(rhtml)
    title = Path(ireads.local).name.split(".")[0]
    threads = context.params.get("cpus", 4)
    context.ExecWithEnv(env=image, cmd=f"""
        fastp --interleaved_in -i {ireads.container} -w {threads} -R "{title}" -j {ojson.container} -h {ohtml.container}
    """)
    return ExecutionResult(manifest=[{rjson: ojson.local, rhtml: ohtml.local}],
                           success=ojson.local.exists() and ohtml.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=4)),
)

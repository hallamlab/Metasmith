# fastp_qc -- FASTP_QC (asv_pipeline.nf:3121)

import json

import yaml
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::fastp.env"))
study  = model.AddRequirement(lib.GetType("aspire::study_metadata"))
meta   = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
reads  = model.AddRequirement(lib.GetType("sequences::short_reads"), parents={meta})
params = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
qc     = model.AddProduct(lib.GetType("aspire::qc_reads"))
rjson  = model.AddProduct(lib.GetType("aspire::fastp_report_json"))
rhtml  = model.AddProduct(lib.GetType("aspire::fastp_report_html"))


def protocol(context: ExecutionContext):
    ireads, iqc = context.Input(reads), context.Output(qc)
    ijson, ihtml = context.Output(rjson), context.Output(rhtml)
    parity = json.loads(context.Input(meta).local.read_text())["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    cfg = yaml.safe_load(context.Input(params).local.read_text())["fastp"]

    # --stdout writes a paired run interleaved, so the product keeps its input's layout.
    trims = f"-f {cfg['trim_front_r1']} -t {cfg['trim_tail_r1']}"
    if parity == "paired":
        trims += f" --interleaved_in -F {cfg['trim_front_r2']} -T {cfg['trim_tail_r2']}"
    adapters = "" if cfg["adapter_trimming"] else "--disable_adapter_trimming"
    context.ExecWithEnv(env=image, cmd=f"""\
        fastp -i {ireads.container} --stdout {trims} \
            -q {cfg['qualified_quality']} -u {cfg['unqualified_percent']} \
            -l {cfg['length_required']} -n {cfg['n_base_limit']} {adapters} \
            -j {ijson.container} -h {ihtml.container} \
            -w {context.params.get('cpus', 4)} \
        | gzip -n > {iqc.container}
    """)

    return ExecutionResult(
        manifest=[{qc: iqc.local, rjson: ijson.local, rhtml: ihtml.local}],
        success=all(p.local.exists() for p in (iqc, ijson, ihtml)) and iqc.local.stat().st_size > 20,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

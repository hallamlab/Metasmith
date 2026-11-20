import json

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={exp})
trim    = model.AddRequirement(lib.GetType("sequences::fastp_json_report"), parents={meta})
stats   = model.AddRequirement(lib.GetType("alignment::alignment_stats"), parents={meta})
qc      = model.AddRequirement(lib.GetType("isolate_rnaseq::count_qc"), parents={exp})
out     = model.AddProduct(lib.GetType("isolate_rnaseq::qc_report"))


def sample_of(context, item):
    src = context.SourceOf(item, meta)
    assert src is not None, f"no read_metadata in the lineage of [{item.local.name}]"
    return json.loads(src.local.read_text())["sample"]


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    reports = {sample_of(context, j): j for j in context.InputGroup(trim)}
    bam_stats = {sample_of(context, s): s for s in context.InputGroup(stats)}
    assert reports.keys() == bam_stats.keys(), (sorted(reports), sorted(bam_stats))
    samples = " ".join(f"--sample {s} {reports[s].container} {bam_stats[s].container}" for s in sorted(reports))
    title = context.Input(exp).local.read_text().strip()
    context.ExecWithEnv(env=image, cmd=f"""\
        python {context.Input(scripts).container}/qc_report.py "{title} QC" {context.Input(qc).container} \\
            {iout.container} {samples}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)

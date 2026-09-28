# read_accounting -- GENERAL_STATS (asv_pipeline.nf:3755), rebuilt from declared inputs.
#
# A fastp report does not name its sample, so each is matched to one through its lineage.
# The count files carry their sample already.

import json

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
rjson   = model.AddRequirement(lib.GetType("aspire::fastp_report_json"), parents={meta})
rcounts = model.AddRequirement(lib.GetType("aspire::read_counts"), parents={meta})
raw     = model.AddRequirement(lib.GetType("amplicon::asv_table"), parents={study})
clean   = model.AddRequirement(lib.GetType("aspire::counts_clean"), parents={study})
removed = model.AddRequirement(lib.GetType("aspire::counts_removed"), parents={study})
fate    = model.AddProduct(lib.GetType("aspire::read_fate"))


def sample_of(context, item):
    src = context.SourceOf(item, meta)
    assert src is not None, f"no read_metadata in the lineage of [{item.local.name}]"
    return json.loads(src.local.read_text())["sample"]


def protocol(context: ExecutionContext):
    ifate = context.Output(fate)
    reports = {sample_of(context, j): j for j in context.InputGroup(rjson)}
    counts = {}
    for c in context.InputGroup(rcounts):
        counts[c.local.read_text().splitlines()[1].split("\t")[0]] = c
    assert reports.keys() == counts.keys(), (sorted(reports), sorted(counts))
    samples = " ".join(f"--sample {s} {reports[s].container} {counts[s].container}"
                       for s in sorted(reports))

    context.ExecWithEnv(env=image, cmd=f"""\
        python {context.Input(scripts).container}/read_accounting.py {samples} \
            --raw {context.Input(raw).container} --clean {context.Input(clean).container} \
            --removed {context.Input(removed).container} --out {ifate.container}
    """)

    return ExecutionResult(
        manifest=[{fate: ifate.local}],
        success=ifate.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)

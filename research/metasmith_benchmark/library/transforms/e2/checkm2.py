import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::checkm2.env"))
db      = model.AddRequirement(lib.GetType("e2::checkm2_database"))
bin_    = model.AddRequirement(lib.GetType("e2::bin"))
out     = model.AddProduct(lib.GetType("e2::checkm2_quality"))


def protocol(context: ExecutionContext):
    idb = context.Input(db)
    cpus = context.params.get("cpus") or 1
    Path("bins").mkdir()
    outputs = {}
    for item in context.AsBatch():
        ibin = item.Input(bin_)
        shutil.copy(ibin.local, Path("bins") / f"{ibin.local.stem}.fa")
        outputs[ibin.local.stem] = item.Output(out)

    # CHECKM2_PREDICT, run as nf-core/mag runs it: no gene-calling workaround.
    context.ExecWithEnv(env=image, cmd=f"""
        checkm2 predict --input bins -x fa --output-directory checkm2_out \
            --threads {cpus} --database_path {idb.container}
    """)

    with open("checkm2_out/quality_report.tsv", newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    header, by_name = rows[0], {r[0]: r for r in rows[1:] if r}
    manifest = []
    for name, iout in outputs.items():
        if name not in by_name:
            continue
        with open(iout.local, "w", newline="") as f:
            csv.writer(f, delimiter="\t", lineterminator="\n").writerows([header, by_name[name]])
        manifest.append({out: iout.local})

    return ExecutionResult(manifest=manifest, success=len(manifest) == len(outputs))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin_,
    batch_size=200,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=4)),
)

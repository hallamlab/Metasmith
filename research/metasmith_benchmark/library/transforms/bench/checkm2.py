# CheckM2 quality for any bin set. The standard library has CheckM 1 only, and the tool table gives E5 CheckM2.
# Same run as E2's e2/checkm2.py (nf-core/mag's CHECKM2_PREDICT), on bench types so no standard transform binds.
import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::checkm2.env"))
db      = model.AddRequirement(lib.GetType("bench::checkm2_database"))
bin_    = model.AddRequirement(lib.GetType("sequences::bin_fasta"))
out     = model.AddProduct(lib.GetType("bench::checkm2_quality"))


def protocol(context: ExecutionContext):
    idb = context.Input(db)
    cpus = context.params.get("cpus") or 1
    Path("bins").mkdir()
    outputs = {}
    for i, item in enumerate(context.AsBatch()):
        ibin = item.Input(bin_)
        # Bin file names repeat across samples, so the batch index keeps them apart.
        name = f"{i}_{ibin.local.stem}"
        shutil.copy(ibin.local, Path("bins") / f"{name}.fa")
        outputs[name] = item.Output(out)

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

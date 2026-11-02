import re
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::busco.env"))
# Given by the user, never produced: a value naming the lineage to fetch.
src     = model.AddRequirement(lib.GetType("annotation::busco_source"))
out     = model.AddProduct(lib.GetType("annotation::busco_lineage"))

DEFAULT_LINEAGE = "eukaryota_odb10"


def protocol(context: ExecutionContext):
    isrc = context.Input(src)
    iout = context.Output(out)

    named = isrc.local.read_text().strip()
    if re.fullmatch(r"\w+_odb\d+(\.\d+)?", named):
        lineage = named
    else:
        lineage = DEFAULT_LINEAGE
        Log.Warn(f"busco_source reads [{named}], not a dataset name like "
                 f"{DEFAULT_LINEAGE}; fetching {lineage}")

    context.ExecWithEnv(env=image, cmd=f"""
        busco \
            --download {lineage} \
            --download_path ./busco_downloads
        mv ./busco_downloads/lineages/{lineage} {iout.container}
    """)

    cfg = iout.local/"dataset.cfg"
    fields = dict(line.strip().split("=", 1) for line in open(cfg) if "=" in line) if cfg.exists() else {}
    n_hmm = len(list((iout.local/"hmms").glob("*.hmm")))
    Log.Info(f"BUSCO lineage [{fields.get('name')}]: {n_hmm:,} HMMs, "
             f"dataset.cfg lists {fields.get('number_of_BUSCOs')}")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=fields.get("name") == lineage and n_hmm > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=src,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

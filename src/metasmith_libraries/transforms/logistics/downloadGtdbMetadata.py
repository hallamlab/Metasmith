from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb_metadata"))

BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
FILES = ["bac120_metadata_r232.tsv.gz", "ar53_metadata_r232.tsv.gz"]


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    fetch = "\n".join(
        f'wget -c --no-check-certificate -O {name} "{BASE}/{name}"' for name in FILES)
    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {iout.local}
        cd {iout.local}
        {fetch}
        gzip -t {" ".join(FILES)}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local / f).exists() for f in FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

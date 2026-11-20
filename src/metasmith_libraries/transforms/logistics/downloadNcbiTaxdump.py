from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::ncbi_taxdump"))

# A dated archive, because the live taxdump.tar.gz changes daily. merged.dmp
# carries the renames, so UniRef TaxIDs older than the snapshot still resolve.
URL = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/taxdmp_2026-10-01.zip"
KEEP = ["nodes.dmp", "names.dmp", "merged.dmp"]


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {iout.local}
        cd {iout.local}
        wget -c --no-check-certificate -O taxdmp.zip "{URL}"
        unzip -o taxdmp.zip {" ".join(KEEP)}
        rm -f taxdmp.zip
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local / f).exists() for f in KEEP),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::blast.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb_rrna"))

# r232 publishes SSU only. The release has no 23S file, so LSU calls carry no
# GTDB vote.
SSU_URL = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0/genomic_files_all/ssu_all_r232.fna.gz"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {iout.local}
        cd {iout.local}
        wget -c --no-check-certificate -O ssu.fna.gz "{SSU_URL}"
        gunzip -f ssu.fna.gz
    """)
    context.ExecWithEnv(
        env=image,
        binds=[(iout.local, "/ref")],
        cmd="makeblastdb -in /ref/ssu.fna -dbtype nucl -out /ref/ssu",
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "ssu.fna").exists() and any(iout.local.glob("ssu*.nsq")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

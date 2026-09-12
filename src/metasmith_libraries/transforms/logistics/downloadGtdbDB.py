from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb"))

BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
FULL_PKG = f"{BASE}/auxillary_files/gtdbtk_package/full_package/gtdbtk_data.tar.gz"
REPS_PKG = f"{BASE}/genomic_files_reps/gtdb_genomes_reps_r232.tar.gz"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {iout.local}
        cd {iout.local}

        # 1. Full data package (skani sketches.db, pplacer refs, taxonomy, masks, msa, …)
        #    Unpacks to ./release232/
        wget -c --no-check-certificate -O gtdbtk_data.tar.gz "{FULL_PKG}"
        tar xzf gtdbtk_data.tar.gz
        rm -f gtdbtk_data.tar.gz

        # 2. Representative genome fastas (per-rep .fna.gz), ~179 GB on disk.
        #    Tarball is pre-nested: contents are
        #    gtdb_genomes_reps_r232/database/<GCA|GCF>/NNN/NNN/NNN/<acc>.fna.gz
        #    Strip the two leading path components on extract so files land
        #    directly under release232/skani/database/ alongside sketches.db.
        wget -c --no-check-certificate -O gtdb_genomes_reps_r232.tar.gz "{REPS_PKG}"
        tar --strip-components=2 -xzf gtdb_genomes_reps_r232.tar.gz \\
            -C release232/skani/database/
        rm -f gtdb_genomes_reps_r232.tar.gz
    """)

    db_dir   = iout.local / "release232" / "skani" / "database"
    sketches = db_dir / "sketches.db"
    has_reps = (db_dir / "GCF").is_dir() or (db_dir / "GCA").is_dir()
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=sketches.exists() and has_reps,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=24),
    ),
)

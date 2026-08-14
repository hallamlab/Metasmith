"""Download + assemble the full GTDB-Tk reference database (release232).

The official `gtdbtk_data.tar.gz` (~100 GB) ships the skani *sketch* DB
(`skani/database/sketches.db`) but OMITS the per-genome representative
`.fna.gz` files. The per-rep fastas must be pulled separately as
`gtdb_genomes_reps_r232.tar.gz` (~179 GB on disk; Content-Length 192 GB).

The r232 reps tarball is ALREADY pre-nested in the layout skani expects —
`gtdb_genomes_reps_r232/database/<GCA|GCF>/NNN/NNN/NNN/<acc>.fna.gz` — so
the upstream `restructure_reps_fna_folder.sh` script (which flat→nests
older releases) is NOT needed for r232. Just strip the two leading
components on extract and the files land directly under
`release232/skani/database/`.

Without the reps, classify_wf fails at the post-pplacer species-level ANI
refinement — and `--skip_ani_screen` (deprecated `--place_species` in 2.7+)
does NOT avoid skani; skani-vs-reps IS the primary screen in r232.

References:
- https://ecogenomics.github.io/GTDBTk/installing/index.html
- https://github.com/Ecogenomics/GTDBTk/wiki/Prepare-GTDB-Tk-data
- https://raw.githubusercontent.com/Ecogenomics/GTDBTk/master/scripts/restructure_reps_fna_folder.sh
  (kept for reference — needed only when reps are pulled as flat .gz files,
   not when using the pre-nested r232 archive above)
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb"))

# Mirror at data.ace.uq.edu.au is the primary; data.gtdb.ecogenomic.org is the
# canonical hostname and serves the same content. Both verified 2026-06-01.
BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
FULL_PKG = f"{BASE}/auxillary_files/gtdbtk_package/full_package/gtdbtk_data.tar.gz"
REPS_PKG = f"{BASE}/genomic_files_reps/gtdb_genomes_reps_r232.tar.gz"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # Run on host: wget/tar/bash are universal and we're moving ~300 GB —
    # no benefit to going through apptainer for I/O work.
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
    # GTDB r232 reps land under both GCA/ and GCF/ subtrees; at least one is required.
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
        duration=Duration(hours=24),  # 300 GB download + restructure can take many hours
    ),
)

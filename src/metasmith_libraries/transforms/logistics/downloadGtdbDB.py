from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb"))

BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
FULL_PKG = f"{BASE}/auxillary_files/gtdbtk_package/full_package/gtdbtk_r232_data.tar.gz"
REPS_PKG = f"{BASE}/genomic_files_reps/gtdb_genomes_reps_r232.tar.gz"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    # Run params, so a renamed upstream file is fixed at launch rather than by a release.
    full_pkg = context.params.get("gtdb_full_package_url", FULL_PKG)
    reps_pkg = context.params.get("gtdb_reps_package_url", REPS_PKG)

    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {iout.local}
        cd {iout.local}

        # 1. Full data package (skani sketches.db, pplacer refs, taxonomy, masks, msa, …).
        #    Every consumer binds this product as GTDBTK_DATA_PATH, which must hold
        #    markers/, taxonomy/, skani/ … directly, so drop the tarball's release232/.
        wget -c --no-check-certificate -O gtdbtk_data.tar.gz "{full_pkg}"
        tar --strip-components=1 -xzf gtdbtk_data.tar.gz
        rm -f gtdbtk_data.tar.gz

        # 2. Representative genome fastas (per-rep .fna.gz), ~179 GB on disk.
        #    Tarball is pre-nested: contents are
        #    gtdb_genomes_reps_r232/database/<GCA|GCF>/NNN/NNN/NNN/<acc>.fna.gz
        #    Strip the two leading path components on extract so files land
        #    directly under skani/database/ alongside sketches.db.
        wget -c --no-check-certificate -O gtdb_genomes_reps_r232.tar.gz "{reps_pkg}"
        tar --strip-components=2 -xzf gtdb_genomes_reps_r232.tar.gz \\
            -C skani/database/
        rm -f gtdb_genomes_reps_r232.tar.gz

        # 3. GTDB-Tk finds each rep through skani/genome_paths.tsv. r232 ships
        #    create_genome_paths.sh in its place, which expects a flat database/;
        #    write the same index from the nested tree instead.
        cd skani
        if [ ! -s genome_paths.tsv ]; then
            find database -name '*_genomic.fna.gz' -printf '%f %h/\\n' > genome_paths.tsv
        fi
    """)

    skani    = iout.local / "skani"
    sketches = skani / "database" / "sketches.db"
    index    = skani / "genome_paths.tsv"
    has_reps = index.exists() and index.stat().st_size > 0
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=sketches.exists() and has_reps and (iout.local / "taxonomy").is_dir(),
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

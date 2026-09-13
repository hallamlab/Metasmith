# The standard gtdbtk.py's classify_wf, with GTDB's genome trees read from one squashfs image.
# The tree costs ~427K inodes on /scratch. classify_wf still reads the genomes after placement,
# skip_ani_screen or not, so they cannot simply be left out.
#
# CAUTION the image binds over skani/database/GCF and GCA, not over skani/database: the sketch files
# sit beside them on the real filesystem. Both directories must exist in ref::gtdb, empty, as mount points.
from metasmith.python_api import *
from pathlib import Path
import shutil

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref    = model.AddRequirement(lib.GetType("ref::gtdb"))
genomes = model.AddRequirement(lib.GetType("bench::gtdb_genomes_image"))
asm    = model.AddRequirement(lib.GetType("sequences::putative_genome"))
tax    = model.AddProduct(lib.GetType("taxonomy::gtdbtk"))


def protocol(context: ExecutionContext):
    iref = context.Input(ref)
    igenomes = context.Input(genomes)

    genome_dir = Path("./assemblies")
    genome_dir.mkdir()
    in2out = {}
    for item in context.AsBatch():
        iasm = item.Input(asm)
        itax = item.Output(tax)
        in2out[iasm.local.stem] = (itax.local.name, itax)
        shutil.copy(iasm.local, genome_dir / iasm.local.name, follow_symlinks=True)

    threads = context.params.get("cpus")
    threads = "" if threads is None else f"--cpus {threads}"
    mem = context.params.get("memory")
    pplacer_cpus = f"--pplacer_cpus {max(1, (int(float(mem)) - 40) // 140)}" if mem else ""
    ext = iasm.container.suffix.replace(".", "")
    out_raw = Path("./gtdb_raw")

    context.ExecWithEnv(
        env=image,
        # apptainer reads `dst:image-src=/GCF` as a bind option, and the engine passes the destination through.
        binds=[
            (iref.external, "/ref"),
            (igenomes.external, "/ref/skani/database/GCF:image-src=/GCF"),
            (igenomes.external, "/ref/skani/database/GCA:image-src=/GCA"),
        ],
        cmd=f"""
            test -e /ref/skani/database/GCF/000/367/345/GCF_000367345.1_genomic.fna.gz
            mkdir -p temp.ws
            export GTDBTK_DATA_PATH=/ref
            gtdbtk classify_wf -x {ext} {threads} {pplacer_cpus} --force --skip_ani_screen \
                --tmpdir temp.ws --genome_dir {genome_dir} --out_dir {out_raw}
        """,
    )

    rows = {}
    last_header = None
    for table in out_raw.glob("classify/*summary.tsv"):
        with open(table) as tsv:
            header = last_header = tsv.readline()
            for line in tsv:
                rows[line.strip().split("\t")[0]] = line, header

    fallback_header = last_header or "user_genome\tclassification\n"
    n_cols = len(fallback_header.rstrip("\n").split("\t"))
    manifest = []
    for genome, (tax_name, tax_handle) in in2out.items():
        if genome in rows:
            row, header = rows[genome]
        else:
            Log.Warn(f"genome [{genome}] absent from summary.tsv; emitting an N/A row")
            row, header = "\t".join([genome] + ["N/A"] * max(0, n_cols - 1)) + "\n", fallback_header
        with open(tax_name, "w") as out:
            out.write(header if header.endswith("\n") else header + "\n")
            out.write(row if row.endswith("\n") else row + "\n")
        manifest.append({tax: tax_handle.local})

    return ExecutionResult(manifest=manifest, success=len(manifest) > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=200,
    resources=Resources(cpus=8, memory=Size.GB(240), duration=Duration(hours=24)),
)

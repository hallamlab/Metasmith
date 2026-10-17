from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::dram.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db    = model.AddRequirement(lib.GetType("annotation::dram_db"))
out   = model.AddProduct(lib.GetType("annotation::dram_annotations_chunk"))


def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    idb    = context.Input(db)
    iannot = context.Output(out)

    threads = context.params.get("cpus", 8)
    annot_dir = "dram_annot"

    context.LocalShell(f"""cat > _run_dram.py << 'DRAMPY'
import os, sys
os.environ["HOME"] = "/tmp"
from mag_annotator.annotate_bins import annotate_called_genes
annotate_called_genes(
    ["input.clean.faa"],
    "{annot_dir}",
    threads={threads},
    config_loc="DRAM.local.config",
    rename_genes=False,
)
DRAMPY""")

    # buildDramDB writes DRAM.config with /db/ paths; point them at wherever the
    # database is in this runtime instead of binding it at /db. Gene ids stay as
    # prodigal named them: DRAM's default prefixes each with the fasta's name.
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            sed "s|\\"/db/|\\"{idb.container}/|g" {idb.container}/DRAM.config > DRAM.local.config
            # DRAM only warns about a database it cannot open, and annotates without it.
            grep -o '"{idb.container}/[^"]*"' DRAM.local.config | tr -d '"' | while read -r f; do
                [ -e "$f" ] || {{ echo "DRAM.config names a missing file [$f]" >&2; exit 1; }}
            done
            sed '/^[^>]/s/\\*//g' {iorfs.container} > input.clean.faa
            python3 _run_dram.py
        """,
    )

    context.LocalShell(f"cp {annot_dir}/annotations.tsv {iannot.local}")

    return ExecutionResult(
        manifest=[
            {
                out: iannot.local,
            },
        ],
        success=iannot.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)

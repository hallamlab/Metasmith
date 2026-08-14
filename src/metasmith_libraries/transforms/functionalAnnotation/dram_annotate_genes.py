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

    # DRAM 1.5.0 bug: annotate_called_genes_cmd() doesn't accept config_loc
    # but argparse always passes it (even as None), crashing annotate_genes.
    # Workaround: write a wrapper script and call annotate_called_genes()
    # directly, which DOES accept config_loc. Using a script file avoids
    # shell quoting issues with inline python -c inside the container arm.
    context.LocalShell(f"""cat > _run_dram.py << 'DRAMPY'
import os, sys
os.environ["HOME"] = "/tmp"
from mag_annotator.annotate_bins import annotate_called_genes
annotate_called_genes(
    ["input.clean.faa"],
    "{annot_dir}",
    threads={threads},
    config_loc="/db/DRAM.config",
)
DRAMPY""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
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

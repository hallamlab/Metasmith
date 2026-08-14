import glob
import os
from pathlib import Path
import shutil
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
# The name is declared but never read here, and that is the whole point: stating
# that an accession descends from a name is what puts the name in the lineage of
# everything this downloads, so a later step can ask which name a given file
# came from. Without it the only way to label a genome is to scrape its header,
# which is not reliably unique -- two assemblies of one species collide.
name    = model.AddRequirement(lib.GetType("ncbi::genome_name"))
dep     = model.AddRequirement(lib.GetType("ncbi::assembly_accession"), parents={name})
image   = model.AddRequirement(lib.GetType("env::ncbi-datasets.env"))
fna     = model.AddProduct(lib.GetType("sequences::isolate_assembly"))
faa     = model.AddProduct(lib.GetType("sequences::orfs"))
gff     = model.AddProduct(lib.GetType("sequences::gff"))
gbk     = model.AddProduct(lib.GetType("sequences::gbk"))

def protocol(context: ExecutionContext):
    dep_path=context.Input(dep)

    with open(dep_path.local) as f:
        acc = f.readline().strip()

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            datasets download genome accession {acc} \
                --include gff3,protein,genome,gbff
        """,
    )
    context.LocalShell(f"unzip ncbi_dataset.zip")

    output_manifest = {}
    def fix_out(dep, p: Path):
        op = context.Output(dep)
        shutil.move(p, op.local)
        output_manifest[dep] = op.local
    for f in glob.glob("ncbi_dataset/*/*/*"):
        p = Path(f)
        Log.Info(f"scanning file [{p}]")
        match(p.name):
            case "genomic.gff":
                fix_out(gff, p)
            case "genomic.gbff":
                fix_out(gbk, p)
            case "protein.faa":
                fix_out(faa, p)
        if not p.name.startswith("cds") and p.name.endswith("genomic.fna"):
                fix_out(fna, p)
    return ExecutionResult(
        manifest=[
            output_manifest,
        ],
        success=len(output_manifest)==len(model.produces[0]), # no branching
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(1),
    )
)

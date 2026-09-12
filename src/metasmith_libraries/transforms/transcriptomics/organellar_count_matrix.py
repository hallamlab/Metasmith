from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
exp   = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
bam   = model.AddRequirement(lib.GetType("transcriptomics::organellar_bam"), parents={exp})
gff   = model.AddRequirement(lib.GetType("transcriptomics::organellar_gff"))
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
out   = model.AddProduct(lib.GetType("transcriptomics::organellar_gene_count_table"))

def protocol(context: ExecutionContext):
    bam_paths = context.InputGroup(bam)
    ihelp = context.Input(helpers)
    gff_paths = context.InputGroup(gff)
    iout      = context.Output(out)

    manifest = Path("sample_manifest.tsv")
    with open(manifest, "w") as f:
        for p in bam_paths:
            f.write(f"{p.container}\n")

    gff_manifest = Path("gff_manifest.tsv")
    with open(gff_manifest, "w") as f:
        for p in gff_paths:
            f.write(f"{p.container}\n")

    context.ExecWithEnv(
        env=image,
        cmd=f"pip install -q pysam && python {ihelp.container}/organellar_count_matrix.py {manifest} {gff_manifest} {iout.container}",
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

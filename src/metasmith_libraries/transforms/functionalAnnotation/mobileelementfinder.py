import csv
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::mobileelementfinder.env"))
asm = model.AddRequirement(lib.GetType("sequences::contig_batch"))
out_results = model.AddProduct(lib.GetType("annotation::mobileelementfinder_results"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 4)

    MEF_BLAST_PATCH = Path(
        "/home/phyberos/project-rpp/spanish_lakes/container_patches/me_finder_blast.py"
    )
    MEF_BLAST_INCONTAINER = (
        "/opt/conda/envs/external_mobileelementfinder_env"
        "/lib/python3.9/site-packages/me_finder/tools/blast.py"
    )
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(MEF_BLAST_PATCH, MEF_BLAST_INCONTAINER)],
        cmd=f"""
            mefinder find --contig {iasm.container} --threads {threads} mef_out
        """,
    )

    src = Path("mef_out.csv")
    if src.exists():
        with open(src, newline="") as fi:
            clean = (ln.rstrip("\r\n") for ln in fi if not ln.lstrip().startswith("#"))
            reader = csv.reader(clean)
            rows = [r for r in reader if r]
        with open(iout.local, "w") as fo:
            for r in rows:
                fo.write("\t".join(r) + "\n")
    if not iout.local.exists() or iout.local.stat().st_size == 0:
        Path(iout.local).write_text("# No mobile elements detected\n")

    return ExecutionResult(
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=3),
    ),
)

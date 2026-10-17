import csv
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::mobileelementfinder.env"))
jsonfix = model.AddRequirement(lib.GetType("lib::mobileelementfinder_jsonfix"))
asm = model.AddRequirement(lib.GetType("sequences::contig_batch"))
out_results = model.AddProduct(lib.GetType("annotation::mobileelementfinder_results"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ijsonfix = context.Input(jsonfix)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 4)

    # A sitecustomize that fails to import is reported and skipped, so the fix is checked before it is relied on.
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export PYTHONPATH={ijsonfix.container}${{PYTHONPATH:+:$PYTHONPATH}}
            python -c "import me_finder.tools.blast as b; assert b.MSM_JSONFIX"
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

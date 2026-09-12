import shlex
from metasmith.python_api import *
import os
from pathlib import Path

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
genomes = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
gpr     = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={genomes})
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
bl        = model.AddRequirement(lib.GetType("buildlib::benchmark"))
out     = model.AddProduct(lib.GetType("ref::gpr_table_denovo"))

LANE_SET = "chosen_4"
EXTENSIONS = ("attribution", "feature", "universe")


def staged_siblings(one_local: Path, one_container: str) -> list[str]:
    key = one_local.name.rsplit("-", 1)[-1]
    found = sorted(Path.cwd().glob(f"*-{key}"))
    if not found:
        found = sorted(one_local.parent.glob(f"*-{key}"))
    if not found:
        return [one_container]
    return [os.readlink(p) if p.is_symlink() else str(p) for p in found]


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ibl = context.Input(bl)
    igpr = context.Input(gpr)
    gpr_paths = staged_siblings(Path(igpr.local), str(igpr.container))
    cmd = f"""
            python3 {ibl.container}/host_gpr_denovo.py \
            --genomes {context.Input(genomes).container} \
            --gpr-paths {shlex.quote(repr(gpr_paths))} \
            --out {iout.container} \
            --ev-lib {context.Input(ev_lib).container} \
            --lane-set {LANE_SET} \
            --extensions '{repr(list(EXTENSIONS))}'
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    made = sorted((iout.local / "hosts").glob("*/gpr_denovo.parquet")) \
        if (iout.local / "hosts").exists() else []
    Log.Info(f"denovo_gpr: {len(made)} host tables")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=bool(made) and (iout.local / "BUILD.json").exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    batch_size=25,
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(minutes=30)),
)

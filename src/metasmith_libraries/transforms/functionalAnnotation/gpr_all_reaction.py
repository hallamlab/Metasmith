from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
# fe.LANE_SETS["all_reaction"]
lanes   = [model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={orfs})
           for k in ("kofam", "clean", "uniref50", "pbert", "deepec", "eggnog")]
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
calib   = model.AddRequirement(lib.GetType("ref::lane_calibration"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out_gpr = model.AddProduct(lib.GetType("annotation::gpr_table_all"))

LANE_SET = "all_reaction"


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out_gpr)
    cmd = f"""
            python3 {context.Input(gpr_lib).container}/gpr_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --annotation-table {" ".join(str(context.Input(t).container) for t in lanes)} \
            --orfs {iorfs.container} \
            --bridge {context.Input(bridge).container} \
            --calibration {context.Input(calib).container} \
            --lane-set {LANE_SET} \
            --source {iorfs.local.stem} \
            --out {iout.container}
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_gpr: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)

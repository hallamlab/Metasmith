import shlex
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
cond_gpr  = model.AddRequirement(lib.GetType("bench::condition_gpr"))
extracts  = model.AddRequirement(lib.GetType("bench::study_extractions"))
het       = model.AddRequirement(lib.GetType("raw::het_screen_records"))
cohorts_m = model.AddRequirement(lib.GetType("buildlib::bench_cohorts.py"))
bl        = model.AddRequirement(lib.GetType("buildlib::benchmark"))
out       = model.AddProduct(lib.GetType("bench::conditions"))

COLUMNS = (
    "condition_id", "arm", "tier", "ptype", "host", "gem", "n_units", "is_control",
    "citation", "note", "gene", "element", "target_mnxm", "target_name",
    "target_basis", "expected_dir", "essential_on_glucose_minimal", "is_neg",
    "obs_id", "host_gem_is_proxy",
)

ELEMENTS = ("C", "N", "P", "S")

def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ibl = context.Input(bl)
    cmd = f"""
            python3 {ibl.container}/conditions.py \
            --lib {context.Input(cohorts_m).container} \
            --cond-gpr {context.Input(cond_gpr).container} \
            --extracts {context.Input(extracts).container} \
            --het {context.Input(het).container} \
            --columns {shlex.quote(repr(COLUMNS))} \
            --elements {shlex.quote(repr(ELEMENTS))} \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)

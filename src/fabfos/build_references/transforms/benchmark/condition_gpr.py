from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
extracts  = model.AddRequirement(lib.GetType("bench::study_extractions"))
het       = model.AddRequirement(lib.GetType("raw::het_screen_records"))
laser     = model.AddRequirement(lib.GetType("raw::laser_records"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
hosts_gem = model.AddRequirement(lib.GetType("ref::gpr_table_gem"))
weights   = model.AddRequirement(lib.GetType("buildlib::bench_evidence_weights.py"))
cohorts_m = model.AddRequirement(lib.GetType("buildlib::bench_cohorts.py"))
edges_m   = model.AddRequirement(lib.GetType("buildlib::bench_edges.py"))
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
bl        = model.AddRequirement(lib.GetType("buildlib::benchmark"))
out       = model.AddProduct(lib.GetType("bench::condition_gpr"))

COHORTS = ("gof_native", "gof_het", "lof", "eydallin")

def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ibl = context.Input(bl)
    cmd = f"""
            python3 {ibl.container}/condition_gpr.py \
            --lib {context.Input(weights).container} \
            --extracts {context.Input(extracts).container} \
            --het {context.Input(het).container} \
            --laser {context.Input(laser).container} \
            --bridge {context.Input(bridge).container} \
            --metanetx {context.Input(metanetx).container} \
            --hosts-gem {context.Input(hosts_gem).container} \
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
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=4)),
)

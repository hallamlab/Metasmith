from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
algebra     = model.AddRequirement(lib.GetType("interm::aam_algebra"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
prior       = model.AddRequirement(lib.GetType("fabfos_data::prior_bake_logs"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_fc      = model.AddProduct(lib.GetType("interm::aam_forecast"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    ialg = context.Input(algebra)
    irx  = context.Input(reactions)
    iec  = context.Input(counts)
    ipl  = context.Input(prior)
    ilib = context.Input(bakelib)
    iout = context.Output(out_fc)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups fc
        ln -sfn {irx.container} _lookups/reactions.parquet

        {py} -m ecspr.bake.aam.forecast build --lookups _lookups \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --element-counts {iec.container} \
            --forced {ialg.container}/forced_pairs.parquet \
            --prior-logs {ipl.container} \
            --out fc/forecast.parquet \
            --out-summary fc/summary.tsv

        cp fc/forecast.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool forecast \
            --file fc/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_fc: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "forecast").is_dir()
                 and any((iev.local / "forecast").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)

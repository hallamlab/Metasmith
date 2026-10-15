from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist  = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
partial   = model.AddRequirement(lib.GetType("interm::aam_partial"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_uni   = model.AddProduct(lib.GetType("interm::aam_universe"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    ipa  = context.Input(partial)
    ilib = context.Input(bakelib)
    iout = context.Output(out_uni)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p uni

        {py} -m ecspr.bake.aam.universe build \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --partial {ipa.container}/partial_universe.parquet \
            --out uni/universe.parquet \
            --out-summary uni/summary.tsv

        cp uni/universe.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool universe \
            --file uni/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_uni: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "universe").is_dir()
                 and any((iev.local / "universe").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(minutes=30)),
)

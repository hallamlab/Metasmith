from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_wl      = model.AddProduct(lib.GetType("interm::aam_worklist"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    ilib = context.Input(bakelib)
    iout = context.Output(out_wl)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p wl
        {py} -m ecspr.bake.aam.worklist build \
            --reactions {irx.container} --metabolites {imt.container} \
            --out wl/worklist.parquet --out-summary wl/summary.tsv
        cp wl/worklist.parquet {iout.container}

        # The SUMMARY is the half a human reads: the verdict histogram, the blocker
        # families, and the two limits the run was made under. A threshold recorded only
        # as a constant in a module is a threshold nobody can check a result against.
        {py} -m ecspr.bake.evidence collect --root _ev --tool worklist \
            --file wl/worklist.parquet wl/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_wl: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "worklist").is_dir()
                 and any((iev.local / "worklist").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)

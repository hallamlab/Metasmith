from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_counts  = model.AddProduct(lib.GetType("lookup::element_counts"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_counts)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p rc
        {py} -m ecspr.bake.aam.recount build \
            --metabolites {imt.container} \
            --out rc/element_counts.parquet --out-summary rc/summary.tsv

        # Before it leaves this step. A recount that disagrees with the node identity
        # contract is not a table to publish and inspect later.
        {py} -m ecspr.bake.aam.recount check \
            --counts rc/element_counts.parquet --atom-ranks {iar.container}

        cp rc/element_counts.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool recount \
            --file rc/element_counts.parquet rc/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_counts: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "recount").is_dir()
                 and any((iev.local / "recount").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)

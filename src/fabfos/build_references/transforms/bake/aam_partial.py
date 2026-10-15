from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))

forecast    = model.AddRequirement(lib.GetType("interm::aam_forecast"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_partial = model.AddProduct(lib.GetType("interm::aam_partial"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ifc  = context.Input(forecast)
    irs  = context.Input(rescue)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_partial)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    stage_lookups = f"""
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet
    """

    cmd = f"""
        set -e
        {stage_lookups}
        mkdir -p partial

        {py} -m ecspr.bake.aam.partial build --lookups _lookups \
            --forecast {ifc.container} \
            --rescue {irs.container} \
            --out partial/partial_universe.parquet \
            --out-forced partial/partial_forced.parquet \
            --out-summary partial/summary.tsv

        # THE THREE FILES TRAVEL TOGETHER. The universe carries the reduced participant
        # lists the extractor must read instead of re-parsing the equation, and the
        # summary is the only record of what the reduction could NOT reach -- which is
        # the half a coverage claim is made of.
        mkdir -p {iout.container}
        cp partial/partial_universe.parquet partial/partial_forced.parquet \
           partial/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool partial \
            --file partial/partial_universe.parquet partial/partial_forced.parquet \
                   partial/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    want = ["partial_universe.parquet", "partial_forced.parquet", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_partial: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() for f in want)
                 and (iev.local / "partial").is_dir()
                 and any((iev.local / "partial").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_alg     = model.AddProduct(lib.GetType("interm::aam_algebra"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    iec  = context.Input(counts)
    ilib = context.Input(bakelib)
    iout = context.Output(out_alg)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups alg
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet

        {py} -m ecspr.bake.aam.algebra build --lookups _lookups \
            --element-counts {iec.container} \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --out-forced alg/forced_pairs.parquet \
            --out-claims alg/claims.tsv \
            --out-summary alg/summary.tsv

        mkdir -p {iout.container}
        cp alg/forced_pairs.parquet alg/claims.tsv alg/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool algebra \
            --file alg/claims.tsv alg/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["forced_pairs.parquet", "claims.tsv", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_alg: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "algebra").is_dir()
                 and any((iev.local / "algebra").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=1)),
)

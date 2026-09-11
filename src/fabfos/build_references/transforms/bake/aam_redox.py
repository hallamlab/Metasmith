from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
stacked     = model.AddRequirement(lib.GetType("interm::aam_stack"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_pairs   = model.AddProduct(lib.GetType("interm::aam_pairs"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ist  = context.Input(stacked)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups redox
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet

        {py} -m ecspr.bake.aam.redox repair \
            --pairs {ist.container} --lookups _lookups \
            --out redox/aam_pairs.parquet \
            --out-refusals redox/refusals.parquet \
            --out-cofactors redox/cofactors.tsv \
            --out-emptied redox/emptied.txt \
            --out-summary redox/summary.tsv

        # THE FIVE FILES TRAVEL TOGETHER. A corrected table with no record of what was
        # corrected is a table nobody can check, and the emptied list is what stops a
        # reaction the repair took reading as one no mapper answered.
        mkdir -p {iout.container}
        cp redox/aam_pairs.parquet redox/refusals.parquet redox/cofactors.tsv \
           redox/emptied.txt redox/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool redox \
            --file redox/refusals.parquet redox/cofactors.tsv redox/emptied.txt \
                   redox/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    want = ["aam_pairs.parquet", "refusals.parquet", "cofactors.tsv", "emptied.txt",
            "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_pairs: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() for f in want)
                 and (iout.local / "aam_pairs.parquet").stat().st_size > 0
                 and (iev.local / "redox").is_dir()
                 and any((iev.local / "redox").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(48), duration=Duration(hours=2)),
)

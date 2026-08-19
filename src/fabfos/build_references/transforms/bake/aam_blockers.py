from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
xrefs       = model.AddRequirement(lib.GetType("lookup::xrefs"))
synonyms    = model.AddRequirement(lib.GetType("lookup::synonyms"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_blk     = model.AddProduct(lib.GetType("interm::aam_blockers"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    ixr  = context.Input(xrefs)
    isy  = context.Input(synonyms)
    iec  = context.Input(counts)
    ilib = context.Input(bakelib)
    iout = context.Output(out_blk)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {ixr.container} _lookups/xrefs.parquet
        ln -sfn {isy.container} _lookups/synonyms.parquet

        {py} -m ecspr.bake.aam.twins blockers --lookups _lookups \
            --element-counts {iec.container} \
            --worklist {iwl.container} \
            --out blockers

        mkdir -p {iout.container}
        cp blockers/crosswalk.tsv blockers/decisions.tsv blockers/summary.tsv \
           {iout.container}/

        # THE REFUSALS ARE THE DELIVERABLE THE NEXT LANE READS. `decisions.tsv` names
        # why each blocker was not resolved, under a closed set of predicates; a run
        # that shipped only the accepts would leave the next argument with nowhere to
        # start.
        {py} -m ecspr.bake.evidence collect --root _ev --tool blockers \
            --file blockers/crosswalk.tsv blockers/decisions.tsv blockers/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["crosswalk.tsv", "decisions.tsv", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_blk: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "blockers").is_dir()
                 and any((iev.local / "blockers").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=2)),
)

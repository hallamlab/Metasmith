from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
xrefs       = model.AddRequirement(lib.GetType("lookup::xrefs"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_nt      = model.AddProduct(lib.GetType("interm::aam_nametwin"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    ixr  = context.Input(xrefs)
    iec  = context.Input(counts)
    ilib = context.Input(bakelib)
    iout = context.Output(out_nt)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {ixr.container} _lookups/xrefs.parquet

        {py} -m ecspr.bake.aam.twins nametwin --lookups _lookups \
            --element-counts {iec.container} \
            --worklist {iwl.container} \
            --out nametwin

        mkdir -p {iout.container}
        cp nametwin/crosswalk.tsv nametwin/decisions.tsv nametwin/summary.tsv \
           {iout.container}/

        # The refused half of this lane is an UPSTREAM DEFECT LIST: a same-name pair
        # MetaNetX filed twice with contradicting evidence is a fact about the release,
        # and it is the only place this build records one.
        {py} -m ecspr.bake.evidence collect --root _ev --tool nametwin \
            --file nametwin/crosswalk.tsv nametwin/decisions.tsv nametwin/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    want = ["crosswalk.tsv", "decisions.tsv", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_nt: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "nametwin").is_dir()
                 and any((iev.local / "nametwin").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)

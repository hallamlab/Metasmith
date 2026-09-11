from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
chebi       = model.AddRequirement(lib.GetType("fabfos_data::chebi"))
modelseed   = model.AddRequirement(lib.GetType("fabfos_data::modelseed"))

worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
xrefs       = model.AddRequirement(lib.GetType("lookup::xrefs"))
synonyms    = model.AddRequirement(lib.GetType("lookup::synonyms"))

counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
blockers    = model.AddRequirement(lib.GetType("interm::aam_blockers"))
nametwin    = model.AddRequirement(lib.GetType("interm::aam_nametwin"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_rescue  = model.AddProduct(lib.GetType("interm::aam_rescue"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ich  = context.Input(chebi)
    ims  = context.Input(modelseed)
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ixr  = context.Input(xrefs)
    isy  = context.Input(synonyms)
    iec  = context.Input(counts)
    ibl  = context.Input(blockers)
    int_ = context.Input(nametwin)
    ilib = context.Input(bakelib)
    iout = context.Output(out_rescue)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    stage_lookups = f"""
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet
        ln -sfn {ixr.container} _lookups/xrefs.parquet
        ln -sfn {isy.container} _lookups/synonyms.parquet
    """

    cmd = f"""
        set -e
        {stage_lookups}
        mkdir -p rescue

        {py} -m ecspr.bake.aam.curation propose --lookups _lookups \
            --worklist {iwl.container} \
            --element-counts {iec.container} \
            --blockers {ibl.container}/crosswalk.tsv \
            --nametwin {int_.container}/crosswalk.tsv \
            --chebi {ich.container} --modelseed {ims.container} \
            --out rescue/crosswalk.tsv

        {py} -m ecspr.bake.aam.curation complete --lookups _lookups \
            --worklist {iwl.container} \
            --element-counts {iec.container} \
            --crosswalk rescue/crosswalk.tsv \
            --out rescue/rescued.parquet \
            --out-balance rescue/balance.tsv \
            --out-placeholders rescue/placeholders.tsv

        # THE FOUR FILES TRAVEL TOGETHER as one product. The extractor needs the
        # placeholder list to suppress scaffolding atoms and the balance table to drop
        # unbalanced elements; handed one build's crosswalk and another's placeholders it
        # would suppress the wrong atoms and say nothing about it.
        mkdir -p {iout.container}
        cp rescue/crosswalk.tsv rescue/placeholders.tsv rescue/balance.tsv \
           rescue/rescued.parquet {iout.container}/

        # The CROSSWALK is the curation: the only place the assertions are written down,
        # each with the basis that warrants it and the lane that made it. Nothing else in
        # the build records what was CLAIMED, as against what survived the gates.
        {py} -m ecspr.bake.evidence collect --root _ev --tool rescue \
            --file rescue/crosswalk.tsv rescue/placeholders.tsv rescue/balance.tsv \
                   rescue/rescued.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    want = ["crosswalk.tsv", "placeholders.tsv", "balance.tsv", "rescued.parquet"]
    return ExecutionResult(
        manifest=[{out_rescue: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "rescue").is_dir()
                 and any((iev.local / "rescue").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=4)),
)

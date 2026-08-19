from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
metacyc    = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
chebi      = model.AddRequirement(lib.GetType("fabfos_data::chebi"))
modelseed  = model.AddRequirement(lib.GetType("fabfos_data::modelseed"))
builder    = model.AddRequirement(lib.GetType("buildlib::mnx_lookups.py"))
bakelib    = model.AddRequirement(lib.GetType("buildlib::ecspr"))

reactions   = model.AddProduct(lib.GetType("lookup::reactions"))
metabolites = model.AddProduct(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddProduct(lib.GetType("lookup::atom_ranks"))
xrefs       = model.AddProduct(lib.GetType("lookup::xrefs"))
synonyms    = model.AddProduct(lib.GetType("lookup::synonyms"))

MC_FILE = "compounds.dat"


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    ich  = context.Input(chebi)
    ims  = context.Input(modelseed)
    ilib = context.Input(builder)
    iecs = context.Input(bakelib)
    libdir = ilib.container.parent
    ecsdir = iecs.container.parent

    outs = {
        "reactions": context.Output(reactions),
        "metabolites": context.Output(metabolites),
        "atom_ranks": context.Output(atom_ranks),
        "xrefs": context.Output(xrefs),
        "synonyms": context.Output(synonyms),
    }

    _cmd = f"""
        set -e
        N=$(find {imnx.container} -mindepth 1 -maxdepth 1 -type d | wc -l)
        if [ "$N" -ne 1 ]; then
            echo "[lookups] expected exactly one release under {imnx.container}, found $N." \\
                 'All four TSVs must come from one MNXref build -- an equation from one' \\
                 'release against a compound set from another is not a case to resolve' \\
                 'by taking the newest' >&2
            exit 1
        fi
        MNX=$(find {imnx.container} -mindepth 1 -maxdepth 1 -type d)

        N=$(find {imc.container} -mindepth 1 -maxdepth 1 -type d | wc -l)
        if [ "$N" -ne 1 ]; then
            echo "[lookups] expected exactly one MetaCyc release under {imc.container}, found $N" >&2
            exit 1
        fi
        MCREL=$(find {imc.container} -mindepth 1 -maxdepth 1 -type d)
        if [ -s "$MCREL/data/{MC_FILE}" ]; then
            MC=$MCREL/data
        elif [ -s "$MCREL/{MC_FILE}" ]; then
            MC=$MCREL
        else
            echo "[lookups] no {MC_FILE} under $MCREL (looked in ./ and ./data/)." \\
                 'MetaCyc flat-files are licensed and not redistributable, so nothing' \\
                 'fetches this -- place the distribution under data/originals/metacyc/<version>/.' \\
                 'Without it the synonym index loses its curated vocabulary AND the AAM' \\
                 'stack loses its base layer, so the refusal is raised here rather than' \\
                 'three transforms later' >&2
            exit 1
        fi
        echo "[lookups] metanetx $(basename $MNX) · metacyc $(basename $MCREL)"

        mkdir -p _lookups
        PYTHONPATH={ecsdir}:{libdir} python3 {libdir}/mnx_lookups.py build \
            --reac-prop $MNX/reac_prop.tsv \
            --chem-prop $MNX/chem_prop.tsv \
            --reac-xref $MNX/reac_xref.tsv \
            --chem-xref $MNX/chem_xref.tsv \
            --chebi {ich.container} \
            --modelseed {ims.container} \
            --metacyc-data $MC \
            --outdir _lookups

        PYTHONPATH={ecsdir}:{libdir} python3 {libdir}/mnx_lookups.py check --outdir _lookups
    """
    for name, o in outs.items():
        _cmd += f"\n        mv _lookups/{name}.parquet {o.container}"

    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    ok = all(o.local.exists() and o.local.stat().st_size > 0 for o in outs.values())
    return ExecutionResult(
        manifest=[{reactions: outs["reactions"].local},
                  {metabolites: outs["metabolites"].local},
                  {atom_ranks: outs["atom_ranks"].local},
                  {xrefs: outs["xrefs"].local},
                  {synonyms: outs["synonyms"].local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=3)),
)

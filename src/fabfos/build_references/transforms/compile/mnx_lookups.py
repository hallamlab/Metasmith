"""L0 -- the five lookups every metabolism step reads instead of re-parsing MetaNetX.

ONE TRANSFORM, FIVE PRODUCTS, because they share one read. `chem_prop.tsv` is 810 MB
and `chem_xref.tsv` is 678 MB; splitting this into five transforms would open them
five times to produce five tables that must agree with each other anyway. Sharing the
read is a side benefit -- the reason they are one step is that the atom node identity
`(mnxm, canonical rank)` has to be produced by exactly one piece of code, and a
transform boundary between `atom_ranks` and `reactions` is an invitation for a second
one to appear.

WHY THESE LAND UNDER data/processed/ RATHER THAN IN THE WORK DIRECTORY. The tier rule
says anything derived and not named in REFERENCES.md is transient. These are named
there, deliberately, and the reason is diagnostic rather than architectural: when a
metabolite fails to resolve three layers later, the question "what did the build think
this metabolite was" has to be answerable without re-running an eight-hour graph.

REQUIRES THE LICENSED DROP-IN, and says so where the file is read. MetaCyc contributes
53,252 compound names to the synonym index -- and it is also layer 1 of the AAM stack,
so a build without the drop-in was never going to produce R6 anyway. The refusal lands
here first, which is earlier and cheaper than at the ensemble.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
metacyc    = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
chebi      = model.AddRequirement(lib.GetType("fabfos_data::chebi"))
modelseed  = model.AddRequirement(lib.GetType("fabfos_data::modelseed"))
builder    = model.AddRequirement(lib.GetType("buildlib::mnx_lookups.py"))
# For ONE import: `atom_pairs.count_element`, the tree's single formula counter. This
# step's own argument is that a derivation five consumers make for themselves drifts,
# and it used to carry a private copy of exactly such a derivation.
bakelib    = model.AddRequirement(lib.GetType("buildlib::ecspr"))

reactions   = model.AddProduct(lib.GetType("lookup::reactions"))
metabolites = model.AddProduct(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddProduct(lib.GetType("lookup::atom_ranks"))
xrefs       = model.AddProduct(lib.GetType("lookup::xrefs"))
synonyms    = model.AddProduct(lib.GetType("lookup::synonyms"))

# The MetaCyc file this step needs, and the two drop-in shapes that are both legitimate:
# a distribution unpacked as downloaded keeps `<release>/data/`, a hand-flattened one
# puts the .dat files at the top of the release directory.
MC_FILE = "compounds.dat"


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    ich  = context.Input(chebi)
    ims  = context.Input(modelseed)
    ilib = context.Input(builder)
    iecs = context.Input(bakelib)
    # TWO directories, not one. Staging is content-addressed, so a library's items are
    # NOT siblings on the executing node -- `mnx_lookups.py` and `ecspr/` each land under
    # their own hash. Both go on PYTHONPATH; only the first is where the script is.
    libdir = ilib.container.parent
    ecsdir = iecs.container.parent

    outs = {
        "reactions": context.Output(reactions),
        "metabolites": context.Output(metabolites),
        "atom_ranks": context.Output(atom_ranks),
        "xrefs": context.Output(xrefs),
        "synonyms": context.Output(synonyms),
    }

    # The release directory is resolved HERE, at run time, on the executing node -- the
    # same rule R5 and R6 follow. Kept free of literal `{`/`}` because this block is an
    # f-string and a stray brace fails in a shell rather than at import.
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
    # NOT labels=["local"]. That label is right for `acquire/` -- a download needs the
    # login node's network -- and copying it here is what pinned every compile to the
    # login node under the slurm preset: `xlocalx` sets `executor = 'local'`, whose pool
    # slurm.nf declares as 8 cores / 8 GB, and Nextflow's local executor REFUSES a
    # process asking for more rather than queueing it. It also sets
    # errorStrategy='ignore' with no retry, so the refusal is silent and the workflow
    # goes green with all five tables absent. This step reads four staged TSVs and
    # allocates 32 GB doing it; it belongs on a compute node.
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=3)),
)

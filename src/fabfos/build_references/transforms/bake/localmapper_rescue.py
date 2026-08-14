"""AAM member lane: LocalMapper over pass 2's GAP. The third rescue vote.

Same role as the pass-1 LocalMapper lane and for the same reason: it maps only what the
other two members of its own pass did not reach. Read `localmapper.py` for why the role
matters more than the tool here, and `indigo_rescue.py` for what the extraction's three
extra arguments do.

THE GAP IS PER PASS, not global. A reaction Indigo mapped in pass 1 under MetaNetX's
SMILES says nothing about the same MNXR completed with curated structures -- it is a
different molecule. So the covered set handed in is pass 2's own two members, and the
reactions left are the completed ones neither could map.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
m_indigo  = model.AddRequirement(lib.GetType("interm::aam_member_indigo_rescue"))
m_rxn     = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper_rescue"))
neural    = model.AddRequirement(lib.GetType("buildlib::aam_neural_members.py"))
sharder   = model.AddRequirement(lib.GetType("buildlib::aam_shard.py"))
extractor = model.AddRequirement(lib.GetType("buildlib::ecspr_atom_pairs.py"))
evidence  = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_localmapper_rescue"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "localmapper"
TOOL = "localmapper_rescue"
MEM_BUDGET_GB = 12
# Same per-reaction bound as pass 1, and NOT because this lane has hit it. It completed
# in minutes on the run where pass 1 had to be killed. But it is the same mapper over the
# same kind of set -- the reactions two other members could not reach -- so the only
# reason it has not stalled yet is that its gap is smaller. An unbounded lane that has so
# far been lucky is still an unbounded lane. Not sharded: pass 2's gap is small enough
# that one process is honest, which is exactly what was believed about pass 1.
TIMEOUT_S = 240

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[localmapper_rescue] expected one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ires = context.Input(rescue)
    iind = context.Input(m_indigo)
    irxn = context.Input(m_rxn)
    ilib = context.Input(neural)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent
    R = ires.container

    resolve = RESOLVE.format(metanetx=imnx.container)
    # Same container-HOME problem as pass 1; see localmapper.py for why DGL cannot import
    # without this. Both lanes need it because both import the same mapper.
    py = (f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 "
          f"HOME=$PWD DGLBACKEND=pytorch "
          f"MPLCONFIGDIR=$PWD/_cache/mpl XDG_CACHE_HOME=$PWD/_cache/xdg python3")

    cmd = f"""
        {resolve}
        mkdir -p members
        {py} {libdir}/aam_neural_members.py --member {MEMBER} \
            --worklist {R}/rescued.parquet \
            --covered {iind.container} {irxn.container} \
            --timeout {TIMEOUT_S} \
            --sidecar members/{TOOL}.attempted \
            --timeout-log members/{TOOL}.timeouts \
            --mem-budget-gb {MEM_BUDGET_GB} \
            --out members/{TOOL}.tsv
        {py} {libdir}/ecspr_atom_pairs.py extract \
            --aam members/{TOOL}.tsv --align strict \
            --placeholders {R}/placeholders.tsv --resolved {R}/crosswalk.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{TOOL}_status.tsv

        {py} {libdir}/build_evidence.py collect --root _ev --tool {TOOL} \
            --file members/{TOOL}.tsv members/{TOOL}.attempted \
                   members/{TOOL}.timeouts \
                   members/{TOOL}_status.tsv {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / TOOL).is_dir()
                 and any((iev.local / TOOL).iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=4)),
)

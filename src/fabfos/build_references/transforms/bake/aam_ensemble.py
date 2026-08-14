"""The AAM assembly: fuse six members, lay the layers down, and mint the bake.

WHAT IS HERE AND WHAT IS NOT. Every member that runs a MODEL is its own lane, and there
are now six of them -- rxnmapper, localmapper and indigo over the adjudicated worklist,
and the same three over the rescued universe. What is left is here: the curated layer,
the arithmetic over the members, the stack, and the ledger close.

THE CURATION SWEEP MOVED OUT, to `aam_rescue`, and that is the substantive change. It
used to run here, after every member had finished, with Indigo mapping the completed
reactions on the spot -- which is why in the deployed table all 9,089 rescue-derived
reactions are `mcs_only`, one member at half weight, on 14.5% of the table. Completing
the reactions BEFORE the mappers run lets all three see them, so agreement becomes
consensus at full weight. `aam_worklist close` reports how many moved.

THE CURATED MEMBER IS READ HERE, FROM THE DROP-IN, rather than arriving as a product.
It briefly had its own transform, and the argument for that was real -- one read of the
licensed release, feeding both ensembles, so the two could not disagree about which
MetaCyc they were built from. The argument against it won: it put a node between the
drop-in and every member lane, because the members took its output as their `--exclude`
list, so nothing could start until the curated extraction finished. Reading the .dat
here costs ~5 minutes and is the same code either way; `direction_ensemble` reads the
other .dat for itself, and the two agree because the drop-in asserts a single release
directory and there is only one to read.

THE ARCHITECTURE IS THE ORDER, and the order is additive:

  L1  metacyc     the curated map, expert-assigned, balancing per element at 99.8%+.
                  Extracted here from the drop-in, and laid down FIRST -- which is what
                  makes the members' overlap with it free rather than contested: stack
                  restricts each layer to what nothing below it claimed.
  L2  ensemble    RXNMapper + LocalMapper + Indigo over the worklist, fused where they
                  agree. The neural increment balances carbon in 12.5% of the candidates
                  it proposes, which is the whole argument for it being second rather
                  than first.
  L3  curation    the SAME THREE over the rescued universe: reactions no mapper could
                  see until `aam_rescue` supplied a structure for their structureless
                  participants. Their bodies had to cancel and an element had to balance
                  before the reaction was completed at all, so what reaches here has
                  already passed the gates that used to run after the mapping.

ADDITIVE MEANS ADDITIVE, AND THE CLAIM IS TESTED. `aam_layers.additive_gates` refuses
rather than warns at every boundary: the added (mnxr, element) is absent from everything
below, zero collisions on the 6-tuple pair key, no element loses reactions, no negative
ranks. A gate that warns is a gate that gets read once.

WHERE MEMBERS DISAGREE the correspondence is not decided by majority or by confidence --
it is spread across the disputed products by member weight, so a contested atom dilutes
its transfer instead of committing. Provenance (method / source / confidence) records who
spoke; it is never a usability gate, and every emitted pair is read.

A MEMBER THAT DID NOT RUN IS A MISSING NODE, not a missing column. Under the old single
transform an absent member was a `[ -s ... ] || continue` inside a shell loop, and a
two-member ensemble looked exactly like a three-member one from the outside. Here each
member is a required input: the planner cannot schedule this step without all six, and
dropping one is an edit to the graph that someone has to make on purpose.

IT CLOSES THE LEDGER. `aam_worklist close` joins the adjudication to the finished table
so every MNXR ends with an outcome from a closed set -- banked, mapped and produced
nothing, rescued and produced nothing, no lane would complete it, or the verdict that
refused it before any lane ran. That is what makes a miss against the deployed table a
result rather than a mystery.

IT MINTS THE BAKE, which used to be a third step reading both assemblies' outputs. Two of
the trio are written here -- the vocabulary and the encoded pairs -- and the direction
assembly writes the third against THIS vocabulary. What made the trio one step was that
all three files must carry a byte-identical identity block; what makes two steps safe is
that the block is minted once, here, and inherited rather than recomputed. The reaction
space is `lookup::reactions`, already an input for the layers, so nothing new is read to
get it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx    = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

metacyc     = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
m_rxn       = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper"))
m_local     = model.AddRequirement(lib.GetType("interm::aam_member_localmapper"))
m_indigo    = model.AddRequirement(lib.GetType("interm::aam_member_indigo"))
m_rxn_r     = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper_rescue"))
m_local_r   = model.AddRequirement(lib.GetType("interm::aam_member_localmapper_rescue"))
m_indigo_r  = model.AddRequirement(lib.GetType("interm::aam_member_indigo_rescue"))

worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))

curated_m   = model.AddRequirement(lib.GetType("buildlib::aam_metacyc_member.py"))
worklib     = model.AddRequirement(lib.GetType("buildlib::aam_worklist.py"))
layers      = model.AddRequirement(lib.GetType("buildlib::aam_layers.py"))
extractor   = model.AddRequirement(lib.GetType("buildlib::ecspr_atom_pairs.py"))
evidence    = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))
encoding    = model.AddRequirement(lib.GetType("buildlib::refs_encoding.py"))
baker       = model.AddRequirement(lib.GetType("buildlib::bake_metabolism.py"))

stacked     = model.AddProduct(lib.GetType("interm::aam_pairs"))
out_vocab   = model.AddProduct(lib.GetType("ref::metabolism_vocab"))
out_pairs   = model.AddProduct(lib.GetType("ref::atom_pairs"))
# ONE evidence product, holding `<tool>/<version>/` for each tool this step ran -- here
# `metacyc/` and `ensemble/`. Two products of one type would collide: an output's filename
# is built from its type key and its branch index, and two products of one transform share
# both.
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))

CURATED_DAT = "atom-mappings-smiles.dat"

RESOLVE = f"""
    set -e
    N=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[aam] expected exactly one release under {{metanetx}}, found $N." \\
             'An equation from one MNXref build against a structure from another is not' \\
             'a case to resolve by taking the newest' >&2
        exit 1
    fi
    MNX=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d)

    N=$(find {{metacyc}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[aam] expected exactly one MetaCyc release under {{metacyc}}, found $N." \\
             'Which release the references are built from is a provenance decision, and' \\
             'this is the ONE input with no external record of that decision' >&2
        exit 1
    fi
    MCREL=$(find {{metacyc}} -mindepth 1 -maxdepth 1 -type d)
    MCVER=$(basename $MCREL)
    # Both drop-in shapes are legitimate: a distribution unpacked as downloaded keeps
    # `<release>/data/`, a hand-flattened one puts the .dat files at the top.
    if [ -s "$MCREL/data/{CURATED_DAT}" ]; then
        MC=$MCREL/data
    elif [ -s "$MCREL/{CURATED_DAT}" ]; then
        MC=$MCREL
    else
        echo "[aam] no {CURATED_DAT} under $MCREL (looked in ./ and ./data/)." \\
             'MetaCyc flat-files are LICENSED and not redistributable, so nothing' \\
             'fetches this -- place the distribution under' \\
             'data/originals/metacyc/<release>/. Losing it does not shrink this' \\
             'ensemble evenly: it removes LAYER 1, the only member that is a curated' \\
             'database rather than a model, and the only one that can break a tie' \\
             'between two transformers over the same SMILES' >&2
        exit 1
    fi
    echo "[aam] metanetx $(basename $MNX) · metacyc $MCVER"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    irxn = context.Input(m_rxn)
    iloc = context.Input(m_local)
    iind = context.Input(m_indigo)
    irxn_r = context.Input(m_rxn_r)
    iloc_r = context.Input(m_local_r)
    iind_r = context.Input(m_indigo_r)
    iwl  = context.Input(worklist)
    ires = context.Input(rescue)
    irx  = context.Input(reactions)
    ilib = context.Input(layers)
    iout = context.Output(stacked)
    ivoc = context.Output(out_vocab)
    ienc = context.Output(out_pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, metacyc=imc.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p L1 L2 L3

        # ---- L1: the curated map --------------------------------------------------
        # `--align structural` is not a loosening. MetaCyc wrote these SMILES over ITS
        # OWN participant set -- it writes the water MetaNetX leaves implicit and omits
        # the proton MetaNetX lists -- so template count and participant count are
        # expected to differ and carry no information. Requiring them equal refused
        # 55.5% of the curated records for a reason that was never chemistry.
        # `--connectivity-fallback` is the InChIKey connectivity match TIER4_FREEZE
        # named and recorded as never attempted.
        {py} {libdir}/aam_metacyc_member.py \
            --smiles-dat $MC/{CURATED_DAT} --reac-xref $MNX/reac_xref.tsv \
            --out L1/metacyc.tsv --out-report L1/refused_residues.tsv
        {py} {libdir}/ecspr_atom_pairs.py extract \
            --aam L1/metacyc.tsv --align structural --connectivity-fallback \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out L1/pairs.parquet --out-status L1/status.tsv

        # The NAMED RESIDUE REPORT is why this evidence is a file and not a print:
        # "MetaCyc reaches 13,947 of 16,818" is only interpretable next to which 2,804
        # were declined and what each cost. MetaCyc has no package to ask, so its
        # RELEASE is the version -- the same one the drop-in is filed under.
        {py} {libdir}/build_evidence.py collect --root _ev --tool metacyc \
            --version $MCVER \
            --file L1/metacyc.tsv L1/refused_residues.tsv L1/pairs.parquet \
                   L1/status.tsv

        # ---- L2: fuse the three pass-1 members ------------------------------------
        # All three are required inputs, so there is no "skip the absent one" branch to
        # write. If a member is to be dropped, it is dropped from the graph.
        {py} {libdir}/aam_layers.py fuse \
            --member rxnmapper={irxn.container} \
            --member localmapper={iloc.container} \
            --member indigo={iind.container} \
            --out L2/stack.parquet

        # ---- L3: fuse the three pass-2 members ------------------------------------
        # THE SAME ARITHMETIC AS L2, over the rescued universe. It is the same fusion
        # because it is the same question -- three mappers, one reaction, who agrees --
        # and the fact that the reaction reached them through a curated structure is
        # already recorded in the crosswalk and already tested by the balance gate that
        # let it through. Fusing rather than stamping `curated_recovery, 1.0` is what
        # makes a rescued row's provenance say which members actually spoke.
        {py} {libdir}/aam_layers.py fuse \
            --member rxnmapper={irxn_r.container} \
            --member localmapper={iloc_r.container} \
            --member indigo={iind_r.container} \
            --out L3/stack.parquet

        # ---- the stack ------------------------------------------------------------
        # In order, most trusted first.
        # Written under its own NAME first and copied to the product path second. The
        # product path is content-addressed and says nothing about what is in it, so
        # collecting it directly would put an opaque filename in the evidence -- and the
        # evidence is the half a human reads.
        {py} {libdir}/aam_layers.py stack \
            --layer metacyc=L1/pairs.parquet,curated,1.0 \
            --layer ensemble=L2/stack.parquet \
            --layer curation=L3/stack.parquet \
            --out aam_pairs.parquet
        cp aam_pairs.parquet {iout.container}

        # ---- close the ledger ------------------------------------------------------
        # Every MNXR gets an outcome, including the ones no lane ever attempted. This is
        # what lets the tier-4 gate report a miss WITH ITS REASON instead of reporting a
        # number nobody can act on.
        {py} {libdir}/aam_worklist.py close \
            --worklist {iwl.container} --pairs aam_pairs.parquet \
            --rescued {ires.container}/rescued.parquet \
            --out L3/ledger.parquet --out-summary L3/ledger_summary.tsv

        # ---- the bake: vocabulary + encoded pairs ---------------------------------
        # Two of the trio. The third is written by `direction_ensemble` against the
        # vocabulary minted here, so the identity block is computed once and inherited
        # rather than agreed on twice. `pairs` bakes AND selftests in one invocation and
        # exits non-zero on a failed round trip -- a merged pair of atoms RAISES the
        # network's conductance, so it reads downstream as an improvement.
        {py} {libdir}/bake_metabolism.py pairs \
            --aam-pairs aam_pairs.parquet --reactions {irx.container} \
            --out-vocab {ivoc.container} --out-pairs {ienc.container}

        # The LEDGER is the half a human reads: what every reaction in the universe did,
        # next to the two fused layer tables that explain it. The crosswalk itself is
        # `aam_rescue`'s evidence, where it is produced.
        {py} {libdir}/build_evidence.py collect --root _ev --tool ensemble \
            --file L2/stack.parquet L3/stack.parquet aam_pairs.parquet \
                   L3/ledger.parquet L3/ledger_summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    # Evidence is a HARD condition, not a diagnostic nicety. The copy runs seconds after
    # the tools finished, in the same command, so a missing directory does not mean the
    # step was busy -- it means something went wrong that a green step would hide.
    # Both tools by name, not "the directory is non-empty": this step runs two, and one
    # of them silently failing to collect is exactly the case a count would pass.
    kept = [iev.local / t for t in ("metacyc", "ensemble")]
    return ExecutionResult(
        manifest=[{stacked: iout.local},
                  {out_vocab: ivoc.local},
                  {out_pairs: ienc.local},
                  {ev: iev.local}],
        success=all(p.exists() and p.stat().st_size > 0
                    for p in (iout.local, ivoc.local, ienc.local))
                and all(d.is_dir() and any(d.iterdir()) for d in kept),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # WAS cpus=8, 64 GB, 8 h, and almost all of it was the curation sweep -- the synonym
    # search and Indigo arbitrating every completed reaction. That is `aam_rescue`'s cost
    # now. What is left is the MetaCyc read, two fusions, a stack and the bake: large
    # tables in memory, no search.
    resources=Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=4)),
)

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx    = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
metacyc     = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))

m_rxn       = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper"))
m_local     = model.AddRequirement(lib.GetType("interm::aam_member_localmapper"))
m_indigo    = model.AddRequirement(lib.GetType("interm::aam_member_indigo"))
partial     = model.AddRequirement(lib.GetType("interm::aam_partial"))
algebra     = model.AddRequirement(lib.GetType("interm::aam_algebra"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

stacked     = model.AddProduct(lib.GetType("interm::aam_stack"))
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
             'data/fabfos/originals/metacyc/<release>/. Losing it does not shrink this' \\
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
    ipar = context.Input(partial)
    ialg = context.Input(algebra)
    ilib = context.Input(bakelib)
    iout = context.Output(stacked)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, metacyc=imc.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    members = (f"--member rxnmapper={irxn.container} "
               f"--member localmapper={iloc.container} "
               f"--member indigo={iind.container}")

    cmd = f"""
        {resolve}
        mkdir -p L1 L2 L3 L5

        # ---- L1: the curated map --------------------------------------------------
        # `--align structural` is not a loosening. MetaCyc wrote these SMILES over ITS
        # OWN participant set -- it writes the water MetaNetX leaves implicit and omits
        # the proton MetaNetX lists -- so template count and participant count are
        # expected to differ and carry no information. Requiring them equal refused
        # 55.5% of the curated records for a reason that was never chemistry.
        # `--connectivity-fallback` is the InChIKey connectivity match TIER4_FREEZE
        # named and recorded as never attempted.
        {py} -m ecspr.bake.aam.metacyc_member \
            --smiles-dat $MC/{CURATED_DAT} --reac-xref $MNX/reac_xref.tsv \
            --out L1/metacyc.tsv --out-report L1/refused_residues.tsv
        {py} -m ecspr.bake.atom_pairs extract \
            --aam L1/metacyc.tsv --align structural --connectivity-fallback \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out L1/pairs.parquet --out-status L1/status.tsv

        # The NAMED RESIDUE REPORT is why this evidence is a file and not a print:
        # "MetaCyc reaches 13,947 of 16,818" is only interpretable next to which 2,804
        # were declined and what each cost. MetaCyc has no package to ask, so its
        # RELEASE is the version -- the same one the drop-in is filed under.
        {py} -m ecspr.bake.evidence collect --root _ev --tool metacyc \
            --version $MCVER \
            --file L1/metacyc.tsv L1/refused_residues.tsv L1/pairs.parquet \
                   L1/status.tsv

        # ---- L2 / L3 / L5: the SAME THREE FILES, partitioned by submission class ----
        # All three members are required inputs, so there is no "skip the absent one"
        # branch to write. If a member is to be dropped, it is dropped from the graph.
        # One pass, three layers, and no layer that exists only because of the order the
        # passes happened to run in.
        {py} -m ecspr.bake.aam.layers fuse --submission-class whole \
            {members} --out L2/stack.parquet

        # The rescue's completions. THE SAME ARITHMETIC AS L2, because it is the same
        # question -- three mappers, one reaction, who agrees. That the reaction reached
        # them through a curated structure is already recorded in the crosswalk and
        # already tested by the balance gate that let it through; fusing rather than
        # stamping `curated_recovery, 1.0` is what makes a rescued row's provenance say
        # which members actually spoke.
        {py} -m ecspr.bake.aam.layers fuse --submission-class completed \
            {members} --out L3/stack.parquet

        # The element reductions. What makes this layer different is not how its members
        # are fused but what a row of it CLAIMS: one element of a reaction nothing mapped
        # whole. That is carried in the method it is stamped with at stack time.
        {py} -m ecspr.bake.aam.layers fuse --submission-class reduced \
            {members} --out L5/stack.parquet

        # ---- the stack ------------------------------------------------------------
        # In order, most trusted first. The two forced arms need no fusion -- conservation
        # settles them, so there is one answer and no members to disagree -- and they are
        # laid down ahead of the mapped partials because "conservation leaves no choice"
        # outranks "three mappers agreed about a reduced submission".
        #
        # Written under its own NAME first and copied to the product path second. The
        # product path is content-addressed and says nothing about what is in it, so
        # collecting it directly would put an opaque filename in the evidence -- and the
        # evidence is the half a human reads.
        {py} -m ecspr.bake.aam.layers stack \
            --layer metacyc=L1/pairs.parquet,curated,1.0 \
            --layer ensemble=L2/stack.parquet \
            --layer curation=L3/stack.parquet \
            --layer algebra={ialg.container}/forced_pairs.parquet,conservation_algebra,1.0 \
            --layer partial={ipar.container}/partial_forced.parquet,partial_forced,1.0 \
            --layer partial=L5/stack.parquet,partial_reduced,1.0 \
            --out aam_stack.parquet
        cp aam_stack.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool stack \
            --file L2/stack.parquet L3/stack.parquet L5/stack.parquet \
                   aam_stack.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    kept = [iev.local / t for t in ("metacyc", "stack")]
    return ExecutionResult(
        manifest=[{stacked: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and all(d.is_dir() and any(d.iterdir()) for d in kept)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=3)),
)

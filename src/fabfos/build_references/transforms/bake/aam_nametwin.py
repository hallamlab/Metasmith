"""Recover a structure from MetaNetX's own duplicate record of the same compound.

MetaNetX files some compounds twice, one record carrying the structure and the other
carrying nothing but the name. Where that is what happened, the fix is to read the
record that is already there -- no stand-in, no inference, no invention.

WHERE IT IS NOT WHAT HAPPENED, A SHARED NAME IS WORTH NOTHING. `UDP` names both a
nucleotide and a glycan in MNXref, and they share no accession and no skeleton. So this
lane demands evidence beyond the name and refuses on either of the two ways the name can
lie: an InChIKey connectivity block that differs, and an acyl or peptide name whose own
nomenclature asserts a size the twin does not hold. That last guard runs BEFORE any
chemistry, because `MNXM900` 'hexadecenoate' against a `CO2*` twin BALANCES -- one carbon
in, one carbon out -- and only the name knows a C16 fatty acid was turned into a formate.

WHAT COUNTS AS EVIDENCE, in strength order: a source accession both records carry (two
databases saying these are one compound), or a per-element balance that closes only after
the substitution and whose unspecified residue slots cancel. Balance is tautological for
a lone unknown in general; it is evidence here because every other participant's count is
fixed, so a wrong twin has to hit an exact number.

Its yield is small by construction -- the reference measurement puts it at 42 reactions --
and it is a separate transform anyway, because a lane admitting REAL atoms on
circumstantial evidence must never have its delta folded into the safe one's.
"""
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

    # NO SYNONYM INDEX HERE, and its absence is the scope of the lane. `aam_blockers`
    # widens its vocabulary with ChEBI/ModelSEED/MetaCyc names because it is hunting a
    # ROLE that different databases spell differently. "Same-name duplicate" is a claim
    # about MetaNetX's own filing, so the name it compares is MetaNetX's own.
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

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
    # Cheaper than its sibling: no synonym index, and the balance arm only runs for the
    # candidates that survive every name and skeleton guard ahead of it.
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)

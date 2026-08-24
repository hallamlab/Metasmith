# BioNumbers -- the quantities metabolomics does not measure.
#
# ONE COMPOUND, AND IT IS WORTH A TRANSFORM. Under the measured-E.-coli rule this source
# contributes dissolved O2 and nothing else: every CO2 record BioNumbers holds is a
# solubility in water at a stated partial pressure with organism `Generic`, which is an
# extracellular equilibrium rather than a cytoplasmic pool. But O2 appears in 10,378
# in-graph reactions -- the third most common participant in the universe after water and
# the proton, both of which eQuilibrator's prime potentials already handle -- and no
# metabolomics database measures it, because mass spectrometry of a cell extract does not
# see a dissolved gas. ECMDB carries 1,186 measurements and not one is a gas.
#
# THERE IS NO BULK DOWNLOAD AND NO API, so the acquisition is a search plus the record pages
# for the BNIDs the bake actually reads. Both are kept: the search response is what makes
# the SELECTION checkable (a reader can see what else was there and was refused), and the
# record pages are what make each VALUE checkable against its own source.
#
# WHICH RECORDS ARE READ IS NOT DECIDED HERE. `sources/bionumbers.tsv` names them and
# `sources/bionumbers_rejected.tsv` records every refusal with its reason. This transform
# fetches; the adapter selects.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::bionumbers"))

RETRIEVED = "2026-08-24"
RECORD_URL = "https://bionumbers.hms.harvard.edu/bionumber.aspx?id={bnid}"

# The BNIDs `sources/bionumbers.tsv` selects. Listed here so the fetch is self-contained --
# a transform that read the adapter's table would need the bake library staged to acquire
# anything, which inverts the tier.
BNIDS = ("108724", "109182")

MIN_RECORDS = 100
MIN_PAGE_BYTES = 5000


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    fetches = "\n".join(
        f"        curl -sfL -A \"$UA\" '{RECORD_URL.format(bnid=b)}' -o $D/records/bnid_{b}.html\n"
        f"        B=$(stat -c%s $D/records/bnid_{b}.html)\n"
        f"        echo \"[bionumbers] BNID {b}: $B bytes\"\n"
        f"        if [ \"$B\" -lt {MIN_PAGE_BYTES} ]; then\n"
        f"            echo '[bionumbers] BNID {b} came back short -- refusing to pin a page'\\\n"
        f"                 'whose value cannot be checked against it' >&2\n"
        f"            exit 1\n"
        f"        fi"
        for b in BNIDS)

    cmd = f"""
        set -e
        UA='Mozilla/5.0 (research; metasmith bake direction lane)'
        D={iout.container}/{RETRIEVED}
        mkdir -p $D/records
{fetches}
        if [ ! -s $D/records.json ]; then
            echo '[bionumbers] records.json is a SEARCH RESPONSE and BioNumbers has no API' \\
                 'to reproduce it from. It is staged with the chunk; see PROVENANCE.md.' >&2
        fi
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    d = iout.local / RETRIEVED
    got = [b for b in BNIDS
           if (d / "records" / f"bnid_{b}.html").exists()
           and (d / "records" / f"bnid_{b}.html").stat().st_size > MIN_PAGE_BYTES]
    Log.Info(f"bionumbers {RETRIEVED}: {len(got)}/{len(BNIDS)} record pages")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(BNIDS),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=15)),
)

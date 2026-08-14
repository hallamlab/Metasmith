"""ChEBI flat files -- one of the two external name/accession suppliers.

WHY A THIRD COMPOUND VOCABULARY AT ALL. The curation sweep's job is to supply a
structure for a MetaNetX metabolite that has none. Measured on the blocking set:
0 of 1,531 blocking stubs carry a formula, InChI, InChIKey or SMILES, and 100% carry
a NAME. A name plus a cross-reference accession is therefore the entire handle, and
resolving either one needs a vocabulary that is not MetaNetX -- otherwise the name is
both the claim and its only support, which is the trap this lane has already been
burned by.

PINNED TO AN ARCHIVED RELEASE, NOT TO `flat_files/`. ChEBI serves the current build
at `pub/databases/chebi/flat_files/`, which rolls: two fetches months apart put
different vocabularies at one path with nothing to tell them apart. `archive/rel<N>/`
is frozen at its own URL, so the release number is a real pin -- the MetaNetX shape,
not the Rhea/KOfam one. The cost is that the archive lags the live path (rel252 is
2026-05-01, the live tree 2026-07-07); the benefit is that this transform returns the
same bytes in a year. A vocabulary two months stale supplies slightly fewer synonyms;
an unpinnable one supplies an unknown number.

NOTHING IS UNPACKED. The three .gz arrive as served; the parse belongs to the
processed tier, the same rule KOfam's tarball follows.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::chebi"))

RELEASE = "rel252"
BASE_URL = f"https://ftp.ebi.ac.uk/pub/databases/chebi/archive/{RELEASE}/flat_files"

# Three of the fourteen the release ships. `names` is the synonym list, `compounds` is
# the id/parent/star table the synonyms hang off, `structures` carries the InChI and
# SMILES a resolved accession is actually worth something for. The other eleven are
# ontology relations and provenance this lane never reads.
FILES = ("compounds.tsv.gz", "names.tsv.gz", "structures.tsv.gz")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # ChEBI publishes no checksum sidecars, so the transfer is verified by re-reading
    # what arrived: a truncated gzip fails `gzip -t`, which is the strongest offline
    # check available for these files and catches exactly the failure a 89 MB download
    # has.
    _cmd = f"""
        set -e
        D={iout.container}/{RELEASE}
        mkdir -p $D
        for f in {" ".join(FILES)}; do
            wget -q {BASE_URL}/$f -O $D/$f
            if ! gzip -t $D/$f; then
                echo "[chebi] $f is not a valid gzip stream -- truncated transfer" >&2
                exit 1
            fi
            echo "[chebi] $f $(stat -c%s $D/$f) bytes, gzip ok"
        done
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    d = iout.local / RELEASE
    got = [f for f in FILES if (d / f).exists() and (d / f).stat().st_size > 0]
    Log.Info(f"chebi {RELEASE}: {len(got)}/{len(FILES)} files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)

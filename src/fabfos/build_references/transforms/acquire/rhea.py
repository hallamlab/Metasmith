"""Rhea -- the reaction/UniProt cross-reference distribution.

The whole served set, mirroring the FTP layout into `rhea/<release>/`:
`tsv/` (7 tables), `ctfiles/rhea-rxn.tar.gz`, plus `rhea-release.properties` and
`LICENSE.txt` at the release root.

THE RELEASE NUMBER IS READ FROM THE SERVER, NOT ASSUMED. Rhea's top-level path is a
ROLLING release -- the same URL serves different bytes over time and only
`old_releases/<n>.tar.bz2` is stable. A snapshot taken without recording the number
is unidentifiable afterwards: the files carry no version header, so the only way
back is matching sizes against 40-odd 400 MB tarballs. That is not hypothetical --
the previous snapshot here had to be dated by comparing file mtimes against release
dates to establish it was 140.

Fetching `rhea-release.properties` FIRST and naming the directory after it makes
the product self-describing, and `old_releases/<n>.tar.bz2` becomes a permanent
re-fetch path for exactly these bytes.

ALL EIGHT DATA FILES, not just the two the bridge reads. `mnxr_lookup` consumes
rhea2uniprot{,_trembl} and nothing else, but the other six are part of what Rhea
published and this tier's contract is fidelity to the source. Under the old
per-file typing they sat on disk untyped, with nothing asserting they belonged.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::rhea"))

BASE_URL = "https://ftp.expasy.org/databases/rhea"

TSV = ("rhea2uniprot.tsv", "rhea2uniprot_trembl.tsv.gz", "rhea-directions.tsv",
       "rhea-reaction-smiles.tsv", "rhea2ec.tsv", "rhea2kegg_reaction.tsv",
       "rhea2xrefs.tsv")
CTFILES = ("rhea-rxn.tar.gz",)
ROOT_FILES = ("rhea-release.properties", "LICENSE.txt")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        mkdir -p {iout.container}
        wget -q {BASE_URL}/rhea-release.properties -O {iout.container}/.release
        REL=$(grep '^rhea.release.number' {iout.container}/.release | cut -d= -f2 | tr -d ' \\r')
        if [ -z "$REL" ]; then
            echo "[rhea] could not read a release number; refusing to write an unlabelled snapshot" >&2
            exit 1
        fi
        echo "[rhea] release $REL"
        D={iout.container}/$REL
        mkdir -p $D/tsv $D/ctfiles
        for f in {" ".join(ROOT_FILES)}; do
            wget -q {BASE_URL}/$f -O $D/$f
        done
        for f in {" ".join(TSV)}; do
            wget -q {BASE_URL}/tsv/$f -O $D/tsv/$f
        done
        for f in {" ".join(CTFILES)}; do
            wget -q {BASE_URL}/ctfiles/$f -O $D/ctfiles/$f
        done
        rm -f {iout.container}/.release
        gzip -t $D/tsv/rhea2uniprot_trembl.tsv.gz
        tar -tzf $D/ctfiles/rhea-rxn.tar.gz > /dev/null
        echo "[rhea] $REL complete; archives are permanently re-fetchable at"
        echo "       {BASE_URL}/old_releases/$REL.tar.bz2"
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    rels = [p for p in iout.local.glob("*") if p.is_dir()]
    n = sum(1 for r in rels for f in TSV if (r / "tsv" / f).exists())
    Log.Info(f"rhea: {[r.name for r in rels]}, {n}/{len(TSV)} tsv files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(rels) == 1 and n == len(TSV),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)

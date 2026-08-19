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

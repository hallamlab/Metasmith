from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
db      = model.AddProduct(lib.GetType("amplicon::silva_db"))

# SINA aligns against SILVA 138.2's ARB export; the QIIME2 artifacts are 138. The same
# pairing ASPIRE's own configs use. The classifier's path pins the scikit-learn it was
# trained with, which the taxonomy env must match.
FILES = {
    "silva.arb.gz": "https://www.arb-silva.de/fileadmin/silva_databases/release_138_2/ARB_files/SILVA_138.2_SSURef_NR99_03_07_24_opt.arb.gz",
    "silva_seqs.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-seqs.qza",
    "silva_tax.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-tax.qza",
    "silva_nb_classifier.qza": "https://data.qiime2.org/classifiers/sklearn-1.4.2/silva/silva-138-99-nb-classifier.qza",
}
BUNDLE = ["silva.arb", "silva_seqs.qza", "silva_tax.qza", "silva_nb_classifier.qza"]

def protocol(context: ExecutionContext):
    idb = context.Output(db)

    fetch = "; ".join(f"urllib.request.urlretrieve('{url}', '{idb.container}/{name}')" for name, url in FILES.items())
    context.ExecWithEnv(
        env=image,
        cmd=f"""\
            mkdir -p {idb.container}
            python3 -c "import urllib.request; {fetch}"
            gunzip {idb.container}/silva.arb.gz
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                db: idb.local,
            },
        ],
        success=all((idb.local / f).exists() for f in BUNDLE),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=3),
    )
)

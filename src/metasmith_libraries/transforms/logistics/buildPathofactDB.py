from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddRequirement(lib.GetType("ref::pathofact_sources"))
db      = model.AddProduct(lib.GetType("ref::pathofact_db"))

LAYOUT = ("HMM/TOX_CDD.hmm", "HMM/virulence_factors_CDD.hmm", "Models/VF/final_model.joblib",
          "Models/TOX/final_model.joblib", "Models/TOX/vectorizer_5.pkl", "cddid_all.tbl",
          "src/vf_prediction2.py", "src/predict.py")


# The tarball was packed on a Mac and carries AppleDouble `._*` files beside the real
# ones; hmmsearch would read `HMM/._TOX_CDD.hmm` as a corrupt model file if globbed.
def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    idb.local.mkdir(parents=True, exist_ok=True)

    context.LocalShell(f"""
        set -euo pipefail
        tar -xzf {isrc.local}/DATABASES.tar.gz -C {idb.local} --strip-components=1 --exclude='._*'
        mkdir -p {idb.local}/src
        tar -xzf {isrc.local}/pathofact2.tar.gz -C {idb.local}/src --strip-components=2 \\
            --wildcards '*/src/*'
        cp {isrc.local}/commit {idb.local}/src/COMMIT
    """)

    missing = [f for f in LAYOUT if not (idb.local/f).exists()]
    if missing:
        raise SystemExit(f"[pathofact] the unpacked databases lack {missing}")
    for hmm in LAYOUT[:2]:
        if open(idb.local/hmm, "rb").read(6) != b"HMMER3":
            raise SystemExit(f"[pathofact] {hmm} is not an HMMER3 file")
    models = {hmm: sum(1 for line in open(idb.local/hmm) if line.startswith("NAME "))
              for hmm in LAYOUT[:2]}
    Log.Info(f"PathoFact HMMs: {models}")

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=all(n > 10_000 for n in models.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

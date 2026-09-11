from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::predictf.env"))
db    = model.AddProduct(lib.GetType("annotation::predictf_db"))

PREDICTF_REPO = "https://github.com/mdsufz/PredicTF.git"

PREDICTF_MODEL_WEBDAV = "https://nc.ufz.de/public.php/webdav"
PREDICTF_MODEL_SHARE  = "e9geJ4FKJk8cWLs"
PREDICTF_MODEL_PASS   = "6oHaiWQQY9"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            set -e
            git clone --depth 1 {PREDICTF_REPO} PredicTF
            mkdir -p {idb.container}
            cp -r PredicTF/BacTFDB/. {idb.container}/

            # (1) rebuild the DIAMOND reference DB with this container's diamond
            conda run -n predictf diamond makedb \
                --in {idb.container}/database/v2/features.fasta \
                -d {idb.container}/database/v2/features

            # (2) fetch the trained LS model (metadata + weights) into model/v2/
            mkdir -p {idb.container}/model/v2
            for f in metadata_LS.pkl model_LS.pkl; do
                wget -q --header="X-Requested-With: XMLHttpRequest" \
                    --user="{PREDICTF_MODEL_SHARE}" --password="{PREDICTF_MODEL_PASS}" \
                    "{PREDICTF_MODEL_WEBDAV}/$f" -O {idb.container}/model/v2/$f
            done
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=(idb.local / "database" / "v2" / "features.dmnd").exists()
                and (idb.local / "model" / "v2" / "model_LS.pkl").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

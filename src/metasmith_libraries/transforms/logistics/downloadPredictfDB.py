from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::predictf.env"))
db    = model.AddProduct(lib.GetType("annotation::predictf_db"))

# master on 2026-10-17, last changed 2023-04-20.
PREDICTF_COMMIT = "66d80329ef6cd3b417bb0cb16a3edf364159d4bd"
PREDICTF_ARCHIVE = "https://github.com/mdsufz/PredicTF/archive/{commit}.tar.gz"

# A public Nextcloud share; WebDAV takes its token and password as basic auth.
PREDICTF_MODEL_WEBDAV = "nc.ufz.de/public.php/webdav"
PREDICTF_MODEL_SHARE  = "e9geJ4FKJk8cWLs"
PREDICTF_MODEL_PASS   = "6oHaiWQQY9"
MODEL_FILES = ["metadata_LS.pkl", "model_LS.pkl"]


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    commit = context.params.get("predictf_commit", PREDICTF_COMMIT)
    webdav = f"https://{PREDICTF_MODEL_SHARE}:{PREDICTF_MODEL_PASS}@{PREDICTF_MODEL_WEBDAV}"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(PREDICTF_ARCHIVE.format(commit=commit), "PredicTF.tar.gz",
                     label=f"PredicTF {commit}"),
        f"mkdir -p {idb.local}/model/v2",
        f"tar -xzf PredicTF.tar.gz -C {idb.local} --strip-components=2 --wildcards '*/BacTFDB/*'",
        *(FetchCommand(f"{webdav}/{f}", idb.local/"model"/"v2"/f, label=f"PredicTF {f}")
          for f in MODEL_FILES),
        "rm -f PredicTF.tar.gz",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # The tracked features.dmnd is rebuilt with this container's diamond.
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            conda run -n predictf diamond makedb \
                --in {idb.container}/database/v2/features.fasta \
                -d {idb.container}/database/v2/features
        """,
    )

    products = [idb.local/"database"/"v2"/"features.dmnd",
                *(idb.local/"model"/"v2"/f for f in MODEL_FILES)]
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=all(p.is_file() and p.stat().st_size > 0 for p in products),
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

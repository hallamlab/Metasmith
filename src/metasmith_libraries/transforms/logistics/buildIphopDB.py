from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

sources = model.AddRequirement(lib.GetType("ref::iphop_sources"))
db      = model.AddProduct(lib.GetType("ref::iphop_db"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    failed = ExecutionResult(manifest=[], success=False)

    md5_list = next(isrc.local.glob("*.tar.gz.md5"))
    lines = [line.split() for line in open(md5_list)]
    whole_md5, whole_name = next(tab for tab in lines if len(tab) == 2)
    chunks = [tab[1] for tab in lines if len(tab) == 2][1:] or [whole_name]

    # Joined into tar as a stream, so the joined archive never lands: peak disk is the
    # chunks plus the unpacked database, about twice the download, where iPHoP's own
    # downloader holds chunks, joined archive and database at once. The whole-archive
    # md5 is taken on the way through.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        "mkdir -p extract",
        "mkfifo joined.fifo",
        "tar -xzf joined.fifo -C extract & untar=$!",
        f"cat {' '.join(str(isrc.local/c) for c in chunks)} | tee joined.fifo | md5sum | cut -d' ' -f1 > joined.md5",
        "wait $untar",
        f'[ "$(cat joined.md5)" = {whole_md5} ] || {{ echo "the joined archive does not match md5 {whole_md5}" >&2; exit 1; }}',
    ])) != 0:
        return failed

    # The archive unpacks to one folder (Jun_2025_pub_rw, Test_db_rw, ...), and that
    # folder is what predict and add_to_db take as --db_dir.
    found = [d for d in Path("extract").iterdir() if (d/"db").is_dir() and (d/"db_infos").is_dir()]
    if len(found) != 1:
        Log.Error(f"expected one folder holding db/ and db_infos/ under extract/, found {[p.name for p in Path('extract').iterdir()]}")
        return failed
    found[0].rename(idb.local)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=(idb.local/"db"/"wish_data").is_dir(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    labels=["noscratch"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=12),
    ),
)

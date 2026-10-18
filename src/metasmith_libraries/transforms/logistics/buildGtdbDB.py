from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
sources = model.AddRequirement(lib.GetType("ref::gtdb_sources"))
out     = model.AddProduct(lib.GetType("ref::gtdb"))

RELEASE = "r232"


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    iout = context.Output(out)

    manifest = (isrc.local/"MANIFEST").read_text()
    names = [line.split()[-1] for line in manifest.splitlines() if line.startswith("PART=")]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        # GTDB-Tk reads GTDBTK_DATA_PATH as the directory holding taxonomy/, skani/ and
        # the rest, so drop the tarball's release232/.
        f"cat {' '.join(str(isrc.local/n) for n in names)} | tar --strip-components=1 -xzf - -C {iout.local}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)
    (iout.local/"MANIFEST").write_text(manifest)

    metadata = iout.local/"metadata"/"metadata.txt"
    whole = all((iout.local/p).exists() for p in (
        "taxonomy/gtdb_taxonomy.tsv", "skani/database/sketches.db",
        "skani/database/index.db", "skani/database/markers.bin",
    )) and metadata.exists() and f"VERSION_DATA={RELEASE}" in metadata.read_text()
    return ExecutionResult(manifest=[{out: iout.local}], success=whole)


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

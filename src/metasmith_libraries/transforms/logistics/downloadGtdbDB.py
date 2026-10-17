from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb"))

# Pinned to r232 on purpose, even after a newer release ships: it is the release the
# Cyanoverse AB48 classifications were made against (GTDB-Tk 2.7.2), and the version
# in gtdbtk.env must read it. A release is a pair with its GTDB-Tk version.
RELEASE = "r232"
MIN_GTDBTK = "2.7.0"
BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
PARTS = "auxillary_files/gtdbtk_package/split_package/gtdbtk_r232_data.tar.gz.part_"

# Only the GTDB-Tk package, about 61 GB packed and 98 GB unpacked. From 2.7 the ANI
# screen and the post-placement ANI both run `skani search` against the shipped
# skani/database/sketches.db, so the 192 GB representative genome set is not read.


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    # A run param, so a mirror that keeps the release's own layout can stand in.
    base = context.params.get("gtdb_release_url", BASE).rstrip("/")

    failed = ExecutionResult(manifest=[], success=False)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/MD5SUM.txt", "MD5SUM.txt", label="GTDB MD5SUM.txt"),
    ])) != 0:
        return failed
    parts = []
    for line in open("MD5SUM.txt"):
        toks = line.split()
        if len(toks) == 2 and PARTS in toks[1]:
            parts.append((toks[1].removeprefix("./"), toks[0]))
    parts.sort()
    assert parts, f"{base}/MD5SUM.txt lists no {PARTS}* files"

    names = [path.rsplit("/", 1)[1] for path, _ in parts]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{path}", name, md5=md5, label=name)
          for (path, md5), name in zip(parts, names)),
        f"mkdir -p {iout.local}",
        # GTDB-Tk reads GTDBTK_DATA_PATH as the directory holding taxonomy/, skani/ and
        # the rest, so drop the tarball's release232/.
        f"cat {' '.join(names)} | tar --strip-components=1 -xzf - -C {iout.local}",
        f"rm -f {' '.join(names)}",
    ])) != 0:
        return failed

    with open(iout.local/"MANIFEST", "w") as f:
        f.write(f"RELEASE={RELEASE}\nMIN_GTDBTK={MIN_GTDBTK}\nSOURCE={base}\n")
        for (path, md5) in parts:
            f.write(f"PART={md5}  {path}\n")

    metadata = iout.local/"metadata"/"metadata.txt"
    whole = all((iout.local/p).exists() for p in (
        "taxonomy/gtdb_taxonomy.tsv", "skani/database/sketches.db",
        "skani/database/index.db", "skani/database/markers.bin",
    )) and metadata.exists() and f"VERSION_DATA={RELEASE}" in metadata.read_text()
    return ExecutionResult(manifest=[{out: iout.local}], success=whole)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=24),
    ),
)

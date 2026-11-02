from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
out   = model.AddProduct(lib.GetType("ref::gtdb_sources"))

# Pinned to r232 on purpose, even after a newer release ships: it is the release the
# Cyanoverse AB48 classifications were made against (GTDB-Tk 2.7.2), and the version
# in gtdbtk.env must read it. A release is a pair with its GTDB-Tk version.
RELEASE = "r232"
MIN_GTDBTK = "2.7.0"
BASE = "https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0"
PARTS = "auxillary_files/gtdbtk_package/split_package/gtdbtk_r232_data.tar.gz.part_"

# Only the GTDB-Tk package, about 61 GB packed and 98 GB unpacked by buildGtdbDB. From 2.7 the ANI
# screen and the post-placement ANI both run `skani search` against the shipped
# skani/database/sketches.db, so the 192 GB representative genome set is not read.


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    staged = context.StagingDir()
    # A run param, so a mirror that keeps the release's own layout can stand in.
    base = context.params.get("gtdb_release_url", BASE).rstrip("/")

    failed = ExecutionResult(manifest=[], success=False)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/MD5SUM.txt", staged.local/"MD5SUM.txt", label="GTDB MD5SUM.txt"),
    ])) != 0:
        return failed
    parts = []
    for line in open(staged.local/"MD5SUM.txt"):
        toks = line.split()
        if len(toks) == 2 and PARTS in toks[1]:
            parts.append((toks[1].removeprefix("./"), toks[0]))
    parts.sort()
    assert parts, f"{base}/MD5SUM.txt lists no {PARTS}* files"

    # Fetch only. buildGtdbDB joins and unpacks the parts on a compute node.
    names = [path.rsplit("/", 1)[1] for path, _ in parts]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{path}", staged.local/name, md5=md5, label=name)
          for (path, md5), name in zip(parts, names)),
        f"mkdir -p {iout.local}",
        *(f"mv {staged.local/name} {iout.local}/" for name in names),
    ])) != 0:
        return failed

    with open(iout.local/"MANIFEST", "w") as f:
        f.write(f"RELEASE={RELEASE}\nMIN_GTDBTK={MIN_GTDBTK}\nSOURCE={base}\n")
        for (path, md5), name in zip(parts, names):
            f.write(f"PART={md5}  {name}\n")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local/name).is_file() for name in names),
    )


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

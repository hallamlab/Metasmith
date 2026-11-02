# E4 SMETANA, stage 1 of 3: cut metaGEM's media table into one table per medium, scoped to one sample.
#
# The E5 lane's smetana_split_media.py carries the full reasoning; this is the same split rooted at
# metaGEM's community unit (one sample of published MAGs) instead of an assembly, and it writes its own
# medium type so the two splits are never two producers of one type.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

media  = model.AddRequirement(lib.GetType("bench::smetana_media_db"))
sample = model.AddRequirement(lib.GetType("bench::gem_community"))
out    = model.AddProduct(lib.GetType("bench::smetana_cplex_medium"))

MEDIA = ["M1", "M2", "M3", "M4", "M5", "M7", "M8", "M9", "M10", "M11",
         "M13", "M14", "M15A", "M15B", "M16"]


def protocol(context: ExecutionContext):
    imedia = context.Input(media)
    lines = Path(imedia.local).read_text().splitlines()
    assert lines, "the media db is empty"
    header, rows = lines[0], lines[1:]
    assert header.split("\t")[0] == "medium", (
        f"media db header is {header!r}; its first column must be `medium`, which is what the split keys on")

    by_medium = {}
    for line in rows:
        if line.strip():
            by_medium.setdefault(line.split("\t", 1)[0], []).append(line)

    missing = [m for m in MEDIA if m not in by_medium]
    assert not missing, f"media db has no rows for {missing}; it holds {sorted(by_medium)}"

    written = []
    for i, m in enumerate(MEDIA):
        p = Path(context.Output(out, i=i).local)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(header + "\n" + "\n".join(by_medium[m]) + "\n")
        written.append(p)
        Log.Info(f"medium {m}: {len(by_medium[m])} compounds -> {p.name}")

    return ExecutionResult(
        manifest=[{out: p} for p in written],
        success=len(written) == len(MEDIA) and all(p.exists() for p in written),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sample,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)

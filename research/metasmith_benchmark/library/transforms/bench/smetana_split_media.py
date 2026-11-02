# SMETANA, stage 1 of 3: cut metaGEM's media table into one table per medium, scoped to one assembly.
#
# WHY THE ASSEMBLY IS A REQUIREMENT IT NEVER READS: requiring it and grouping on it puts the assembly
# in every medium's LINEAGE, which is what lets a per-medium task pair with that assembly's own
# community and lets the merge group back on the assembly. Splitting the media once, globally, would
# force a fanned-out medium to be paired with a per-assembly community across two INDEPENDENT
# fan-outs, which is the B23 tie-break hazard: the planner is then free to answer the pairing wrong.
#
# MEASURED on a live 12-model cami community (job 60106630_0, 16 Sep, 5h26m in): per-medium wall
# tracks the medium's COMPOUND COUNT almost monotonically -- M15A/M15B (19 compounds) and M13 (24)
# finished in 10-25 min, M2 (58) took ~2 h, and M3/M4/M8/M10 (74 each) had not finished at 5h26m.
# All fifteen shared ONE 20 h wall, so the eleven finished tables were hostage to the four slowest:
# missing that wall discards every medium and recomputes from zero. That is what this split fixes --
# a fast medium banks in minutes and only a straggler retries.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

media = model.AddRequirement(lib.GetType("bench::smetana_media_db"))
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
out   = model.AddProduct(lib.GetType("bench::smetana_medium"))

# metaGEM's own fifteen, in its config.yaml order. The table also carries "MILK " (with a trailing
# space) which metaGEM does not score, so the media are named explicitly rather than discovered.
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
    group_by=asm,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)

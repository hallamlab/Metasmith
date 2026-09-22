# E4 SMETANA, stage 3 of 3: stack one sample's per-medium tables into the detailed table.
#
# The product type is unchanged from the monolith this replaces, so the E4 driver's target binds as before
# and no driver edit is needed. The monolith is deleted rather than masked, so this is the only producer of
# bench::smetana_detailed_cplex.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

sample = model.AddRequirement(lib.GetType("bench::gem_community"))
medium = model.AddRequirement(lib.GetType("bench::smetana_cplex_medium"), parents={sample})
tables = model.AddRequirement(lib.GetType("bench::smetana_cplex_medium_detailed"), parents={medium})
out    = model.AddProduct(lib.GetType("bench::smetana_detailed_cplex"))

N_MEDIA = 15


def protocol(context: ExecutionContext):
    ounit = context.Output(out)

    collected = []
    for handle in context.InputGroup(tables):
        lines = Path(handle.local).read_text().splitlines(keepends=True)
        assert lines, f"{handle.local.name} is empty; every per-medium task writes at least a header"
        body = [l for l in lines[1:] if l.strip()]
        # `medium` is column 2 of a detailed row. A header-only table is a legitimate zero-score result,
        # so it orders by its file name rather than by a row it does not have.
        key = body[0].split("\t")[1] if body else handle.local.name
        collected.append((key, lines[0], body))

    # Grouped slots arrive in arbitrary order, so the stack is ordered explicitly.
    collected.sort(key=lambda t: t[0])

    headers = {h.rstrip("\n") for _, h, _ in collected}
    assert len(headers) <= 1, f"per-medium tables disagree on their header: {sorted(headers)}"

    # A missing medium is the retry-then-ignore failure: the lane reports complete while its product is
    # short. Failing loudly keeps a 14-medium table from being read as a 15-medium one.
    assert len(collected) == N_MEDIA, (
        f"{len(collected)} per-medium tables, expected {N_MEDIA}: "
        f"{sorted(k for k, _, _ in collected)}. A medium whose task exhausted its retries was ignored, "
        "so this sample's table would silently be short a scenario.")

    with open(ounit.local, "w") as fh:
        fh.write(collected[0][1])
        for _, _, body in collected:
            fh.writelines(body)

    n_rows = sum(len(b) for _, _, b in collected)
    empty = [k for k, _, b in collected if not b]
    Log.Info(f"merged {len(collected)} media -> {n_rows} rows"
             + (f"; zero-score media: {sorted(empty)}" if empty else ""))
    return ExecutionResult(manifest=[{out: ounit.local}], success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sample,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)

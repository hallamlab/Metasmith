import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

asm   = model.AddRequirement(lib.GetType("sequences::assembly"))

# DAS Tool has already chosen one bin per genome from the three binners, so the quality
# filter is CheckM2 on its bins alone and no within-sample dereplication follows.
bins   = model.AddRequirement(lib.GetType("sequences::das_tool_bin_fasta"), parents={asm})
checks = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={bins})

out    = model.AddProduct(lib.GetType("binning_local::quality_bin_fasta"))

MIN_COMPLETENESS  = 50.0
MAX_CONTAMINATION = 10.0


def _parse_checkm(path):
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        line = f.readline().rstrip("\n")
        if not line:
            return None
        cols = line.split("\t")
        row = dict(zip(header, cols))
    try:
        bin_id = cols[0] if not header else row.get("Bin Id", cols[0])
        return bin_id, float(row["Completeness"]), float(row["Contamination"])
    except (KeyError, ValueError, IndexError):
        return None


def protocol(context: ExecutionContext):
    metrics = {}
    for ck_path in context.InputGroup(checks):
        parsed = _parse_checkm(ck_path.local)
        if parsed is None:
            Log.Warn(f"unparseable checkm at [{ck_path.local}]; dropping")
            continue
        bin_id, comp, cont = parsed
        metrics[bin_id] = (comp, cont)
    kept = []
    for p in context.InputGroup(bins):
        if p.local.stem not in metrics:
            Log.Warn(f"missing checkm for bin [{p.local.stem}]; dropping")
            continue
        comp, cont = metrics[p.local.stem]
        if comp >= MIN_COMPLETENESS and cont <= MAX_CONTAMINATION:
            kept.append((p.local.stem, p))

    Log.Info(f"kept [{len(kept)}] quality MAGs")

    manifest = []
    for k, (stem, src) in enumerate(kept):
        iout = context.Output(out, i=k)
        shutil.copy(src.local, iout.local, follow_symlinks=True)
        manifest.append({out: iout.local})

    return ExecutionResult(
        manifest=manifest,
        success=len(kept) > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)

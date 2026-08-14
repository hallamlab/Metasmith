"""Pool quality MAGs across the three binners.

Inputs: per-binner bin_fasta sets + their checkm_stats.
Filter each binner's bins by completeness/contamination thresholds, union the
survivors, emit each as a `binning_local::quality_bin_fasta` product.

Output type is standalone (does NOT extend bin_fasta) so gtdbtk/checkm cannot
re-run on the pooled set; per-binner checkm/gtdbtk happen upstream.
"""
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

asm   = model.AddRequirement(lib.GetType("sequences::assembly"))

mb_bin = model.AddRequirement(lib.GetType("sequences::metabat2_bin_fasta"), parents={asm})
mb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={mb_bin})

sb_bin = model.AddRequirement(lib.GetType("sequences::semibin2_bin_fasta"), parents={asm})
sb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={sb_bin})

cb_bin = model.AddRequirement(lib.GetType("sequences::comebin_bin_fasta"), parents={asm})
cb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={cb_bin})

out    = model.AddProduct(lib.GetType("binning_local::quality_bin_fasta"))

MIN_COMPLETENESS  = 50.0
MAX_CONTAMINATION = 10.0


def _parse_checkm(path):
    """Return (bin_id, completeness, contamination) or None.

    Bin Id is column 0 of the first data row. We need it because nextflow's
    output naming gives the checkm file a different stem than the bin file
    (each metasmith data instance gets its own hash); the in-file Bin Id is
    the only stable join key.
    """
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
    kept = []  # list of (stem, source bin path)

    for bin_dep, ck_dep, label in [
        (mb_bin, mb_ck, "metabat2"),
        (sb_bin, sb_ck, "semibin2"),
        (cb_bin, cb_ck, "comebin"),
    ]:
        bins = {p.local.stem: p for p in context.InputGroup(bin_dep)}
        # Build {bin_id -> (completeness, contamination)} by reading the checkm
        # file's first data row (Bin Id column) rather than matching by file
        # stem — see _parse_checkm.
        checks_by_bin_id: dict[str, tuple[float, float]] = {}
        for ck_path in context.InputGroup(ck_dep):
            parsed = _parse_checkm(ck_path.local)
            if parsed is None:
                Log.Warn(f"[{label}] unparseable checkm at [{ck_path.local}]; dropping")
                continue
            bin_id, comp, cont = parsed
            checks_by_bin_id[bin_id] = (comp, cont)
        for stem, bp in bins.items():
            metrics = checks_by_bin_id.get(stem)
            if metrics is None:
                Log.Warn(f"[{label}] missing checkm for bin [{stem}]; dropping")
                continue
            comp, cont = metrics
            if comp >= MIN_COMPLETENESS and cont <= MAX_CONTAMINATION:
                kept.append((f"{label}__{stem}", bp))

    Log.Info(f"aggregator kept [{len(kept)}] quality MAGs")

    manifest = []
    for k, (stem, src) in enumerate(kept):
        iout = context.Output(out, i=k)
        # Keep iout.local verbatim — metasmith publishes outputs by glob and
        # any custom rename breaks the match. Binner attribution survives via
        # skani_dedup's cluster_table (bin_id column carries through).
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

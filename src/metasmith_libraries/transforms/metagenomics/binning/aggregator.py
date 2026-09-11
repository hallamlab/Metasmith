import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

asm   = model.AddRequirement(lib.GetType("sequences::assembly"))

# One `checkm_stats` and one `gtdbtk` per bin set, each parented to that set. The
# parent is what forks the fan-out: an unparented requirement is answered once,
# from whichever binner the planner likes, and the other two binners' bins reach
# no instance at all. The pool this transform emits is what iphop_add_to_db and
# cctyper consume downstream, so taxonomy has to cover every binner that can
# contribute to it -- with gtdbtk named only by a downstream target it ran on
# SemiBin2's bins alone, and metabat2's and comebin's kept bins joined to nothing.
# The protocol reads checkm and not gtdbtk: quality is what selects a bin, while
# the gtdbtk slots exist solely to shape the plan, the way `getNcbiAssembly`
# requires a name it never opens.
mb_bin = model.AddRequirement(lib.GetType("sequences::metabat2_bin_fasta"), parents={asm})
mb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={mb_bin})
mb_tax = model.AddRequirement(lib.GetType("taxonomy::gtdbtk"), parents={mb_bin})

sb_bin = model.AddRequirement(lib.GetType("sequences::semibin2_bin_fasta"), parents={asm})
sb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={sb_bin})
sb_tax = model.AddRequirement(lib.GetType("taxonomy::gtdbtk"), parents={sb_bin})

cb_bin = model.AddRequirement(lib.GetType("sequences::comebin_bin_fasta"), parents={asm})
cb_ck  = model.AddRequirement(lib.GetType("taxonomy::checkm_stats"), parents={cb_bin})
cb_tax = model.AddRequirement(lib.GetType("taxonomy::gtdbtk"), parents={cb_bin})

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
    kept = []

    for bin_dep, ck_dep, label in [
        (mb_bin, mb_ck, "metabat2"),
        (sb_bin, sb_ck, "semibin2"),
        (cb_bin, cb_ck, "comebin"),
    ]:
        bins = {p.local.stem: p for p in context.InputGroup(bin_dep)}
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

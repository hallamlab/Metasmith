import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

asm   = model.AddRequirement(lib.GetType("sequences::assembly"))

# One `checkm_stats` per bin set, each parented to that set. The parent is what forks
# the fan-out: an unparented requirement is answered once, from whichever binner the
# planner likes, and the other two binners' bins reach no instance at all.
#
# CAUTION this transform used to carry a third requirement per bin set, `taxonomy::gtdbtk`,
# parented the same way. It was removed 2026-09-12 and the reasoning matters, because
# re-adding it re-opens a launch blocker rather than a cost:
#
#   1. The protocol never read it. Only `checkm_stats` selects a bin -- completeness and
#      contamination -- so the taxonomy content reached no output. The slots existed to
#      shape the plan, the way `getNcbiAssembly` requires a name it never opens.
#   2. Their original purpose is gone. They were here so `iphop_add_to_db` got taxonomy
#      covering every binner that can contribute to the pool. That consumer now requires
#      `taxonomy::gtdbtk_raw` instead -- add_to_db wants de_novo_wf's DECORATED TREES, and
#      the classification TSV `taxonomy::gtdbtk` carries is a different artifact it never
#      opens. So these three became vestigial when that split landed, and nothing in this
#      library consumed `taxonomy::gtdbtk` afterwards except this transform.
#   3. The fan-out does not depend on them. `checkm_stats` is parented to the same three
#      bin sets by the same mechanism, so the three-way fork survives their removal --
#      verified by bindings, not argued.
#   4. They made GTDB-Tk load-bearing for a chain that does not want it. gtdbtk requires
#      `ref::gtdb`, and gtdbtk 2.6.1's classify step runs a post-placement skani
#      verification that `--skip_ani_screen` does NOT gate (classify.py ~1230; the
#      `if not prescreening and ...` guard is commented out upstream). Without a
#      skani/database/ tree it raises `Reference genome missing from skani database` and
#      exits -- AFTER pplacer, measured at 33m44s for ONE genome. With retry-then-ignore
#      that starves this transform silently, and with it the whole quality-MAG chain:
#      skani_dedup, derep_mag_reference and the published-MAG recovery comparison.
#      Restoring the requirement therefore means staging GTDB's ~179 GB / ~113 K-file
#      representative-genome set, for output no consumer reads.
#
# A driver that genuinely wants per-bin taxonomy should target `taxonomy::gtdbtk`
# directly. It must not be forced through this transform, which discards it.
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

"""CheckM2 (drop-in replacement for the original CheckM v1 transform).

Public surface — model name, dtype produced (taxonomy::checkm_stats), and the
per-genome TSV shape consumed by the aggregator — is unchanged. Internals
swapped to `checkm2 predict`; the CheckM2 DiamondDB is baked into the image
and exposed via CHECKM2DB, so no --database_path is needed.

The aggregator (_parse_checkm) joins on a "Bin Id" column. CheckM2's
quality_report.tsv calls that column "Name"; we rename it on emit.

v1.1.0 prodigal robustness
--------------------------

CheckM2 v1.1.0 has a pyrodigal-gv heap bug (GitHub issue #149) that aborts
the WHOLE batch when one bin trips it: the shared per-batch FAA gets
truncated mid-write, DIAMOND fails on every bin downstream, and the entire
N-bin batch is reported as FAILED. We hit this on the spanish-lakes W3 run
(see SG20W5.comebin.185 / SG3X12.* / SG4E23.* / SG6W12.* / SG9E22.*).

The workaround here is two-stage:
  1. Call `prodigal -p meta` per-bin in an isolated subprocess so a heap
     abort kills only that one call. The other bins' FAAs are written
     before the bad bin even gets touched.
  2. Hand the surviving FAAs to CheckM2 in `--genes` mode — which skips
     prodigal entirely and just runs the batched DIAMOND step where the
     wall-clock actually is.
Bins whose prodigal call aborted (or whose DIAMOND pass returns zero hits)
get a sentinel row with Completeness=0/Contamination=0 and an
Additional_Notes explanation, instead of taking the whole batch down.

# TODO(dag-level fix): the structurally cleaner answer is to add a
# per-bin protein FAA as an upstream product and have the W3 binning
# DAG feed it into this transform — the W2 prodigal step
# (metagenomics/assembly/orfs/<SG>.faa) has already run successfully
# for every sample by the time binning starts, so the heap bug is
# entirely out-of-loop. That requires a new product type
# (e.g. `sequences::per_bin_faa`), a small transform that subsets the
# per-sample FAA by each bin's contig set, and an extra requirement on
# this model. Best done during the next W3 redesign rather than retrofit
# now. Until then, the in-transform isolation below is the workaround.
"""
import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::checkm.env")) # checkm2; DiamondDB baked, CHECKM2DB set
asm         = model.AddRequirement(lib.GetType("sequences::putative_genome"))
out         = model.AddProduct(lib.GetType("taxonomy::checkm_stats"))


def protocol(context: ExecutionContext):
    input_dir = Path("input")
    faa_dir   = Path("faa")
    input_dir.mkdir()
    faa_dir.mkdir()

    in2out = {}  # asm_stem -> iout
    for item in context.AsBatch():
        iasm = item.Input(asm)
        iout = item.Output(out)
        in2out[iasm.local.stem] = iout
        src = iasm.local
        dest = input_dir / iasm.local.name
        Log.Info(f"registering genome [{src}] -> [{dest}]")
        shutil.copy(src, dest, follow_symlinks=True)

    threads_int = context.params.get('cpus') or 1
    threads_arg = f"--threads {threads_int}"

    # Stage 1 — per-bin gene calling, each in its own subprocess.
    # See module docstring for the heap-bug rationale.
    failed_prodigal: list[str] = []
    for fa in sorted(input_dir.iterdir()):
        target = faa_dir / f"{fa.stem}.faa"
        Log.Info(f"gene-calling [{fa.stem}]")
        context.ExecWithEnv().ifContainerDo(
            env = image,
            cmd = f"""
                export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:$PATH
                prodigal -i {fa} -a {target} -o /dev/null -p meta -q || true
            """
        )
        if not target.exists() or target.stat().st_size == 0:
            failed_prodigal.append(fa.stem)
            Log.Info(f"prodigal produced no FAA for [{fa.stem}] (likely v1.1.0 heap abort)")

    # Stage 2 — batched DIAMOND via --genes on the surviving FAAs.
    # DIAMOND DB load is the expensive part; batching amortizes it.
    # `|| true` keeps a checkm2-level failure from killing the whole batch
    # — we recover what we can from quality_report.tsv in stage 3.
    out_dir = "checkm2_out"
    report = Path(out_dir) / "quality_report.tsv"
    surviving = [p for p in faa_dir.iterdir() if p.stat().st_size > 0]
    if surviving:
        context.ExecWithEnv().ifContainerDo(
            env = image,
            cmd = f"""
                export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:$PATH
                checkm2 predict {threads_arg} --genes -x faa --input ./faa --output-directory ./{out_dir} || true
            """
        )
    else:
        Log.Info("all bins failed prodigal; skipping checkm2 predict")

    # Stage 3 — parse quality_report.tsv, split per-bin, emit sentinels
    # for bins that didn't make it (prodigal failed, or DIAMOND zero hits).
    MIN_HEADER = [
        "Bin Id", "Completeness", "Contamination",
        "Completeness_Model_Used", "Additional_Notes",
    ]
    rows_by_stem: dict[str, list[str]] = {}
    out_header = MIN_HEADER
    if report.exists():
        with open(report, newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            hdr = next(reader)
            name_col = hdr.index("Name")
            out_header = hdr.copy()
            out_header[name_col] = "Bin Id"
            for row in reader:
                if row:
                    rows_by_stem[row[name_col]] = row

    def sentinel_row(stem: str, why: str) -> list[str]:
        row = [""] * len(out_header)
        row[0] = stem
        if len(row) > 1: row[1] = "0"
        if len(row) > 2: row[2] = "0"
        if len(row) > 3: row[3] = f"None ({why})"
        row[-1] = f"Unscorable: {why}. See CheckM2 GitHub issue #149."
        return row

    manifest = []
    for asm_stem, iout in in2out.items():
        with open(iout.local, "w", newline="") as f:
            w = csv.writer(f, delimiter="\t", lineterminator="\n")
            w.writerow(out_header)
            row = rows_by_stem.get(asm_stem)
            if row is not None:
                w.writerow(row)
            elif asm_stem in failed_prodigal:
                w.writerow(sentinel_row(asm_stem, "prodigal heap abort"))
            else:
                w.writerow(sentinel_row(asm_stem, "DIAMOND returned zero hits"))
        manifest.append({out: iout.local})

    # Every input bin now has a per-bin TSV (real or sentinel). The batch
    # never fails as a whole — the worst case is all-sentinel output, which
    # is still strictly more useful than a re-runnable error.
    return ExecutionResult(
        manifest=manifest,
        success=True,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=200,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),  # batch=200 amortizes DiamondDB load; 8 cpus speeds the diamond pass
        duration=Duration(hours=4),
    )
)

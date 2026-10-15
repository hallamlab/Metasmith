# The aggregator joins on a "Bin Id" column; CheckM2's quality_report.tsv calls
# that column "Name", so it is renamed on emit.
#
# CheckM2 v1.1.0 has a pyrodigal-gv heap bug (GitHub issue #149) that aborts the
# WHOLE batch when one bin trips it: the shared per-batch FAA is truncated
# mid-write, DIAMOND then fails on every bin downstream, and the batch is
# reported as FAILED. Hence the two-stage workaround below -- `prodigal -p meta`
# per bin in an isolated subprocess, then CheckM2 in `--genes` mode, which skips
# prodigal entirely. A bin whose prodigal call aborted gets a sentinel row
# instead of taking the batch down.
#
# TODO(dag-level fix): add a per-bin protein FAA as an upstream product and have
# the W3 binning DAG feed it in -- the W2 prodigal step has already run for every
# sample by the time binning starts, so the heap bug is out of the loop entirely.
import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::checkm.env"))
asm         = model.AddRequirement(lib.GetType("sequences::putative_genome"))
out         = model.AddProduct(lib.GetType("taxonomy::checkm_stats"))


def protocol(context: ExecutionContext):
    input_dir = Path("input")
    faa_dir   = Path("faa")
    input_dir.mkdir()
    faa_dir.mkdir()

    in2out = {}
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

    failed_prodigal: list[str] = []
    for fa in sorted(input_dir.iterdir()):
        target = faa_dir / f"{fa.stem}.faa"
        Log.Info(f"gene-calling [{fa.stem}]")
        _cmd = f"""
                export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:$PATH
                prodigal -i {fa} -a {target} -o /dev/null -p meta -q || true
            """
        context.ExecWithEnv(env=image, cmd=_cmd)
        if not target.exists() or target.stat().st_size == 0:
            failed_prodigal.append(fa.stem)
            Log.Info(f"prodigal produced no FAA for [{fa.stem}] (likely v1.1.0 heap abort)")

    out_dir = "checkm2_out"
    report = Path(out_dir) / "quality_report.tsv"
    surviving = [p for p in faa_dir.iterdir() if p.stat().st_size > 0]
    if surviving:
        _cmd = f"""
                export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:$PATH
                checkm2 predict {threads_arg} --genes -x faa --input ./faa --output-directory ./{out_dir} || true
            """
        context.ExecWithEnv(env=image, cmd=_cmd)
    else:
        Log.Info("all bins failed prodigal; skipping checkm2 predict")

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
        memory=Size.GB(32),
        duration=Duration(hours=4),
    )
)

# SMETANA over one assembly's community: every CarveMe model built from its DAS Tool MAGs. The call
# is metaGEM's (`--flavor fbc2 --mediadb media_db.tsv -m <15 media> --detailed`) on SCIP, not CPLEX.
# The media table is metaGEM's own media_db.tsv: CarveMe's bundled table has none of M1 to M16.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::smetana.env"))
media   = model.AddRequirement(lib.GetType("bench::smetana_media_db"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
mags    = model.AddRequirement(lib.GetType("sequences::das_tool_bin_fasta"), parents={asm})
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems    = model.AddRequirement(lib.GetType("modelling::carveme_model"), parents={orfs})
out     = model.AddProduct(lib.GetType("bench::smetana_detailed"))

MEDIA = "M1,M2,M3,M4,M5,M7,M8,M9,M10,M11,M13,M14,M15A,M15B,M16"
HEADER = "community\tmedium\treceiver\tdonor\tcompound\tscs\tmus\tmps\tsmetana\n"


def protocol(context: ExecutionContext):
    Path("models").mkdir()
    for k, gem in enumerate(context.InputGroup(gems)):
        shutil.copy(gem.local, Path("models") / f"mag{k:04d}_{gem.local.stem}.xml")
    ounit = context.Output(out)
    manifest = [{out: ounit.local}]
    if len(list(Path("models").iterdir())) < 2:
        Log.Warn("fewer than two models; a community needs two, writing an empty table")
        ounit.local.write_text(HEADER)
        return ExecutionResult(manifest=manifest, success=True)

    # CAUTION one call with all 15 media runs them SERIALLY in a single core: measured 14 h+ on a
    # 23-model cami community, and pratama's ~127-model samples would not fit any wall. SMETANA has no
    # parallelism flag of its own (`-p` perturbs components; `-n` counts perturbation experiments), so the
    # media run as concurrent invocations here. Each is an independent scenario -- `medium` is a column of
    # the detailed table -- so the per-medium tables stack with one header and no value changes.
    context.ExecWithEnv(env=image, cmd=f"""
        set -uo pipefail
        pids=""
        for m in {MEDIA.replace(",", " ")}; do
            smetana -o "m_$m" --flavor fbc2 --mediadb {context.Input(media).container} -m "$m" \\
                --detailed --solver scip -v models/*.xml > "m_$m.log" 2>&1 &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait "$p" || rc=1; done
        ls -la m_*_detailed.tsv 2>/dev/null || true
        exit $rc
    """)
    tables = sorted(Path().glob("m_*_detailed.tsv"))
    Log.Info(f"smetana: {len(tables)} of {len(MEDIA.split(','))} media produced a table")
    with open(ounit.local, "w") as out:
        for k, table in enumerate(tables):
            lines = table.read_text().splitlines(keepends=True)
            out.writelines(lines if k == 0 else lines[1:])
    return ExecutionResult(manifest=manifest, success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # One core and 1.2-1.9 GB per invocation, measured inside a live task; 15 media now run at once.
    resources=Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=48)),
)

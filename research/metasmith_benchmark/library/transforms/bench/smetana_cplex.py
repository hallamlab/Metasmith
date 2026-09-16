# SMETANA over one metaGEM sample's community: every CarveMe CPLEX model built from that sample's
# published MAGs. metaGEM's own call and solver (config.yaml: smetanaSolver CPLEX, smetanaMedia the 15
# below), against metaGEM's media_db.tsv. CPLEX arrives the way carveme_from_orfs_cplex.py gets it: the
# driver registers the runtime directory and PYTHONPATH points at it, so no image carries a solver licence.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::smetana.env"))
media   = model.AddRequirement(lib.GetType("bench::smetana_media_db"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
sample  = model.AddRequirement(lib.GetType("bench::gem_community"))
mags    = model.AddRequirement(lib.GetType("sequences::bin_fasta"), parents={sample})
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems    = model.AddRequirement(lib.GetType("modelling::carveme_model_cplex"), parents={orfs})
out     = model.AddProduct(lib.GetType("bench::smetana_detailed_cplex"))

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

    # The media run concurrently, as in the SCIP lane: one call with all 15 runs them serially in a single
    # core (14 h+ on a 23-model community), and SMETANA has no parallelism flag of its own. Each medium is
    # an independent scenario -- `medium` is a column of the detailed table -- so the tables stack.
    context.ExecWithEnv(env=image, cmd=f"""
        set -uo pipefail
        export PYTHONPATH={context.Input(cplex).container}:$PYTHONPATH
        pids=""
        for m in {MEDIA.replace(",", " ")}; do
            smetana -o "m_$m" --flavor fbc2 --mediadb {context.Input(media).container} -m "$m" \\
                --detailed --solver cplex -v models/*.xml > "m_$m.log" 2>&1 &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait "$p" || rc=1; done
        ls -la m_*_detailed.tsv 2>/dev/null || true
        exit $rc
    """)
    tables = sorted(Path().glob("m_*_detailed.tsv"))
    Log.Info(f"smetana_cplex: {len(tables)} of {len(MEDIA.split(','))} media produced a table")
    # CAUTION do not bind this handle to `out`: that is the module-level product, and rebinding it here
    # makes `out` a function local, so `context.Output(out)` above raises UnboundLocalError before the
    # tool ever runs.
    with open(ounit.local, "w") as fh:
        for k, table in enumerate(tables):
            lines = table.read_text().splitlines(keepends=True)
            fh.writelines(lines if k == 0 else lines[1:])
    return ExecutionResult(manifest=manifest, success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sample,
    # 15 media at once, each one core and ~2 GB; metaGEM's own config asks 12 cores for the serial form.
    resources=Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=48)),
)

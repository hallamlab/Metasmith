# E4 SMETANA, stage 2 of 3: score ONE medium against one metaGEM sample's community, on CPLEX.
#
# metaGEM's own call and solver (config.yaml: smetanaSolver CPLEX). CPLEX arrives the way
# carveme_from_orfs_cplex.py gets it -- the driver registers the runtime directory and PYTHONPATH points
# at it -- so no image carries a solver licence.
#
# Splitting by medium changes no value: `medium` is a column of the detailed table and each medium is an
# independent scenario. It is what keeps a fast medium from sharing a wall with a slow one; on the E5 lane
# that spread was measured at 10 min versus >5.4 h inside a single task.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("bench::smetana.env"))
cplex  = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
sample = model.AddRequirement(lib.GetType("bench::gem_community"))
medium = model.AddRequirement(lib.GetType("bench::smetana_cplex_medium"), parents={sample})
mags   = model.AddRequirement(lib.GetType("sequences::bin_fasta"), parents={sample})
orfs   = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems   = model.AddRequirement(lib.GetType("modelling::carveme_model_cplex"), parents={orfs})
out    = model.AddProduct(lib.GetType("bench::smetana_cplex_medium_detailed"))

HEADER = "community\tmedium\treceiver\tdonor\tcompound\tscs\tmus\tmps\tsmetana\n"


def protocol(context: ExecutionContext):
    imedium = context.Input(medium)
    rows = [l for l in Path(imedium.local).read_text().splitlines()[1:] if l.strip()]
    assert rows, "the medium table has no compound rows"
    ids = {l.split("\t", 1)[0] for l in rows}
    assert len(ids) == 1, f"medium table mixes {sorted(ids)}; the split writes exactly one medium per file"
    mid = ids.pop()

    Path("models").mkdir()
    n_models = 0
    for k, gem in enumerate(context.InputGroup(gems)):
        shutil.copy(gem.local, Path("models") / f"mag{k:04d}_{gem.local.stem}.xml")
        n_models += 1

    ounit = context.Output(out)
    manifest = [{out: ounit.local}]
    if n_models < 2:
        Log.Warn(f"{mid}: {n_models} model(s); a community needs two, writing an empty table")
        ounit.local.write_text(HEADER)
        return ExecutionResult(manifest=manifest, success=True)

    Log.Info(f"{mid}: {len(rows)} compounds over {n_models} models")
    # CAUTION `${PYTHONPATH:-}`, not `$PYTHONPATH`: `set -u` aborts on an unset variable, and PYTHONPATH
    # is not always set in the image.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        export PYTHONPATH={context.Input(cplex).container}:${{PYTHONPATH:-}}
        smetana -o out --flavor fbc2 --mediadb {imedium.container} -m {mid} \\
            --detailed --solver cplex -v models/*.xml
        test -f out_detailed.tsv
    """)
    context.LocalShell(f"cp out_detailed.tsv {ounit.local}")
    n_rows = max(0, len(ounit.local.read_text().splitlines()) - 1)
    Log.Info(f"{mid}: {n_rows} detailed rows")
    return ExecutionResult(manifest=manifest, success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=medium,
    # SMETANA has no parallelism flag of its own, and a live task measured cputime == elapsed on one core.
    # CAUTION 20 h, not 48: fir refuses ANY job over 7.0 days at submit time, so a 48 h base makes rungs 3
    # and 4 (192 h, 384 h) unsubmittable, and each ignored submission failure decrements nextflow's running
    # counter with no matching increment until the run wedges. 20/40/80/160 are all legal.
    resources=Resources(cpus=1, memory=Size.GB(16), duration=Duration(hours=20)),
)

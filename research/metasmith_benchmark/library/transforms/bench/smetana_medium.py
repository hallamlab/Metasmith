# SMETANA, stage 2 of 3: score ONE medium against one assembly's community of CarveMe models.
#
# metaGEM's call and solver (`--flavor fbc2 --mediadb <one medium> -m <that medium> --detailed`) on
# SCIP, one medium per task. Splitting by medium changes no value: `medium` is a column of the
# detailed table and each medium is an independent scenario, so the per-medium tables stack.
#
# SCIP IS NOT A CHOICE HERE. SMETANA 1.2.1 scores through ReFramed 1.6.0, whose solver registry holds
# exactly three backends -- cplex, gurobi and scip -- and this image imports only pyscipopt (no
# highspy, no glpk, no optlang). There is no HiGHS backend to install, so a faster open-source solver
# is not available at any image cost, and CPLEX is ruled out for E5 on publication grounds.
#
# CAUTION the medium id is read from the TABLE'S OWN CONTENT, never from its file name: a pool given
# carries a content-hash name, so a name-derived medium would silently score the wrong scenario.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("bench::smetana.env"))
asm    = model.AddRequirement(lib.GetType("sequences::assembly"))
medium = model.AddRequirement(lib.GetType("bench::smetana_medium"), parents={asm})
mags   = model.AddRequirement(lib.GetType("sequences::das_tool_bin_fasta"), parents={asm})
orfs   = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems   = model.AddRequirement(lib.GetType("modelling::carveme_model"), parents={orfs})
out    = model.AddProduct(lib.GetType("bench::smetana_medium_detailed"))

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
    # One medium, so the task fails on a real SMETANA failure instead of masking it behind fourteen
    # siblings. A header-only table is NOT a failure: SMETANA omits zero-score entries without `-z`,
    # and three cami media legitimately produced 61-byte tables.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        smetana -o out --flavor fbc2 --mediadb {imedium.container} -m {mid} \\
            --detailed --solver scip -v models/*.xml
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
    # SMETANA has no parallelism flag of its own (`-p` perturbs components, `-n` counts perturbation
    # experiments), and a live task measured cputime == elapsed on one core, so extra cpus buy nothing.
    # Measured RSS 1.2-2.2 GB over 12- and 29-model communities; 16 GB carries a much larger one.
    # CAUTION duration must satisfy base x 2^(tries-1) <= 168 h: fir refuses ANY job over 7.0 days at
    # submit time, and an ignored SUBMISSION failure decrements nextflow's running counter with no
    # matching increment, so the monitor never sees the queue drain and the run wedges. 20/40/80/160
    # are all legal.
    resources=Resources(cpus=1, memory=Size.GB(16), duration=Duration(hours=20)),
)

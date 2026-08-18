"""Adjudicate the reaction universe, then hand every mapper lane the same todo list.

THE FIRST STEP OF THE AAM SIDE, and the only one that looks at reactions no mapper will
ever see. Its product is one row per MNXR -- verdict, size, blockers and their families
-- which is three things at once:

  * THE LANES' TODO LIST. Each member used to derive its own universe from
    `lookup::reactions`; now they all read `verdict == mappable` from here, so "the three
    members saw the same reactions" is a fact about the graph rather than three filters
    that happen to agree today.
  * THE SIZE CUT, WHICH ROUTES RATHER THAN REFUSES. Reactions over the atom threshold
    are recorded as `oversize`, and that verdict now means "the neural members will not
    see this" -- Indigo does. The threshold bounds a 512-token transformer and the lane
    that was OOM-killed twice; Indigo is a compiled substructure search with a recorded
    timeout and neither limit applies to it. The yield curve that once justified refusing
    outright turns out to be censored -- every mapper method in the deployed table stops
    dead at 600 because the same cut was applied upstream of all three, and the only
    thing banked above it is `curated`, which never sees a mapper. See
    `ecspr.bake.aam.worklist.ATOM_LIMIT` and `research/fabfos/benchmarks/aam_cap/`.
  * THE LEDGER SPINE. Every reaction ends the build with a reason, so the tier-4 gate can
    say WHY each reaction it expected is missing. A miss with a named reason is a result;
    a miss with no reason is a bug, and before this step they looked the same.

IT DOES NOT READ MetaCyc, deliberately. The curated layer answers a quarter of the
universe and it would be natural to record that here -- but taking the licensed drop-in
as a requirement would put it upstream of every member lane, which is the coupling
`aam_ensemble` was restructured to remove. `aam_layers.stack` restricts each layer to
what nothing below it claimed anyway, so knowing here would change no row.

CHEAP, and that matters because everything downstream waits on it: one rdkit parse per
buildable reaction to count atoms, no mapping, single-threaded, minutes.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_wl      = model.AddProduct(lib.GetType("interm::aam_worklist"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    ilib = context.Input(bakelib)
    iout = context.Output(out_wl)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p wl
        {py} -m ecspr.bake.aam.worklist build \
            --reactions {irx.container} --metabolites {imt.container} \
            --out wl/worklist.parquet --out-summary wl/summary.tsv
        cp wl/worklist.parquet {iout.container}

        # The SUMMARY is the half a human reads: the verdict histogram, the blocker
        # families, and the two limits the run was made under. A threshold recorded only
        # as a constant in a module is a threshold nobody can check a result against.
        {py} -m ecspr.bake.evidence collect --root _ev --tool worklist \
            --file wl/worklist.parquet wl/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_wl: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "worklist").is_dir()
                 and any((iev.local / "worklist").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::carveme.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
# NOT registered by any download transform -- a driver `RegisterItem`s a real
# path on the cluster, the way `research/cami/run_cami_metag.py`'s `DB_PATHS`
# does, never `DEFERRED`. Reaching this requirement at all is what keeps the
# CPLEX path opt-in: nothing in this library can manufacture it, so a plan
# only lands here when a target names `modelling::carveme_model_cplex` AND a
# driver has supplied the installation. Register it at a NODE-LOCAL path, not
# the Lustre one directly -- `resources/lib/modelling/stage_cplex_installation.sh`
# is the copy-to-scratch hook (a `beforeScript`, the same shape
# `run_cami_metag.py`'s `make_slurm_config` uses for COMEBin's GPU arm); a
# driver wires it into this transform's process config.
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model_cplex"))

def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    icplex  = context.Input(cplex)
    iout    = context.Output(out)

    # PYTHONPATH, not a bind trick of our own: the engine already stages
    # `icplex` (a directory item like any `ref::` reference) into the
    # container, so putting it first on PYTHONPATH is enough for `import
    # cplex` to resolve to the full runtime rather than nothing at all. See
    # `carveme_from_orfs_cplex.py` (the resources/lib script, same name, a
    # different file) for why nothing here forces a solver.
    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            export PYTHONPATH={icplex.container}:$PYTHONPATH
            python {ihelp.container}/carveme_from_orfs_cplex.py \
                {iorfs.container} {imedia.container} {imedium.container} {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

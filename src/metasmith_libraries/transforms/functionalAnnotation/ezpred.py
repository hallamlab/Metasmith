from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::ezpred.env"))
ezmodel  = model.AddRequirement(lib.GetType("ref::ezpred_model"))
lay      = model.AddRequirement(lib.GetType("annotation::esm_c_layer_means"))
idx      = model.AddRequirement(lib.GetType("annotation::esm_c_index"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
wrapper_lib = model.AddRequirement(lib.GetType("lib::ezpred_wrapper.py"))
out_pred = model.AddProduct(lib.GetType("annotation::ezpred_predictions"))


TOP_TERMS = 500


def protocol(context: ExecutionContext):
    ilay  = context.Input(lay)
    iwrap = context.Input(wrapper_lib)
    iidx  = context.Input(idx)
    iorfs = context.Input(orfs)
    iez   = context.Input(ezmodel)
    ipred = context.Output(out_pred)
    context.LocalShell("mkdir -p ezpred_run")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"ezpred_run", "/work/ezpred_run"),
            (iez.local, "/work/EZpred"),
        ],
        cmd=f"""
            EZPRED_LAYERS={ilay.container} \
            EZPRED_INDEX={iidx.container} \
            EZPRED_ORFS={iorfs.container} \
            EZPRED_OUT_CSV={ipred.container} \
            EZPRED_TOP_TERMS={TOP_TERMS} \
            EZPRED_WORK=/work/ezpred_run \
            python {iwrap.container}
        """,
    )

    n_rows = sum(1 for _ in open(ipred.local)) - 1 if ipred.local.exists() else 0
    print(f"[ezpred] {n_rows:,} EC calls", flush=True)
    return ExecutionResult(
        manifest=[{out_pred: ipred.local}],
        success=n_rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=lay,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

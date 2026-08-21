from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs"))
kofam     = model.AddRequirement(lib.GetType("annotation::kofamscan_results"), parents={orfs})
clean     = model.AddRequirement(lib.GetType("annotation::clean_predictions"), parents={orfs})
uniref    = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results"), parents={orfs})
uniref_d  = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_descriptions"), parents={orfs})
pbert_emb = model.AddRequirement(lib.GetType("annotation::proteinbert_embeddings"), parents={orfs})
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
landmarks = model.AddRequirement(lib.GetType("ref::label_transfer_landmarks"))
ev_lib    = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib   = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out_gpr   = model.AddProduct(lib.GetType("annotation::gpr_table"))

LANE_SET = "chosen_4"

THREADS = 8

def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    ikof  = context.Input(kofam)
    icln  = context.Input(clean)
    iuni  = context.Input(uniref)
    iunid = context.Input(uniref_d)
    ipe   = context.Input(pbert_emb)
    ibr   = context.Input(bridge)
    ilm   = context.Input(landmarks)
    iev   = context.Input(ev_lib)
    igpr  = context.Input(gpr_lib)
    iout  = context.Output(out_gpr)

    cmd = f"""
            python3 {igpr.container}/gpr_4lane.py \
            --ev-lib {iev.container} \
            --orfs {iorfs.container} \
            --kofam {ikof.container} \
            --clean {icln.container} \
            --uniref {iuni.container} \
            --uniref-descriptions {iunid.container} \
            --pbert-emb {ipe.container} \
            --bridge {ibr.container} \
            --landmarks {ilm.container} \
            --out {iout.container} \
            --lane-set {LANE_SET} \
            --source {iorfs.local.stem} \
            --threads {THREADS}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_gpr: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=THREADS,
        memory=Size.GB(24),
        duration=Duration(hours=2),
    ),
)

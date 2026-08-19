from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs"))
kofam     = model.AddRequirement(lib.GetType("annotation::kofamscan_results"), parents={orfs})
clean     = model.AddRequirement(lib.GetType("annotation::clean_predictions"), parents={orfs})
deepec    = model.AddRequirement(lib.GetType("annotation::deepec_predictions"), parents={orfs})
ezpred    = model.AddRequirement(lib.GetType("annotation::ezpred_predictions"), parents={orfs})
uniref    = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results"), parents={orfs})
pbert_emb = model.AddRequirement(lib.GetType("annotation::proteinbert_embeddings"), parents={orfs})
pbert_idx = model.AddRequirement(lib.GetType("annotation::proteinbert_index"), parents={orfs})
esmc_emb  = model.AddRequirement(lib.GetType("annotation::esm_c_embeddings"), parents={orfs})
esmc_idx  = model.AddRequirement(lib.GetType("annotation::esm_c_index"), parents={orfs})
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
pool      = model.AddRequirement(lib.GetType("ref::label_transfer_landmarks"))
pool_esmc = model.AddRequirement(lib.GetType("ref::label_transfer_landmarks_esmc"))
ev_lib    = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib   = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out_gpr   = model.AddProduct(lib.GetType("annotation::gpr_table_7lane"))

LANE_SET = "full_7"


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    ikof  = context.Input(kofam)
    icln  = context.Input(clean)
    idec  = context.Input(deepec)
    iez   = context.Input(ezpred)
    iuni  = context.Input(uniref)
    ipe   = context.Input(pbert_emb)
    ipi   = context.Input(pbert_idx)
    iee   = context.Input(esmc_emb)
    iei   = context.Input(esmc_idx)
    ibr   = context.Input(bridge)
    ipool = context.Input(pool)
    ipesm = context.Input(pool_esmc)
    iev   = context.Input(ev_lib)
    igpr  = context.Input(gpr_lib)
    iout  = context.Output(out_gpr)

    cmd = f"""
            python3 {igpr.container}/gpr_7lane.py \
            --ev-lib {iev.container} \
            --orfs {iorfs.container} \
            --kofam {ikof.container} \
            --clean {icln.container} \
            --deepec {idec.container} \
            --ezpred {iez.container} \
            --uniref {iuni.container} \
            --pbert-emb {ipe.container} \
            --pbert-idx {ipi.container} \
            --esmc-emb {iee.container} \
            --esmc-idx {iei.container} \
            --bridge {ibr.container} \
            --pool {ipool.container} \
            --pool-esmc {ipesm.container} \
            --out {iout.container} \
            --lane-set {LANE_SET} \
            --source {iorfs.local.stem}
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
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)

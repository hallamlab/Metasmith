from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs"))
kofam     = model.AddRequirement(lib.GetType("annotation::kofamscan_results"), parents={orfs})
kofam_d   = model.AddRequirement(lib.GetType("annotation::kofamscan_descriptions"), parents={orfs})
clean     = model.AddRequirement(lib.GetType("annotation::clean_predictions"), parents={orfs})
uniref    = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results"), parents={orfs})
uniref_d  = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_descriptions"), parents={orfs})
pbert_emb = model.AddRequirement(lib.GetType("annotation::proteinbert_embeddings"), parents={orfs})
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
landmarks = model.AddRequirement(lib.GetType("ref::label_transfer_landmarks"))
calib     = model.AddRequirement(lib.GetType("ref::lane_calibration"))
ev_lib    = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib   = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
out_gpr   = model.AddProduct(lib.GetType("annotation::gpr_table"))

LANE_SET = "chosen_4"

THREADS = 8

# The chosen-4 subset of the annotation table, built from those four lanes alone so a
# fosmid pool reaches it from its ORFs without the assembly-scoped lanes. It runs the same
# two programs `annotation_table` and `gpr_from_annotation_table` run, so its rows are the
# chosen-4 rows of `annotation::gpr_table_all` for the same ORF set.
def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    ikof  = context.Input(kofam)
    ikofd = context.Input(kofam_d)
    icln  = context.Input(clean)
    iuni  = context.Input(uniref)
    iunid = context.Input(uniref_d)
    ipe   = context.Input(pbert_emb)
    ibr   = context.Input(bridge)
    ilm   = context.Input(landmarks)
    ical  = context.Input(calib)
    iev   = context.Input(ev_lib)
    igpr  = context.Input(gpr_lib)
    iout  = context.Output(out_gpr)

    cmd = f"""
            python3 {igpr.container}/annotation_table.py \
            --ev-lib {iev.container} \
            --orfs {iorfs.container} \
            --source {iorfs.local.stem} \
            --lane kofam {ikof.container} {ikofd.container} \
            --lane clean {icln.container} \
            --lane uniref50 {iuni.container} {iunid.container} \
            --lane pbert {ipe.container} \
            --landmarks {ilm.container} \
            --threads {THREADS} \
            --out annotation_{LANE_SET}.parquet && \
            python3 {igpr.container}/gpr_table.py \
            --ev-lib {iev.container} \
            --annotation-table annotation_{LANE_SET}.parquet \
            --orfs {iorfs.container} \
            --bridge {ibr.container} \
            --calibration {ical.container} \
            --lane-set {LANE_SET} \
            --source {iorfs.local.stem} \
            --out {iout.container}
    """
    context.ExecWithEnv(env=image, cmd=cmd)

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

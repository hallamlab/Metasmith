from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm       = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
landmarks = model.AddRequirement(lib.GetType("ref::label_transfer_landmarks"))
ev_lib    = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib   = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))

# lane -> its product(s), in the order `annotation_table.py --lane` takes them. A fixed
# set: a requirement cannot be optional, so this transform is the "every lane" table, and
# a subset is a consumer filtering on `lane` (`gpr_from_annotation_table`, `gpr_4lane`).
# PathoFact is absent: it has no merge over its contig batches and its output layout is
# not known here.
ORF_LANES = {
    "kofam":         ("annotation::kofamscan_results", "annotation::kofamscan_descriptions"),
    "clean":         ("annotation::clean_predictions",),
    "uniref50":      ("annotation::diamond_uniref50_results",
                      "annotation::diamond_uniref50_descriptions"),
    "pbert":         ("annotation::proteinbert_embeddings",),
    "deepec":        ("annotation::deepec_predictions",),
    "eggnog":        ("annotation::eggnog_results",),
    "interproscan":  ("annotation::interproscan_results", "annotation::interproscan_descriptions"),
    "mobileog":      ("annotation::mobileog_diamond_results",),
    "megares":       ("annotation::megares_diamond_results",),
    "bacmet":        ("annotation::bacmet_diamond_results",),
    "vfdb":          ("annotation::vfdb_diamond_results",),
    "bigg":          ("annotation::bigg_diamond_hits",),
    "rgi":           ("annotation::rgi_results",),
    "amrfinderplus": ("annotation::amrfinderplus_results",),
}
# Read off the assembly by tools that call their own genes, so they name contigs.
CONTIG_LANES = {
    "bakta_nc":  ("annotation::bakta_tsv",),
    "antismash": ("annotation::antismash_json",),
}
lanes = {
    **{k: [model.AddRequirement(lib.GetType(t), parents={orfs}) for t in ts]
       for k, ts in ORF_LANES.items()},
    **{k: [model.AddRequirement(lib.GetType(t), parents={asm}) for t in ts]
       for k, ts in CONTIG_LANES.items()},
}
out = model.AddProduct(lib.GetType("annotation::annotation_table"))

THREADS = 8


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout  = context.Output(out)
    lane_args = " ".join(
        "--lane " + name + " " + " ".join(str(context.Input(d).container) for d in deps)
        for name, deps in lanes.items())
    cmd = f"""
            python3 {context.Input(gpr_lib).container}/annotation_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --orfs {iorfs.container} \
            --source {iorfs.local.stem} \
            --landmarks {context.Input(landmarks).container} \
            --threads {THREADS} \
            {lane_args} \
            --out {iout.container}
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    # The assembly, not the ORFs: the contig lanes descend from it and not from the ORF
    # set, and a slot parented to anything but the key collects every candidate.
    group_by=asm,
    resources=Resources(
        cpus=THREADS,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)

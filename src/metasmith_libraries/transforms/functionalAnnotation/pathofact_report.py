from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
lane    = lambda k: model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={orfs})
arms    = [lane("pathofact_hmm"), lane("pathofact_ml")]
rgi, amrfp, deeparg = lane("rgi"), lane("amrfinderplus"), lane("deeparg")
antismash = model.AddRequirement(lib.GetType("annotation::antismash_json"), parents={asm})
# geNomad runs once per contig batch of the assembly, so each slot holds one file a batch.
GENOMAD = {
    "plasmid-summary": model.AddRequirement(lib.GetType("taxonomy::genomad_plasmid_summary"), parents={asm}),
    "plasmid-genes":   model.AddRequirement(lib.GetType("taxonomy::genomad_plasmid_genes"), parents={asm}),
    "virus-summary":   model.AddRequirement(lib.GetType("taxonomy::genomad_virus_summary"), parents={asm}),
    "virus-genes":     model.AddRequirement(lib.GetType("taxonomy::genomad_virus_genes"), parents={asm}),
}
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))

REPORTS = {
    "vf":            model.AddProduct(lib.GetType("annotation::pathofact_vf")),
    "vf-ambiguous":  model.AddProduct(lib.GetType("annotation::pathofact_vf_ambiguous")),
    "tox":           model.AddProduct(lib.GetType("annotation::pathofact_tox")),
    "tox-ambiguous": model.AddProduct(lib.GetType("annotation::pathofact_tox_ambiguous")),
    "amr":           model.AddProduct(lib.GetType("annotation::pathofact_amr")),
    "combined":      model.AddProduct(lib.GetType("annotation::pathofact_report")),
}


# PathoFact 2.0's report steps for a contig sample with geNomad on, over the library's lanes.
def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    outs  = {k: context.Output(t) for k, t in REPORTS.items()}
    files = lambda slot: " ".join(str(f.container) for f in
                                  sorted(context.InputGroup(slot), key=lambda p: str(p.local)))

    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(gpr_lib).container}/pathofact_report.py \\
            --ev-lib {context.Input(ev_lib).container} \\
            --annotation-table {" ".join(str(context.Input(t).container) for t in arms)} \\
            --orfs {iorfs.container} --sample {context.Input(asm).local.stem} \\
            --deeparg {context.Input(deeparg).container} \\
            --amrfinderplus {context.Input(amrfp).container} \\
            --rgi {context.Input(rgi).container} \\
            --antismash-json {files(antismash)} \\
            {" ".join(f"--{flag} {files(slot)}" for flag, slot in GENOMAD.items())} \\
            {" ".join(f"--out-{k} {o.container}" for k, o in outs.items())}
    """)

    return ExecutionResult(
        manifest=[{REPORTS[k]: o.local for k, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

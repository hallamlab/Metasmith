from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
metanetx = model.AddRequirement(lib.GetType("ref::metanetx_reactions"))
kegg     = model.AddRequirement(lib.GetType("ref::kegg_ko_reaction"))
rhea     = model.AddRequirement(lib.GetType("ref::rhea_uniprot"))
ev_lib   = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
builder  = model.AddRequirement(lib.GetType("lib::mnxr_lookup.py"))
bridge   = model.AddProduct(lib.GetType("ref::mnxr_lookup"))


def protocol(context: ExecutionContext):
    imnx, ikeg, irhe = (context.Input(x).container for x in (metanetx, kegg, rhea))
    iout = context.Output(bridge)
    context.ExecWithEnv(env=image, cmd=f"""
        python3 {context.Input(builder).container} \
            --ev-lib {context.Input(ev_lib).container} \
            --reac-prop {imnx}/reac_prop.tsv \
            --reac-xref {imnx}/reac_xref.tsv \
            --ko2reaction {ikeg}/ko2reaction.tsv \
            --rhea2uniprot {irhe}/rhea2uniprot.tsv \
            --rhea2uniprot-trembl {irhe}/rhea2uniprot_trembl.tsv.gz \
            --out {iout.container}
    """)
    return ExecutionResult(
        manifest=[{bridge: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=2)),
)

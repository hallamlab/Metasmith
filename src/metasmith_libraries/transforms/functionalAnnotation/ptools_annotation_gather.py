from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()

image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs   = model.AddRequirement(lib.GetType("sequences::orfs"))
# gather.DATABASES, the four searches MetaPathways 4 feeds PathoLogic.
lanes  = [model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={orfs})
          for k in ("metacyc", "uniref90", "uniref50", "swissprot")]
ec     = model.AddRequirement(lib.GetType("ref::uniprot_ec_maps"))
rxn    = model.AddRequirement(lib.GetType("ref::metacyc_monomer_reactions"))
ev_lib = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
pt_lib = model.AddRequirement(lib.GetType("lib::pathologic"))
out    = model.AddProduct(lib.GetType("annotation::ptools_annotation_table"))


# The lane tables carry every call; gather.py applies MP4's rules to them.
def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    iec    = context.Input(ec)
    irxn   = context.Input(rxn)
    iev    = context.Input(ev_lib)
    ipt    = context.Input(pt_lib)
    iout   = context.Output(out)

    context.ExecWithEnv(env=image, cmd=f"""
        PYTHONPATH=$(dirname {iev.container}) python3 {ipt.container}/gather.py \
            --orfs {iorfs.container} --annotations {" ".join(str(context.Input(t).container) for t in lanes)} \
            --ec-maps {iec.container} --metacyc-reactions {irxn.container} --out {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)

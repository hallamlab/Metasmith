from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm      = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
hits     = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results"), parents={orfs})
descs    = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_descriptions"), parents={orfs})
metabuli = model.AddRequirement(lib.GetType("taxonomy::metabuli"), parents={asm})
rrna     = model.AddRequirement(lib.GetType("taxonomy::rrna_gtdb"), parents={asm})
tax_map  = model.AddRequirement(lib.GetType("ref::ncbi_to_gtdb_map"))
mb_ref   = model.AddRequirement(lib.GetType("ref::metabuli_ref"))
lca_lib  = model.AddRequirement(lib.GetType("lib::lcastar"))
out      = model.AddProduct(lib.GetType("taxonomy::contig_lcastar"))


def protocol(context: ExecutionContext):
    ihits = context.Input(hits)
    idescs = context.Input(descs)
    imb = context.Input(metabuli)
    irrna = context.Input(rrna)
    imap = context.Input(tax_map)
    iref = context.Input(mb_ref)
    ilib = context.Input(lca_lib)
    iout = context.Output(out)

    weights = " ".join(
        f"--{k.replace('_', '-')} {context.params[k]}"
        for k in ("orf_weight", "metabuli_weight", "rrna_weight")
        if context.params.get(k) is not None)
    context.ExecWithEnv(env=image, cmd=f"""
        PYTHONPATH={ilib.container} python3 {ilib.container}/contig_taxonomy.py \
            --hits {ihits.container} --descriptions {idescs.container} --map {imap.container} \
            --metabuli {imb.container} --metabuli-ref {iref.container} --rrna {irrna.container} \
            --out {iout.container} {weights}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)

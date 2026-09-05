from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bakta.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
db      = model.AddRequirement(lib.GetType("annotation::bakta_db"))
hits    = model.AddProduct(lib.GetType("annotation::bakta_protein_hits"))
infer   = model.AddProduct(lib.GetType("annotation::bakta_protein_inference"))
hypo    = model.AddProduct(lib.GetType("annotation::bakta_hypotheticals"))
record  = model.AddProduct(lib.GetType("annotation::bakta_protein_json"))
named   = model.AddProduct(lib.GetType("annotation::bakta_annotated_proteins"))

def protocol(context: ExecutionContext):
    iorfs=context.Input(orfs)
    idb=context.Input(db)
    ihits=context.Output(hits)
    iinfer=context.Output(infer)
    ihypo=context.Output(hypo)
    irecord=context.Output(record)
    inamed=context.Output(named)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    # `bakta_proteins` is a SEPARATE entry point beside `bakta` in the pinned 1.11.0 image
    # and takes a protein FASTA, so this is the CDS arm the shipped `bakta_noncoding.py`
    # skips with --skip-cds. The two are complementary, not alternatives.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""\
            bakta_proteins --db /db/db-light {threads} \
                --force --prefix bakta_cds --output bakta_out \
                {iorfs.container}
        """,
    )

    PREFIX = "bakta_out/bakta_cds"
    for suffix, handle in [
        ("tsv", ihits),
        ("inference.tsv", iinfer),
        ("hypotheticals.tsv", ihypo),
        ("json", irecord),
        ("faa", inamed),
    ]:
        context.LocalShell(f"cp {PREFIX}.{suffix} {handle.local}")

    return ExecutionResult(
        manifest=[
            {
                hits:   ihits.local,
                infer:  iinfer.local,
                hypo:   ihypo.local,
                record: irecord.local,
                named:  inamed.local,
            },
        ],
        success=all(
            h.local.exists()
            for h in (ihits, iinfer, ihypo, irecord, inamed)
        ),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)

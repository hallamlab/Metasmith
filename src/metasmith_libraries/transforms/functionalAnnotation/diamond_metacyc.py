from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

diamond = model.AddRequirement(lib.GetType("env::diamond.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
db      = model.AddRequirement(lib.GetType("ref::metacyc_protein_diamond_db"))
bsr_lib = model.AddRequirement(lib.GetType("lib::diamond_bsr.py"))
hits    = model.AddProduct(lib.GetType("annotation::diamond_metacyc_results"))
descs   = model.AddProduct(lib.GetType("annotation::diamond_metacyc_descriptions"))

CPUS = 8


def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    idb    = context.Input(db)
    ihits  = context.Output(hits)
    idescs = context.Output(descs)

    db_dir = context.Mount(idb.external.parent, "/db")
    context.ExecWithEnv(env=diamond, binds=[db_dir], cmd=f"""
        diamond blastp --query {iorfs.container} --db {db_dir}/{idb.external.name} --out raw.tsv \
            --threads {CPUS} --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle \
            --max-target-seqs 5 --evalue 1e-5 --sensitive
    """)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(bsr_lib).container} split --raw raw.tsv --query {iorfs.container} \
            --hits {ihits.container} --descriptions {idescs.container}
    """)

    return ExecutionResult(
        manifest=[{hits: ihits.local, descs: idescs.local}],
        success=ihits.local.exists() and idescs.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=CPUS, memory=Size.GB(16), duration=Duration(hours=4)),
)

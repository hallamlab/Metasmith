from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::diamond.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db = model.AddRequirement(lib.GetType("annotation::tcdb_diamond_db"))
out_results = model.AddProduct(lib.GetType("annotation::diamond_tcdb_results_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb = context.Input(db)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)
    mem = context.params.get("memory")
    block_size = 2.0  # default
    if mem:
        mem_gb = int(float(mem))
        block_size = max(1.0, min(12.0, (mem_gb - 4) / 6))

    # Run DIAMOND blastp against TCDB
    context.ExecWithEnv().ifContainerDo(
        binds=[(idb.external.parent, "/db")],
        env=image,
        cmd=f"""
            diamond blastp \
                --query {iorfs.container} \
                --db /db/{idb.external.name} \
                --out {iout.container} \
                --threads {threads} \
                --block-size {block_size} \
                --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle \
                --max-target-seqs 5 \
                --evalue 1e-5 \
                --sensitive
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(minutes=30),
    ),
)

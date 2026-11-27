from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::diamond.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db = model.AddRequirement(lib.GetType("ref::refseq_viral_diamond_db"))
out_results = model.AddProduct(lib.GetType("annotation::refseq_viral_diamond_results_chunk"))

# DRAM-v searches the RefSeq viral proteins with mmseqs and keeps each ORF's top hit at
# bitscore >= 60. DIAMOND here, with stitle, which is the header DRAM-v reports as
# `viral_hit`; the top-hit cut is the consumer's.


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb = context.Input(db)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)
    mem = context.params.get("memory")
    block_size = 2.0
    if mem:
        mem_gb = int(float(mem))
        block_size = max(1.0, min(12.0, (mem_gb - 4) / 6))

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            diamond blastp \
                --query {iorfs.container} \
                --db {idb.container} \
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
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

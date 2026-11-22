from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::hmmer.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db    = model.AddRequirement(lib.GetType("ref::vogdb_hmm"))
out   = model.AddProduct(lib.GetType("annotation::vogdb_hmmsearch_results_chunk"))

# hmmsearch's E-value scales with the number of sequences searched, so a chunk's E-values
# would depend on how the ORFs were chunked. A fixed -Z makes them a property of the ORF:
# the ORF chunk size, and about the gene count of one genome, which is what DRAM's
# per-genome search sees. -E 1e-5 sits far above DRAM's own cut-offs (1e-18 dbCAN,
# 1e-15 VOGDB), so the cut belongs to the consumer.
SEARCH_Z = 5000


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb   = context.Input(db)
    iout  = context.Output(out)
    threads = context.params.get("cpus", 4)
    z = context.params.get("hmmsearch_z", SEARCH_Z)

    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        sed '/^[^>]/s/\\*//g' {iorfs.container} > query.faa
        hmmsearch --cpu {threads} -Z {z} -E 1e-5 --noali -o /dev/null \\
            --domtblout {iout.container} {idb.container}/vog.hmm query.faa
    """)

    # hmmsearch writes its comment trailer even with no hits, so an empty file is a crash.
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)

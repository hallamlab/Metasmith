"""amrfinderplus — NCBI AMRFinderPlus protein search (second-layer ARG validation).

Runs on a per-sample protein CHUNK (sequences::orf_chunk, sample-prefixed by
w4_rebatch.py) so AMRFinderPlus, like every other AMR tool, consumes batched ORFs
and never a whole-sample file. The "Protein id" column echoes the input header
(SG<id>~k141_XXXXXX_N), preserving the sample + contig ID for the downstream
merge_amrfinderplus + w4_recompile de-prefix.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::amrfinderplus.env"))
chunk = model.AddRequirement(lib.GetType("sequences::orf_batch"))
db = model.AddRequirement(lib.GetType("annotation::amrfinderplus_db"))
out_results = model.AddProduct(lib.GetType("annotation::amrfinderplus_results_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(chunk)
    idb = context.Input(db)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/amrdb")],
        cmd=f"""
            amrfinder \
                -p {iorfs.container} \
                -d /amrdb \
                --plus \
                --threads {threads} \
                -o {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=chunk,
    resources=Resources(
        cpus=8,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

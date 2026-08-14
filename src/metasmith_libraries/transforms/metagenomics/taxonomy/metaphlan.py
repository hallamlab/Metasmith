from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::metaphlan.env"))
db      = model.AddRequirement(lib.GetType("ref::metaphlan_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

profile = model.AddProduct(lib.GetType("taxonomy::metaphlan_profile"))
sam     = model.AddProduct(lib.GetType("taxonomy::metaphlan_sam"))

def protocol(context: ExecutionContext):
    idb      = context.Input(db)
    ireads   = context.Input(reads)
    iprof    = context.Output(profile)
    isam     = context.Output(sam)

    threads  = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--nproc {threads}"

    # v4.2 quirks: --offline avoids compute-node firewall hits; interleaved
    # input is just a single fastq stream to metaphlan (bowtie2 underneath),
    # which doesn't enforce pair semantics on the input; --db_dir replaces the
    # v4.1 --bowtie2db flag.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            metaphlan {ireads.container} \
                --input_type fastq --offline \
                --db_dir {idb.container} {threads_arg} \
                -o {iprof.container} \
                --mapout {isam.container}
        """
    )

    return ExecutionResult(
        manifest=[{
            profile: iprof.local,
            sam:     isam.local,
        }],
        success=iprof.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),  # bowtie2 indexes are ~33 GB; 32 GB OOMs
        duration=Duration(hours=2),
    )
)

from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::short_reads_qc"))
image   = model.AddRequirement(lib.GetType("std::oci_image_spades"))
out     = model.AddProduct(lib.GetType("std::short_reads_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    reads_path = context.Input(reads)

    # Thread count comes from $task.cpus (surfaced as params["cpus"] via the
    # step metadata). Fall back to a sane default so SPAdes is NEVER silently
    # single-threaded when that plumbing does not populate cpus — the pilot saw
    # tasks pinned to 1 CPU, which turned a minutes-long toy assembly into a
    # >80-min crawl. A toy 100k-pair --isolate assembly tolerates mild
    # oversubscription far better than running on one core. See plan T3.
    cpus = context.params.get("cpus") or 8
    cpus_string = f"--threads {cpus}"

    # isolate short-read assembly; --12 = interleaved paired-end reads.
    # SPAdes writes contigs.fasta into its output dir; expose that file as the
    # assembly product so downstream tools (prodigal/bakta) get a FASTA.
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                spades.py \
                    --isolate \
                    {cpus_string} \
                    --12 {reads_path.container} \
                    -o spades_out
                cp spades_out/contigs.fasta {out_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)

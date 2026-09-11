from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::sra-tools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
dep     = model.AddRequirement(lib.GetType("ncbi::sra_accession"), parents={meta})

# One accession yields exactly one of these, and which one is a fact about the
# run that only the metadata knows -- so they are alternative product groups
# rather than three products. Downstream tools require the concrete type
# (bbduk takes short_reads, filtlong takes long_reads), which is why this does
# not simplify to a single `sequences::reads` product.
long     = model.AddProduct(lib.GetType("sequences::long_reads"))
model.NewProductGroup()
short_pe = model.AddProduct(lib.GetType("sequences::short_reads_pe"))
model.NewProductGroup()
short_se = model.AddProduct(lib.GetType("sequences::short_reads_se"))

def protocol(context: ExecutionContext):
    iacc = context.Input(dep)
    imeta = context.Input(meta)

    with open(iacc.local) as f:
        acc = f.readline().strip()
    Log.Info(f"received SRA accession was [{acc}]")

    with open(imeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    parity = read_meta["parity"]
    Log.Info(f"length_class=[{length_class}], parity=[{parity}]")
    assert length_class in {"short", "long"}, f"unknown [length_class] = [{length_class}]"
    assert parity in {"single", "paired"}, f"unknown [parity] = [{parity}]"

    if length_class == "long":
        reads = long
    elif parity == "paired":
        reads = short_pe
    else:
        reads = short_se
    ireads = context.Output(reads)

    cpus = context.params.get('cpus')
    threads_param = "" if cpus is None else f"-p {cpus}"

    # prefetch leaves the cache in ./{acc}, which is where fastq-dump looks for
    # it, so the two halves need nothing between them. --split-spot writes both
    # mates of a paired spot consecutively, which is the interleaving
    # `short_reads_pe` declares.
    context.ExecWithEnv(
        env = image,
        cmd = f"""
        echo "downloading"
        prefetch {acc} --max-size 1T
        du -shL {acc}
        echo "dumping"
        fastq-dump --split-spot --offset 33 --defline-seq '@$si/$ri' --defline-qual '+' --stdout \
            {acc} \
        | pigz {threads_param} -c >{ireads.container}
        """,
        shell="sh",
    )

    return ExecutionResult(
        manifest=[
            {
                reads: ireads.local,
            }
        ],
        success=ireads.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    # The download needs egress, so the whole step runs wherever the site's
    # nextflow config sends `local` -- the cost of not splitting the dump onto
    # a compute node, and cheaper than staging a multi-GB prefetch cache
    # between two steps to buy it back.
    labels=["local"],
    resources=Resources(
        cpus=4,
        # Both halves stream -- prefetch to disk, then fastq-dump piped through
        # pigz -- so this is bounded by disk and network, not by memory. The 64
        # GB the two retired transforms each asked for was above the local
        # executor's ceiling, and a step nextflow refuses at submit is one
        # `errorStrategy = ignore` turns into a run that completes having
        # produced nothing.
        memory=Size.GB(8),
        duration=Duration(hours=12),
    )
)

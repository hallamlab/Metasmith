import gzip
import json
import re
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::sra-tools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
dep     = model.AddRequirement(lib.GetType("ncbi::sra_long_read_accession"), parents={meta})
reads   = model.AddProduct(lib.GetType("sequences::long_reads"))

def protocol(context: ExecutionContext):
    iacc = context.Input(dep)
    imeta = context.Input(meta)

    with open(iacc.local) as f:
        acc = f.readline().strip()
    Log.Info(f"received SRA accession was [{acc}]")

    with open(imeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    Log.Info(f"length_class=[{length_class}]")
    assert length_class == "long", f"[length_class] = [{length_class}] was given a long-read accession"

    ireads = context.Output(reads)

    cpus = context.params.get('cpus')
    threads_param = "" if cpus is None else f"-p {cpus}"

    # prefetch leaves the cache in ./{acc}, which is where fastq-dump looks for
    # it, so the two halves need nothing between them. The image's sh is busybox
    # ash, which honours the pipefail ExecWithEnv sets.
    dump_log = "fastq-dump.log"
    context.ExecWithEnv(
        env = image,
        cmd = f"""
        echo "downloading"
        prefetch {acc} --max-size 1T
        du -shL {acc}
        echo "dumping"
        fastq-dump --split-spot --offset 33 --defline-seq '@$si/$ri' --defline-qual '+' --stdout \
            {acc} 2>{dump_log} \
        | pigz {threads_param} -c >{ireads.container} \
        || {{ c=$?; cat {dump_log} >&2; exit $c; }}
        cat {dump_log}
        """,
        shell="sh",
    )

    # fastq-dump can exit 0 having written nothing, and a gzip of nothing is a
    # valid 20-byte file, so the count it reports and the first record decide.
    written = re.findall(r"^Written (\d+) spots", open(dump_log).read(), re.M)
    spots = int(written[-1]) if written else 0
    with gzip.open(ireads.local, "rt", errors="replace") as f:
        first = [f.readline() for _ in range(4)]
    is_fastq = first[0].startswith("@") and first[2].startswith("+") and len(first[1]) > 1
    Log.Info(f"[{acc}] fastq-dump wrote {spots:,} spots, first record [{first[0].strip()}]")

    return ExecutionResult(
        manifest=[
            {
                reads: ireads.local,
            }
        ],
        success=spots > 0 and is_fastq,
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

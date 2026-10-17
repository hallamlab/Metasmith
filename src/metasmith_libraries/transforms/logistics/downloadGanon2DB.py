from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::ganon.env"))
out   = model.AddProduct(lib.GetType("ref::ganon2_sources"))

NCBI = "https://ftp.ncbi.nlm.nih.gov"
# genome_updater misses an assembly NCBI lists but no longer serves now and then, and
# one such gap should not fail a forty-thousand-genome download.
MIN_FETCHED = 0.99


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ncbi = context.params.get("ganon2_ncbi_url", NCBI).rstrip("/")
    threads = context.params.get("cpus") or 4
    failed = ExecutionResult(manifest=[], success=False)

    # The selection and flags `ganon build --source refseq --organism-group archaea
    # bacteria --top 1` hands genome_updater (ganon 2.4.1, build_update.py). Its own
    # default base is ftp://, which a cluster firewall tends to drop. The genomes are
    # hard-linked out rather than moved. A retry starts in a fresh work dir and fetches
    # from zero; -i only helps a rerun inside one attempt.
    context.ExecWithEnv(env=image, cmd=f"""
        export ncbi_base_url={ncbi}/
        fix=""
        if [ -e gu/assembly_summary.txt ]; then fix=-i; fi
        genome_updater.sh -d refseq -g archaea,bacteria -A 1 -f genomic.fna.gz \
            -t {threads} -o gu -m -G -N split -w $fix
        version=$(dirname "$(readlink -f gu/assembly_summary.txt)")
        mkdir -p {iout.container}/files
        cp $version/assembly_summary.txt {iout.container}/assembly_summary.txt
        cp -alf $version/files/. {iout.container}/files/
    """)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{ncbi}/pub/taxonomy/taxdump.tar.gz.md5", "taxdump.tar.gz.md5"),
    ])) != 0:
        return failed
    taxdump_md5 = open("taxdump.tar.gz.md5").read().split()[0]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{ncbi}/pub/taxonomy/taxdump.tar.gz", iout.local/"taxdump.tar.gz", md5=taxdump_md5),
        FetchCommand(f"{ncbi}/genomes/ASSEMBLY_REPORTS/species_genome_size.txt.gz",
                     iout.local/"species_genome_size.txt.gz"),
    ])) != 0:
        return failed

    listed = sum(1 for line in open(iout.local/"assembly_summary.txt") if not line.startswith("#"))
    fetched = sum(1 for _ in (iout.local/"files").rglob("*.fna.gz"))
    print(f"genome_updater fetched {fetched} of the {listed} assemblies it listed")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=listed > 0 and fetched >= MIN_FETCHED*listed,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=24),
    ),
)

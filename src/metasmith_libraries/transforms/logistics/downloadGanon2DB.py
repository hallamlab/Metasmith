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
    staged = context.StagingDir()
    ncbi = context.params.get("ganon2_ncbi_url", NCBI).rstrip("/")
    threads = context.params.get("cpus") or 4
    # A run param, so a smaller study or a test can fetch one group, e.g. archaea.
    groups = context.params.get("ganon2_organism_groups", "archaea,bacteria")
    failed = ExecutionResult(manifest=[], success=False)

    # The selection and flags `ganon build --source refseq --organism-group archaea
    # bacteria --top 1` hands genome_updater (ganon 2.4.1, build_update.py). Its own
    # default base is ftp://, which a cluster firewall tends to drop. The run lives in
    # the staging dir, so a retry's -i continues the attempt before it. -m stays here:
    # genome_updater re-fetches a file whose md5 differs, which only a step with the
    # network can do.
    context.ExecWithEnv(env=image, cmd=f"""
        export ncbi_base_url={ncbi}/
        fix=""
        if [ -e {staged.container}/gu/assembly_summary.txt ]; then fix=-i; fi
        genome_updater.sh -d refseq -g {groups} -A 1 -f genomic.fna.gz \
            -t {threads} -o {staged.container}/gu -m -G -N split -w $fix
    """)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{ncbi}/pub/taxonomy/taxdump.tar.gz.md5", staged.local/"taxdump.tar.gz.md5"),
    ])) != 0:
        return failed
    taxdump_md5 = open(staged.local/"taxdump.tar.gz.md5").read().split()[0]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{ncbi}/pub/taxonomy/taxdump.tar.gz", staged.local/"taxdump.tar.gz", md5=taxdump_md5),
        FetchCommand(f"{ncbi}/genomes/ASSEMBLY_REPORTS/species_genome_size.txt.gz",
                     staged.local/"species_genome_size.txt.gz"),
    ])) != 0:
        return failed

    version = (staged.local/"gu"/"assembly_summary.txt").resolve().parent
    listed = sum(1 for line in open(version/"assembly_summary.txt") if not line.startswith("#"))
    fetched = sum(1 for _ in (version/"files").rglob("*.fna.gz"))
    print(f"genome_updater fetched {fetched} of the {listed} assemblies it listed")
    if not (listed > 0 and fetched >= MIN_FETCHED*listed):
        return failed

    # Moved, not copied: the product is the only copy, and a later attempt never
    # runs once this one succeeded.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        f"cp {version}/assembly_summary.txt {iout.local}/assembly_summary.txt",
        f"mv {version}/files {iout.local}/files",
        f"mv {staged.local}/taxdump.tar.gz {staged.local}/species_genome_size.txt.gz {iout.local}/",
    ])) != 0:
        return failed
    return ExecutionResult(manifest=[{out: iout.local}], success=True)


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

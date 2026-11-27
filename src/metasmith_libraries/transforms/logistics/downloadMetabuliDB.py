from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::metabuli.env"))
scripts = model.AddRequirement(lib.GetType("lib::metabuli_gtdb"))
sources = model.AddProduct(lib.GetType("ref::metabuli_sources"))

# Every source of the GTDB r232 Metabuli database. r232 is the release GTDB-Tk is pinned to
# in downloadGtdbDB.py, so bin-level and contig-level taxonomy agree. The genome archive is
# 192 GB; buildMetabuliDB extracts the selected genomes from it on a compute node.
# gtdb-taxdump is GTDB r232 as an NCBI-style dump with the accession-to-taxid map that
# `metabuli build --gtdb 1` reads. The NCBI taxdump is a dated snapshot, because the live
# one changes daily; it places human and the viruses. RefSeq viral is the current release
# (238 when pinned), which NCBI replaces in place, so RELEASE_NUMBER records which one
# was fetched.
URLS = {
    "gtdb_genomes_reps_r232.tar.gz": "https://data.gtdb.ecogenomic.org/releases/release232/232.0/genomic_files_reps/gtdb_genomes_reps_r232.tar.gz",
    "bac120_metadata_r232.tsv.gz": "https://data.gtdb.ecogenomic.org/releases/release232/232.0/bac120_metadata_r232.tsv.gz",
    "ar53_metadata_r232.tsv.gz": "https://data.gtdb.ecogenomic.org/releases/release232/232.0/ar53_metadata_r232.tsv.gz",
    "gtdb-taxdump-R232.tar.gz": "https://github.com/shenwei356/gtdb-taxdump/releases/download/v0.7.0/gtdb-taxdump-R232.tar.gz",
    "GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz": "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/009/914/755/GCF_009914755.1_T2T-CHM13v2.0/GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz",
    "taxdmp_2026-10-01.zip": "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/taxdmp_2026-10-01.zip",
    "viral.1.1.genomic.fna.gz": "https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/viral.1.1.genomic.fna.gz",
    "viral.1.genomic.gbff.gz": "https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/viral.1.genomic.gbff.gz",
    "RELEASE_NUMBER": "https://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER",
}
# From GTDB's MD5SUM.txt and NCBI's md5checksums.txt. The archive's md5 is checked by
# buildMetabuliDB while it reads the archive, so the login node does not read 192 GB twice.
MD5 = {
    "bac120_metadata_r232.tsv.gz": "0ba4237077b65cfc5556e1d8be797485",
    "ar53_metadata_r232.tsv.gz": "0f51fe6f01b4ccbf0d9bab6c07df87af",
    "gtdb-taxdump-R232.tar.gz": "4a1dcbdc39ba3abc883f4f182002460f",
    "GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz": "5a2c359a054041f405aabb4facaba76d",
}
ARCHIVE_ROOT = "gtdb_genomes_reps_r232"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    names = list(URLS)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(url, staged.local/name, md5=MD5.get(name), label=name)
          for name, url in URLS.items()),
        f"mkdir -p {isrc.local}",
        *(f"mv {staged.local/name} {isrc.local}/" for name in names),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    iscripts = context.Input(scripts)
    if context.LocalShell(
        f"python3 {iscripts.local}/select_reps.py --archive-root {ARCHIVE_ROOT} --out {isrc.local}"
        f" --metadata {isrc.local}/bac120_metadata_r232.tsv.gz {isrc.local}/ar53_metadata_r232.tsv.gz"
    ) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=all((isrc.local/f).is_file() for f in [*names, "selected.tsv", "members.txt"]),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(180),  # the 192 GB genome archive plus the small sources
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=48),
    ),
)

# OPERA-MS on E5's MEGAHIT contigs. MEGAHIT is OPERA-MS's own default short-read assembler, so handing
# it the contigs reproduces its default pipeline while sharing the assembly with the short-read lane.
# --no-ref-clustering: the reference database would name CAMI's public genomes for it, which a novel
# community like Pratama's never gets. --no-polishing drops OPERA-MS's Pilon pass, which asks ~1 GB per Mb
# of assembly: the contigs are MEGAHIT's, already short-read accurate, and the lane measures graph resolution.
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e5::opera_ms.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
contigs = model.AddRequirement(lib.GetType("sequences::megahit_assembly"), parents={meta})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={meta})
nano    = model.AddRequirement(lib.GetType("e3::nanopore_reads"), parents={pair})
out     = model.AddProduct(lib.GetType("e5::opera_ms_assembly"))

OPERA_MS = "/home/mambauser/operams/OPERA-MS.pl"


def protocol(context: ExecutionContext):
    icontigs, ireads, inano, iout = context.Input(contigs), context.Input(reads), context.Input(nano), context.Output(out)
    threads = context.params.get("cpus") or 2

    # OPERA-MS 0.9.0 refuses a gzipped long-read file.
    context.ExecWithEnv(env=image, cmd=f"""
        gzip -dcf {ireads.container} \
            | awk '{{ if (int((NR-1)/4) % 2 == 0) print > "r1.fastq"; else print > "r2.fastq" }}'
        gzip -dcf {inano.container} > long.fastq
        perl {OPERA_MS} --contig-file {icontigs.container} --short-read1 r1.fastq --short-read2 r2.fastq \
            --long-read long.fastq --no-ref-clustering --no-polishing --num-processors {threads} --out-dir opera
        rm r1.fastq r2.fastq long.fastq
        cat opera/assembly.stats || true
        cp opera/contigs.fasta {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    # The README's medium-complexity gut sample: 2.7 h and 10.2 GB at 16 threads, with the reference database.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)

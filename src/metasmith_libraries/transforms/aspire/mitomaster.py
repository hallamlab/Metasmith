# mitomaster -- MITOMASTER (asv_pipeline.nf:3544) with PREPARE_BLAST_DATABASES (3511), BLAST only.
#
# MitoMaster itself posts every ASV to mitomap.org, and compute nodes have no internet. The
# table is written header-only, which is upstream's own `run_mitomaster: false` output, and
# mito_checker.py reads it as no MitoMaster calls. The image's gzip is busybox, which has no
# `-f` passthrough, so an uncompressed reference is read with cat. A reference is staged into
# the work dir under its own file name, so the screens' copies take a prefix: writing to that
# name writes through the link into the reference.
#
# The contaminant screen is one database over every FASTA in the set. Each header becomes
# `<stem>|<id>`, so a hit names its list; contaminant_hits.py splits it on the same `|`.
# makeblastdb -parse_seqids keeps the tagged id whole, and refuses one over 50 characters.

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::blast.env"))
study    = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fcounts  = model.AddRequirement(lib.GetType("aspire::asv_filtered_counts"), parents={study})
fseqs    = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={study})
mito_src = model.AddRequirement(lib.GetType("aspire::mito_reference_source"))
cont_set = model.AddRequirement(lib.GetType("aspire::contaminant_reference_set"))
master   = model.AddProduct(lib.GetType("aspire::mitomaster_table"))
mhits    = model.AddProduct(lib.GetType("aspire::mito_blast6"))
chits    = model.AddProduct(lib.GetType("aspire::contaminant_blast6"))

BLAST6 = "6 qseqid sseqid pident length qlen mismatch gapopen qstart qend sstart send evalue bitscore"


def protocol(context: ExecutionContext):
    imaster, imhits, ichits = context.Output(master), context.Output(mhits), context.Output(chits)
    threads = context.params.get("cpus", 8)
    imito, iset = context.Input(mito_src).container, context.Input(cont_set).container
    screens = ""
    for name, out in (("mito", imhits), ("contaminants", ichits)):
        screens += f"""
        makeblastdb -in screen_{name}.fasta -dbtype nucl -parse_seqids -out {name}_db
        blastn -query asvs.fasta -db {name}_db -outfmt "{BLAST6}" -out {out.container} -num_threads {threads}
        """
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        gzip -cd {context.Input(fseqs).container} > asvs.fasta
        case {imito} in *.gz) gzip -cd {imito} ;; *) cat {imito} ;; esac > screen_mito.fasta
        : > screen_contaminants.fasta
        for f in {iset}/*.fasta {iset}/*.fasta.gz; do
            [ -e "$f" ] || continue
            stem=$(basename "$f"); stem=${{stem%.gz}}; stem=${{stem%.fasta}}
            case "$f" in *.gz) gzip -cd "$f" ;; *) cat "$f" ;; esac \
                | awk -v s="$stem" '/^>/ {{ print ">" s "|" substr($0, 2); next }} {{ print }}' >> screen_contaminants.fasta
        done
        [ -s screen_contaminants.fasta ] || {{ echo "no .fasta or .fasta.gz in the contaminant set {iset}" >&2; exit 1; }}
        {screens}
        printf 'Sequence_ID\\thaplo\\n' > {imaster.container}
        rm -f asvs.fasta screen_*.fasta mito_db.* contaminants_db.*
    """)

    return ExecutionResult(
        manifest=[{master: imaster.local, mhits: imhits.local, chits: ichits.local}],
        success=all(p.local.exists() for p in (imaster, imhits, ichits)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)

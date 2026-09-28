# mitomaster -- MITOMASTER (asv_pipeline.nf:3544) with PREPARE_BLAST_DATABASES (3511), BLAST only.
#
# MitoMaster itself posts every ASV to mitomap.org, and compute nodes have no internet. The
# table is written header-only, which is upstream's own `run_mitomaster: false` output, and
# mito_checker.py reads it as no MitoMaster calls. The image's gzip is busybox, which has no
# `-f` passthrough, so an uncompressed reference is read with cat.

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::blast.env"))
study    = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fcounts  = model.AddRequirement(lib.GetType("aspire::asv_filtered_counts"), parents={study})
fseqs    = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={study})
mito_src = model.AddRequirement(lib.GetType("aspire::mito_reference_source"))
cont_src = model.AddRequirement(lib.GetType("aspire::contaminant_reference_source"))
master   = model.AddProduct(lib.GetType("aspire::mitomaster_table"))
mhits    = model.AddProduct(lib.GetType("aspire::mito_blast6"))
chits    = model.AddProduct(lib.GetType("aspire::contaminant_blast6"))

BLAST6 = "6 qseqid sseqid pident length qlen mismatch gapopen qstart qend sstart send evalue bitscore"


def protocol(context: ExecutionContext):
    imaster, imhits, ichits = context.Output(master), context.Output(mhits), context.Output(chits)
    threads = context.params.get("cpus", 8)
    screens = ""
    for name, src, out in (("mito", mito_src, imhits), ("contaminants", cont_src, ichits)):
        screens += f"""
        case {context.Input(src).container} in *.gz) gzip -cd {context.Input(src).container} ;; *) cat {context.Input(src).container} ;; esac > {name}.fasta
        makeblastdb -in {name}.fasta -dbtype nucl -parse_seqids -out {name}_db
        blastn -query asvs.fasta -db {name}_db -outfmt "{BLAST6}" -out {out.container} -num_threads {threads}
        """
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        gzip -cd {context.Input(fseqs).container} > asvs.fasta
        {screens}
        printf 'Sequence_ID\\thaplo\\n' > {imaster.container}
        rm -f asvs.fasta mito.fasta contaminants.fasta mito_db.* contaminants_db.*
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

# ASPIRE's default contaminant list: ASVs denoised from the published negative controls pinned in
# lib::aspire's literature_controls.tsv. Each source is merged, filtered and denoised alone with
# ASPIRE's own vsearch settings, but at vsearch's default minimum size of 8, so a list entry is
# never one stray read. control_asvs.py's docstring says what `studies` and `blanks` count.
#
# Weyrich's figshare FASTA carries no qualities and is passed through unfiltered. The build
# downloads about 13 GB and runs once; the product is pinned with DVC rather than rebuilt per run.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
vsearch = model.AddRequirement(lib.GetType("env::vsearch.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
listed  = model.AddProduct(lib.GetType("aspire::contaminants_literature"))

SOURCES = ("weyrich2019", "katharoseq2018", "dyrhovden2021")


def protocol(context: ExecutionContext):
    ilisted, iscripts = context.Output(listed), context.Input(scripts)
    script = f"python {iscripts.container}/control_asvs.py"
    threads = context.params.get("cpus", 8)

    for source in SOURCES:
        context.ExecWithEnv(env=python, cmd=f"""\
            set -euo pipefail
            {script} fetch --source {source} --dir work/{source} --threads {threads}
        """)
        context.ExecWithEnv(env=vsearch, cmd=f"""\
            set -euo pipefail
            cd work/{source}
            mkdir -p qc
            while IFS=$'\\t' read -r sample layout r1 r2; do
                case "$layout" in
                    fasta) mv "$r1" "qc/$sample.fasta"; continue ;;
                    paired)
                        vsearch --fastq_mergepairs "$r1" --reverse "$r2" --fastqout merged.fastq \\
                            --fastq_maxdiffs 20 --fastq_minovlen 5 --fastq_truncqual 5 \\
                            --fastq_allowmergestagger --threads {threads} --quiet
                        reads=merged.fastq ;;
                    *) reads="$r1" ;;
                esac
                vsearch --fastq_filter "$reads" --fastq_maxee 1.0 --fastq_maxns 0 \\
                    --fastaout "qc/$sample.fasta" --quiet
                rm -f merged.fastq "$r1" ${{r2:+"$r2"}}
            done < manifest.tsv
        """)
        context.ExecWithEnv(env=python, cmd=f"""\
            set -euo pipefail
            {script} trim --source {source} --dir work/{source} --out work/{source}/reads.fasta
            rm -rf work/{source}/raw work/{source}/qc
        """)
        context.ExecWithEnv(env=vsearch, cmd=f"""\
            set -euo pipefail
            cd work/{source}
            vsearch --derep_fulllength reads.fasta --output derep.fasta --sizeout --threads {threads}
            vsearch --cluster_unoise derep.fasta --centroids centroids.fasta \
                --sizein --sizeout --relabel ASV --minsize 8 --threads {threads}
            vsearch --uchime3_denovo centroids.fasta --nonchimeras nochimeras.fasta --sizein
            vsearch --fastx_filter nochimeras.fasta --xsize --fastaout asvs.fasta
            vsearch --usearch_global reads.fasta --db asvs.fasta --id 0.99 \\
                --otutabout otutab.tsv --threads {threads}
            rm reads.fasta derep.fasta centroids.fasta nochimeras.fasta
        """)

    context.ExecWithEnv(env=python, cmd=f"{script} pool --work work --out pooled.fasta")
    context.ExecWithEnv(env=vsearch, cmd="""\
        set -euo pipefail
        vsearch --derep_prefix pooled.fasta --output prefixed.fasta --uc prefixed.uc
    """)
    context.ExecWithEnv(env=python, cmd=f"""\
        set -euo pipefail
        {script} assemble --work work --uc prefixed.uc --fasta pooled.fasta --out {ilisted.container}
    """)

    return ExecutionResult(
        manifest=[{listed: ilisted.local}],
        success=ilisted.local.exists() and ilisted.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=python,
    labels=["local"],
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    ),
)

# taxonomy -- TAXONOMY (asv_pipeline.nf:3470), upstream's qiime_vs_classifier.py unchanged.
#
# QIIME's plugin load imports umap, whose numba functions cache beside themselves unless
# NUMBA_CACHE_DIR names somewhere writable. The image's site-packages is read-only under
# apptainer, and `--no-home` leaves no home to fall back on.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::qiime2.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
trimmed = model.AddRequirement(lib.GetType("aspire::sina_trimmed_seqs"), parents={study})
silva   = model.AddRequirement(lib.GetType("amplicon::silva_db"))
tax     = model.AddProduct(lib.GetType("amplicon::asv_taxonomy"))
upper   = model.AddProduct(lib.GetType("aspire::taxonomy_uppercase_seqs"))
stats   = model.AddProduct(lib.GetType("aspire::taxonomy_stats"))


def protocol(context: ExecutionContext):
    itax, iupper, istats = context.Output(tax), context.Output(upper), context.Output(stats)
    isilva = context.Input(silva)
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export HOME=$PWD NUMBA_CACHE_DIR=$PWD/.numba
        gzip -cd {context.Input(trimmed).container} \
            | awk '/^>/ {{ print; next }} {{ print toupper($0) }}' > upper.fasta
        python {context.Input(scripts).container}/qiime_vs_classifier.py \
            --input-fasta upper.fasta \
            --ref-taxonomy {isilva.container}/silva_tax.qza --ref-seqs {isilva.container}/silva_seqs.qza \
            --output-tsv {itax.container} --stats-output {istats.container} \
            --threads {context.params.get('cpus', 8)}
        gzip -n -c upper.fasta > {iupper.container}
        rm upper.fasta
    """)

    return ExecutionResult(
        manifest=[{tax: itax.local, upper: iupper.local, stats: istats.local}],
        success=all(p.local.exists() for p in (itax, iupper, istats)),
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

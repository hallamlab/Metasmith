# clustermaps -- CLUSTERMAPS (asv_pipeline.nf:5324), once per label of the sample sheet with at
# least two levels, at upstream's config defaults. Upstream's three annotation bars default to
# lung-study columns; here bar 1 is the label, bar 2 the next analysable label (or the label
# itself) and bar 3 is off.
#
# Upstream searches the indicator directory for `<column>_indicator_species_summary.tsv`, so
# each label's own summary gates its heatmap when indicspecies wrote one. The mito heatmaps run
# when the removed counts hold mitochondrial ASVs.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
am      = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={study})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={study})
tables  = model.AddRequirement(lib.GetType("aspire::indicspecies_tables"), parents={study})
removed = model.AddRequirement(lib.GetType("aspire::counts_removed"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::clustermap_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o, isa = iout.container, context.Input(tables).container
    sid_col = context.Input(study).local.read_text().splitlines()[0].split("\t")[0]

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o}
        n=$(python {s}/upstream_layout.py mito --removed {context.Input(removed).container} --out mito.tsv)
        python {s}/upstream_layout.py labels --sheet {context.Input(study).container} \
            --table {context.Input(md).container} --skipped {o}/skipped_labels.tsv > labels.tsv
        while IFS=$'\\t' read -r L D L2; do
            extra=()
            [ -f "{isa}/$L"_indicator_species_summary.tsv ] && extra+=(--isa "{isa}/$L"_indicator_species_summary.tsv)
            [ "$n" -gt 0 ] && extra+=(--mito-asv mito.tsv --mito-outdir {o}/$D/mito)
            python {s}/upstream_layout.py recolor --label "$L" {context.Input(md).container} md.tsv
            python {s}/plot_clustermaps.py --asv-meta {context.Input(am).container} --metadata md.tsv \
                --outdir {o}/$D --sample-col "{sid_col}" --sample-code-col sample_code \
                --asv-id-col ASV_ID --group1-col "$L" --group2-col "$L2" --group3-col "" \
                --ranks Phylum,Class,Order,Family,Genus,Species,ASV_ID \
                --topN Phylum=30,Class=30,Order=30,Family=30,Genus=30,Species=30,ASV_ID=6000 \
                --count-col corr_count --isa-min-stat 0.6 --formats pdf,png,svg \
                --mito-sample-mode auto "${{extra[@]}}"
        done < labels.tsv
        rm -f labels.tsv md.tsv mito.tsv
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "skipped_labels.tsv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)

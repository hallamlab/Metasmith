# graph_network -- GRAPH_NETWORK (asv_pipeline.nf:5579) at upstream's config defaults, with
# every label of the sample sheet as an indicator overlay and the first as the module source.
#
# The script globs its working directory for indicator summaries, as upstream stages them
# there, so they are copied in. It requires two overlays, as upstream's study has two grouping
# columns; a lone label is overlaid twice, the second time as `<label>_twin`. The product doubles as upstream's spieceasi directory, which
# the master summary scans: it also holds the SpiecEasi node features and the module tables.
#
# This row reads no params, so a count table from outside ASPIRE can reach it.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
policy  = model.AddRequirement(lib.GetType("aspire::graph_network_on"), parents={survey})
all     = model.AddRequirement(lib.GetType("aspire::network_graph_all"), parents={survey})
thr     = model.AddRequirement(lib.GetType("aspire::network_graph_thr"), parents={survey})
nf      = model.AddRequirement(lib.GetType("aspire::network_node_features"), parents={survey})
counts  = model.AddRequirement(lib.GetType("amplicon::abundance_table"), parents={survey})
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={survey})
pairing = model.AddRequirement(lib.GetType("aspire::asv_mag_pairing"), parents={survey})
tax     = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={survey})
tables  = model.AddRequirement(lib.GetType("aspire::indicspecies_tables"), parents={survey})
sub     = model.AddRequirement(lib.GetType("aspire::network_modules_sub"), parents={survey})
mall    = model.AddRequirement(lib.GetType("aspire::network_modules_all"), parents={survey})
out     = model.AddProduct(lib.GetType("aspire::network_outputs"))


def protocol(context: ExecutionContext):
    iout, s = context.Output(out), context.Input(scripts).container
    o = iout.container
    header = context.Input(survey).local.read_text().splitlines()[0].split("\t")
    sid_col, labels = header[0], header[1:]
    i = {p: context.Input(p).container for p in (all, thr, nf, counts, md, pairing, tax, tables, sub, mall)}

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl NUMBA_CACHE_DIR=$PWD/.cache/numba
        mkdir -p {o} isa
        cp {i[tables]}/*_indicator_species*_summary.tsv isa/ 2>/dev/null || true
        cp {i[nf]} {o}/spieceasi_node_features.csv
        cp {i[sub]} {o}/spieceasi_modules_sub.tsv
        cp {i[mall]} {o}/spieceasi_modules_all.tsv
        overlays=$(python {s}/upstream_layout.py isa_overlays --dir isa --metadata {i[md]} --out md.tsv)
        if [ -z "$overlays" ]; then
            echo "no label has an indicator summary: nothing to overlay" > {o}/NOTE.txt
        else
            cd isa
            python {s}/graph_network.py --data-dir . --outdir {o} \
                --graph-pos-all {i[all]} --graph-pos-sub {i[thr]} --node-features {i[nf]} \
                --asv-counts {i[counts]} --taxonomy {i[tax]} --metadata ../md.tsv \
                --sample-col "{sid_col}" --isa-group-cols "$overlays" \
                --isa-summary-mode default --asv-mag-pairing {i[pairing]} --color-col Color \
                --module-best-only --module-best-min-size 5 --module-best-min-stability 0.7 \
                --module-isa-source "{labels[0]}" --module-isa-min-stat 0.25 --module-isa-max-q 0.05 \
                --modules-sub {i[sub]} --modules-all {i[mall]} --layout-seed 42 --layout-scale 3.0 \
                --degree-scale 80 --degree-size-mode legacy --degree-min-area 0 \
                --edge-width-scale 5 --isa-scale 700 --abundance-size-mode legacy \
                --abundance-reference 5000 --abundance-reference-area 80 --abundance-min-area 8 \
                --abundance-max-area 420 --abundance-scale-power 1.6
            cd ..
        fi
        rm -rf isa md.tsv
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "spieceasi_node_features.csv").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)

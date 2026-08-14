from metasmith.python_api import *
import json

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("sequences::long_reads"))
image   = model.AddRequirement(lib.GetType("env::nanoplot.env"))
stats   = model.AddProduct(lib.GetType("sequences::read_qc_stats"))
html    = model.AddProduct(lib.GetType("sequences::nanoplot_html_report"))
raw     = model.AddProduct(lib.GetType("sequences::nanoplot_raw_report"))

def protocol(context: ExecutionContext):
    istats = context.Output(stats)
    ihtml = context.Output(html)
    iraw = context.Output(raw)
    ireads = context.Input(reads)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            NanoPlot --tsv_stats --no_static --plots dot \
                {threads} \
                --fastq {ireads.container} \
                --outdir nanoplot_out
            mv nanoplot_out/NanoPlot-report.html {ihtml.container}
            tar -czf {iraw.container} nanoplot_out
        """,
    )

    meta = {}
    def _as_number(x):
        if "." not in x:
            try:
                return int(x)
            except ValueError:
                pass
        try:
            return float(x)
        except ValueError:
            return x
    with open("nanoplot_out/NanoStats.txt") as f:
        for l in f:
            k, v = l[:-1].split("\t")
            meta[k] = _as_number(v)

    stats_json = dict(
        reads = meta["number_of_reads"],
        bases = meta["number_of_bases"],
        N50 = meta["n50"],
        median_quality = meta["median_qual"],
        _raw = meta,
    )
    with open(istats.local, "w") as j:
        json.dump(stats_json, j)
    return ExecutionResult(
        manifest=[{
            html:ihtml.local,
            raw:iraw.local,
            stats:istats.local,
        }],
        success=istats.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    )
)

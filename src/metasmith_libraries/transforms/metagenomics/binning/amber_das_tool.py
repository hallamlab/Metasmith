# amber.py fans out over the shared binning::contig_to_bin_table supertype on
# purpose, so it scores metabat2, semibin2 and comebin alike. That same
# breadth is what makes it unreachable for DAS Tool's pooled table alone: DAS
# Tool's table also satisfies contig_to_bin_table, and lineage matching in
# this planner is ancestral, so a target pinned to "the amber slot descending
# from the pooled table" cannot be told apart from "descending from the raw
# tables the pooled table itself descends from" -- the slot stays ambiguous
# and the target is dropped rather than narrowed. Confirmed the hard way in
# research/cami/run_cami_metag.py's `_build_binning_targets`: pinning amber's
# existing table requirement to the DAS Tool table drops the target outright,
# and masking the other binners out of the library so only that table remains
# still fails, which is what shows amber cannot consume it through that
# requirement at all rather than merely preferring another producer.
#
# This transform requires the CONCRETE das_tool_contig_to_bin_table instead of
# the shared supertype. Exactly one transform (das_tool.py) produces that
# type, so there is no ambiguity to resolve and a target naming this
# transform's own products needs no parent pin. Its products are their own
# distinct types (das_tool_amber_results / das_tool_amber_bin_metrics) rather
# than amber.py's amber_results / amber_bin_metrics, because two transforms
# producing one type is the other half of this trap -- see binning.yml.
#
# Everything below is amber.py's own protocol, unchanged.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::amber.env"))
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
table = model.AddRequirement(lib.GetType("binning::das_tool_contig_to_bin_table"), parents={asm})
gold  = model.AddRequirement(lib.GetType("binning::contig_gold_standard_table"), parents={asm})

out_results     = model.AddProduct(lib.GetType("binning::das_tool_amber_results"))
out_bin_metrics = model.AddProduct(lib.GetType("binning::das_tool_amber_bin_metrics"))


def protocol(context: ExecutionContext):
    itable = context.Input(table)
    igold = context.Input(gold)
    oresults = context.Output(out_results)
    obin_metrics = context.Output(out_bin_metrics)

    # binning::das_tool_contig_to_bin_table -> "das_tool", used only as
    # AMBER's display label and the genome/<label>/ subdirectory it writes
    # into -- same derivation as amber.py.
    label = context.GetMeta(table).type_name.split("::")[-1].removesuffix("_contig_to_bin_table")

    # amber.py matches gold standard and prediction by @SampleID -- reuse the
    # gold standard's own so the two agree.
    sample_id = None
    with open(igold.local) as f:
        for line in f:
            if line.startswith("@SampleID:"):
                sample_id = line.strip().split(":", 1)[1]
                break
    assert sample_id, f"gold standard [{igold.local}] has no @SampleID header"

    pred_path = Path("prediction.tsv")
    with open(itable.local) as fin, open(pred_path, "w") as fout:
        fin.readline()  # our own "contig\tbin" header, not Bioboxes'
        fout.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n@@SEQUENCEID\tBINID\n")
        for line in fin:
            fout.write(line)

    work = "amber_out"
    _cmd = f"""
            amber.py -g {igold.container} -l {label} -o {work} --skip_gs \
                {pred_path}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    results_src = Path(work) / "results.tsv"
    bin_metrics_src = Path(work) / "genome" / label / "metrics_per_bin.tsv"
    assert results_src.exists(), (
        f"amber.py wrote no results.tsv for [{label}] -- check {work}/log.txt"
    )
    assert bin_metrics_src.exists(), (
        f"amber.py wrote no genome/{label}/metrics_per_bin.tsv -- check {work}/log.txt"
    )
    oresults.local.write_bytes(results_src.read_bytes())
    obin_metrics.local.write_bytes(bin_metrics_src.read_bytes())

    return ExecutionResult(
        manifest=[{out_results: oresults.local, out_bin_metrics: obin_metrics.local}],
        success=oresults.local.exists() and obin_metrics.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=table,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)

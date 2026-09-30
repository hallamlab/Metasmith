import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::amber.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
gold    = model.AddRequirement(lib.GetType("e2::contig_gold_standard"), parents={asm})
# One AMBER run scores every binner beside DAS Tool, so each lane reports what DAS Tool's
# score threshold kept against the bin sets it chose from. Labels match nf-core/mag's.
tables  = {
    label: model.AddRequirement(lib.GetType(f"e2::{name}_contig_to_bin"), parents={asm})
    for label, name in (("MetaBAT2", "metabat2"), ("SemiBin2", "semibin2"),
                        ("COMEBin", "comebin"), ("DASTool", "das_tool"))
}
results = model.AddProduct(lib.GetType("e2::amber_results"))
per_bin = model.AddProduct(lib.GetType("e2::amber_bin_metrics"))


def protocol(context: ExecutionContext):
    igold = context.Input(gold)
    oresults = context.Output(results)
    oper_bin = context.Output(per_bin)

    sample_id = next(
        line.strip().split(":", 1)[1]
        for line in Path(igold.local).read_text().splitlines()
        if line.startswith("@SampleID:")
    )
    # MEGAHIT names contigs k141_<N> in every assembly, so a gold standard from another sample
    # shares nearly every contig name with this table. Only @SampleID tells them apart.
    sample = json.loads(Path(context.Input(meta).local).read_text())["sample"]
    if sample_id != sample:
        Log.Error(f"gold standard @SampleID [{sample_id}] is not this group's sample [{sample}]")
        return ExecutionResult(manifest=[], success=False)

    scored = {}
    for label, table in tables.items():
        # DAS Tool's contig2bin under --write_unbinned files every leftover contig under a bin named
        # `unbinned`, and lists some contigs twice. AMBER would score that as one huge bin.
        rows = dict.fromkeys(
            line for line in Path(context.Input(table).local).read_text().splitlines()
            if line.strip() and line.split("\t")[-1] != "unbinned"
        )
        # A binner with no bins for this sample has nothing for AMBER to read, and AMBER aborts
        # the whole run on an empty prediction. Its absence from results.tsv records the zero.
        if not rows:
            Log.Warn(f"{label} binned no contig in [{sample}]; not scored")
            continue
        path = f"prediction_{label}.tsv"
        with open(path, "w") as f:
            f.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n@@SEQUENCEID\tBINID\n")
            f.write("".join(f"{row}\n" for row in rows))
        scored[label] = path
    if not scored:
        Log.Error(f"no binner binned a contig in [{sample}]")
        return ExecutionResult(manifest=[], success=False)
    predictions = list(scored.values())

    labels = ",".join(scored)
    context.ExecWithEnv(env=image, cmd=f"""
        amber.py -g {igold.container} -l {labels} -o amber_out --skip_gs {" ".join(predictions)}
        cp amber_out/results.tsv {oresults.container}
        first=1
        for label in {" ".join(scored)}; do
            f=amber_out/genome/$label/metrics_per_bin.tsv
            if [ $first = 1 ]; then head -1 $f | sed 's/^/Tool\\t/'; first=0; fi
            tail -n +2 $f | sed "s/^/$label\\t/"
        done > {oper_bin.container}
    """)

    return ExecutionResult(
        manifest=[{results: oresults.local, per_bin: oper_bin.local}],
        success=oresults.local.exists() and oper_bin.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)

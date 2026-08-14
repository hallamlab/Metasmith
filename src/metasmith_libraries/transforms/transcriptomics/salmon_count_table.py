from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
quant   = model.AddRequirement(lib.GetType("transcriptomics::salmon_quant"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::count_table"))

def protocol(context: ExecutionContext):
    quant_paths=context.InputGroup(quant)
    iout=context.Output(out)

    # Write a manifest of quant.sf files for the merge script
    manifest = Path("quant_manifest.tsv")
    with open(manifest, "w") as f:
        for i, p in enumerate(quant_paths):
            sample_name = p.local.parent.name if p.local.parent.name != "." else f"sample_{i}"
            f.write(f"{sample_name}\t{p.container}\n")

    # Python script to merge quant.sf files into a single count table
    script = Path("merge_counts.py")
    with open(script, "w") as f:
        f.write("""\
import pandas as pd
import sys

manifest = sys.argv[1]
output = sys.argv[2]

samples = []
with open(manifest) as m:
    for line in m:
        name, path = line.strip().split("\\t")
        samples.append((name, path))

merged = None
for name, path in samples:
    df = pd.read_csv(path, sep="\\t")
    if merged is None:
        merged = df[["Name", "Length"]].copy()
    merged[f"TPM_{name}"] = df["TPM"]
    merged[f"NumReads_{name}"] = df["NumReads"]

merged.to_csv(output, sep="\\t", index=False)
""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python merge_counts.py {manifest} {iout.container}",
    )
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    )
)

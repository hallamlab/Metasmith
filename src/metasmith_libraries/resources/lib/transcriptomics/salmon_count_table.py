import pandas as pd
import sys

manifest = sys.argv[1]
output = sys.argv[2]

samples = []
with open(manifest) as m:
    for line in m:
        name, path = line.strip().split("\t")
        samples.append((name, path))

merged = None
for name, path in samples:
    df = pd.read_csv(path, sep="\t")
    if merged is None:
        merged = df[["Name", "Length"]].copy()
    merged[f"TPM_{name}"] = df["TPM"]
    merged[f"NumReads_{name}"] = df["NumReads"]

merged.to_csv(output, sep="\t", index=False)

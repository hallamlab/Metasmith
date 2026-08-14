from metasmith.python_api import *
import json
import pandas as pd
import numpy as np

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("sequences::reads"))
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
stats   = model.AddProduct(lib.GetType("sequences::read_qc_stats"))

def protocol(context: ExecutionContext):
    istats = context.Output(stats)
    ireads = context.Input(reads)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    seqkit_stats_file = "seqkit_stats.tsv"
    seqkit_guess_enc_file = "guess_encoding.txt"
    # example output of convert
    # [INFO] possible quality encodings: [Sanger Illumina-1.8+]
    # [INFO] guessed quality encoding: Sanger
    # [INFO] converting Sanger -> Sanger
    # [WARN] source and target quality encoding match.
    context.ExecWithEnv().ifContainerDo(
        env = img_sqk,
        cmd = f"""
            seqkit convert --dry-run {ireads.container} 2>&1 | tee {seqkit_guess_enc_file}
            seqkit stat {threads} --all --tabular {ireads.container} | tee {seqkit_stats_file}
        """,
    )
    with open(seqkit_guess_enc_file) as f:
        K = "guessed quality encoding:"
        encoding = "sanger"
        for l in f:
            if K not in l: continue
            _enc = l[:-1].split(K)[-1].strip()
            encoding = _enc
            break
        encoding = encoding.lower()

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
    def _from_np(x):
        if pd.isna(x): # includes nan, but isnan excludes None
            return None
        if isinstance(x, str):
            if x.endswith("%"):
                x = x[:-1]
            return _as_number(x)
        if isinstance(x, np.floating) or isinstance(x, np.integer):
            return x.item()
        return x
    df = pd.read_csv(seqkit_stats_file, sep="\t")
    seqkit_stats = {k:_from_np(v) for k, v in dict(df.iloc[0]).items()}

    curated_stats = dict(
        reads=seqkit_stats["num_seqs"],
        bases=seqkit_stats["sum_len"],
        mean_quality=seqkit_stats["AvgQual"],
        q20=seqkit_stats.get("Q20(%)"),
        q30=seqkit_stats.get("Q30(%)"),
        phred_encoding=encoding,
        N50=seqkit_stats["N50"],
        GC=seqkit_stats["GC(%)"],
        _raw=seqkit_stats,
    )
    print(f"stats:")
    for k, v in curated_stats.items():
        if k.startswith("_"): continue
        print(f"    {k}: {v}")

    with open(istats.local, "w") as j:
        json.dump(curated_stats, j)
        
    return ExecutionResult(
        manifest=[{
            stats:istats.local,
        }],
        success=istats.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    )
)

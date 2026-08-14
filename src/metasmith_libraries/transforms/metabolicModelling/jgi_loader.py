from metasmith.python_api import *
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
dataset = model.AddRequirement(lib.GetType("metabolomics::jgi_metabolomics_dataset"))
out_ft  = model.AddProduct(lib.GetType("metabolomics::metabolomics_feature_table"))


def protocol(context: ExecutionContext):
    idataset = context.Input(dataset)
    iout     = context.Output(out_ft)

    script = Path("jgi_loader_script.py")
    script.write_text('''
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path

data_dir   = Path(sys.argv[1])
output_csv = Path(sys.argv[2])

targeted_dir   = data_dir / "Targeted"
untargeted_dir = data_dir / "Untargeted"


def parse_sample_meta(name):
    n = name.lower()
    if "0h" in n:
        return {"condition": "0h", "timepoint_h": 0}
    for cond in ["8", "p", "s"]:
        if f"-{cond}-" in n:
            for tp in [24, 72, 144]:
                if f"{tp}h" in n:
                    return {"condition": cond, "timepoint_h": tp}
    return {"condition": "unknown", "timepoint_h": -1}


def load_targeted_run(ema_dir, istd_dir, polarity, chrom):
    atlas_files = sorted(ema_dir.glob("CompoundAtlas__*.csv"))
    if not atlas_files:
        return None
    atlas = pd.read_csv(atlas_files[0])

    data_sheet_dir = ema_dir / f"{polarity}_data_sheets"
    height_files = sorted(data_sheet_dir.glob(f"{polarity}_peak_height.tab"))
    if not height_files:
        return None
    heights = pd.read_csv(height_files[0], sep="\\t", index_col=0)

    pe_cols = [c for c in heights.columns if "161-pe" in c.lower()]
    if not pe_cols:
        return None
    heights = heights[pe_cols].copy()

    # ISTD normalization
    if istd_dir and istd_dir.exists():
        istd_atlas_files = sorted(istd_dir.glob("CompoundAtlas__*.csv"))
        istd_sheet_dir   = istd_dir / f"{polarity}_data_sheets"
        istd_height_files = sorted(istd_sheet_dir.glob(f"{polarity}_peak_height.tab")) if istd_sheet_dir.exists() else []
        if istd_atlas_files and istd_height_files:
            istd_h = pd.read_csv(istd_height_files[0], sep="\\t", index_col=0)
            istd_pe = [c for c in istd_h.columns if "161-pe" in c.lower()]
            if istd_pe:
                istd_h = istd_h[istd_pe]
                nf = istd_h.median(axis=0).replace(0, np.nan)
                heights = heights.div(nf, axis=1)

    heights.index.name = "compound_name"
    df = heights.reset_index()

    # Merge compound metadata from atlas
    meta_cols = [c for c in atlas.columns if c.lower() in
                 {"label", "name", "rt", "mz", "inchi_key", "kegg_id", "adduct", "polarity"}]
    if "label" in atlas.columns:
        atlas = atlas.rename(columns={"label": "compound_name"})
    elif "name" in atlas.columns:
        atlas = atlas.rename(columns={"name": "compound_name"})
    else:
        atlas["compound_name"] = atlas.index.astype(str)

    keep_cols = ["compound_name"] + [c for c in meta_cols if c != "label" and c in atlas.columns]
    df = df.merge(atlas[keep_cols].drop_duplicates("compound_name"), on="compound_name", how="left")
    df["polarity"]       = polarity
    df["chromatography"] = chrom
    df["source"]         = "targeted"
    return df


def load_untargeted_run(run_dir, chrom):
    height_files  = sorted(run_dir.glob("*_peak-height-filtered-3x-exctrl.csv"))
    meta_files    = sorted(run_dir.glob("*_metadata.tab"))
    library_files = sorted(run_dir.glob("*_gnps2-fbmn-library-results.tsv"))
    if not height_files:
        return None

    heights = pd.read_csv(height_files[0], index_col=0)

    if meta_files:
        meta = pd.read_csv(meta_files[0], sep="\\t")
        att_col = next((c for c in meta.columns if "sampletype" in c.lower()), None)
        fn_col  = next((c for c in meta.columns if "filename" in c.lower()), None)
        if att_col and fn_col:
            pe_meta = meta[meta[att_col].str.contains("161-pe", case=False, na=False)]
            pe_fns  = set(pe_meta[fn_col].str.replace(r"\\.mzML$", "", regex=True))
            pe_cols = [c for c in heights.columns
                       if any(fn in c for fn in pe_fns) or "161-pe" in c.lower()]
        else:
            pe_cols = [c for c in heights.columns if "161-pe" in c.lower()]
    else:
        pe_cols = [c for c in heights.columns if "161-pe" in c.lower()]

    if not pe_cols:
        return None
    heights = heights[pe_cols].copy()

    df = heights.reset_index()
    feat_col = df.columns[0]
    df = df.rename(columns={feat_col: "feature_id"})

    if library_files:
        lib_df = pd.read_csv(library_files[0], sep="\\t")
        scan_col = next((c for c in lib_df.columns if "scan" in c.lower()), None)
        name_col = next((c for c in lib_df.columns if "compound" in c.lower() or "name" in c.lower()), None)
        inchi_col = next((c for c in lib_df.columns if "inchi" in c.lower()), None)
        if scan_col and name_col:
            merge_cols = {scan_col: "feature_id", name_col: "compound_name"}
            if inchi_col:
                merge_cols[inchi_col] = "inchi_key"
            lib_sub = lib_df[[c for c in merge_cols]].rename(columns=merge_cols).drop_duplicates("feature_id")
            df = df.merge(lib_sub, on="feature_id", how="left")
        # Keep only annotated features for downstream analysis
        if "compound_name" in df.columns:
            df = df.dropna(subset=["compound_name"])

    df["polarity"]       = "unknown"
    df["chromatography"] = chrom
    df["source"]         = "untargeted"
    return df


all_dfs = []

# --- Targeted ---
if targeted_dir.exists():
    for chrom_dir in sorted(targeted_dir.iterdir()):
        if not chrom_dir.is_dir():
            continue
        chrom = "HILIC" if "HILIC" in chrom_dir.name.upper() else "C18"
        for pol in ["NEG", "POS"]:
            ema_dirs  = sorted(chrom_dir.glob(f"EMA-{pol}_*"))
            istd_dirs = sorted(chrom_dir.glob(f"ISTDsEtc*-{pol}_*"))
            istd_dir  = istd_dirs[0] if istd_dirs else None
            for ema_dir in ema_dirs:
                df = load_targeted_run(ema_dir, istd_dir, pol, chrom)
                if df is not None:
                    all_dfs.append(df)

# --- Untargeted ---
if untargeted_dir.exists():
    for run_dir in sorted(untargeted_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        chrom = "HILIC" if "HILIC" in run_dir.name.upper() else "C18"
        df = load_untargeted_run(run_dir, chrom)
        if df is not None:
            all_dfs.append(df)

if not all_dfs:
    print("ERROR: No data found in dataset")
    sys.exit(1)

result = pd.concat(all_dfs, ignore_index=True, sort=False)

# Identify sample columns
sample_cols = sorted({c for c in result.columns if "161-pe" in c.lower()})

# Log2-transform with zero imputation
for col in sample_cols:
    vals = result[col].fillna(0)
    min_pos = vals[vals > 0].min()
    impute = min_pos / 2 if pd.notna(min_pos) else 1.0
    result[col] = np.log2(vals.replace(0, impute))

# Deduplicate by inchi_key + polarity (keep targeted over untargeted)
if "inchi_key" in result.columns:
    result = result.sort_values("source", ascending=True)  # targeted < untargeted
    result = result.drop_duplicates(subset=["inchi_key", "polarity"], keep="first")

output_csv.parent.mkdir(parents=True, exist_ok=True)
result.to_csv(output_csv, index=False)

# Save metadata sidecar
conditions = {col: parse_sample_meta(col)["condition"] for col in sample_cols}
timepoints = {col: parse_sample_meta(col)["timepoint_h"] for col in sample_cols}
source_counts = result["source"].value_counts().to_dict() if "source" in result.columns else {}
meta_out = {
    "sample_columns": sample_cols,
    "conditions":     conditions,
    "timepoints":     timepoints,
    "n_features":     int(len(result)),
    "feature_sources": source_counts,
}
meta_path = str(output_csv).replace(".csv", "_metadata.json")
with open(meta_path, "w") as f:
    json.dump(meta_out, f, indent=2)

print(f"Saved {len(result)} features x {len(sample_cols)} samples -> {output_csv}")
''')

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idataset.external, "/jgi_data")],
        cmd=f"python {script} /jgi_data {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_ft: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dataset,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)

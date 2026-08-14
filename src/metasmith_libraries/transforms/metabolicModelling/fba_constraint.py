from metasmith.python_api import *
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image    = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
diff_dir = model.AddRequirement(lib.GetType("metabolomics::metabolomics_differential"))
sbml     = model.AddRequirement(lib.GetType("metabolomics::metabolic_model_sbml"))
out_dir  = model.AddProduct(lib.GetType("metabolomics::metabolomics_fba_results"))


def protocol(context: ExecutionContext):
    idiff  = context.Input(diff_dir)
    isbml  = context.Input(sbml)
    iout   = context.Output(out_dir)

    script = Path("fba_constraint_script.py")
    script.write_text('''
import sys
import json
import warnings
import numpy as np
import pandas as pd
from pathlib import Path

diff_dir  = Path(sys.argv[1])
sbml_path = Path(sys.argv[2])
out_dir   = Path(sys.argv[3])
out_dir.mkdir(parents=True, exist_ok=True)

import cobra

# -------------------------------------------------------------------
# 1. Load template GSMM
# -------------------------------------------------------------------
model = cobra.io.read_sbml_model(str(sbml_path))
print(f"Loaded model: {model.id} with {len(model.reactions)} reactions, "
      f"{len(model.metabolites)} metabolites")

# -------------------------------------------------------------------
# 2. Build KEGG -> model metabolite mapping via annotations
# -------------------------------------------------------------------
kegg_to_met = {}
for met in model.metabolites:
    for db, ids in met.annotation.items():
        if "kegg" in db.lower():
            if isinstance(ids, str):
                ids = [ids]
            for kid in ids:
                kid_clean = kid.split("/")[-1] if "/" in kid else kid
                kegg_to_met.setdefault(kid_clean, []).append(met.id)

# -------------------------------------------------------------------
# 3. Load differential results per condition
# -------------------------------------------------------------------
condition_sig = {}  # condition_str -> DataFrame of significant metabolites

for csv_path in sorted(diff_dir.glob("*_vs_*.csv")):
    if csv_path.name == "all_comparisons_summary.csv":
        continue
    df = pd.read_csv(csv_path)
    kegg_col = next((c for c in df.columns if c.lower() in {"kegg_id", "kegg"}), None)
    if kegg_col is None:
        continue
    sig = df[df.get("significant", False) == True].copy()
    if sig.empty:
        continue
    # Extract condition from filename (e.g. 8_vs_0h_t24h -> 8)
    cond = csv_path.stem.split("_vs_")[0]
    if cond not in condition_sig:
        condition_sig[cond] = []
    condition_sig[cond].append(sig)

# Merge per condition across timepoints
cond_data = {}
for cond, dfs in condition_sig.items():
    merged = pd.concat(dfs, ignore_index=True)
    kegg_col = next((c for c in merged.columns if c.lower() in {"kegg_id", "kegg"}), None)
    if kegg_col:
        cond_data[cond] = merged

if not cond_data:
    print("No significant metabolites with KEGG IDs found; running unconstrained FBA only.")

# -------------------------------------------------------------------
# 4. Run pFBA per condition
# -------------------------------------------------------------------
all_fluxes = {}

# Baseline (unconstrained)
with model as m:
    sol = cobra.flux_analysis.pfba(m)
    if sol.status == "optimal":
        all_fluxes["baseline"] = sol.fluxes

for cond, sig_df in cond_data.items():
    kegg_col = next((c for c in sig_df.columns if c.lower() in {"kegg_id", "kegg"}), None)
    with model as m:
        constrained = 0
        for _, row in sig_df.iterrows():
            kid = str(row[kegg_col])
            if kid not in kegg_to_met:
                continue
            fc = row.get("log2FC", 0)
            model_mets = kegg_to_met[kid]
            for met_id in model_mets:
                # Find exchange reactions involving this metabolite
                met_obj = m.metabolites.get_by_id(met_id) if met_id in [x.id for x in m.metabolites] else None
                if met_obj is None:
                    continue
                for rxn in met_obj.reactions:
                    if rxn.id.startswith("EX_"):
                        if fc < -1:
                            # Depleted: restrict uptake (make lower bound less negative)
                            rxn.lower_bound = max(rxn.lower_bound, rxn.lower_bound * 0.5)
                            constrained += 1
                        elif fc > 1:
                            # Accumulated: restrict secretion or expand
                            rxn.upper_bound = min(rxn.upper_bound * 1.5, 1000)
                            constrained += 1

        print(f"Condition {cond}: constrained {constrained} exchange reaction bounds")
        try:
            sol = cobra.flux_analysis.pfba(m)
            if sol.status == "optimal":
                all_fluxes[cond] = sol.fluxes
            else:
                print(f"  Warning: {cond} solution status = {sol.status}")
        except Exception as e:
            print(f"  Error in pFBA for {cond}: {e}")

# -------------------------------------------------------------------
# 5. Output flux table
# -------------------------------------------------------------------
if all_fluxes:
    flux_df = pd.DataFrame(all_fluxes)
    flux_df.index.name = "reaction"
    flux_df.to_csv(out_dir / "condition_fluxes.csv")

    # Compute fold-change relative to baseline
    if "baseline" in all_fluxes:
        base = all_fluxes["baseline"]
        fc_records = []
        for cond in [c for c in all_fluxes if c != "baseline"]:
            for rxn_id in base.index:
                b_val = base[rxn_id]
                c_val = all_fluxes[cond].get(rxn_id, 0)
                if abs(b_val) > 1e-9:
                    fc = c_val / b_val
                else:
                    fc = np.nan
                fc_records.append({"reaction": rxn_id, "condition": cond,
                                   "baseline_flux": b_val, "condition_flux": c_val,
                                   "fold_change": fc})
        if fc_records:
            fc_df = pd.DataFrame(fc_records)
            fc_df.to_csv(out_dir / "flux_fold_changes.csv", index=False)

    # Escher-compatible JSON with flux data per condition
    escher_data = {}
    for cond, fluxes in all_fluxes.items():
        escher_data[cond] = {rxn: float(val) for rxn, val in fluxes.items() if abs(val) > 1e-9}
    with open(out_dir / "escher_flux_overlay.json", "w") as f:
        json.dump(escher_data, f, indent=2)

    print(f"FBA complete. {len(all_fluxes)} conditions, {len(flux_df)} reactions.")
else:
    print("No FBA solutions obtained.")

print(f"Outputs in {out_dir}")
''')

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (idiff.external, "/diff_data"),
            (isbml.external.parent, "/model_dir"),
        ],
        cmd=f"python {script} /diff_data /model_dir/{isbml.external.name} {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_dir: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=diff_dir,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

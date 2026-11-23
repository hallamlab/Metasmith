from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::dram.env"))
sheets = model.AddProduct(lib.GetType("ref::dram_sheets"))

# The forms of the DRAM release dram.env runs, by tag: DRAM-setup.py fetches `master`,
# which is whatever the repository holds on the day.
BASE_URL = "https://raw.githubusercontent.com/WrightonLabCSU/DRAM/v1.5.0/data"
SHEETS = ("genome_summary_form", "module_step_form", "etc_module_database",
          "function_heatmap_form", "amg_database")


def protocol(context: ExecutionContext):
    isheets = context.Output(sheets)
    staged = context.StagingDir()
    base = context.params.get("dram_sheets_base_url", BASE_URL).rstrip("/")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{name}.tsv", staged.local/f"{name}.tsv", label=f"DRAM {name}")
          for name in SHEETS),
        f"mkdir -p {isheets.local}",
        *(f"mv {staged.local}/{name}.tsv {isheets.local}/" for name in SHEETS),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # A web page passes the fetch; every form is a TSV whose first column is named.
    def is_form(name):
        header = open(isheets.local/f"{name}.tsv", errors="replace").readline()
        return "\t" in header and "<html" not in header.lower()

    return ExecutionResult(
        manifest=[{sheets: isheets.local}],
        success=all(is_form(name) for name in SHEETS),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.01),  # five DRAM form sheets
    resources=Resources(
        cpus=1,
        memory=Size.GB(1),
        duration=Duration(hours=1),
    ),
)

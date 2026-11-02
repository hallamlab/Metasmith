import glob
import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::das_tool.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
mb      = model.AddRequirement(lib.GetType("e2::metabat2_contig_to_bin"), parents={asm})
sb      = model.AddRequirement(lib.GetType("e2::semibin2_contig_to_bin"), parents={asm})
cb      = model.AddRequirement(lib.GetType("e2::comebin_contig_to_bin"), parents={asm})
bins    = model.AddProduct(lib.GetType("e2::das_tool_bin"))
table   = model.AddProduct(lib.GetType("e2::das_tool_contig_to_bin"))
summary = model.AddProduct(lib.GetType("e2::das_tool_summary"))

# nf-core/mag 5.5.0 DASTOOL_DASTOOL, refine_bins_dastool_threshold 0.5. It passes no proteins, so
# DAS Tool calls genes itself.
ARGS = "--write_bins --write_unbinned --write_bin_evals --score_threshold 0.5"


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    iasm = context.Input(asm)
    sample = json.loads(Path(imeta.local).read_text())["sample"]
    cpus = context.params.get("cpus") or 1
    prefix = f"DASTool-{sample}"

    # The file names become DAS Tool's binner labels.
    tables = []
    for binner, dep in (("MetaBAT2", mb), ("SemiBin2", sb), ("COMEBin", cb)):
        local = Path(f"{binner}.tsv")
        local.write_bytes(context.Input(dep).local.read_bytes())
        tables.append(str(local))

    context.ExecWithEnv(env=image, cmd=f"""
        DAS_Tool {ARGS} -t {cpus} -i {",".join(tables)} -c {iasm.container} -o {prefix}
    """)

    bin_files = sorted(p for p in glob.glob(f"{prefix}_DASTool_bins/*.fa") if Path(p).stem != "unbinned")
    manifest = []
    for i, path in enumerate(bin_files):
        obin = context.Output(bins, i=i)
        obin.local.write_bytes(Path(path).read_bytes())
        manifest.append({bins: obin.local})
    otable = context.Output(table)
    otable.local.write_bytes(Path(f"{prefix}_DASTool_contig2bin.tsv").read_bytes())
    osummary = context.Output(summary)
    osummary.local.write_bytes(Path(f"{prefix}_DASTool_summary.tsv").read_bytes())

    return ExecutionResult(
        manifest=manifest + [{table: otable.local, summary: osummary.local}],
        success=len(manifest) > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=4)),
)

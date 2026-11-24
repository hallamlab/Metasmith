# das_tool.py over MetaBAT2, SemiBin2 and MetaDecoder in place of COMEBin: the same
# consolidator and products for a binner set without COMEBin's GPU or CPU-day cost.
# Both transforms produce the das_tool types; a template opts into this one by giving
# binning::lite_binner_set (see binning.yml).
import glob
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::das_tool.env"))
model.AddRequirement(lib.GetType("binning::lite_binner_set"))
asm    = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs   = model.AddRequirement(lib.GetType("sequences::assembly_orfs"), parents={asm})
TABLES = {
    "metabat2": model.AddRequirement(lib.GetType("binning::metabat2_contig_to_bin_table"), parents={asm}),
    "semibin2": model.AddRequirement(lib.GetType("binning::semibin2_contig_to_bin_table"), parents={asm}),
    "metadecoder": model.AddRequirement(lib.GetType("binning::metadecoder_contig_to_bin_table"), parents={asm}),
}

bin_fasta = model.AddProduct(lib.GetType("sequences::das_tool_bin_fasta"))
table     = model.AddProduct(lib.GetType("binning::das_tool_contig_to_bin_table"))
summary   = model.AddProduct(lib.GetType("binning::das_tool_summary"))


def _strip_our_header(src: Path, dst: Path) -> None:
    with open(src) as f:
        lines = f.readlines()
    if lines and lines[0].split("\t")[0].strip().lower() == "contig":
        lines = lines[1:]
    dst.write_text("".join(lines))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    iorfs = context.Input(orfs)

    threads = context.params.get("cpus", 16)
    outbase = "das_tool"

    clean = []
    for label, req in TABLES.items():
        path = Path(f"{label}.clean.tsv")
        _strip_our_header(context.Input(req).local, path)
        clean.append(str(path))

    _cmd = f"""
        ln -s "{iorfs.container}" proteins.faa
        DAS_Tool \
            -i {",".join(clean)} \
            -l {",".join(TABLES)} \
            -c {iasm.container} \
            -p proteins.faa \
            -o {outbase} \
            -t {threads} \
            --write_bins --write_bin_evals
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    contig2bin = Path(f"{outbase}_DASTool_contig2bin.tsv")
    summary_file = Path(f"{outbase}_DASTool_summary.tsv")
    bins_dir = Path(f"{outbase}_DASTool_bins")
    assert contig2bin.exists(), f"DAS_Tool wrote no {contig2bin}"
    assert summary_file.exists(), f"DAS_Tool wrote no {summary_file}"

    bin_files = sorted(glob.glob(f"{bins_dir}/*.fa"))
    Log.Info(f"DAS Tool consolidated {len(bin_files)} bins")

    outputs = []
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        out_bin.local.write_bytes(Path(bin_path).read_bytes())
        outputs.append({bin_fasta: out_bin.local})

    otable = context.Output(table)
    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        f.write(contig2bin.read_text())

    osummary = context.Output(summary)
    osummary.local.write_bytes(summary_file.read_bytes())

    return ExecutionResult(
        manifest=outputs + [{table: otable.local, summary: osummary.local}],
        success=len(outputs) > 0 and otable.local.exists() and osummary.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)

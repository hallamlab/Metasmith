# DAS Tool is batch 2's consolidator across MetaBAT2, SemiBin2 and COMEBin,
# standing where MetaWRAP's bin_refinement stands for batch 1's MetaBAT2/
# MaxBin2/CONCOCT trio. It needs none of the three binners' own bin FASTAs --
# `-i` takes each binner's contig2bin TABLE and `-c` the whole assembly, and
# DAS_Tool cuts its own winning bins out of that assembly itself. Its
# `Fasta_to_Contig2Bin` helper (bin fastas -> table) is therefore not wired
# in here: this library already produces the tables directly.
#
# It also takes `-p` externally-predicted proteins in prodigal's own fasta
# format (>contigID_geneNo), which is exactly what transforms/metagenomics/
# prodigal.py's sequences::orfs already is -- checked by running pprodigal on
# a synthetic assembly and comparing the header shape against DAS_Tool's own
# `--help` and its DAS_Tool.R (`contig_id := gsub('_[0-9]+$','', protein_id)`).
# So gene prediction is not duplicated at runtime: this transform skips
# DAS_Tool's own prodigal call by handing it prodigal.py's output.
import glob
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# The image bundles the SCG reference (db/{bac,arc}.{all,scg}.faa + .scg.lookup,
# ~83 MB) at its --dbDirectory default, so no separate reference input exists
# to require. See resources/env/das_tool.env for what was checked.
image    = model.AddRequirement(lib.GetType("env::das_tool.env"))
asm      = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
mb_table = model.AddRequirement(lib.GetType("binning::metabat2_contig_to_bin_table"), parents={asm})
sb_table = model.AddRequirement(lib.GetType("binning::semibin2_contig_to_bin_table"), parents={asm})
cb_table = model.AddRequirement(lib.GetType("binning::comebin_contig_to_bin_table"), parents={asm})

bin_fasta = model.AddProduct(lib.GetType("sequences::das_tool_bin_fasta"))
table     = model.AddProduct(lib.GetType("binning::das_tool_contig_to_bin_table"))
summary   = model.AddProduct(lib.GetType("binning::das_tool_summary"))


def _strip_our_header(src: Path, dst: Path) -> None:
    # metabat2.py and (empirically) SemiBin2 itself both write a literal
    # "contig\tbin" header row; comebin_res.tsv has none. DAS_Tool's own
    # reader is unconditionally headerless (`fread(..., header=F)`) and takes
    # row one as data, so an un-stripped header becomes a phantom contig
    # literally named "contig" -- and DAS_Tool halts rather than ignoring it:
    # "Error: Contigs of contig2bin files not found in assembly: 2 / contig, /
    # contig", reproduced against real fir output before this check existed.
    with open(src) as f:
        lines = f.readlines()
    if lines and lines[0].split("\t")[0].strip().lower() == "contig":
        lines = lines[1:]
    dst.write_text("".join(lines))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    iorfs = context.Input(orfs)
    imb = context.Input(mb_table)
    isb = context.Input(sb_table)
    icb = context.Input(cb_table)

    threads = context.params.get("cpus", 16)
    outbase = "das_tool"

    clean_mb = Path("metabat2.clean.tsv")
    clean_sb = Path("semibin2.clean.tsv")
    clean_cb = Path("comebin.clean.tsv")
    _strip_our_header(imb.local, clean_mb)
    _strip_our_header(isb.local, clean_sb)
    _strip_our_header(icb.local, clean_cb)

    _cmd = f"""
        DAS_Tool \
            -i {clean_mb},{clean_sb},{clean_cb} \
            -l metabat2,semibin2,comebin \
            -c {iasm.container} \
            -p {iorfs.container} \
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

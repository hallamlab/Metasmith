import glob
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::metabat2.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
bin_fasta   = model.AddProduct(lib.GetType("sequences::metabat2_bin_fasta"))
table       = model.AddProduct(lib.GetType("binning::metabat2_contig_to_bin_table"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)

    threads = context.params.get('cpus', 8)
    depth_file = "depth.txt"
    bin_dir = "metabat_bins"
    bin_prefix = f"{bin_dir}/bin"

    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            mkdir -p {bin_dir}
            jgi_summarize_bam_contig_depths --outputDepth {depth_file} {ibam.container}
            metabat2 -i {iasm.container} -a {depth_file} -o {bin_prefix} -t {threads}
        """
    )

    # Find all bin files and output each one separately
    outputs = []
    bin_files = sorted(glob.glob(f"{bin_dir}/*.fa"))

    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

    # Generate contig-to-bin table from bin files
    otable = context.Output(table)
    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        for bin_path in bin_files:
            bin_name = Path(bin_path).stem
            with open(bin_path) as bf:
                for line in bf:
                    if line.startswith(">"):
                        contig = line[1:].strip().split()[0]
                        f.write(f"{contig}\t{bin_name}\n")

    return ExecutionResult(
        manifest=outputs + [{table: otable.local}],
        success=len(outputs) > 0 and otable.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    )
)

import glob
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::semibin.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
bin_fasta   = model.AddProduct(lib.GetType("sequences::semibin2_bin_fasta"))
table       = model.AddProduct(lib.GetType("binning::semibin2_contig_to_bin_table"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)

    threads = context.params.get('cpus', 8)
    workdir = "semibin_out"

    # Use global environment model (works for most samples)
    environment = "global"

    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            export PATH=/opt/conda/bin:$PATH
            SemiBin2 single_easy_bin \
                -i {iasm.container} \
                -b {ibam.container} \
                -o {workdir} \
                --environment {environment} \
                -t {threads}
        """
    )

    # Find all bin files and output each one separately
    outputs = []
    bin_files = sorted(glob.glob(f"{workdir}/output_bins/*.fa.gz") + glob.glob(f"{workdir}/output_bins/*.fa"))

    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        # Decompress if gzipped, otherwise just copy
        if bin_path.endswith('.gz'):
            context.LocalShell(f"gunzip -c {bin_path} > {out_bin.local}")
        else:
            context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

    # Copy contig-to-bin table
    otable = context.Output(table)
    context.LocalShell(f"cp {workdir}/contig_bins.tsv {otable.local}")

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
        duration=Duration(hours=4),
    )
)

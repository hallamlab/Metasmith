import glob
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::metadecoder.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
bin_fasta   = model.AddProduct(lib.GetType("sequences::metadecoder_bin_fasta"))
table       = model.AddProduct(lib.GetType("binning::metadecoder_contig_to_bin_table"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)

    threads = context.params.get('cpus', 8)
    seed = context.params.get('metadecoder_seed', 1)
    prefix = "metadecoder_bins/METADECODER"

    # `metadecoder coverage` writes its index as <bam>.index beside the path it is given;
    # through a link here, so it lands in this work dir and not beside the cached BAM.
    _cmd = f"""
            ln -s "{ibam.container}" sample.bam
            mkdir -p metadecoder_bins
            metadecoder coverage -b sample.bam -o metadecoder.coverage --threads {threads}
            metadecoder seed -f {iasm.container} -o metadecoder.seed --threads {threads}
            metadecoder cluster -f {iasm.container} -c metadecoder.coverage -s metadecoder.seed \
                -o {prefix} --random_number {seed} --threads {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    outputs = []
    bin_files = sorted(glob.glob(f"{prefix}.*.fasta"))

    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

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
        duration=Duration(hours=4),
    )
)

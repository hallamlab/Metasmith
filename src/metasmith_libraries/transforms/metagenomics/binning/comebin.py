import os
import glob
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::comebin.env"))
cpu_patch   = model.AddRequirement(lib.GetType("lib::comebin_cpu"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
bin_fasta   = model.AddProduct(lib.GetType("sequences::comebin_bin_fasta"))
table       = model.AddProduct(lib.GetType("binning::comebin_contig_to_bin_table"))


# The patch's sitecustomize keeps the bin step's Leiden sweep from hanging on any device. Without a GPU it also
# spreads training, which uses one core of many, across them, and its dropout pool needs OpenMP's idle workers
# parked rather than spinning on the same cores: at 48 threads a step took 1.26 s spinning and 0.79 s parked.
def _comebin_patch(icpu_patch):
    env = f"""
            export PYTHONPATH={icpu_patch.container}${{PYTHONPATH:+:$PYTHONPATH}}"""
    if os.environ.get("CUDA_VISIBLE_DEVICES", ""):
        return env
    return env + """
            export OMP_WAIT_POLICY=PASSIVE"""


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    icpu_patch = context.Input(cpu_patch)

    threads = context.params.get('cpus', 8)
    workdir = "comebin_out"
    bam_dir = "bam_input"

    # A private copy, because COMEBin writes its fragments and seeds beside the
    # contig file it is given, and that file is the assembler's cached product.
    contigs = "contigs.fa"
    if context.LocalShell(
        f"cp -L {iasm.local} {contigs}\n"
        "awk '/^>/{if(l>=1000)n++; l=0; next}{l+=length($0)}"
        f"END{{if(l>=1000)n++; print n+0}}' {contigs} > usable_contig_count.txt"
    ) != 0:
        return ExecutionResult(manifest=[], success=False)
    usable_contigs = int(Path("usable_contig_count.txt").read_text().strip())
    if usable_contigs < 2:
        return ExecutionResult(manifest=[], success=False)
    batch_size = min(usable_contigs, 1024)

    _cmd = f"""{_comebin_patch(icpu_patch)}
            mkdir -p {bam_dir}
            ln -s "{ibam.container}" {bam_dir}/
            mkdir -p {workdir}
            run_comebin.sh -a {contigs} -o {workdir} -p {bam_dir} -t {threads} -b {batch_size}
        """
    context.ExecWithEnv(
            env=image,
            args=[
                "--nv",
                "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}",
            ],
            cmd=_cmd,
        )

    sweep = Path(f"{workdir}/comebin_res/leiden_sweep.state")
    if sweep.exists() and sweep.read_text().strip() == "running":
        Log.Error("COMEBin's bin step died mid-sweep, most likely out of memory; failing the step so a retry gets more")
        return ExecutionResult(manifest=[], success=False, exit_code=137)

    outputs = []
    bin_dir = f"{workdir}/comebin_res/comebin_res_bins"
    bin_files = sorted(glob.glob(f"{bin_dir}/*.fa"))
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

    otable = context.Output(table)
    context.LocalShell(f"cp {workdir}/comebin_res/comebin_res.tsv {otable.local}")

    return ExecutionResult(
        manifest=outputs + [{table: otable.local}],
        success=len(outputs) > 0 and otable.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # The CPU lane as sized for the CAMI marine samples: 48 cores for allocation
    # efficiency, fir's 4 GB per core, and 3 days because at 96 cores training alone
    # took 6 h and at half the cores the slow tail lands near 20 h.
    resources=Resources(
        cpus=48,
        memory=Size.GB(192),
        duration=Duration(hours=72),
    )
)

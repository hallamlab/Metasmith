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

    # --seed is set unconditionally, and that is a correctness fix rather than a tuning
    # choice: metabat2's own default is `--seed 0`, which its help documents as "use
    # random seed", so without this the SAME inputs give DIFFERENT bins run to run. A
    # benchmark arm whose binning is not reproducible cannot be compared with anything,
    # including itself.
    #
    # --minContig is read from params with metabat2's OWN default preserved, so nothing
    # outside a caller that asks is affected. It exists because nf-core/mag pins
    # `-m 1500` (conf/modules.config) while metabat2 defaults to 2500, and a
    # tool-for-tool comparison against that pipeline with an unstated 1000 bp difference
    # in which contigs are binnable at all is not a comparison. Callers doing parity
    # against nf-core/mag must pass 1500.
    #
    # CAUTION both reads below were INERT until bootstrap.py was taught to load
    # workflow.params.yml. `RunWorkflow(params=...)` reached nextflow only, and
    # `context.params` was built purely from `.command.metadata`, which carries just
    # res/gpu/rootfs. A driver that passed `metabat2_min_contig=1500` got 2500 anyway,
    # and its source plainly showed the pin.
    #
    # The reason nobody caught it is the shape to remember: `--seed` read CORRECTLY
    # because the default here (1) happens to equal what the caller pinned (1), so its
    # log line was indistinguishable from a delivered pin, and nothing prompted a look
    # at --minContig. A `.get(key, default)` whose default equals the intended value
    # cannot be used as evidence that the channel works. Assert the value in the
    # rendered command instead.
    seed = context.params.get('metabat2_seed', 1)
    min_contig = context.params.get('metabat2_min_contig', 2500)

    _cmd = f"""
            mkdir -p {bin_dir}
            jgi_summarize_bam_contig_depths --outputDepth {depth_file} {ibam.container}
            metabat2 -i {iasm.container} -a {depth_file} -o {bin_prefix} -t {threads} \
                --seed {seed} --minContig {min_contig}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    outputs = []
    bin_files = sorted(glob.glob(f"{bin_dir}/*.fa"))

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
        duration=Duration(hours=2),
    )
)

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

BYTES_PER_PAIR = 6

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)

    threads = context.params.get('cpus', 8)
    workdir = "semibin_out"

    # SemiBin2's built-in model. `global` is also nf-core/mag's own default
    # (`semibin_environment`), so this matches rather than deviates.
    environment = "global"

    # --random-seed is set unconditionally, and like metabat2's --seed this is a
    # correctness fix rather than a tuning choice: SemiBin2's own help says "the default
    # is that the seed is set by the system", so without this the SAME inputs give
    # DIFFERENT bins run to run, and a benchmark arm whose binning is not reproducible
    # cannot be compared with anything, including itself. nf-core/mag pins
    # `--random-seed 1` (conf/modules.config, `semibin_rng_seed` default 1), so 1 is both
    # the reproducible choice and the parity-matching one.
    seed = context.params.get('semibin2_seed', 1)

    # --min-len is passed when a caller asks, or when the assembly is too large to bin at
    # SemiBin2's own floor (below). Left unset, SemiBin2 derives its floor from
    # `--ratio` (0.05, relative to 2500 bp) rather than from a fixed length; nf-core/mag
    # instead passes `--min-len ${params.min_contig_size}`, i.e. 1500. Those are different
    # rules, not merely different numbers, so a parity caller must pass 1500 explicitly.
    min_len = context.params.get('semibin2_min_len')

    # Single-sample SemiBin2 holds a dense N x N matrix over the contigs it keeps, so
    # its memory grows with the square of the count: a 16 GB and then a 32 GB request
    # were OOM-killed 20 minutes in. SemiBin 2.1.0 builds it from float32 depths
    # (cal_kl), 4 bytes a pair. Measured on a CAMI marine megahit assembly at 19.6k, 40.0k
    # and 72.1k contigs, the peak fits 1.8 GB + 4.06 bytes a pair, so 6 with no intercept
    # stays above it with a third to spare at the size where the check matters. A large
    # assembly drops to the 2500 bp floor SemiBin2 would itself pick at a high --ratio,
    # and one that cannot fit even then fails at once as an OOM, so the retry asks for
    # more memory instead of paying 20 minutes to learn the same thing.
    floor = int(min_len) if min_len else 1000
    if context.LocalShell(
        f"awk -v f={floor} '/^>/{{if(l>=f)a++; if(l>=2500)b++; l=0; next}}{{l+=length($0)}}"
        f"END{{if(l>=f)a++; if(l>=2500)b++; print a+0, b+0}}' {iasm.local} > contig_counts.txt"
    ) != 0:
        return ExecutionResult(manifest=[], success=False)
    n, n2500 = (int(x) for x in Path("contig_counts.txt").read_text().split())
    budget = (context.params.get('memory') or 64) * 1024**3
    if not min_len and BYTES_PER_PAIR * n**2 > budget:
        Log.Info(f"{n} contigs of 1000 bp or more need ~{BYTES_PER_PAIR*n**2/1024**3:.0f} GB; binning the {n2500} of 2500 bp or more")
        min_len, n = 2500, n2500
    if BYTES_PER_PAIR * n**2 > budget:
        Log.Error(f"{n} contigs need ~{BYTES_PER_PAIR*n**2/1024**3:.0f} GB in SemiBin2's pairwise matrix, more than the {budget/1024**3:.0f} GB requested")
        return ExecutionResult(manifest=[], success=False, exit_code=137)
    min_len_arg = f" --min-len {min_len}" if min_len else ""

    _cmd = f"""
            export PATH=/opt/conda/bin:$PATH
            SemiBin2 single_easy_bin \
                -i {iasm.container} \
                -b {ibam.container} \
                -o {workdir} \
                --environment {environment} \
                --random-seed {seed}{min_len_arg} \
                -t {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    outputs = []
    bin_files = sorted(glob.glob(f"{workdir}/output_bins/*.fa.gz") + glob.glob(f"{workdir}/output_bins/*.fa"))

    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        if bin_path.endswith('.gz'):
            context.LocalShell(f"gunzip -c {bin_path} > {out_bin.local}")
        else:
            context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

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
        memory=Size.GB(64),
        duration=Duration(hours=4),
    )
)

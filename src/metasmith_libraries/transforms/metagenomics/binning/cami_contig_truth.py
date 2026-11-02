# CAMI's own binning_gs.tsv/gsa_mapping.tsv.gz key on the CAMI-provided gold
# standard assembly's own contig ids (e.g. S9C933009), not on whatever a
# participant's own assembler emits (e.g. megahit's k141_N). A
# contig_to_bin_table built on our own assembly cannot be scored against that
# file directly -- the SEQUENCEID columns share no id space and amber.py would
# join on nothing. This transform builds the gold standard amber.py actually
# needs: for each of our OWN contigs, majority-vote the true genome of the
# reads alignment::bam mapped there, using CAMISIM's per-read truth table
# (cami_read_truth). See resources/lib/cami_gold_standard.py for the vote.
from pathlib import Path
from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()

image_sam  = model.AddRequirement(lib.GetType("env::samtools.env"))
image_pl   = model.AddRequirement(lib.GetType("env::polars.env"))
script     = model.AddRequirement(lib.GetType("lib::cami_gold_standard.py"))
meta       = model.AddRequirement(lib.GetType("sequences::read_metadata"))
truth      = model.AddRequirement(lib.GetType("binning::cami_read_truth"), parents={meta})
asm        = model.AddRequirement(lib.GetType("sequences::assembly"))
bam        = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
out        = model.AddProduct(lib.GetType("binning::contig_gold_standard_table"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    itruth = context.Input(truth)
    iscript = context.Input(script)
    iout = context.Output(out)

    sample_id = Path(iasm.local).stem
    threads = context.params.get("cpus", 4)

    read_contig_file = "read_contig.tsv"
    _cmd = f"""
            samtools view -@ {threads} -F 0x904 {ibam.container} \
                | cut -f1,3 > {read_contig_file}
        """
    context.ExecWithEnv(env=image_sam, cmd=_cmd)

    Log.Info("measuring contig lengths from the assembly")
    contig_lengths_file = "contig_lengths.tsv"
    contig2length = {}
    with open(iasm.local) as fa:
        current = None
        length = 0
        def _submit():
            if current is not None:
                contig2length[current] = length
        for l in fa:
            if l[0] == ">":
                _submit()
                current = l[1:-1].split(" ")[0]
                length = 0
            else:
                length += len(l) - 1
        _submit()
    with open(contig_lengths_file, "w") as f:
        for contig, length in contig2length.items():
            f.write(f"{contig}\t{length}\n")

    _cmd = f"""
            python {iscript.container} {read_contig_file} {itruth.container} \
                {contig_lengths_file} {sample_id} {iout.container}
        """
    context.ExecWithEnv(env=image_pl, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)

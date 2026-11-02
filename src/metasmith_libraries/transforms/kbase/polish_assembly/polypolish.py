from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polypolish.env"))
# The alignments polypolish consumes are bwa's, and the polypolish image carries
# polypolish alone. bwa comes in as a second environment rather than as its own
# transform because `bwa mem -a` emits every alignment of every read -- gigabytes
# of SAM that exist only to be filtered here, and that no other transform wants.
bwa_env = model.AddRequirement(lib.GetType("env::bwa.env"))
# The reads and the assembly are SIBLINGS, both descended from the isolate's read
# metadata -- which is the idiom `assembly/assembly_stats.py` and `assembly/megahit.py`
# already use to say "these came from the same sample". Hanging the reads off the
# assembly instead makes this transform unreachable: no read set in this library
# descends from an assembly, so the requirement has no candidates and the target is
# dropped with no plan at all. See research/kbase/curation/r4/analyses.md.
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={meta})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("sequences::polished_assembly"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    ireads=context.Input(reads)
    iout=context.Output(out)
    parity = json.loads(context.Input(meta).local.read_text())["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"

    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads

    # The assembly is copied before indexing because `bwa index` writes its five index
    # files beside the fasta, which for a staged input is a directory shared with every
    # other task reading it. `gzip -dcf` covers both the gzipped form the cleaners emit
    # and a plain fastq.
    _cmd = f"""\
            cp {iasm.container} ref.fa
            bwa index ref.fa
        """
    if parity == "paired":
        # polypolish reads the two directions separately -- `filter` keeps an alignment
        # by its mate's placement -- and this library's paired clean_short_reads are
        # interleaved.
        _cmd += f"""\
            gzip -dcf {ireads.container} \
                | awk '{{ if (int((NR-1)/4) % 2 == 0) print > "r1.fastq"; else print > "r2.fastq" }}'
            bwa mem -a -t {threads} ref.fa r1.fastq > a1.sam
            bwa mem -a -t {threads} ref.fa r2.fastq > a2.sam
        """
        polish = f"""\
            polypolish filter --in1 a1.sam --in2 a2.sam --out1 f1.sam --out2 f2.sam
            polypolish polish ref.fa f1.sam f2.sam > {iout.container}
        """
    else:
        _cmd += f"""\
            bwa mem -a -t {threads} ref.fa {ireads.container} > a.sam
        """
        polish = f"""\
            polypolish polish ref.fa a.sam > {iout.container}
        """
    context.ExecWithEnv(env=bwa_env, cmd=_cmd)
    context.ExecWithEnv(env=image, cmd=polish)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)

from metasmith.python_api import *

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
    # STUB. The protocol this replaces:
    #   bwa index {iasm.container}
    #   bwa mem -a -t $cpus {iasm.container} R1.fastq > a1.sam
    #   bwa mem -a -t $cpus {iasm.container} R2.fastq > a2.sam
    #   polypolish filter --in1 a1.sam --in2 a2.sam --out1 f1.sam --out2 f2.sam
    #   polypolish polish {iasm.container} f1.sam f2.sam > {iout.container}
    #
    # CAUTION polypolish needs ALL alignments per read (`bwa mem -a`), which is the whole
    # point of it, and the biocontainer carries polypolish alone. Whoever writes this body
    # decides whether bwa comes in as a second env requirement or the alignment step
    # becomes its own transform.
    made = {out: context.Output(out)}
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = set()

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

import json

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::subread.env"))
sam   = model.AddRequirement(lib.GetType("env::samtools.env"))
exp   = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
meta  = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={exp})
bam   = model.AddRequirement(lib.GetType("transcriptomics::bowtie2_bam"), parents={meta})
gff   = model.AddRequirement(lib.GetType("sequences::gff"), parents={exp})
out   = model.AddProduct(lib.GetType("isolate_rnaseq::featurecounts_sweep"))


def sample_of(context, item):
    src = context.SourceOf(item, meta)
    assert src is not None, f"no read_metadata in the lineage of [{item.local.name}]"
    return json.loads(src.local.read_text())["sample"]


def protocol(context: ExecutionContext):
    bams = {sample_of(context, b): b for b in context.InputGroup(bam)}
    order = sorted(bams)
    iout = context.Output(out)
    iout.local.mkdir(parents=True, exist_ok=True)
    (iout.local / "samples.tsv").write_text("\n".join(order) + "\n")
    threads = context.params.get("cpus") or 8

    # featureCounts pairs mates by holding reads until their mate arrives, which on a
    # coordinate-sorted BAM of this depth took 50 min per run. Sorting by name once first
    # makes each of the six runs a few minutes.
    context.ExecWithEnv(env=sam, cmd="\n".join(
        f"samtools sort -n -@ {threads} -m 2G -T nsort_{s} -o {s}.name.bam {bams[s].container}" for s in order))
    files = " ".join(f"{s}.name.bam" for s in order)

    # bowtie2 sets no NH tag, so featureCounts cannot see a multi-mapped read as one: under
    # -Q 0 it lands on whichever copy bowtie2 picked, and under -Q 10 it is dropped. Both are
    # kept so the next step can tell a silent gene from a duplicated one.
    context.ExecWithEnv(env=image, cmd=f"""\
        for s in 0 1 2; do for q in 0 10; do
            featureCounts -T {threads} -p --countReadPairs -B -C -s $s -Q $q \\
                -t gene -g locus_tag --extraAttributes gene_biotype,Name \\
                -a {context.Input(gff).container} -o {iout.container}/s${{s}}_Q${{q}}.txt {files}
        done; done
    """)
    context.LocalShell(" ".join(["rm -f"] + [f"{s}.name.bam" for s in order]))
    made = [iout.local / f"s{s}_Q{q}.txt.summary" for s in (0, 1, 2) for q in (0, 10)]
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all(p.exists() for p in made),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=8)),
)

# MetaPop over one study, Pratama's call `metapop --min_cov 70`: every sample's clean reads mapped to the
# study's vOTU representatives (the first column of the vOTU membership table, cut from the frozen set).
# Pratama names no mapper, so bowtie2 maps with defaults and MetaPop's own filters apply after it.
# Mean π over 100 vOTUs x 1,000 subsamplings is computed after the run from the microdiversity table.
import gzip
import json
import tarfile
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::metapop.env"))
envimg  = model.AddRequirement(lib.GetType("bench::metapop_env_image"))
study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
# No read_metadata slot: every sample's metadata file holds the same text, so the grouped task would stage
# several files under one content-hash name, and nextflow aborts the run on the collision.
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={pair})
frozen  = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
votus   = model.AddRequirement(lib.GetType("viromics::votu_cluster_table"), parents={frozen})
micro   = model.AddProduct(lib.GetType("bench::metapop_microdiversity"))
results = model.AddProduct(lib.GetType("bench::metapop_results"))


def sample_label(read_pair: Path) -> str:
    # A pool given's file is named for its content hash; the sample id is the file's text.
    try:
        text = read_pair.read_text().strip()
    except UnicodeDecodeError:
        return read_pair.stem
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = text
    return value if isinstance(value, str) and value and "\n" not in value else read_pair.stem


def is_interleaved(reads: Path) -> bool:
    """bbduk writes a paired sample interleaved: records 1 and 2 then share a read name."""
    with gzip.open(reads, "rt") as f:
        names = [line.split()[0].removesuffix("/1").removesuffix("/2")
                 for i, line in zip(range(8), f) if i % 4 == 0]
    return len(names) == 2 and names[0] == names[1]


def protocol(context: ExecutionContext):
    reps = {line.split("\t")[0] for line in Path(context.Input(votus).local).read_text().splitlines() if line}
    keep = False
    # MetaPop's --reference lists a directory of FASTA files; a file path fails in os.listdir.
    Path("ref").mkdir()
    with open(context.Input(frozen).local) as src, open("ref/votus.fna", "w") as dst:
        for line in src:
            if line.startswith(">"):
                keep = line[1:].split()[0] in reps
            if keep:
                dst.write(line)

    cpus = context.params.get("cpus") or 1
    steps = []
    for unit in context.InputGroup(reads):
        sample = sample_label(Path(context.SourceOf(unit, pair).local))
        mode = "--interleaved" if is_interleaved(unit.local) else "-U"
        steps.append(f"bowtie2 -p {cpus} -x idx {mode} {unit.container} --no-unal 2> bams/{sample}.log"
                     f" | samtools view -b -o bams/{sample}.bam -")
        steps.append(f"printf '%s\\t%s\\n' {sample} $(( $(zcat -f {unit.container} | wc -l) / 4 )) >> norm.tsv")

    omicro, oresults = context.Output(micro), context.Output(results)
    context.ExecWithEnv(
        env=image,
        binds=[(context.Input(envimg).external, "/opt/metapop:image-src=/")],
        cmd=f"""
            set -euo pipefail
            export PATH=/opt/metapop/bin:$PATH
            mkdir -p bams
            bowtie2-build --threads {cpus} ref/votus.fna idx > idx.log
            {chr(10).join(steps)}
            metapop --input_samples $PWD/bams --reference $PWD/ref --norm $PWD/norm.tsv \\
                --threads {cpus} --min_cov 70 --output $PWD/metapop
        """,
    )
    table = Path("metapop/MetaPop/10.Microdiversity/global_contig_microdiversity.tsv")
    if table.exists():
        omicro.local.write_bytes(table.read_bytes())
    else:
        Log.Warn("MetaPop wrote no microdiversity table: no vOTU passed its coverage filters")
        omicro.local.write_text("")
    with tarfile.open(oresults.local, "w:gz") as tar:
        tar.add("metapop", arcname="metapop")
        tar.add("norm.tsv", arcname="norm.tsv")
    return ExecutionResult(manifest=[{micro: omicro.local, results: oresults.local}], success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=48)),
)

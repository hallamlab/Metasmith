import re
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
img_sam = model.AddRequirement(lib.GetType("env::samtools.env"))
img_is  = model.AddRequirement(lib.GetType("env::instrain.env"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"))
magref  = model.AddRequirement(lib.GetType("binning::derep_mag_ref"))
out_profile = model.AddProduct(lib.GetType("annotation::instrain_profile"))
out_genome  = model.AddProduct(lib.GetType("annotation::instrain_genome_info"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    imagref = context.Input(magref)
    iprofile = context.Output(out_profile)
    igenome = context.Output(out_genome)

    threads = context.params.get("cpus", 8)

    sample_slug = re.sub(r"[^A-Za-z0-9]+", "_", Path(ireads.container).name).strip("_") or "sample"
    bam = f"{sample_slug}.bam"

    magref_dir = context.Mount(imagref.external, "/magref")
    context.ExecWithEnv(
        env=img_mm2,
        binds=[magref_dir],
        cmd=f"""
            zcat {ireads.container} | awk '{{ if (NR%8>=1 && NR%8<=4) print | "gzip > r1.fq.gz"; else print | "gzip > r2.fq.gz" }}'
            minimap2 -x sr -a -2 -t {threads} \
                {magref_dir}/mag_ref.mmi r1.fq.gz r2.fq.gz > temp.sam
            rm -f r1.fq.gz r2.fq.gz
        """,
    )

    context.ExecWithEnv(
        env=img_sam,
        cmd=f"""
            samtools view -@ {threads} -b temp.sam \
                | samtools sort -@ {threads} -o {bam} -O bam
            samtools index -@ {threads} {bam}
            rm -f temp.sam
        """,
    )

    context.ExecWithEnv(
        env=img_is,
        binds=[magref_dir],
        cmd=f"""
            inStrain profile \
                {bam} \
                {magref_dir}/mag_ref.fna \
                -o instrain_out \
                -p {threads} \
                -s {magref_dir}/mag_ref.stb \
                --database_mode \
                --skip_plot_generation
        """,
    )

    Path("instrain_out").rename(iprofile.local)
    gi = iprofile.local / "output" / "instrain_out_genome_info.tsv"
    # A completed database-mode profile always writes its genome table, even with nothing mapped.
    if not gi.exists():
        Log.Error(f"inStrain wrote no [{gi.name}]; the profile did not complete")
        return ExecutionResult(manifest=[], success=False)
    context.LocalShell(f"cp {gi} {igenome.local}")

    return ExecutionResult(
        manifest=[{out_profile: iprofile.local, out_genome: igenome.local}],
        success=iprofile.local.exists() and igenome.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)

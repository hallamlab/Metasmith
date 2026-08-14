"""instrain — inStrain profile of a sample against the shared derep-MAG reference.

Option (b) for cross-lake strain sharing: instead of profiling each sample against
its OWN assembly (which makes profiles incomparable), every sample's clean short
reads are mapped to ONE project-level dereplicated representative-MAG reference
(binning::derep_mag_ref — the is_centroid_95==1 bins, scaffolds namespaced by
bin_id) and profiled against it. The cross-sample `inStrain compare` step
(instrain_compare.py) then computes popANI / strain sharing over the common ref.

This transform folds the mapping in: minimap2 (-x sr) -> samtools sort/index ->
inStrain profile, so no intermediate BAM type is published. The reference's
scaffold->genome table (mag_ref.stb) partitions scaffolds into MAGs.
"""
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

    # inStrain `compare` names each profile by os.path.basename(bam_loc)
    # (compare_controller.py: `name = os.path.basename(ISP.get('bam_loc'))`), so a
    # bam named identically across samples makes every profile collide on the same
    # name and `inStrain compare` aborts with the issue-#79 duplicate-name assert.
    # Name the bam per-sample (from the reads filename, which carries SG<id>) so the
    # downstream compare gets a unique, meaningful label for each sample.
    sample_slug = re.sub(r"[^A-Za-z0-9]+", "_", Path(ireads.container).name).strip("_") or "sample"
    bam = f"{sample_slug}.bam"

    # 1) map the sample's reads to the shared derep-MAG reference (short-read
    #    preset, prebuilt single-part .mmi index). The clean reads are
    #    INTERLEAVED paired-end (R1,R2,R1,R2,...) and minimap2 has no interleaved
    #    mode, so deinterleave into R1/R2 first (one awk pass, streamed to gzip —
    #    no big temp file) and map paired so inStrain's read-pair filter applies.
    context.ExecWithEnv().ifContainerDo(
        env=img_mm2,
        binds=[(imagref.external, "/magref")],
        cmd=f"""
            zcat {ireads.container} | awk '{{ if (NR%8>=1 && NR%8<=4) print | "gzip > r1.fq.gz"; else print | "gzip > r2.fq.gz" }}'
            minimap2 -x sr -a -2 -t {threads} \
                /magref/mag_ref.mmi r1.fq.gz r2.fq.gz > temp.sam
            rm -f r1.fq.gz r2.fq.gz
        """,
    )

    # 2) SAM -> sorted+indexed BAM.
    context.ExecWithEnv().ifContainerDo(
        env=img_sam,
        cmd=f"""
            samtools view -@ {threads} -b temp.sam \
                | samtools sort -@ {threads} -o {bam} -O bam
            samtools index -@ {threads} {bam}
            rm -f temp.sam
        """,
    )

    # 3) inStrain profile vs the reference, partitioned into MAGs by the stb.
    context.ExecWithEnv().ifContainerDo(
        env=img_is,
        binds=[(imagref.external, "/magref")],
        cmd=f"""
            inStrain profile \
                {bam} \
                /magref/mag_ref.fna \
                -o instrain_out \
                -p {threads} \
                -s /magref/mag_ref.stb \
                --database_mode \
                --skip_plot_generation
        """,
    )

    Path("instrain_out").rename(iprofile.local)
    gi = iprofile.local / "output" / "instrain_out_genome_info.tsv"
    if gi.exists():
        context.LocalShell(f"cp {gi} {igenome.local}")
    else:
        Path(igenome.local).write_text("genome\tcoverage\tbreadth\tnucl_diversity\n")

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

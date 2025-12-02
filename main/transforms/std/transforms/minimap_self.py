from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

REF_HIFI    = "std::hifi_reads"
REF_NP      = "std::nanopore_reads"
REF_SR      = "std::short_reads"
for t in [REF_HIFI, REF_NP, REF_SR]:
    lib.GetType(t) # fail now if types are wrong, not during job

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_samtools"))
# out_sam         = model.AddProduct(lib.GetType("std::sequence_alignment_map"))
out_bam         = model.AddProduct(lib.GetType("std::self_mappings"))
out_bam_csi     = model.AddProduct(lib.GetType("std::self_mappings_csi"))

def protocol(context: ExecutionContext):
    reads_path     = context.Input(reads)
    temp_sam_path = Path("./alignments.sam")
    out_bam_path   = context.Output(out_bam)

    # https://lh3.github.io/minimap2/minimap2.html
    # minimap2 options:
    # -x sr                 short read preset
    # -x map-hifi           pacbio hifi (type of read that is more accurate) long read preset
    # -x map-ont            oxford nanopore long read preset
    # --sr                  Enable short-read alignment heuristics, more sensitivity
    # -2                    use two io threads, more peak memory
    # -a                    SAM format
    # --secondary=no        Whether to output secondary alignments [no]
    # --sam-hit-only        In SAM, don’t output unmapped reads. !this results in report saying 100% reads mapped!
    # --heap-sort=no|yes    Heap merge is faster for short reads, but slower for long reads. [no]
    #   Preset:
    #     -x STR       preset (always applied before other options; see minimap2.1 for details) []
    #                 - map-pb/map-ont: PacBio/Nanopore vs reference mapping
    #                 - ava-pb/ava-ont: PacBio/Nanopore read overlap
    #                 - asm5/asm10/asm20: asm-to-ref mapping, for ~0.1/1/5% sequence divergence
    #                 - splice: long-read spliced alignment
    #                 - sr: genomic short-read mapping
    reads_meta = context.GetMeta(reads)
    reads_type = reads_meta.endpoint
    presets = [ # order matters, first match is chosen
        (REF_HIFI,  "-x asm10"), # https://github.com/lh3/minimap2/issues/739, but shouldn't we use the more stringent divergence? (using 1% here)
        (REF_NP,    "-x map-ont"),
   ] 
    preset = "" # default
    for tname, p in presets:
        t = lib.GetType(tname)
        if not reads_type.IsA(t): continue
        preset = p
        Log.Info(f"selected preset for [{tname}]")
        break
    if preset == "": Log.Info(f"using default parameters")

    Log.Info("start minimap align")
    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"-t {cpus}"
    context.ExecWithContainer(
        image = image_minimap2,
        cmd = f"""
            minimap2 {preset} -a -2 {cpus_string} {reads_path.container} {reads_path.container} > {temp_sam_path}
        """
    )

    Log.Info("convert to BAM, sort and index")
    cpus_string = "" if cpus is None else f"-@ {cpus}"
    context.ExecWithContainer(
        image = image_samtools,
        cmd = f"""
            samtools view {cpus_string} -b {temp_sam_path} \
                | samtools sort {cpus_string} -o {out_bam_path.container} -O bam
            samtools index -c {out_bam_path.container}
        """
    )
    return ExecutionResult(success=context.Output(out_bam_csi).local.exists())

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    output_signature = {
        # out_sam:      "alignments.sam",
        out_bam:      "alignments.bam",
        out_bam_csi:  "alignments.bam.csi"
    },
)

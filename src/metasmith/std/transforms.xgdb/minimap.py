from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

REF_HIFI    = "std::hifi_reads"
REF_NP      = "std::nanopore_reads"
REF_SR      = "std::short_reads"
for t in [REF_HIFI, REF_NP, REF_SR]:
    lib.GetType(t)

reads           = model.AddRequirement(lib.GetType("std::reads"))
assembly        = model.AddRequirement(lib.GetType("std::assembly"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_samtools"))
out_bam         = model.AddProduct(lib.GetType("std::binary_alignment_map"))
out_bam_csi     = model.AddProduct(lib.GetType("std::binary_alignment_map_csi"))

def protocol(context: ExecutionContext):
    reads_path     = context.Input(reads)
    assembly_path  = context.Input(assembly)
    temp_sam_path = Path("./alignments.sam")
    out_bam_path   = context.Output(out_bam)

    reads_meta = context.GetMeta(reads)
    reads_type = reads_meta.endpoint
    presets = [
        (REF_SR,    "-x sr"),
        (REF_HIFI,  "-x asm10"),
        (REF_NP,    "-x map-ont"),
   ] 
    preset = ""
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
            minimap2 {preset} -a -2 {cpus_string} {assembly_path.container} {reads_path.container} > {temp_sam_path}
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
    return ExecutionResult(
        manifest=[{
            out_bam: out_bam_path.local,
            out_bam_csi: context.Output(out_bam_csi).local,
        }],
        success=context.Output(out_bam_csi).local.exists()
    )

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    output_signature = {
        out_bam:      "alignments.bam",
        out_bam_csi:  "alignments.bam.csi"
    },
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)

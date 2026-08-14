from pathlib import Path
import json
import pandas as pd
import numpy as np
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
img_sam = model.AddRequirement(lib.GetType("env::samtools.env"))
img_bed = model.AddRequirement(lib.GetType("env::bedtools.env"))
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::reads"), parents={meta})
rstats  = model.AddRequirement(lib.GetType("sequences::read_qc_stats"), parents={meta})
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={meta})
stats   = model.AddProduct(lib.GetType("sequences::assembly_stats"))
concov  = model.AddProduct(lib.GetType("sequences::assembly_per_contig_coverage"))
bpcov   = model.AddProduct(lib.GetType("sequences::assembly_per_bp_coverage"))
bam     = model.AddProduct(lib.GetType("alignment::bam"))

def protocol(context: ExecutionContext):
    irmeta = context.Input(meta)
    ireads = context.Input(reads)
    irstats = context.Input(rstats)
    iasm = context.Input(asm)
    istats = context.Output(stats)
    icontig_cov = context.Output(concov)
    ibp_cov = context.Output(bpcov)
    obam = context.Output(bam)

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
    # https://www.nature.com/articles/s41587-023-01983-6
    # minimap2 -x asm20     this is for hifi

    with open(irmeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    assert length_class in {"short", "long"}, f"unknown length_class: [{length_class}]"
    is_long = length_class == "long"

    with open(irstats.local) as j:
        read_stats = json.load(j)
    q = read_stats["mean_quality"]

    if is_long:
        if q>=30:
            preset = "-x asm5"  # divergence <0.1%
        elif q>=20:
            preset = "-x asm10" # divergence <1%
        else:
            preset = "-x asm20" # divergence "severaal" %
    else:
        preset = "-x sr"

    Log.Info("start minimap align")
    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"-t {cpus}"
    temp_sam_path = Path("./temp.sam")
    context.ExecWithEnv().ifContainerDo(
        env = img_mm2,
        cmd = f"""
            minimap2 {preset} -a -2 {cpus_string} \
                {iasm.container} {ireads.container} > {temp_sam_path}
        """
    )

    Log.Info("convert to BAM, sort and index")
    cpus_string = "" if cpus is None else f"-@ {cpus}"
    bam_file = "temp.bam"
    alignment_stats_file = "alignment_stats.tsv"
    context.ExecWithEnv().ifContainerDo(
        env = img_sam,
        cmd = f"""
            samtools view {cpus_string} -b {temp_sam_path} \
                | samtools sort {cpus_string} -o {bam_file} -O bam
            samtools index {cpus_string} -c {bam_file}
            samtools flagstat {cpus_string} -O tsv {bam_file} >{alignment_stats_file}
        """
    )

    Log.Info("calculating per bp coverage")
    cov_tsv = "bp_cov.tsv"
    _header = "\t".join(["contig", "start", "end", "fold_coverage"])
    context.ExecWithEnv().ifContainerDo(
        env = img_bed,
        cmd = f"""
            echo "{_header}" >{cov_tsv}
            bedtools genomecov -ibam {bam_file} -bg >>{cov_tsv}
        """
    )

    Log.Info("compressing per bp coverage")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-p {cpus}"
    context.LocalShell(f"pigz -c {cpus_string} {cov_tsv} >{ibp_cov.local}")

    Log.Info("summarizing per contig converage")
    contig2length = {}
    with open(iasm.local) as fa:
            current = None
            length = 0
            def _submita():
                contig2length[current] = length
            for l in fa:
                if l[0] == ">":
                    if current is not None: _submita()
                    current = l[1:-1].split(" ")[0]
                    length = 0
                else:
                    length += len(l)-1 # minus 1 for "\n"
            _submita()
    with open(cov_tsv) as f:
        with open(icontig_cov.local, "w") as of:
            of.write("\t".join(["contig", "fold_coverage", "contig_length"])+"\n")
            last_k = None
            entry = []
            seen = set()
            def _submit():
                if last_k is None: return
                nonlocal entry
                total = contig2length[last_k]
                seen.add(last_k)
                c = 0.0
                for span, val in entry:
                    c += (span/total)*val
                # assume no overlap, so total == total span of contig
                of.write("\t".join(str(x) for x in [last_k, c, total])+"\n")
                entry = []

            f.readline() # header
            for l in f:
                k, s, e, val = l[:-1].split("\t")
                s, e, val = [int(x) for x in [s, e, val]]
                if k != last_k:
                    _submit()
                    last_k = k
                entry.append((e-s, val))
            _submit()

            # write no coverage contigs
            for k, l in contig2length.items():
                if k in seen: continue
                of.write("\t".join(str(x) for x in [k, 0, l])+"\n")

    Log.Info("getting alignment stats")
    df = pd.read_csv(alignment_stats_file, sep="\t", header=None)
    alignment_stats = {}
    alignment_stats_2nd_values = {}
    def _as_number(x):
        if "." not in x:
            try:
                return int(x)
            except ValueError:
                pass
        try:
            return float(x)
        except ValueError:
            return x
    def _from_np(x):
        if pd.isna(x): # includes nan, but isnan excludes None
            return None
        if isinstance(x, str):
            if x.endswith("%"):
                x = x[:-1]
            return _as_number(x)
        if isinstance(x, np.floating) or isinstance(x, np.integer):
            return x.item()
        return x
    for _, r in df.iterrows():
        v1, v2, k = r
        alignment_stats[k] = _from_np(v1)
        alignment_stats_2nd_values[k] = _from_np(v2)

    Log.Info("running seqkit")
    seqkit_stats_file = "seqkit_stats.tsv"
    context.ExecWithEnv().ifContainerDo(
        env = img_sqk,
        cmd = f"""
            seqkit stat --all --tabular {iasm.container} >{seqkit_stats_file}
        """
    )
    Log.Info("compiling stats")
    df = pd.read_csv(seqkit_stats_file, sep="\t")
    seqkit_stats = {k:_from_np(v) for k, v in dict(df.iloc[0]).items()}
    assembly_stats = dict(
        fraction_reads_mapped=alignment_stats["mapped"]/alignment_stats["total (QC-passed reads + QC-failed reads)"],
        length=seqkit_stats["sum_len"],
        N50=seqkit_stats["N50"],
        GC=seqkit_stats["GC(%)"],
        number_of_contigs=seqkit_stats["num_seqs"],
        _raw_mapping=alignment_stats,
        _raw_seqkit=seqkit_stats,
    )
    with open(istats.local, "w") as j:
        json.dump(assembly_stats, j)

    # copy BAM to output
    Log.Info("copying BAM to output")
    context.LocalShell(f"cp {bam_file} {obam.local}")

    # just a bit of cleanup
    if temp_sam_path.exists(): temp_sam_path.unlink()

    return ExecutionResult(
        manifest=[{
            stats: istats.local,
            concov: icontig_cov.local,
            bpcov: ibp_cov.local,
            bam: obam.local,
        }],
        success=obam.local.exists() and icontig_cov.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=meta,
    model = model,
    resources=Resources(
        cpus=4,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)

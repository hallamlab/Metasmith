# DESIGNED, NOT YET RUN AGAINST REAL DATA: our own binning::derep_mag_ref does
# not exist until the Pratama groundwater reproduction itself runs (still not
# started per .awm/context.md), so this has only been checked against the
# `skani dist --qi/--ri` command shape and column names (verified with a
# synthetic 3 kb pair, staphb/skani:0.2.2) -- not against a real derep_mag_ref
# or the real 1275-MAG Zenodo archive.
#
# binning::derep_mag_ref pools every MAG's contigs into ONE fasta, scaffolds
# namespaced `<bin_id>~<contig_id>` (see its data_types/binning.yml comment) --
# built that way for inStrain read mapping, not for whole-genome comparison.
# skani scores one genome per input file, so scoring it as-is would score each
# SCAFFOLD against the published MAGs rather than each MAG, fragmenting a
# multi-contig MAG's ANI across its own contigs. This transform re-splits it
# back into one file per bin_id first, by the same `~` convention, so a MAG
# with several contigs is scored as the one genome it is.
import glob
from collections import defaultdict
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::skani.env"))
published = model.AddRequirement(lib.GetType("pratama::published_mags"))
ours      = model.AddRequirement(lib.GetType("binning::derep_mag_ref"))
out       = model.AddProduct(lib.GetType("pratama::mag_recovery_table"))


def protocol(context: ExecutionContext):
    ipub = context.Input(published)
    iours = context.Input(ours)
    iout = context.Output(out)

    pub_list = Path("published_mags.list")
    with open(pub_list, "w") as lf:
        for p in sorted(glob.glob(f"{ipub.local}/*.fasta")):
            lf.write(f"{p}\n")

    Log.Info("re-splitting derep_mag_ref by its <bin_id>~<contig_id> namespacing")
    our_ref_fna = Path(f"{iours.local}/mag_ref.fna")
    by_bin: dict[str, list[str]] = defaultdict(list)
    current_bin = None
    with open(our_ref_fna) as fa:
        for line in fa:
            if line.startswith(">"):
                header = line[1:].strip().split(" ")[0]
                current_bin, _, _ = header.partition("~")
                by_bin[current_bin].append(line)
            else:
                by_bin[current_bin].append(line)

    staged = Path("staged_our_mags")
    staged.mkdir()
    our_list = Path("our_mags.list")
    with open(our_list, "w") as lf:
        for bin_id, lines in by_bin.items():
            dest = staged / f"{bin_id}.fna"
            dest.write_text("".join(lines))
            lf.write(f"{dest.resolve()}\n")

    threads = context.params.get("cpus", 8)
    _cmd = f"""
            skani dist --ql {pub_list} --rl {our_list} \
                --min-af 15 -o {iout.container} -t {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=published,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)

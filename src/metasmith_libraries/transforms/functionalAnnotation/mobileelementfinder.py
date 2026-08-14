"""mobileelementfinder — MobileElementFinder (mefinder) on assembled contigs.

Detects IS elements / transposases flanking ARGs. Runs on sequences::contig_batch
(~5 Mbp contig batches, w4_rebatch.py), like the other contig-level annotators
(integron_finder, virsorter2, genomad) — NOT on the whole assembly. The contig id
(SG<id>~k141_XXXXXX, sample-prefixed) is carried in the output's contig column, so
w4_recompile.py strips the prefix + regroups results per sample. Tolerates samples
/ batches with no MGEs.

mefinder emits a comma-separated CSV with leading '##' comment lines; we normalise
it to a TSV here (proper CSV parse, drop comment lines, keep the header once) so
w4_recompile can de-prefix by the contig column uniformly with the other tools.
"""
import csv
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::mobileelementfinder.env"))
# ~5 Mbp contig batch (w4_rebatch.py), sample-prefixed headers; per-sample regroup
# happens in w4_recompile.py. Sibling of assembly, not a subtype.
asm = model.AddRequirement(lib.GetType("sequences::contig_batch"))
out_results = model.AddProduct(lib.GetType("annotation::mobileelementfinder_results"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 4)

    # mefinder writes mef_out.csv in the work dir; the bundled MGE database ships
    # inside the image. A no-hit batch exits 0 and writes a comment-only CSV, so we
    # do NOT mask the exit code: a genuine non-zero exit (e.g. the historical
    # blastn `-outfmt 15` multi-document JSONDecodeError on hit-rich batches) must
    # FAIL the task loudly, not be swallowed into a silent empty (false-negative)
    # result. That JSON bug lives in me_finder/tools/blast.py's bare json.load().
    #
    # The deployed env::mobileelementfinder.env resolves to the STOCK
    # quay.io/hallamlab/external_mobileelementfinder:1.1.2 image (the intended
    # >= 1.1.2-jsonfix1 build was never published), so we inject the fix at run
    # time: bind our raw_decode-fixed blast.py (parses every concatenated blast
    # JSON doc) over the container's copy. The patch lives in fir project space
    # (staged from container_builds/main/mobileelementfinder/patches/me_finder_blast.py);
    # DURABLE FOLLOW-UP is to bake it into a jsonfix1 image + repoint the type,
    # then drop this bind. The in-container path was confirmed by inspecting the
    # 1.1.2 SIF (conda env external_mobileelementfinder_env, python3.9).
    MEF_BLAST_PATCH = Path(
        "/home/phyberos/project-rpp/spanish_lakes/container_patches/me_finder_blast.py"
    )
    MEF_BLAST_INCONTAINER = (
        "/opt/conda/envs/external_mobileelementfinder_env"
        "/lib/python3.9/site-packages/me_finder/tools/blast.py"
    )
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(MEF_BLAST_PATCH, MEF_BLAST_INCONTAINER)],
        cmd=f"""
            mefinder find --contig {iasm.container} --threads {threads} mef_out
        """,
    )

    # Normalise the mefinder CSV -> TSV: drop '#'/'##' comment lines, keep the
    # column header once, tab-join fields (proper CSV parse so quoted fields with
    # embedded commas survive). The contig id stays byte-exact.
    src = Path("mef_out.csv")
    if src.exists():
        # mefinder writes 5 leading '#' comment lines (LF) then a header + data
        # rows with CRLF endings; strip CR/LF and drop comments before parsing.
        with open(src, newline="") as fi:
            clean = (ln.rstrip("\r\n") for ln in fi if not ln.lstrip().startswith("#"))
            reader = csv.reader(clean)
            rows = [r for r in reader if r]
        with open(iout.local, "w") as fo:
            for r in rows:
                fo.write("\t".join(r) + "\n")
    if not iout.local.exists() or iout.local.stat().st_size == 0:
        Path(iout.local).write_text("# No mobile elements detected\n")

    return ExecutionResult(
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=3),
    ),
)

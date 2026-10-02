# One metaQUAST over all four of a sample's assemblies, side by side against the same references.
# Separate runs cannot be planned: a target's parent means "descends from", and the polished assembly
# descends from Flye's (and OPERA-MS's from MEGAHIT's), so per-assembly reports collapse into one another.
import json
from pathlib import Path

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("e5::quast.env"))
study    = model.AddRequirement(lib.GetType("viromics::contig_study"))
meta     = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
genomes  = model.AddRequirement(lib.GetType("e5::source_genomes"), parents={study})
present  = model.AddRequirement(lib.GetType("e5::sample_references"), parents={meta})
megahit  = model.AddRequirement(lib.GetType("sequences::megahit_assembly"), parents={meta})
opera    = model.AddRequirement(lib.GetType("e5::opera_ms_assembly"), parents={meta})
flye     = model.AddRequirement(lib.GetType("e5::flye_assembly"), parents={meta})
polca    = model.AddRequirement(lib.GetType("e5::polca_assembly"), parents={meta})
out      = model.AddProduct(lib.GetType("e5::metaquast_report"))

LABELS = {"megahit": megahit, "opera_ms": opera, "flye": flye, "flye_polca": polca}


def protocol(context: ExecutionContext):
    igenomes, iout = context.Input(genomes), context.Output(out)
    threads = context.params.get("cpus") or 4
    fastas = " ".join(str(context.Input(r).container) for r in LABELS.values())
    keep = json.loads(context.Input(present).local.read_text())["references"]
    Path("keep.txt").write_text("\n".join(keep) + "\n")

    # One reference per genome FASTA, wherever the tarball nests it, kept only if the sample holds that genome:
    # toy_humangut's study ships 2,078 genomes, and metaQUAST runs QUAST once per reference it aligns to. --fragmented because CAMI's source
    # genomes are drafts: a contig spanning two pieces of a draft is not a misassembly. --max-ref-number 0
    # stops metaQUAST reaching for SILVA when it is given references. metaQUAST exits 0 when minimap2 dies
    # under the memory cap and drops that assembly from every table, so a failed alignment fails the step.
    context.ExecWithEnv(env=image, cmd=f"""
        mkdir -p unpacked refs
        tar xzf {igenomes.container} -C unpacked
        find unpacked -type f \\( -name '*.fna' -o -name '*.fa' -o -name '*.fasta' \\) \
            | while read f; do ln -s "$PWD/$f" "refs/$(basename "$f")"; done
        find unpacked -type f \\( -name '*.fna.gz' -o -name '*.fa.gz' -o -name '*.fasta.gz' \\) \
            | while read f; do gzip -dc "$f" > "refs/$(basename "$f" .gz)"; done
        for f in refs/*; do s=$(basename "$f"); grep -qxF "${{s%.*}}" keep.txt || rm "$f"; done
        echo "references: $(ls refs | wc -l) of {len(keep)} present"
        [ "$(ls refs | wc -l)" -gt 0 ] || exit 1
        metaquast.py {fastas} -l {",".join(LABELS)} -r refs -o mq -t {threads} --fragmented --max-ref-number 0 \
            --no-icarus --no-plots --no-html
        if grep -q "Failed aligning" mq/combined_reference/quast.log; then exit 1; fi
        cd mq && tar czf {iout.container} $(ls -d combined_reference/*.tsv summary/TSV \
            runs_per_reference/*/report.tsv not_aligned/report.tsv metaquast.log 2>/dev/null)
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=12)),
)

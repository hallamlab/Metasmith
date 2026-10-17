# Pratama's geNomad (reproduction_map B3):
# `end-to-end --cleanup --splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1`.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::genomad.env"))
assembly = model.AddRequirement(lib.GetType("sequences::contig_batch"))
ref      = model.AddRequirement(lib.GetType("ref::genomad"))

virus_summary_out   = model.AddProduct(lib.GetType("taxonomy::genomad_virus_summary"))
plasmid_summary_out = model.AddProduct(lib.GetType("taxonomy::genomad_plasmid_summary"))
virus_genes_out     = model.AddProduct(lib.GetType("taxonomy::genomad_virus_genes"))
taxonomy_out        = model.AddProduct(lib.GetType("taxonomy::genomad_taxonomy"))
calls_out           = model.AddProduct(lib.GetType("viromics::genomad_candidate_virus"))


def _write_calls(summary_tsv: Path, out: Path):
    # A provirus carries 1-based inclusive `start-end` in `coordinates`; a whole-contig call has NA there.
    with open(summary_tsv) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {name: i for i, name in enumerate(header)}
        missing = [c for c in ("seq_name", "length", "coordinates", "virus_score") if c not in col]
        assert not missing, f"geNomad's virus_summary.tsv has no {missing}; header was {header}"
        with open(out, "w") as o:
            o.write("contig_id\tstart\tend\tcaller\tscore\n")
            for line in f:
                if not line.strip():
                    continue
                row = line.rstrip("\n").split("\t")
                coords = row[col["coordinates"]]
                if coords in ("NA", "", "."):
                    start, end = 1, int(row[col["length"]])
                else:
                    start, end = (int(x) for x in coords.split("-"))
                o.write(f"{row[col['seq_name']].split('|')[0]}\t{start}\t{end}\tgenomad\t{row[col['virus_score']]}\n")


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb = context.Input(ref)
    threads = context.params.get("cpus", 16)

    context.ExecWithEnv(env=image, cmd=f"""
        /usr/local/bin/_entrypoint.sh genomad end-to-end {iasm.container} genomad_output {idb.container} \
            -t {threads} --cleanup --splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1
    """)

    prefix = Path(iasm.local).stem
    summary = Path(f"genomad_output/{prefix}_summary")
    outs = {p: context.Output(p) for p in (virus_summary_out, plasmid_summary_out, virus_genes_out,
                                           taxonomy_out, calls_out)}
    shutil.copy2(summary / f"{prefix}_virus_summary.tsv", outs[virus_summary_out].local)
    shutil.copy2(summary / f"{prefix}_plasmid_summary.tsv", outs[plasmid_summary_out].local)
    shutil.copy2(summary / f"{prefix}_virus_genes.tsv", outs[virus_genes_out].local)
    # --cleanup removes only _mmseqs2, so the annotate directory survives it.
    shutil.copy2(Path(f"genomad_output/{prefix}_annotate/{prefix}_taxonomy.tsv"), outs[taxonomy_out].local)
    _write_calls(outs[virus_summary_out].local, outs[calls_out].local)
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=12)),
)

# Methods P43: remove a vOTU representative if it is >=100 kb AND geNomad's annotate table
# calls one of its genes island-associated -- the paper's categories only, read over the
# columns Antonio's 10_filtering_3.sh reads (:72-82). A vOTU below 100 kb was never annotated
# (votu_island_annotate's own gate), so its absence from the gene table already is
# the length gate; nothing here re-checks length against the genes table.
#
# geNomad names a gene <contig_id>_<gene_number>, and a vOTU representative's own id can end in
# digits -- the same trap as CheckV's provirus suffix. Only the last underscore in a gene name
# is guaranteed to be geNomad's own separator, and the assert against the annotated contig set
# (the representatives actually fed to genomad annotate) catches anything that isn't.
import re
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reps  = model.AddRequirement(lib.GetType("e3::votu_representatives"))
genes = model.AddRequirement(lib.GetType("e3::genomad_island_genes"), parents={reps})
out   = model.AddProduct(lib.GetType("e3::final_votu_representatives"))

ISLAND_LENGTH_BP = 100_000

# The Methods' categories: transposons, lipopolysaccharide genes (glycosyltransferase, nucleotidyl
# transferase, carbohydrate kinase, nucleotide sugar epimerase), endonuclease, integrase and plasmid
# stability. Antonio's list (10_filtering_3.sh:29-41) adds partition, toxin-antitoxin and a bare
# "stability", which the paper does not name. A transposon is annotated by its transposase.
BAD_PATTERNS = re.compile(
    r"transpos(?:on|ase)|"
    r"lipopolysaccharide|\blps\b|"
    r"glycosyl.?transferase|"
    r"nucleotidyl.?transferase|"
    r"carbohydrate kinase|"
    r"nucleotide.?sugar epimerase|"
    r"endonuclease|"
    r"integrase|"
    r"plasmid stability",
    re.IGNORECASE,
)
# 10_filtering_3.sh:72-82.
ANNOTATION_COLUMNS = ("marker", "annotation_description", "annotation_accessions",
                      "annotation_conjscan", "annotation_amr", "taxname")


def _contig_of(gene: str, known_ids) -> str:
    base, sep, tail = gene.rpartition("_")
    assert sep and tail.isdigit() and base in known_ids, (
        f"gene {gene!r} does not decompose into an annotated contig id plus geNomad's own _<n> suffix")
    return base


def _bad_contigs(path: Path, known_ids) -> set:
    bad = set()
    if path.stat().st_size == 0:
        return bad
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {n: i for i, n in enumerate(header)}
        assert "gene" in col, f"{path.name} has no gene column; header was {header}"
        annot = [col[c] for c in ANNOTATION_COLUMNS if c in col]
        for line in f:
            if not line.strip():
                continue
            row = line.rstrip("\n").split("\t")
            contig = _contig_of(row[col["gene"]], known_ids)
            text = " ".join(row[i] for i in annot if i < len(row))
            if BAD_PATTERNS.search(text):
                bad.add(contig)
    return bad


def _iter_fasta(path: Path):
    header, seq = None, []
    for line in open(path):
        if line.startswith(">"):
            if header is not None:
                yield header, "".join(seq)
            header, seq = line[1:].strip().split()[0], []
        elif line.strip():
            seq.append(line.strip())
    if header is not None:
        yield header, "".join(seq)


def protocol(context: ExecutionContext):
    ireps = context.Input(reps)
    igenes = context.Input(genes)
    o = context.Output(out)

    records = list(_iter_fasta(Path(ireps.local)))
    annotated = {rid for rid, seq in records if len(seq) >= ISLAND_LENGTH_BP}
    bad = _bad_contigs(Path(igenes.local), annotated)

    n_removed = 0
    with open(o.local, "w") as out_fh:
        for rid, seq in records:
            if rid in bad:
                n_removed += 1
                continue
            out_fh.write(f">{rid}\n")
            for i in range(0, len(seq), 70):
                out_fh.write(seq[i:i + 70] + "\n")

    Log.Info(f"{len(records)} vOTU representatives, {len(annotated)} annotated (>={ISLAND_LENGTH_BP} bp), "
             f"{n_removed} removed by the island filter -> {len(records) - n_removed}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reps,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)

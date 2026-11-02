# Filtering 2 (Pratama Methods P42, Supp Fig 1): keep a frozen contig if it has any viral
# genes, or has neither viral nor host genes, or is >=75% unknown-gene content -- CheckV's
# quality_summary counts answer all three, so no geNomad gene table is read here. There is
# deliberately no length>=1kb rule; the paper's Supp Fig 1 wording drops it.
#
# A kept contig with provirus=="Yes" is emitted from CheckV's own trimmed region(s) in
# proviruses.fna rather than whole -- trimming precedes clustering. CheckV numbers a contig's
# excised regions <contig_id>_1, <contig_id>_2, ..., and a frozen_id can itself end in digits
# (a start_end segment, an assembler name like NODE_12_..._cov_3.1), so recovering the original
# id cannot be a blind regex strip: only the LAST underscore in a proviruses.fna header is
# guaranteed to be CheckV's own separator, and the keep rule's known contig_id set from
# quality_summary confirms that split rather than trusting it.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
batch  = model.AddRequirement(lib.GetType("e3::viral_contig_batch"), parents={frozen})

viruses    = model.AddRequirement(lib.GetType("e3::checkv_viruses_batch"), parents={batch})
proviruses = model.AddRequirement(lib.GetType("e3::checkv_proviruses_batch"), parents={batch})
quality    = model.AddRequirement(lib.GetType("e3::checkv_quality_summary_batch"), parents={batch})

out_curated = model.AddProduct(lib.GetType("e3::curated_candidate_virus_batch"))


def _keep(viral_genes: int, host_genes: int, gene_count: int) -> bool:
    if viral_genes > 0:
        return True
    if viral_genes == 0 and host_genes == 0:
        return True
    return gene_count > 0 and (gene_count - viral_genes - host_genes) / gene_count >= 0.75


def _read_quality(path: Path):
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {n: i for i, n in enumerate(header)}
        missing = [c for c in ("contig_id", "provirus", "gene_count", "viral_genes", "host_genes") if c not in col]
        assert not missing, f"{path.name} has no {missing}; header was {header}"
        for line in f:
            if line.strip():
                r = line.rstrip("\n").split("\t")
                yield (r[col["contig_id"]], r[col["provirus"]] == "Yes",
                       int(r[col["gene_count"]]), int(r[col["viral_genes"]]), int(r[col["host_genes"]]))


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


def _split_provirus_id(record_id: str, known_provirus_ids) -> str:
    base, sep, tail = record_id.rpartition("_")
    assert sep and tail.isdigit() and base in known_provirus_ids, (
        f"proviruses.fna record {record_id!r} does not decompose into a known provirus contig_id "
        "plus CheckV's own _<n> suffix")
    return base


def _write_record(fh, record_id: str, seq: str):
    fh.write(f">{record_id}\n")
    for i in range(0, len(seq), 70):
        fh.write(seq[i:i + 70] + "\n")


# The trimmed-vs-whole choice: which contig_ids are kept whole (viruses.fna), which are kept as
# a provirus trim (proviruses.fna), and every provirus-flagged id regardless of the keep rule --
# needed to recognise a discarded contig's fragments in proviruses.fna as genuinely its own,
# not a decomposition failure.
def _classify(rows):
    all_provirus_ids, keep_whole, keep_provirus = set(), set(), set()
    for contig_id, is_provirus, gene_count, viral_genes, host_genes in rows:
        if is_provirus:
            all_provirus_ids.add(contig_id)
        if not _keep(viral_genes, host_genes, gene_count):
            continue
        (keep_provirus if is_provirus else keep_whole).add(contig_id)
    return keep_whole, keep_provirus, all_provirus_ids


def protocol(context: ExecutionContext):
    iviruses = Path(context.Input(viruses).local)
    iproviruses = Path(context.Input(proviruses).local)
    iquality = Path(context.Input(quality).local)
    o = context.Output(out_curated)

    rows = list(_read_quality(iquality))
    n_total = len(rows)
    keep_whole, keep_provirus, all_provirus_ids = _classify(rows)

    seen_provirus = set()
    with open(o.local, "w") as out:
        for record_id, seq in _iter_fasta(iviruses):
            if record_id in keep_whole:
                _write_record(out, record_id, seq)
        for record_id, seq in _iter_fasta(iproviruses):
            base = _split_provirus_id(record_id, all_provirus_ids)
            if base in keep_provirus:
                seen_provirus.add(base)
                _write_record(out, record_id, seq)

    missing = keep_provirus - seen_provirus
    if missing:
        Log.Warn(f"[{iquality.name}] {len(missing)} contigs kept as provirus but absent from "
                 f"proviruses.fna: {sorted(missing)[:5]}")

    Log.Info(f"{n_total} contigs scored, {len(keep_whole)} kept whole, {len(keep_provirus)} kept as "
             f"provirus ({len(seen_provirus)} trimmed)")
    return ExecutionResult(manifest=[{out_curated: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=batch,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
)

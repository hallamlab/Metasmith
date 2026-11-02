# Concatenate the per-batch prodigal-gv calls back into the frozen set's ORFs.
#
# Both products keep the types the E3 driver targets (e3::viral_orfs, e3::viral_gff), so no driver edit is
# needed and the merge is the only producer of either.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
batch  = model.AddRequirement(lib.GetType("e3::viral_contig_batch"), parents={frozen})
cds_b  = model.AddRequirement(lib.GetType("e3::viral_orfs_batch"), parents={batch})
gff_b  = model.AddRequirement(lib.GetType("e3::viral_gff_batch"), parents={batch})

cds = model.AddProduct(lib.GetType("e3::viral_orfs"))
gff = model.AddProduct(lib.GetType("e3::viral_gff"))


def _first_record_id(path: Path) -> str:
    """The first FASTA header in a batch's proteins, used to order the concatenation.

    Ordering by content rather than by file name, because a pool given carries a content-hash name and
    grouped slots arrive in arbitrary order. Order does not change any call -- gene calling is per contig
    -- but a stable order makes the merged file byte-reproducible.
    """
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                return line[1:].strip()
    return ""


def protocol(context: ExecutionContext):
    # Grouped slots are related by lineage, never by index: pairing them positionally is this library's
    # documented quiet failure. Each batch's proteins and GFF are paired through their shared batch.
    per_batch = {}
    for slot, key in ((cds_b, "faa"), (gff_b, "gff")):
        for handle in context.InputGroup(slot):
            src = context.SourceOf(handle, batch)
            assert src is not None, (
                f"no viral_contig_batch in the lineage of [{handle.local.name}] -- without it the "
                "proteins cannot be paired with their own GFF")
            per_batch.setdefault(src.local, {})[key] = handle.local

    assert per_batch, "no batches in the group; the split produced nothing"
    incomplete = {k.name: sorted(v) for k, v in per_batch.items() if len(v) != 2}
    assert not incomplete, f"these batches are missing a product: {incomplete}"

    order = sorted(per_batch, key=lambda b: _first_record_id(per_batch[b]["faa"]))

    ocds = context.Output(cds)
    ogff = context.Output(gff)

    n_prot = 0
    with open(ocds.local, "w") as out:
        for b in order:
            for line in open(per_batch[b]["faa"]):
                if line.startswith(">"):
                    n_prot += 1
                out.write(line)

    # prodigal writes a `##gff-version` pragma at the top of every file; the merged file keeps one.
    n_feat = 0
    seen_pragma = False
    with open(ogff.local, "w") as out:
        for b in order:
            for line in open(per_batch[b]["gff"]):
                if line.startswith("##gff-version"):
                    if seen_pragma:
                        continue
                    seen_pragma = True
                elif not line.startswith("#") and line.strip():
                    n_feat += 1
                out.write(line)

    Log.Info(f"{len(order)} batches -> {n_prot} proteins, {n_feat} gff features")
    assert n_prot > 0, "the merged protein file is empty; every batch called no genes"
    return ExecutionResult(manifest=[{cds: ocds.local, gff: ogff.local}],
                           success=ocds.local.exists() and ogff.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={cds: "viral_orfs.faa", gff: "viral_orfs.gff"},
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
)

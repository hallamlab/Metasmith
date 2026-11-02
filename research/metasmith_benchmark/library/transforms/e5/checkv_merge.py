# Stack the per-batch CheckV tables back into the frozen set's four tables.
#
# All four products keep the standard viromics::checkv_* types the E3 driver targets, so no target moves.
# The driver masks the standard checkv.py, which makes this the only producer of those four types.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
batch  = model.AddRequirement(lib.GetType("e3::viral_contig_batch"), parents={frozen})

b_contamination = model.AddRequirement(lib.GetType("e3::checkv_contamination_batch"), parents={batch})
b_quality       = model.AddRequirement(lib.GetType("e3::checkv_quality_summary_batch"), parents={batch})
b_completeness  = model.AddRequirement(lib.GetType("e3::checkv_completeness_batch"), parents={batch})
b_complete      = model.AddRequirement(lib.GetType("e3::checkv_complete_genomes_batch"), parents={batch})

out_contamination = model.AddProduct(lib.GetType("viromics::checkv_contamination"))
out_quality       = model.AddProduct(lib.GetType("viromics::checkv_quality_summary"))
out_completeness  = model.AddProduct(lib.GetType("viromics::checkv_completeness"))
out_complete      = model.AddProduct(lib.GetType("viromics::checkv_complete_genomes"))

PAIRS = [
    (b_contamination, out_contamination, "contamination.tsv"),
    (b_quality, out_quality, "quality_summary.tsv"),
    (b_completeness, out_completeness, "completeness.tsv"),
    (b_complete, out_complete, "complete_genomes.tsv"),
]


def _first_record_id(path: Path) -> str:
    """The first FASTA header of a batch, used to order the concatenation.

    Ordering by content, not by file name: a pool given carries a content-hash name and grouped slots
    arrive in arbitrary order. Order changes no row -- every CheckV table is one row per contig -- but a
    stable order makes the merged tables byte-reproducible.
    """
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                return line[1:].strip()
    return ""


def protocol(context: ExecutionContext):
    # Grouped slots are related by lineage, never by index. Each table is filed under the batch it was
    # computed from, so the four tables of one batch stay together.
    per_batch = {}
    for slot, _, name in PAIRS:
        for handle in context.InputGroup(slot):
            src = context.SourceOf(handle, batch)
            assert src is not None, (
                f"no viral_contig_batch in the lineage of [{handle.local.name}] -- without it this table "
                "cannot be placed among the batches")
            per_batch.setdefault(src.local, {})[name] = handle.local

    assert per_batch, "no batches in the group; the split produced nothing"
    incomplete = {k.name: sorted(v) for k, v in per_batch.items() if len(v) != len(PAIRS)}
    assert not incomplete, (
        f"these batches are missing a CheckV table: {incomplete}. end_to_end writes all four together, so "
        "a batch short a table is a batch whose task did not finish -- merging it would silently drop "
        "those contigs from one table only.")

    order = sorted(per_batch, key=_first_record_id)

    manifest = {}
    for _, prod, name in PAIRS:
        o = context.Output(prod)
        header = None
        n_rows = 0
        with open(o.local, "w") as out:
            for b in order:
                lines = Path(per_batch[b][name]).read_text().splitlines(keepends=True)
                if not lines:
                    continue
                if header is None:
                    header = lines[0]
                    out.write(header)
                else:
                    assert lines[0] == header, (
                        f"{name} headers differ between batches: {lines[0]!r} vs {header!r}")
                body = [l for l in lines[1:] if l.strip()]
                out.writelines(body)
                n_rows += len(body)
        assert header is not None, f"every batch wrote an empty {name}"
        Log.Info(f"{name}: {len(order)} batches -> {n_rows} rows")
        manifest[prod] = o.local

    return ExecutionResult(manifest=[manifest],
                           success=all(p.exists() for p in manifest.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={
        out_contamination: "contamination.tsv",
        out_quality: "quality_summary.tsv",
        out_completeness: "completeness.tsv",
        out_complete: "complete_genomes.tsv",
    },
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=4)),
)

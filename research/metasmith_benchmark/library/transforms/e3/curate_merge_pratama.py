# Pool every slice's curated, host-trimmed contigs into one set -- what mmseqs_votu_pratama
# clusters instead of the raw frozen pool. Ordering follows checkv_merge_pratama: by content,
# since a pool given's file is named for its content hash and grouped slots arrive in
# arbitrary order.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen        = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
batch         = model.AddRequirement(lib.GetType("e3::viral_contig_batch"), parents={frozen})
curated_batch = model.AddRequirement(lib.GetType("e3::curated_candidate_virus_batch"), parents={batch})

out_curated = model.AddProduct(lib.GetType("e3::curated_candidate_virus"))


def _first_record_id(path: Path) -> str:
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                return line[1:].strip()
    return ""


def protocol(context: ExecutionContext):
    paths = []
    for handle in context.InputGroup(curated_batch):
        src = context.SourceOf(handle, batch)
        assert src is not None, f"no viral_contig_batch in the lineage of [{handle.local.name}]"
        paths.append(Path(handle.local))

    assert paths, "no curated batches in the group; curation produced nothing"
    order = sorted(paths, key=_first_record_id)

    o = context.Output(out_curated)
    n_batches = n_contigs = 0
    with open(o.local, "w") as out:
        for p in order:
            text = p.read_text()
            out.write(text)
            n_batches += 1
            n_contigs += text.count(">")

    Log.Info(f"{n_batches} curated batches -> {n_contigs} curated contigs")
    return ExecutionResult(manifest=[{out_curated: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out_curated: "curated_candidate_virus.fna"},
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
)

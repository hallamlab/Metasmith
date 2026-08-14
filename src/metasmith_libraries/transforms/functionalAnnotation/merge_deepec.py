"""merge_deepec — gather per-chunk DeepEC TSVs back into a per-sample artifact.

Uses sequences::orfs as the sample-identity anchor; metasmith's lineage tracker
gathers every annotation::deepec_predictions chunk that descends from this orfs
instance via the splitter (chunkOrfsForAnnotation → orf_chunk → deepec).

Header is one TSV line on the first chunk; subsequent chunks contribute data
only. Chunk files are unlinked post-merge to reclaim /scratch inodes
(fir_scratch_inode_quota cap is 1M).
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::deepec_predictions_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::deepec_predictions"))


def protocol(context: ExecutionContext):
    import os
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)

    # DeepEC's TSV emits one `Query ID\tPredicted EC number` header per
    # internal inference batch — so even a single chunk may contain
    # multiple header lines. Strategy: detect the header from chunk 0,
    # emit it once, then strip every occurrence (including chunk 0's own
    # repeats) from the data.
    header = None
    with open(chunks[0].local) as fin:
        header = fin.readline()

    with open(iout.local, "w") as fout:
        if header:
            fout.write(header)
        for cf in chunks:
            with open(cf.local) as fin:
                for line in fin:
                    if line == header:
                        continue
                    fout.write(line)

    for cf in chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=parent_orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)

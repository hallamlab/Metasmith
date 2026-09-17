# Slice the frozen viral set for the per-contig callers, on the AMR precedent (splitContigsForAmr).
#
# MEASURED: the frozen set is 11,343,384,184 bytes over 4,597,542 contigs, about 11.0 Gbp. Both of its
# expensive consumers ran it as one task and both missed their walls -- prodigal-gv failed 140:0 at
# 07:59:01 on its 8 h rung and retried at 16 h, and CheckV hit its 16 h wall and retried at 128 G / 32 h.
# Neither tool checkpoints, so every retry restarts from zero. At 500 Mbp a batch that is ~23 slices of
# ~200 K contigs; the failed prodigal-gv attempt covered ~2.42 M contigs in 3:59, so a slice is ~20-25 min
# of gene calling and well under an hour of CheckV.
#
# The frozen set's headers are already sample-prefixed (`sample|contig|start_end`, minted by
# merge_candidate_calls), so a slice needs no renaming and the records are copied through byte for byte.
#
# e3::viral_contig_batch is a SIBLING type, never a subtype of sequences::contig_batch: a subtype would let
# the standard per-batch callers bind to viral slices, which is the B23 tie-break hazard.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
batch  = model.AddProduct(lib.GetType("e3::viral_contig_batch"))

VIRAL_BATCH_BP = 500_000_000


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    target = int(context.params.get("viral_contig_batch_bp", VIRAL_BATCH_BP))

    out_paths = []
    fh = None
    cur_bp = 0
    cur_n = 0
    total_n = 0
    total_bp = 0

    def _open(i):
        p = Path(context.Output(batch, i=i).local)
        p.parent.mkdir(parents=True, exist_ok=True)
        out_paths.append(p)
        return open(p, "w")

    # Streamed line by line: the set is ~10.6 GiB, so it is never held in memory, and writing the lines
    # as read preserves each record's original wrapping exactly.
    with open(ifrozen.local) as src:
        for line in src:
            if line.startswith(">"):
                if fh is None:
                    fh = _open(0)
                elif cur_n > 0 and cur_bp >= target:
                    fh.close()
                    fh = _open(len(out_paths))
                    cur_bp = 0
                    cur_n = 0
                cur_n += 1
                total_n += 1
            else:
                n = len(line.strip())
                cur_bp += n
                total_bp += n
            if fh is not None:
                fh.write(line)
    if fh is not None:
        fh.close()

    Log.Info(f"{total_n} contigs, {total_bp} bp -> {len(out_paths)} batches at ~{target} bp")
    assert out_paths, "the frozen set held no contigs"
    return ExecutionResult(
        manifest=[{batch: p} for p in out_paths],
        success=all(p.exists() for p in out_paths),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
)

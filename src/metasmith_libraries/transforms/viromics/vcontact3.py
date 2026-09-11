# Antonio's step 20, run ONCE on the frozen set. vConTACT3 has no path that
# reuses protein clusters against a subset of genomes -- `-p/-g` skips gene
# calling but not clustering, and the parquet intermediates are skipped only on
# an exact re-run -- so a filtered rerun costs the whole thing. Run it on
# everything and treat the cluster assignment as a per-contig annotation.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::vcontact3.env"))
ref    = model.AddRequirement(lib.GetType("ref::vcontact3_db"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))

out_assignments = model.AddProduct(lib.GetType("viromics::vcontact3_assignments"))
out_network     = model.AddProduct(lib.GetType("viromics::vcontact3_network"))

# No ANI product, and that is the image's fault rather than a choice. `vclust` is
# not on this container at all, and 3.1.4's guard for that case reads
# `exports.remove('vclust')` when the name in the list is 'ani' -- so asking for
# the ANI export does not degrade, it raises ValueError before the run starts.
# Restore viromics::vcontact3_ani when an image ships vclust.


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    iref = context.Input(ref)
    threads = context.params.get("cpus", 8)

    out_dir = Path("vcontact3_out")

    # -n, not the `-p proteins -g gene2genome -l lengths` trio Antonio used. The
    # -n route calls genes itself with pyrodigal-gv, which drops the dependency
    # on DRAM-v's protein FASTA and on building a gene2genome map -- one
    # requirement instead of three. -p/-g is also mutually exclusive with -n and
    # disables the ANI export.
    #
    # No --db-version: the reference tree holds one release and vConTACT3 picks
    # the newest json it finds, so pinning it here would be a second place to
    # keep in step with downloadVcontact3DB.
    #
    # `-e` is not decoration. It defaults to nothing, so without it the run
    # writes only final_assignments.csv and performance_metrics.csv -- no network,
    # silently, after paying for all of the clustering that would have produced
    # it. `cosmograph` is the node/edge table pair. `ani` is deliberately absent;
    # see the note on the products above.
    _cmd = f"""
        vcontact3 run -n {ifrozen.container} -o {out_dir} \
            --db-path {iref.container} -t {threads} \
            -e cosmograph
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    # A frozen contig whose id collides with a genome already in the reference is
    # dropped with a per-record WARNING, and if every one collides the run dies
    # much later on `KeyError: "None of ['genome_id'] are in the columns"` -- the
    # empty length table, not the collision, is what raises. The pipeline's own
    # ids are `<sample>|<contig>|<start>_<end>` and cannot collide; a hand-made
    # input of named reference phages can, and did.
    def _find(name: str) -> Path:
        hits = sorted(out_dir.rglob(name))
        assert hits, (
            f"vConTACT3 wrote no {name}; {out_dir} holds "
            f"{sorted(q.name for q in out_dir.rglob('*') if q.is_file())[:20]}"
        )
        return hits[0]

    def _collect(patterns, dest: Path) -> list[str]:
        found = []
        for pattern in patterns:
            for src in sorted(out_dir.rglob(pattern)):
                (dest/src.name).write_bytes(src.read_bytes())
                found.append(src.name)
        return found

    oassign = context.Output(out_assignments)
    onet = context.Output(out_network)

    # `genome_by_genome_overview.csv` and `viral_cluster_overview.csv` in
    # Antonio's step table are vConTACT **2** names and do not exist in 3.x.
    oassign.local.write_bytes(_find("final_assignments.csv").read_bytes())

    # Filled by pattern rather than by name: `nodes.csv` and `edges.csv` are
    # never written despite both names appearing in the package source, because
    # the cosmograph writer builds its pair from the output prefix instead --
    # `<prefix>_metadata.csv` and `<prefix>_data.csv`.
    onet.local.mkdir(parents=True, exist_ok=True)
    net = _collect(("*_metadata.csv", "*_data.csv"), onet.local)
    assert net, (
        "the cosmograph export wrote no node or edge table; "
        f"{out_dir} holds {sorted(q.name for q in out_dir.rglob('*') if q.is_file())[:20]}"
    )
    Log.Info(f"network export: {net}")

    outs = {out_assignments: oassign, out_network: onet}
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={
        out_assignments: "final_assignments.csv",
        out_network: "network",
    },
    resources=Resources(cpus=32, memory=Size.GB(128), duration=Duration(hours=24)),
)

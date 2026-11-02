# Antonio's step 20 / Pratama's genus-level clustering, run on the >=10 kb vOTU representatives
# rather than on the whole frozen set.
#
# WHY THIS EXISTS AS AN E3 TRANSFORM. The standard `viromics/vcontact3.py` requires
# `viromics::dereplicated_candidate_virus` and its header argues for running on everything, because
# vConTACT3 has no path that reuses protein clusters against a subset -- so a filtered rerun costs
# the whole thing. That argument is sound and its conclusion is still wrong here, because it prices
# the protein clustering (near-linear, disk-backed MMseqs2) and not the two stages after it, which
# are quadratic in the number of genomes:
#
#   resolver.build_distance_matrix   shared_hmms = matrix.dot(matrix.T), then vectorized_sqroot_dist
#                                    materialises one array per genome PAIR sharing >=1 HMM.
#   gbga                             dense_matrix = component_submatrix.toarray() (or float16),
#                                    then squareform() + fastcluster average linkage -- a DENSE
#                                    N_component x N_component object, by algorithm, not by sloppiness.
#
# E3's frozen set is 4,597,542 contigs. Measured on fir: three attempts OOM'd at 128, 256 and
# 512 GiB, the last inside `vectorized_sqroot_dist` five minutes after entering it, and vConTACT3's
# own source calls 180K genomes in one component a 360 GB problem and prints a low-memory warning
# above 100K. At 4.6 M the dense float16 component matrix alone is ~38 TiB. More memory is not a
# fix at any grant fir can serve; a smaller N is, and because the cost is quadratic the subset is
# ~4,000x cheaper rather than more expensive.
#
# WHAT THIS COSTS SCIENTIFICALLY. Cluster assignments now cover the >=10 kb vOTU representatives
# (70,785 of them) instead of every frozen contig. That is the set Pratama analyses -- the same cut
# `dramv_checkv_pratama` already takes (reproduction_map B10) -- so the reproduction target is
# unchanged. The join through `viromics::votu_cluster_table` only reaches the OTHER members of a
# vOTU whose REPRESENTATIVE is itself among the 70,785: votu_representatives_10kb_pratama applies
# `seqkit seq -m 10000` to the representative, not to the member being looked up, so any vOTU whose
# representative falls under 10 kb is absent from this run entirely and the join recovers nothing
# for its members. The frozen-set design's groupby-not-rerun premise holds only within that scope.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vcontact3.env"))
ref   = model.AddRequirement(lib.GetType("ref::vcontact3_db"))
reps  = model.AddRequirement(lib.GetType("e3::votu_representatives_10kb"))

out_assignments = model.AddProduct(lib.GetType("viromics::vcontact3_assignments"))
out_network     = model.AddProduct(lib.GetType("viromics::vcontact3_network"))

# No ANI product, and that is the image's fault rather than a choice. `vclust` is not on this
# container at all, and 3.1.4's guard for that case reads `exports.remove('vclust')` when the name
# in the list is 'ani' -- so asking for the ANI export raises ValueError before the run starts.


def protocol(context: ExecutionContext):
    ireps = context.Input(reps)
    iref = context.Input(ref)
    threads = context.params.get("cpus", 8)

    out_dir = Path("vcontact3_out")

    # -n, not the `-p proteins -g gene2genome -l lengths` trio Antonio used. The -n route calls genes
    # itself with pyrodigal-gv, which drops the dependency on DRAM-v's protein FASTA and on building
    # a gene2genome map. -p/-g is also mutually exclusive with -n and disables the ANI export.
    #
    # No --db-version: the reference tree holds one release and vConTACT3 picks the newest json it
    # finds, so pinning it here would be a second place to keep in step with downloadVcontact3DB.
    #
    # `-e` is not decoration. It defaults to nothing, so without it the run writes only
    # final_assignments.csv and performance_metrics.csv -- no network, silently, after paying for all
    # of the clustering that would have produced it.
    #
    # Deliberately NOT --reduce-memory. It swaps the agglomerative stage to float16 (0.99985 ARI at
    # order and family, per the source's own note), and vConTACT3 engages that path by itself above
    # 100K nodes in a component. 70,785 representatives cannot reach that threshold, so forcing it
    # would spend accuracy for nothing.
    _cmd = f"""
        vcontact3 run -n {ireps.container} -o {out_dir} \
            --db-path {iref.container} -t {threads} \
            -e cosmograph
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    # A contig whose id collides with a genome already in the reference is dropped with a per-record
    # WARNING, and if every one collides the run dies much later on
    # `KeyError: "None of ['genome_id'] are in the columns"` -- the empty length table, not the
    # collision, is what raises. The pipeline's own ids cannot collide; a hand-made input of named
    # reference phages can, and did.
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

    # `genome_by_genome_overview.csv` and `viral_cluster_overview.csv` in Antonio's step table are
    # vConTACT **2** names and do not exist in 3.x.
    oassign.local.write_bytes(_find("final_assignments.csv").read_bytes())

    # Filled by pattern rather than by name: `nodes.csv` and `edges.csv` are never written despite
    # both names appearing in the package source, because the cosmograph writer builds its pair from
    # the output prefix instead -- `<prefix>_metadata.csv` and `<prefix>_data.csv`.
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
    group_by=reps,
    output_signature={
        out_assignments: "final_assignments.csv",
        out_network: "network",
    },
    # EVERY rung stays inside metaSPAdes' declared envelope (48 cpus / 192 GB / 24 h), which is the
    # reproducibility bar: 24/48/96/192 GB tops out exactly at it, 6/12/24/24 h never passes the 24 h
    # cap, and 32 cpus is under its 48. A grant that only fits on one 6 TB node is not a result anyone
    # else can reproduce, which is the whole reason the bar is SPAdes and not the biggest node fir has.
    #
    # Sized from the binding stage rather than from the observed OOM. gbga materialises a DENSE
    # N_component x N_component float32 matrix plus its squareform; at the pathological worst case
    # where all 70,785 representatives fall in ONE component that is ~18.7 + ~9.3 GiB, so rung 1
    # carries it and rung 2 carries it with the profile build's own footprint on top. The realistic
    # case is far smaller, because the gene-sharing network fragments and gbga clusters each
    # component separately.
    #
    # CAUTION duration must satisfy base x 2^(tries-1) <= the params ceiling AND <= 168 h, because
    # fir refuses any job over 7.0 days at submit time and an ignored submission failure wedges the
    # run. The clamp in Resources.AsNextflowFormat enforces the ceiling; this base keeps the
    # unclamped rungs sane too.
    #
    # To re-provision this step WITHOUT editing this file -- which would re-key its product slots and
    # orphan its shards -- add it to the driver's SCALED dict instead. `_common.make_slurm_config`
    # renders that as a `withName` block into workflow.config.nf, which nextflow applies AFTER
    # workflow.resources.nf and which therefore overrides everything declared here. SCALED also sets
    # cpus, which this ladder does not scale.
    resources=Resources(cpus=32, memory=Size.GB(24), duration=Duration(hours=6)),
)

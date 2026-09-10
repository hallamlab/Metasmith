# Antonio's step 21b: the survey's own MAGs are added to iPHoP's host database
# before any host is predicted. This is one of the two transforms that make the
# viral and MAG lanes one template rather than two.
#
# The three collected slots are joined on bin id inside the task, not by lineage:
# GTDB-Tk runs on each binner's bins while the quality pool is the aggregator's
# output, so no single quality bin has one gtdbtk ancestor to recover. gtdbtk's
# `user_genome` column names the bin file, which is the join every consumer of
# these two files already uses.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::iphop.env"))
base    = model.AddRequirement(lib.GetType("ref::iphop_db"))
study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
bin     = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={study})
cluster = model.AddRequirement(lib.GetType("binning_local::cluster_table"), parents={study})
# gtdbtk_raw, not gtdbtk: add_to_db wants the DECORATED TREES, which only
# de_novo_wf writes. The classification TSV that `taxonomy::gtdbtk` carries is a
# different artifact and add_to_db never looks at it.
tax     = model.AddRequirement(lib.GetType("taxonomy::gtdbtk_raw"), parents={study})

out_db  = model.AddProduct(lib.GetType("viromics::iphop_augmented_db"))


def _centroids(tables) -> set[str]:
    """The bin ids skani marked as their 95% cluster's medoid."""
    keep = set()
    for handle in tables:
        with open(handle.local) as f:
            header = f.readline().rstrip("\n").split("\t")
            col = {n: i for i, n in enumerate(header)}
            missing = [c for c in ("bin_id", "is_centroid_95") if c not in col]
            assert not missing, (
                f"{handle.local.name} has no {missing}; header was {header}. "
                "skani_dedup writes this shape -- a rename is a defect there."
            )
            for line in f:
                if not line.strip():
                    continue
                r = line.rstrip("\n").split("\t")
                if r[col["is_centroid_95"]] == "1":
                    keep.add(r[col["bin_id"]])
    return keep


def protocol(context: ExecutionContext):
    ibase = context.Input(base)
    threads = context.params.get("cpus", 8)

    keep = _centroids(context.InputGroup(cluster))
    Log.Info(f"{len(keep)} centroid bins across the survey")

    # One fasta per genome, named for the genome, because that basename is what
    # add_to_db matches against the tree's leaf labels -- and it is the same bin
    # id skani clustered on and GTDB-Tk classified under `user_genome`.
    mag_dir = Path("mags")
    mag_dir.mkdir(exist_ok=True)
    n = 0
    for handle in context.InputGroup(bin):
        stem = handle.local.stem
        if stem not in keep:
            continue
        (mag_dir/f"{stem}.fna").write_bytes(handle.local.read_bytes())
        n += 1
    Log.Info(f"staged {n} MAGs for the host database")

    o = context.Output(out_db)

    trees = [h.container for h in context.InputGroup(tax)]
    assert trees, "no gtdbtk de_novo output reached add_to_db"
    if n == 0:
        # Nothing to add. Handing the base database through unchanged keeps the
        # data dependency that orders this before predict, and predict against
        # the shipped database is a real answer rather than a failure.
        Log.Info("no centroid MAG survived; passing the shipped database through")
        o.local.symlink_to(ibase.local)
        return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())

    # One call per GTDB-Tk directory, chained. add_to_db takes a single
    # --gtdb_dir and reads exactly one bacterial and one archaeal decorated tree
    # out of it, so N per-sample directories cannot be merged into one without
    # fabricating a tree that does not describe the taxonomy beside it. Chaining
    # is what the tool supports: each round adds the genomes named in that
    # round's tree and leaves the rest alone, because a genome absent from the
    # tree-taxonomy table is simply never looked up.
    steps = []
    db_in = ibase.container
    for i, tree_dir in enumerate(trees):
        db_out = f"./db_round_{i}"
        steps.append(
            f"iphop add_to_db --fna_dir {mag_dir} --gtdb_dir {tree_dir} "
            f"--db_dir {db_in} --out_dir {db_out} -t {threads}"
        )
        db_in = db_out
    _cmd = "\n".join(steps) + f"\nmv {db_in} ./iphop_augmented\n"
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    staged = Path("iphop_augmented")
    for half in ("db", "db_infos"):
        assert (staged/half).is_dir(), f"augmented database is missing {half}/"
    staged.rename(o.local)

    return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={out_db: "iphop_db"},
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=24)),
)

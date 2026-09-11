# Antonio's step 21b: the survey's own MAGs are added to iPHoP's host database
# before any host is predicted. This is one of the two transforms that make the
# viral and MAG lanes one template rather than two.
#
# The three collected slots are joined on bin id inside the task, not by lineage:
# GTDB-Tk runs on each binner's bins while the quality pool is the aggregator's
# output, so no single quality bin has one gtdbtk ancestor to recover. gtdbtk's
# `user_genome` column names the bin file, which is the join every consumer of
# these two files already uses.
import re
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


# add_to_db globs the tree itself at either the top of the directory or under `infer/`,
# but the tree-taxonomy only under `infer/`. Read both, because which one de_novo_wf
# writes has moved between GTDB-Tk versions.
def _domain_files(tree_dir: Path, domain: str, suffix: str) -> list[Path]:
    found = []
    for parent in (tree_dir, tree_dir/"infer"):
        found += sorted(parent.glob(f"gtdbtk.{domain}.{suffix}"))
    return found


_LEAF = re.compile(r"[(,]\s*([^(),:;]+)\s*:")


def _leaves(newick: str) -> set[str]:
    return {m.group(1).strip() for m in _LEAF.finditer(newick)}


def _tree_members(tree_dir: Path) -> set[str]:
    members = set()
    for domain in ("bac120", "ar122"):
        for table in _domain_files(tree_dir, domain, "decorated.tree-taxonomy"):
            with open(table) as f:
                for line in f:
                    if line.strip():
                        members.add(line.split("\t")[0].strip())
    return members


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
    staged_mags: dict[str, Path] = {}
    for handle in context.InputGroup(bin):
        stem = handle.local.stem
        if stem not in keep:
            continue
        dest = mag_dir/f"{stem}.fna"
        dest.write_bytes(handle.local.read_bytes())
        staged_mags[stem] = dest
    n = len(staged_mags)
    Log.Info(f"staged {n} MAGs for the host database")

    o = context.Output(out_db)

    tree_handles = list(context.InputGroup(tax))
    assert tree_handles, "no gtdbtk de_novo output reached add_to_db"
    if n == 0:
        # Nothing to add. Handing the base database through unchanged keeps the
        # data dependency that orders this before predict, and predict against
        # the shipped database is a real answer rather than a failure.
        Log.Info("no centroid MAG survived; passing the shipped database through")
        o.local.symlink_to(ibase.local)
        return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())

    # ONE call against ONE merged GTDB-Tk directory, because chaining a call per
    # directory does not work and cannot be made to. add_to_db treats the tree it is
    # handed as the authoritative membership list: it rebuilds Host_Genomes.tsv keeping
    # only rows whose representative appears in that round's tree-taxonomy. So round 1,
    # carrying the second sample's tree, DELETES the genomes round 0 added. Measured on
    # the test database: two bacterial MAGs added in round 0, one archaeal MAG in round 1,
    # and one row in the result. It also copies the whole database per round, which for
    # the real Aug23 release is 350 GB of copying per extra directory.
    #
    # Merging is sound because add_to_db never reads the tree's topology. It uses the
    # tree twice, both times as a set: `get_tree_members` marks a user genome as a new
    # representative if it is a leaf, and the tree is then copied verbatim into the new
    # db_infos. So the union of the taxonomy tables plus a tree carrying every new MAG as
    # a leaf is exactly what one de_novo_wf over all the MAGs would have handed it, which
    # is how the paper ran it -- the per-binner split is ours, not theirs.
    merged = Path("gtdb_merged/infer")
    merged.mkdir(parents=True, exist_ok=True)
    for domain in ("bac120", "ar122"):
        rows: dict[str, str] = {}
        best_tree, best_leaves = None, -1
        for handle in tree_handles:
            for table in _domain_files(handle.local, domain, "decorated.tree-taxonomy"):
                with open(table) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        gid, _, taxon = line.rstrip("\n").partition("\t")
                        rows.setdefault(gid, taxon)
            for tree in _domain_files(handle.local, domain, "decorated.tree"):
                leaves = _leaves(tree.read_text())
                if len(leaves) > best_leaves:
                    best_tree, best_leaves = tree, len(leaves)
        if best_tree is None:
            continue
        newick = best_tree.read_text()
        present = _leaves(newick)
        graft = [g for g in rows if g in staged_mags and g not in present]
        if graft:
            body = newick.strip().rstrip(";").strip()
            newick = f"({body}," + ",".join(f"{g}:0.1" for g in graft) + ");\n"
        (merged/f"gtdbtk.{domain}.decorated.tree").write_text(newick)
        with open(merged/f"gtdbtk.{domain}.decorated.tree-taxonomy", "w") as f:
            for gid, taxon in rows.items():
                f.write(f"{gid}\t{taxon}\n")
        Log.Info(
            f"{domain}: {len(rows)} taxonomy rows from {len(tree_handles)} directories,"
            f" {best_leaves} tree leaves, {len(graft)} MAGs grafted"
        )

    named = sum(1 for stem in staged_mags if any(
        stem in _tree_members(h.local) for h in tree_handles))
    if named == 0:
        Log.Warn("no gtdbtk directory named a centroid MAG; passing the base database through")
        o.local.symlink_to(ibase.local)
        return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())
    Log.Info(f"{named} of {n} staged MAGs are named by a decorated tree")

    # A round's output is NOT a complete database: add_to_db silently drops
    # `db/wish_data/`, the decoy phage set WIsH fits its null model against, so the
    # product would be missing it. Backfill every entry the base database has and the
    # output lacks. It is 28 MB.
    backfill = "\n".join(
        f"""
        for src in {ibase.container}/{sub}/*; do
            dst="./iphop_augmented/{sub}/$(basename "$src")"
            [ -e "$dst" ] || cp -r "$src" "$dst"
        done
        """
        for sub in ("db", "db_infos")
    )

    _cmd = (
        f"iphop add_to_db --fna_dir {mag_dir} --gtdb_dir ./gtdb_merged "
        f"--db_dir {ibase.container} --out_dir ./iphop_augmented -t {threads}\n"
        f"{backfill}\n"
    )
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    staged = Path("iphop_augmented")
    for half in ("db", "db_infos", "db/wish_data"):
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

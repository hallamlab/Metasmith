# Antonio's step 21b: the survey's own MAGs are added to iPHoP's host database
# before any host is predicted. This is one of the two transforms that make the
# viral and MAG lanes one template rather than two.
#
# The three collected slots are joined on bin id inside the task, not by lineage:
# GTDB-Tk runs on each binner's bins while the quality pool is the aggregator's
# output, so no single quality bin has one gtdbtk ancestor to recover. gtdbtk's
# `user_genome` column names the bin file, which is the join every consumer of
# these two files already uses.
import json
import textwrap
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
tax     = model.AddRequirement(lib.GetType("taxonomy::gtdbtk_raw"), parents={study, bin})
graft   = model.AddRequirement(lib.GetType("lib::iphop_graft.py"))

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

    # ONE call against ONE GTDB-Tk directory, because chaining a call per directory does
    # not work: add_to_db treats the tree it is handed as the authoritative membership list
    # and rebuilds Host_Genomes.tsv keeping only rows whose representative that round's
    # tree-taxonomy names. Round 1 deletes what round 0 added, and each round copies the
    # whole database.
    #
    # The directory holds the base database's OWN trees with the MAGs grafted in, not the
    # de_novo trees. A de_novo tree names GTDB's representatives alone, so on Jun25_rw it
    # cost 133,705 of 1,155,965 hosts, 48,379 of them iPHoP's own IMG and MGN
    # representatives that no GTDB-Tk tree carries. And predict walks the tree between
    # hosts, so a host it cannot find there fails the whole prediction.
    igraft = context.Input(graft)
    trees = " ".join(str(h.container) for h in tree_handles)
    context.ExecWithEnv(
        env=image,
        cmd=f"python {igraft.container} --base {ibase.container} --mags {mag_dir} "
            f"--out ./gtdb_merged {trees} > graft.json",
    )
    summary = json.loads(Path("graft.json").read_text())
    named = sum(len(d["mags"]) for d in summary.values())
    for marker, d in summary.items():
        Log.Info(f"{marker}: {d['base_leaves']} base leaves, {d['grafted']} MAGs grafted"
                 f" beside a base leaf, {d['at_root']} at the root")
    if named == 0:
        Log.Warn("no gtdbtk directory placed a centroid MAG; passing the base database through")
        o.local.symlink_to(ibase.local)
        return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())
    Log.Info(f"{named} of {n} staged MAGs are in a tree")

    context.ExecWithEnv(
        env=image,
        cmd=f"iphop add_to_db --fna_dir {mag_dir} --gtdb_dir ./gtdb_merged "
            f"--db_dir {ibase.container} --out_dir ./iphop_augmented -t {threads}",
    )

    # add_to_db's output is not a database on its own. It omits `db/wish_data/` and more,
    # it symlinks rafah_data and rewish_models into the base, and its Host_Genomes is a
    # BLAST alias naming the base volumes and the new genomes by ABSOLUTE path, the second
    # being this task's own work dir, gone once the product is promoted. Each is
    # hardlinked in from the base and the alias is made relative, so the product reads
    # nothing outside itself. That needs one filesystem with the cache, hence `noscratch`,
    # and the host's shell, because the protocol's container reaches the work dir and the
    # base through two binds of that filesystem, and link() refuses to cross them (EXDEV).
    relocate = Path("relocate_db.sh")
    relocate.write_text(textwrap.dedent(f"""
        set -euo pipefail
        B=$(readlink -f {ibase.external})
        A={context.external_cwd}/iphop_augmented
        for sub in db db_infos; do
            for src in $B/$sub/*; do
                dst="$A/$sub/$(basename "$src")"
                [ -e "$dst" ] || [ -L "$dst" ] || cp -al "$src" "$dst"
            done
        done
        find $A -maxdepth 2 -type l | while read -r link; do
            target=$(readlink -f "$link")
            rm "$link"
            cp -al "$target" "$link"
        done
        mkdir $A/db/Host_Genomes/base
        cp -al $B/db/Host_Genomes/. $A/db/Host_Genomes/base/
        sed -i 's|^DBLIST .*|DBLIST "base/Host_Genomes" "New_host_genomes"|' $A/db/Host_Genomes/Host_Genomes.nal
    """))
    res = context.external_shell.Exec(f"bash {context.external_cwd/relocate}", timeout=None, history=True)
    if res.exit_code != 0:
        Log.Error(f"relocating the augmented database exited [{res.exit_code}]")
        return ExecutionResult(manifest=[], success=False)

    staged = Path("iphop_augmented")
    for half in ("db", "db_infos", "db/wish_data"):
        assert (staged/half).is_dir(), f"augmented database is missing {half}/"
    staged.rename(o.local)

    return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    labels=["noscratch"],
    output_signature={out_db: "iphop_db"},
    # On fir with 42 MAGs against Jun25_rw: 3 m 20 s and 19.8 GB, the graft 23 s.
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=4)),
)

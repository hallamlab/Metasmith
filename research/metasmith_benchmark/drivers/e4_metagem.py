#!/usr/bin/env python3
"""E4: metaGEM's published MAGs through Prodigal, CarveMe (CPLEX) and MEMOTE, and GTDB-Tk r232.

`run` solves against a local dry-run home and renders the DAG. `import`, `--stage-only`
and `--launch` act on the fir agent home, from a Slurm job there, after
e4_extract_mags.sh has unpacked the MAGs.

Subcommands: list, import, run [--study ...] [--limit N] [--with-gtdbtk] [--dag] [--stage-only | --launch].
"""

import argparse
import csv
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E4_CACHE_DIR", HERE / ".cache" / "e4"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

MAG_LIST = HERE / "e4_published_mags.tsv"
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
STUDY_ORDER = ["li2019", "korem2015", "karlsson2013", "bissett_base", "sunagawa2015"]
MEDIUM_TSV = c.REPO / "research" / "metasmith_libraries" / "carveme_m8_medium.tsv"
MEDIUM_NAME = "M8"
# metaGEM's workflow/scripts/media_db.tsv, the table its SMETANA rule passes as --mediadb.
SMETANA_MEDIA_DB = Path(__file__).parent / "refs" / "metagem_media_db.tsv"
# SMETANA scores one sample's GEMs together, which needs MAG givens under a sample root. Those are new
# pool entries with new identities, so their models are built again: the lane is one study, not all five.
SMETANA_STUDY = "li2019"
CPLEX_ROOT = Path(os.environ.get("CPLEX_ROOT", "/home/phyberos/projects/rpp-shallam/phyberos/cplex/cplex_runtime"))

# First-attempt (cpus, GB, hours); the retry gets 32 GB and 24 h. B11: 3.75% of gapfills failed at the
# transform's 2 h wall or on memory, with MaxRSS up to 14.6 GiB and one OOM at 32 GiB.
SCALED = {"carveme_from_orfs_cplex": (4, 16, 12)}
# Chunk size is a SAFETY lever, not a capacity one. A MAG costs 54.8 inodes while its chunk is live and
# leaves 17.84 behind, both measured rather than estimated, so the end state after all 12,108 remaining
# MAGs is ~939,000 of the 1,000,000 inode quota whatever this value is -- the residue term dominates and
# shrinking a chunk only shrinks the live term, buying at most ~35K of peak relief. What it does buy is
# room for the guard to sit ABOVE the peak and still warn: at 500 the worst chunk peaks at 958,388, which
# leaves quota_stop.sbatch's INODE_STOP=970,000 both 11,612 clear of a healthy run and 30,000 short of
# the wall. At 1000 the peak is 976,888 and no threshold is both, so the guard could only ever be a
# tripwire that aborts a good chunk. Cost of 500 is 25 chunks instead of 13.
#
# CAUTION chunk boundaries are pinned by the tracked e4_published_mags.tsv, not by the filesystem, so
# changing this renumbers every chunk. The completed run lE94xbfH took sorted(mags)[0:2000] -- 1,996 of
# 2,000 models banked in metaGEM's task_cache -- which is exactly new chunks 1 through 4. Resume at
# chunk 5 and run through chunk 29; chunk 29 carries the remaining 108.
CHUNK_SIZE = 500


def enumerate_mags():
    rows = list(csv.DictReader(MAG_LIST.open(), delimiter="\t"))
    return [(r["study"], r["mag"], PUBLISHED / r["study"] / "mags" / f"{r['mag']}.fa") for r in rows]


def select(mags, args):
    if args.study:
        unknown = set(args.study) - set(STUDY_ORDER)
        if unknown:
            sys.exit(f"unknown study: {sorted(unknown)}")
        mags = [m for m in mags if m[0] in args.study]
    if args.limit:
        per_study = defaultdict(list)
        for m in mags:
            per_study[m[0]].append(m)
        mags = [m for s in STUDY_ORDER for m in per_study[s][: args.limit]]
    if args.chunk:
        # One run of all 14,108 MAGs needs ~635K inodes. Chunks share the metaGEM home, so their results pool.
        mags = sorted(mags)[(args.chunk - 1) * CHUNK_SIZE: args.chunk * CHUNK_SIZE]
    return mags


def declare_globals(smith, solver, location, ensure, with_gtdbtk=False, with_checkm2=False, with_metapop=False,
                    with_smetana=False):
    """The medium, the solver, GTDB, CheckM2's database, MetaPop's image and SMETANA's media, as one resource library."""
    givens = smith.PoolGivens()
    c.add_value(givens, "ref/modelling::media", MEDIUM_TSV.read_text(), "modelling::media", tags=["reference"])
    c.add_value(givens, "ref/modelling::medium_name", MEDIUM_NAME, "modelling::medium_name", tags=["reference"])
    if solver == "cplex":
        c.declare_refs(givens, {"modelling::cplex_installation": CPLEX_ROOT})
    if with_gtdbtk:
        c.declare_refs(givens, {"ref::gtdb": c.STAGED_REFS["ref::gtdb"], **c.GTDB_GENOMES_IMAGE})
    if with_checkm2:
        c.declare_refs(givens, {"bench::checkm2_database": c.CHECKM2_DB})
    if with_metapop:
        c.declare_refs(givens, c.METAPOP_ENV_IMAGE)
    if with_smetana:
        c.add_value(givens, "ref/bench::smetana_media_db", SMETANA_MEDIA_DB.read_text(), "bench::smetana_media_db",
                    tags=["reference"])
    types = [c.MLIB / "data_types" / t for t in ("modelling.yml", "ref.yml")] + [c.LIBRARY / "data_types" / "bench.yml"]
    return c.cite(givens, location, types, ensure)


def declare_givens(smith, mags, ensure, name="all"):
    givens = smith.PoolGivens()
    for study, mag, path in mags:
        c.add_file(givens, f"e4/{study}/{mag}", path, "sequences::bin_fasta", tags=["e4", study])
    return c.cite(givens, CACHE_DIR / f"e4_inputs_{name}.xgdb", [c.MLIB / "data_types" / "sequences.yml"], ensure)


def sample_of(mag):
    """metaGEM names a MAG for the run it came from: ERR671910.bin.1.orig, ERR260137_bin.1.p."""
    return re.split(r"[._]bin\.", mag)[0]


def declare_smetana_givens(smith, ensure):
    """SMETANA's own givens: one community root per sample, with that sample's MAGs under it.

    A pool entry's parents are fixed at import, so the ungrouped `e4/<study>/<mag>` givens cannot gain a
    sample parent; these are separate names, and their models are built again rather than reused. That is
    why the lane is scoped to one study: 172 CarveMe runs, not 14,108.
    """
    givens = smith.PoolGivens()
    rows = [(s, m, p) for s, m, p in enumerate_mags() if s == SMETANA_STUDY]
    assert rows, f"no MAGs for {SMETANA_STUDY}"
    roots = {}
    for study, mag, path in rows:
        sample = sample_of(mag)
        tags = ["e4", "smetana", study, sample]
        if sample not in roots:
            roots[sample] = c.add_value(givens, f"e4s/{study}/{sample}", sample, "bench::gem_community", tags=tags)
        c.add_file(givens, f"e4s/{study}/{sample}/{mag}", path, "sequences::bin_fasta",
                   parents=[roots[sample]], tags=tags)
    print(f"SMETANA lane: {len(rows)} {SMETANA_STUDY} MAGs in {len(roots)} samples")
    return c.cite(givens, CACHE_DIR / "e4_smetana_inputs.xgdb",
                  [c.MLIB / "data_types" / "sequences.yml", c.LIBRARY / "data_types" / "bench.yml"], ensure)


def build_targets(solver, with_gtdbtk=False, with_smetana=False):
    t = TargetBuilder()
    orfs = t.Add("sequences::bin_orfs")
    model_type = "modelling::carveme_model_cplex" if solver == "cplex" else "modelling::carveme_model"
    model = t.Add(model_type, parents=[orfs])
    t.Add("modelling::memote_score", parents=[model])
    if with_gtdbtk:
        t.Add("taxonomy::gtdbtk")
    if with_smetana:
        t.Add("bench::smetana_detailed_cplex", parents=[model])
    return t


def cmd_list(args):
    mags = select(enumerate_mags(), args)
    for study, n in sorted(Counter(m[0] for m in mags).items(), key=lambda kv: STUDY_ORDER.index(kv[0])):
        print(f"{study:14s} {n:5d} MAGs")
    print(f"{len(mags)} MAGs")
    return 0


def cmd_run(args):
    mags = select(enumerate_mags(), args)
    if not mags:
        sys.exit("no MAGs selected")
    print(f"{len(mags)} MAGs: " + ", ".join(f"{s}={n}" for s, n in Counter(m[0] for m in mags).items()))

    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    smith = c.agent_for("metagem", remote, CACHE_DIR / "dryrun_home")
    ensure = importing or args.import_givens or not remote
    # The name keys the inputs library and the task-key record, so it carries every filter.
    name = "_".join([f"chunk{args.chunk}" if args.chunk else "all", *(args.study or []),
                     *([f"limit{args.limit}"] if args.limit else [])])
    if args.with_smetana:
        # Its own sample-rooted inputs library, so the ungrouped MAG givens stay as they are.
        inputs = declare_smetana_givens(smith, ensure)
    else:
        inputs = declare_givens(smith, mags, ensure, name)
    globals_lib = declare_globals(smith, args.solver, CACHE_DIR / "e4_globals.xgdb", ensure, args.with_gtdbtk,
                                  with_smetana=args.with_smetana)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {len(mags)} MAGs")
        return 0

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::bin_fasta")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), globals_lib,
                   # bench::smetana.env lives here, and nothing produces it.
                   *([DataInstanceLibrary.Load(c.LIBRARY / "resources" / "bench")] if args.with_smetana else [])],
        transforms=[TransformInstanceLibrary.Load(c.MLIB / "transforms" / "logistics"),
                    TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metabolicModelling")
                    .AsView({Path("memote_score.py")}, invert=True),
                    TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "modelling"),
                    TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench")
                    # SMETANA is three transforms, not one: the media are split per medium, scored one
                    # medium per task, then merged back per sample. The view is a whitelist, so all
                    # three have to be named or the lane dead-ends at bench::smetana_detailed_cplex.
                    .AsView({Path("gtdbtk_image.py")} | ({Path("smetana_cplex_split_media.py"),
                                                          Path("smetana_cplex_medium.py"),
                                                          Path("smetana_cplex_merge.py")}
                                                         if args.with_smetana else set()))],
        targets=build_targets(args.solver, args.with_gtdbtk, args.with_smetana),
    )
    n_bins = sum(1 for s, _, _ in enumerate_mags() if s == SMETANA_STUDY) if args.with_smetana else len(mags)
    expected = {"sequences::bin_fasta": n_bins, "modelling::media": 1, "modelling::medium_name": 1}
    if args.with_smetana:
        expected["bench::smetana_media_db"] = 1
    if args.solver == "cplex":
        expected["modelling::cplex_installation"] = 1
    if args.with_gtdbtk:
        expected.update({"ref::gtdb": 1, "bench::gtdb_genomes_image": 1})
    c.check_plan(task, expected)
    c.print_plan(task, 28)

    if args.dag:
        c.write_dag(task, "e4_metagem", CACHE_DIR)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR, args.tag or f"e4_{name}", stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=500), process=dict(tries=2, array=100)),
                        scaled=SCALED, materialise=args.materialise)
    else:
        print("(dry run; nothing staged or submitted)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("import", cmd_run), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--study", nargs="*", help=", ".join(STUDY_ORDER))
        p.add_argument("--limit", type=int, help="first N MAGs per study")
        p.add_argument("--chunk", type=int, help=f"1-based chunk of {CHUNK_SIZE} MAGs, sorted by study and MAG")
        p.set_defaults(fn=fn, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       with_gtdbtk=False, with_smetana=False)
        if name != "list":
            p.add_argument("--solver", default="cplex", choices=["cplex", "open"])
            p.add_argument("--with-gtdbtk", action="store_true",
                           help="GTDB-Tk r232 on every MAG; needs ref::gtdb's representative genomes")
            p.add_argument("--with-smetana", action="store_true",
                           help=f"metaGEM's SMETANA lane on {SMETANA_STUDY}: one community per sample, CPLEX. "
                                "Its own sample-rooted givens, so its CarveMe models are built again.")
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e4_metagem.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            mode.add_argument("--materialise", action="store_true", help="stage, fetch every image the plan needs, stop")
            p.add_argument("--tag")
            p.add_argument("--import", dest="import_givens", action="store_true",
                           help="import what the pool lacks before planning, as `import` does")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

"""publish_r1.py — assemble the r1 results into the published tree on fir.

Destination mirrors `spanish_lakes/metagenomics/`, so anyone who knows that tree
knows this one:

  ~/project-rpp/steven_c_gmcf3495/metagenomics/

The run computes on fir and publishes on fir, so this is a filesystem assembly,
not a transfer. The brain is local (attribution); the hands are remote (a
generated shell script run over ssh). Nothing is decided on the cluster side.

WHY NAMES HAVE TO BE DERIVED RATHER THAN READ
Everything metasmith publishes is content-hashed: `1-1-1.<hash>-<hash>.fna` says
nothing about which sample it belongs to. The sample id survives into the run
only in the read filenames, so every published name is derived by walking each
product back to those.

*** NOT THROUGH METASMITH'S OWN LINEAGE GRAPH. *** It is wrong on 0.19.1 and
wrong quietly: see `nxf_attribution.py`, which carries the evidence and is what
this reads instead. The consequence for this file is that publishing off that
graph would have given files the wrong sample's name, and the result would have
looked entirely normal.

Two of the DAG's own products cannot be labelled from any graph, and are
published from the gap-fill catalogue instead -- see `_route_catalogue`.

WHY BIN QUALITY COMES FROM THE CATALOGUE, NOT FROM `taxonomy::checkm_stats`
checkm ran batched across assemblies rather than per sample, so 716 of its 781
per-bin CSVs descend from several samples at once and only 65 from one. And
`binning_local::quality_bin_fasta` has all three binners upstream of every bin
by construction, so its binner is not recoverable either. The catalogue carries
both by construction and covers more bins besides -- the ones the aggregator's
all-three-binners gate discarded, and the gap-filled ones that never entered the
DAG.

WHY FAN-OUTS ARE TARRED
rpp-shallam has 361K of 500K inodes used. Spanish Lakes' bin fastas alone are
31,149 files. Per-bin products therefore ship as one tarball per sample plus a
flat summary table, which keeps a few thousand bins to a few dozen inodes.

Usage:
  python publish_r1.py plan                 # resolve + report, write nothing
  python publish_r1.py run --dry-run        # render the remote script, show it
  python publish_r1.py run                  # assemble on fir
  python publish_r1.py readme               # (re)write the tree README only
"""
import os
import re
import sys
import json
import shlex
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict

import run_arbutus_campaigns as C

ROOT = Path(__file__).resolve().parent
DEST = os.environ.get(
    "MSM_PUBLISH_DEST",
    "/home/phyberos/project-rpp/steven_c_gmcf3495/metagenomics",
)
GTDB_RELEASE = "r232"

# dtype -> (destination subdirectory, file extension, mode)
#
# mode:
#   "sample"  one file per sample, named <sample>.<ext>
#   "bins"    per-bin fan-out -> one <sample>.tar per sample
#   "single"  exactly one file for the whole run
#
# `taxonomy::checkm_stats` and `binning_local::quality_bin_fasta` are absent on
# purpose -- neither can be labelled from the graph; `_route_catalogue` publishes
# the same information, attributed, from the gap-fill catalogue.
ROUTES = {
    "sequences::short_reads":                 ("reads/interleaved",             "fq.gz",  "sample"),
    "sequences::clean_short_reads":           ("reads/filtered",                "fq.gz",  "sample"),
    "sequences::discarded_short_reads":       ("reads/discarded",               "fq.gz",  "sample"),
    "sequences::read_qc_stats":               ("reads/qc_stats",                "json",   "sample"),

    "sequences::megahit_assembly":            ("assembly/fna",                  "fna",    "sample"),
    "sequences::orfs":                        ("assembly/orfs",                 "faa",    "sample"),
    "sequences::gff":                         ("assembly/gffs",                 "gff",    "sample"),
    "sequences::assembly_stats":              ("assembly/qc_stats",             "json",   "sample"),
    "sequences::assembly_per_contig_coverage":("assembly/coverage_per_contig",  "tsv",    "sample"),
    "sequences::assembly_per_bp_coverage":    ("assembly/coverage_per_bp",      "tsv.gz", "sample"),
    "alignment::bam":                         ("assembly/alignment_bams",       "bam",    "sample"),

    "taxonomy::kraken2_classifications":      ("taxonomy_reads/kraken2_classifications",     "parquet", "sample"),
    "taxonomy::kraken2_report":               ("taxonomy_reads/kraken2_report",              "txt",     "sample"),
    "taxonomy::bracken_species":              ("taxonomy_reads/bracken_species",             "tsv",     "sample"),
    "taxonomy::bracken_kreport":              ("taxonomy_reads/bracken_kreport",             "txt",     "sample"),
    "taxonomy::centrifuger_classifications":  ("taxonomy_reads/centrifuger_classifications", "parquet", "sample"),
    "taxonomy::centrifuger_kreport":          ("taxonomy_reads/centrifuger_kreport",         "txt",     "sample"),
    "taxonomy::centrifuger_summary":          ("taxonomy_reads/centrifuger_summary",         "tsv",     "sample"),

    "annotation::diamond_uniref50_results":   ("annotation_orfs/diamond_uniref50", "tsv",     "sample"),
    "annotation::kofamscan_results":          ("annotation_orfs/kofamscan",        "csv",     "sample"),
    "annotation::eggnog_results":             ("annotation_orfs/eggnog_mapper",    "tsv",     "sample"),
    "annotation::proteinbert_embeddings":     ("annotation_orfs/proteinbert",      "parquet", "sample"),
    "annotation::proteinbert_index":          ("annotation_orfs/proteinbert_index","csv",     "sample"),

    "binning::metabat2_contig_to_bin_table":  ("binning/contig_to_bin_metabat2", "tsv", "sample"),
    "binning::semibin2_contig_to_bin_table":  ("binning/contig_to_bin_semibin2", "tsv", "sample"),
    "binning::comebin_contig_to_bin_table":   ("binning/contig_to_bin_comebin",  "tsv", "sample"),

    "sequences::metabat2_bin_fasta":          ("binning/fna_metabat2", "fa", "bins"),
    "sequences::semibin2_bin_fasta":          ("binning/fna_semibin2", "fa", "bins"),
    "sequences::comebin_bin_fasta":           ("binning/fna_comebin",  "fa", "bins"),
}

# `binning_local::cluster_table` is deliberately absent. It is the aggregator's
# per-assembly skani clustering, computed over the gated subset (an assembly
# contributes nothing unless all three binners produced bins), and it was
# recomputed over the complete post-gap-fill bin set -- see `_route_catalogue`.
# Publishing both would put two disagreeing cluster tables in one tree with
# nothing on disk saying which is current. There were also 8 of them, all under
# one dtype declared `single`, so they all claimed `binning/cluster_table.tsv`;
# `render_script`'s clash check catches that, but the fix is the supersession,
# not a rename.


def resolve(task_key=None, refresh=True):
    """Return (run_dir, key, attrib, products).

    *** THIS NO LONGER READS METASMITH'S LINEAGE GRAPH. *** On 0.19.1 that graph
    disagrees with what nextflow actually ran -- 0 of 13 interleave steps agreed
    on the run it was measured against, and one product resolved to exactly one
    sample and the wrong one. Every published filename here is derived from an
    attribution, so publishing off that graph would have named files for the
    wrong libraries with nothing about the output looking wrong. The evidence and
    the replacement are in nxf_attribution.py; both drivers now share it.
    """
    run_dir, key = C.find_run_dir(task_key)
    attrib = C.attribution(run_dir, refresh=refresh)
    products = C.published_products(run_dir)
    return run_dir, key, attrib, products


def _sample_of(attrib, product):
    """The one sample a product descends from, or None if not exactly one."""
    row = attrib.get(product)
    if not row:
        return None
    return row["sample"] if row["sample"] not in ("?", "*") else None


# Products the DAG published short, rebuilt by gapfill/pbert_remerge.py. Keyed
# by dtype -> (rebuilt file extension, ) since the rebuilt files are named
# <sample>.<ext> under GAPFILL/pbert.
REBUILT = {
    "annotation::proteinbert_embeddings": "parquet",
    "annotation::proteinbert_index":      "csv",
}
_rebuilt_cache = {}


def _rebuilt(dtype, sample):
    """A rebuilt replacement for this product, or None to use the DAG's own.

    `merge_proteinbert` is grouped by the sample's ORFs and inherits metasmith's
    group() drop race, but drops chunks *within* a group rather than whole
    samples: the merge runs, exits 0, and publishes fewer rows than the sample
    has ORFs. Four samples were short and one (S19) lost its group entirely, so
    no merge task existed for it at all. Nothing about the run said so -- it was
    found by counting merged index rows against ORFs in the published .faa.

    Substituting here rather than adding a second item keeps one source per
    destination, which is what `render_script`'s clash check requires.
    """
    ext = REBUILT.get(dtype)
    if not ext:
        return None
    return _rebuilt_index().get((ext, sample))


def _rebuilt_index():
    """(ext, sample) -> path, for whatever pbert_remerge.py has produced."""
    if not _rebuilt_cache:
        listing = C.ssh_out(f"ls {GAPFILL}/pbert/*.parquet {GAPFILL}/pbert/*.csv "
                            f"2>/dev/null || true")
        for p in listing.split():
            _rebuilt_cache[(Path(p).suffix.lstrip("."), Path(p).stem)] = p
        _rebuilt_cache[("__probed__", "")] = ""
    return _rebuilt_cache


def _rebuilt_samples(ext):
    return sorted(s for e, s in _rebuilt_index() if e == ext)


def build_manifest(attrib, products, run_dir, catalogue=True):
    """[(remote_src, dest_relpath, mode, sample)], plus a list of problems.

    `catalogue` covers the whole run rather than one run dir, so the second
    publish pass -- the scoped S13/S22 run, which lands in its own run dir --
    passes False. Emitting it twice would put both passes' items on the same
    destinations and trip `render_script`'s clash check.
    """
    items, problems = [], []

    # counters give every per-bin product a stable ordinal within its sample
    ordinal = defaultdict(int)

    for dtype, (subdir, ext, mode) in ROUTES.items():
        for product, remote_path in sorted(products.get(dtype, {}).items()):
            if mode == "single":
                items.append((remote_path, f"{subdir}/cluster_table.tsv", "file", None))
                continue
            sample = _sample_of(attrib, product)
            if sample is None:
                problems.append((dtype, product, "no unique sample"))
                continue
            if mode == "sample":
                src = _rebuilt(dtype, sample) or remote_path
                items.append((src, f"{subdir}/{sample}.{ext}", "file", sample))
            else:  # bins -> staged into a per-sample tar
                ordinal[(subdir, sample)] += 1
                n = ordinal[(subdir, sample)]
                binner = subdir.rsplit("_", 1)[-1]
                member = f"{sample}.{binner}.{n:04d}.{ext}"
                items.append((remote_path, f"{subdir}/{sample}/{member}", "tar", sample))

    # A rebuilt product usually *replaces* one the DAG published short, and the
    # substitution above covers that. S19 is different: group() lost its whole
    # group, so no merge task ran and there is no product to substitute for.
    # Nothing would publish it unless it is added outright.
    claimed = {d for _, d, _, _ in items}
    for dtype, ext in REBUILT.items():
        subdir = ROUTES[dtype][0]
        for sample in _rebuilt_samples(ext):
            dest = f"{subdir}/{sample}.{ext}"
            if dest not in claimed:
                items.append((f"{GAPFILL}/pbert/{sample}.{ext}", dest, "file", sample))

    # bin quality, the census, the clustering and the MAGs
    if catalogue:
        items_c, problems_c = _route_catalogue()
        items += items_c
        problems += problems_c

    items_ar, problems_ar = _route_arbutus()
    items += items_ar
    problems += problems_ar
    return items, problems


# Where the campaign driver's outputs land on fir before assembly. They are the
# only products that do not already live there.
ARBUTUS_STAGE = "/scratch/phyberos/gmcf3495/.publish_arbutus"

# campaign -> (local subdir under STAGING/<campaign>, destination subdir)
ARBUTUS_ROUTES = {
    "metabuli": ("per_sample", "taxonomy_contigs/metabuli"),
    "gtdbtk":   ("merged",     "taxonomy_binning/gtdbtk"),
}


def _route_arbutus():
    """Route the two off-cluster campaigns' products.

    Everything else in the manifest comes out of the metasmith lineage graph and
    already sits on fir under `<run_dir>/results/`. These do not: metabuli and
    GTDB-Tk ran on Arbutus, and their tables came back to *this* machine. So
    they need a real source path on fir before `render_script` -- which runs
    remotely and only copies -- can place them.

    `upload` is that: a mode `render_script` turns into an rsync from here to
    ARBUTUS_STAGE. Adding a mode rather than pre-uploading behind the manifest's
    back keeps every published file visible to `plan`, which is the one place a
    missing product is supposed to be catchable.
    """
    items, problems = [], []
    for campaign, (sub, dest) in ARBUTUS_ROUTES.items():
        src_dir = C.STAGING / campaign / sub
        if not src_dir.is_dir():
            problems.append((f"arbutus::{campaign}", str(src_dir),
                             "campaign has not produced this directory yet"))
            continue
        files = sorted(p for p in src_dir.iterdir()
                       if p.is_file() and p.suffix in (".tsv", ".txt", ".csv", ".json"))
        if not files:
            problems.append((f"arbutus::{campaign}", str(src_dir), "no tables in it"))
            continue
        for p in files:
            items.append((str(p), f"{dest}/{p.name}", "upload", None))
    return items, problems


# The gap-fill working tree on fir: the unified bin catalogue, the 34x3 binner
# census, the recomputed skani clustering, and the bins themselves.
GAPFILL = "/scratch/phyberos/gmcf3495/gapfill"

# Everything `_route_catalogue` writes, for `run --replace` to clear first. This
# layer is the only part of the tree that is recomputed rather than copied, so
# it is the only part a second publish has to be allowed to overwrite.
CATALOGUE_DEST = (
    "binning/bin_catalogue.tsv",
    "binning/binner_census.tsv",
    "binning/cluster_tables",
    "binning/quality_bins",
    # Superseded, and actively misleading if left behind: an earlier pass routed
    # `taxonomy::checkm_stats` by binner and published the one batch that
    # happened to attribute. It reads as "CheckM2 statistics for semibin2 on
    # S25" when what actually happened is that checkm ran on every bin and only
    # that batch resolved to a single sample. bin_catalogue.tsv has all of it.
    "binning/qc_stats_semibin2",
)


def _route_catalogue():
    """Route the bin catalogue, the binner census, the clustering, and the MAGs.

    These stand in for the two DAG products whose dtype cannot be labelled, and
    the substitution is not a workaround -- it is the only correct source.

      - `taxonomy::checkm_stats` is one CSV per bin, but checkm ran batched
        across assemblies: 716 of 781 products resolve to several samples and
        exactly 65 to one. Nothing about a batch boundary makes that fixable by
        walking the graph harder.
      - `binning_local::quality_bin_fasta` has all three binners upstream of
        every bin by construction, so its binner is not recoverable at all --
        see `C.binner_of`, which says so and returns None rather than guessing.

    The catalogue has both by construction. Each bin is attributed through the
    work-dir join *before* checkm sees it, and carries its own completeness and
    contamination on its own row. It also covers what the DAG's own numbers
    cannot: the bins the aggregator's all-three-binners gate discarded, and the
    gap-filled bins that never entered the DAG at all.

    Quality bins ship as one tar per sample for the same inode reason as every
    other per-bin fan-out here.
    """
    items, problems = [], []

    cat = C.ssh_out(f"cat {GAPFILL}/catalogue.tsv 2>/dev/null || true").splitlines()
    if len(cat) < 2:
        problems.append(("catalogue", f"{GAPFILL}/catalogue.tsv",
                         "no catalogue on fir; run build_catalogue.py first"))
        return items, problems

    items.append((f"{GAPFILL}/catalogue.tsv", "binning/bin_catalogue.tsv", "file", None))

    census = C.ssh_out(f"test -f {GAPFILL}/binner_status.tsv && echo y || true").strip()
    if census == "y":
        items.append((f"{GAPFILL}/binner_status.tsv",
                      "binning/binner_census.tsv", "file", None))
    else:
        problems.append(("census", f"{GAPFILL}/binner_status.tsv", "missing"))

    header = cat[0].split("\t")
    ix = {c: i for i, c in enumerate(header)}
    ordinal = defaultdict(int)
    n_unscored = 0
    for line in cat[1:]:
        f = line.split("\t")
        if len(f) < len(header):
            continue
        if f[ix["completeness"]] == "":
            n_unscored += 1
            continue
        if f[ix["quality"]] != "1":
            continue
        sample, binner, path = f[ix["sample"]], f[ix["binner"]], f[ix["path"]]
        if sample in ("", "?", "*") or not path:
            problems.append(("catalogue::quality_bin", f[ix["uid"]],
                             f"sample={sample!r} path={path!r}"))
            continue
        ordinal[sample] += 1
        member = f"{sample}.{binner}.{ordinal[sample]:04d}.fa"
        items.append((path, f"binning/quality_bins/{sample}/{member}", "tar", sample))

    # A bin with no checkm row is not a quality bin and not a non-quality bin --
    # it is unmeasured, and a catalogue that silently treats it as failing would
    # under-report the MAG count with nothing to notice. Surface it.
    if n_unscored:
        problems.append(("catalogue", "completeness", f"{n_unscored} bin(s) not yet scored"))

    listing = C.ssh_out(f"ls {GAPFILL}/skani/*/cluster_table.tsv 2>/dev/null || true")
    for p in listing.split():
        sample = Path(p).parent.name
        items.append((p, f"binning/cluster_tables/{sample}.tsv", "file", sample))
    if not listing.strip():
        problems.append(("clustering", f"{GAPFILL}/skani", "no cluster tables; run skani + cluster.py"))

    return items, problems


def render_script(items, replace=()):
    """A shell script that assembles the tree on fir.

    Per-sample tar directories are staged then tarred and removed, so the
    published tree never holds the fan-out as loose files -- the inode cost is
    paid in scratch, which has 1M inodes and 270K used, not in the project
    allocation, which has 139K left.
    """
    # Anything this loop does not recognise would be dropped from the tree with
    # no error at all -- a product silently missing from a published deliverable
    # is the worst failure this script has, because it looks exactly like
    # success. Refuse up front instead.
    unknown = sorted({m for _, _, m, _ in items} - {"file", "tar", "upload"})
    if unknown:
        raise SystemExit(f"render_script: unhandled item mode(s) {unknown}; "
                         f"build_manifest and render_script have drifted apart")

    # Same failure, different cause: two products routed to one destination.
    # Every copy below is `cpn`, which skips an existing destination, so the
    # second product would vanish without a line of output anywhere -- not in
    # `plan`, which counts items rather than destinations, and not in the
    # run, which would report success. Two sources on one path is always a
    # routing bug (a "single" dtype that produced two nodes, or a dtype with
    # more than one product per sample routed as `sample`), so refuse and name
    # both sources rather than let the tree be quietly short. An exact repeat of
    # the same source is merely redundant and collapses.
    dests = defaultdict(set)
    for src, dst, _, _ in items:
        dests[dst].add(src)
    clashes = sorted((d, sorted(s)) for d, s in dests.items() if len(s) > 1)
    if clashes:
        detail = "\n".join(f"  {d}\n" + "".join(f"      <- {s}\n" for s in srcs)
                           for d, srcs in clashes)
        raise SystemExit(
            f"render_script: {len(clashes)} destination(s) claimed by more than "
            f"one product; `cpn` would drop all but the first silently:\n{detail}")
    items = list(dict.fromkeys(items))

    lines = ["set -euo pipefail", f"DEST={shlex.quote(DEST)}",
             f"ARB={shlex.quote(ARBUTUS_STAGE)}",
             'STAGE=$(mktemp -d /scratch/phyberos/.publish.XXXXXX)',
             'trap "rm -rf $STAGE" EXIT',
             "",
             # Not `cp -n`: coreutils 9.3 (fir's) exits 1 when -n skips, which
             # under `set -e` aborts the whole publish at the first destination
             # that already exists. The first publish only worked because the
             # tree was empty; every re-publish died on reads/filtered/S24.fq.gz.
             # Skip-if-present has to be the test, not the copy's exit status.
             'cpn() { [ -e "$2" ] || cp "$1" "$2"; }',
             ""]
    # Every copy below is `cpn`, so a second publish over an existing tree is a
    # no-op -- which is right for a product that is content-addressed and cannot
    # change, and wrong for the catalogue layer, which is recomputed every time a
    # binner finishes. Without this the tree would keep the first catalogue while
    # reporting success. Named paths only: a blanket refresh would delete
    # products this pass cannot rebuild.
    for path in replace:
        lines.append(f'rm -rf "$DEST"/{shlex.quote(path)}')
    if replace:
        lines.append("")
    dirs = sorted({str(Path(d).parent) for _, d, m, _ in items if m in ("file", "upload")})
    for d in dirs:
        lines.append(f'mkdir -p "$DEST"/{shlex.quote(d)}')
    lines.append("")

    for src, dst, mode, _ in items:
        if mode == "file":
            lines.append(f'cpn {shlex.quote(src)} "$DEST"/{shlex.quote(dst)}')
        elif mode == "upload":
            # Source is local, so cmd_run has already rsynced it to $ARB under
            # its destination relpath -- which is unique by construction, where
            # basenames alone would collide between the two campaigns. Assert it
            # arrived: a silently missing product is this script's worst failure.
            staged = f'"$ARB"/{shlex.quote(dst)}'
            lines.append(f'test -f {staged} || {{ echo "missing upload: {dst}" >&2; exit 1; }}')
            lines.append(f'cpn {staged} "$DEST"/{shlex.quote(dst)}')

    tars = defaultdict(list)
    for src, dst, mode, sample in items:
        if mode == "tar":
            p = Path(dst)
            tars[(str(p.parent.parent), sample)].append((src, p.name))
    for (subdir, sample), members in sorted(tars.items()):
        lines += ["", f'# {subdir} :: {sample} ({len(members)} members)',
                  f'mkdir -p "$STAGE"/{shlex.quote(sample)} "$DEST"/{shlex.quote(subdir)}']
        for src, name in members:
            lines.append(f'cpn {shlex.quote(src)} "$STAGE"/{shlex.quote(sample)}/{shlex.quote(name)}')
        lines.append(
            f'tar -C "$STAGE" -cf "$DEST"/{shlex.quote(subdir)}/{shlex.quote(sample)}.tar '
            f'{shlex.quote(sample)}')
        lines.append(f'rm -rf "$STAGE"/{shlex.quote(sample)}')
    return "\n".join(lines) + "\n"


def cmd_plan(args):
    run_dir, key, attrib, products = resolve(args.task_key)
    items, problems = build_manifest(attrib, products, run_dir, args.catalogue)
    by_dir = defaultdict(int)
    for _, dst, mode, _ in items:
        by_dir[str(Path(dst).parent if mode in ("file", "upload")
                   else Path(dst).parent.parent)] += 1
    print(f"run {key}  ->  {DEST}")
    n_samples = len({r["sample"] for r in attrib.values()
                     if r["sample"] not in ("?", "*")})
    print(f"{len(items)} product(s) over {n_samples} sample(s), "
          f"{len(attrib)} task output(s) attributed\n")
    for d, n in sorted(by_dir.items()):
        print(f"  {n:>6}  {d}")
    n_tar = len({(str(Path(d).parent.parent), s) for _, d, m, s in items if m == "tar"})
    # uploads become ordinary files in the tree, so they cost inodes exactly as
    # `file` items do -- counting only `file` would under-report the allocation
    # cost by one per sample per campaign.
    n_file = sum(1 for _, _, m, _ in items if m in ("file", "upload"))
    print(f"\ninodes into the project allocation: ~{n_file + n_tar} "
          f"({n_file} files + {n_tar} tarballs)")
    if problems:
        print(f"\n{len(problems)} unattributable product(s):", file=sys.stderr)
        for dtype, p, why in problems[:15]:
            print(f"  {dtype}  {p}  [{why}]", file=sys.stderr)
    return items


def cmd_run(args):
    run_dir, key, attrib, products = resolve(args.task_key)
    items, problems = build_manifest(attrib, products, run_dir, args.catalogue)
    if problems and not args.force:
        raise SystemExit(
            f"{len(problems)} product(s) could not be attributed; refusing to publish a\n"
            f"  partially-labelled tree. Re-run `plan` to see them, or pass --force to\n"
            f"  publish everything that did resolve and leave the rest behind.")
    replace = CATALOGUE_DEST if (args.catalogue and args.replace) else ()
    script = render_script(items, replace)
    (ROOT / ".cache").mkdir(exist_ok=True)
    script_path = ROOT / ".cache" / f"publish_{key}.sh"
    script_path.write_text(script)
    print(f"rendered {len(script.splitlines())} lines -> {script_path}")
    if args.dry_run:
        print(script[:4000])
        return
    _upload_arbutus(items)
    r = subprocess.run(["ssh", C.HPC_HOST, "bash -s"], input=script,
                       text=True, capture_output=True, timeout=86400)
    print(r.stdout[-4000:])
    if r.returncode != 0:
        raise SystemExit(f"publish failed (rc={r.returncode}):\n{r.stderr[-4000:]}")
    print("published; now `readme`")


def _upload_arbutus(items):
    """Push the off-cluster campaign tables to fir, keyed by destination relpath.

    Staging under the destination relpath rather than the basename is what keeps
    the two campaigns from colliding -- both emit plain `.tsv` -- and it makes
    the remote script's `test -f` a real check rather than a coincidence.
    """
    ups = [(src, dst) for src, dst, mode, _ in items if mode == "upload"]
    if not ups:
        return
    print(f"uploading {len(ups)} Arbutus product(s) to {C.HPC_HOST}:{ARBUTUS_STAGE}")
    dirs = sorted({str(Path(d).parent) for _, d in ups})
    subprocess.run(["ssh", C.HPC_HOST,
                    "mkdir -p " + " ".join(shlex.quote(f"{ARBUTUS_STAGE}/{d}") for d in dirs)],
                   check=True, timeout=300)
    for src, dst in ups:
        subprocess.run(["rsync", "-a", src,
                        f"{C.HPC_HOST}:{ARBUTUS_STAGE}/{dst}"],
                       check=True, timeout=3600)


README_HEAD = """# steven_c_gmcf3495/metagenomics

Short-read metagenomics outputs for Steven Chen's GMCF_3495 dataset: 34 paired
libraries, laid out to match `spanish_lakes/metagenomics/` so the two trees read
the same way.

**`NTC` is the negative control.** It is carried through every step deliberately
and is not a biological sample; anything it shares with a real sample is
contamination signal, not a finding.

## `reads/interleaved` exists on the archive only

The interleaved raw reads — the input to filtering, 34 files and 372 GiB — are
**archived to Globus and deliberately not kept on fir**:

    chinook:/Manuscripts/Science/2024-09-25_Lung_microbiome/
            WGA_metagenomics_via_metasmith/reads/interleaved/

They are reconstructible from two things that are themselves archived on that
same collection: the per-lane raw fastqs under `/Received_raw_data/GMCF_3495/`,
and a deterministic interleave step whose every product had its read count
checked against an independently derived ground truth. A third copy on the
allocation buys nothing, and the layout table below is generated from whichever
tree you are reading, so `reads/interleaved` appears in it on the archive and not
on fir. That asymmetry is the decision, not a failed publish.

## Taxonomy: reads, contigs and bins

All three levels are here. Contig taxonomy (metabuli) and bin taxonomy (GTDB-Tk)
ran as on-demand Arbutus cloud services rather than on fir, because their
reference databases are too large to keep on the allocation — metabuli's {rel}
build alone is ~744 GB. Everything GTDB-based is {rel} and is directly comparable
across all three levels.

| product | reference |
|---|---|
| centrifuger (reads) | GTDB {rel} + RefSeq hvfpc — a *superset* of bare {rel} |
| kraken2 / bracken (reads) | **NCBI**, full standard — not GTDB |
| metabuli (contigs) | GTDB {rel} |
| GTDB-Tk (bins) | GTDB {rel} |

kraken2/bracken is the deliberate non-GTDB second opinion and was not
"reconciled" onto {rel}.

## Read this before using the bin set: 8 of the 67 quality bins are human

**This dataset is host-dominated.** Metabuli classified all 3,340,538 contigs, and
**92.4% are Eukaryota** against 3.3% Bacteria and 4.1% unclassified. That is the
single most important fact about these libraries, and it is not visible from bin
counts or CheckM2 scores.

It has a concrete consequence for the bin set. GTDB-Tk classified 67/67 quality
bins with no failures: 59 got species-level assignments, and **8 came back
Unclassified because they contain zero of the 120 bacterial and zero of the 53
archaeal marker genes** — despite being 3.2–7.2 Mbp with contigs up to 70 kb.
Looking those bins' own contigs up in the metabuli output settles what they are:
**5,584 of their 5,587 classified contigs are *Homo sapiens*.** Bins that GTDB-Tk
placed via the ANI screen are 100% Bacteria by the same check.

    S13.semibin2.0010   S25.semibin2.0001   S25.semibin2.0002   S26.semibin2.0001
    S6.semibin2.0004    S6.semibin2.0011    S9.semibin2.0001    S9.semibin2.0002

All 8 are semibin2, and all 8 sit at the acceptance boundary — 51.5–56.8%
complete, 7.4–9.6% contaminated. CheckM2 admitted them because its model scores
prokaryotic completeness and is not trained to reject non-prokaryotic input; a
genome-sized bin with no ribosomal proteins is a contradiction, not a novel
lineage. They are left in the tree because "Unclassified" is a legitimate GTDB-Tk
result and removing data silently is worse — but **the honest headline is 59
prokaryotic MAGs plus 8 host-DNA artifacts, not 67 MAGs.** Filter on the
`classification` column of `taxonomy_binning/gtdbtk/gtdbtk.bac120.summary.tsv`
before using the bin set for anything biological.

### {rel} renamed groups you may be expecting

GTDB {rel} reorganized several well-known clades, so a name that looks like a
failure may just be current. The clearest case: *Escherichia coli* K-12 MG1655
(`GCA_000005845.2`) is `g__G047199095; s__G047199095 sp047199095` in {rel}, where
R226 had it as `g__Escherichia; s__Escherichia coli`. Placeholder names of the
form `g__G<digits>` are real {rel} assignments, not unclassified bins. Compare
against an {rel} table, not an older one.

## Per-bin products are tarballs

One tar per sample, not one file per bin. The allocation has a 500K inode limit
and the equivalent Spanish Lakes fan-out is 31,149 files on its own. Read a
single bin with `tar -xOf <sample>.tar <member>`; list with `tar -tf`.

## Layout

```
"""

README_TAIL = """```

## Provenance

| | |
|---|---|
| pipeline | metasmith {msm} |
| run keys | `QkqCNJOo` (32 libraries) and `hEYVT7HY` (S13, S22) |
| cluster | fir (Alliance), account rrg-shallam-ab |
| tool versions | one container per tool; see the run's `resources/env/*.env` |

S13 and S22 were dropped from the main run by a race in the pipeline's grouping
and were carried through the same DAG afterwards as a scoped run, so this tree is
assembled from two run directories. Some binning and the ProteinBERT merges for
S12, S13, S19 and S25 were completed outside the DAG for the same reason; the bin
catalogue and census below record what came from where.

Generated by `main/r1/publish_r1.py`. Every filename here was derived by walking
each product back to the read files it was actually computed from, using the
input list each nextflow task recorded in its own `.command.sh` — *not* the
pipeline's lineage graph, which does not agree with what the tasks ran, and not
by parsing paths.

## How to read the binning results

Start at **`binning/bin_catalogue.tsv`** — one row per bin produced anywhere in
this run, with its sample, its binner, its CheckM2 completeness and
contamination, and whether it clears the quality threshold (completeness >=50%,
contamination <=10%). Every quality bin appears in `binning/quality_bins/` as
`<sample>.<binner>.<n>.fa` inside that sample's tarball, and in that sample's
`binning/cluster_tables/<sample>.tsv` under the same id.

**`binning/binner_census.tsv` records what did not happen, too.** It has a row
for every sample x binner pair, and where a binner produced no bins it carries
the reason the tool itself gave rather than an empty cell. A silent absence and
a genuine zero look identical in a bin count; they do not look identical here,
which is the point. Several of these libraries are low-biomass and cannot yield
MAGs — for those, zero is the correct answer, not a failure.

**The catalogue is broader than the pipeline's own bin set.** The pipeline's
aggregator discards every bin from an assembly unless all three binners produced
output for it, and it emits bins under content-hashed names with all three
binners upstream, so neither its counts nor its binner labels survive. The
catalogue judges each bin on its own and attributes it before scoring. The skani
clustering here was likewise recomputed over the complete bin set, so it
supersedes the pipeline's own per-assembly cluster tables, which are not
published.
"""


def agent_version(run_dir):
    """The metasmith version that ACTUALLY ran, read off the deployed agent.

    Not `import metasmith`: that reports whichever tree the caller's PYTHONPATH
    happens to resolve, which is the editable 0.18.7 install unless the driver
    env is set. Running `readme` from a plain shell therefore stamped the
    published README "metasmith 0.18.7" for a run executed by 0.19.1 -- a wrong
    provenance number, silently, in the one file whose job is provenance.

    The agent deployed on the cluster is the code that ran, and the run lives at
    <agent_home>/runs/<key>, so its version.txt sits two levels up. No fallback:
    a version we cannot read is not a version worth guessing.
    """
    home = Path(run_dir).parent.parent
    try:
        v = C.ssh_out(f"cat {home}/dev/metasmith/version.txt").strip()
    except RuntimeError:
        v = ""
    if not re.fullmatch(r"\d+\.\d+\.\d+\S*", v):
        sys.exit(f"could not read the deployed agent version at "
                 f"{home}/dev/metasmith/version.txt (got {v!r}); refusing to "
                 f"publish a README with a guessed pipeline version")
    return v


def cmd_readme(args):
    run_dir, key, attrib, products = resolve(args.task_key)

    # Count the published tree, not this pass's manifest. The tree is assembled
    # by two passes -- the main run and the scoped S13/S22 run -- so a manifest
    # describes at most half of it, and the README claimed 32 files in eight
    # directories that hold 34. What is on disk is also the only thing a reader
    # can check the README against.
    listing = C.ssh_out(
        f"cd {shlex.quote(DEST)} && for d in $(find . -mindepth 1 -type d | sort); do "
        f"n=$(find \"$d\" -maxdepth 1 -type f | wc -l); "
        f"[ \"$n\" -gt 0 ] && echo \"${{d#./}} $n\"; done")
    counts = {}
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) == 2:
            counts[parts[0]] = int(parts[1])

    # A tar directory's member total has to be read out of the tarballs; it is
    # the number a reader most wants and the one they cannot see with `ls`.
    tar_dirs = sorted(d for d in counts if d.startswith("binning/")
                      and d.rsplit("/", 1)[-1] != "cluster_tables"
                      and ("fna_" in d or d.endswith("quality_bins")))
    members = {}
    if tar_dirs:
        # One `tar -tf` per archive. Concatenating them and listing the stream
        # once counts only the first: tar stops at the end-of-archive marker.
        cmd = "; ".join(
            f'echo "{d} $(for t in {shlex.quote(DEST)}/{d}/*.tar; do '
            f'tar -tf "$t" 2>/dev/null; done | grep -c "[^/]$")"' for d in tar_dirs)
        for line in C.ssh_out(cmd).splitlines():
            parts = line.split()
            if len(parts) == 2:
                members[parts[0]] = parts[1]

    tree = []
    for d in sorted(counts):
        if d in members:
            tree.append(f"{d:<38}  {counts[d]:>4} tar   {members[d]} members total")
        else:
            tree.append(f"{d:<38}  {counts[d]:>4} files")

    msm_version = agent_version(run_dir)

    body = (README_HEAD.format(rel=GTDB_RELEASE)
            + "\n".join(tree) + "\n"
            + README_TAIL.format(msm=msm_version, key=key))
    if args.dry_run:
        print(body)
        return
    r = subprocess.run(["ssh", C.HPC_HOST, f"cat > {shlex.quote(DEST)}/README.md"],
                       input=body, text=True, capture_output=True, timeout=600)
    if r.returncode != 0:
        raise SystemExit(f"could not write README: {r.stderr}")
    print(f"README.md written to {DEST}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--task-key", default=None)
    # The catalogue layer describes the whole run, not one run dir. The scoped
    # S13/S22 pass must turn it off or both passes claim the same destinations.
    ap.add_argument("--no-catalogue", dest="catalogue", action="store_false",
                    help="skip the bin catalogue/census/clustering layer "
                         "(use on the second, scoped publish pass)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan").set_defaults(fn=cmd_plan)
    p = sub.add_parser("run"); p.set_defaults(fn=cmd_run)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true",
                   help="publish resolved products even if some are unattributable")
    p.add_argument("--replace", action="store_true",
                   help="delete the catalogue layer before copying, so a re-publish "
                        "after more bins land actually updates it (cpn would not)")
    r = sub.add_parser("readme"); r.set_defaults(fn=cmd_readme)
    r.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""The SCADC fosmid arm: 170 inserts over the EPI300 host, measured to conductance.

    scadc_ecspr_basis.py                 the bake decode and the terminals, first
    scadc_ecspr_arm.py --stage           working GPR copies, under names that cannot collide
                       --conditions      the two tables, then the ORF-counted strata
                       --measure         the observed run, 171 solves
                       --host-probe      the host's own draw vector on this bake
                       --draw --shards   26,000 null constructions, split for the array
                       --fir-status      one batched connect, never a loop
                       --retrieve        every shard, or refuse
                       --score           delta, z, p, q and the gate, per null style
                       --publish         into data/fabfos/runs/scadc_fosmids/ecspr/

The basis step runs first and this reads its `terminals.json` rather than restating four
ids; it refuses outright if that file was resolved against a different bake.

WHAT IS MEASURED. One condition per insert: the host's GEM rows and that insert's rows
both selected, their conductances summing, glucose injected and the four biomass hubs
merged into one ground. The baseline is the host alone, and every arm reads against it.
That is the same experiment the retired `scadc_ecspr.py` open-coded -- and the reason it
is now four CLI calls instead is that the null has to traverse the identical code path to
be a null at all, which a hand-rolled sampler beside a hand-rolled observation cannot
promise.

THE UNIT IS THE INSERT, WHICH IT WAS NOT BEFORE. The generation this replaces measured
183 contigs against a table that carried one anonymous copy; the rebuilt insert set is
170, and `genome_unit` now names each one. Belief is conserved per nominator, so which
rows share a unit changes every `E_r` in the table, not just the labels.

WHY `in_atom_universe` IS RECOMPUTED HERE. A perturbation that reaches no reaction of the
atom-pair table cannot move the measurement, and `recipes` reads that column to mark such
an arm a control rather than dropping it -- which is what makes the noise floor a measured
quantity. The fosmid table carries no such column, and the host's was computed against an
older bake (46 of its 1,383 `True` reactions are outside r10's carbon universe and 674 of
its `False` ones are inside it), so the column is derived here against the bake actually
being measured on. The host table is staged UNCHANGED: it is the background, and
`_reaches_universe` is only ever asked about the perturbation.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))

import bake_identity  # noqa: E402
import bake_pairs  # noqa: E402

WORK = REPO / "data/scratch/scadc_ecspr"
BASIS = WORK / "basis"
TABLES = WORK / "tables"

FOSMIDS = REPO / "data/fabfos/runs/scadc_fosmids/gpr/gpr_4lane.parquet"
HOST_GEM = REPO / "data/fabfos/runs/e_coli_epi300/gpr/gpr_gem.parquet"
METAG = REPO / "data/fabfos/runs/scadc_metagenome/gpr/gpr_4lane.parquet"

# Staged under names that cannot collide. A conditions table names its table by BASENAME,
# and the fosmid pool, the metagenome pool and three host tables are all called
# `gpr_4lane.parquet` or `gpr_gem.parquet` where they live -- so two of them staged
# together would leave one unreachable, and `load_corpus` refuses rather than picking.
T_FOSMIDS = "scadc_fosmids_4lane.parquet"
T_HOST = "epi300_gem.parquet"
T_METAG = "scadc_metag_4lane.parquet"

HOST_UNIT = "iECDH10B_1368"
ELEMENT = "C"
BASELINE_ID = "epi300_host"


def _ecspr(*argv: str) -> None:
    cmd = [sys.executable, "-m", "ecspr", *map(str, argv)]
    env = dict(os.environ, PYTHONPATH=str(REPO / "src"))
    print("$ ecspr " + " ".join(map(str, argv)), flush=True)
    subprocess.run(cmd, check=True, cwd=REPO, env=env)


def _write(df: pd.DataFrame, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".writing")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, dest)
    return dest


def terminals() -> dict:
    path = BASIS / "terminals.json"
    if not path.exists():
        raise SystemExit(f"no {path.relative_to(REPO)} -- run scadc_ecspr_basis.py first")
    doc = json.loads(path.read_text())
    if doc["bake_identity"] != bake_identity.identity():
        raise SystemExit(
            f"{path.relative_to(REPO)} was resolved against {doc['bake_identity']}\n"
            f"  the deployed bake is {bake_identity.identity()}\n"
            f"  re-run scadc_ecspr_basis.py")
    return doc


def stage() -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    pairs = pd.read_parquet(bake_pairs.atom_pairs(), columns=["mnxr", "element"])
    universe = set(pairs[pairs.element == ELEMENT].mnxr)

    fos = pd.read_parquet(FOSMIDS)
    fos["in_atom_universe"] = fos.mnxr.isin(universe)
    fos.loc[fos.mnxr.isna(), "in_atom_universe"] = False
    _write(fos, TABLES / T_FOSMIDS)

    reach = fos.groupby("genome_unit")["in_atom_universe"].any()
    print(f"[stage] {T_FOSMIDS}: {len(fos):,} rows, {fos.genome_unit.nunique()} inserts, "
          f"{fos.mnxr.nunique():,} reactions")
    print(f"[stage]   {int(reach.sum())} inserts reach the {ELEMENT} atom universe, "
          f"{int((~reach).sum())} do not and become controls")
    print(f"[stage]   {int(fos.in_atom_universe.sum()):,} of {len(fos):,} rows are in it")

    shutil.copyfile(HOST_GEM, TABLES / T_HOST)
    host = pd.read_parquet(TABLES / T_HOST)
    print(f"[stage] {T_HOST}: {len(host):,} rows, genome_unit "
          f"{sorted(host.genome_unit.unique())}, staged unchanged")

    shutil.copyfile(METAG, TABLES / T_METAG)
    print(f"[stage] {T_METAG}: the null pool, staged unchanged "
          f"({(TABLES / T_METAG).stat().st_size / 1e6:.0f} MB)")


def conditions() -> None:
    t = terminals()
    _ecspr("conditions",
           "--gpr", TABLES / T_HOST, TABLES / T_FOSMIDS,
           "--background", f"{T_HOST}:genome_unit:{HOST_UNIT}",
           "--table", T_FOSMIDS, "--column", "genome_unit", "--family", "gof_each",
           "--probe", "two-point", "--element", ELEMENT,
           "--source", t["source"], "--sinks", *t["sinks"].values(),
           "--baseline-id", BASELINE_ID,
           "--out", WORK / "conditions.parquet",
           "--out-tests", WORK / "tests.parquet")


# THE STRATUM IS COUNTED IN ORFs HERE, AND THAT IS A DECISION THIS ARM HAS TO MAKE.
# `recipes` writes `n_units` as the number of distinct `genome_unit`s a perturbation
# selects, which is 1 for every insert, and `nulls.draw` uses that number as the count it
# draws from the pool. Those two agree only when the null is drawn on `genome_unit` --
# and the SCADC metagenome carries exactly one, because its real unit is a MAG and the
# table has no binning. So the pool can only be drawn on `orf`, and a stratum of 1 would
# match inserts carrying 9 to 81 ORFs against single-ORF draws: every insert would clear
# a null it outweighs by construction, and nothing would raise.
#
# Counts are grouped so no bucket spans more than 1.15x, which bounds what an insert
# loses by being measured against its bucket's size at 7%. The bucket count and the
# representatives come out at 13 and 9..79 -- within one of what the retired 183-insert
# generation derived by the same rule, on a different insert set.
BUCKET_SPAN = 1.15


def _buckets(counts) -> dict:
    vals = sorted(counts)
    groups, cur = [], [vals[0]]
    for v in vals[1:]:
        if v / cur[0] <= BUCKET_SPAN:
            cur.append(v)
        else:
            groups.append(cur)
            cur = [v]
    groups.append(cur)
    return {v: int(pd.Series(g).median()) for g in groups for v in g}


def strata() -> None:
    fos = pd.read_parquet(TABLES / T_FOSMIDS, columns=["genome_unit", "orf"])
    n_orf = fos.groupby("genome_unit")["orf"].nunique()
    rep = _buckets(n_orf.tolist())

    tests = pd.read_parquet(WORK / "tests.parquet")
    sizes = tests.test_id.map(n_orf)
    tests["n_units"] = sizes.map(rep)
    _write(tests, WORK / "tests.parquet")

    sidecar = pd.DataFrame({"insert": n_orf.index, "n_orf": n_orf.values})
    sidecar["stratum"] = sidecar.n_orf.map(rep)
    sidecar["lost_pct"] = (100 * (sidecar.n_orf - sidecar.stratum).abs()
                           / sidecar.n_orf).round(2)
    _write(sidecar, WORK / "insert_sizes.parquet")

    used = sorted(set(rep.values()))
    print(f"[strata] {len(n_orf)} inserts, {n_orf.min()}-{n_orf.max()} ORFs -> "
          f"{len(used)} strata {used}")
    print(f"[strata] worst size mismatch {sidecar.lost_pct.max():.1f}%, "
          f"median {sidecar.lost_pct.median():.1f}%")
    print(f"[strata] baseline row carries n_units "
          f"{tests.loc[tests.test_id == BASELINE_ID, 'n_units'].tolist()} "
          f"(no draw is matched to it; it is what every arm reads against)")


# No `--source` / `--sinks` here, deliberately: a tests table carries its own terminals
# and the probe refuses both, which is what stops a run measuring one pair of terminals
# while its tests table records another.
def measure() -> None:
    terminals()
    _ecspr("two-point",
           "--atom-pairs", bake_pairs.atom_pairs(),
           "--direction", bake_pairs.direction_ratios(),
           "--element", ELEMENT,
           "--gpr", TABLES / T_HOST, TABLES / T_FOSMIDS,
           "--conditions", WORK / "conditions.parquet",
           "--tests", WORK / "tests.parquet",
           "--out", WORK / "results.parquet",
           "--log", WORK / "measure.log")


# =====================================================================
# The null: two sampler styles, both measured by the same probe verb
# =====================================================================
#
# STYLE A (uniform) is `ecspr draw`: N ORF ids drawn uniformly from the whole metagenome
# inventory, independent of contig. STYLE D (contiguous) is emitted here in the same two
# schemas: an N-ORF run on ONE contig, at a uniform start offset among the contigs long
# enough to hold it.
#
# D EXISTS BECAUSE A IS THE WRONG SHAPE FOR A FOSMID. A real insert is one ~35 kb window
# of one genome, so its ORFs are neighbours and share an operon structure, a GC content
# and an organism; style A's thirty ORFs come from thirty unrelated genomic contexts and
# carry thirty organisms' worth of metabolic breadth. Which way that biases the
# comparison is not obvious a priori, which is exactly why both are drawn rather than one
# being argued for. `ecspr draw` has no contiguous family, so D is built here -- but only
# the SAMPLING is; the conditions it writes go through the same probe, the same weight
# resolution and the same scorer as A and as the observation.
def _tests_for_draw() -> Path:
    # The baseline carries no n_units, and `nulls.draw` refuses a test without one rather
    # than drawing an unmatched null. That refusal is right and the baseline is simply
    # not a thing to draw against: it is what every arm READS against, and it is measured
    # in the observed run.
    #
    # `read_against` GOES WITH IT, and it has to: a tests table refuses to load while a
    # row points at a baseline the table does not contain, which is the same integrity
    # check that would catch a real dangling reference. The draw reads only `n_units` and
    # the measurement settings, and writes its own tests with no baseline at all -- the
    # observed table keeps the reference, and that is the one the scorer reads.
    t = pd.read_parquet(WORK / "tests.parquet")
    t = t[t.test_id != BASELINE_ID].copy()
    t["read_against"] = ""
    return _write(t, WORK / "tests_for_draw.parquet")


def draw_uniform(n: int, seed: int) -> None:
    _ecspr("draw",
           "--gpr", TABLES / T_METAG,
           "--like-tests", _tests_for_draw(),
           "--like-conditions", WORK / "conditions.parquet",
           "--background", BASELINE_ID,
           "--pool-table", T_METAG,
           "--draw-column", "orf", "-n", n, "--seed", seed,
           "--out", WORK / "nullA_conditions.parquet",
           "--out-tests", WORK / "nullA_tests.parquet",
           "--log", WORK / "draw.log")


def draw_contiguous(n: int, seed: int) -> None:
    import numpy as np

    from ecspr.model import conditions as cond_mod
    from ecspr.model import tests as test_mod

    conds = cond_mod.read(WORK / "conditions.parquet")
    background = {c.condition_id: c for c in conds}[BASELINE_ID].steps
    like = test_mod.read(WORK / "tests_for_draw.parquet", "ecspr")
    proto = like[0]
    strata = sorted({int(t.meta["n_units"]) for t in like})

    pool = pd.read_parquet(TABLES / T_METAG, columns=["orf"]).orf.drop_duplicates()
    # ORFs are named `<contig>_<index>`, and the index is the gene's order along the
    # contig -- so a contiguous window is a slice of the index-sorted run, and sorting
    # the id as a STRING would put gene 10 next to gene 1.
    idx = pool.str.rsplit("_", n=1)
    frame = pd.DataFrame({"orf": pool.values, "contig": idx.str[0].values,
                          "gene": pd.to_numeric(idx.str[1], errors="coerce").values})
    frame = frame.dropna(subset=["gene"]).sort_values(["contig", "gene"])
    runs = {c: g.orf.tolist() for c, g in frame.groupby("contig", sort=False)}
    lengths = pd.Series({c: len(v) for c, v in runs.items()})

    out_conds, out_tests = [], []
    for s in strata:
        eligible = lengths[lengths >= s].index.to_numpy()
        if eligible.size == 0:
            raise SystemExit(f"no contig holds {s} ORFs; the contiguous null cannot "
                             f"cover the {s}-ORF stratum")
        rng = np.random.default_rng([seed, s, 0xD])
        for j in range(n):
            contig = str(rng.choice(eligible))
            run = runs[contig]
            start = int(rng.integers(0, len(run) - s + 1))
            pick = tuple(sorted(run[start:start + s]))
            cid = f"nullD:s{s}:{j:06d}"
            out_conds.append(cond_mod.Condition(
                condition_id=cid,
                steps=tuple(background) + (cond_mod.Step(
                    table=T_METAG, type="orf", set=pick, operation="union"),)))
            out_tests.append(test_mod.Test(
                test_id=f"nullD:t0:s{s}:{j:06d}", network=cid, arm="ecspr",
                settings=dict(proto.settings),
                meta=dict(arm="null", stratum=s, n_units=s, draw_id=j,
                          is_control=False, cohort=None)))
    cond_mod.write(out_conds, WORK / "nullD_conditions.parquet")
    test_mod.write(out_tests, WORK / "nullD_tests.parquet", "ecspr")
    print(f"[drawD] {len(eligible):,} eligible contigs at the largest stratum | "
          f"{len(strata)} strata {strata} x {n} draws -> {len(out_conds):,} conditions")
    print(f"[drawD] {WORK / 'nullD_conditions.parquet'}")


# ONE SHARD PER (style, stratum), which is the natural unit and not just a convenient
# one: every condition in a shard draws the same number of ORFs, so the shards are the
# same size in work, and a task loads the 19.7M-row pool table once and then never
# touches another shard's. Splitting here rather than filtering on the cluster keeps the
# array task a single command with no selection logic to disagree with this file's.
def shards() -> None:
    out = WORK / "shards"
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.parquet"):
        old.unlink()
    n = 0
    for style in ("A", "D"):
        t = pd.read_parquet(WORK / f"null{style}_tests.parquet")
        c = pd.read_parquet(WORK / f"null{style}_conditions.parquet")
        for s, ts in t.groupby("stratum"):
            s = int(s)
            _write(ts, out / f"null{style}_s{s}_tests.parquet")
            _write(c[c.condition_id.isin(set(ts.network))],
                   out / f"null{style}_s{s}_conditions.parquet")
            n += 1
    names = sorted(p.name.replace("_tests.parquet", "")
                   for p in out.glob("*_tests.parquet"))
    (out / "SHARDS.txt").write_text("\n".join(names) + "\n")
    print(f"[shards] {n} shards -> {out.relative_to(REPO)} "
          f"({len(names)} listed in SHARDS.txt)")


PUBLISH_AT = REPO / "data/fabfos/runs/scadc_fosmids/ecspr"
HOST_PUBLISH_AT = REPO / "data/fabfos/runs/e_coli_epi300/ecspr"

#: What lands in the chunk, and under which name. The two null styles keep their own
#: results, tests and scores rather than being concatenated: they are two different
#: questions about the same observation, and a reader has to be able to see that an
#: insert clears one and not the other.
PUBLISH = {
    "results.parquet": "results.parquet",
    "conditions.parquet": "conditions.parquet",
    "tests.parquet": "tests.parquet",
    "insert_sizes.parquet": "insert_sizes.parquet",
    "nullA_results.parquet": "nullA_results.parquet",
    "nullA_tests.parquet": "nullA_tests.parquet",
    "nullD_results.parquet": "nullD_results.parquet",
    "nullD_tests.parquet": "nullD_tests.parquet",
    "scored_nullA.parquet": "scored_nullA.parquet",
    "scored_nullD.parquet": "scored_nullD.parquet",
    "gate_nullA.tsv": "gate_nullA.tsv",
    "gate_nullD.tsv": "gate_nullD.tsv",
    "PROVENANCE.md": "PROVENANCE.md",
}

FIR_WORK = "/scratch/phyberos/scadc_ecspr_r10"


def fir_status() -> None:
    # ONE CONNECT, NEVER A LOOP. Every ssh to this host is a Duo push, and ten failed
    # pushes in a row locked the account in this project on 2026-07-02. Everything worth
    # knowing is one batched command.
    subprocess.run(["ssh", "-o", "BatchMode=yes", "fir", f"""
        squeue -u phyberos -n scadc_null_r10 -h -o '%T' | sort | uniq -c
        echo '--- solved per shard (1000 = complete) ---'
        for d in {FIR_WORK}/results/*.shards; do
          echo "$(basename $d .shards) $(ls "$d" 2>/dev/null | wc -l)"; done
        echo '--- finished shards ---'
        ls {FIR_WORK}/results/*_results.parquet 2>/dev/null | wc -l
    """], check=True)


def retrieve() -> None:
    dest = WORK / "fir_results"
    dest.mkdir(parents=True, exist_ok=True)
    subprocess.run(["rsync", "-a", "--info=stats1",
                    f"fir:{FIR_WORK}/results/", str(dest) + "/",
                    "--include=*_results.parquet", "--include=*/", "--exclude=*"],
                   check=True)
    expect = sorted((WORK / "shards" / "SHARDS.txt").read_text().split())
    for style in ("A", "D"):
        want = [n for n in expect if n.startswith(f"null{style}_")]
        got = [n for n in want if (dest / f"{n}_results.parquet").exists()]
        # A MISSING STRATUM IS NOT A SMALLER NULL, it is a wrong one: every observed
        # insert nearest to it would be scored against a different size. Refuse rather
        # than concatenate what arrived.
        if len(got) != len(want):
            raise SystemExit(
                f"style {style}: {len(got)} of {len(want)} shards retrieved, missing "
                f"{sorted(set(want) - set(got))}. Scoring a partial null silently "
                f"matches those inserts to the wrong stratum")
        frames = [pd.read_parquet(dest / f"{n}_results.parquet") for n in want]
        out = pd.concat(frames, ignore_index=True)
        _write(out, WORK / f"null{style}_results.parquet")
        n_tests = out.test_id.nunique()
        conv = out[out.readout == "_converged"].value
        abst = out[out.readout == "_abstained"].value
        print(f"[retrieve] style {style}: {len(want)} shards -> {n_tests:,} tests, "
              f"{len(out):,} rows | converged {int(conv.sum()):,}/{len(conv):,} | "
              f"abstained {int(abst.sum()):,}")


def score() -> None:
    for style in ("A", "D"):
        _ecspr("score",
               "--results", WORK / "results.parquet",
               "--null", WORK / f"null{style}_results.parquet",
               "--tests", WORK / "tests.parquet",
               "--null-tests", WORK / f"null{style}_tests.parquet",
               "--out", WORK / f"scored_null{style}.parquet",
               "--gate-out", WORK / f"gate_null{style}.tsv",
               "--log", WORK / "score.log")


# The numbers a report quotes, printed from the scored tables rather than remembered.
#
# `z_stratum` / `p_stratum` ARE THE PRIMARY READOUT, not `z` / `p_emp`. The pooled columns
# rank an insert against every draw of every size, and this arm's inserts span 9 to 81
# ORFs -- so a pooled rank is substantially a size rank. The stratum columns are the
# comparison the strata were built for.
def report() -> None:
    obs = pd.read_parquet(WORK / "results.parquet")
    w = obs.pivot_table(index="test_id", columns="readout", values="value",
                        aggfunc="first")
    base = w.loc[BASELINE_ID, "total"]
    print(f"host baseline C(glucose -> 4 hubs) = {base:.9g}")
    print(f"171 tests, converged {int(w._converged.sum())}, "
          f"abstained {int(w._abstained.sum())}, "
          f"CHOLMOD {int(w._cholmod_available.sum())}")

    for style, what in (("A", "uniform ORF draw"), ("D", "contiguous ORF run")):
        s = pd.read_parquet(WORK / f"scored_null{style}.parquet")
        tot = s[(s.readout == "total") & (s.test_id != BASELINE_ID)].copy()
        n_null = int(tot.null_n_stratum.median()) if len(tot) else 0
        floor = 1.0 / (n_null + 1) if n_null else float("nan")
        sig = tot[tot.q_bh < 0.05] if "q_bh" in tot else tot.iloc[:0]
        print(f"\nnull {style} ({what}): {n_null:,} draws per stratum, "
              f"empirical p floor {floor:.2e}")
        print(f"  q_bh < 0.05: {len(sig)} of {len(tot)} inserts")
        if len(tot):
            print(f"  z_stratum: min {tot.z_stratum.min():.3f} "
                  f"median {tot.z_stratum.median():.3f} max {tot.z_stratum.max():.3f}")
            top = tot.nlargest(5, "z_stratum")[
                ["test_id", "delta", "z_stratum", "p_stratum", "q_bh", "stratum"]]
            print(top.to_string(index=False))

    a = pd.read_parquet(WORK / "scored_nullA.parquet")
    d = pd.read_parquet(WORK / "scored_nullD.parquet")
    key = ["test_id", "readout"]
    j = (a[a.readout == "total"].set_index(key).z_stratum.rename("A")
         .to_frame().join(d[d.readout == "total"].set_index(key).z_stratum.rename("D")))
    j = j.dropna()
    # THE TWO NULLS ARE NOT INTERCHANGEABLE and this is where that shows. If the
    # contiguous null is systematically harder, an insert that clears A and not D is not
    # a finding, it is a statement about what A left out.
    print(f"\nA vs D on {len(j)} inserts: spearman "
          f"{j.corr(method='spearman').iloc[0, 1]:.4f}; "
          f"z higher under A for {int((j.A > j.D).sum())}, under D for "
          f"{int((j.D > j.A).sum())}")


# A PUBLISH IS A COPY ONTO A FRESH NAME, never a write through one. Under `data/` a
# checked-out file is one inode with many names -- DVC's cache type here is
# `hardlink,symlink` -- so opening a published path for writing truncates the cache
# itself and every other worktree holding that content silently becomes the new file,
# with `dvc status` clean throughout. Unlinking first drops this directory's name from
# the inode and leaves the cached content alone.
def publish(dry_run: bool = False) -> None:
    PUBLISH_AT.mkdir(parents=True, exist_ok=True)
    for src_name, dest_name in PUBLISH.items():
        src = WORK / src_name
        if not src.exists():
            raise SystemExit(f"{src.relative_to(REPO)} is missing -- the arm is not "
                             f"finished, and half a chunk is worse than none")
        dest = PUBLISH_AT / dest_name
        print(f"  {src_name} -> {dest.relative_to(REPO)}")
        if not dry_run:
            if dest.exists() or dest.is_symlink():
                dest.unlink()
            shutil.copyfile(src, dest)

    host_src = WORK / "host_glucose_probe.parquet"
    if host_src.exists():
        # The retired `glucose_probe.{json,tsv}` beside it STAYS, under its own name. It
        # is not the same measurement re-taken: its `transit` column is node throughput,
        # which this engine emits at no setting, so overwriting it would silently drop a
        # quantity rather than update one.
        dest = HOST_PUBLISH_AT / "glucose_probe_r10.parquet"
        print(f"  host_glucose_probe.parquet -> {dest.relative_to(REPO)}")
        if not dry_run:
            if dest.exists() or dest.is_symlink():
                dest.unlink()
            shutil.copyfile(host_src, dest)
    print(f"\n{len(PUBLISH)} table(s) -> {PUBLISH_AT.relative_to(REPO)}/"
          + ("   (dry run)" if dry_run else ""))


# The host's own probe, on the bake the fosmid arm is measured on. `data/fabfos/runs/
# e_coli_epi300/ecspr/` carries one from before the first bake, and TWO OF ITS THREE
# QUANTITIES CANNOT BE REPRODUCED AGAINST THIS ENGINE rather than merely being stale:
# its `transit` column is node throughput, which `ecspr.model.probes` emits at no
# setting, and `pairwise_Ieff_sparse.npz` is 1,489 separate solves from a cache store
# this tree no longer builds. What is reproducible is the draw vector, so that is what
# is refreshed -- and it is taken with the fosmid arm's own four hubs as PRECURSORS
# rather than against a bare universal leak, which is what makes it the same experiment
# read one metabolite at a time. Under a bare leak every metabolite drains its ~1/n
# share and the vector says nothing; that is what the retired file's near-constant
# `drain` column is.
def host_probe() -> None:
    t = terminals()
    _ecspr("ground",
           "--atom-pairs", bake_pairs.atom_pairs(),
           "--direction", bake_pairs.direction_ratios(),
           "--element", ELEMENT,
           "--gpr", TABLES / T_HOST,
           "--source", t["source"], "--precursors", *t["sinks"].values(),
           "--readouts", "all",
           "--out", WORK / "host_glucose_probe.parquet",
           "--log", WORK / "host_probe.log")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--stage", action="store_true")
    p.add_argument("--host-probe", action="store_true", dest="host_probe",
                   help="the EPI300 host's own draw vector, on this bake")
    p.add_argument("--conditions", action="store_true",
                   help="build the two tables, then set the ORF-counted strata")
    p.add_argument("--measure", action="store_true")
    p.add_argument("--draw", action="store_true",
                   help="both null styles, as conditions + tests tables")
    p.add_argument("-n", type=int, default=1000, help="draws per stratum per style")
    p.add_argument("--seed", type=int, default=20260825)
    p.add_argument("--shards", action="store_true",
                   help="split both null tables into one shard per (style, stratum)")
    p.add_argument("--fir-status", action="store_true", dest="fir_status")
    p.add_argument("--retrieve", action="store_true")
    p.add_argument("--score", action="store_true")
    p.add_argument("--report", action="store_true")
    p.add_argument("--publish", action="store_true")
    p.add_argument("--dry-run", action="store_true", dest="dry_run")
    a = p.parse_args(argv)
    if not (a.stage or a.conditions or a.measure or a.host_probe or a.draw
            or a.shards or a.fir_status or a.retrieve or a.score
            or a.publish or a.report):
        p.error("pick one of --stage / --conditions / --measure / --host-probe / "
                "--draw / --shards / --fir-status / --retrieve / --score / "
                "--publish / --report")
    if a.stage:
        stage()
    if a.conditions:
        conditions()
        strata()
    if a.measure:
        measure()
    if a.host_probe:
        host_probe()
    if a.draw:
        draw_uniform(a.n, a.seed)
        draw_contiguous(a.n, a.seed)
    if a.shards:
        shards()
    if a.fir_status:
        fir_status()
    if a.retrieve:
        retrieve()
    if a.score:
        score()
    if a.report:
        report()
    if a.publish:
        publish(dry_run=a.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

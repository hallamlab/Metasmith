"""The reference gate: check what the build produced, against what it claims.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python build_references/check_references.py --results <run>/results

Ported from the pre-library ``build_references/check_references.py``, with its path
registry replaced by explicit arguments -- the tables now come out of a metasmith run
directory, not out of a fixed layout under ``data/``.

NOTE vs FAILURE, which is the distinction the whole script is built around: a gap you can
SEE is a different thing from a gap you cannot. A reference that covers less than you
hoped is a NOTE; a reference that disagrees with itself is a FAILURE. Only the second
exits non-zero.

The load-bearing check is the equivalence one. ``ecspr.bake.metabolism``'s own selftest proves
the encoding round-trips row by row; this proves the encoded tables BUILD THE SAME GRAPH
as the reference builder does from the string tables -- same nodes, same edge count, same
conductances, per element. Those are different claims: a bake can round-trip perfectly and
still be joined wrongly at compile time, and the resulting graph is plausible rather than
broken.

The constants check exists for a subtler reason. ``ecspr.bake.direction.canon`` is a copy of
``src/fabfos/_deprecated_canon.py``'s ``DIR_*`` block, because the direction ensemble runs in a
conda env that has no import path to the fabfos package. Two copies of a constant drift, and
this pair drifts silently: one COMPUTES a ratio and the other VALIDATES a table carrying
one, so a divergence produces a table that passes its own validator while meaning
something else.
"""
from __future__ import annotations

import argparse
import importlib
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
BREF = Path(__file__).resolve().parent
# Both halves of what this gate compares now come from one package: `ecspr.bake` is
# the build method and `ecspr.model` is the run-side consumer it is checked against.
# Nothing is added to sys.path -- an ImportError below means the env lacks `ecspr`
# rather than that a path was wrong.

from ecspr.bake import encoding as refs  # noqa: E402

FAILURES: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""),
          flush=True)
    if not ok:
        FAILURES.append(label)


def note(msg: str) -> None:
    print(f"  [NOTE] {msg}", flush=True)


def find(results: Path, name: str) -> Path | None:
    """The first FILE matching `name`. Directories are skipped rather than returned:
    a bake chunk names its seam directory `aam_pairs/`, so `*aam_pairs*` matched the
    directory, and `read_parquet` died on the zero-byte `emptied.txt` inside it.
    """
    hits = sorted(p for p in results.rglob(name) if p.is_file())
    return hits[0] if hits else None


# =====================================================================

def check_bake(vocab_p: Path, pairs_p: Path, dir_p: Path) -> dict | None:
    print("\nbake -- the trio is one artifact")
    try:
        ident = refs.assert_same_bake(vocab_p, pairs_p, dir_p)
    except Exception as e:
        check("all three files carry the same bake identity", False, str(e))
        return None
    check("all three files carry the same bake identity", True,
          f"bake {ident['vocab_sha256'][:16]}")
    check("rank field is wide enough for the observed maximum",
          ident["max_atom_rank"] < (1 << ident["rank_bits"]),
          f"{ident['max_atom_rank']} < {1 << ident['rank_bits']}")
    check("metabolite field is wide enough for the vocabulary",
          ident["n_met"] <= (1 << ident["met_bits"]),
          f"{ident['n_met']:,} <= {1 << ident['met_bits']:,}")
    check("edge key fits in int64",
          2 * (ident["met_bits"] + ident["rank_bits"]) <= refs.NODE_KEY_BUDGET)
    # n_rxn is the size of the reaction UNIVERSE, not of the set the pairs cover -- the
    # vocabulary is coded against lookup::reactions so that `ratio_by_code` is fully
    # addressed. Reporting it as "atom pairs over N reactions" read as coverage and was
    # wrong by ~20k.
    note(f"{ident['atom_pairs_rows']:,} atom pairs, {ident['n_met']:,} metabolites, "
         f"coded against a {ident['n_rxn']:,}-reaction universe")
    fmeta = refs.read_file_meta(dir_p)
    if fmeta:
        note(f"{fmeta['direction_rows']:,} reactions carry a direction row")
    return ident


def check_equivalence(ident: dict, vocab_p: Path, pairs_p: Path, dir_p: Path,
                      src_pairs: Path, src_dir: Path) -> None:
    """The compiled tables must build the same graph as the reference builder.

    ``ecspr.model.build.graph_from_pairs`` works on the STRING tables and is the definition;
    ``ecspr.bake.encoding.compile_atom_graph`` works on the compiled ones and is the thing being
    checked. Node ORDER differs -- the baked table is sorted, so first-seen order differs
    -- which is why nodes are compared as sets and conductances sorted before comparison.
    """
    print("\nequivalence -- the compiled tables build the same graph as the builder")
    try:
        from ecspr.model.build import graph_from_pairs
    except ImportError as e:
        note(f"the `ecspr` package is not installed in this env ({e}); equivalence "
             f"not checked -- run this under the `ecspr` or `build-refs-cobra` env")
        return
    if not (src_pairs.exists() and src_dir.exists()):
        note("the ensemble intermediates are not in this run's results, so the string "
             "tables the builder needs are unavailable; equivalence not checked")
        return

    src = pd.read_parquet(src_pairs)
    dsrc = pd.read_parquet(src_dir, columns=["mnxr", "ratio"])
    ratios = {str(r): float(v) for r, v in zip(dsrc["mnxr"], dsrc["ratio"])
              if str(r) != "EMPTY"}
    # Uniform weights over every reaction the pairs cover. The deployed gate used a
    # staged evidence-weight fixture; uniform E is the same test with one fewer input --
    # what is being compared is the join, the flip and the edge factorisation, none of
    # which depends on the weights being interesting.
    weights = {str(r): 1.0 for r in src["mnxr"].unique()}

    V = refs.load_vocab(vocab_p)
    direction = refs.load_direction(dir_p)
    lut = refs.ratio_by_code(V, direction)
    t0 = time.perf_counter()
    allp = refs.load_atom_pairs(pairs_p)
    t_load = time.perf_counter() - t0

    t_ref = t_bake = 0.0
    for X in refs.ELEMENT_ORDER:
        t0 = time.perf_counter()
        g = graph_from_pairs(src, X, weights, ratios)
        t_ref += time.perf_counter() - t0
        t0 = time.perf_counter()
        b = refs.compile_atom_graph(X, weights, ident=ident, vocab=V, pairs=allp,
                                    ratio_lut=lut)
        t_bake += time.perf_counter() - t0
        same = (g.n == b.n and g.m == b.m and set(g.nodes) == set(b.nodes)
                and np.allclose(np.sort(g.gp), np.sort(b.gp), rtol=1e-6, atol=0)
                and np.allclose(np.sort(g.gm), np.sort(b.gm), rtol=1e-6, atol=0))
        check(f"element {X}: same nodes, edges and conductances", same,
              f"{g.n:,} nodes / {g.m:,} edges from "
              f"{g.meta['n_reactions_used']:,} reactions")
    note(f"compile: {t_bake*1000:.0f} ms + {t_load*1000:.0f} ms load, against the "
         f"reference builder's {t_ref*1000:.0f} ms")


MIRROR = "fabfos._deprecated_canon"


def check_direction_constants() -> None:
    """The build-side copy of the DIR_* block must equal the run-side one.

    See the module docstring: these are the same numbers used at two different times, and
    a divergence is invisible from either side alone.

    A FAILURE TO IMPORT IS A FAILURE, not a note. This check spent a generation passing
    because it named `fabfos.canon`, which had been renamed to `fabfos._deprecated_canon`
    -- the ImportError was caught, reported as a note, and the mirror it exists to guard
    went unchecked through every run since. An unimportable mirror and a diverged one are
    the same outcome for the reader, so they get the same verdict.
    """
    print("\nconstants -- the direction ensemble's two copies agree")
    sys.path.insert(0, str(REPO / "src"))
    try:
        from ecspr.bake.direction import canon as dir_canon
        canon = importlib.import_module(MIRROR)
    except Exception as e:                                       # pragma: no cover
        check(f"both copies of the DIR_* block import", False,
              f"{type(e).__name__}: {e}")
        return
    names = [n for n in dir(dir_canon) if n.startswith("DIR_")]
    bad = [n for n in names
           if not hasattr(canon, n) or getattr(canon, n) != getattr(dir_canon, n)]
    check(f"all {len(names)} DIR_* constants match {MIRROR}", not bad,
          f"diverged: {bad}" if bad else f"{len(names)} names")


def check_bridge(bridge_p: Path) -> None:
    print("\nbridge -- one table, three id spaces")
    if not bridge_p.exists():
        note("mnxr_lookup.parquet is not in this run's results; bridge not checked")
        return
    b = pd.read_parquet(bridge_p, columns=["id", "id_source", "mnxr", "evidence_quality"])
    per_id = b.groupby("id")["id_source"].nunique()
    clashes = int((per_id > 1).sum())
    # `id_source` is a LABEL, not a disambiguator -- the consumer slices on it and joins on
    # `id` alone, so a collision would mix two namespaces' claims into one lane.
    check("no id appears in more than one id_source", clashes == 0,
          f"{clashes:,} colliding ids" if clashes else f"{b['id'].nunique():,} distinct ids")
    dupes = int(b.duplicated(subset=["id", "mnxr"]).sum())
    check("deduped to distinct (id, mnxr)", dupes == 0, f"{dupes:,} duplicate pairs")
    note(f"{len(b):,} rows, {b['mnxr'].nunique():,} MNXR, "
         f"sources {b['id_source'].value_counts().to_dict()}")
    note(f"evidence_quality {b['evidence_quality'].value_counts().to_dict()}")


def check_gem_tables(results: Path) -> None:
    print("\nGEM GPR -- ruleless rows kept, and EPI300 == DH10B modulo host")
    tables = sorted(results.rglob("*gpr_table_gem*")) + sorted(results.rglob("gpr_gem*"))
    tables = [t for t in tables if t.suffix == ".parquet"]
    if not tables:
        note("no GEM GPR tables in this run's results; not checked")
        return
    frames = {}
    for t in tables:
        d = pd.read_parquet(t)
        host = d["host"].iat[0] if len(d) else t.stem
        frames[host] = d
        n_ruleless = int((d["feature_kind"] == "ruleless").sum())
        # Dropping ruleless reactions makes every gene set look like starvation, because
        # exchanges and spontaneous chemistry are live in every condition.
        check(f"{host}: ruleless reactions have rows", n_ruleless > 0,
              f"{n_ruleless:,} of {len(d):,}")
        uniform = bool((d["raw_score"] == 1.0).all())
        check(f"{host}: raw_score is uniform 1.0", uniform,
              "a curated model asserts presence, not evidence strength")

    if "e_coli_epi300" in frames and "e_coli_dh10b" in frames:
        cols = [c for c in frames["e_coli_epi300"].columns if c != "host"]
        same = frames["e_coli_epi300"][cols].reset_index(drop=True).equals(
            frames["e_coli_dh10b"][cols].reset_index(drop=True))
        # They share iECDH10B_1368 and the measured edit list is EMPTY, so identical is
        # the correct outcome; a divergence means one of them silently used another model.
        check("EPI300 and DH10B tables are identical apart from the host tag", same)



# =====================================================================
# The annotation references, checked where they are PINNED rather than where a run
# happened to leave them. Each of the five the four canonical lanes need gets a
# named assertion, because "the file exists" is what every one of these failed on
# in a way that only showed up hours downstream.
# =====================================================================

# The query lane emits 512 dims (functionalAnnotation/proteinbert.py takes the last
# 512 of ProteinBERT's 3,072-wide global representation). A pool of any other width
# does not produce a worse kNN vote -- it produces a shape error, or worse, a
# silently valid dot product against a different space.
POOL_EMBED_DIM = 512
# ESM-C 600M is 1152-dim; the ESM-C pool and the query lane must agree on it.
ESMC_EMBED_DIM = 1152
# Five per head is what EZpred's DL-only path averages over.
EZPRED_ENSEMBLE = 5


def check_annotation_refs(processed: Path) -> None:
    print(f"\nannotation references -- {processed}")
    if not processed.is_dir():
        note(f"{processed} does not exist; the annotation half has not been published")
        return

    # --- R3: the KOfam pair. They are ONE artifact in two files.
    profiles = processed / "kofam_ref" / "profiles"
    ko_list = processed / "kofam_ref" / "ko_list.tsv"
    if profiles.is_dir() and ko_list.exists():
        hmms = list(profiles.rglob("*.hmm"))
        kl = pd.read_csv(ko_list, sep="\t", dtype=str)
        check("kofam: profiles present", len(hmms) > 1000, f"{len(hmms):,} .hmm files")
        check("kofam: ko_list carries thresholds",
              {"knum", "threshold"} <= set(kl.columns),
              f"columns {list(kl.columns)[:6]}")
        # A ko_list paired with profiles from another build applies the wrong cut to
        # every hit and raises nothing, so the pairing is the assertion -- but only in
        # ONE direction, and it was written in the other.
        #
        # A profile with no threshold is the failure: hmmsearch finds it and nothing
        # says where to cut, so every hit on that family is unscored. A ko_list entry
        # with no profile is not: hmmsearch simply cannot produce a hit for it. KOfam
        # ships exactly that gap upstream -- 28,277 listed against 27,754 profiled in
        # release 2026-06-30, verified against `profiles.tar.gz` itself -- so asserting
        # it away would fail every faithful build of this reference forever.
        have = {p.stem for p in hmms}
        want = set(kl["knum"].dropna())
        unscored = have - want
        check("kofam: every profile has a threshold", not unscored,
              f"{len(unscored):,} of {len(have):,} profiles are not in ko_list, so a "
              f"hit on them would be cut at nothing")
        unprofiled = want - have
        if unprofiled:
            note(f"kofam: {len(unprofiled):,} of {len(want):,} listed KOs have no "
                 f"profile and therefore cannot be hit -- upstream ships this gap")
    else:
        note("kofam_ref/{profiles,ko_list.tsv} not published; not checked")

    # --- R4: the DIAMOND database. Its sequence count is readable only by diamond
    # itself, so absent the binary this is a size floor and says so.
    dmnd = processed / "uniref50_dmnd" / "uniref50.dmnd"
    if dmnd.exists():
        gb = dmnd.stat().st_size / 1e9
        check("uniref50: database is a plausible size", gb > 5.0, f"{gb:.1f} GB")
        import shutil as _sh, subprocess as _sp
        if _sh.which("diamond"):
            r = _sp.run(["diamond", "dbinfo", "-d", str(dmnd)],
                        capture_output=True, text=True)
            note("uniref50: " + " ".join(r.stdout.split()))
        else:
            note("uniref50: `diamond` is not on PATH, so the sequence count -- the "
                 "only check that compares the database against its source fasta -- "
                 "was not run")
    else:
        note("uniref50_dmnd/uniref50.dmnd not published; not checked")

    # --- R7: the labelled landmarks. One row per accession, carrying its labels and
    # its embedding, so the checks are about the CONTENT rather than about two files
    # agreeing -- which is what the retired index-beside-a-stack layout got wrong.
    lm = processed / "label_transfer_landmarks" / "landmarks"
    lm_table = lm / "landmarks.parquet"
    if lm.is_dir():
        if lm_table.exists():
            t = pd.read_parquet(lm_table)
            dims = [c for c in t.columns if c.startswith("dim_")]
            check("landmarks: dim columns run contiguously from 0",
                  sorted(dims, key=lambda c: int(c[4:]))
                  == [f"dim_{i}" for i in range(len(dims))],
                  "a gap stacks into a matrix of the wrong width rather than failing")
            check("landmarks: embedding width matches what the query lane emits",
                  len(dims) == POOL_EMBED_DIM,
                  f"{len(dims)} dims, expected {POOL_EMBED_DIM}")
            check("landmarks: every accession appears once",
                  t["accession"].is_unique)
            unlabelled = int((t["mnxr_list"].fillna("") == "").sum())
            check("landmarks: every member carries a label", unlabelled == 0,
                  f"{unlabelled:,} unlabelled -- the set was SELECTED on having one")
            # A deterministic embedder gives identical sequences identical vectors, so
            # duplicated rows are expected and duplicated rows ALONE are not. A stack
            # assembled in the wrong chunk order still holds the right vectors, and the
            # only thing that betrays it without the sequences in hand is the labels
            # landing on rows that do not match: see `research/.../repair_pool_index.py`
            # for the full signature test. This is its cheap half.
            n_dup_vec = len(t) - len(t.drop_duplicates(subset=dims))
            note(f"landmarks: {len(t):,} references, "
                 f"{t['mnxr_list'].str.split(';').explode().nunique():,} distinct MNXR, "
                 f"{n_dup_vec:,} rows share their embedding with another")
            src = lm / "source.txt"
            if src.exists():
                for line in src.read_text().strip().splitlines():
                    note("landmarks: " + line.replace("\t", " = "))
            else:
                # Without it, which model and which sequence release produced the
                # landmarks is not recoverable from the table.
                check("landmarks: source.txt records model and release", False)
        else:
            check("landmarks: landmarks.parquet is present", False, str(lm_table))
    else:
        note("label_transfer_landmarks/landmarks not published; not checked")

    # --- R8: the ESM-C weights, for the decided-against ESM-C and EZpred lanes.
    esmc = processed / "esm_c_weights" / "esmc_600m.tgz"
    if esmc.exists():
        import tarfile
        want = "data/weights/esmc_600m_2024_12_v0.pth"
        with tarfile.open(esmc, "r:gz") as tf:
            members = {m.name.lstrip("./"): m for m in tf.getmembers()}
        m = members.get(want)
        check("esm_c: the checkpoint is in the archive at the path the SDK resolves",
              m is not None, want)
        if m is not None:
            check("esm_c: checkpoint is a plausible size", m.size > 2_000_000_000,
                  f"{m.size:,} bytes -- a gated 401 body and a truncated transfer "
                  f"both leave a member that exists")
    else:
        note("esm_c_weights/esmc_600m.tgz not published; the ESM-C and EZpred lanes "
             "are not runnable")

    # --- R10: the ESM-C half of the landmarks. Everything R7 is checked for, plus the
    # one thing that only makes sense across the two: they must describe the SAME
    # accessions in the SAME order. A kNN lane compares its query against whichever
    # landmarks it was pointed at, so two sets that disagree about row i do not fail --
    # they answer, from the wrong reference, and the two lanes stop being comparable,
    # which is the entire reason the ESM-C lane exists.
    lm_e = processed / "label_transfer_landmarks_esmc" / "landmarks_esmc"
    lm_e_table = lm_e / "landmarks.parquet"
    if lm_e.is_dir():
        if lm_e_table.exists():
            t_e = pd.read_parquet(lm_e_table)
            dims_e = [c for c in t_e.columns if c.startswith("dim_")]
            check("landmarks_esmc: embedding width is ESM-C 600M's",
                  len(dims_e) == ESMC_EMBED_DIM,
                  f"{len(dims_e)} dims, expected {ESMC_EMBED_DIM}")
            check("landmarks_esmc: every accession appears once",
                  t_e["accession"].is_unique)
            unlabelled = int((t_e["mnxr_list"].fillna("") == "").sum())
            check("landmarks_esmc: every member carries a label", unlabelled == 0,
                  f"{unlabelled:,} unlabelled")
            if lm_table.exists():
                t_b = pd.read_parquet(lm_table, columns=["accession"])
                check("landmarks_esmc: same accessions, same order as the pbert set",
                      len(t_e) == len(t_b)
                      and list(t_e["accession"]) == list(t_b["accession"]),
                      "the two kNN lanes must differ in their embedder and in nothing "
                      "else")
            src = lm_e / "source.txt"
            if src.exists():
                for line in src.read_text().strip().splitlines():
                    note("landmarks_esmc: " + line.replace("\t", " = "))
        else:
            check("landmarks_esmc: landmarks.parquet is present", False, str(lm_e_table))
    else:
        note("label_transfer_landmarks_esmc/landmarks_esmc not published; the ESM-C kNN "
             "lane is not runnable")

    # --- R9: the EZpred bundle. It has NO producer in the graph -- its compile is
    # parked in transforms/_deferred/ -- so the pin plus these assertions are the whole
    # of what makes it reproducible. Five members per head is what the DL-only path
    # averages over; four is a different model wearing the same name, and unzip returns
    # 0 on a member it never matched.
    ez = processed / "ezpred_model" / "EZpred"
    if ez.is_dir():
        check("ezpred: predict.py is present", (ez / "predict.py").exists())
        check("ezpred: settings.py is present", (ez / "settings.py").exists())
        for head in ("enzyme", "nonenzyme"):
            d = ez / "models" / head / "EC"
            n = len(list(d.glob("*.pt"))) if d.is_dir() else 0
            check(f"ezpred: the {head} head carries its full {EZPRED_ENSEMBLE}-member "
                  f"ensemble", n == EZPRED_ENSEMBLE,
                  f"{n} .pt files -- a partial unpack yields a bundle that imports "
                  f"cleanly and then predicts from a smaller ensemble than it names")
            check(f"ezpred: the {head} head has its child matrix",
                  (d / "child_matrix_ssp.npz").exists() if d.is_dir() else False)
        check("ezpred: parse_isa is present -- predict() shells out to it to rewrite "
              "its own output", (ez / "utils" / "parse_isa").exists())
    else:
        note("ezpred_model/EZpred not published; the EZpred lane is not runnable")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, default=None,
                    help="a metasmith run's results directory. Optional: the "
                         "annotation half is checked against the PIN, which outlives "
                         "the run, so `--processed` alone is a complete gate for it. "
                         "Only the metabolism checks need a results tree.")
    ap.add_argument("--processed", type=Path, default=REPO / "data" / "fabfos" / "processed",
                    help="the pinned processed tier, where the annotation references "
                         "are checked -- a run's results are transient, the pin is not")
    a = ap.parse_args()
    results = a.results
    if results is not None and not results.exists():
        raise SystemExit(f"no results at {results}")

    if results is None:
        print(f"== reference gate over {a.processed} (pin only) ==")
        check_annotation_refs(a.processed)
        print()
        if FAILURES:
            print(f"{len(FAILURES)} FAILED: " + "; ".join(FAILURES))
            return 1
        print("all checks passed")
        return 0

    print(f"== reference gate over {results} ==")
    vocab_p = find(results, "*metabolism_vocab*") or find(results, "vocab.parquet")
    pairs_p = find(results, "*atom_pairs*") or find(results, "atom_pairs.parquet")
    dir_p = find(results, "*direction_ratios*") or find(results, "direction.parquet")
    src_pairs = find(results, "*aam_pairs*")
    src_dir = find(results, "*direction_annotation*")
    bridge_p = find(results, "*mnxr_lookup*")

    if vocab_p and pairs_p and dir_p:
        ident = check_bake(vocab_p, pairs_p, dir_p)
        if ident and src_pairs and src_dir:
            check_equivalence(ident, vocab_p, pairs_p, dir_p, src_pairs, src_dir)
        elif ident:
            note("the ensemble intermediates are transient and were not collected, so "
                 "the equivalence check has no string tables to compare against")
    else:
        note("the metabolism trio is not in this run's results; bake not checked")

    check_direction_constants()
    if bridge_p:
        check_bridge(bridge_p)
    else:
        note("mnxr_lookup.parquet is not in this run's results; bridge not checked")
    check_gem_tables(results)
    check_annotation_refs(a.processed)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: " + "; ".join(FAILURES))
        return 1
    print("all checks passed (see NOTEs above for what was not checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

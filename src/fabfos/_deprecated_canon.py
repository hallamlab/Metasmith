"""The canonical ECSPr basis, as data.

This module is the only place these values exist. Prose may point at a name
defined here; prose may not restate its value. That rule is the whole point of
the file, and it is checked mechanically by `check_no_transcribed_numbers.py`.

The failure this corrects was not an absence of documentation. There were four
live documents each declaring itself the canonical method, and they contradicted
each other, because nothing checked them against anything. A fifth well-written
document would reproduce the disease exactly. So the countermeasure is not a
better document: it is that there is nothing left to transcribe. Import the name.

Two consumers, two directions:

  * Experiment drivers (`run_experiment.py`) read these values and stage them as
    inputs. Values flow OUT of this module into the engine.
  * Figure generators call the assertion helpers to refuse a table that is not
    the canonical one.

The engine library (`metasmith-libraries/fabfos`) MUST NOT import this module.
It is a consumer of the basis, never a reader of it — the reverse edge, library
depending on a specific experiment, is the disease we are removing. The engine
derives what it needs from its staged inputs.

Status is `STATUS` below, and it is load-bearing: while PROVISIONAL this file
merely *describes what the incumbent claims*. It becomes the definition only when
the parity gate proves the FabFos path reproduces the incumbent. See MIGRATION.md.
"""
from __future__ import annotations

from pathlib import Path

# =====================================================================
# Status
# =====================================================================
# CANONICAL since 2026-07-14, when the parity gate went green on both lanes:
# the engine reproduces the incumbent tables to ~1e-16 (machine epsilon) on every
# joined row, with exact at_floor agreement and the split contigs present.
# Re-provable at any time, in seconds, on CPU:
#
#     python parity/run_parity.py --all
#
# This is a handoff, not just a test. From this line the incumbent tables are a
# FROZEN REFERENT -- read, never authoritative -- and this file is the definition.
# It proves reproduction, not truth: it shows the engine reproduces the incumbent,
# and does not revisit whether the incumbent is right.
STATUS = "CANONICAL"
STATUS_SINCE = "2026-07-14"
STATUS_NOTE = (
    "Parity gate green on both lanes at ~1e-16 vs the incumbent; "
    "the incumbent is now a frozen referent. See MIGRATION.md."
)

# =====================================================================
# Roots -- resolved through the data-dependency library, not hardcoded
# =====================================================================
# Every reference path below is a KEY into `.awm/data/ref/`, the metasmith
# DataInstanceLibrary built by `transforms/build/_stage/build_ref_library.py`.
# Nothing here is an absolute path to this machine.
#
# Resolution is lazy (PEP 562 module-level __getattr__) and memoized, so
# importing canon for a scalar -- `canon.KMAX`, `canon.DRAW_SIZES` -- costs
# nothing and works with no library present and no metasmith installed. That
# matters: the paper worktrees import this module for its constants and do not
# have metasmith.
#
# Failure is LOUD. A missing key raises CanonError naming both the canon symbol
# and the key it wanted. There is deliberately NO fallback to an absolute path:
# a fallback would make the whole migration untestable, because everything would
# keep working whether or not the library was correct.
import os as _os

_LIB_ENV = "FABFOS_REF"
_DEFAULT_LIB = Path(__file__).resolve().parents[2] / ".awm" / "data" / "ref"


class CanonError(RuntimeError):
    """A canon symbol could not be resolved through the data library."""


def library_root() -> Path:
    root = Path(_os.environ.get(_LIB_ENV, _DEFAULT_LIB))
    if not (root / "_metadata").is_dir():
        raise CanonError(
            f"no data library at [{root}]. Set ${_LIB_ENV}, or build it with\n"
            f"    python transforms/build/_stage/build_ref_library.py --stage --place --index"
        )
    return root


# -----------------------------------------------------------------
# Experiment roots -- deliberately NOT library items
# -----------------------------------------------------------------
# Run results on actual use cases are out of scope for the data-dependency
# library: they are outputs of a particular experiment, not dependencies of the
# method. But canon still has to name them, so they get env-overridable roots
# rather than the hardcoded absolutes they used to be. Point $FABFOS_DATA at
# your own run tree and the ORFS_FAA / EVIDENCE_TABLE / INSERTS_FNA / AXES_TSV
# constants follow.
DATA = Path(_os.environ.get("FABFOS_DATA", "/home/tony/agentic_workspace/data/scadc"))
INCUMBENT_ROOT = Path(_os.environ.get(
    "FABFOS_INCUMBENT",
    "/home/tony/agentic_workspace/projects/scadc/metabolic-modelling"
    "/main/metabolic-modelling/04_reaction_network",
))
INCUMBENT_CACHE = INCUMBENT_ROOT / "cache"
# The hand-made byte-copy backup.
#
# CORRECTED 2026-07-20 -- this used to read "the live cache is never a safe source; this
# directory is." For the CURRENT draw grid that is stale AND inverted, and following it
# silently narrows the null basis:
#
#   * K1000 holds ONLY retired sizes, every one stamped 2026-07-14. It has NONE of the
#     live grid.
#   * The live cache holds the whole live grid, written in one coherent run on 2026-07-18.
#
# So for DRAW_SIZES the live cache is both the only source and the internally consistent
# one, and build_directed_null.py reading it is correct rather than the defect it looks
# like. The one divergence, cache/N14 vs K1000/N14, is 1,000 of 4,000 rows confined
# ENTIRELY to null style E (A/B/D byte-identical): old-E vs new-E, not corruption.
#
# NEITHER directory is safe to GLOB -- both mix live and retired sizes. Curate by explicit
# list (see FROZEN_NULL_FILES) and read the grid from DRAW_SIZES.
INCUMBENT_K1000 = INCUMBENT_CACHE / "K1000"
ENGINE_LIB = Path(_os.environ.get(
    "FABFOS_ENGINE_LIB",
    "/home/tony/agentic_workspace/projects/metasmith-libraries/fabfos",
))

# The experiment-side METHOD scripts -- the solve/null/parity/direction spine. Not a
# library item (it is code, not data) and not importable as a package (the scripts are
# CLIs that import each other as siblings), so consumers put the relevant subdirectory on
# sys.path. Named here so they stop doing it with a relative hop: the scripts used to sit
# in a `main/fabfos/` copy inside each figure scope, and a consumer reached them with
# `HERE.parents[1] / "fabfos" / "directed"` -- which silently bound to whichever scope's
# copy the caller happened to live in.
METHODS_DIR = Path(__file__).resolve().parents[2] / "methods"

_manifest_cache: dict | None = None


def _manifest() -> dict:
    """path -> type name, read once.

    Prefers metasmith's loader; falls back to reading _metadata/index.yml
    directly so a worktree without metasmith can still resolve paths.
    """
    global _manifest_cache
    if _manifest_cache is not None:
        return _manifest_cache
    root = library_root()
    try:
        from metasmith.models.libraries import DataInstanceLibrary
        lib = DataInstanceLibrary.Load(root)
        _manifest_cache = {str(k): v for k, v in lib.manifest.items()}
    except Exception:
        import yaml
        index = root / "_metadata" / "index.yml"
        if not index.exists():
            raise CanonError(f"library at [{root}] has no _metadata/index.yml")
        raw = yaml.safe_load(index.open()) or {}
        # index.yml IS the manifest -- a flat `path: type` mapping written in
        # YAML's explicit-key form. It has no `manifest:` wrapper, so asking
        # for one yields {} and every symbol then fails as "not in the
        # manifest", pointing at the declaration instead of at this reader.
        # Accept the wrapper if a future writer adds one; otherwise take the
        # document itself.
        man = raw.get("manifest", raw) if isinstance(raw, dict) else {}
        _manifest_cache = {
            str(k): (v["type"] if isinstance(v, dict) else v)
            for k, v in man.items()
        }
    # An EMPTY manifest is a broken read, never a legitimately empty library:
    # every caller is asking for a path that must exist. Failing here names the
    # real fault; failing later names an innocent symbol.
    if not _manifest_cache:
        raise CanonError(
            f"library at [{root}] resolved an EMPTY manifest. The library is "
            f"not built, or _metadata/index.yml is not in the expected "
            f"`path: type` form. This is a reader/library fault, not a bad "
            f"symbol -- do not chase the declaration."
        )
    return _manifest_cache


def _resolve(symbol: str, key: str) -> Path:
    man = _manifest()
    root = library_root()
    if key in man:
        return root / key
    # a directory item is not itself a manifest key -- its members are
    prefix = key.rstrip("/") + "/"
    if any(k.startswith(prefix) for k in man):
        return root / key
    raise CanonError(
        f"canon.{symbol} wants library key [{key}], which is not in the manifest "
        f"at [{root}]. Either the library is stale (rebuild it) or the "
        f"declaration in provenance/data/_declared.yml is wrong."
    )


# canon symbol -> library key. The ONE table mapping code to data.
_PATHS: dict[str, str] = {
    "REFERENCE_ROOT":              "derived/mnxref-4_5",
    "REFERENCE_GRAPH_DIR":         "derived/mnxref-4_5/graph",
    "REFERENCE_SOLVE_DIR":         "derived/mnxref-4_5/solve",
    "REFERENCE_SOLVE_DIRECTED_DIR":"derived/mnxref-4_5/solve_directed",
    "REFERENCE_DIRECTION":         "derived/mnxref-4_5/direction.parquet",
    "REFERENCE_MANIFEST":          "derived/mnxref-4_5/MANIFEST.json",
    "REFERENCE_ATOM_PAIRS":        "derived/mnxref-4_5/atom_pairs.parquet",
    "REFERENCE_LEDGER":            "derived/mnxref-4_5/closure_ledger.parquet",
    "REFERENCE_NULL_UNDIRECTED_DIR": "derived/mnxref-4_5/null",
    "REFERENCE_NULL_DIRECTED_DIR": "derived/mnxref-4_5/null_directed",
    "AXES_TESTABLE_JSON":          "derived/mnxref-4_5/solve/axes_testable.json",
    "SIGNIFICANCE_DIR":            "validation/significance",
    "VALIDATION_DIR":              "validation/dual_network",
    "REAC_PROP":                   "external/metanetx/4.5/reac_prop.tsv",
    "CHEM_PROP":                   "external/metanetx/4.5/chem_prop.tsv",
    "CHEM_XREF":                   "external/metanetx/4.5/chem_xref.tsv",
    "REAC_XREF":                   "external/metanetx/4.5/reac_xref.tsv",
    "DIR_REAC_PROP":               "external/metanetx/4.5/reac_prop.tsv",
    "DIR_CHEM_PROP":               "external/metanetx/4.5/chem_prop.tsv",
    "DIR_CHEM_XREF":               "external/metanetx/4.5/chem_xref.tsv",
    "NETA_GEM":                    "external/gem/iECDH10B_1368.json",
    "DIR_DATA":                    "derived/direction",
    "DIR_TABLE":                   "derived/direction/direction_annotation.parquet",
    "DIR_CALIBRATION":             "derived/direction/calibration.parquet",
    "DIR_CURATED":                 "derived/direction/curated_per_mnxr.parquet",
    # Network A's OWN direction table -- NOT the thermodynamic ensemble. Network B and
    # the reference take direction from exp(dG'/RT); Network A was built from a curated
    # GEM (iECDH10B) whose reactions encode direction NATIVELY as flux bounds, so its
    # honest directionality is those bounds, not thermodynamics. Two columns
    # (mnxr, ratio) -- the exact ecspr_network.load_direction_ratios contract.
    "NETA_DIR_TABLE":              "derived/direction/netA_gem_direction.parquet",
    "METACYC_FLATFILES":           "external/licensed/metacyc26_flatfiles",
    "UNIREF50_DMND":               "derived/uniref50/uniref50.dmnd",
    "ESMC_WEIGHTS":                "external/esmc/esmc_600m.tgz",
    "KOFAM_KO_LIST":               "external/kofam/ko_list",
    "KOFAM_PROFILES":              "external/kofam/profiles.tar.gz",
    # Aliases the original file defined by plain assignment. They must route
    # through __getattr__ too: a module-level binding always wins over
    # __getattr__, so leaving the assignment in would silently restore the old
    # absolute path while every test still passed.
    "BIPARTITE_DIR":               "derived/mnxref-4_5/graph",
    "SOLVE_BASE_DIR":              "derived/mnxref-4_5/solve",
    # ---- the evidence basis ----
    # These four were the last inputs on the method path still resolving to
    # absolute paths in the incumbent tree. EVIDENCE_WEIGHTS is the one that
    # cost something: with no symbol here, it could not be repointed when the
    # basis moved to the 199-fosmid CLEAN evidence, so it silently stayed
    # pre-CLEAN and set the effective host universe. See evidence.weights in
    # _declared.yml. A symbol that does not exist upstream cannot be rewritten
    # by this table, which is why canon.py had to name it first.
    "EVIDENCE_TABLE":              "derived/evidence/evidence_table_clean.parquet",
    "EVIDENCE_WEIGHTS":            "derived/evidence/evidence_weights.parquet",
    "ADDITION_WEIGHTS":            "derived/evidence/fosmid_addition_weights.pkl",
    "AXES_JSON":                   "derived/axes/biomass_dag_axes_set4.json",
    # ---- the X/Y benchmark, v3 ----
    # One SELF-CONTAINED tree: X, the contract shape, the ground truth the key
    # is derived from, the baseline and v1's provenance all live inside v3, so
    # deleting a sibling version cannot break this one. BENCH_V3_Y is the
    # answer key and is declared MISSING until the v3 key is built and frozen;
    # touching it before then raises CanonError naming the symbol, which is the
    # intended refusal -- scoring against an absent key must never quietly
    # produce an empty result.
    "BENCH_V3_ROOT":               "validation/benchmark/v3",
    "BENCH_V3_OBSERVATIONS":       "validation/benchmark/v3/observations",
    "BENCH_V3_DECISIONS":          "validation/benchmark/v3/decisions",
    "BENCH_V3_X":                  "validation/benchmark/v3/X",
    "BENCH_V3_CONTRACT":           "validation/benchmark/v3/contract",
    "BENCH_V3_GROUND_TRUTH":       "validation/benchmark/v3/ground_truth",
    "BENCH_V3_BASELINE":           "validation/benchmark/v3/baseline",
    "BENCH_V3_V1_PROVENANCE":      "validation/benchmark/v3/v1",
    "BENCH_V3_BASE_GRAPHS":        "validation/benchmark/v3/base_graphs",
    "BENCH_V3_UNIVERSE":           "validation/benchmark/v3/universe",
    "BENCH_V3_Y":                  "validation/benchmark/v3/Y",
}

# Declared, but absent from every machine we have looked at. Named so the
# failure says what is missing and how to get it, instead of FileNotFoundError
# on a path nobody recognises.
_UNAVAILABLE: dict[str, str] = {
    "DIR_METACYC_PGDB": (
        "metacyc26.pgdb is not on this machine and was not found anywhere in the "
        "data tree. It is a licensed BioCyc artifact (subscription required, not "
        "redistributable). Only metacyc26_flatfiles survives -- see "
        "canon.METACYC_FLATFILES and provenance/data/biocyc.pgdbs.yml."
    ),
    "DIR_ECOCYC_PGDB": (
        "ecocyc26.pgdb is not on this machine and was not found anywhere in the "
        "data tree. See provenance/data/biocyc.pgdbs.yml for acquisition and for "
        "what the direction ensemble loses without it."
    ),
}


def __getattr__(name: str) -> Path:
    """PEP 562: resolve data paths on first touch, never at import."""
    if name in _UNAVAILABLE:
        raise CanonError(f"canon.{name}: {_UNAVAILABLE[name]}")
    if name in _PATHS:
        return _resolve(name, _PATHS[name])
    if name == "REFERENCE_NULL_DIR":
        return __getattr__(
            "REFERENCE_NULL_DIRECTED_DIR" if _DIRECTED else "REFERENCE_NULL_UNDIRECTED_DIR"
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _lib(name: str) -> Path:
    """Resolve a library-backed symbol FROM INSIDE this module.

    Module-level `__getattr__` is only consulted for attribute access from OUTSIDE
    (`canon.REFERENCE_SOLVE_DIR`). A bare name in a function body here is an ordinary
    global lookup, and since these symbols deliberately have no module-level binding --
    that is the whole point, an assignment would shadow __getattr__ and silently restore
    a hardcoded path -- the lookup raises NameError.

    Three functions did exactly that and were dead on every call:
    reference_axes_report(), undirected_axes_report(), assert_canonical_reference().
    They were never covered, because check_canon.py tests the ATTRIBUTE path
    (getattr(canon, name)), which works fine. Route internal reads through here.
    """
    return __getattr__(name)


def __dir__() -> list:
    return sorted(list(globals()) + list(_PATHS) + list(_UNAVAILABLE) + ["REFERENCE_NULL_DIR"])

# =====================================================================
# Solver orientation
# =====================================================================
# The canonical solver ORIENTATION. Undirected until 2026-07-18; flipped to "directed"
# after the fixed diode solver (softplus-smoothed rectification, engine 933d93e -> a
# strictly convex energy with a unique, start-independent minimiser) regenerated the
# directed null on the honest reference graph and it cleared the replacement gates: the
# symmetric-limit parity (directed force-noop reproduces the undirected reference to ~1e-6,
# directed/directed_parity.py), the engine self-tests (worst |d|~1e-15), and observed<->null
# lockstep (identical testable-axis sets per element). The undirected solve + null are
# RETAINED as the frozen symmetric-limit referent the parity gates join against -- they are
# no longer the graph+solver the canonical solve runs on. Flip this one constant to revert;
# every resolver below follows it.
CANONICAL_ORIENTATION = "directed"
ORIENTATIONS = ("undirected", "directed")
_ORIENTATIONS = ORIENTATIONS          # back-compat alias
assert CANONICAL_ORIENTATION in ORIENTATIONS
_DIRECTED = CANONICAL_ORIENTATION == "directed"

# =====================================================================
# The atom-pair universe TIER
# =====================================================================
# Tier 4 = tier 3 (the predicted AAM ensemble) + the strictly ADDITIVE MetaCyc curated
# increment, frozen 2026-07-20. Recorded here so consumers can NAME the universe they
# ran on; it is not a switch. The tier is chosen in exactly one place -- the `src:`
# fields of provenance/data/_declared.yml -- and the library's `dest:` paths are
# deliberately UN-suffixed, so every path constant above is tier-agnostic and a tier
# swap moves no code.
#
# WHY THAT MATTERS HERE: it means nothing in this module can tell you which tier the
# library holds. Neither can assert_canonical_reference() -- both hashes it pins
# (reac_prop.tsv, direction.parquet) are tier-INVARIANT. Use
# transforms/build/_stage/check_universe_pair.py, which discriminates on g_base.
#
# Tier 4 restores one carbon axis that tier 3's graph could not reach, so the testable
# axis set is the FULL AXES_PER_ELEMENT (79) rather than tier 3's strict subset (78).
MNXREF_VERSION = "4.5"   # the frozen MetaNetX release the whole basis is bound to
REFERENCE_TIER = 4
REFERENCE_TIER_FROZEN = "2026-07-20"

# WHERE PRODUCERS WRITE -- deliberately NOT the library.
#
# Every REFERENCE_* path above resolves INTO `.awm/data/ref/`, whose members are
# HARDLINKS into the scadc data tree. They share inodes, so a producer that "writes into
# the library" does not write a new file: it mutates the data-tree file the library was
# built from, in place, with no record. Read paths come from the library; WRITE paths
# come from here, and the library is rebuilt afterwards to adopt the result.
#
# Tier-suffixed for the same reason the inputs are: the tier3 significance tables are the
# only evidence of what the tier change moved, so a tier4 run must not land on top of
# them. An un-suffixed output directory is exactly how that would happen -- the producer
# had one, hardcoded, and it named the tier3 directory.
REFERENCE_OUT_ROOT = DATA / "ecspr_reference" / f"mnxref-{MNXREF_VERSION.replace('.', '_')}"
_TIER_SUFFIX = "" if REFERENCE_TIER == 3 else f"_tier{REFERENCE_TIER}"
REFERENCE_SIGNIFICANCE_DIR = REFERENCE_OUT_ROOT / f"significance{_TIER_SUFFIX}"
REFERENCE_ATOM_PAIRS_SHA256 = (
    "7b3b217f91373f3141404767f9be3ad20a87be10d756ccef6a7ecdb1311abe93"
)

# The per-reaction direction ratios (exp(dG'/RT), MNXR-keyed) the directed solve rectifies
# each edge with. Frozen beside the reference; ratio 1.0 == no evidence == reversible ==
# undirected parity. Baked into the frozen directed reports/null below -- pinned here as the
# provenance of which direction table produced the canonical directed solve, and hash-checked
# in assert_canonical_reference() so a silent regenerate cannot change the canonical answer.
# [resolved through the library] REFERENCE_DIRECTION = REFERENCE_ROOT / "direction.parquet"
REFERENCE_DIRECTION_SHA256 = (
    "c80009055601e64342ba56aaf48547f609f2621c47fa4f495abff8c6fd6a8fc5"
)

# The observed DIRECTED solve, pinned per lane. Every other frozen artifact in this file
# is pinned so a silent REGENERATE cannot change the answer. This one is pinned because
# a regenerate DOES change the answer.
#
# It was believed to have no producer. It has one -- methods/directed/build_directed_observed.py,
# which takes every input from this module. Re-running it on 2026-07-20 against the same
# tier-4 base and graph did NOT reproduce these tables: per-cell `delta` moved by a median
# of 14.4%, with 54% of cells past 10%, on 15,621 cells both times. That is a solver
# convergence floor rather than a bad run -- frozen<->null, rerun<->null and frozen<->rerun
# all sit at the same ~2e-05 median distance on g_base, and the solve emits
# `CholmodWarning: Matrix is nearly singular` with rcond to ~1e-27.
#
# So the canonical result is THIS ARTIFACT, not "what the method computes". Any per-cell
# number is reproducible only by reading these exact bytes, which is what the pin protects.
# Corollary worth carrying: two runs of the same solve agree at AXIS level to rho
# 0.951-0.965, which is indistinguishable from tier3-vs-tier4 agreement -- so an
# axis-level correlation between two solves cannot license a claim about what changed
# between them. See reports/tier3_to_tier4_movement.md.
REFERENCE_SOLVE_DIRECTED_SHA256 = {
    "ieff": "1d1a5c9053faafe2d13b9e3be3171f21cded2303752cf30ee7ff65df5ae99113",
    "reff": "bfd9c2e09822c78842958993ec066568bf520ba18e165987dbfc93da229e7796",
}

# The canonical null: the reference-GRAPH null (reference/build_reference_null.py for the
# undirected referent; directed/build_directed_null.py for the directed canonical), the
# matched pair to reference_axes_report -- same graph AND same orientation, so delta_obs and
# the null it is scored against are never on different solvers. Same draws/ORF weights across
# both orientations (graph-independent, reused verbatim). Curated by the explicit
# FROZEN_NULL_FILES list below -- never a glob.
# [resolved through the library] REFERENCE_NULL_UNDIRECTED_DIR = REFERENCE_ROOT / "null"
# [resolved through the library] REFERENCE_NULL_DIRECTED_DIR = REFERENCE_ROOT / "null_directed"
# [resolved through the library] REFERENCE_NULL_DIR = REFERENCE_NULL_DIRECTED_DIR if _DIRECTED else REFERENCE_NULL_UNDIRECTED_DIR

# =====================================================================
# Scorer
# =====================================================================
SCORER = "sig_mix"
SCORER_DESC = "full-mixture survival function"
# Retired scorers. Named here so tooling can refuse them by name rather than by
# a human remembering which of the sibling tables is current.
RETIRED_SCORERS = ("sig_negbin", "sig_emp")

# =====================================================================
# Fosmid basis
# =====================================================================
FOSMID_BASIS = 199
ORFS_FAA = DATA / "fabfos_2026" / f"orfs_{FOSMID_BASIS}.faa"
# Split contigs. Present in the basis and dropped silently by a `\w`-based ORF
# regex, because `\w` excludes the dot. Their presence in a scored table is a
# positive check that the ORF counter is the rsplit form.
SPLIT_CONTIGS = ("C00310.A", "C00310.B", "C00708.A", "C00708.B")

# The figure basis is a filter applied at figure time, not a second table:
# score once on FOSMID_BASIS and subset. BH-q is not load-bearing (ranking is by
# effect size, which is per-row and basis-independent), so subsetting is safe.
FIGURE_BASIS_OPEN = 132

# =====================================================================
# Null draws
# =====================================================================
# The sizes the SCORER interpolates across. NOT the sizes the incumbent null
# GENERATOR declares -- those two forked inside one directory, which is one of
# the failures this work exists to remove.
DRAW_SIZES = (14, 25, 30, 35, 43)
# The grid is the ORF-count distribution of the 199 canonical inserts sampled at the
# p10/p30/p50/p70/p90 percentiles (orfs_199.faa ORF calls; median 30 ORFs, 0.96 ORF/kb).
# Superseded the earlier (14,28,34,42,51) when the directed model was frozen: those knots
# ran to n_orf~p95 (irregular spacing p10/p39/p62/p88/p95); this grid is evenly spaced in
# ORF-count rank so the scorer interpolates on a uniform lattice over the real insert range.
# What the incumbent null GENERATOR declares, against which the scorer above had
# already moved on. Recorded so the fork is visible as data rather than as a
# discovery someone has to make twice.
RETIRED_DRAW_SIZES = (21, 28, 34, 42, 51, 56)
DRAW_K = 1000
DRAW_SEED = 42
DRAW_N_ORFS = 30
MIN_CONTIG_ORFS = 30

STYLES = ("A", "B", "D", "E")
PRIMARY_STYLE = "D"
STYLE_DESC = {
    "A": "uniform over all metagenome ORFs",
    "B": "uniform over ORFs on contigs above the contig-ORF minimum",
    "D": "contiguous adjacent ORFs on one contig (operon-like)",
    "E": "uniform over evidence-bearing ORFs only",
}

LANES = ("reff", "ieff")
CANONICAL_LANE = "ieff"

# The explicit curated list. Never glob the cache or the backup: both hold
# retired sizes beside the canonical ones. The filename stem follows the canonical
# orientation: the directed null is "{lane}_null_directed_N{n}", the undirected referent
# "{lane}_null_canonical_N{n}" -- so a directed run can never silently score against the
# undirected null (different files, not a flag on the same file).
_NULL_STEM = "directed" if _DIRECTED else "canonical"
FROZEN_NULL_FILES = tuple(
    f"{lane}_null_{_NULL_STEM}_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
# The two orientations' lists by name, so reference_null_files(orientation=...) can
# serve a gate that asks for the NON-canonical one without re-deriving the convention.
FROZEN_NULL_FILES_DIRECTED = tuple(
    f"{lane}_null_directed_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
FROZEN_NULL_FILES_UNDIRECTED = tuple(
    f"{lane}_null_canonical_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
FROZEN_DRAWS_FILES = tuple(
    f"null_canonical_N{n}_draws.parquet" for n in DRAW_SIZES
)

# =====================================================================
# Fitter / scorer knobs
# =====================================================================
ELEMENTS = ("C", "N", "S", "P")
KMAX = 4          # pinned; an implicit cap makes the incumbent comparison dishonest
FIT_SEED = 0
EPS = 1e-12
Q_THRESHOLD = 0.05

# =====================================================================
# Axes
# =====================================================================
AXIS_SET = "set4"
RETIRED_AXIS_SETS = ("set2", "set2cat")
# Lives in the PUBLISH tree while being read as a pipeline INPUT. That inversion
# is logged in MIGRATION.md; the path is recorded here so nothing has to guess it.
# PATH CORRECTED 2026-07-20. This named publish/03_model/ecspr_scadc/, which does not
# exist -- the table lives under publish/05_scadc/, and that is the only copy on disk.
# The dangling value survived because no consumer used the symbol: the one loader that
# reads this table (main/ecspr/method/_inputs.py) transcribed the correct path itself
# instead of importing the name, so canon's copy was never exercised. That is the exact
# failure mode this module exists to prevent, inverted -- prose was right, canon was
# wrong, and the duplicate hid it.
# The axis DEFINITIONS (source/sink species per axis). Graph-INDEPENDENT: it is set4
# itself, so it is exactly AXES_N regardless of which graph the solve runs on. Assert
# canonical axes against THIS.
# [resolved through the library] AXES_JSON = INCUMBENT_CACHE / f"biomass_dag_axes_{AXIS_SET}.json"
# The TESTABLE subset: which of the AXES_JSON axes have both endpoints in the solve
# graph's LCC. Graph-DEPENDENT, so it may move with the canonical graph, and it is
# NOT asserted equal to AXES_PER_ELEMENT. It lives beside the reference solve it
# describes.
#
# CORRECTED 2026-07-20. This comment used to state that on the honest reference
# graph one carbon axis (a phospholipid endpoint the star reached only via a
# fabricated transit) is no longer testable, i.e. 39/40. That attribution was
# wrong and the same claim is repeated in MIGRATION.md. The axis was lost to the
# STALE EVIDENCE_WEIGHTS, not to the graph: rebuilding the host base from weights
# coherent with EVIDENCE_TABLE restores it on BOTH the old and tier-4 universes
# (C 40/40, total 79/79). Tier 4 does not move the testable set at all.
# [resolved through the library] AXES_TESTABLE_JSON = REFERENCE_SOLVE_DIR / "axes_testable.json"
AXES_N = 79
AXES_PER_ELEMENT = {"C": 40, "N": 23, "S": 7, "P": 9}

# =====================================================================
# Evidence
# =====================================================================
CLEAN_FLOOR = 0.01   # F1-optimal; CLEAN does not abstain, so an unguarded lane over-nominates

# The compiled per-(source, orf, mnxr) evidence behind the canonical basis.
# Two byte-identical copies exist -- one in the code tree, one here in the data
# tree. Prefer this one: data belongs in the data tree, and a pipeline input read
# out of a code checkout is the same inversion as the axis table below.
# [resolved through the library] EVIDENCE_TABLE = (DATA / "fabfos_2026_199" / "ecspr_clean" / "evidence_network"
#                   / "evidence_table_clean.parquet")

# The nucleotide basis. Note the filename does NOT record the count -- the sibling
# that does is the retired subset. Assert the count; do not read the name.
INSERTS_FNA = DATA / "fabfos_2026" / "putative_inserts.fna"

# The reactions each fosmid injects onto the host base. Derived from the evidence
# table; staged rather than recomputed, so that a Network A run and a Network B run
# differ ONLY in the base and the comparison between them is about the host.
# [resolved through the library] ADDITION_WEIGHTS = (DATA / "fabfos_2026_199" / "ecspr_clean" / "evidence_network"
#                     / "fosmid_addition_weights.pkl")

# The per-(source, mnxr) conductances the HOST base is induced with -- the other
# half of the pair above. It had no name here until 2026-07-20, and that absence
# was not cosmetic: every other input moved to the 199-fosmid CLEAN basis while
# the weights kept being hand-passed from the incumbent 04_reaction_network cache,
# because there was no symbol to repoint. The frozen reference solve was therefore
# built from a MISMATCHED pair -- CLEAN evidence naming the reactions, pre-CLEAN
# weights setting their conductance -- which silently zeroed 4,688 CLEAN-nominated
# reactions and admitted 195 weight keys the evidence does not contain. The
# visible symptom was a carbon axis that appeared to lose its LCC endpoint "on the
# honest graph" (MIGRATION.md); it was the stale weights, not the graph.
# Regenerate with `ecspr_network.py weights --evidence <EVIDENCE_TABLE>`; it is a
# pure function of the evidence table and carries its own conservation assertion.
# [resolved through the library] EVIDENCE_WEIGHTS = (DATA / "fabfos_2026_199" / "ecspr_clean" / "evidence_network"
#                     / "evidence_weights.parquet")

# The per-(source, mnxr) conductances the HOST base is induced with -- the other half of
# the pair above. It had no name here until 2026-07-20, and that absence was not
# cosmetic: every other input moved to the 199-fosmid CLEAN basis while the weights kept
# being hand-passed from the incumbent 04_reaction_network cache, because there was no
# symbol to repoint. The frozen reference solve was therefore built from a MISMATCHED
# pair -- CLEAN evidence naming the reactions, pre-CLEAN weights setting their
# conductance -- which silently zeroed 4,688 CLEAN-nominated reactions and admitted 195
# weight keys the evidence does not contain.
#
# The visible symptom was a carbon axis that appeared to lose its LCC endpoint "on the
# honest graph", and it was misattributed to the graph twice -- first to the reference
# promotion, then to the tier-4 promotion. It was neither. Rebuilding the base from
# weights coherent with EVIDENCE_TABLE restores the axis on BOTH universes (C 40/40).
# The atom-pair tier cannot do this: it revises edge WEIGHTS and leaves topology
# identical, and reachability is topology.
#
# Regenerate with `ecspr_network.py weights --evidence <EVIDENCE_TABLE>`; it is a pure
# function of the evidence table and carries its own conservation assertion.
EVIDENCE_WEIGHTS = (DATA / "fabfos_2026_199" / "ecspr_clean" / "evidence_network"
                    / "evidence_weights.parquet")

# =====================================================================
# Network A -- the curated-GEM host
# =====================================================================
# The second, independent reconstruction of the host. Network B induces the host
# from the 4-lane annotation of its ORFs; Network A crosswalks a curated GEM's
# reactome to current MNXR and induces it with UNIFORM conductance. They share no
# derivation path, which is the entire point: a finding that holds on both rests on
# neither.
#
# The sibling curated model is K-12 (iML1515) and is a DIFFERENT STRAIN with a
# different genotype. It is the validation lane's referent, not the SCADC host.
# Experiments assert the model id from inside the JSON rather than trusting the
# filename -- the filename is what drifted, everywhere else in this file.
# [resolved through the library] NETA_GEM = DATA / "metabolic_modelling" / "networks" / "iECDH10B_1368.json"
NETA_GEM_STRAIN = "DH10B"
# [resolved through the library] REAC_XREF = DATA / "references" / "metanetx" / "reac_xref.tsv"

# =====================================================================
# The frozen AAM + direction reference
# =====================================================================
# ECSPr's atom-atom mapping and directionality are NOT dynamic: every reaction in
# any network is a MetaNetX id, so AAM(mnxr) and direction(mnxr) are static
# functions of that id. They are pre-baked ONCE over the whole MetaNetX universe as
# a frozen, version-pinned, MNXR-keyed asset (main/fabfos/reference/reference.py),
# driven to 100% adjudication by the closure ledger. The method module owns the
# schema, verdict vocabulary, closure, and the reac_prop content pin; it is the sole
# authority on those, exactly as this file is on the basis.
#
# What canon adds is the EXPERIMENT->UNIVERSE binding. reference.assert_reference
# proves the manifest is SELF-consistent (its recorded reac_prop hash equals the
# current file's), but a fresh rebuild against a DIFFERENT MetaNetX release is
# self-consistent too. This basis was validated against exactly one frozen universe;
# canon records that universe's reac_prop hash so a swap underneath fails at the
# experiment boundary, not silently. The value lives in the reference MANIFEST.json
# (written by build_ledger.py); it is data, not prose, and is pinned here so a
# consumer proves the binding with assert_canonical_reference().
REFERENCE_REAC_PROP_SHA256 = (
    "8582cc187d03ce127f8e914f8f298282ac9036f1448918117af044ac55b980db"
)

# =====================================================================
# Reference graphs
# =====================================================================
# Derived from ELEMENTS, deliberately. The dir also holds a bipartite map for an
# element outside the basis, so a glob of `mnx_bipartite_*.pkl` silently widens the
# run. Same failure as globbing the null cache, different directory.
BIPARTITE_FILES = tuple(f"mnx_bipartite_{e}.pkl" for e in ELEMENTS)
# The per-element universe bipartite the fosmid-addition map projects onto. Now the
# honest reference graph (see REFERENCE_GRAPH_DIR); the incumbent cache is a frozen
# referent, no longer the graph the solve runs on.
# [resolved through the library] BIPARTITE_DIR = REFERENCE_GRAPH_DIR

# The per-element host base graphs the solve runs on. `ADDITION_WEIGHTS` above is
# the other half; there is deliberately no second name for it here, because a
# duplicate name for one value is the disease this file exists to cure. These are the
# reference-fed bases beside the frozen reference solve.
# [resolved through the library] SOLVE_BASE_DIR = REFERENCE_SOLVE_DIR
BASE_GRAPH_FILES = tuple(f"base_{e}.pkl" for e in ELEMENTS)

# =====================================================================
# Compute
# =====================================================================
# Staged as a content-hashed input so it enters the task hash. See MIGRATION.md
# on why context.params cannot carry this.
COMPUTE_CPU = "device: cpu\ndtype: float64\n"
COMPUTE_GPU = "device: cuda\ndtype: float64\n"

# =====================================================================
# Significance table schema
# =====================================================================
# The full-mixture SF interpolates across flanking anchors, so there is no single
# matched size. `matched_N` belonged to the retired nearest-size scorer and its
# presence in a table is positive evidence that the table is stale.
SIG_COLUMNS = (
    "fosmid", "element", "axis_id", "n_orfs", "N_lo", "N_hi", "w", "null",
    "delta_obs", "p", "p_emp", "k_lo", "k_hi", "n_bg", "p_floor", "at_floor",
    "q", "survives",
)
SIG_STALE_COLUMNS = ("matched_N",)

# =====================================================================
# Tolerances
# =====================================================================
# Committed BEFORE the runs they gate. A threshold chosen after seeing the number
# is a rationalization, not a gate.
PARITY_TOL = 1e-9        # library scorer vs the incumbent table, CPU vs CPU
GPU_CPU_TOL = 1e-6       # GPU float64 vs CPU float64; agrees closely, not bit-exactly

# =====================================================================
# Direction annotator
# =====================================================================
# Per-reaction conductance directionality: every base-graph reaction gets a ratio
# g_reverse/g_forward = exp(dG'/RT), fused from three members spanning method
# families -- eQuilibrator (measured + group contribution), dGbyG (learned GNN),
# and BioCyc REACTION-DIRECTION (curated physiology, orientation-aligned to MNXR).
# No evidence -> dG'=0 -> ratio 1.0 -> reversible -> parity with the undirected
# model. The ratio is the median transform exp(mu_eff/RT); the sign is expressed
# in MNXR equation orientation. All knobs live here, committed before the run.
import math as _math

DIR_R = 8.314e-3                        # kJ/mol/K
DIR_T = 298.15                          # K
DIR_RT = DIR_R * DIR_T                  # the ratio's natural scale, ~2.48 kJ/mol
DIR_DECADE = DIR_RT * _math.log(10.0)   # one decade of conductance, ~5.71 kJ/mol

# Floors, committed BEFORE the run. TAU_SHARED is the TECRDB common-mode error the
# correlated eQ/dGbyG pair's spread cannot see; it floors a PREDICTED thermo vote
# (group-contribution arm / dGbyG) and the fused pair, so two correlated predictors
# never vote as two independent. TAU_CUR_FLOOR: a curated category alone resolves no
# better than one decade. Both are physical, not tuned.
DIR_TAU_SHARED = DIR_DECADE
DIR_TAU_CUR_FLOOR = DIR_DECADE
DIR_S_MEAS_FLOOR = 0.1                  # kJ/mol; numerical only -- a real measurement
                                        # is trusted at its own sigma
DIR_SIGMA_CEILING = 100.0               # kJ/mol; a wider eQ uncertainty is no
                                        # information -> the reaction is eQ-silent

# The reversible-default prior width = robust marginal spread of measured dG' on the
# eQuilibrator reactant-contribution arm (1.4826*MAD). ESTIMATOR committed here; the
# VALUE is frozen from the calibration run that produced it (464 measured reactions,
# marginal median 0.000 -> no orientation offset). It must fall in the plausibility
# band or it is a finding, not a constant. The robust spread runs BELOW the
# outlier-inflated std, i.e. toward more shrinkage / more reversible -- the safe side.
DIR_SIGMA_0 = 9.505                     # kJ/mol
DIR_SIGMA_0_BAND = (5.0, 40.0)          # outside => stop, it is a finding

# The ratio must stay a FINITE two-way conductance ratio -- never a hard one-way
# gate (the standing ruling). A handful of macromolecular/polymer reactions carry a
# genuine |dG'| of thousands of kJ/mol, whose exp() underflows to 0.0 (an infinite
# gate). |dG'| is clamped to this bound: beyond ~the steepest realistic single-
# reaction drive in metabolism, the flux-force is saturated, and clamping keeps the
# ratio finite and > 0 (~3e-18 .. 3e17). Physical bound, committed independent of the
# data; clamped rows are flagged, not hidden.
DIR_DG_CLAMP = 100.0                     # kJ/mol

# The five curated REACTION-DIRECTION values, in MNXR orientation. A sixth token
# would be a KeyError at the aligner, not a silent default (which is how the ~7%
# right-to-left corpus would otherwise invert).
DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

# Emitted schema. dir_tier > 0 means "carries directional information", NOT "usable":
# every row is usable and ratio==1.0 (reversible) is a real physical statement, so a
# consumer must read ALL rows. A `df[df.dir_tier>0]` filter would silently drop every
# reversible reaction -- turning "default reversible" into "default absent".
DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")

# Inputs and artifacts. The curated member reads the BioCyc pgdbs read-only; the
# MetaNetX crosswalks are shared with the annotation lanes. REAC_XREF is above.
# [resolved through the library] DIR_METACYC_PGDB = Path(
#     "/home/tony/agentic_workspace/projects/self-improvement/main/staging/metacyc26.pgdb")
# [resolved through the library] DIR_ECOCYC_PGDB = Path(
#     "/home/tony/agentic_workspace/projects/self-improvement/main/staging/ecocyc26.pgdb")
# [resolved through the library] DIR_REAC_PROP = DATA / "references" / "metanetx" / "reac_prop.tsv"
# [resolved through the library] DIR_CHEM_PROP = DATA / "references" / "metanetx" / "chem_prop.tsv"
# [resolved through the library] DIR_CHEM_XREF = DATA / "references" / "metanetx" / "chem_xref.tsv"
# [resolved through the library] DIR_DATA = DATA / "direction"                    # produced artifacts (shared data tree)
# [resolved through the library] DIR_TABLE = DIR_DATA / "direction_annotation.parquet"   # the per-reaction annotator
# [resolved through the library] DIR_CALIBRATION = DIR_DATA / "calibration.parquet"
# [resolved through the library] DIR_CURATED = DIR_DATA / "curated_per_mnxr.parquet"

# =====================================================================
# RETIRED: the star-lane solve reports (2026-07-20)
# =====================================================================
# `reference_axes_report()`, `reference_axes_report_directed()`,
# `undirected_axes_report()`, `reference_null_files()`, `production_status()`,
# `incumbent_axes_report()` and `incumbent_sig_table()` addressed the two-terminal
# per-axis reff/ieff tables and their nulls. ECSPr no longer produces them: the star
# topology joined every metabolite to reaction-node HUBS, and eliminating a reaction
# node -- which is what a Woodbury update does -- left a CLIQUE over its participants,
# giving two participants sharing no atom a conductance between them (measured on
# MNXR106432, carbon: a zero-carbon channel 21x a real one). The measurement moved to
# the atom-resolved graph; see `ground_probe_report()` / `ground_null_files()` below.
#
# The CONSTANTS that address those frozen bytes are deliberately KEPT --
# CANONICAL_ORIENTATION, LANES, CANONICAL_LANE, FROZEN_NULL_FILES, DRAW_SIZES,
# REFERENCE_SOLVE_DIR, REFERENCE_NULL_*_DIR -- because the frozen tables are still
# declared in the data library and the tier3->tier4 report and the benchmark history
# still read them. What is gone is every FUNCTION that fed a live chain off them. Do
# not delete the frozen artifacts; they are provenance, not dead weight.
#
# The ground probe's artifacts. `ground_null_files()` is an EXPLICIT curated list for
# the same reason FROZEN_NULL_FILES was: the scorer derives its draw sizes by listing
# the staged directory, so a glob over a cache would silently widen the null basis.
GROUND_NULL_STEM = "ground_null_N{n}.tsv"


def ground_probe_dir() -> Path:
    """Where the ground probe's artifacts live, under the reference root.

    A FUNCTION, not a module-level constant: REFERENCE_ROOT resolves lazily through the
    library manifest via module __getattr__, and a module-level assignment here would
    shadow that and silently restore a hardcoded path. Three earlier accessors made
    exactly that mistake and were dead on every call -- see `_lib`.
    """
    return _lib("REFERENCE_ROOT") / "ground_probe"


def ground_null_dir() -> Path:
    return _lib("REFERENCE_ROOT") / "ground_null"


def ground_significance_dir() -> Path:
    return _lib("REFERENCE_ROOT") / "ground_significance"


def ground_probe_report(basis: str = "epi300") -> Path:
    """The base media->ground probe table for one evidence basis."""
    return ground_probe_dir() / f"ground_probe_{basis}.tsv"


def ground_effect_report(basis: str = "epi300") -> Path:
    """The observed per-unit effect table (delta_total + delta_clr per precursor)."""
    return ground_probe_dir() / f"ground_effects_{basis}.tsv"


def ground_null_files() -> tuple:
    """The curated null file list -- never a glob. One per staged draw size."""
    return tuple(GROUND_NULL_STEM.format(n=n) for n in DRAW_SIZES)


def ground_null_paths() -> list:
    return [ground_null_dir() / n for n in ground_null_files()]


def _require_lane(lane: str) -> None:
    if lane not in LANES:
        raise ValueError(f"unknown lane {lane!r}; expected one of {LANES}")


# =====================================================================
# Assertion helpers
# =====================================================================
# The two checks a consumer runs before trusting a table. They exist so that a
# stale input fails loudly at the point of use, instead of silently producing a
# figure that asserts the opposite of its own annotations -- which has already
# shipped once.

class CanonError(AssertionError):
    """A table is not the canonical one. Always loud, never a warning."""


def _read(table):
    import pandas as pd
    if hasattr(table, "columns"):
        return table
    p = Path(table)
    if not p.exists():
        raise CanonError(f"table does not exist: {p}")
    return pd.read_csv(p, sep="\t")


def assert_canonical_significance(table, *, basis: int = FOSMID_BASIS,
                                  lane: str | None = None):
    """Refuse a significance table that is not the canonical scorer's output.

    Pass a path or a DataFrame. Returns the DataFrame so it can wrap a read:

        sig = canon.assert_canonical_significance(path)

    Checks, in the order that gives the most useful error first:
      * the filename does not name a retired scorer;
      * the schema is the mixture scorer's, not the retired nearest-size one;
      * the split contigs survived the ORF counter;
      * the fosmid count is the expected basis.
    """
    if isinstance(table, (str, Path)):
        name = Path(table).name
        for retired in RETIRED_SCORERS:
            if name.startswith(retired):
                raise CanonError(
                    f"{name} is the retired {retired} scorer's table. "
                    f"The canonical scorer is {SCORER} ({SCORER_DESC}); "
                    f"see canon.incumbent_sig_table()."
                )
    df = _read(table)

    stale = [c for c in SIG_STALE_COLUMNS if c in df.columns]
    if stale:
        raise CanonError(
            f"table carries retired column(s) {stale} -- that schema belongs to "
            f"the nearest-size scorer, which the {SCORER_DESC} replaced. "
            f"Regenerate against the canonical scorer."
        )
    missing = [c for c in SIG_COLUMNS if c not in df.columns]
    if missing:
        raise CanonError(
            f"table is missing canonical column(s) {missing}. "
            f"Expected canon.SIG_COLUMNS."
        )

    if lane is not None:
        _require_lane(lane)

    fosmids = set(df["fosmid"].astype(str))
    missing_splits = [c for c in SPLIT_CONTIGS if c not in fosmids]
    if missing_splits:
        raise CanonError(
            f"split contigs absent from the table: {missing_splits}. These are "
            f"dropped silently by a `\\w`-based ORF-id regex (the dot is not a "
            f"word character); their absence means the ORF counter is the wrong "
            f"one, not that the data lacks them."
        )

    if len(fosmids) != basis:
        raise CanonError(
            f"table covers {len(fosmids)} fosmids; expected {basis}. "
            f"A different count means a different basis -- subset at figure time "
            f"rather than scoring a second table."
        )
    return df


def assert_canonical_axes(table):
    """Refuse an axis table that is not the canonical axis set.

    Accepts the axes TSV, the axes JSON, or the testable JSON (path), or a
    DataFrame of the TSV. Asserts the count and the per-element split rather
    than trusting the filename, because the filename is what drifted.
    """
    if isinstance(table, (str, Path)):
        p = Path(table)
        for retired in RETIRED_AXIS_SETS:
            if f"_{retired}." in p.name or p.name.endswith(f"_{retired}.json"):
                raise CanonError(
                    f"{p.name} is the retired {retired} axis set; the canonical "
                    f"set is canon.AXIS_SET."
                )
        if p.suffix == ".json":
            import json
            data = json.loads(p.read_text())
            # testable json: {element: [axis_id, ...]}; axes json: {axis_id: {...}}
            if data and all(isinstance(v, list) for v in data.values()):
                per_el = {k: len(v) for k, v in data.items()}
                if per_el != AXES_PER_ELEMENT:
                    raise CanonError(
                        f"axis per-element split {per_el} != canon.AXES_PER_ELEMENT "
                        f"{AXES_PER_ELEMENT}"
                    )
                total = sum(per_el.values())
            else:
                total = len(data)
            if total != AXES_N:
                raise CanonError(f"{total} axes; expected canon.AXES_N ({AXES_N})")
            return data

    df = _read(table)
    if len(df) != AXES_N:
        raise CanonError(
            f"axis table has {len(df)} rows; expected canon.AXES_N ({AXES_N}). "
            f"Assert the count, do not trust the filename."
        )
    if "element" in df.columns:
        per_el = df["element"].value_counts().to_dict()
        if per_el != AXES_PER_ELEMENT:
            raise CanonError(
                f"axis per-element split {per_el} != canon.AXES_PER_ELEMENT "
                f"{AXES_PER_ELEMENT}"
            )
    return df


def assert_canonical_direction_table(table, *, n_reactions: int | None = None):
    """Refuse a direction table that cannot be trusted as a per-reaction annotator.

    Pass a path (parquet or tsv) or a DataFrame; returns the DataFrame. Unlike the
    significance idiom, `dir_tier > 0` is NOT a usability filter here -- every row
    is usable and ratio==1.0 (reversible) is a real value, so the checks are about
    completeness and the ratio being a well-formed conductance ratio:
      * the schema is canon.DIR_COLUMNS;
      * exactly one row per reaction (no missing, no duplicate MNXR);
      * ratio is never null -- a no-evidence reaction is ratio 1.0, never absent;
      * ratio is strictly positive (it is exp(dG'/RT));
      * if n_reactions is given, the row count matches (the base-graph basis).
    """
    if hasattr(table, "columns"):
        df = table
    else:
        p = Path(table)
        if not p.exists():
            raise CanonError(f"table does not exist: {p}")
        import pandas as pd
        df = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p, sep="\t")

    missing = [c for c in DIR_COLUMNS if c not in df.columns]
    if missing:
        raise CanonError(
            f"direction table missing canonical column(s) {missing}. "
            f"Expected canon.DIR_COLUMNS."
        )
    dups = df["mnxr"][df["mnxr"].duplicated()].unique().tolist()
    if dups:
        raise CanonError(
            f"duplicate MNXR row(s) {dups[:5]}{' ...' if len(dups) > 5 else ''}: the "
            f"annotator is one row per reaction, and a many-to-one curated collapse "
            f"must resolve, not duplicate."
        )
    if df["ratio"].isna().any():
        n = int(df["ratio"].isna().sum())
        raise CanonError(
            f"{n} row(s) carry a null ratio. A reaction with no evidence is ratio "
            f"1.0 (reversible), never null -- a null here means the default-reversible "
            f"limit was skipped, not that direction is missing."
        )
    if not (df["ratio"] > 0).all():
        raise CanonError(
            "ratio must be strictly positive: it is exp(dG'/RT), a conductance ratio, "
            "not a signed quantity."
        )
    if n_reactions is not None and len(df) != n_reactions:
        raise CanonError(
            f"table covers {len(df)} reactions; expected {n_reactions}. The annotator "
            f"is defined on the whole base graph -- a short table means reactions were "
            f"dropped instead of defaulted to reversible."
        )
    return df


ATOM_PAIRS_COLS = ("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx",
                   "pair_w", "method", "source", "confidence")

# =====================================================================
# Verdict vocabulary (finite, class-based)
# =====================================================================
# Every reaction carries exactly one AAM verdict and one direction verdict. A verdict is
# one of three STATES; a refusal additionally carries one reason CLASS. This IS the
# acceptance instrument: "100% adjudicated" means every reaction has a verdict whose
# state is one of these and none is left pending -- which is what n_pending == 0 asserts
# in assert_canonical_reference() above.
#
# Moved here from the figure-scope `reference.py` during the canon consolidation. It is
# vocabulary the closure ledger and its consumers share, so it belongs with the rest of
# the basis rather than in a module that only one tree could import.
V_RESOLVED = "resolved"            # a confident map / a ratio with evidence
V_DILUTED = "diluted-ambiguous"    # known but spread (fanout, symmetry, disagreement)
V_REFUSED = "refused"              # an explicit, reasoned refusal (carries a reason class)
VERDICT_STATES = (V_RESOLVED, V_DILUTED, V_REFUSED)
V_PENDING = "pending"              # the one non-verdict the ledger must drive to zero


def assert_canonical_reference(*, check_hash: bool = True):
    """Refuse an AAM+direction reference that is not the pinned, closed one.

    SELF-CONTAINED as of the canon consolidation. This previously delegated the schema /
    closure / version checks to ``main/fabfos/reference/reference.py``, imported by
    sys.path injection from a sibling directory of the CALLER. That module lived in the
    scadc figure scopes -- the very duplicate this consolidation removes -- so the
    delegation was a dependency running the wrong way: the single-source canon reaching
    back into one of the copies it replaces. It was also already dead here, because
    ``src/fabfos/`` has no ``reference/`` sibling, so every call raised ImportError.

    The checks are inlined against the frozen MANIFEST.json, which is itself a library
    member. What was worth keeping from the old module is its ORDER -- most useful error
    first:
      * the manifest pins this MetaNetX release;
      * the reference is CLOSED (0 un-adjudicated reactions);
      * both tables exist and carry the declared MNXR-keyed schema;
      * the universe hash is the exact one this basis was validated against;
      * the direction table is the exact one the directed null was built with.

    Returns the manifest.
    """
    import json

    man = json.loads(_lib("REFERENCE_MANIFEST").read_text())

    if man.get("mnxref_version") != MNXREF_VERSION:
        raise CanonError(
            f"manifest pins MetaNetX {man.get('mnxref_version')!r} but canon is "
            f"{MNXREF_VERSION!r}. A reference built against one release does not name "
            f"the universe of another."
        )

    n_pending = man.get("n_pending")
    if n_pending is None:
        raise CanonError(
            "manifest does not record n_pending; the closure ledger has not asserted "
            "completeness. Re-run the ledger and refreeze.")
    if n_pending != 0:
        raise CanonError(
            f"closure ledger reports {n_pending} un-adjudicated reaction(s); the "
            f"reference is not closed. Every MNXR must carry a verdict (resolved / "
            f"diluted-ambiguous / refused) before the reference may be trusted.")

    import pandas as pd
    for path, cols, label in ((_lib("REFERENCE_ATOM_PAIRS"), ATOM_PAIRS_COLS, "atom-pairs"),
                              (_lib("REFERENCE_DIRECTION"), DIR_COLUMNS, "direction")):
        if not path.exists():
            raise CanonError(f"{label} table absent at {path}; reference incomplete.")
        head = pd.read_parquet(path).head(0)
        missing = [c for c in cols if c not in head.columns]
        if missing:
            raise CanonError(
                f"{label} table {path.name} missing column(s) {missing}.")
        if "mnxr" not in head.columns:
            raise CanonError(f"{label} table is not MNXR-keyed.")

    # NOTE what this does NOT prove. The manifest records the atom-pair table it was
    # frozen beside as a PATH STRING, and at tier4 that string still reads
    # "atom_pairs.parquet" -- the tier lives in the library declaration, not the
    # filename. So none of the checks here can tell tier3 from tier4; the two hashes
    # pinned below are both tier-invariant by construction. See check_universe_pair.py.
    if man.get("reac_prop_sha256") != REFERENCE_REAC_PROP_SHA256:
        raise CanonError(
            f"the frozen reference pins universe {man.get('reac_prop_sha256')!r}, but "
            f"this basis expects canon.REFERENCE_REAC_PROP_SHA256. The reference is a "
            f"different MetaNetX universe than the one the basis was validated against; "
            f"rebuild the reference or re-pin the basis, do not trust it."
        )
    # Directed orientation folds a SECOND frozen input into the canonical answer: the
    # per-reaction direction ratios. Pin it the same way as the universe -- a silent
    # regenerate of direction.parquet would change every directed edge without touching the
    # graph, so the reac_prop hash alone cannot catch it.
    if _DIRECTED and check_hash:
        import hashlib
        direction = _lib("REFERENCE_DIRECTION")
        if not direction.exists():
            raise CanonError(
                f"CANONICAL_ORIENTATION is 'directed' but the direction table is missing: "
                f"{direction}. The directed solve is scored against a null built "
                f"with these ratios; without it the canonical answer cannot be trusted."
            )
        got = hashlib.sha256(direction.read_bytes()).hexdigest()
        if got != REFERENCE_DIRECTION_SHA256:
            raise CanonError(
                f"direction table {direction} hashes {got!r}, but this basis "
                f"pins canon.REFERENCE_DIRECTION_SHA256. A different direction table means "
                f"different directed edges than the canonical directed null was built on; "
                f"rebuild the directed solve+null or re-pin, do not trust it."
            )

        # THE ACCEPTED SOLVE, FROZEN. solve_directed has no committed producer: it was
        # made by hand (local, env p312, softplus-diode directed solve, ~82 min serial)
        # and nothing in any tree regenerates it. That gap was accepted deliberately --
        # so the countermeasure is immutability rather than reproducibility. These pins
        # are the whole of it. If a re-solve ever happens, diff it against these; if
        # this check fires without one, the canonical numbers moved and nobody said so.
        for lane, pinned in REFERENCE_SOLVE_DIRECTED_SHA256.items():
            p = reference_axes_report_directed(lane)
            got = hashlib.sha256(p.read_bytes()).hexdigest()
            if got != pinned:
                raise CanonError(
                    f"observed directed solve {p} hashes {got!r}, but this basis pins "
                    f"canon.REFERENCE_SOLVE_DIRECTED_SHA256[{lane!r}]. This artifact has "
                    f"NO producer -- it cannot be legitimately regenerated, so a changed "
                    f"hash means it was overwritten or the library points somewhere else. "
                    f"Do not trust it; find out what wrote it."
                )
    return man

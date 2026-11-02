from __future__ import annotations

from pathlib import Path

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
    pass
def library_root() -> Path:
    root = Path(_os.environ.get(_LIB_ENV, _DEFAULT_LIB))
    if not (root / "_metadata").is_dir():
        raise CanonError(
            f"no data library at [{root}]. Set ${_LIB_ENV}, or build it with\n"
            f"    python transforms/build/_stage/build_ref_library.py --stage --place --index"
        )
    return root


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
    str(Path(__file__).resolve().parents[1] / "metasmith_libraries"),
))

METHODS_DIR = Path(__file__).resolve().parents[2] / "methods"

_manifest_cache: dict | None = None


def _manifest() -> dict:
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
        man = raw.get("manifest", raw) if isinstance(raw, dict) else {}
        _manifest_cache = {
            str(k): (v["type"] if isinstance(v, dict) else v)
            for k, v in man.items()
        }
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
    prefix = key.rstrip("/") + "/"
    if any(k.startswith(prefix) for k in man):
        return root / key
    raise CanonError(
        f"canon.{symbol} wants library key [{key}], which is not in the manifest "
        f"at [{root}]. Either the library is stale (rebuild it) or the "
        f"declaration in provenance/data/_declared.yml is wrong."
    )


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
    return __getattr__(name)


def __dir__() -> list:
    return sorted(list(globals()) + list(_PATHS) + list(_UNAVAILABLE) + ["REFERENCE_NULL_DIR"])

CANONICAL_ORIENTATION = "directed"
ORIENTATIONS = ("undirected", "directed")
_ORIENTATIONS = ORIENTATIONS
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
MNXREF_VERSION = "4.5"
REFERENCE_TIER = 4
REFERENCE_TIER_FROZEN = "2026-07-20"

REFERENCE_OUT_ROOT = DATA / "ecspr_reference" / f"mnxref-{MNXREF_VERSION.replace('.', '_')}"
_TIER_SUFFIX = "" if REFERENCE_TIER == 3 else f"_tier{REFERENCE_TIER}"
REFERENCE_SIGNIFICANCE_DIR = REFERENCE_OUT_ROOT / f"significance{_TIER_SUFFIX}"
REFERENCE_ATOM_PAIRS_SHA256 = (
    "7b3b217f91373f3141404767f9be3ad20a87be10d756ccef6a7ecdb1311abe93"
)

REFERENCE_DIRECTION_SHA256 = (
    "c80009055601e64342ba56aaf48547f609f2621c47fa4f495abff8c6fd6a8fc5"
)

REFERENCE_SOLVE_DIRECTED_SHA256 = {
    "ieff": "1d1a5c9053faafe2d13b9e3be3171f21cded2303752cf30ee7ff65df5ae99113",
    "reff": "bfd9c2e09822c78842958993ec066568bf520ba18e165987dbfc93da229e7796",
}


SCORER = "sig_mix"
SCORER_DESC = "full-mixture survival function"
RETIRED_SCORERS = ("sig_negbin", "sig_emp")

FOSMID_BASIS = 199
ORFS_FAA = DATA / "fabfos_2026" / f"orfs_{FOSMID_BASIS}.faa"
SPLIT_CONTIGS = ("C00310.A", "C00310.B", "C00708.A", "C00708.B")

FIGURE_BASIS_OPEN = 132

DRAW_SIZES = (14, 25, 30, 35, 43)
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

_NULL_STEM = "directed" if _DIRECTED else "canonical"
FROZEN_NULL_FILES = tuple(
    f"{lane}_null_{_NULL_STEM}_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
FROZEN_NULL_FILES_DIRECTED = tuple(
    f"{lane}_null_directed_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
FROZEN_NULL_FILES_UNDIRECTED = tuple(
    f"{lane}_null_canonical_N{n}.tsv" for lane in LANES for n in DRAW_SIZES
)
FROZEN_DRAWS_FILES = tuple(
    f"null_canonical_N{n}_draws.parquet" for n in DRAW_SIZES
)

ELEMENTS = ("C", "N", "S", "P")
KMAX = 4
FIT_SEED = 0
EPS = 1e-12
Q_THRESHOLD = 0.05

AXIS_SET = "set4"
RETIRED_AXIS_SETS = ("set2", "set2cat")
AXES_N = 79
AXES_PER_ELEMENT = {"C": 40, "N": 23, "S": 7, "P": 9}

CLEAN_FLOOR = 0.01


INSERTS_FNA = DATA / "fabfos_2026" / "putative_inserts.fna"


EVIDENCE_WEIGHTS = (DATA / "fabfos_2026_199" / "ecspr_clean" / "evidence_network"
                    / "evidence_weights.parquet")

NETA_GEM_STRAIN = "DH10B"

REFERENCE_REAC_PROP_SHA256 = (
    "8582cc187d03ce127f8e914f8f298282ac9036f1448918117af044ac55b980db"
)

BIPARTITE_FILES = tuple(f"mnx_bipartite_{e}.pkl" for e in ELEMENTS)

BASE_GRAPH_FILES = tuple(f"base_{e}.pkl" for e in ELEMENTS)

COMPUTE_CPU = "device: cpu\ndtype: float64\n"
COMPUTE_GPU = "device: cuda\ndtype: float64\n"

SIG_COLUMNS = (
    "fosmid", "element", "axis_id", "n_orfs", "N_lo", "N_hi", "w", "null",
    "delta_obs", "p", "p_emp", "k_lo", "k_hi", "n_bg", "p_floor", "at_floor",
    "q", "survives",
)
SIG_STALE_COLUMNS = ("matched_N",)

PARITY_TOL = 1e-9
GPU_CPU_TOL = 1e-6

import math as _math

DIR_R = 8.314e-3
DIR_T = 298.15
DIR_RT = DIR_R * DIR_T
DIR_DECADE = DIR_RT * _math.log(10.0)

DIR_TAU_SHARED = DIR_DECADE
# A GUARD, AND UNDER r10's PRIOR IT NEVER BINDS. It caps how confident one curated category
# may be by flooring its fitted width at one decade. Swept 0.25 to 3 decades on the held-out
# folds, the accuracy, the decided count and every ratio are identical, because the narrowest
# directional bin fitted on the corrected number is 19.06 kJ/mol -- above the widest floor
# swept. It stays at one decade as a bound on a future fit, not as a fitted value.
DIR_TAU_CUR_FLOOR = DIR_DECADE
DIR_S_MEAS_FLOOR = 0.1                  # kJ/mol; numerical only -- a real measurement
                                        # is trusted at its own sigma
DIR_SIGMA_CEILING = 100.0               # kJ/mol; a wider eQ uncertainty is no
                                        # information -> the reaction is eQ-silent
DIR_SIGMA_FLOOR = 1e-4                  # kJ/mol; a NARROWER one is no information
                                        # either. eQuilibrator returns dG'=0 at this
                                        # floor when a reaction's groups cancel
                                        # identically -- a statement about the equation,
                                        # not a measurement of it. Both the calibration
                                        # arm and the combiner's vote must reject these.

# The reversible-default prior width = robust marginal spread of measured dG' on the
# eQuilibrator reactant-contribution arm (1.4826*MAD), over the rows that clear
# DIR_SIGMA_FLOOR. ESTIMATOR committed here; the VALUE is frozen from the calibration
# run that produced it (532 measured reactions, marginal median -1.54). It must fall in
# the plausibility band or it is a finding, not a constant. The robust spread runs BELOW
# the outlier-inflated std, i.e. toward more shrinkage / more reversible -- the safe side.
#
# The previous value, 9.505, was fitted without the floor: 116 of its 471 anchors were
# group-cancelling zeros. A quarter of the mass sitting at dG'=0 exactly is what pinned
# that fit's median to 0.000 and halved its MAD, so it shrank every row of every bake
# too hard. Fit this over the floored arm or it will drift back.
# r10 RE-FITS IT ON THE PHYSIOLOGICAL QUANTITY, which is the one now being shrunk. Same
# estimator, same arm, same floor -- 565 measured anchors (up from r9's 532, because the
# balance gate moved below the member and stopped discarding restaged equations). The
# marginal median moves toward zero, -1.165 -> -0.683, so the corrected quantity is the
# more symmetric of the two, and the robust spread widens 24.075 -> 28.017 because a
# concentration correction is information the standard-state number did not carry.
#
# It lands inside DIR_SIGMA_0_BAND, so the band stands as committed rather than needing a
# re-derivation of its own.
#
# REFITTING THE CURATED PRIOR DOES NOT MOVE THIS. sigma_0 is the MARGINAL spread of the
# anchors, so it is a property of the points rather than of the fit -- the prior enters
# neither the population nor the estimator. `poc_constants.py` re-derives it.
DIR_SIGMA_0 = 28.017                    # kJ/mol
DIR_SIGMA_0_BAND = (5.0, 40.0)          # outside => stop, it is a finding

# The ratio must stay a FINITE conductance ratio, never a one-way gate: a handful of
# polymer reactions carry a genuine |dG'| in the thousands of kJ/mol, whose exp()
# underflows to 0.0. Clamped rows are flagged, not hidden.
#
# THE BOUND IS STATED IN DECADES OF CONDUCTANCE, not kJ/mol, because that is the unit
# the ratio is consumed in -- one decade per DIR_DECADE. Unbounded pass-through hands
# the graph asymmetries of 1e17, far past where the reverse branch is numerically dead.
# RE-EARNED ON r10's OWN TABLE, because the concentration term and the refitted prior both
# move the distribution, and a cap whose warrant describes a different distribution is not a
# warrant. Sweeping the mean forward share of the two-way conductance, 2r/(1+r), over the
# final posterior: 1.050603 at one decade, 1.060666 at two, 1.061456 at three, 1.061514 at
# four, and 1.061518 at six and at nine. Three decades sits 0.006% from where the level stops
# moving, and unbounded overflows exp() -- which is the other half of why this bound exists.
# `poc_constants.py` re-runs the sweep against a run's own annotation.
DIR_DG_CLAMP = 3.0 * DIR_DECADE          # three decades == 1000:1

# =====================================================================
# the reaction quotient
# =====================================================================
# THE RATIO IS A PHYSIOLOGICAL QUANTITY, so the number shrunk toward reversible is
# dG'o + RT*ln(Q), not dG'o. The correction decomposes into two parts with completely
# different data requirements, and conflating them is why the cheap fix looks sufficient:
#
#   molecularity  RT * dn * ln(1 mM)             needs NO data. This is exactly what
#                                                eQuilibrator's `physiological_dg_prime`
#                                                returns, and over the voted set its
#                                                MEDIAN |value| is 0.00 kJ/mol -- most
#                                                biochemistry conserves solute count.
#   skew          RT * sum(nu_i * ln(c_i/1 mM))  needs a measured concentration per
#                                                participant, and is identically zero
#                                                under a uniform concentration. 30.7% of
#                                                in-graph reactions carry one past a
#                                                decade, which no uniform prior can see.
#
# A PARTICIPANT WITH NO MEASUREMENT IS PRICED AT THE DEFAULT, NEVER SKIPPED. Skipping
# leaves it at the 1 M standard state while its partners move to millimolar, which makes
# the correction one-sided: on MNXR145036 that returns -17.3 kJ/mol where the balanced
# answer is +5.4. A one-sided quotient is worse than none.
DIR_CONC_DEFAULT_mM = 1.0

# Widths, in DECADES of concentration, folded into sigma at the seam sigma_sub uses.
# The floor exists because 41% of the measured metabolites rest on a single growth
# condition, and a single measurement has no spread of its own -- a confident zero width
# on one number is the failure this prevents.
DIR_CONC_SPREAD_FLOOR = 0.30
# sigma of a log-uniform over the 1 uM .. 10 mM window MDF analyses use: 4/sqrt(12).
DIR_CONC_SPREAD_DEFAULT = 4.0 / _math.sqrt(12.0)

# Already inside eQuilibrator's prime potentials. Adding them again double-counts.
DIR_CONC_IMPLICIT = frozenset({"WATER", "MNXM1"})

# A DISSOLVED GAS PARTICIPATES LIKE ANY OTHER SOLUTE. Its activity is set by a partial
# pressure rather than by a pool the cell titrates, so its measurement comes from a
# solubility at a stated pO2 rather than from metabolomics -- but that is a statement about
# PROVENANCE, not a reason to leave it out.
#
# EXCLUDING A GAS IS THE WORST OF THE THREE OPTIONS, which is why this set is a label and
# not a skip list. Dropping a participant from the quotient is arithmetically identical to
# pricing it at the 1 M standard state: for dissolved O2 that is 1000 mM against a measured
# 0.264, an overstatement of 3.58 decades, where even the flat 1 mM default is only 0.58
# out. At 7,028 in-graph incidences O2 is the single largest participant in the universe,
# so the difference is not academic.
#
# Counted separately so a reader can see which of a reaction's concentrations came from a
# gas-phase source.
DIR_CONC_GAS_PHASE = frozenset({"MNXM735438", "MNXM13", "MNXM1098", "MNXM1101872",
                                "MNXM10917", "MNXM732448"})

# A polymer or an unspecified acceptor has no free-solute concentration. This is the same
# assertion `substitute.py` makes about its standard term, and it is why the phosphorylase
# family needs its polymer budget balanced BEFORE a concentration term means anything.
DIR_CONC_UNIT_ACTIVITY = frozenset({"MNXM738130", "MNXM8348", "MNXM727735", "MNXM725902",
                                    "BIOMASS", "MNXM01", "MNXM8975"})

# How wide a name expansion may go before it is refused.
#
# SET BY WHAT IT HAS TO REACH, because the held-out curated benchmark cannot choose it.
# Across caps 1 to unbounded that benchmark moves between 91.95% and 92.03% -- 7 reactions
# out of 9,060 decided -- so it does not discriminate, and reading its 0.08-point preference
# for the narrowest table as a result would be reading noise.
#
# What DOES discriminate is whether the join reaches `MNXM1364212`. KEGG's C00103 resolves
# to the alpha anomer `MNXM1364214`, while the reaction universe writes glycogen
# phosphorylase with the other id; the expansion has to be at least 3 wide to carry the
# measurement across. Below that, MNXR145036's correction is +3.99 kJ/mol against a
# break-even of +4.175 -- the reaction that motivated this whole lane stays backwards, and
# the benchmark cannot see it, because MNXR145036 carries no curated directional category
# and is therefore absent from the validation population entirely.
#
# 3 is the SMALLEST value that reaches it. Going wider buys nothing measurable and starts
# linking through generic entries: unbounded, `kegg.compound:C08353` reaches seven
# accessions including `lyxose` and `aldehydo-L-ribose`, a C2 epimer and an enantiomer that
# formula and charge cannot separate.
DIR_CONC_MAX_EXPANSION = 3

DIR_CONC_COLUMNS = ("mnxr", "member", "molecularity", "skew", "dG_correction",
                    "sigma_conc", "n_conc_measured", "n_conc_defaulted",
                    "n_conc_excluded", "n_conc_gas_phase", "delta_n",
                    "restaged_balanced")

# WHICH STORED SPREAD ESTIMATOR THE CURATED PRIOR USES.
#
# `calibrate.fit` computes a robust centre (`median`) and BOTH widths from the same points:
# `mad_spread` (1.4826*MAD, robust) and `tau` (a plain variance minus the mean member
# variance, not robust). The combiner paired the robust centre with the non-robust width,
# so a handful of outliers set the width of a bin whose centre had already been protected
# from them, and shrinkage then annihilated the vote -- PHYSIOL-RIGHT-TO-LEFT fits to
# +24.76 kJ/mol, a 21,734:1 ratio, and shipped 1.37.
#
# A ROBUST CENTRE AND A NON-ROBUST SCALE ARE NOT A PAIR.
# WHERE THE BALANCE TEST SITS RELATIVE TO THE MEMBER.
#
# `is_balanced` comes off raw `reac_prop`, but the member does not score the raw equation:
# the substitution lane restages polymer and carrier chemistry first. 546 curated reactions
# that reac_prop calls unbalanced come back from the member balanced and answered, and
# refusing them on the raw verdict discards a real measurement AND changes the population
# the category prior is fitted on -- the directional bins roughly double.
#
# `before_member` is r9's behaviour, kept so the re-bake can price this change as its own
# arm rather than confound it with the estimator change.
DIR_BALANCE_GATES = ("after_member", "before_member")
DIR_BALANCE_GATE = "after_member"

DIR_PRIOR_WIDTH_KINDS = ("robust", "tau")
DIR_PRIOR_WIDTH_KIND = "robust"

# WHICH QUANTITY THE CURATED PRIOR IS FITTED AGAINST.
#
# The calibration fits a category's centre from the eQuilibrator member's dG, and the
# combiner averages that centre with the thermo vote. Once the vote carries a reaction
# quotient the vote is a PHYSIOLOGICAL dG' and the centre is still a STANDARD-STATE one, so
# the two numbers being averaged are numbers about different things.
#
# WHAT THE COMBINER NEEDS IS THE DISTRIBUTION OF dG' WITHIN A BIN, so every category is
# fitted on the corrected column. The label's semantics explain why the bins differ from
# each other; they do not choose the column. Measured on the calibration population, every
# bin's centre moves the way its own label asserts once the correction lands --
# PHYSIOL-RIGHT-TO-LEFT 25.04 -> 36.09, PHYSIOL-LEFT-TO-RIGHT -20.05 -> -24.68,
# RIGHT-TO-LEFT 14.01 -> 21.14, LEFT-TO-RIGHT -27.11 -> -30.21, and REVERSIBLE toward zero,
# -1.45 -> -0.90. The correction never saw the labels, so that is corroboration rather than
# a fit.
#
# A PER-REACTION TRANSPORT OF THE IRREVERSIBLE BINS WAS TRIED AND REJECTED. Splitting the
# taxonomy by the `PHYSIOL-` prefix -- fitting the irreversible bins on dG'o and adding each
# reaction's own correction to that centre -- moves 16 held-out reactions and every one of
# them off the curated side, all in the two transported bins. An irreversibility claim is
# about the reaction, and pricing it per reaction injects concentration noise into a
# statement that was never about concentrations. `poc_prior_holdout.py` still scores that
# arm, so the rejection stays reproducible.
#
# `standard` is r9's behaviour -- fitted on dG'o and used as if it were dG'. It is kept so
# the re-bake prices this change as its own arm, and it writes `unstated` into the
# calibration table so the annotation names the scale it was built on.
DIR_PRIOR_QUANTITIES = ("physiological", "standard")
DIR_PRIOR_QUANTITY = "physiological"

# THE MAGNITUDE CAP'S SECOND JOB, MADE EXPLICIT.
#
# `DIR_DG_CLAMP` is a numerical bound and its warrant is a saturation sweep. It was also
# silently absorbing a population whose posterior is not thermodynamics -- the unbalanced
# fraction rises monotonically with |dG'|, 2.7% under one decade to 66.4% past a hundred.
# A direction label cannot find that bound, because clamping is monotone and
# sign-preserving, so decided accuracy is identical at every value.
#
# A suspect row is WIDENED by this much rather than dropped or squashed. Widening reuses
# the seam `sigma_sub` and `sigma_conc` already use, needs no new branch, preserves
# coverage, and keeps reversible-by-default a limit of one rule instead of an if-branch.
# Three decades: enough that a broken equation cannot carry a confident call, not so much
# that it is silenced.
DIR_SUSPECT_SIGMA = 3.0 * DIR_DECADE

# When two members disagree by more than this, the equation is the suspect rather than
# either estimate. MEASURED: across bands of |eq - dgbyg| the unbalanced fraction runs
# 2.5%, 8.7%, 11.1%, 18.4%, 21.9% -- monotone, and further-reaching than the magnitude
# test. This is the band where it passes 10%.
DIR_MEMBER_GAP = 15.0

DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")


GROUND_NULL_STEM = "ground_null_N{n}.tsv"


def ground_probe_dir() -> Path:
    return _lib("REFERENCE_ROOT") / "ground_probe"


def ground_null_dir() -> Path:
    return _lib("REFERENCE_ROOT") / "ground_null"


def ground_significance_dir() -> Path:
    return _lib("REFERENCE_ROOT") / "ground_significance"


def ground_probe_report(basis: str = "epi300") -> Path:
    return ground_probe_dir() / f"ground_probe_{basis}.tsv"


def ground_effect_report(basis: str = "epi300") -> Path:
    return ground_probe_dir() / f"ground_effects_{basis}.tsv"


def ground_null_files() -> tuple:
    return tuple(GROUND_NULL_STEM.format(n=n) for n in DRAW_SIZES)


def ground_null_paths() -> list:
    return [ground_null_dir() / n for n in ground_null_files()]


def _require_lane(lane: str) -> None:
    if lane not in LANES:
        raise ValueError(f"unknown lane {lane!r}; expected one of {LANES}")


class CanonError(AssertionError):
    pass
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

V_RESOLVED = "resolved"
V_DILUTED = "diluted-ambiguous"
V_REFUSED = "refused"
VERDICT_STATES = (V_RESOLVED, V_DILUTED, V_REFUSED)
V_PENDING = "pending"


def assert_canonical_reference(*, check_hash: bool = True):
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

    if man.get("reac_prop_sha256") != REFERENCE_REAC_PROP_SHA256:
        raise CanonError(
            f"the frozen reference pins universe {man.get('reac_prop_sha256')!r}, but "
            f"this basis expects canon.REFERENCE_REAC_PROP_SHA256. The reference is a "
            f"different MetaNetX universe than the one the basis was validated against; "
            f"rebuild the reference or re-pin the basis, do not trust it."
        )
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

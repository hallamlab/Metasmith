"""ECSPr Layer-2 evidence: MNXR bridges + 4-lane readers + evidence-table compiler.

Faithful port of the scadc `03_layer2_evidence/` pipeline into a single
path-parameterized CLI so a metasmith transform can call it (no hardcoded scadc
paths). Ported verbatim (method-preserving) from:
  - _lanes.py                     (bridge loaders + the 4 lane readers, 8-col schema)
  - 00_build_uniprot_to_mnxr.py   (UniProt->MNXR Rhea DR bridge)
  - 11_build_evidence_table_dlec.py (the dl_ec 4-channel compiler)

Every lane reader emits the unified 11-column schema declared below as SCHEMA_COLS.
The declarations there -- the channel vocabulary, the raw_score direction/range
contract, and `validate_gpr` -- are the GPR table's definition, and both mappers in
`transforms/fabfos/` read them from here rather than restating them.

The compiler folds the four fresh-annotation lanes into one evidence table (the
canonical `dl_ec` variant); ECSPr consumes it as functional_annotation::evidence_table.

SUBCOMMANDS
  build-uniprot-bridge  reac_xref + rhea2uniprot{,_trembl}  -> uniprot_to_mnxr.parquet
  build-ec-bridge       reac_prop.tsv                       -> ec_to_mnxr.tsv
  build-mnxr-lookup     the ko/ec/uniprot trio              -> mnxr_lookup.parquet
  compile               per-lane annotator outputs + bridges -> evidence_table.parquet

ko_to_mnxr is a reused reference table (staged input), not built here.

`build-mnxr-lookup` folds the trio into the single `ref::mnxr_lookup` the lanes
now read; the per-bridge loaders above stay for reading the trio directly.

GATES: the lane INPUTS are produced fresh upstream by the heavy annotator
transforms (kofamscan, EZpred/ESM-C dl_ec, DIAMOND-vs-UniRef50, embed-transfer).
This module only needs pandas + a parquet engine (see below) and the MetaNetX/Rhea
reference tables (present on disk); it recomputes NOTHING that must be reused (the
metaG null lives elsewhere).
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

# =====================================================================
# parquet I/O
# =====================================================================
# Two engines, because this module is imported in two places whose dependency
# sets do not overlap: the `python_for_data_science` image, which carries polars
# and no pyarrow, and the local analysis env, which carries pyarrow and no
# polars. pandas is the common language but cannot be the common engine --
# `pd.read_parquet` is itself a pyarrow/fastparquet front end.
#
# Every parquet touchpoint in this file and in both GPR mappers goes through the
# two functions below, so an image whose engine changes costs one branch here
# rather than a scatter of ModuleNotFoundErrors nine steps into a run.
try:
    import pyarrow  # noqa: F401
    _PARQUET_ENGINE = "pyarrow"
except ModuleNotFoundError:
    _PARQUET_ENGINE = "polars"

# Filters are (column, op, value) triples in pyarrow's own vocabulary, so that
# branch passes them straight through. Both engines push them into the reader --
# that pushdown is what keeps the bridge's 30.4M-row uniprot slice off the heap
# when a lane wants only its own ids.
_FILTER_OPS = {
    "==": lambda col, v: col == v,
    ">=": lambda col, v: col >= v,
    "in": lambda col, v: col.is_in(list(v)),
}


def read_parquet(path, columns=None, filters=None) -> pd.DataFrame:
    if _PARQUET_ENGINE == "pyarrow":
        return pd.read_parquet(path, columns=columns, filters=filters)
    import polars as pl
    lf = pl.scan_parquet(path)
    for col, op, value in filters or ():
        if op not in _FILTER_OPS:
            raise ValueError(f"unsupported parquet filter {op!r}")
        lf = lf.filter(_FILTER_OPS[op](pl.col(col), value))
    if columns is not None:
        lf = lf.select(list(columns))
    return _from_polars(lf.collect())


def write_parquet(df: pd.DataFrame, path, compression=None) -> None:
    if _PARQUET_ENGINE == "pyarrow":
        extra = {} if compression is None else {"compression": compression}
        df.to_parquet(path, index=False, **extra)
        return
    _to_polars(df).write_parquet(path, compression=compression or "zstd")


# The two conversions below go column-wise through numpy on purpose: both of
# polars' pandas bridges (`from_pandas`, `to_pandas`) are implemented over arrow,
# and so need the one dependency this branch exists to do without.

def _to_polars(df: pd.DataFrame):
    import polars as pl
    cols = {}
    for name in df.columns:
        s = df[name]
        arr = s.to_numpy()
        if arr.dtype.kind in "OUS":
            # pandas spells a missing string NaN -- under pandas 3's `str` dtype as
            # much as under object -- and polars refuses a float among strings, so
            # the sentinel has to be translated rather than passed through.
            arr = s.astype(object).where(s.notna(), None).to_numpy()
        cols[name] = arr
    return pl.DataFrame(cols)


def _from_polars(df) -> pd.DataFrame:
    return pd.DataFrame({c: df[c].to_numpy() for c in df.columns})

SCHEMA_COLS = [
    "source", "orf", "channel", "mnxr",
    "intermediate_id", "intermediate_name",
    "raw_score", "score_kind", "projection_via",
    "evidence_quality", "lane_set",
]

# The grain: one row per (source, orf, channel, intermediate_id, mnxr). NO cross-lane
# dedup -- the same ORF->MNXR claim from two lanes stays two rows, distinguished by
# `channel`. That is the point of having lanes.
#
# `orf` NAMES WHATEVER NOMINATES THE REACTION, and is never null. Usually that is an
# ORF; on a curated row it is the construct or the model gene, because a curated set
# names a construct rather than a sequence. One always-populated column is what lets
# belief conservation group per nominator without a fallback chain deciding, per table,
# which column that is.

# ---------------------------------------------------------------------------
# EXTENSION BLOCKS. Every GPR table in this tree is SCHEMA_COLS plus zero or more of
# these, in this order -- hosts, clones, cohorts and runs alike. They exist because
# three layers each grew the columns they needed and none of them declared any, which
# left four incompatible layouts that one validator could not check.
#
# A block is added when a LAYER needs it, not when a table happens to have it: a
# consumer can then test for the block rather than for a column.
SCHEMA_EXTENSIONS = {
    # WHICH BUILD, WHICH ORGANISM, WHICH BACKGROUND. `unit_id` is the background
    # selector `ecspr.model.gpr` reads -- the model or proteome these rows describe --
    # and is what makes "this condition runs against this host" a concatenation.
    "attribution": ("build_id", "host", "unit_id"),
    # HOW THE NOMINATOR MAPS TO REACTIONS. An ORF maps per gene; a GEM gene maps
    # through a boolean rule, and `feature_kind=ruleless` says the model gave none.
    "feature": ("feature_kind", "feature_name", "gpr_rule"),
    # Whether the reaction has atom pairs, i.e. whether ECSPr can carry current through
    # it. Null means NOT ASSERTED and is never defaulted to True: a row wrongly marked
    # in-universe claims an edge for a reaction that has no atom pairs.
    "universe": ("in_atom_universe",),
    # WHICH EXPERIMENTAL CONDITION a row belongs to, for a cohort table.
    "cohort": ("condition_id", "cohort", "action", "source_organism"),
}


GRAIN_KEY = ["source", "orf", "channel", "intermediate_id", "mnxr"]


def grain_key(extensions=()) -> list:
    """One row per this. `cohort` widens it: an ORF nominated under two conditions is
    two claims about two constructs, not one claim written twice -- ASKA shares ORFs
    between clones, and collapsing them would silently halve a clone's evidence."""
    key = list(GRAIN_KEY)
    if "cohort" in extensions:
        # `action` too, and it is not redundant: a complementation condition deletes
        # the chromosomal copy and adds a plasmid one, so the same gene appears twice
        # under one condition_id with opposite actions. Both are true of it.
        key.extend(("condition_id", "action"))
    return key


def extensions_of(df) -> tuple:
    """The blocks a table carries, read off its columns.

    Block membership is all-or-nothing: a table has the block or it does not. Counting
    columns cannot answer this -- core plus attribution+feature+universe is 18 wide,
    and so is the old cohort layout.
    """
    cols = set(df.columns)
    found = []
    for name, block in SCHEMA_EXTENSIONS.items():
        have = [c for c in block if c in cols]
        if not have:
            continue
        if len(have) != len(block):
            raise SystemExit(f"[gpr] block {name!r} is half present: has {have}, "
                             f"missing {[c for c in block if c not in cols]}. A block "
                             f"is a unit; a table with part of one is not describable")
        found.append(name)
    return tuple(found)


def is_unified(df) -> bool:
    """True when the table already carries the core plus whole blocks, in order."""
    return list(df.columns) == schema_for(extensions_of(df))


def schema_for(extensions=()) -> list:
    """The column list a table with these extension blocks must have, in order."""
    cols = list(SCHEMA_COLS)
    for b in extensions:
        if b not in SCHEMA_EXTENSIONS:
            raise SystemExit(f"[gpr] unknown schema extension {b!r}; "
                             f"known: {sorted(SCHEMA_EXTENSIONS)}")
        cols.extend(SCHEMA_EXTENSIONS[b])
    return cols

# ---------------------------------------------------------------------------
# The frozen channel vocabulary. One name per lane, and the same name in every
# producer and consumer. Two vocabularies existed here before (`dl_ec`,
# `uniref50_dr`, `pbert_transfer` beside the mappers' `ezpred`, `uniref50`,
# `pbert`); a join written against one silently returns nothing against the other,
# so the names are declared once, here, and read from here.
LANE_SETS = {
    "chosen_4": ("kofam", "clean", "uniref50", "pbert"),
    "full_7": ("kofam", "clean", "deepec", "ezpred", "uniref50", "pbert", "esmc"),
}
CHANNELS = tuple(sorted(set(c for cs in LANE_SETS.values() for c in cs)))

# ASSERTIONS, not lane evidence. A lane can fail -- which is why a lane set is checked
# for completeness, by name. These cannot: there is no "the gem_gpr lane returned
# nothing", only a table that does or does not carry a curated model. They are legal in
# any table, exempt from the completeness check, and score `presence`.
# channel -> the score_kind it carries.
ASSERTION_CHANNELS = {
    "gem_gpr": "presence",           # a curated genome-scale model's own GPR
    "manual_gpr": "presence",        # a curator's reading of a study
    "curated_insertion": "presence", # an engineered construct the study introduced
    "bridge": "bridge",              # an edge composed between two community members
}
# The `lane_set` value for a table that carries assertions and no lane evidence.
CURATED = "curated"

# ---------------------------------------------------------------------------
# `raw_score` is a DIRECTION AND RANGE contract, not a calibration.
#
# It is deliberately NOT comparable across channels: the one scheme that consumes it
# numerically (buildlib/bench_evidence_weights.nomination_contributions) normalises
# strictly within (orf, channel) as a share of sum, so a cross-channel calibration
# would buy nothing. What that share-of-sum DOES require, and what was missing, is
# that the number be non-null, finite, non-negative, and HIGHER-IS-STRONGER in every
# channel -- otherwise the weakest call in a lane gets the largest weight, or a NaN
# collapses the lane to uniform with nothing raised.
#
# `score_kind` says what the number is, so a consumer that reaches for a cross-channel
# comparison can see that it must not.
SCORE_KINDS = {
    # HMM bitscore, above the family's own KOfam threshold. Unbounded above.
    "hmm_bitscore": (0.0, None),
    # CLEAN's GMM-calibrated confidence, exactly as its wrapper writes it. Already
    # higher-is-stronger and already in [0, 1], so the lane stores it unchanged.
    "clean_confidence": (0.0, 1.0),
    # RETIRED, AND REVERSED. `clean_score` was read as a maxsep DISTANCE and stored
    # as 1/(1+d), which ranked every CLEAN call backwards -- the weakest call in the
    # lane got the largest weight. It is not a distance. Measured against the
    # 1,288-ORF DH10B truth set: correct calls sit at a median clean_score of 0.9973
    # and wrong ones at 0.1328 (AUC 0.897 in the higher-is-better direction), and
    # ORFs with no curated EC at all average 0.044 against 0.884 for those that have
    # one. The mechanism is CLEAN's GMM calibration, which its wrapper enables by
    # linking `data/pretrained/gmm_ensumble.pkl`; the numbers above are the evidence,
    # and they do not depend on that reading. The kind is kept so a table produced
    # before the fix SAYS its CLEAN lane is inverted rather than passing as sound;
    # nothing writes it now, and such a lane must be re-derived, not rescaled.
    "clean_maxsep_inv": (0.0, 1.0),
    # DIAMOND bit-score ratio. ~1.0 for a self-hit; the ceiling is slack, not a claim.
    "blast_bsr": (0.0, 4.0),
    # kNN label-transfer vote fraction over the top-K reference neighbours.
    "knn_vote": (0.0, 1.0),
    # A classifier head's softmax over EC classes.
    "softmax": (0.0, 1.0),
    # A score-less tool asserting presence. Exactly 1.0 -- NOT NaN, which the
    # share-of-sum reads as "no evidence anywhere in this lane".
    "presence": (1.0, 1.0),
    # A composed edge between two members of a community. Unit weight because the
    # bridge's conductance is set where the network is composed, not where it is
    # nominated -- see the compose step, which reads this as an existence claim.
    "bridge": (1.0, 1.0),
}

CHANNEL_SCORE_KIND = {
    "kofam": "hmm_bitscore",
    "clean": "clean_confidence",
    "uniref50": "blast_bsr",
    "pbert": "knn_vote",
    "esmc": "knn_vote",
    "deepec": "presence",
    "ezpred": "softmax",
}

# A score_kind no producer writes any more -> what replaced it. `validate_gpr` still
# ACCEPTS a table carrying one, because refusing would only make an already-wrong
# table unreadable; it names the retirement instead, so the reason is on screen where
# the table is used rather than in a commit message.
RETIRED_SCORE_KINDS = {
    "clean_maxsep_inv": "clean_confidence",
}

# Carried from ref::mnxr_lookup, which already computes it and whose keep-first dedup
# already retains the stronger claim -- it was simply being dropped at the join. It is
# the only thing separating a Swiss-Prot-backed reaction call from a TrEMBL one.
# `unknown` is what a row migrated off one of the pre-schema layouts carries: those
# tables dropped the quality at the join, and the migration will not invent one.
# `synthetic` is a row nothing observed: a composition's bridge pseudo-reaction exists
# because the composition put it there. Keeping it distinct from `reviewed` is what
# stops a community's own scaffolding being counted as evidence about its members.
EVIDENCE_QUALITY = ("reviewed", "unreviewed", "unknown", "synthetic")

_MNXR_RE = re.compile(r"^MNXR\d+$")
# A composition namespaces each member's reactions by member (`ERY:MNXR183533`) so two
# members' copies of one reaction stay distinct nodes, and adds `BRIDGE:...`
# pseudo-reactions for the edges between them. Both only ever appear under `composed`.
_MNXR_COMPOSED_RE = re.compile(r"^(?:[A-Za-z0-9_.-]+:)?MNXR\d+$|^BRIDGE:[^\s]+$")

# =====================================================================
# LANE CUT-OFFS. Every threshold a lane abstains on, declared once and read by both
# mappers -- the 4-lane and the 7-lane -- so a retuned number cannot ship in one and
# not the other. They lived as literals in three files, which is how the pbert floor
# came to have three homes.
#
# EVERY ONE OF THESE IS APPLIED INSIDE ITS LANE, BEFORE THE GPR TABLE EXISTS. That is
# the kofam pattern, and kofam is the reason: it has always kept only `score >=
# thrshld`, its family's own cutoff, at parse time. A lane that emits its weak calls
# and leaves the cut to a reader has published them. There is nothing here to opt out
# of, for the same reason there is no way to opt out of kofam's threshold.
# =====================================================================

# EZpred `ezpred` lane keeps level-4 ECs whose softmax score clears this floor.
DL_EC_SCORE_FLOOR = 0.3

# --- CLEAN -----------------------------------------------------------------
# CLEAN never declines: it emits a full level-4 EC for ~99% of ORFs, so its coverage
# measures its willingness rather than its reach. Its confidence separates well
# (median 0.9973 on correct calls vs 0.1328 on wrong ones), so the abstain is a plain
# threshold on it. 0.02 is the F1 argmax on the DH10B cohort -- P 0.954 / R 0.907 /
# F1 0.930 / coverage 0.950, against P 0.919 / F1 0.918 with no abstain at all. The
# scadc ablation's 0.01 scores 0.9293 on the same cohort, a tie within one grid step;
# this is not a win over that study, it is the same cut re-derived on this chassis.
CLEAN_MIN_SCORE = 0.02

# --- ProteinBERT kNN label transfer ----------------------------------------
# Tuned on the 1,288-ORF DH10B cohort against the 222,019-landmark set, choosing the
# cell that maximises label-level (MNXR macro set-overlap) F1 under the `twin`
# leakage condition -- every landmark at cosine >= 0.99 hidden. Label-level is the
# axis that PENALISES over-prediction, which is the failure being fixed; the ORF-level
# axis cannot see flooding at all, because more labels per ORF make an intersection
# with the truth set EASIER. The cell sits on a broad plateau (0.4018-0.4036 across
# nn_min 0.65-0.75 and tau 0.90-0.98), not a spike.
#
# `twin` IS A COSINE STAND-IN, NOT HOMOLOGY REMOVAL. It hides exact-ish duplicates,
# not paralogs, isozymes, or a related E. coli protein sitting at cosine 0.95 -- and
# Swiss-Prot contains E. coli's own proteome. The source study removed homologs by
# DIAMOND clustering, so these numbers are an UPPER BOUND against lanes measured that
# way, not a like-for-like comparison. See
# research/fabfos/annotation_lanes/pbert/threshold_cosine_dh10b.tsv.
#
#                      ORF-level (EC)              label-level (MNXR)
#   twin       P 0.6555 -> 0.8985   F1 0.6374 -> 0.8254   P 0.5655 -> 0.8050
#   self       P 0.6852 -> 0.9204   F1 0.6683 -> 0.8505   P 0.5938 -> 0.8550
#   pool       P 0.7104 -> 0.9611   F1 0.6929 -> 0.8983   P 0.6093 -> 0.9075
#
# Coverage falls from 0.946 to 0.849 on `twin`: 189 of 1,288 ORFs now get no pbert
# call at all. That is the point of the change, not a side effect of it.

# THE ORF-LEVEL CUT ON PROXIMITY, and the refusal this lane did not have. An ORF
# whose nearest landmark is below it gets no call. PBERT_FLOOR cannot do this job:
# it thresholds a vote normalised within the admitted set, so it measures neighbour
# AGREEMENT -- thirty neighbours at cosine 0.15 that agree score 1.0.
PBERT_NN_MIN = 0.70
# THE RELATIVE BAND. A neighbour votes only if its cosine is also within this
# fraction of the best one for that ORF, so a dense neighbourhood votes with many
# neighbours and a thin one with a few or with exactly one. It is nearly invisible on
# the ORF-level axis and worth a great deal on the label-level one -- at nn_min 0.75
# it lifts label precision from 0.708 to 0.826 while moving ORF-level F1 by 0.008 --
# because emitting more labels per ORF makes an ORF-level intersection EASIER, so
# that axis cannot see flooding and this band is what stops it.
PBERT_TAU = 0.95
PBERT_K_MAX = 30
# ZERO, AND MEASURED. The vote floor was 0.20 and its every increase costs F1 in all
# three conditions (twin: 0.8254 at 0.00, 0.8081 at 0.20, 0.7888 at 0.50) -- once
# nn_min decides which neighbours are close enough to speak and tau decides which of
# those speak together, an agreement threshold has nothing left to reject but true
# positives. It was standing in for a proximity gate, badly, and there is now a real
# one. Kept as a knob rather than deleted; `lane_embed` refuses only a NEGATIVE
# floor, because zero means "every label an admitted neighbour carries" and that is
# a real setting, not a disabled one.
PBERT_FLOOR = 0.00

# THE ARCHIVE'S FLOOR IS NOT THE LANE'S. `read_embed_transfer` reads the retired
# `embed_transfer_candidates.parquet`, which a different producer wrote against a
# different reference pool with its own frozen 0.20 baked in. Sharing PBERT_FLOOR
# with it meant retuning the live lane silently retuned how an archived table is
# read -- and at 0.00 that filter became a no-op nobody asked for.
EMBED_ARCHIVE_FLOOR = 0.20

# --- ESM-C kNN label transfer ----------------------------------------------
# The same lane against a different backbone, and cosine is not comparable between two
# embedding spaces -- so pbert's tuned numbers are not transferable here, and are not
# copied. 0.0 for both quota knobs is the pre-quota behaviour stated explicitly:
# retrieve K_MAX, admit all of them, refuse no ORF. The seven-lane reproduction has
# not been measured, and an unmeasured cut-off would refuse ORFs on a number nobody
# checked. Tune these on an ESM-C cohort before quoting an ESM-C precision.
ESMC_NN_MIN = 0.0
ESMC_TAU = 0.0
ESMC_K_MAX = 30
ESMC_FLOOR = 0.10


# =====================================================================
# migration off the pre-schema layouts
# =====================================================================

# The retired spellings, and what each is now. A join written against one silently
# returns nothing against the other, which is why they are mapped here once rather
# than wherever a reader happens to notice.
RETIRED_CHANNELS = {
    "clean_ec": "clean",
    "uniref50_dr": "uniref50",
    "dl_ec": "ezpred",
}

# The 14-column benchmark layout and the 18-column cohort layout, by the column each
# one contributed. Both are the same table with different blocks attached.
_LEGACY_CORE = {
    "feature_id": "orf",
    "evidence_id": "intermediate_id",
    "evidence_name": "intermediate_name",
}
# `feature_id` is null on a curated row -- a curated set names a construct, not a
# sequence -- so the nominator is recovered in this order and `orf` is populated once,
# here, instead of every consumer re-deciding which column to read.
_ORF_SOURCES = ("feature_id", "feature_name", "evidence_id")


def normalise_channel(ch: str) -> str:
    """A legacy channel spelling -> the frozen vocabulary.

    Strips the `denovo_` prefix as well: it existed so a lane survived a merge with a
    GEM table, and `lane_set` and the assertion channels now carry that distinction.
    """
    ch = str(ch)
    if ch.startswith("denovo_"):
        ch = ch[len("denovo_"):]
    return RETIRED_CHANNELS.get(ch, ch)


def to_unified(df, extensions=("attribution", "feature", "universe")):
    """One of the pre-schema layouts -> SCHEMA_COLS plus `extensions`.

    Single place for the mapping, so a producer and a one-off data migration cannot
    disagree about it. The caller names the blocks because only the caller knows which
    layer the table belongs to; the 18-column cohort tables add `cohort`.

    Raises rather than guessing: an assertion channel whose `raw_score` is not 1.0, or
    a table carrying more than one `unit_id`, is a table this mapping does not describe.
    """
    import pandas as pd

    out = df.rename(columns=_LEGACY_CORE).copy()

    orf = None
    for c in _ORF_SOURCES:
        col = _LEGACY_CORE.get(c, c)
        if col in out.columns:
            orf = out[col] if orf is None else orf.fillna(out[col])
    if orf is None or orf.isna().any():
        raise SystemExit(f"[gpr] {int(orf.isna().sum()) if orf is not None else 'every'} "
                         f"row(s) name no nominator in any of {_ORF_SOURCES}")
    out["orf"] = orf.astype(str)

    out["channel"] = out["channel"].map(normalise_channel)
    lanes = sorted(set(out["channel"]) - set(ASSERTION_CHANNELS))
    unknown = sorted(set(lanes) - set(CHANNELS))
    if unknown:
        raise SystemExit(f"[gpr] channel(s) {unknown} are neither a declared lane nor "
                         f"an assertion; the vocabulary is {list(CHANNELS)} plus "
                         f"{sorted(ASSERTION_CHANNELS)}")

    out["score_kind"] = out["channel"].map(
        lambda c: "presence" if c in ASSERTION_CHANNELS else CHANNEL_SCORE_KIND[c])
    assertion = out["channel"].isin(ASSERTION_CHANNELS)
    bad = out.loc[assertion & (out["raw_score"].astype(float) != 1.0)]
    if len(bad):
        raise SystemExit(f"[gpr] {len(bad):,} assertion rows carry a raw_score that is "
                         f"not 1.0, e.g. {sorted(set(bad['raw_score']))[:3]} -- an "
                         f"assertion is a presence claim, not a ranked one")

    if "unit_id" in out.columns and out["unit_id"].nunique() != 1:
        raise SystemExit(f"[gpr] {out['unit_id'].nunique()} unit_ids in one table "
                         f"{sorted(out['unit_id'].unique())[:4]}; `source` names one "
                         f"artifact, so this table is really several")
    # `source` names the artifact the rows were read out of. A pre-schema table did not
    # record one, and `unit_id` is the closest thing it has -- so a migrated table says
    # the model or proteome where a freshly produced one says the ORF set it was mapped
    # from. Both are true of the table; they are not the same string, and a reader
    # comparing `source` across the two eras has to know that.
    out["source"] = out["unit_id"].astype(str) if "unit_id" in out.columns else ""
    # The lane set is a property of the evidence, not of the file: a table with no lane
    # channels asserts rather than measures, whatever it is called.
    out["lane_set"] = _lane_set_for(set(lanes))
    if "evidence_quality" not in out.columns:
        out["evidence_quality"] = "unknown"
    if "projection_via" not in out.columns:
        out["projection_via"] = ""
    out["projection_via"] = out["projection_via"].fillna("")
    out["intermediate_id"] = out["intermediate_id"].fillna("")
    out["intermediate_name"] = out["intermediate_name"].fillna("")

    want = schema_for(extensions)
    missing = [c for c in want if c not in out.columns]
    if missing:
        raise SystemExit(f"[gpr] the source table has no {missing} to carry into "
                         f"extension blocks {list(extensions)}")
    return out[want].reset_index(drop=True)


def read_gpr(paths, extensions=None):
    """Read one or more GPR tables and return them on the declared schema.

    The point of entry for a CONSUMER. A table written before the schema is converted
    on the way through, so a reader never has to know which layout it got -- which is
    what the four layouts cost every reader until now, each one re-deciding whether the
    nominator lived in `feature_id`, `feature_name` or `evidence_id`.

    `extensions` defaults to the blocks the table already carries. Pass it to require
    a block: a reader that needs `unit_id` should say so and fail on a table without it,
    rather than discover the gap as a KeyError three frames later.
    """
    if isinstance(paths, (str, bytes)) or hasattr(paths, "__fspath__"):
        paths = [paths]
    frames = []
    for path in paths:
        df = read_parquet(path)
        ext = tuple(extensions) if extensions is not None else extensions_of(df)
        if not is_unified(df) or (extensions is not None
                                  and list(df.columns) != schema_for(ext)):
            df = to_unified(df, ext)
        frames.append(df)
    if len(frames) == 1:
        return frames[0]
    # Concatenating is how "this condition runs against this host" is expressed, and it
    # only means anything once every frame is on one schema -- which it now is.
    return pd.concat(frames, ignore_index=True)


def _lane_set_for(lanes: set) -> str:
    """The declared set these lanes are, or CURATED when there are none."""
    if not lanes:
        return CURATED
    for name, members in LANE_SETS.items():
        if lanes == set(members):
            return name
    raise SystemExit(
        f"[gpr] lanes {sorted(lanes)} are not a declared set. Known: "
        + "; ".join(f"{k}={list(v)}" for k, v in LANE_SETS.items())
        + ". A table carrying some of a set is the failure this schema exists to name.")


# =====================================================================
# the validator -- called by both mappers immediately before to_parquet
# =====================================================================

def validate_gpr(df, lane_set: str, orf_ids, source: str, extensions=(),
                 composed: bool = False):
    """Refuse to write a GPR table that violates the contract above.

    `extensions` names the blocks this table carries beyond SCHEMA_COLS (see
    SCHEMA_EXTENSIONS). `lane_set` is a LANE_SETS key, or CURATED for a table of
    assertions with no lane evidence. `orf_ids` may be None when there is no ORF set to
    check against -- a curated model names genes, not sequences. `composed` says the
    table is a community composition rather than one artifact's evidence, which is the
    only thing that may namespace `mnxr` and mix `bridge` rows in.

    Every check here exists because its absence was silent. An empty concat, an
    ORF-id mismatch between two lanes, a CLEAN header drift, or a lane whose
    reference never staged all produced a zero-row parquet and reported success.

    Raises SystemExit naming the specific failure; prints per-channel coverage and
    score distributions on the way through, because the first real run is also the
    measurement that settles what these numbers look like.
    """
    import numpy as np  # local: the module is pandas-only for its bridge callers

    if lane_set == CURATED:
        expected = ()
    else:
        expected = LANE_SETS.get(lane_set)
        if expected is None:
            raise SystemExit(f"[gpr] unknown lane_set {lane_set!r}; known: "
                             f"{sorted(LANE_SETS) + [CURATED]}")

    want = schema_for(extensions)
    if list(df.columns) != want:
        raise SystemExit(
            f"[gpr] column set/order is not the schema for extensions {list(extensions)}.\n"
            f"      got:      {list(df.columns)}\n"
            f"      expected: {want}")

    if len(df) == 0:
        raise SystemExit(
            "[gpr] the table is empty. Every lane joined to nothing, which is a "
            "staging or id-space failure, not a biological finding -- 157 fosmid "
            "inserts do not encode zero recognisable enzymes")

    # The CORE is non-null by contract. Extension columns are not: `in_atom_universe`
    # null means "not asserted" and `gpr_rule` null means the model gave no rule, and
    # both are information a consumer can see and act on.
    assertion = df["channel"].isin(ASSERTION_CHANNELS)
    # `mnxr` is null exactly on a ROSTER row: an assertion that this nominator is in the
    # population and resolves to no reaction at all. A census keeps those rows on
    # purpose -- dropping them would silently shrink the denominator of every rate
    # measured over the population. On a lane row a null mnxr is a broken join.
    core = [c for c in SCHEMA_COLS if c != "mnxr"]
    nulls = {c: int(df[c].isna().sum()) for c in core if df[c].isna().any()}
    n_bad_mnxr = int((df["mnxr"].isna() & ~assertion).sum())
    if n_bad_mnxr:
        nulls["mnxr (on lane rows)"] = n_bad_mnxr
    if nulls:
        raise SystemExit(f"[gpr] null values in {nulls} -- every core column is "
                         f"non-null by contract, except `mnxr` on an assertion row")

    allowed = set(expected) | set(ASSERTION_CHANNELS)

    unknown = set(df["channel"].unique()) - allowed
    if unknown:
        raise SystemExit(
            f"[gpr] channels not in lane_set {lane_set}: {sorted(unknown)}; "
            f"the vocabulary is {list(expected)} plus the assertion channels "
            f"{sorted(ASSERTION_CHANNELS)}")

    # An assertion is a presence claim. Scoring one any other way would put it on a
    # lane's scale, where the share-of-sum would then rank it against real evidence.
    bad_assert = sorted(
        f"{c}:{k}" for c, k in
        df.loc[assertion, ["channel", "score_kind"]].drop_duplicates().itertuples(index=False)
        if k != ASSERTION_CHANNELS[c])
    if bad_assert:
        raise SystemExit(f"[gpr] assertion channel(s) carry the wrong score_kind "
                         f"{bad_assert}; the declared kinds are {ASSERTION_CHANNELS}")

    # --- score contract
    bad_kind = set(df["score_kind"].unique()) - set(SCORE_KINDS)
    if bad_kind:
        raise SystemExit(f"[gpr] unknown score_kind(s) {sorted(bad_kind)}; known: {sorted(SCORE_KINDS)}")
    for ch, kind in df.groupby("channel")["score_kind"].agg(lambda s: sorted(set(s))).items():
        want = ASSERTION_CHANNELS.get(ch) or CHANNEL_SCORE_KIND[ch]
        # A retired kind is accepted ONLY when it is the whole channel. A table
        # concatenated from a pre-fix and a post-fix run carries both kinds on one
        # channel, and that is the one case that must not pass: both declare the
        # range (0, 1) so no bound catches it, no consumer branches on score_kind,
        # and nothing marks which rows are which -- half the column would be
        # 1/(1+d) and half the raw confidence, unrecoverably.
        retired = sorted(k for k in kind if RETIRED_SCORE_KINDS.get(k) == want)
        if kind == retired:
            print(f"[gpr] WARNING: channel {ch!r} carries the retired score_kind "
                  f"{retired}, now {want!r}. The table is readable and its raw_score "
                  f"is not -- see SCORE_KINDS for what that number actually is. "
                  f"Re-run the mapper rather than rescaling it.", flush=True)
        elif kind != [want]:
            extra = f" -- it mixes the retired {retired} with {want!r}, so the column " \
                    f"is half one scale and half the other" if retired else ""
            raise SystemExit(f"[gpr] channel {ch!r} carries score_kind {kind}, "
                             f"expected ['{want}']{extra}")
    s = df["raw_score"].astype(float)
    if not np.isfinite(s).all():
        n = int((~np.isfinite(s)).sum())
        raise SystemExit(
            f"[gpr] {n:,} raw_score values are NaN or infinite. The downstream "
            f"share-of-sum reads a NaN lane total as zero and silently falls back "
            f"to a uniform split, so this must never reach the table")
    # float32 slack: a kNN vote is a convex combination that sums to 1 in exact
    # arithmetic and to 1.0000001 in float32. The bound is a contract about the
    # scale, not about the last mantissa bit.
    tol = 1e-6
    for kind, grp in df.groupby("score_kind"):
        lo, hi = SCORE_KINDS[kind]
        v = grp["raw_score"].astype(float)
        if lo is not None and v.min() < lo - tol:
            raise SystemExit(f"[gpr] score_kind {kind!r}: min {v.min():.9g} below the declared floor {lo}")
        if hi is not None and v.max() > hi + tol:
            raise SystemExit(f"[gpr] score_kind {kind!r}: max {v.max():.9g} above the declared ceiling {hi}")

    # --- keys and vocabularies
    named = df.loc[df["mnxr"].notna()]
    pattern = _MNXR_COMPOSED_RE if composed else _MNXR_RE
    bad_mnxr = named.loc[~named["mnxr"].astype(str).str.match(pattern), "mnxr"].unique()
    if len(bad_mnxr):
        raise SystemExit(f"[gpr] {len(bad_mnxr):,} non-MNXR reaction ids, e.g. {list(bad_mnxr[:5])}")
    bad_eq = set(df["evidence_quality"].unique()) - set(EVIDENCE_QUALITY)
    if bad_eq:
        raise SystemExit(f"[gpr] unknown evidence_quality {sorted(bad_eq)}; known: {list(EVIDENCE_QUALITY)}")
    # A COMPOSED network is a community, and its rows come from two places: each
    # member's own lane evidence, and the bridges that join them. Those are different
    # claims, so they carry different `lane_set` values in one table -- which is why a
    # composition has to say so rather than be validated as a single-source table.
    want_ls = {lane_set} | ({"bridge"} if composed else set())
    if not set(df["lane_set"].unique()) <= want_ls or lane_set not in set(df["lane_set"]):
        raise SystemExit(f"[gpr] lane_set column carries {sorted(set(df['lane_set']))}, "
                         f"expected {sorted(want_ls)}")
    if set(df["source"].unique()) != {source}:
        raise SystemExit(f"[gpr] source column carries {sorted(set(df['source']))}, expected [{source!r}]")

    key = grain_key(extensions)
    dupes = int(df.duplicated(subset=key).sum())
    if dupes:
        raise SystemExit(f"[gpr] {dupes:,} rows duplicate the grain key {key}")

    ids = set(orf_ids) if orf_ids is not None else None
    stray = (set(df["orf"].unique()) - ids) if ids is not None else set()
    if stray:
        raise SystemExit(
            f"[gpr] {len(stray):,} ORF ids are not in the input FASTA, e.g. "
            f"{sorted(stray)[:5]}. The lanes annotate different ORF sets, or one "
            f"lane's tool rewrote the ids (CLEAN splits on whitespace, DIAMOND does "
            f"not) -- a join on gene id across lanes is meaningless until this is 0")

    # --- completeness, last: the per-row contract above is what a malformed table
    # violates, and a lane that emptied is more legible once those are known good.
    # Named individually, so the message says WHICH lane is missing.
    for ch in expected:
        if int((df["channel"] == ch).sum()) == 0:
            raise SystemExit(
                f"[gpr] channel {ch!r} contributed 0 rows. A lane that produces "
                f"nothing is a broken join or an unstaged reference; writing the "
                f"table anyway hides which of the {len(expected)} lanes failed")

    # --- the measurement half: everything below prints, nothing below raises
    n_ids = len(ids) if ids is not None else df["orf"].nunique()
    print(f"[gpr] {len(df):,} rows | source={source} | lane_set={lane_set} | "
          f"{df['orf'].nunique():,} of {n_ids:,} nominators | "
          f"{df['mnxr'].nunique():,} MNXR", flush=True)
    present_assertions = [c for c in sorted(ASSERTION_CHANNELS)
                          if (df["channel"] == c).any()]
    for ch in tuple(expected) + tuple(present_assertions):
        g = df[df["channel"] == ch]
        v = g["raw_score"].astype(float)
        print(f"[gpr]   {ch:9s} {len(g):>9,} rows  {g['orf'].nunique():>7,} ORFs "
              f"({100.0*g['orf'].nunique()/max(n_ids,1):5.1f}% cover)  "
              f"{g['mnxr'].nunique():>6,} MNXR  "
              f"score[{g['score_kind'].iat[0]}] min={v.min():.4g} "
              f"med={v.median():.4g} max={v.max():.4g}", flush=True)
    print(f"[gpr]   evidence_quality {df['evidence_quality'].value_counts().to_dict()}", flush=True)
    return df


# =====================================================================
# bridge loaders
# =====================================================================

def load_ko_to_mnxr(path: Path) -> pd.DataFrame:
    """KO -> MNXR fan-out table (reused reference cache). Multi-row per KO is normal."""
    df = pd.read_csv(path, sep="\t")
    df = df.rename(columns={"ko": "ko", "mnx_r": "mnxr"})
    df = df[["ko", "mnxr"]].drop_duplicates()
    return df


def load_ec_to_mnxr(path: Path) -> pd.DataFrame:
    """Parse reac_prop.tsv classifs (col 4) -> long-format ec -> mnxr fan-out.

    classifs is `;`-separated; each token is a raw EC number (no `ec:` prefix).
    Partial ECs like "1.2.1" are kept verbatim.
    """
    rows = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            mnxr, classifs = parts[0], parts[3]
            if not mnxr.startswith("MNXR") or not classifs:
                continue
            for ec in classifs.split(";"):
                ec = ec.strip()
                if not ec:
                    continue
                rows.append((ec, mnxr))
    return pd.DataFrame(rows, columns=["ec", "mnxr"]).drop_duplicates()


def load_uniprot_to_mnxr(path: Path) -> pd.DataFrame:
    """The cache produced by build-uniprot-bridge."""
    return read_parquet(path)


# The three id spaces share no ids, so `id` alone is unambiguous in the
# consolidated lookup and `id_source` is a label rather than a disambiguator.
MNXR_LOOKUP_SOURCES = ("ko", "ec", "uniprot")


def load_mnxr_lookup(path: Path, id_source: str) -> pd.DataFrame:
    """One lane's slice of the consolidated `ref::mnxr_lookup` bridge.

    Returns the two columns the lane readers join on, renamed to that lane's id
    column, so the loaders above and this one are interchangeable at the call
    site. Reading with a pushdown filter keeps the uniprot slice (30.4M rows) off
    the heap when a lane only wants ko or ec.
    """
    if id_source not in MNXR_LOOKUP_SOURCES:
        raise ValueError(f"unknown id_source {id_source!r}; expected one of {MNXR_LOOKUP_SOURCES}")
    df = read_parquet(
        path, columns=["id", "mnxr"], filters=[("id_source", "==", id_source)],
    )
    if df.empty:
        raise ValueError(f"mnxr_lookup at {path} carries no {id_source!r} rows")
    key = {"uniprot": "uniprot_accession"}.get(id_source, id_source)
    return df.rename(columns={"id": key}).drop_duplicates()


# =====================================================================
# helpers
# =====================================================================

_BOILERPLATE_RE = re.compile(r"\s+(n=\d+|Tax=.+?|RepID=\S+)(?=\s|$)")


def _clean_stitle(stitle: str) -> str:
    """Strip UniRef50 boilerplate from a stitle to get a readable name."""
    if not isinstance(stitle, str):
        return ""
    s = stitle
    if s.startswith("UniRef50_"):
        head, _, rest = s.partition(" ")
        s = rest
    return _BOILERPLATE_RE.sub("", s).strip()


# =====================================================================
# lane readers (unified 11-col schema)
#
# These read the DEPLOYED scadc lane layouts, which differ from the ones the
# `transforms/fabfos/` mappers consume (a kofam CSV with `fosmid`/`contig`
# columns, an EZpred parquet rather than a CSV). They are kept because that
# layout still exists in the archive; the mappers are authoritative for the
# pipeline as it runs now. Both emit the same schema and the same channel names.
# =====================================================================

def _finish(df: pd.DataFrame, source: str, channel: str, lane_set: str) -> pd.DataFrame:
    """Stamp the columns every lane sets identically and order to the schema."""
    df["source"] = source
    df["channel"] = channel
    df["score_kind"] = CHANNEL_SCORE_KIND[channel]
    df["lane_set"] = lane_set
    if "evidence_quality" not in df.columns:
        df["evidence_quality"] = "reviewed"
    if "intermediate_name" not in df.columns:
        df["intermediate_name"] = ""
    df["intermediate_name"] = df["intermediate_name"].fillna("")
    return df[SCHEMA_COLS]


def read_kofam(path, source: str, ko_to_mnxr: pd.DataFrame,
               lane_set: str = "chosen_4") -> pd.DataFrame:
    """Load *.kofam.csv, keep above-threshold hits, project KO -> MNXR."""
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_csv(path)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["hmm_threshold"] = pd.to_numeric(df["hmm_threshold"], errors="coerce")
    df = df[df["score"].notna() & df["hmm_threshold"].notna()]
    df = df[df["score"] >= df["hmm_threshold"]]
    if "fosmid" in df.columns:
        df["orf_id"] = df["fosmid"].astype(str) + "_" + df["orf"].astype(str)
    elif "contig" in df.columns:
        df["orf_id"] = df["contig"].astype(str) + "_" + df["orf"].astype(str)
    else:
        df["orf_id"] = df["orf"].astype(str)
    df = df[["orf_id", "ko", "score", "description"]].rename(
        columns={"orf_id": "orf", "score": "raw_score", "description": "intermediate_name"}
    )
    df = df.merge(ko_to_mnxr, on="ko", how="inner")
    df["intermediate_id"] = df["ko"]
    df["projection_via"] = "kegg.reaction"
    return _finish(df, source, "kofam", lane_set)


def read_dl_ec(path, source: str, ec_to_mnxr: pd.DataFrame,
               lane_set: str = "full_7") -> pd.DataFrame:
    """Load EZpred (ESM-C 600M DL-only) EC predictions, project EC -> MNXR.

    parquet columns: sequence_id, ec_number, score, head_kind. Keeps enzyme-head,
    level-4 ECs (x.x.x.x) clearing DL_EC_SCORE_FLOOR; raw_score carries the
    EZpred confidence. Filters pushed into the reader so huge parquets never fully load.
    """
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = read_parquet(
        path,
        columns=["sequence_id", "ec_number", "score"],
        filters=[("head_kind", "==", "enzyme"), ("score", ">=", DL_EC_SCORE_FLOOR)],
    )
    df = df[df["ec_number"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, left_on="ec_number", right_on="ec", how="inner")
    df["intermediate_id"] = df["ec_number"]
    df = df.rename(columns={"sequence_id": "orf", "score": "raw_score"})
    df["orf"] = df["orf"].str.replace(r"-(\d+)$", r"_\1", regex=True)
    df["projection_via"] = "ec"
    return _finish(df, source, "ezpred", lane_set)


_BLAST6_BSR_COLS = [
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore", "bsr",
]


def read_uniref50_titles(path) -> dict:
    """`sseqid -> stitle` from the descriptions table beside a hit table."""
    if path is None or not Path(path).exists():
        return {}
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    if list(df.columns[:2]) != ["sseqid", "description"]:
        raise SystemExit(
            "[evidence] uniref50 descriptions header is not (sseqid, description): "
            "got " + repr(list(df.columns)) + ". merge_diamond_uniref50 writes it, "
            "so a drift here silently empties every intermediate_name")
    return dict(zip(df["sseqid"], df["description"]))


def read_uniref50(path, source: str, uniprot_to_mnxr: pd.DataFrame,
                  lane_set: str = "chosen_4", descriptions=None) -> pd.DataFrame:
    """Load DIAMOND BLAST6+BSR; best-hit per ORF; project UniProt -> MNXR."""
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_csv(path, sep="\t", dtype=str)
    if list(df.columns) != _BLAST6_BSR_COLS:
        raise SystemExit(
            "[evidence] uniref50 hit header is not the " + str(len(_BLAST6_BSR_COLS))
            + " columns this lane parses: got " + repr(list(df.columns))
            + ". diamond_uniref50 writes the header, so a drift here silently "
            "renames every column and empties the lane")
    df["evalue"] = pd.to_numeric(df["evalue"], errors="coerce")
    df["bitscore"] = pd.to_numeric(df["bitscore"], errors="coerce")
    df["bsr"] = pd.to_numeric(df["bsr"], errors="coerce")
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["uniprot_accession"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    titles = read_uniref50_titles(descriptions)
    df["intermediate_name"] = df["sseqid"].map(titles).fillna("").apply(_clean_stitle)
    df = df[["qseqid", "uniprot_accession", "intermediate_name", "bsr"]].rename(
        columns={"qseqid": "orf", "bsr": "raw_score"})
    df["orf"] = df["orf"].str.replace(r"-(\d+)$", r"_\1", regex=True)
    cols = ["uniprot_accession", "dr_source", "mnxr"]
    if "evidence_quality" in uniprot_to_mnxr.columns:
        cols.append("evidence_quality")
    joined = df.merge(uniprot_to_mnxr[cols], on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return _finish(joined, source, "uniref50", lane_set)


def read_embed_transfer(path, source: str, _bridge=None,
                        lane_set: str = "chosen_4") -> pd.DataFrame:
    """Load the embedding-transfer candidate table, ProteinBERT channel.

    Label transfer *is* the projection (mnxr already present,
    projection_via='embedding_knn'), so this only cuts on the vote floor and
    restamps. Accepts the archived `pbert_transfer` channel name as well as the
    frozen `pbert` -- the deployed tables on disk carry the former.
    """
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = read_parquet(path)
    df = df[df["channel"].isin(("pbert", "pbert_transfer"))
            & (df["raw_score"] >= EMBED_ARCHIVE_FLOOR)].copy()
    # The pool is the bridge's `reviewed` cut by construction, so every transferred
    # label inherits that quality -- see compile/label_transfer_landmarks.py.
    df["evidence_quality"] = "reviewed"
    return _finish(df, source, "pbert", lane_set)


# =====================================================================
# build-uniprot-bridge  (port of 00_build_uniprot_to_mnxr.py)
# =====================================================================

def _load_rhea_to_mnxr(reac_xref: Path) -> pd.DataFrame:
    rows = []
    with open(reac_xref) as fh:
        for line in fh:
            if line.startswith("#") or line.startswith("EMPTY"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxr = parts[0], parts[1]
            if not src.startswith("rhea:") or not mnxr.startswith("MNXR"):
                continue
            rows.append((int(src[len("rhea:"):]), mnxr))
    return pd.DataFrame(rows, columns=["rhea_id", "mnxr"])


def _load_rhea2uniprot(path: Path, evidence_quality: str) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep="\t",
        dtype={"RHEA_ID": "int64", "DIRECTION": "string", "MASTER_ID": "int64", "ID": "string"},
    )
    df = df.rename(columns={"RHEA_ID": "rhea_id", "DIRECTION": "direction", "ID": "uniprot_accession"})
    df = df[["rhea_id", "direction", "uniprot_accession"]]
    df["evidence_quality"] = evidence_quality
    return df


def build_uniprot_bridge(reac_xref: Path, rhea_swiss: Path, rhea_trembl: Path, out: Path):
    print("[bridge] rhea -> MNXR from reac_xref...", flush=True)
    rhea_mnxr = _load_rhea_to_mnxr(reac_xref)
    print(f"         {len(rhea_mnxr):,} rows ({rhea_mnxr['mnxr'].nunique():,} MNXRs)", flush=True)
    frames = [_load_rhea2uniprot(rhea_swiss, "reviewed")]
    if rhea_trembl is not None and Path(rhea_trembl).exists():
        print("[bridge] rhea2uniprot TrEMBL (big)...", flush=True)
        frames.append(_load_rhea2uniprot(rhea_trembl, "unreviewed"))
    all_u = pd.concat(frames, ignore_index=True)
    joined = all_u.merge(rhea_mnxr, on="rhea_id", how="inner")
    out_df = pd.DataFrame({
        "uniprot_accession": joined["uniprot_accession"],
        "protein_name": "", "gene_name": "",
        "dr_source": "rhea",
        "external_id": "rhea:" + joined["rhea_id"].astype(str),
        "mnxr": joined["mnxr"],
        "evidence_quality": joined["evidence_quality"],
        "direction": joined["direction"],
    })
    out_df = (out_df.sort_values(["uniprot_accession", "mnxr", "evidence_quality"])
                    .drop_duplicates(subset=["uniprot_accession", "external_id", "mnxr"], keep="first"))
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    write_parquet(out_df, out)
    print(f"[bridge] wrote {len(out_df):,} rows ({out_df['uniprot_accession'].nunique():,} UniProts) -> {out}", flush=True)


# =====================================================================
# build-mnxr-lookup  (consolidate the ko/ec/uniprot trio into one table)
# =====================================================================

def build_mnxr_lookup(ko: Path, ec: Path, uniprot: Path, out: Path):
    """Fold the three bridges into `id, id_source, mnxr, evidence_quality`.

    The three id spaces are disjoint, so one `id` column is unambiguous. The
    uniprot side deduplicates to distinct (accession, mnxr): the shipped bridge
    keys its dedup on `external_id` as well, so the same accession-reaction claim
    appears once per Rhea id that reaches it -- and nothing downstream reads
    `external_id`. `evidence_quality` survives the dedup because "reviewed" sorts
    before "unreviewed", so keep-first already retains the stronger claim.

    ko and ec are curated database mappings with no per-row provenance flag, so
    they carry `evidence_quality` empty rather than being assigned one here.
    `dr_source` is dropped: it is constant "rhea" across every uniprot row, and
    the lane that used it already declares `projection_via` itself.

    The uniprot side is 30.4M rows and is the one step here that will not fit in
    pandas -- object-dtype strings at that count are gigabytes to then dedup away
    -- so this function is polars-only rather than going through `read_parquet`
    above. It is a build step, run in the image that carries polars.
    """
    import polars as pl

    frames = []
    for path, source, col in ((ko, "ko", "ko"), (ec, "ec", "ec")):
        df = pd.read_csv(path, sep="\t")[[col, "mnxr"]].drop_duplicates()
        print(f"[lookup] {source}: {len(df):,} distinct pairs", flush=True)
        frames.append(pl.LazyFrame({
            "id": df[col].to_numpy(),
            "id_source": [source] * len(df),
            "mnxr": df["mnxr"].to_numpy(),
            "evidence_quality": [""] * len(df),
        }))

    up = pl.scan_parquet(uniprot).select(
        ["uniprot_accession", "mnxr", "evidence_quality", "dr_source"])
    seen = up.select(pl.col("dr_source").unique()).collect().to_series().to_list()
    if seen != ["rhea"]:
        raise ValueError(f"dr_source is not constant 'rhea' ({seen}); it cannot be dropped")
    up = (
        up.group_by(["uniprot_accession", "mnxr"])
          # min() over the group keeps "reviewed" wherever any route was reviewed.
          .agg(pl.col("evidence_quality").min())
          .select([
              pl.col("uniprot_accession").alias("id"),
              pl.lit("uniprot").alias("id_source"),
              pl.col("mnxr"),
              pl.col("evidence_quality"),
          ])
    )

    # Sorting on the two keys the readers filter and join by is what makes the
    # `id_source` pushdown skip row groups rather than scan them, and it is most
    # of the difference between 100 MB and 82 MB on disk.
    table = pl.concat(frames + [up], how="vertical").sort(["id_source", "id"]).collect()
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    table.write_parquet(out, compression="zstd")
    print(f"[lookup] wrote {len(table):,} rows -> {out}", flush=True)
    return len(table)


# =====================================================================
# compile  (port of 11_build_evidence_table_dlec.py, single-source capable)
# =====================================================================

def compile_evidence(source, kofam, dl_ec, uniref50, embed,
                     ko_to_mnxr_path, ec_to_mnxr_path, uniprot_to_mnxr_path, out,
                     uniref50_descriptions=None):
    print("[compile] loading bridges...", flush=True)
    ko_to_mnxr = load_ko_to_mnxr(ko_to_mnxr_path) if ko_to_mnxr_path else pd.DataFrame(columns=["ko", "mnxr"])
    # ec_to_mnxr here is the PRE-BUILT 2-col bridge (from build-ec-bridge), not
    # reac_prop -- read it plainly rather than re-running the reac_prop parser.
    if ec_to_mnxr_path:
        ec_to_mnxr = pd.read_csv(ec_to_mnxr_path, sep="\t")[["ec", "mnxr"]].drop_duplicates()
    else:
        ec_to_mnxr = pd.DataFrame(columns=["ec", "mnxr"])
    if uniprot_to_mnxr_path and Path(uniprot_to_mnxr_path).exists():
        uniprot_to_mnxr = load_uniprot_to_mnxr(uniprot_to_mnxr_path)
    else:
        print("[compile] uniprot_to_mnxr absent -> uniref50 lane skipped", flush=True)
        uniprot_to_mnxr = pd.DataFrame(columns=["uniprot_accession", "dr_source", "mnxr"])

    lanes = [
        ("kofam", read_kofam, kofam, ko_to_mnxr),
        ("ezpred", read_dl_ec, dl_ec, ec_to_mnxr),
        ("uniref50", read_uniref50, uniref50, uniprot_to_mnxr),
        ("pbert", read_embed_transfer, embed, None),
    ]
    extra = {"uniref50": {"descriptions": uniref50_descriptions}}
    frames = []
    for name, reader, path, bridge in lanes:
        if not path:
            continue
        f = reader(path, source, bridge, **extra.get(name, {}))
        print(f"[compile] {name}: {len(f):,} rows ({f['orf'].nunique():,} ORFs, {f['mnxr'].nunique():,} MNXRs)", flush=True)
        frames.append(f)
    if not frames:
        raise SystemExit("no lane inputs provided; nothing to compile")
    out_df = pd.concat(frames, ignore_index=True)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    write_parquet(out_df, out)
    print(f"[compile] wrote {len(out_df):,} rows -> {out}", flush=True)


# =====================================================================
# CLI
# =====================================================================

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build-uniprot-bridge", help="UniProt->MNXR via Rhea DR")
    b.add_argument("--reac-xref", type=Path, required=True)
    b.add_argument("--rhea-swiss", type=Path, required=True)
    b.add_argument("--rhea-trembl", type=Path, default=None)
    b.add_argument("--out", type=Path, required=True)

    e = sub.add_parser("build-ec-bridge", help="EC(level-4)->MNXR from reac_prop classifs")
    e.add_argument("--reac-prop", type=Path, required=True)
    e.add_argument("--out", type=Path, required=True)

    m = sub.add_parser("build-mnxr-lookup", help="ko+ec+uniprot bridges -> one mnxr_lookup")
    m.add_argument("--ko-to-mnxr", type=Path, required=True)
    m.add_argument("--ec-to-mnxr", type=Path, required=True)
    m.add_argument("--uniprot-to-mnxr", type=Path, required=True)
    m.add_argument("--out", type=Path, required=True)

    c = sub.add_parser("compile", help="fold lanes -> evidence_table.parquet")
    c.add_argument("--source", default="fosmid")
    c.add_argument("--kofam", type=Path, default=None)
    c.add_argument("--dl-ec", type=Path, default=None)
    c.add_argument("--uniref50", type=Path, default=None)
    c.add_argument("--uniref50-descriptions", type=Path, default=None)
    c.add_argument("--embed", type=Path, default=None)
    c.add_argument("--ko-to-mnxr", type=Path, default=None)
    c.add_argument("--ec-to-mnxr", type=Path, default=None)
    c.add_argument("--uniprot-to-mnxr", type=Path, default=None)
    c.add_argument("--out", type=Path, required=True)

    a = ap.parse_args()
    if a.cmd == "build-uniprot-bridge":
        build_uniprot_bridge(a.reac_xref, a.rhea_swiss, a.rhea_trembl, a.out)
    elif a.cmd == "build-ec-bridge":
        df = load_ec_to_mnxr(a.reac_prop)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(a.out, sep="\t", index=False)
        print(f"[bridge] wrote {len(df):,} ec->mnxr rows ({df['ec'].nunique():,} ECs) -> {a.out}", flush=True)
    elif a.cmd == "build-mnxr-lookup":
        build_mnxr_lookup(a.ko_to_mnxr, a.ec_to_mnxr, a.uniprot_to_mnxr, a.out)
    elif a.cmd == "compile":
        compile_evidence(a.source, a.kofam, a.dl_ec, a.uniref50, a.embed,
                         a.ko_to_mnxr, a.ec_to_mnxr, a.uniprot_to_mnxr, a.out,
                         uniref50_descriptions=a.uniref50_descriptions)


if __name__ == "__main__":
    main()

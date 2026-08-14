"""Every route from a benchmark entry to a set of MNXR reaction edges, and the layering.

WHAT AN ENTRY IS OWED. The benchmark exists to say, for each observation drawn from a
published study, which reactions that observation's genetic change touches. An entry with
no reactions is unscoreable: Y is derived by walking each entry's reactions to the
metabolites their atoms flow into, so no reactions means no expectations means no answer
key. Growing entry-to-reaction coverage is the only thing that grows the answer key.

WHY THIS MODULE EXISTS. The conditions lane reached one route -- gene label to UniProt
accession to the bridge -- and 203 of 845 entries. Three others were already on disk and
read by nothing:

  curated           the extraction's own `add_mnxr`/`del_mnxr` columns. Every one of
                    LASER's 382 rows carries at least one; Keio has 138 of 166. Five
                    hits for those column names across the whole tree, all in the
                    per-study builder.
  curated_override  `mapping_notes`, 129 rows of per-gene curator decisions in an
                    executable-looking form. A per-GENE reaction assignment inside a
                    per-OBSERVATION table, and the strictest evidence here.
  metacyc_rxn       LASER's own `Gene-Reaction Pairings.txt`, 3,370 rows keyed on
                    MetaCyc reaction frame ids. 97.7% of its rows join MetaNetX, at a
                    fanout of exactly one reaction per id.

LAYERED, CURATED WINS, AGREEMENT RECORDED. Routes are laid down in `ROUTES` order and a
lower route contributes only edges no higher route claimed for that entry. Independently
of that, EVERY route that produced a given edge is recorded in `agreed_by`, because the
cross-check is the accuracy evidence: an edge two routes reached by different paths is a
stronger claim than either alone, and precedence would otherwise throw that away.

Same shape as `aam_layers.stack`/`fuse_members` -- key on the fact being asserted, record
the agreeing members as a sorted `+`-joined string, restrict before the gate so the gate
is an assertion rather than a restatement. Not an import of it: that module's key is a
six-tuple over atom correspondences, and forcing an edge through it would mean inventing
atom indices for a claim that has none.

THE EC ROUTE IS GATED AT DEPTH FOUR, and that is a correctness gate rather than a tuning
knob. Measured on this bridge: a full four-level EC reaches a median of 3 reactions; a
depth-three key reaches a median of 5, a mean of 26 and as many as 658. A depth-three EC
is a claim about a class of chemistry, not about a reaction, and admitting it would trade
accuracy for a coverage number.

ACTIONS. The curated route's action is which column the id was written in, which is the
curator's own attribution and is authoritative. Every derived route takes the action from
the cohort row: `reg+` is an addition (a reaction the host already carries enters as a
parallel copy, never as an overwrite -- this project has hit that once already), `reg-`
and `del` are deletions, and the LOF and screen arms are uniformly deletions because a
knockout removes a route. `mut` alone is neither, and rather than pick one the raw token
string rides along in `action_raw` so a reader can see what was collapsed.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

# In precedence order, strongest first. A lower route contributes only edges no higher
# route claimed for that entry. The set is CLOSED: a route not named here cannot be
# layered, which is what stops an unlabelled edge from entering the table.
ROUTES = ("curated", "curated_override", "host_gem", "metacyc_rxn", "bridge_uniprot",
          "bridge_ec", "bridge_ko", "web")

EDGE_COLS = ("cohort", "condition_id", "gene_label", "mnxr", "action", "action_raw",
             "route", "route_key", "strength")

# The identity of one edge, AT THE GRAIN THE OBJECTIVE IS STATED IN: an entry and the
# reaction it touches. An observation that adds a reaction and one that deletes it are
# different claims about the same reaction, so `action` is part of the key rather than a
# property of it. This is what layering restricts on and what agreement is recorded over.
EDGE_KEY = ("cohort", "condition_id", "mnxr", "action")

# The identity of one ROW, which is finer, because a row is ONE NOMINATION MEMBER -- the
# `(protein, channel, intermediate_id, mnxr)` grain `bench_evidence_weights` normalises
# over. Two things legitimately produce several rows for one edge and neither is a
# duplicate: several genes of one observation naming the same reaction (frdABCD is one
# succinate dehydrogenase and all four subunits map to MNXR143783), and one gene carrying
# two ECs that reach it. The layering restriction is on EDGE_KEY; this is what must be
# unique.
ROW_KEY = ("cohort", "condition_id", "gene_label", "mnxr", "action", "route", "route_key")

_ADD_TOKENS = {"add", "reg+"}
_DEL_TOKENS = {"del", "reg-"}

# Cohorts whose action is a property of the arm rather than of the row. A Keio strain is
# a deletion; so is a screen hit. Neither table writes an action column worth reading.
_ARM_ACTION = {"lof": "del", "eydallin": "del"}

_EC4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")


# =====================================================================
# shared parsing -- one definition each
# =====================================================================

def split_ids(cell) -> list:
    """A comma/semicolon list cell -> ids. NaN is TRUTHY, so `or ""` is not enough."""
    if cell is None or (isinstance(cell, float) and cell != cell):
        return []
    return [x.strip() for x in str(cell).replace(";", ",").split(",") if x.strip()]


def decode_entities(s) -> str:
    """HTML entities out of a LASER label, BEFORE anything splits on a delimiter.

    `&alpha;` ENDS IN A SEMICOLON, which is also the separator `mapping_notes` uses
    between per-gene decisions. A label like `(+)-&alpha;-pinene synthase` therefore
    fragments into `(+)-&alpha` and `-pinene synthase:...` the instant it is split, and
    both halves look like ordinary malformed input rather than like one label. Decoding
    first is the only order that works, and it is also what makes the same label join
    across files that write it differently.
    """
    if s is None or (isinstance(s, float) and s != s):
        return ""
    t = str(s)
    for ent, ch in (("&alpha;", "alpha"), ("&beta;", "beta"), ("&gamma;", "gamma"),
                    ("&delta;", "delta"), ("&omega;", "omega"), ("&epsilon;", "epsilon"),
                    ("&prime;", "'"), ("&amp;", "&")):
        t = t.replace(ent, ch)
    return t


def norm_name(s) -> str:
    """A gene label or organism name, comparably: entity-decoded, collapsed, lowered."""
    return re.sub(r"\s+", " ", decode_entities(s).strip()).lower()


def norm_ec(tok) -> str | None:
    """`EC-2.3.1.86` / `2.3.1.86` -> `2.3.1.86`; anything not a full four-level EC -> None.

    THE PREFIX STRIP IS A REAL FIX, not tidying. LASER writes inline ECs with an `EC-`
    prefix and the resolver queried them verbatim, so every prefixed EC matched nothing
    and the miss was indistinguishable from a genuinely unknown enzyme.
    """
    if tok is None:
        return None
    t = str(tok).strip()
    if t.upper().startswith("EC-"):
        t = t[3:]
    if t.upper().startswith("EC:"):
        t = t[3:]
    t = t.strip()
    return t if _EC4.match(t) else None


def organism_match(a, b) -> bool:
    """Two organism strings naming the same species, tolerating the genus initial.

    LASER writes `R. communis` where MetaCyc writes `Ricinus communis`. Matching on the
    species epithet plus a compatible genus initial is the same test `curate_het_screen`
    makes; matching on the epithet alone would merge `E. coli` with `A. coli`.
    """
    a, b = norm_name(a), norm_name(b)
    if not a or not b:
        return False
    if a == b:
        return True
    ta, tb = a.replace(".", ". ").split(), b.replace(".", ". ").split()
    if len(ta) < 2 or len(tb) < 2 or ta[1] != tb[1]:
        return False
    return ta[0][0] == tb[0][0]


def canonical_action(cohort: str, action_raw) -> str:
    """add / del / mut, from the cohort and the row's action tokens."""
    if cohort in _ARM_ACTION:
        return _ARM_ACTION[cohort]
    toks = {t.strip() for t in str(action_raw or "").split(",") if t.strip()}
    if toks & _DEL_TOKENS and not toks & _ADD_TOKENS:
        return "del"
    if toks & _ADD_TOKENS:
        return "add"
    return "mut" if toks else "add"


def _frame(rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(EDGE_COLS))


# =====================================================================
# route: curated -- the reaction ids the extraction already carries
# =====================================================================

def curated_edges(extracts, entries: pd.DataFrame) -> pd.DataFrame:
    """`add_mnxr` / `del_mnxr` off the extractions, attached to every entry they name.

    PER OBSERVATION, NOT PER GENE, and the table says so by leaving `gene_label` null.
    The LASER extraction resolves reactions for the observation as a whole -- `genes_json`
    names each gene's action but no gene carries its own MNXR -- so splitting the list
    across the observation's genes would manufacture an attribution the curator never
    made. `curated_override` is where a per-gene curated claim lives, and it is the only
    place one exists.

    A LASER observation appears under BOTH gof_native and gof_het when it perturbs the
    host's own genes and adds a heterologous one, so its curated set attaches to both.
    That is duplication in the middle and it is deliberate: the reaction set belongs to
    the perturbation, and every entry drawn from that perturbation is owed it.
    """
    extracts = Path(extracts)
    per_obs = {}          # obs_id -> {action -> set(mnxr)}
    strength = {}         # obs_id -> mapping_provenance
    for study, cols in (("laser", (("add", "add_mnxr"), ("del", "del_mnxr"))),
                        ("keio", (("del", "del_mnxr"),))):
        src = extracts / study / "extraction.tsv"
        if not src.exists():
            raise SystemExit(
                f"[edges] curated route: {src} is absent.\n"
                f"  It is the study's extraction, and it carries the reaction ids the "
                f"benchmark was curated with.\n"
                f"  This is a NAMED refusal. Building without it would ship an edge "
                f"table that looks complete and is missing its strongest route.")
        df = pd.read_csv(src, sep="\t", dtype=str)
        for r in df.to_dict("records"):
            obs = str(r.get("obs_id") or "").strip()
            if not obs:
                continue
            for action, col in cols:
                ids = split_ids(r.get(col))
                if ids:
                    per_obs.setdefault(obs, {}).setdefault(action, set()).update(ids)
            prov = str(r.get("mapping_provenance") or "").strip()
            if prov:
                strength[obs] = prov

    rows = []
    for e in entries.itertuples(index=False):
        for action, ids in per_obs.get(e.condition_id, {}).items():
            for mnxr in ids:
                rows.append((e.cohort, e.condition_id, None, mnxr, action, action,
                             "curated", e.condition_id,
                             strength.get(e.condition_id, "curated_source_table")))
    return _frame(rows).drop_duplicates()


# =====================================================================
# route: curated_override -- mapping_notes, the per-gene curator decisions
# =====================================================================

# `gene:` is optional (`conflict(...)` carries no gene) and so are the parentheses
# (`species_fallback` is a bare provenance note). The gene group is non-greedy and the
# verb is anchored to the end, which is what lets an ARG CONTAIN A COLON --
# `conflict(MNXR190515:add/del->del)` parses as verb `conflict` rather than as a gene
# called `conflict(MNXR190515`.
_NOTE = re.compile(r"^(?:(?P<gene>.+?):)?(?P<verb>[a-z_]+)(?:\((?P<arg>.*)\))?$")

# The curator's whole vocabulary, and which part of it asserts a reaction.
#
#   override_select_mnxr  a reaction, per gene. The strictest evidence in the tree.
#   override_select_ec    an EC, per gene -- a key for the EC route, not an edge, so it
#                         enters at that route's strength rather than at this one's.
#   override_reject       a resolution was wrong. Asserts nothing new about which
#                         reaction the gene IS.
#   override_confirm      the union already computed is right. Approval of an inference
#                         is not the curator's own assignment and must not be read as one.
#   conflict              two actions disagree about the same reaction; the arg records
#                         how it was settled, and the settled value is already in the
#                         add_mnxr / del_mnxr columns.
#   species_fallback      the organism restriction was relaxed to resolve the gene.
#   gated_by_record_ec    the record's own EC was used to gate the resolution.
_NOTE_VERBS = {"override_select_mnxr", "override_select_ec", "override_reject",
               "override_confirm", "conflict", "species_fallback", "gated_by_record_ec"}


def parse_mapping_notes(cell) -> list:
    """`gene:verb(arg); gene:verb(arg)` -> [(gene, verb, arg)], REFUSING on a malformed one.

    A note that fails to parse looks exactly like an observation that had none, so a
    silent skip here would be invisible attrition dressed as an absence. An unknown verb
    refuses for the same reason: the curator's vocabulary growing is a thing to be taught,
    not a thing to shrug at.
    """
    text = decode_entities(cell)
    out = []
    for chunk in (c.strip() for c in text.split(";")):
        if not chunk:
            continue
        m = _NOTE.match(chunk)
        if not m:
            raise SystemExit(
                f"[edges] mapping_notes: cannot parse {chunk!r}.\n"
                f"  Expected `[gene:]verb[(arg)]`. A note this parser skips is "
                f"indistinguishable from an observation that carried none, which is why "
                f"this refuses instead.")
        verb, arg = m.group("verb"), (m.group("arg") or "").strip()
        if verb not in _NOTE_VERBS:
            raise SystemExit(
                f"[edges] mapping_notes: unknown verb {verb!r} in {chunk!r}. The curator's "
                f"vocabulary has grown; teach this parser rather than let it drop the note.")
        out.append(((m.group("gene") or "").strip(), verb, arg))
    return out


def override_edges(extracts, entries: pd.DataFrame) -> tuple:
    """(edges, ec_overrides). `override_select_mnxr` is an edge; `override_select_ec` is
    a key for the EC route, carried out separately so it enters at that route's strength
    rather than at this one's.

    `override_reject` and `override_confirm` assert nothing new about which reaction a
    gene is: reject says a resolution was wrong, confirm says the union already computed
    is right. Neither produces an edge, and reading them as one would put a curator's
    approval of an inference into the layer reserved for their own assignments.
    """
    src = Path(extracts) / "laser" / "extraction.tsv"
    df = pd.read_csv(src, sep="\t", dtype=str)
    by_obs = {}
    ec_over = {}
    for r in df.to_dict("records"):
        obs = str(r.get("obs_id") or "").strip()
        for gene, verb, arg in parse_mapping_notes(r.get("mapping_notes")):
            if verb == "override_select_mnxr":
                # The curator separates a two-reaction override with a SLASH
                # (`MNXR113188/MNXR196253`), which `split_ids` does not treat as a
                # separator anywhere else -- `conflict(MNXR190515:add/del->del)` uses one
                # to mean "and", not "or". Handled here, where the argument's shape is
                # known, rather than by teaching the shared splitter a third delimiter.
                for mnxr in split_ids(arg.replace("/", ",")):
                    by_obs.setdefault(obs, []).append((norm_name(gene), mnxr))
            elif verb == "override_select_ec":
                ec = norm_ec(arg)
                if ec:
                    ec_over.setdefault((obs, norm_name(gene)), set()).add(ec)

    rows = []
    for e in entries.itertuples(index=False):
        for gene, mnxr in by_obs.get(e.condition_id, ()):
            # The override names a gene; it belongs to the entry that carries that gene,
            # and to no other. An override whose gene is in the other cohort's half of
            # the observation must not leak across.
            if gene and norm_name(e.gene_label) != gene:
                continue
            act = canonical_action(e.cohort, e.action)
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                         "curated_override", f"{e.condition_id}:{gene}", "override"))
    return _frame(rows).drop_duplicates(), ec_over


# =====================================================================
# route: host_gem -- a native gene resolved against the host's own curated model
# =====================================================================

def host_gem_edges(gpr_gem, entries: pd.DataFrame) -> pd.DataFrame:
    """A NATIVE gene symbol (or b-number) -> the reactions the host's GEM gives it.

    This is what `lanes="host_gpr"` always promised and never delivered: the native, LOF
    and screen arms were emitted with a null reaction and the note that their edges are
    the host's, resolved at network construction. That is a defensible routing decision
    for a network builder and a useless one for an ANSWER KEY, because Y is derived by
    walking an entry's reactions and an entry with none produces no expectations. The
    Eydallin arm's expectation table is empty for exactly this reason.

    ONLY FOR NATIVE GENES. A heterologous protein has no entry in the host's model, and a
    symbol collision -- the host happening to carry a gene of the same name -- would
    attribute E. coli's chemistry to a foreign enzyme. The source-organism test is the
    same one the cohort loader splits the GOF arm on.

    THE HOST IS DECLARED, NOT INFERRED. Every study in this lane is pinned to
    `e_coli_k12` by the per-study builder, and the `host_hint` column carries strain names
    (MG1655, BW25113, BL21(DE3)) that no host directory is named after. Reading the model
    off a strain string would silently give two observations of the same study different
    background chemistry.
    """
    g = gpr_gem if isinstance(gpr_gem, pd.DataFrame) else pd.read_parquet(gpr_gem)
    by_sym = {}
    for fid, fname, mnxr in zip(g["feature_id"], g["feature_name"], g["mnxr"]):
        if not isinstance(mnxr, str) or not mnxr:
            continue
        for k in (fid, fname):
            if isinstance(k, str) and k.strip():
                by_sym.setdefault(norm_name(k), set()).add(mnxr)

    rows = []
    for e in entries.itertuples(index=False):
        if not _native(e.source_organism):
            continue
        key = norm_name(e.gene_label)
        for mnxr in sorted(by_sym.get(key, ())):
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr,
                         canonical_action(e.cohort, e.action), e.action,
                         "host_gem", key, "gem_gpr"))
    return _frame(rows).drop_duplicates()


def _native(source) -> bool:
    """Blank / `None` / anything *coli* is the host's own genome.

    The same test `bench_cohorts.is_native` makes, restated rather than imported so this
    module does not require the cohort loader to be importable; the two must agree, and
    the cost of them not agreeing is a foreign protein resolved against E. coli.
    """
    s = str(source or "").strip().lower()
    return s in ("", "none", "n/a", "na", "null", "not specified", "-") or "coli" in s


# =====================================================================
# route: metacyc_rxn -- LASER's own gene-to-reaction table
# =====================================================================

def load_pairings(pairings_path) -> pd.DataFrame:
    p = Path(pairings_path)
    if not p.exists():
        raise SystemExit(
            f"[edges] metacyc route: {p} is absent.\n"
            f"  It is LASER's `Gene-Reaction Pairings.txt`, 3,370 rows of gene, species "
            f"and MetaCyc reaction frame id.\n"
            f"  This is a NAMED refusal.")
    df = pd.read_csv(p, sep="\t", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    need = {"Gene Name", "Species", "Unique ID", "Discovered EC Number"}
    missing = need - set(df.columns)
    if missing:
        raise SystemExit(f"[edges] {p}: columns {sorted(missing)} are absent -- the "
                         f"upstream table's layout has changed")
    return df


def metacyc_edges(pairings: pd.DataFrame, metacyc_map: dict,
                  entries: pd.DataFrame) -> pd.DataFrame:
    """(gene, species) -> MetaCyc frame id -> MNXR.

    JOINED ON GENE AND SPECIES, NOT ON GENE ALONE. The file carries 178 species and a
    symbol like `adh` names different reactions in different organisms; joining on the
    label alone reaches ~4% more rows and attributes one organism's chemistry to
    another's condition. A native gene has no source organism of its own -- it is the
    host's -- so it is matched against E. coli.
    """
    by_gs = {}
    for gene, sp, uid in zip(pairings["Gene Name"], pairings["Species"],
                             pairings["Unique ID"]):
        mnxrs = metacyc_map.get(str(uid).strip())
        if mnxrs:
            by_gs.setdefault(norm_name(gene), []).append((norm_name(sp), set(mnxrs)))

    rows = []
    for e in entries.itertuples(index=False):
        cands = by_gs.get(norm_name(e.gene_label))
        if not cands:
            continue
        want = e.source_organism or "Escherichia coli"
        act = canonical_action(e.cohort, e.action)
        # KEYED ON WHAT WAS MATCHED, NOT ON WHICH SPELLING WON. The pairings file writes
        # the same species both ways -- `L. lactis` and `Lactococcus lactis` are 3,370
        # rows apart and both real -- so keying on the file's string makes one gene into
        # two nominations, splits its belief mass in half and emits every edge twice.
        key = norm_name(e.gene_label) + "@" + norm_name(want)
        for sp, mnxrs in cands:
            if not organism_match(sp, want):
                continue
            for mnxr in mnxrs:
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "metacyc_rxn", key, "metacyc_frame"))
    return _frame(rows).drop_duplicates()


# =====================================================================
# route: the bridge -- accession, EC, KO
# =====================================================================

def bridge_slice(bridge_path, id_source: str) -> dict:
    """`ref::mnxr_lookup` restricted to one id space, as `{id -> set(mnxr)}`."""
    b = pd.read_parquet(bridge_path, columns=["id", "id_source", "mnxr"])
    b = b[b["id_source"] == id_source]
    return b.groupby("id")["mnxr"].apply(set).to_dict()


def uniprot_edges(up_map: dict, entries: pd.DataFrame, web_accessions=(),
                  native_accessions=None) -> pd.DataFrame:
    """Accession -> MNXR. Accessions recovered from the literature lane are tagged `web`.

    The tier is distinct on purpose: an accession a curator's query returned and one a
    paper's methods section named are both accessions, but only the second is a reading
    of prose, and a reader has to be able to tell which they are looking at.

    `native_accessions` covers the other direction: a host gene the host's own model does
    not carry has no accession in the cohort frame, because only the heterologous arm ever
    had one. Those are looked up by symbol and used ONLY where the row is native and has
    no accession of its own -- a heterologous protein resolved by symbol against E. coli
    would be the exact organism confusion the source-organism test exists to prevent.
    """
    web = {str(a).strip() for a in web_accessions if str(a).strip()}
    native = native_accessions or {}
    rows = []
    for e in entries.itertuples(index=False):
        acc = e.uniprot
        if acc is None or (isinstance(acc, float) and acc != acc):
            if not (_native(e.source_organism) and native):
                continue
            acc = native.get(norm_name(e.gene_label))
            if not acc:
                continue
        mnxrs = up_map.get(str(acc).strip())
        if not mnxrs:
            continue
        route = "web" if str(acc).strip() in web else "bridge_uniprot"
        act = canonical_action(e.cohort, e.action)
        for mnxr in mnxrs:
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                         route, str(acc).strip(), "bridge"))
    return _frame(rows).drop_duplicates()


def ec_index(extracts, pairings: pd.DataFrame, het_path, ec_overrides=None) -> dict:
    """`{normalised gene label -> set(four-level EC)}`, from every place one is free.

    Four sources, none of which costs a query: the inline `ec` field on the extraction's
    gene records, Keio's own `EC` column (populated on all 166 of its rows), the pairings
    file's `Discovered EC Number` column, and the het curation's `reason` strings, which
    encode the EC that produced an accession for 244 of its 456 rows. Partial ECs are
    dropped by `norm_ec` rather than carried -- see the depth-four gate in the module
    docstring.
    """
    out = {}

    def add(gene, tok):
        ec = norm_ec(tok)
        if ec:
            out.setdefault(norm_name(gene), set()).add(ec)

    L = pd.read_csv(Path(extracts) / "laser" / "extraction.tsv", sep="\t", dtype=str)
    for cell in L["genes_json"].fillna(""):
        if not str(cell).strip():
            continue
        for g in json.loads(cell):
            for tok in str(g.get("ec") or "").split():
                add(g.get("gene"), tok)

    K = pd.read_csv(Path(extracts) / "keio" / "extraction.tsv", sep="\t", dtype=str)
    for gene, ecs in zip(K["gene_set"].fillna(""), K["EC"].fillna("")):
        # Keio writes the action into the gene field (`lysC:del`) and separates its ECs
        # with semicolons.
        for tok in str(ecs).replace(",", ";").split(";"):
            add(str(gene).split(":")[0], tok)

    for gene, ecs in zip(pairings["Gene Name"], pairings["Discovered EC Number"]):
        for tok in str(ecs).split():
            add(gene, tok)

    het = Path(het_path) / "heterologous_uniprot.tsv"
    if het.exists():
        H = pd.read_csv(het, sep="\t", dtype=str).fillna("")
        for gene, reason in zip(H["gene"], H["reason"]):
            if ":" in reason and reason.split(":", 1)[0].startswith("ec"):
                add(gene, reason.split(":", 1)[1])

    for (_obs, gene), ecs in (ec_overrides or {}).items():
        for ec in ecs:
            add(gene, ec)
    return out


def ec_edges(ec_map: dict, gene_ecs: dict, entries: pd.DataFrame) -> pd.DataFrame:
    """EC -> MNXR, over four-level keys only. See the module docstring for the gate."""
    rows = []
    for e in entries.itertuples(index=False):
        act = canonical_action(e.cohort, e.action)
        for ec in sorted(gene_ecs.get(norm_name(e.gene_label), ())):
            for mnxr in ec_map.get(ec, ()):
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "bridge_ec", ec, "bridge"))
    return _frame(rows).drop_duplicates()


def ko_edges(ko_map: dict, gene_kos: dict, entries: pd.DataFrame) -> pd.DataFrame:
    """KO -> MNXR. Declared and empty on this benchmark, which is a fact worth printing.

    NOTHING IN THE BENCHMARK'S INPUTS CARRIES A KO. The extractions do not, LASER's
    tables do not, and the het curation does not. The route exists so the closed route
    vocabulary matches the bridge's id spaces and so a future extraction that does carry
    KOs is wired rather than invented; it is reported as zero rather than omitted,
    because an absent route and an empty one read identically in a summary otherwise.
    """
    rows = []
    for e in entries.itertuples(index=False):
        act = canonical_action(e.cohort, e.action)
        for ko in sorted(gene_kos.get(norm_name(e.gene_label), ())):
            for mnxr in ko_map.get(ko, ()):
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "bridge_ko", ko, "bridge"))
    return _frame(rows).drop_duplicates()


# =====================================================================
# the layering
# =====================================================================

def layer(frames: dict, universe=None) -> tuple:
    """Lay the routes down in ROUTES order; return (edges, report).

    A lower route contributes only edges whose `EDGE_KEY` no higher route claimed. That
    restriction is what "curated wins" means. It happens BEFORE the agreement column is
    written, and the agreement is computed over what every route OFFERED rather than over
    what survived -- otherwise precedence would erase exactly the corroboration the
    column exists to record.

    `universe` (optional) is the set of reactions MetaNetX knows. Edges naming a reaction
    outside it are dropped and counted PER ROUTE, never silently: a curated id that
    MetaNetX has retired is a fact about the curation and is the kind of thing that
    otherwise shows up months later as an entry that scores zero for no visible reason.
    """
    offered = {}
    report = []
    for route in ROUTES:
        df = frames.get(route)
        if df is None or not len(df):
            report.append(dict(route=route, offered=0, out_of_universe=0,
                               out_of_universe_ids=[], added=0, entries=0, mnxr=0))
            offered[route] = _frame([])
            continue
        n0 = len(df)
        gone = []
        if universe is not None:
            bad = ~df["mnxr"].isin(universe)
            gone = sorted(set(df.loc[bad, "mnxr"]))
            df = df[~bad]
        offered[route] = df
        report.append(dict(route=route, offered=n0, out_of_universe=n0 - len(df),
                           out_of_universe_ids=gone, added=0, entries=0, mnxr=0))

    # agreement over what every route OFFERED, computed once
    agreed = {}
    for route in ROUTES:
        df = offered[route]
        if not len(df):
            continue
        for k in zip(*(df[c] for c in EDGE_KEY)):
            agreed.setdefault(k, set()).add(route)

    claimed = set()
    kept = []
    for route, rec in zip(ROUTES, report):
        df = offered[route]
        if not len(df):
            continue
        keys = list(zip(*(df[c] for c in EDGE_KEY)))
        mask = [k not in claimed for k in keys]
        add = df[pd.Series(mask, index=df.index)]
        claimed.update(k for k, m in zip(keys, mask) if m)
        rec["added"] = len(add)
        if len(add):
            rec["entries"] = int(add[["cohort", "condition_id"]].drop_duplicates().shape[0])
            rec["mnxr"] = int(add["mnxr"].nunique())
            kept.append(add)

    out = (pd.concat(kept, ignore_index=True) if kept
           else _frame([]))
    if len(out):
        out["agreed_by"] = ["+".join(sorted(agreed.get(k, {r})))
                            for k, r in zip(zip(*(out[c] for c in EDGE_KEY)),
                                            out["route"])]
        out["n_agree"] = out["agreed_by"].str.count(r"\+") + 1

        # THE ENTRY'S EDGE SET IS THE BEST ROUTE THAT REACHED IT, NOT THE UNION.
        #
        # Edge-level layering stops one edge being supplied twice; it does not stop a
        # weak route bulking out an entry a strong one already answered. Measured: the
        # curated route reaches 731 of 845 entries on its own and the EC route adds 7 --
        # while contributing 90% of the rows. Unioning them would take an observation
        # whose curator wrote down 5 reactions and hand back 60, most of them inferred
        # from an EC's promiscuity. That is more recall and less accuracy, and accuracy
        # is what an answer key is for.
        #
        # Nothing is discarded. Every route's edges stay in the table with their own
        # `route` and their `agreed_by`, which is what makes corroboration readable; the
        # authoritative set is one filter away and unambiguous.
        rank = {r: i for i, r in enumerate(ROUTES)}
        entry_top = (out.assign(_r=out["route"].map(rank))
                        .groupby(["cohort", "condition_id"])["_r"].min())
        out["in_entry_set"] = [
            rank[r] == entry_top.get((c, i), -1)
            for r, c, i in zip(out["route"], out["cohort"], out["condition_id"])]
    else:
        out["agreed_by"] = pd.Series(dtype=str)
        out["n_agree"] = pd.Series(dtype=int)
        out["in_entry_set"] = pd.Series(dtype=bool)

    # Two claims, asserted rather than assumed.
    if len(out):
        dup = out[out.duplicated(subset=list(ROW_KEY), keep=False)]
        if len(dup):
            raise SystemExit(
                f"[edges] {len(dup):,} rows share a row key -- the same route is "
                f"asserting the same gene-to-reaction edge twice. "
                f"First few:\n{dup.head(5).to_string()}")
        by_edge = out.groupby(list(EDGE_KEY))["route"].nunique()
        if (by_edge > 1).any():
            bad = by_edge[by_edge > 1]
            raise SystemExit(
                f"[edges] {len(bad):,} edges are supplied by more than one route -- the "
                f"layering restriction did not hold. First few:\n{bad.head(5)}")
        prim = (out[out["in_entry_set"]].groupby("route")[["cohort", "condition_id"]]
                .apply(lambda d: d.drop_duplicates().shape[0]).to_dict())
        for rec in report:
            rec["primary_entries"] = prim.get(rec["route"], 0)
    return out.reset_index(drop=True), report


def report_lines(report: list, tag: str = "edges") -> list:
    lines = []
    for r in report:
        lines.append(f"[{tag}] {r['route']:<18} offered {r['offered']:>7,}  "
                     f"added {r['added']:>7,}  {r['entries']:>4,} entries  "
                     f"{r['mnxr']:>5,} reactions"
                     + (f"  primary for {r['primary_entries']:>4,}"
                        if r.get("primary_entries") is not None else ""))
        # NAMED, not counted. A reaction id MetaNetX has retired is a fact about the
        # curation and the only place it can be noticed is here -- downstream it is
        # simply an entry that scores lower than it should for no visible reason.
        if r["out_of_universe"]:
            ids = r["out_of_universe_ids"]
            shown = ids[:8]
            more = f" and {len(ids) - len(shown):,} more" if len(ids) > len(shown) else ""
            lines.append(f"[{tag}]   {r['out_of_universe']:,} of those name a reaction "
                         f"MetaNetX does not define and are DROPPED: {shown}{more}")
    return lines

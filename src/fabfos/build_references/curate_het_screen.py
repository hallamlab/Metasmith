"""The benchmark's gene labels -> UniProt accessions and sequences.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python build_references/curate_het_screen.py                 # dry run
    PATH="..." python build_references/curate_het_screen.py --network
    PATH="..." python build_references/curate_het_screen.py --network --publish

Writes `data/originals/benchmarks/het_screen/heterologous_uniprot.tsv`, which is the
`raw::het_screen_records` two benchmark steps require and nothing has ever produced,
and `native_uniprot.tsv` beside it.

TWO HALVES, ONE PASS, BECAUSE THEY ARE THE SAME QUESTION ASKED OF DIFFERENT ORGANISMS.
The heterologous half is the bulk and is what the type is named after. The native half
is the benchmark's own host genes that the host's curated model does not carry -- 53 of
them, all from the Eydallin screen, and every one is an entry with no reaction at all
until it has an accession. A regulator genuinely has none and is written out by name;
about a quarter of them turn out to be enzymes the GEM simply omits.

WHAT THIS TABLE IS, because the tier's PROVENANCE recorded the question and left it
open. It is not a peer study. It is LASER's own heterologous slice resolved to
accessions: 456 of the 467 rows in the deployed table are exactly LASER `(gene, source)`
pairs carrying an `add` action, and the 11 that are not are spelling and action variants
of LASER rows, not a separate screen. So this is a LOOKUP keyed on LASER's key, and
`bench_cohorts.load_all_cohorts` must join it rather than concatenate it -- concatenating
counts 456 proteins twice, which is the double-count that PROVENANCE warned about.

THE FREE-TEXT NAMES ARE RESOLVABLE, and the earlier verdict that they were not came from
reading the wrong column. LASER's `gene_set` packs `label:action` and drops everything
else; `genes_json` carries the same labels WITH their source organism, and every one of
the 204 free-text labels has one. A name plus an organism is a lookup, not a guess. On
top of that, 118 of the 204 carry an EC -- 27 inline in `genes_json` and 97 more from
LASER's own `Gene-Reaction Pairings.txt`, which is already pinned under
`originals/benchmarks/laser/inputs/`.

TIERS, WEAKEST LAST, AND THE TABLE SAYS WHICH ANSWERED. An EC restricted to the named
organism is a near-identification; a cross-reference to the accession the paper cited is
one outright; a gene symbol or a name restricted to the organism is strong; a free-text
term search is taken only when the hit's own name overlaps the label. A name with no
organism left to restrict it is a guess and is recorded as such rather than taken. What
no tier answers is written out by name with the reason, which is the same discipline
`study_sequences.py` applies to non-coding genes.

NO TIER MAY DROP THE ORGANISM RESTRICTION while an organism is known. Simulated over the
76 unresolved gene-shaped tokens, an unrestricted symbol query "resolves" 66 of them --
including a rat, two humans, a mouse, a nematode, a fruit fly, barley and potato. The
alternative symbol `BTE` alone returns a Brevibacillus protein for a plant thioesterase.
A wrong protein is worse than no protein, because nothing downstream can tell.

THE RESUME CACHE IS VERSIONED, and that is not bookkeeping. It stores MISSES as well as
hits -- 129 of 456 lines -- so adding a tier and re-running skips every row the old
ladder failed on, which is precisely the set the new tier exists for. Deleting the cache
instead throws away 327 good answers and an hour of somebody else's rate limit. Each line
carries the ladder version that produced it; a miss from an older ladder is re-queued and
a hit is kept.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
BENCH = REPO / "data" / "fabfos" / "benchmarks"
LASER_ROOT = REPO / "data" / "fabfos" / "originals" / "benchmarks" / "laser"
LASER_INPUTS = LASER_ROOT / "inputs"
PUBLISH_AT = REPO / "data" / "fabfos" / "originals" / "benchmarks" / "het_screen"
OUT_ROOT = REPO / "data" / "fabfos" / "scratch" / "het_screen"

# Hand-authored, git text, beside the code that reads it. `curated/` is gone as a data
# tier (REFERENCES.md) and anything hand-authored that comes back has to come back as
# git-tracked text under a tier that admits it has no producer -- which for a file this
# small is the source tree.
WEB_ACCEPT = Path(__file__).resolve().parent / "het_web_accessions.tsv"

# The label normalisation the EDGE builder uses, imported rather than restated: a label
# these two disagree about resolves to an accession under one spelling and joins to
# nothing under the other.
sys.path.insert(0, str(Path(__file__).resolve().parent / "resources" / "buildlib"))
from bench_edges import decode_entities, norm_ec              # noqa: E402
from bench_cohorts import is_native                           # noqa: E402

UNIPROT = "https://rest.uniprot.org/uniprotkb/search"

# The deployed schema, unchanged: `bench_cohorts.load_het` reads `gene`, `source` and
# `uniprot`, and `study_sequences.py` reads `seq`. Widening it would make this table a
# new artifact rather than the one the benchmark already expects.
COLUMNS = ("gene", "source", "n_obs", "actions", "uniprot", "organism_resolved",
           "organism_match", "protein_name", "aa_len", "reviewed", "reason", "seq")

# A gene symbol, same test `study_sequences.classify` uses -- and it must stay the same
# test, or a label is a symbol to one half of the build and a description to the other.
_SYMBOL = re.compile(r"^(b\d{4}|[a-z]{2,5}[A-Z]{0,2}\d{0,2})$")

# LASER GeneSource strings a UniProt organism query cannot take literally. Hand-authored,
# and IN CODE rather than in a data file so a change to it shows up in a diff beside the
# code that reads it -- the same argument acquire/genomes.py makes for the host set.
#
# An empty value means the source is not an organism at all; every label carrying one is
# written out unresolved with that note rather than searched against nothing.
ORGANISM_ALIASES = {
    # whitespace lost upstream
    "Bsubtilis": "Bacillus subtilis",
    "Saureus": "Staphylococcus aureus",
    "Koxytoca": "Klebsiella oxytoca",
    "Pcrispum": "Petroselinum crispum",
    # a common name
    "Pig": "Sus scrofa",
    # not an organism
    "HumanDesigned": "",
    "Designed": "",
    # typos, genus or epithet
    "Abies grandes": "Abies grandis",
    "Abies grandi": "Abies grandis",
    "Arabdopsis thaliana": "Arabidopsis thaliana",
    "Aradopsis thaliana": "Arabidopsis thaliana",
    "Clostridia acetobutylicum": "Clostridium acetobutylicum",
    "Enterococcus faecilis": "Enterococcus faecalis",
    "Haemophilus influenza": "Haemophilus influenzae",
    "Nocardia farcinia": "Nocardia farcinica",
    "Terponema denticola DSM14222": "Treponema denticola",
    "Petroselinum crispus": "Petroselinum crispum",
    "Salmonella arizona": "Salmonella enterica",
    # a wrong genus, not a typo: aat is Bacteroides
    "Bacillus fragilis": "Bacteroides fragilis",
    # reclassified since the paper
    "Ralstonia eutropha": "Cupriavidus necator",
    "Ralstonia eutrophus": "Cupriavidus necator",
    "Erwinia carotovora": "Pectobacterium carotovorum",
    "Erwinia herbicola": "Pantoea agglomerans",
}

# `G. biloba`, `R. communis` -- an abbreviated binomial, which is the common shape and is
# handled generically rather than by 60 more alias rows: query the epithet and require the
# returned organism's genus initial to match.
_ABBREV = re.compile(r"^([A-Z])\.\s+([a-z][a-z-]+)$")


def normalise_organism(src: str) -> tuple[str, str]:
    """`(query_organism, how)` for a LASER GeneSource string.

    `query_organism` empty means do not search: either the source is blank, or it names
    something that is not an organism. `how` is carried into the table so a reader can
    see whether a binomial was taken as written, corrected, or expanded from an
    abbreviation.
    """
    s = (src or "").strip()
    if not s:
        return "", "no_source"
    if s in ORGANISM_ALIASES:
        v = ORGANISM_ALIASES[s]
        return v, ("alias" if v else "not_an_organism")
    m = _ABBREV.match(s)
    if m:
        return m.group(2), "abbreviated_binomial"
    return s, "as_written"


def genus_initial_ok(src: str, resolved: str) -> bool:
    """For an abbreviated binomial, does the hit's genus start with the right letter?

    `R. communis` matching `Ricinus communis` is the answer; matching `Bacillus communis`
    would not be, and an epithet-only query cannot tell the two apart on its own.
    """
    m = _ABBREV.match((src or "").strip())
    if not m or not resolved:
        return True
    return resolved[:1].upper() == m.group(1).upper()


# ---------------------------------------------------------------------------
# the sources
# ---------------------------------------------------------------------------

def laser_labels() -> dict:
    """Every heterologous label LASER names, with its source organisms and inline ECs.

    Read from `genes_json`, not `gene_set`. `gene_set` is `label:action` and nothing
    else, which is why the source organism looked absent: it was never in the column
    being read.
    """
    import pandas as pd
    d = pd.read_csv(BENCH / "laser" / "extraction.tsv", sep="\t", dtype=str)
    out: dict = {}
    for i, v in d["genes_json"].fillna("").items():
        if not v.strip():
            continue
        for g in json.loads(v):
            name = (g.get("gene") or "").strip()
            if not name:
                continue
            actions = [a for a in (g.get("actions") or []) if a]
            src = (g.get("source") or "").strip()
            # HETEROLOGOUS IS A SOURCE ORGANISM, NOT AN ACTION. The test used to be
            # `add in actions`, on the reasoning that a label LASER only overexpresses is
            # a host gene under a description. That is true of a label with no source and
            # false of one that names another organism: a heterologous part can be
            # deleted or mutated in a later round, and eight labels -- with their own
            # organisms -- were invisible to this pass for exactly that reason. The
            # source-organism test is the one `bench_cohorts` splits the GOF arm on, so
            # the two halves of the build now agree about what heterologous means.
            if is_native(src):
                continue
            rec = out.setdefault(name, dict(
                gene=name, sources={}, actions=set(), n_obs=0, ecs=set()))
            rec["n_obs"] += 1
            rec["actions"].update(actions)
            rec["sources"][src] = rec["sources"].get(src, 0) + 1
            # NORMALISED, not stored raw. LASER writes inline ECs with an `EC-` prefix
            # and this stored them verbatim, so `ec:EC-1.1.1.35` matched nothing and the
            # miss was indistinguishable from an unknown enzyme.
            for tok in str(g.get("ec") or "").split():
                ec = norm_ec(tok)
                if ec:
                    rec["ecs"].add(ec)
    return out


def record_nicknames() -> dict:
    """`label -> {alternative symbol, ...}` from LASER's own record store.

    `GeneNickname` is on all 2,636 mutation records and was dropped when the records were
    projected into `genes_json`. It is LASER's own alternative name for the part -- `BTE`
    for a Bay Laurel thioesterase -- and it resolves nine labels the description does not,
    four of them reachable no other way.

    USED ONLY INSIDE AN ORGANISM-RESTRICTED TIER. `BTE` unrestricted returns a
    Brevibacillus protein, which is the whole reason the nickname is an alias and not an
    identifier.
    """
    out: dict = {}
    pat = re.compile(r'^(Mutant\d+\.Mutation\d+)\.(GeneName|GeneNickname)="(.*)"\s*$')
    for path in sorted(LASER_ROOT.glob("database_store/*/Record*.txt")):
        per: dict = {}
        for line in path.read_text(errors="replace").splitlines():
            m = pat.match(line.strip())
            if m:
                per.setdefault(m.group(1), {})[m.group(2)] = m.group(3).strip()
        for fields in per.values():
            name, nick = fields.get("GeneName", ""), fields.get("GeneNickname", "")
            if name and nick and nick.lower() != name.lower():
                out.setdefault(name, set()).add(nick)
    return out


def web_accessions() -> dict:
    """`(gene, source) -> row` from the hand-authored literature accept list.

    AUTO-ACCEPTED, UNDER ITS OWN TIER. A label a query cannot reach and a paper's methods
    section names outright is still an identification, and holding it behind a human
    accept step just means it never lands. What makes that safe is that it is recorded
    differently: `reason` is `web:<doi>`, the edge builder routes it as `web` rather than
    `bridge_uniprot`, and the file carries the DOI, the URL, the verbatim quote and the
    date it was read, so the claim is checkable rather than merely present.
    """
    if not WEB_ACCEPT.exists():
        return {}
    out = {}
    with WEB_ACCEPT.open() as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r.get("gene", "").startswith("#") or not r.get("uniprot", "").strip():
                continue
            missing = [k for k in ("doi", "url", "quote", "retrieved")
                       if not (r.get(k) or "").strip()]
            if missing:
                raise SystemExit(
                    f"{WEB_ACCEPT.name}: {r['gene']}/{r.get('source')} is missing "
                    f"{missing}. A literature accession without its source is a bare "
                    f"assertion; the columns are what make it auditable, so this refuses.")
            out[(r["gene"].strip(), (r.get("source") or "").strip())] = r
    return out


def pairings_ec() -> dict:
    """`label -> {EC, ...}` from LASER's own Gene-Reaction Pairings.

    LASER wrote these ECs itself, against the same labels it wrote into the records, so
    this is the database's own answer rather than a third-party guess -- which is what
    makes it usable without a curator. Case-folded, because the records and the pairings
    do not agree on capitalisation.
    """
    path = LASER_INPUTS / "Gene-Reaction Pairings.txt"
    out: dict = {}
    with open(path, errors="replace") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            name = (r.get("Gene Name") or "").strip()
            ecs = (r.get("Discovered EC Number") or "").strip()
            if not name or not ecs:
                continue
            # `norm_ec` strips the `EC-` prefix and rejects anything that is not a full
            # four-level EC. The old form kept only tokens that HAD the prefix and kept
            # them with it, which is two bugs pointing opposite ways.
            for tok in ecs.split():
                ec = norm_ec(tok)
                if ec:
                    out.setdefault(name.lower(), set()).add(ec)
    return out


def native_labels() -> dict:
    """The benchmark's NATIVE gene labels the host's curated model does not carry.

    Every entry in the LOF and screen arms names a host gene, and its reactions were
    always "the host's, resolved at network construction". That is a routing decision for
    a network builder and a dead end for an answer key: Y is derived by walking an entry's
    reactions, so a gene the GEM omits produces no expectations at all. 53 Eydallin genes
    sit in exactly that state.

    Restricted to what the GEM MISSES, deliberately. A gene the model already carries has
    a curated answer from a published model, which is stronger than anything a query
    returns, and asking about it would only add a second opinion nothing needs.
    """
    import pandas as pd
    gem = pd.read_parquet(BENCH / "hosts" / "e_coli_k12" / "gpr_gem.parquet",
                          columns=["feature_id", "feature_name"])
    have = {str(x).strip().lower()
            for col in ("feature_id", "feature_name") for x in gem[col] if str(x).strip()}

    out: dict = {}
    def add(name, obs):
        name = (name or "").strip()
        if not name or name.lower() in have:
            return
        rec = out.setdefault(name, dict(gene=name, sources={"Escherichia coli": 0},
                                        actions={"del"}, n_obs=0, ecs=set()))
        rec["n_obs"] += 1
        rec["sources"]["Escherichia coli"] += 1

    e = pd.read_csv(BENCH / "eydallin" / "extraction.tsv", sep="\t", dtype=str).fillna("")
    for r in e.to_dict("records"):
        add(r.get("gene_norm") or r.get("gene"), r.get("gene"))
    k = pd.read_csv(BENCH / "keio" / "extraction.tsv", sep="\t", dtype=str).fillna("")
    for r in k.to_dict("records"):
        add(str(r.get("gene_set") or "").split(":")[0], r.get("obs_id"))
    return out


# ---------------------------------------------------------------------------
# UniProt
# ---------------------------------------------------------------------------

def _uniprot(query: str, *, size: int = 5, tries: int = 4) -> list:
    """One query, RETRIED. A single timeout must not end the pass.

    456 rows at several tiers each is thousands of requests over the better part of an
    hour, and one `URLError: timed out` at row 150 threw away 150 rows of answers. Retry
    with a widening backoff; only a persistent failure raises, and the resume file below
    is what makes even that cheap.
    """
    url = (f"{UNIPROT}?query={urllib.parse.quote(query)}"
           f"&format=json&size={size}&fields=accession,protein_name,organism_name,"
           f"sequence,reviewed,gene_names")
    req = urllib.request.Request(url, headers={"User-Agent": "fabfos-benchmark/1.0"})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read()).get("results", [])
        except urllib.error.HTTPError as e:
            # A 400 is a malformed query -- a label with a character the search syntax
            # takes as an operator. That is this row's answer, not a transport fault.
            if e.code < 500:
                return []
            if attempt == tries - 1:
                raise
        except Exception:                                             # noqa: BLE001
            if attempt == tries - 1:
                raise
        time.sleep(2 ** attempt)
    return []


def _quote(s: str) -> str:
    """A phrase UniProt's search will take as a phrase.

    THE QUOTES ARE NOT ENOUGH ON THEIR OWN. `(+)-alpha-pinene synthase` carries a `+`,
    parentheses and a `-`, and Lucene reads all three as operators; the resulting 400 is
    swallowed by the client as an empty result, so the failure looks exactly like a label
    nothing knows about. Escaping them is the difference between a label that was asked
    about and one that only appeared to be.
    """
    s = decode_entities(s).replace('"', "")
    for ch in "+-&|!(){}[]^~*?:\\/":
        s = s.replace(ch, " ")
    return '"' + re.sub(r"\s+", " ", s).strip() + '"'


# An INSDC nucleotide accession used as a gene label -- LASER does this where the paper
# cited a GenBank record rather than naming the enzyme. Resolvable through UniProt's own
# cross-reference index with no NCBI client at all.
_GENBANK = re.compile(r"^[A-Z]{1,2}\d{5,6}(\.\d+)?$")

_STOP = {"protein", "enzyme", "synthase", "putative", "probable", "chain", "subunit",
         "type", "family", "like", "from", "and", "the"}


def _tokens(s: str) -> set:
    return {t for t in re.split(r"[^a-z0-9]+", decode_entities(s).lower())
            if len(t) >= 4 and t not in _STOP}


def name_overlaps(label: str, hit: dict) -> bool:
    """Does the hit's own name back up a free-text term search?

    A term search over the default field matches an abstract-ish blob and will return
    SOMETHING for almost any phrase. The gate is that the entry's own protein name or gene
    names share most of the label's content words -- which is what separates
    `acyl-ACP thioesterase` finding a thioesterase from it finding whatever sorted first.
    """
    want = _tokens(label)
    if not want:
        return False
    have = _tokens(_hit_name(hit)) | {
        g.get("geneName", {}).get("value", "").lower()
        for g in hit.get("genes", []) or []}
    return len(want & have) >= max(1, int(round(0.6 * len(want))))


def _hit_name(hit: dict) -> str:
    pdsc = hit.get("proteinDescription", {}) or {}
    rec = pdsc.get("recommendedName") or {}
    if rec:
        return (rec.get("fullName") or {}).get("value", "")
    subs = pdsc.get("submissionNames") or []
    if subs:
        return (subs[0].get("fullName") or {}).get("value", "")
    return ""


# Bumped whenever a tier is added, removed or reordered. The resume cache stores misses,
# so this is what stops a re-run from skipping exactly the rows a new tier was added for.
LADDER_VERSION = 2


def resolve(label: str, organism: str, ecs: set, *, sleep: float, aliases=()) -> tuple:
    """`(hit, method)` -- the tier ladder, strongest first, weakest recorded as weak.

    The organism restriction is what makes the strong tiers identifications rather than
    picks: LASER names the organism the part was taken FROM, and a heterologous part is
    the one thing whose organism is never the host's. The last tier has no restriction
    left and returns whichever entry sorts first, which is a guess -- it is taken only so
    the label is not silently absent, and `reason` says so.
    """
    # AT MOST TWO ECs, most specific first. LASER's pairings give some labels three or
    # four -- a fully-specified `1.1.1.35` beside a partial `1.1.1.-` for the same
    # enzyme -- and asking every one of them turns a 3-call row into an 8-call one across
    # 456 rows. The specific one answers or the label is not an EC question.
    ranked = sorted(ecs, key=lambda e: (e.count("-"), -len(e)))[:2]
    symbols = [s for s in ([label] if _SYMBOL.match(label) else []) + list(aliases) if s]

    tiers = []
    # A cited nucleotide accession IS the identification; no organism restriction exists
    # to add and none is needed.
    if _GENBANK.match(label.strip()):
        tiers.append((f"xref:embl-{label.strip().split('.')[0]}", "embl_xref"))
    if organism:
        for ec in ranked:
            tiers.append((f"ec:{ec} AND organism_name:{_quote(organism)}",
                          f"ec_organism:{ec}"))
        # THE SYMBOL TIER, which this resolver did not have at all. 43 of the 72
        # symbol-shaped unresolved rows answer here, at the right organism.
        for sym in symbols:
            tiers.append((f"gene:{sym} AND organism_name:{_quote(organism)} "
                          f"AND reviewed:true", f"gene_organism_reviewed:{sym}"))
        for sym in symbols:
            tiers.append((f"gene:{sym} AND organism_name:{_quote(organism)}",
                          f"gene_organism:{sym}"))
        tiers.append((f'protein_name:{_quote(label)} AND organism_name:{_quote(organism)}'
                      f" AND reviewed:true", "name_organism_reviewed"))
        tiers.append((f"protein_name:{_quote(label)} AND organism_name:{_quote(organism)}",
                      "name_organism"))
        # Free text over the default field, GATED on the hit's own name. `protein_name:`
        # is an exact-ish field match and a label like `Bay Laurel Thioesterase` is not
        # what UniProt calls the protein; a term search finds it and the gate is what
        # keeps that from being a coin flip.
        tiers.append((f"{_quote(label)} AND organism_name:{_quote(organism)}",
                      "term_organism"))
    for ec in ranked:
        tiers.append((f"ec:{ec} AND reviewed:true", f"ec_any_organism:{ec}"))

    for q, method in tiers:
        hits = _uniprot(q)
        time.sleep(sleep)
        for h in hits:
            got = h.get("organism", {}).get("scientificName", "")
            if not genus_initial_ok(organism, got):
                continue
            if method == "term_organism" and not name_overlaps(label, h):
                continue
            return h, method
    return None, "unresolved"


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--network", action="store_true",
                    help="query UniProt; without it this reports what WOULD be asked")
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {PUBLISH_AT.relative_to(REPO)}")
    ap.add_argument("--out", default=None)
    ap.add_argument("--sleep", type=float, default=0.34,
                    help="between UniProt calls; their published limit is 3/s")
    a = ap.parse_args()

    labels = laser_labels()
    pair = pairings_ec()
    nicks = record_nicknames()
    web = web_accessions()
    natives = native_labels()
    n_sym = sum(1 for k in labels if _SYMBOL.match(k))
    print(f"LASER heterologous labels: {len(labels):,} "
          f"({n_sym:,} symbols, {len(labels) - n_sym:,} free text)")
    print(f"native labels the host GEM does not carry: {len(natives):,}")
    print(f"alternative symbols from the record store: {len(nicks):,} labels; "
          f"literature accept list: {len(web):,} rows")

    def build_rows(src_labels, half):
        rows = []
        for name in sorted(src_labels):
            rec = src_labels[name]
            ecs = set(rec["ecs"]) | pair.get(name.lower(), set())
            # ONE ROW PER (label, source), not per label. Two organisms' versions of the
            # same enzyme are two proteins, and `bench_cohorts` joins on the pair for
            # exactly that reason -- collapsing here would attribute one organism's
            # sequence to the other's condition.
            for src, n in sorted(rec["sources"].items()):
                organism, how = normalise_organism(src)
                rows.append(dict(
                    gene=name, source=src, n_obs=n,
                    actions=",".join(sorted(rec["actions"])),
                    uniprot="", organism_resolved="", organism_match=how,
                    protein_name="", aa_len="", reviewed="",
                    reason="" if organism else how, seq="",
                    _ecs=ecs, _organism=organism, _half=half,
                    _aliases=sorted(nicks.get(name, ()))))
        return rows

    rows = build_rows(labels, "het")
    native_rows = build_rows(natives, "native")

    # The literature lane, applied before anything is asked. It is an identification the
    # query tiers by construction cannot reach -- that is why the row is on the list.
    for r in rows + native_rows:
        w = web.get((r["gene"], r["source"]))
        if w:
            r.update(uniprot=w["uniprot"].strip(), seq=(w.get("seq") or "").strip(),
                     protein_name=(w.get("protein_name") or "").strip(),
                     organism_resolved=(w.get("organism") or "").strip(),
                     reason=f"web:{w['doi'].strip()}")

    todo = [r for r in rows + native_rows if r["_organism"] and not r["uniprot"]]
    with_ec = sum(1 for r in todo if r["_ecs"])
    print(f"rows: {len(rows):,} het + {len(native_rows):,} native  "
          f"to ask: {len(todo):,}  of those carrying an EC: {with_ec:,}  "
          f"already answered from the literature list: "
          f"{sum(1 for r in rows + native_rows if r['uniprot']):,}")
    if not a.network:
        print("\ndry run. Re-run with --network to query UniProt.")
        return 0

    out_root = Path(a.out).resolve() if a.out else OUT_ROOT
    out_root.mkdir(parents=True, exist_ok=True)

    # RESUMABLE, one line per row, appended and flushed as each answer lands. A pass over
    # 456 rows is the better part of an hour of somebody else's rate limit; losing it to
    # one timeout at row 150 is the failure this file exists in spite of, not because of.
    # Re-running skips what is already here, so a resume costs only what is left.
    cache_path = out_root / "_resolved.jsonl"
    done = {}
    stale = 0
    if cache_path.exists():
        for ln in cache_path.read_text().splitlines():
            if not ln.strip():
                continue
            c = json.loads(ln)
            # A MISS FROM AN OLDER LADDER IS NOT AN ANSWER. It records that a shorter list
            # of tiers failed, which is exactly the state a new tier is added to change;
            # keeping it would make the re-run skip the only rows that could move. A HIT
            # is kept regardless of ladder -- a stronger tier answering first would not
            # change an accession that a weaker one already got right.
            if not c.get("uniprot") and c.get("ladder", 1) < LADDER_VERSION:
                stale += 1
                continue
            done[(c["gene"], c["source"])] = c
        print(f"resuming: {len(done):,} row(s) already answered in {cache_path.name}"
              + (f"; {stale:,} miss(es) from an older tier ladder re-queued" if stale
                 else ""))

    with cache_path.open("a") as cache:
        for i, r in enumerate(todo, 1):
            prev = done.get((r["gene"], r["source"]))
            if prev is None:
                hit, method = resolve(r["gene"], r["_organism"], r["_ecs"],
                                      sleep=a.sleep, aliases=r["_aliases"])
                prev = dict(gene=r["gene"], source=r["source"], reason=method,
                            uniprot="", organism_resolved="", protein_name="",
                            aa_len="", reviewed="", seq="", ladder=LADDER_VERSION)
                if hit:
                    prev["uniprot"] = hit["primaryAccession"]
                    prev["organism_resolved"] = hit.get("organism", {}).get(
                        "scientificName", "")
                    pn = hit.get("proteinDescription", {}).get("recommendedName", {})
                    prev["protein_name"] = pn.get("fullName", {}).get("value", "")
                    seq = hit.get("sequence", {}).get("value", "")
                    prev["seq"] = seq
                    prev["aa_len"] = str(len(seq))
                    prev["reviewed"] = ("1" if hit.get("entryType", "").startswith(
                        "UniProtKB/Swiss") else "0")
                else:
                    prev["reason"] = "no_uniprot_hit"
                cache.write(json.dumps(prev) + "\n")
                cache.flush()
            r.update({k: v for k, v in prev.items() if k in COLUMNS})
            if i % 50 == 0:
                print(f"  {i}/{len(todo)}", flush=True)
    for fname, half in (("heterologous_uniprot.tsv", rows),
                        ("native_uniprot.tsv", native_rows)):
        with (out_root / fname).open("w") as f:
            w = csv.DictWriter(f, fieldnames=list(COLUMNS), delimiter="\t",
                               extrasaction="ignore")
            w.writeheader()
            for r in half:
                w.writerow(r)

    for tag, half in (("heterologous", rows), ("native", native_rows)):
        ok = sum(1 for r in half if r["uniprot"])
        free = [r for r in half if not _SYMBOL.match(r["gene"])]
        free_ok = sum(1 for r in free if r["uniprot"])
        by_reason: dict = {}
        for r in half:
            k = r["reason"].split(":")[0]
            by_reason[k] = by_reason.get(k, 0) + 1
        print(f"\n{tag}: resolved {ok:,}/{len(half):,} rows; free-text labels "
              f"{free_ok:,}/{len(free):,}")
        for k, v in sorted(by_reason.items(), key=lambda kv: -kv[1]):
            print(f"  {k:<28} {v:,}")

    # EVERY MISS BY NAME. A count is not a curation list, and this is the file a human
    # picks up from -- the same discipline study_sequences.py applies to its own misses.
    miss = [r for r in rows + native_rows if not r["uniprot"]]
    with (out_root / "unresolved.tsv").open("w") as f:
        f.write("gene\tsource\thalf\tn_obs\tec\treason\n")
        for r in sorted(miss, key=lambda r: (r["_half"], r["reason"], r["gene"])):
            f.write(f"{r['gene']}\t{r['source']}\t{r['_half']}\t{r['n_obs']}\t"
                    f"{';'.join(sorted(r['_ecs']))}\t{r['reason']}\n")
    print(f"\n{len(miss):,} unresolved -> {out_root / 'unresolved.tsv'}")

    if a.publish:
        PUBLISH_AT.mkdir(parents=True, exist_ok=True)
        for name in ("heterologous_uniprot.tsv", "native_uniprot.tsv",
                     "unresolved.tsv"):
            dest = PUBLISH_AT / name
            # UNLINK, do not overwrite: anything already under data/ may be a DVC
            # hardlink into a cache several worktrees share, and opening it for writing
            # writes through the link into every one of them.
            if dest.exists() or dest.is_symlink():
                dest.unlink()
            dest.write_text((out_root / name).read_text())
        print(f"published -> {PUBLISH_AT}")
    else:
        print(f"\nnot published. Re-run with --publish to write into {PUBLISH_AT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

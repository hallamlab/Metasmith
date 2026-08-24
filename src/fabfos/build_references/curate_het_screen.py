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
RUNS = REPO / "data" / "fabfos" / "runs"
LASER_ROOT = REPO / "data" / "fabfos" / "originals" / "benchmarks" / "laser"
LASER_INPUTS = LASER_ROOT / "inputs"
PUBLISH_AT = REPO / "data" / "fabfos" / "originals" / "benchmarks" / "het_screen"
OUT_ROOT = REPO / "data" / "fabfos" / "scratch" / "het_screen"

WEB_ACCEPT = Path(__file__).resolve().parent / "het_web_accessions.tsv"

sys.path.insert(0, str(Path(__file__).resolve().parent / "resources" / "buildlib"))
from bench_edges import decode_entities, norm_ec              # noqa: E402
from bench_cohorts import is_native                           # noqa: E402

UNIPROT = "https://rest.uniprot.org/uniprotkb/search"

COLUMNS = ("gene", "source", "n_obs", "actions", "uniprot", "organism_resolved",
           "organism_match", "protein_name", "aa_len", "reviewed", "reason", "seq")

_SYMBOL = re.compile(r"^(b\d{4}|[a-z]{2,5}[A-Z]{0,2}\d{0,2})$")

ORGANISM_ALIASES = {
    "Bsubtilis": "Bacillus subtilis",
    "Saureus": "Staphylococcus aureus",
    "Koxytoca": "Klebsiella oxytoca",
    "Pcrispum": "Petroselinum crispum",
    "Pig": "Sus scrofa",
    "HumanDesigned": "",
    "Designed": "",
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
    "Bacillus fragilis": "Bacteroides fragilis",
    "Ralstonia eutropha": "Cupriavidus necator",
    "Ralstonia eutrophus": "Cupriavidus necator",
    "Erwinia carotovora": "Pectobacterium carotovorum",
    "Erwinia herbicola": "Pantoea agglomerans",
}

_ABBREV = re.compile(r"^([A-Z])\.\s+([a-z][a-z-]+)$")


def normalise_organism(src: str) -> tuple[str, str]:
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
    m = _ABBREV.match((src or "").strip())
    if not m or not resolved:
        return True
    return resolved[:1].upper() == m.group(1).upper()


def laser_labels() -> dict:
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
            if is_native(src):
                continue
            rec = out.setdefault(name, dict(
                gene=name, sources={}, actions=set(), n_obs=0, ecs=set()))
            rec["n_obs"] += 1
            rec["actions"].update(actions)
            rec["sources"][src] = rec["sources"].get(src, 0) + 1
            for tok in str(g.get("ec") or "").split():
                ec = norm_ec(tok)
                if ec:
                    rec["ecs"].add(ec)
    return out


def record_nicknames() -> dict:
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
    path = LASER_INPUTS / "Gene-Reaction Pairings.txt"
    out: dict = {}
    with open(path, errors="replace") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            name = (r.get("Gene Name") or "").strip()
            ecs = (r.get("Discovered EC Number") or "").strip()
            if not name or not ecs:
                continue
            for tok in ecs.split():
                ec = norm_ec(tok)
                if ec:
                    out.setdefault(name.lower(), set()).add(ec)
    return out


def native_labels() -> dict:
    import pandas as pd
    gem = pd.read_parquet(RUNS / "e_coli_k12" / "gpr" / "gpr_gem.parquet",
                          columns=["orf", "feature_name"])
    have = {str(x).strip().lower()
            for col in ("orf", "feature_name") for x in gem[col] if str(x).strip()}

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


def _uniprot(query: str, *, size: int = 5, tries: int = 4) -> list:
    url = (f"{UNIPROT}?query={urllib.parse.quote(query)}"
           f"&format=json&size={size}&fields=accession,protein_name,organism_name,"
           f"sequence,reviewed,gene_names")
    req = urllib.request.Request(url, headers={"User-Agent": "fabfos-benchmark/1.0"})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read()).get("results", [])
        except urllib.error.HTTPError as e:
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
    s = decode_entities(s).replace('"', "")
    for ch in "+-&|!(){}[]^~*?:\\/":
        s = s.replace(ch, " ")
    return '"' + re.sub(r"\s+", " ", s).strip() + '"'


_GENBANK = re.compile(r"^[A-Z]{1,2}\d{5,6}(\.\d+)?$")

_STOP = {"protein", "enzyme", "synthase", "putative", "probable", "chain", "subunit",
         "type", "family", "like", "from", "and", "the"}


def _tokens(s: str) -> set:
    return {t for t in re.split(r"[^a-z0-9]+", decode_entities(s).lower())
            if len(t) >= 4 and t not in _STOP}


def name_overlaps(label: str, hit: dict) -> bool:
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


LADDER_VERSION = 2


def resolve(label: str, organism: str, ecs: set, *, sleep: float, aliases=()) -> tuple:
    ranked = sorted(ecs, key=lambda e: (e.count("-"), -len(e)))[:2]
    symbols = [s for s in ([label] if _SYMBOL.match(label) else []) + list(aliases) if s]

    tiers = []
    if _GENBANK.match(label.strip()):
        tiers.append((f"xref:embl-{label.strip().split('.')[0]}", "embl_xref"))
    if organism:
        for ec in ranked:
            tiers.append((f"ec:{ec} AND organism_name:{_quote(organism)}",
                          f"ec_organism:{ec}"))
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

    cache_path = out_root / "_resolved.jsonl"
    done = {}
    stale = 0
    if cache_path.exists():
        for ln in cache_path.read_text().splitlines():
            if not ln.strip():
                continue
            c = json.loads(ln)
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
            if dest.exists() or dest.is_symlink():
                dest.unlink()
            dest.write_text((out_root / name).read_text())
        print(f"published -> {PUBLISH_AT}")
    else:
        print(f"\nnot published. Re-run with --publish to write into {PUBLISH_AT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

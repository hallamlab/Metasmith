"""EPI300 transcriptomics bridge onto iECDH10B_1368.

    /home/tony/lib/miniforge3/envs/build-refs-cobra/bin/python \\
        src/fabfos/build_references/derive_epi300_fba_bridge.py [--dry-run]

Writes data/fabfos/runs/e_coli_dh10b/fba/: the DH10B model and EPI300's chromosome and
GenBank record (hardlinked), every EPI300 gene as nucleotide FASTA and GTF, and
gpr_gem_epi300.csv -- EPI300's gpr_gem table re-keyed from model gene ids onto EPI300
ORF ids. check_epi300_fba_bridge.py asserts the result.

The model's genes carry DH10B's 2008 locus tags, EPI300's annotation carries its own,
and no id is shared. Each model gene is therefore resolved in two hops:

  1. model gene -> DH10B locus, by /old_locus_tag, then by unique /gene symbol, then by
     operon neighbours (the gene's partners in its own GPR rules). Pseudo CDS count.
  2. DH10B locus -> EPI300 locus, BY POSITION on a collinear chain of anchors (proteins
     unique and identical in both genomes), confirmed by sequence. Position is the only
     thing that tells the two copies of DH10B's ~113 kb duplication apart and the only
     thing that finds a pseudogene, which has no protein to match.

A pair that position proposes and sequence does not confirm stops the build.
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_epi300_identity import (EXPECTED_LOST_REACTIONS, GENOMES, REPO,  # noqa: E402
                                   model_genes, model_rules, rule_holds)

DH10B_GENOME = GENOMES / "e_coli_dh10b" / "genome"
EPI300_GENOME = GENOMES / "e_coli_epi300" / "genome"
GEM = GENOMES / "e_coli_dh10b" / "GEM" / "iECDH10B_1368.json"
GPR_GEM = REPO / "data" / "fabfos" / "runs" / "e_coli_epi300" / "gpr" / "gpr_gem.parquet"
EVIDENCE_LIB = REPO / "src" / "metasmith_libraries" / "resources" / "lib"
OUT = REPO / "data" / "fabfos" / "runs" / "e_coli_dh10b" / "fba"

EPI300_SEQ = "CP189566.1"
PRODUCTS = ("CDS", "tRNA", "rRNA", "ncRNA", "tmRNA")
SPONTANEOUS = "s0001"
MIN_NT_IDENTITY = 0.95
SEARCH_KB = 20
NEIGHBOUR_KB = 5
BUILD_ID = "gem_iECDH10B_1368_epi300"
MODEL_ID = "EPI300"
EXTENSIONS = ("attribution", "feature", "universe")
LANE_SET = "curated"

SPOT_CHECK = ("ECDH10B_1939", "fruB", "mglA", "ytfT", "gatZ", "mhpD", "nupG", "nrfB",
              "proV", "fhuA", "ECDH10B_0530")

_CODE = "FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG"
CODONS = {a + b + c: _CODE[16 * i + 4 * j + k]
          for i, a in enumerate("TCAG") for j, b in enumerate("TCAG")
          for k, c in enumerate("TCAG")}
_COMPLEMENT = str.maketrans("ACGTRYKMSWBDHVN", "TGCAYRMKSWVHDBN")


def revcomp(s: str) -> str:
    return s.translate(_COMPLEMENT)[::-1]


def translate(nt: str) -> str:
    aa = "".join(CODONS.get(nt[i:i + 3], "X") for i in range(0, len(nt) - 2, 3))
    return ("M" + aa[1:]).rstrip("*")


@dataclass
class Feature:
    kind: str
    location: str
    quals: dict = field(default_factory=dict)
    parts: list = field(default_factory=list)
    nt: str = ""
    fid: str = ""
    protein: str | None = None

    def q(self, key):
        v = self.quals.get(key)
        return v[0] if v else None

    @property
    def locus(self):
        return self.q("locus_tag")

    @property
    def old_locus(self):
        return self.q("old_locus_tag")

    @property
    def gene(self):
        return self.q("gene")

    @property
    def product(self):
        return self.q("product")

    @property
    def pseudo(self):
        return "pseudo" in self.quals or "pseudogene" in self.quals

    @property
    def strand(self):
        return self.parts[0][2]

    @property
    def five_prime(self):
        a, b, s = self.parts[0]
        return a if s > 0 else b

    @property
    def intact(self):
        return self.kind == "CDS" and not self.pseudo


def _split_top(s: str) -> list[str]:
    out, depth, cur = [], 0, []
    for ch in s:
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
            continue
        depth += (ch == "(") - (ch == ")")
        cur.append(ch)
    out.append("".join(cur))
    return out


def parse_location(s: str) -> list[tuple[int, int, int]]:
    s = s.replace(" ", "")
    if s.startswith("complement(") and s.endswith(")"):
        return [(a, b, -st) for a, b, st in reversed(parse_location(s[11:-1]))]
    for op in ("join(", "order("):
        if s.startswith(op) and s.endswith(")"):
            return [p for part in _split_top(s[len(op):-1]) for p in parse_location(part)]
    m = re.fullmatch(r"[<>]?(\d+)(?:\.\.[<>]?(\d+))?", s)
    if not m:
        raise SystemExit(f"unsupported GenBank location {s!r}")
    a = int(m.group(1))
    return [(a, int(m.group(2) or a), 1)]


def read_genbank(path: Path) -> tuple[str, list[Feature]]:
    features: list[Feature] = []
    seq: list[str] = []
    section = None
    cur = None
    key = None
    open_quote = False
    for raw in path.read_text().splitlines():
        if raw.startswith("FEATURES"):
            section = "features"
            continue
        if raw.startswith("ORIGIN"):
            section = "origin"
            continue
        if raw.startswith("//"):
            break
        if section == "origin":
            seq.append("".join(raw.split()[1:]))
            continue
        if section != "features":
            continue
        if raw.startswith("     ") and raw[5] != " ":
            cur = Feature(kind=raw[5:21].strip(), location=raw[21:].strip())
            features.append(cur)
            key, open_quote = None, False
            continue
        text = raw[21:]
        if text.startswith("/") and not open_quote:
            k, eq, v = text[1:].partition("=")
            key = k
            cur.quals.setdefault(k, []).append(v if eq else "")
            open_quote = v.count('"') % 2 == 1
        elif key is None:
            cur.location += text.strip()
        else:
            sep = "" if key == "translation" else " "
            cur.quals[key][-1] += sep + text.strip()
            open_quote = (open_quote + text.count('"')) % 2 == 1
    for f in features:
        f.quals = {k: [v.strip('"') for v in vs] for k, vs in f.quals.items()}
        f.parts = parse_location(f.location)
    genome = "".join(seq).upper()
    for f in features:
        f.nt = "".join(genome[a - 1:b] if st > 0 else revcomp(genome[a - 1:b])
                       for a, b, st in f.parts)
    return genome, features


def read_faa(path: Path) -> dict[str, tuple[str, str]]:
    out: dict[str, tuple[str, list[str]]] = {}
    locus = None
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            m = re.search(r"\[locus_tag=([^\]]+)\]", line)
            locus = m.group(1)
            if locus in out:
                raise SystemExit(f"{path.name}: locus_tag {locus} appears twice")
            # NCBI's `lcl|` prefix goes: cobra's GPR parser reads `|` as `or`.
            out[locus] = (line[1:].split()[0].removeprefix("lcl|"), [])
        else:
            out[locus][1].append(line.strip())
    return {k: (i, "".join(s)) for k, (i, s) in out.items()}


def load_genome(gbk: Path, faa: Path, seqname: str) -> tuple[str, list[Feature]]:
    genome, features = read_genbank(gbk)
    products = [f for f in features if f.kind in PRODUCTS]
    faa_by_locus = read_faa(faa)
    cds = [f for f in products if f.kind == "CDS"]
    if len(faa_by_locus) != len(cds):
        raise SystemExit(f"{faa.name} has {len(faa_by_locus)} entries for {len(cds)} CDS")
    bad = []
    for f in products:
        if f.kind == "CDS":
            f.fid, prot = faa_by_locus[f.locus]
            if f.intact:
                f.protein = prot
                mine = translate(f.nt)
                sec = "transl_except" in f.quals
                same = len(mine) == len(prot) and all(
                    x == y or x == "X" or (sec and x == "*" and y == "U")
                    for x, y in zip(mine, prot))
                if not same:
                    bad.append(f.locus)
        else:
            f.fid = f"{seqname}_{f.kind.lower()}_{f.locus}"
    if bad:
        raise SystemExit(f"{gbk.name}: {len(bad)} intact CDS do not translate to their "
                         f".faa protein, e.g. {bad[:5]}. The location parser or the "
                         f"sequence extraction is wrong; nothing downstream can be trusted")
    print(f"{gbk.name}: {len(genome):,} bp, {len(products):,} gene features "
          f"({dict(Counter(f.kind for f in products))}), "
          f"{sum(f.pseudo for f in products):,} pseudo; every intact CDS translates to "
          f"its .faa protein", flush=True)
    return genome, products


def nt_identity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    sm = SequenceMatcher(None, a, b, autojunk=False)
    return sum(m.size for m in sm.get_matching_blocks()) / min(len(a), len(b))


def circ(d: int, n: int) -> int:
    return (d + n // 2) % n - n // 2


def rule_tokens(rule: str) -> list[str]:
    return re.findall(r"\(|\)|[^\s()]+", rule or "")


def rule_genes(rule: str) -> set[str]:
    return {t for t in rule_tokens(rule) if t not in ("(", ")", "and", "or")}


@dataclass
class Resolution:
    gene: str
    route: str
    dh: list = field(default_factory=list)
    epi: list = field(default_factory=list)
    evidence: list = field(default_factory=list)
    note: str = ""


def resolve_in_dh10b(genes, rules, dh: list[Feature], dh_len: int) -> dict[str, Resolution]:
    by_old = {f.old_locus: f for f in dh if f.old_locus}
    by_symbol = defaultdict(list)
    for f in dh:
        if f.kind == "CDS" and f.gene:
            by_symbol[f.gene].append(f)
    partners = defaultdict(set)
    for _, rule in rules:
        gs = rule_genes(rule)
        for g in gs:
            partners[g] |= gs - {g}

    def one_disrupted_gene(hits):
        # An IS insertion or rearrangement leaves one gene as several /pseudo pieces that
        # keep its symbol and product; they are one model gene, not paralogues.
        return (len(hits) > 1 and all(f.pseudo for f in hits)
                and len({f.product for f in hits}) == 1
                and max(abs(circ(a.five_prime - b.five_prime, dh_len))
                        for a in hits for b in hits) <= SEARCH_KB * 1000)

    res: dict[str, Resolution] = {}
    for g in genes:
        hits = by_symbol.get(g, [])
        if g == SPONTANEOUS:
            res[g] = Resolution(g, "spontaneous")
        elif g in by_old:
            res[g] = Resolution(g, "old_tag", [by_old[g]])
        elif len(hits) == 1:
            res[g] = Resolution(g, "symbol", list(hits))
        elif one_disrupted_gene(hits):
            res[g] = Resolution(g, "symbol", list(hits),
                                note=f"{len(hits)} pseudo fragments of one disrupted gene")

    order = sorted(dh, key=lambda f: f.five_prime)
    for g in genes:
        if g in res:
            continue
        anchors = [res[p].dh[0].five_prime for p in partners[g]
                   if p in res and res[p].dh]
        near = [f for f in order if f.kind == "CDS" and anchors and
                min(abs(circ(f.five_prime - a, dh_len)) for a in anchors)
                <= NEIGHBOUR_KB * 1000]
        named = [f for f in near if f.gene == g]
        if not named:
            raise SystemExit(f"model gene {g!r}: no old_locus_tag, {len(by_symbol.get(g, ()))} "
                             f"symbol hits, and no /gene={g} within {NEIGHBOUR_KB} kb of its "
                             f"rule partners {sorted(partners[g])}")
        products = {f.product for f in named}
        claimed = {id(r.dh[0]) for r in res.values() if r.dh}
        frags = [f for f in near if f.pseudo and f.product in products
                 and id(f) not in claimed and f not in named]
        res[g] = Resolution(g, "neighbour", named + frags,
                            note=f"{len(by_symbol.get(g, ()))} symbol hits genome-wide; "
                                 f"{len(named) + len(frags)} fragment(s) beside "
                                 f"{sorted(partners[g])}")

    model_loci = {id(f) for r in res.values() for f in r.dh}
    by_protein = defaultdict(list)
    by_nt = defaultdict(list)
    for f in dh:
        if f.kind == "CDS":
            (by_protein[f.protein] if f.intact else by_nt[f.nt]).append(f)
    for r in res.values():
        extra = []
        for f in r.dh:
            same = by_protein[f.protein] if f.intact else by_nt[f.nt]
            extra += [c for c in same if c is not f and id(c) not in model_loci]
        if extra:
            r.dh += extra
            r.note = (r.note + "; " if r.note else "") + \
                f"{len(extra)} identical DH10B cop{'y' if len(extra) == 1 else 'ies'} not named by the model"
    return res


def anchor_chain(dh: list[Feature], epi: list[Feature], epi_len: int):
    def unique(features):
        c = Counter(f.protein for f in features if f.intact)
        return {f.protein: f for f in features if f.intact and c[f.protein] == 1}
    ud, ue = unique(dh), unique(epi)
    pairs = [(ud[p], ue[p]) for p in ud.keys() & ue.keys()]
    offsets = sorted((e.five_prime - d.five_prime) % epi_len for d, e in pairs)
    d0 = offsets[len(offsets) // 2]
    pts = sorted((d.five_prime, d.five_prime + circ(e.five_prime - d0 - d.five_prime, epi_len),
                  d.strand == e.strand) for d, e in pairs)
    pts = [p for p in pts if p[2]]

    tails, prev, idx = [], [-1] * len(pts), []
    for i, (_, r, _) in enumerate(pts):
        k = bisect.bisect_left([pts[j][1] for j in idx], r)
        if k > 0:
            prev[i] = idx[k - 1]
        if k == len(idx):
            idx.append(i)
        else:
            idx[k] = i
    chain, i = [], idx[-1] if idx else -1
    while i >= 0:
        chain.append(pts[i][:2])
        i = prev[i]
    chain.reverse()
    print(f"anchors: {len(pairs):,} proteins unique and identical in both genomes, "
          f"{len(pts):,} on the same strand, {len(chain):,} on the collinear chain "
          f"(origin offset {d0:,} bp)", flush=True)
    return d0, chain


def encodes_whole(d: Feature, e: Feature, genome: str) -> bool:
    # proV is the case: EPI300's PGAP flags it "missing C-terminus" while the chromosome
    # carries DH10B's whole gene, byte for byte. The two annotations disagree about a
    # lesion both strains share, so the gene is not lost in EPI300.
    if len(e.parts) != 1:
        return False
    n, p = len(d.nt), e.five_prime
    seq = genome[p - 1:p - 1 + n] if e.strand > 0 else revcomp(genome[p - n:p])
    return seq == d.nt


def classify(d: Feature, e: Feature, genome: str) -> tuple[str, float] | None:
    if d.intact and e.intact:
        if d.protein == e.protein:
            return "identical_protein", 1.0
        ident = nt_identity(d.nt, e.nt)
        return ("start_variant", ident) if ident >= MIN_NT_IDENTITY else None
    ident = nt_identity(d.nt, e.nt)
    if ident < MIN_NT_IDENTITY:
        return None
    if d.pseudo and e.pseudo:
        return "pseudo_both", ident
    if not e.pseudo:
        return "pseudo_dh10b_only", ident
    if encodes_whole(d, e, genome):
        return "pseudo_annotation_only", ident
    return "pseudo_epi300_only", ident


def place(dh_features, epi: list[Feature], d0: int, chain, epi_genome: str):
    epi_len = len(epi_genome)
    hs = [h for h, _ in chain]
    by_kind_strand = defaultdict(list)
    for e in epi:
        by_kind_strand[(e.kind, e.strand)].append(e)
    placed = {}
    failures = []
    for d in dh_features:
        # Predicted from BOTH flanking anchors: an insertion between them shifts the far
        # side, and the flank on the gene's own side of it predicts the gene exactly.
        k = bisect.bisect_left(hs, d.five_prime)
        preds = [d0 + d.five_prime + (chain[c][1] - chain[c][0])
                 for c in (k - 1, k) if 0 <= c < len(chain)]
        cands = sorted(
            ((min(abs(circ(e.five_prime - p, epi_len)) for p in preds), e)
             for e in by_kind_strand[(d.kind, d.strand)]),
            key=lambda t: t[0])
        cands = [c for c in cands if c[0] <= SEARCH_KB * 1000]
        for drift, e in cands:
            c = classify(d, e, epi_genome)
            if c:
                placed[id(d)] = (e, c[0], c[1], drift)
                break
        else:
            failures.append((d.locus, d.gene, d.old_locus, len(cands)))
    if failures:
        for f in failures:
            print(f"  UNCONFIRMED: DH10B {f}", flush=True)
        raise SystemExit(f"{len(failures)} DH10B loci have no EPI300 feature within "
                         f"{SEARCH_KB} kb of their predicted position that sequence "
                         f"confirms (>= {MIN_NT_IDENTITY:.0%} nt identity, identical "
                         f"protein when both are intact). Stopping rather than guessing.")
    return placed


def sibling_fragments(d: Feature, e: Feature, epi: list[Feature], taken: set[int],
                      epi_len: int) -> list[Feature]:
    # The other pieces of a gene an insertion split. Found by sequence rather than by
    # adjacency or product: the IS sits between the pieces, and NCBI names each piece by
    # whatever its own fragment resembles (fhuA's 3' half is a "TonB-dependent
    # siderophore receptor").
    return [f for f in epi
            if f is not e and f.kind == "CDS" and f.pseudo and id(f) not in taken
            and f.strand == e.strand and len(f.nt) < len(d.nt)
            and abs(circ(f.five_prime - e.five_prime, epi_len)) <= NEIGHBOUR_KB * 1000
            and nt_identity(d.nt, f.nt) >= MIN_NT_IDENTITY]


def resolve():
    genes = model_genes(GEM)
    rules = model_rules(GEM)
    dh_len_genome, dh = load_genome(DH10B_GENOME / "NC_010473.1.gbk",
                                    DH10B_GENOME / "NC_010473.1.faa", "NC_010473.1")
    epi_genome, epi = load_genome(EPI300_GENOME / f"{EPI300_SEQ}.gbk",
                                  EPI300_GENOME / f"{EPI300_SEQ}.faa", EPI300_SEQ)
    res = resolve_in_dh10b(genes, rules, dh, len(dh_len_genome))
    d0, chain = anchor_chain(dh, epi, len(epi_genome))

    wanted = [f for r in res.values() for f in r.dh]
    placed = place(wanted, epi, d0, chain, epi_genome)

    taken = {id(v[0]) for v in placed.values()}
    for r in res.values():
        for d in r.dh:
            e, cls, ident, drift = placed[id(d)]
            if e not in r.epi:
                r.epi.append(e)
                r.evidence.append((d, e, cls, ident, drift))
            if e.pseudo:
                for frag in sibling_fragments(d, e, epi, taken, len(epi_genome)):
                    taken.add(id(frag))
                    r.epi.append(frag)
                    r.evidence.append((d, frag, "pseudo_fragment",
                                       nt_identity(d.nt, frag.nt), None))

    owner = defaultdict(set)
    for r in res.values():
        for e in r.epi:
            owner[e.fid].add(r.gene)
    shared = {k: v for k, v in owner.items() if len(v) > 1}
    if shared:
        raise SystemExit(f"{len(shared)} EPI300 ORFs claimed by more than one model gene: "
                         f"{list(shared.items())[:5]}")
    return genes, rules, res, epi, epi_genome


def report(genes, res, epi):
    print("\nspot checks:")
    for g in SPOT_CHECK:
        r = res.get(g) or next(r for r in res.values() if any(d.gene == g for d in r.dh))
        print(f"  {r.gene:<13} route={r.route:<9} {r.note}")
        for d, e, cls, ident, drift in r.evidence:
            print(f"      DH10B {d.locus:<16} {d.old_locus or '-':<13} {d.gene or '-':<6} "
                  f"{'pseudo' if d.pseudo else 'intact':<6} -> EPI300 {e.locus} "
                  f"{e.gene or '-':<6} {'pseudo' if e.pseudo else 'intact':<6} "
                  f"{cls:<18} nt_id={ident:.3f} drift={drift}")
    first = next(e for e in epi if e.locus == "ACP6EX_00005")
    print(f"  origin-spanning {first.locus} {first.gene} {first.location} -> "
          f"{len(first.nt)} nt, protein {'matches' if first.protein else 'MISSING'}")

    routes = Counter(r.route for r in res.values())
    outcomes = Counter()
    for r in res.values():
        if r.route == "spontaneous":
            outcomes["spontaneous (s0001)"] += 1
            continue
        classes = {c for *_, c, _, _ in [(x[0], x[1], x[2], x[3], x[4]) for x in r.evidence]}
        if "pseudo_epi300_only" in classes:
            outcomes["pseudo in EPI300 only"] += 1
        elif "pseudo_annotation_only" in classes:
            outcomes["pseudo in EPI300's annotation only, DNA identical"] += 1
        elif classes & {"pseudo_both", "pseudo_fragment"}:
            outcomes["pseudo in both"] += 1
        elif len(r.epi) > 1:
            outcomes[f"intact 1:{len(r.epi)}"] += 1
        else:
            outcomes["intact 1:1"] += 1
    ev = Counter(x[2] for r in res.values() for x in r.evidence)
    drifts = [x[4] for r in res.values() for x in r.evidence if x[4] is not None]
    print(f"\nmodel genes: {len(genes):,}")
    print(f"  routes:   {dict(routes)}")
    print(f"  outcomes: {dict(outcomes)}")
    print(f"  pairs by evidence: {dict(ev)}")
    print(f"  positional drift: max {max(drifts)} bp, "
          f"{sum(d > 0 for d in drifts)} of {len(drifts)} pairs off the prediction")
    for r in res.values():
        for d, e, cls, ident, drift in r.evidence:
            if cls not in ("identical_protein",):
                print(f"    {r.gene:<13} {d.locus:<16} -> {e.locus:<13} {cls:<18} "
                      f"nt_id={ident:.3f} len {len(d.nt)} -> {len(e.nt)}")
    for r in res.values():
        if r.route in ("symbol", "neighbour"):
            print(f"    {r.route:<9} {r.gene:<6} -> DH10B {[d.locus for d in r.dh]} "
                  f"-> EPI300 {[e.locus for e in r.epi]}")
    mapped = {e.fid for r in res.values() for e in r.epi}
    n_cds = sum(e.kind == "CDS" for e in epi)
    n_unmapped = sum(e.kind == "CDS" and e.fid not in mapped for e in epi)
    print(f"  EPI300 CDS with no model gene: {n_unmapped:,} of {n_cds:,} "
          f"({sum(e.kind == 'CDS' and e.pseudo and e.fid not in mapped for e in epi)} pseudo)")


def rewrite_rule(rule: str, orfs: dict[str, list[str]]) -> str:
    out = []
    for t in rule_tokens(rule):
        if t in ("(", ")", "and", "or"):
            out.append(t)
        elif t == SPONTANEOUS:
            out.append(t)
        else:
            ids = orfs[t]
            out.append(ids[0] if len(ids) == 1 else "( " + " or ".join(ids) + " )")
    return re.sub(r"\( | \)", lambda m: m.group(0).strip(), " ".join(out))


def published_rule(rule: str) -> str | None:
    # The CSV's rules name ORFs only. Every model rule that mentions s0001 is satisfied by
    # s0001 alone, i.e. always live, and an empty rule already evaluates that way.
    if SPONTANEOUS not in rule_genes(rule):
        return rule or None
    if not rule_holds(rule, {SPONTANEOUS}):
        raise SystemExit(f"s0001 is not a sufficient alternative in {rule!r}")
    return None


def live_reactions(rules, present: set[str]) -> set[str]:
    return {rid for rid, rule in rules if rule_holds(rule, present)}


def write_atomic(path: Path, text: str):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def link_original(src: Path, dst: Path):
    # rename(2) onto another name for the same inode is a no-op that leaves tmp behind.
    if dst.exists() and os.path.samefile(src, dst):
        return
    tmp = dst.with_name(dst.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    os.link(src, tmp)
    os.replace(tmp, dst)


def fasta_record(e: Feature) -> str:
    desc = [f"[locus_tag={e.locus}]", f"[gbkey={e.kind}]"]
    if e.gene:
        desc.append(f"[gene={e.gene}]")
    if e.product:
        desc.append(f"[product={e.product}]")
    desc.append(f"[pseudo={'true' if e.pseudo else 'false'}]")
    desc.append(f"[location={e.location}]")
    body = "\n".join(e.nt[i:i + 80] for i in range(0, len(e.nt), 80))
    return f">{e.fid} {' '.join(desc)}\n{body}\n"


def biotype(e: Feature) -> str:
    if e.kind == "CDS":
        return "pseudogene" if e.pseudo else "protein_coding"
    return e.kind


def gtf_records(e: Feature, genome_len: int) -> list[str]:
    attrs = (f'gene_id "{e.fid}"; transcript_id "{e.fid}"; locus_tag "{e.locus}"; '
             f'gene_name "{e.gene or ""}"; gene_biotype "{biotype(e)}"; '
             f'pseudo "{"true" if e.pseudo else "false"}";')
    strand = "+" if e.strand > 0 else "-"
    rows = []
    lo = min(a for a, _, _ in e.parts)
    hi = max(b for _, b, _ in e.parts)
    wraps = hi - lo + 1 > sum(b - a + 1 for a, b, _ in e.parts) + genome_len // 2
    if not wraps:
        rows.append(f"{EPI300_SEQ}\tNCBI\tgene\t{lo}\t{hi}\t.\t{strand}\t.\t{attrs}")
    for n, (a, b, _) in enumerate(e.parts, 1):
        rows.append(f"{EPI300_SEQ}\tNCBI\texon\t{a}\t{b}\t.\t{strand}\t.\t"
                    f'{attrs} exon_number "{n}";')
    return rows


def write_epi300_model(res, epi, out: Path):
    m = json.loads(GEM.read_text())
    orfs = {r.gene: [e.fid for e in sorted(r.epi, key=lambda f: f.five_prime)]
            for r in res.values() if r.route != "spontaneous"}
    model_gene_of = {fid: g for g, ids in orfs.items() for fid in ids}
    feature = {e.fid: e for e in epi}
    used = set()
    for r in m["reactions"]:
        rule = published_rule(rewrite_rule(r.get("gene_reaction_rule") or "", orfs)) or ""
        r["gene_reaction_rule"] = rule
        used |= rule_genes(rule)
        if r["id"] in EXPECTED_LOST_REACTIONS:
            r["lower_bound"] = r["upper_bound"] = 0.0
    m["genes"] = [{"id": fid, "name": feature[fid].gene or "",
                   "notes": {"model_gene": [model_gene_of[fid]],
                             "pseudo": ["true" if feature[fid].pseudo else "false"]},
                   "annotation": {"locus_tag": [feature[fid].locus]}}
                  for fid in sorted(used)]
    m["id"] = MODEL_ID
    m["name"] = f"{MODEL_ID}: {GEM.stem} on {EPI300_SEQ} ORFs"
    m["notes"] = {
        "derived_from": [f"{GEM.stem} (BiGG; Monk et al. 2013, PMID 24277855)"],
        "genome": [f"{EPI300_SEQ} (GCF_052692645.1)"],
        "closed_reactions": sorted(EXPECTED_LOST_REACTIONS),
        "closed_because": ["fhuA is split by an ISVsa5 insertion in EPI300"],
        "builder": ["src/fabfos/build_references/derive_epi300_fba_bridge.py"],
    }
    write_atomic(out / f"{MODEL_ID}.json", json.dumps(m))


def write_outputs(genes, rules, res, epi, epi_genome, out: Path):
    import numpy as np
    import pandas as pd
    sys.path.insert(0, str(EVIDENCE_LIB))
    import fabfos_evidence as fe

    out.mkdir(parents=True, exist_ok=True)
    link_original(GEM, out / GEM.name)
    for ext in ("fna", "gbk"):
        link_original(EPI300_GENOME / f"{EPI300_SEQ}.{ext}", out / f"{EPI300_SEQ}.{ext}")

    epi_sorted = sorted(epi, key=lambda f: min(a for a, _, _ in f.parts))
    write_atomic(out / "epi300_genes.fna", "".join(fasta_record(e) for e in epi_sorted))
    gtf = [f"#gtf-version 2.2", f"#!genome-build {EPI300_SEQ} (GCF_052692645.1)"]
    for e in epi_sorted:
        gtf += gtf_records(e, len(epi_genome))
    write_atomic(out / "epi300_genes.gtf", "\n".join(gtf) + "\n")

    orfs = {r.gene: [e.fid for e in sorted(r.epi, key=lambda f: f.five_prime)]
            for r in res.values() if r.route != "spontaneous"}
    symbol = {e.fid: e.gene for e in epi}
    src = pd.read_parquet(GPR_GEM)
    rows = []
    for row in src.to_dict("records"):
        row["build_id"] = BUILD_ID
        if row["feature_kind"] != "gem_gene":
            rows.append(row)
            continue
        model_gene = row["orf"]
        row["gpr_rule"] = rewrite_rule(row["gpr_rule"], orfs)
        row["feature_name"] = model_gene
        if model_gene == SPONTANEOUS:
            row["gene"] = None
            rows.append(row)
            continue
        for fid in orfs[model_gene]:
            rows.append(dict(row, orf=fid, gene=symbol[fid]))
    df = pd.DataFrame(rows, columns=fe.schema_for(EXTENSIONS))
    df["llr"] = df["llr"].astype(np.float64)
    df["in_atom_universe"] = df["in_atom_universe"].astype(bool)
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                        na_position="last").reset_index(drop=True)
    ids = {e.fid for e in epi} | {SPONTANEOUS} | \
        set(df.loc[df["feature_kind"] == "ruleless", "orf"])
    fe.validate_gpr(df, LANE_SET, ids, src["source"].iloc[0], EXTENSIONS)
    df["orf"] = df["orf"].where(df["orf"].isin({e.fid for e in epi}), None)
    df["gpr_rule"] = df["gpr_rule"].map(lambda r: published_rule(r) if isinstance(r, str) else None)
    tmp = out / "gpr_gem_epi300.csv.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, out / "gpr_gem_epi300.csv")
    write_epi300_model(res, epi, out)
    print(f"\nwrote {out}: {len(epi_sorted):,} genes in FASTA and GTF, {len(df):,} GPR rows "
          f"({(df['feature_kind'] == 'gem_gene').sum():,} gem_gene over "
          f"{df.loc[df['feature_kind'] == 'gem_gene', 'orf'].nunique():,} ORFs)", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--dry-run", action="store_true",
                    help="resolve and report the mapping without writing anything")
    a = ap.parse_args()

    genes, rules, res, epi, epi_genome = resolve()
    report(genes, res, epi)

    orfs = {r.gene: [e.fid for e in r.epi] for r in res.values()}
    lost_in_epi = {e.fid for r in res.values() for d, e, cls, *_ in r.evidence
                   if d.intact and e.pseudo and cls != "pseudo_annotation_only"}
    everything = {e.fid for e in epi} | {SPONTANEOUS}
    rewritten = [(rid, rewrite_rule(rule, orfs)) for rid, rule in rules]
    model_live = live_reactions(rules, set(genes))
    dark = model_live - live_reactions(rewritten, everything - lost_in_epi)
    all_pseudo = {e.fid for e in epi if e.pseudo}
    dark_strict = model_live - live_reactions(rewritten, everything - all_pseudo)
    print(f"\nreactions dark in EPI300 against the model: {len(dark)} {sorted(dark)}")
    print(f"  treating every pseudogene as absent, including the 8 the model already "
          f"carries as working: {len(dark_strict)} {sorted(dark_strict)}")
    if dark != EXPECTED_LOST_REACTIONS:
        raise SystemExit(f"dark set {sorted(dark)} is not check_epi300_identity's "
                         f"{sorted(EXPECTED_LOST_REACTIONS)}")

    if not a.dry_run:
        write_outputs(genes, rules, res, epi, epi_genome, a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

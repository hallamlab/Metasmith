"""Resolve every study's gene tokens to protein sequences -> one ORF set per study.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python build_references/study_sequences.py                  # local pass only
    PATH="..." python build_references/study_sequences.py --network  # + UniProt
    PATH="..." python build_references/study_sequences.py --publish

The study half of the benchmark needs the same de-novo evidence the host half gets: the
shipped 4-lane mapper run over each study's genes. That needs the genes as PROTEIN
SEQUENCES, and the extractions carry only identifiers -- b-numbers, gene symbols, and
in LASER's case free-text enzyme names. This closes that gap and, more importantly,
states exactly how far it got.

THREE CLASSES OF TOKEN, AND CONFLATING THEM IS THE FAILURE THIS EXISTS TO AVOID.

  gene              a symbol or locus tag that names one protein: `lysC`, `b4024`,
                    `pcaB`. Resolvable, and a failure to resolve one is a real gap.
  pathway_shorthand NOT A GENE. The gain-of-function studies name their targets and
                    controls as lumped conversions -- `glucose_to_3PG`,
                    `E4P_to_chorismate`, `cdpdag_to_CL` -- which are what the readout
                    is measured over, never something with a sequence. Counting these
                    as unresolved genes reports a 0% resolution rate on a study whose
                    every actual gene resolved.
  enzyme_name       a free-text description rather than an identifier: "4-coumarate-CoA
                    ligase", "(+)-&alpha;-pinene synthase". LASER's heterologous
                    additions are written this way because the paper wrote them that
                    way. A name plus no organism is not a lookup, it is a curation
                    decision, and this reports them for a curator rather than guessing.

LOCAL FIRST, AND MOST OF IT IS LOCAL. The three host proteomes already pinned under
`data/fabfos/originals/genomes/` carry `[gene=]` and `[locus_tag=]` on every header, and every
study but LASER is an E. coli K-12 derivative -- so the great majority of tokens resolve
with no network at all, against the exact sequences the host half was annotated from.
That is a stronger provenance than a fresh fetch would give: the study's genes and the
host background are then literally the same protein records.

NOTHING IS SILENTLY DROPPED. Every token appears in `<study>/genes_resolution.tsv` with
its class, its outcome and the reason -- `local`, `uniprot_gene`, `uniprot_ec`,
`not_a_gene`, `unresolved`. Coverage is reported per study per class. The parent plan is
explicit that coverage IS the honest form of "the benchmark does not fit this network",
so a study that resolves 76 of 86 says so rather than shipping 76 and looking complete.
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
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"
OUT_ROOT = REPO / "data" / "fabfos" / "scratch" / "study_sequences"
HET_TABLE = (REPO / "data" / "fabfos" / "originals" / "benchmarks" / "het_screen"
             / "heterologous_uniprot.tsv")

STUDIES = ("laser", "keio", "eydallin", "aromatic", "fa_supply", "forsberg",
           "pg_anionic", "fang")

ECOLI_TAXON = "562"

UNIPROT = "https://rest.uniprot.org/uniprotkb/search"

_SHORTHAND = re.compile(r"_to_", re.IGNORECASE)
_SYMBOL = re.compile(r"^(b\d{4}|[a-z]{2,5}[A-Z]{0,2}\d{0,2})$")


def index_proteomes() -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    index: dict[str, tuple[str, str]] = {}
    seqs: dict[str, str] = {}
    for faa in sorted(GENOMES.glob("*/genome/*.faa")):
        host = faa.parent.parent.name
        acc = None
        for line in faa.open():
            if line.startswith(">"):
                acc = line[1:].split()[0]
                seqs[acc] = ""
                for pat in (r"\[gene=([^\]]+)\]", r"\[locus_tag=([^\]]+)\]"):
                    m = re.search(pat, line)
                    if m:
                        index.setdefault(m.group(1).strip().lower(), (acc, host))
            elif acc:
                seqs[acc] += line.strip()
    return index, seqs


def classify(token: str, role: str) -> str:
    t = token.strip()
    if not t:
        return "empty"
    if role in ("target", "control") or _SHORTHAND.search(t):
        return "pathway_shorthand"
    if _SYMBOL.match(t):
        return "gene"
    return "enzyme_name"


def tokens_for(study: str) -> list[dict]:
    import pandas as pd
    d = pd.read_csv(BENCH / study / "extraction.tsv", sep="\t", dtype=str)
    out: list[dict] = []
    if study == "laser":
        src_for = {}
        for i, gj in d["genes_json"].fillna("").items():
            if not gj.strip():
                continue
            for g in json.loads(gj):
                n = (g.get("gene") or "").strip()
                s = (g.get("source") or "").strip()
                if n and s:
                    src_for.setdefault(n, {}).setdefault(s, 0)
                    src_for[n][s] += 1
        for i, gs in d["gene_set"].fillna("").items():
            for part in gs.split("|"):
                if not part.strip():
                    continue
                tok, _, act = part.rpartition(":")
                if not tok:
                    tok, act = act, "?"
                tok = tok.strip()
                out.append(dict(token=tok, role=act.strip(),
                                sources=src_for.get(tok, {}),
                                obs=d.at[i, "obs_id"]))
    elif study == "keio":
        for i, gs in d["gene_set"].fillna("").items():
            tok = gs.split(":")[0].strip()
            b = str(d.at[i, "b_number"] or "").strip()
            out.append(dict(token=b or tok, alt=tok, role="del",
                            obs=d.at[i, "obs_id"]))
    elif study == "eydallin":
        for i in d.index:
            norm = str(d.at[i, "gene_norm"] or "").strip()
            orig = str(d.at[i, "gene"] or "").strip()
            out.append(dict(token=norm or orig, alt=orig, role="del", obs=orig))
    else:
        for i in d.index:
            out.append(dict(token=str(d.at[i, "gene"] or "").strip(),
                            role=str(d.at[i, "role"] or "").strip(),
                            ec=str(d.at[i, "ec"] or "").strip(),
                            obs=str(d.at[i, "mnxr"] or "")))
    return out


def _uniprot(query: str, *, size: int = 1) -> list[dict]:
    url = (f"{UNIPROT}?query={urllib.parse.quote(query)}"
           f"&format=json&size={size}&fields=accession,id,gene_names,protein_name,"
           f"organism_name,sequence,reviewed")
    req = urllib.request.Request(url, headers={"User-Agent": "fabfos-benchmark/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read()).get("results", [])


def uniprot_by_gene(symbol: str, *, organisms=()) -> tuple[dict | None, str]:
    orgs = [str(o).strip() for o in organisms if str(o).strip()] or ["Escherichia coli"]
    attempts = []
    for reviewed, method in ((" AND reviewed:true", "uniprot_gene_organism_reviewed"),
                             ("", "uniprot_gene_organism")):
        for org in orgs:
            q = '"' + org.replace('"', "") + '"'
            attempts.append((f"gene:{symbol} AND organism_name:{q}{reviewed}", method))
    for q, method in attempts:
        hits = _uniprot(q)
        if hits:
            return hits[0], method
    return None, "no_organism_match"


def load_het_curation(path: Path) -> dict:
    if not path.exists():
        return {}
    out, by_label = {}, {}
    with path.open() as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            acc = (r.get("uniprot") or "").strip()
            if not acc:
                continue
            v = (acc, (r.get("organism_resolved") or "").strip(),
                 (r.get("seq") or "").strip(), (r.get("reason") or "").strip())
            out[((r.get("gene") or "").strip(), (r.get("source") or "").strip())] = v
            by_label.setdefault((r.get("gene") or "").strip(), []).append(v)
    for label, vs in by_label.items():
        if len({v[0] for v in vs}) == 1:
            out.setdefault((label, ""), vs[0])
    return out


def uniprot_by_ec(ec: str, name: str) -> tuple[dict | None, str]:
    for q, method in ((f"ec:{ec} AND reviewed:true", "uniprot_ec"),
                      (f"ec:{ec}", "uniprot_ec_trembl")):
        hits = _uniprot(q, size=5)
        if not hits:
            continue
        for h in hits:
            genes = " ".join(g.get("geneName", {}).get("value", "")
                             for g in h.get("genes", []))
            if name.lower() in genes.lower():
                return h, method
        return hits[0], method
    return None, "uniprot_ec"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--network", action="store_true",
                    help="query UniProt for what the local proteomes did not resolve. "
                         "Latency-bound rather than heavy; runs where there is an "
                         "outbound route, which a cluster compute node is not.")
    ap.add_argument("--studies", default=",".join(STUDIES))
    ap.add_argument("--out", default=None)
    ap.add_argument("--het-table", default=str(HET_TABLE),
                    help="the heterologous curation curate_het_screen.py writes")
    ap.add_argument("--sleep", type=float, default=0.34,
                    help="between UniProt calls; their published limit is 3/s")
    a = ap.parse_args()

    out_root = Path(a.out).resolve() if a.out else OUT_ROOT
    studies = [s.strip() for s in a.studies.split(",") if s.strip()]

    index, seqs = index_proteomes()
    print(f"local index: {len(index):,} gene/locus keys over "
          f"{len(list(GENOMES.glob('*/genome/*.faa')))} proteomes")
    het = load_het_curation(Path(a.het_table))
    print(f"het curation: {len(het):,} keys from {a.het_table}"
          if het else
          f"het curation: ABSENT at {a.het_table} -- every heterologous label falls "
          f"through to UniProt or to unresolved. Run curate_het_screen.py --publish.")
    print()

    grand = {}
    for study in studies:
        rows = tokens_for(study)
        seen: dict[str, dict] = {}
        for r in rows:
            tok = r["token"]
            if not tok:
                continue
            rec = seen.setdefault(tok, dict(
                token=tok, alt=r.get("alt", ""), ec=r.get("ec", ""),
                cls=classify(tok, r.get("role", "")), roles=set(), n=0,
                sources=set(), accession="", host="", method="", organism="", note=""))
            rec["roles"].add(r.get("role", ""))
            rec["sources"].update(r.get("sources", {}))
            rec["n"] += 1
            if rec["cls"] == "pathway_shorthand" and \
                    classify(tok, r.get("role", "")) != "pathway_shorthand":
                rec["cls"] = classify(tok, r.get("role", ""))

        for rec in seen.values():
            if rec["cls"] == "pathway_shorthand":
                rec["method"] = "not_a_gene"
                rec["note"] = "a lumped conversion the readout is measured over"
                continue
            for src in sorted(rec["sources"]) + [""]:
                hit = het.get((rec["token"], src))
                if hit:
                    rec["accession"], rec["organism"], seq, how = hit
                    if seq:
                        seqs[rec["accession"]] = seq
                    rec["method"] = "curated_het"
                    rec["note"] = f"from the het curation ({how})"
                    break
            if rec["accession"]:
                continue
            for key in (rec["token"], rec["alt"]):
                if key and key.strip().lower() in index:
                    rec["accession"], rec["host"] = index[key.strip().lower()]
                    rec["method"] = "local"
                    rec["organism"] = rec["host"]
                    break

        todo = [r for r in seen.values() if not r["accession"]
                and r["cls"] in ("gene", "enzyme_name")]
        if a.network and todo:
            print(f"  {study}: {len(todo)} token(s) to UniProt")
            for rec in todo:
                hit = None
                if rec["cls"] == "gene" and rec["ec"]:
                    hit, rec["method"] = uniprot_by_ec(rec["ec"], rec["token"])
                elif rec["cls"] == "gene":
                    hit, rec["method"] = uniprot_by_gene(
                        rec["token"], organisms=sorted(rec["sources"]))
                elif rec["ec"]:
                    hit, rec["method"] = uniprot_by_ec(rec["ec"], rec["token"])
                else:
                    rec["method"] = "unresolved"
                    rec["note"] = ("free-text label absent from the het curation; add it "
                                   "there, not here -- see curate_het_screen.py")
                time.sleep(a.sleep)
                if hit:
                    rec["accession"] = hit["primaryAccession"]
                    rec["organism"] = hit.get("organism", {}).get("scientificName", "")
                    seqs[rec["accession"]] = hit.get("sequence", {}).get("value", "")
                elif rec["method"] != "unresolved":
                    rec["note"] = (f"no protein entry at any tier, reviewed or not "
                                   f"(from {rec['method']}); either absent from UniProt "
                                   f"or a non-coding gene, which has no protein at all")
                    rec["method"] = "unresolved"

        for rec in seen.values():
            if not rec["accession"] and rec["method"] not in ("not_a_gene", "unresolved"):
                rec["method"] = "unresolved"
                if rec["note"]:
                    pass
                elif rec["cls"] == "enzyme_name":
                    rec["note"] = ("free-text label absent from the het curation; add it "
                                   "there, not here -- see curate_het_screen.py's "
                                   "unresolved.tsv")
                else:
                    rec["note"] = ("not in the pinned proteomes; --network was not run")

        d = out_root / study
        d.mkdir(parents=True, exist_ok=True)
        with (d / "genes_resolution.tsv").open("w") as f:
            f.write("token\talt\tclass\troles\tn_rows\tmethod\taccession\torganism\t"
                    "ec\tnote\n")
            for rec in sorted(seen.values(), key=lambda r: (r["cls"], r["token"])):
                f.write("\t".join([
                    rec["token"], rec["alt"], rec["cls"],
                    ";".join(sorted(x for x in rec["roles"] if x)), str(rec["n"]),
                    rec["method"], rec["accession"], rec["organism"], rec["ec"],
                    rec["note"]]) + "\n")

        wrote = 0
        with (d / f"{study}.faa").open("w") as f:
            for rec in sorted(seen.values(), key=lambda r: r["token"]):
                s = seqs.get(rec["accession"], "")
                if not rec["accession"] or not s:
                    continue
                f.write(f">{study}|{rec['token']}|{rec['accession']} "
                        f"[method={rec['method']}] [organism={rec['organism']}]\n")
                for i in range(0, len(s), 60):
                    f.write(s[i:i + 60] + "\n")
                wrote += 1

        by_cls = {}
        for rec in seen.values():
            c = by_cls.setdefault(rec["cls"], dict(n=0, ok=0))
            c["n"] += 1
            c["ok"] += bool(rec["accession"])
        grand[study] = dict(tokens=len(seen), sequences=wrote, by_class=by_cls)
        parts = ", ".join(f"{k} {v['ok']}/{v['n']}" for k, v in sorted(by_cls.items()))
        print(f"{study:12s} {len(seen):4d} tokens -> {wrote:4d} sequences   [{parts}]")

    (out_root / "COVERAGE.json").write_text(json.dumps(grand, indent=2))
    print(f"\n-> {out_root}")
    for study, g in grand.items():
        gene = g["by_class"].get("gene", dict(n=0, ok=0))
        if gene["ok"] < gene["n"]:
            missing = [ln.split("\t")[0] for ln in
                       (out_root / study / "genes_resolution.tsv").read_text()
                       .splitlines()[1:]
                       if ln.split("\t")[2] == "gene" and ln.split("\t")[5] == "unresolved"]
            print(f"  {study}: {len(missing)} unresolved GENE token(s): {missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

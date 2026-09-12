from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

GEM_HOST = "e_coli_dh10b"
BORROWER = "e_coli_epi300"

BOUNDARY_AA = 20

EXPECTED_BROKEN = {"proV", "fhuA"}
EXPECTED_LOST_REACTIONS = {
    "PROabcpp", "CRNabcpp", "CRNDabcpp", "CTBTabcpp",
    "FE3HOXtonex", "FECRMtonex", "FEOXAMtonex",
}


def read_proteome(faa: Path) -> dict[str, str]:
    seqs: dict[str, list[str]] = {}
    pid = skip = None
    for line in faa.read_text().splitlines():
        if line.startswith(">"):
            pid = None
            for field in line.split("["):
                if field.startswith("protein_id="):
                    pid = field.split("=", 1)[1].rstrip("] ")
                    break
            if pid is None:
                pid = line[1:].split()[0]
            skip = pid in seqs
            if not skip:
                seqs[pid] = []
        elif pid is not None and not skip:
            seqs[pid].append(line.strip())
    return {k: "".join(v) for k, v in seqs.items()}


def read_cds(gbk: Path) -> list[dict]:
    out: list[dict] = []
    cur: dict = {}
    in_cds = False

    def flush():
        if in_cds and (cur.get("protein_id") or cur.get("pseudo")):
            out.append(dict(cur))

    for raw in gbk.read_text().splitlines():
        line = raw.strip()
        if raw[:5].strip() and not raw.startswith(" "):
            continue
        if line.startswith("CDS "):
            flush()
            cur.clear()
            in_cds = True
            continue
        if not in_cds:
            continue
        if line == "/pseudo":
            cur["pseudo"] = True
            continue
        for key, tag in (("locus", "/locus_tag="), ("old_locus", "/old_locus_tag="),
                         ("gene", "/gene="), ("protein_id", "/protein_id=")):
            if line.startswith(tag):
                cur.setdefault(key, line.split('"')[1])
    flush()
    return out


def index_by_id(cds: list[dict]) -> dict[str, str]:
    index: dict[str, str] = {}
    for c in cds:
        if not c.get("protein_id"):
            continue
        for k in ("locus", "old_locus", "gene"):
            if c.get(k):
                index.setdefault(c[k], c["protein_id"])
    return index


def pseudo_names(cds: list[dict]) -> set[str]:
    return {c[k] for c in cds if c.get("pseudo")
            for k in ("locus", "old_locus", "gene") if c.get(k)}


def model_genes(gem: Path) -> list[str]:
    import json
    m = json.loads(gem.read_text())
    return [g["id"] for g in m.get("genes", [])]


def model_rules(gem: Path) -> list[tuple[str, str]]:
    import json
    m = json.loads(gem.read_text())
    return [(r["id"], (r.get("gene_reaction_rule") or "").strip())
            for r in m.get("reactions", [])]


def rule_holds(rule: str, present: set[str]) -> bool:
    if not rule:
        return True
    expr = rule.replace("(", " ( ").replace(")", " ) ")
    py = " ".join(
        {"and": "and", "or": "or", "(": "(", ")": ")"}.get(t, str(t in present))
        for t in expr.split()
    )
    try:
        return bool(eval(py, {"__builtins__": {}}, {}))
    except SyntaxError:
        raise SystemExit(f"could not parse gene_reaction_rule {rule!r}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--genomes", type=Path, default=GENOMES)
    ap.add_argument("--gpr", type=Path, default=None,
                    help="a built ref::gpr_table_gem directory; if given, the two "
                         "hosts' tables are asserted equal on every column but `host`")
    a = ap.parse_args()

    gem_dir = a.genomes / GEM_HOST / "GEM"
    gems = sorted(gem_dir.glob("*.json"))
    if len(gems) != 1:
        print(f"FAIL: expected one model under {gem_dir}, found {[p.name for p in gems]}")
        return 1
    genes = model_genes(gems[0])
    print(f"{gems[0].name}: {len(genes):,} genes\n")

    cds, idx, prot = {}, {}, {}
    for host in (GEM_HOST, BORROWER):
        g = a.genomes / host / "genome"
        gbk = sorted(g.glob("*.gbk"))
        faa = sorted(g.glob("*.faa"))
        if len(gbk) != 1 or len(faa) != 1:
            print(f"FAIL: {host} does not have exactly one .gbk and one .faa under {g}")
            return 1
        cds[host] = read_cds(gbk[0])
        idx[host] = index_by_id(cds[host])
        prot[host] = read_proteome(faa[0])
        print(f"{host:16s} {len(cds[host]):,} CDS, {len(idx[host]):,} gene keys, "
              f"{len(prot[host]):,} proteins")

    unresolved = [g for g in genes if g not in idx[GEM_HOST]]
    resolved = [g for g in genes if g in idx[GEM_HOST]]
    print(f"\n{len(resolved):,}/{len(genes):,} model genes resolve in {GEM_HOST}")
    if unresolved:
        print(f"  {len(unresolved):,} unresolved here and therefore UNTESTED: "
              f"{unresolved[:10]}{' ...' if len(unresolved) > 10 else ''}")

    by_symbol = {c["gene"]: c["protein_id"] for c in cds[BORROWER]
                 if c.get("gene") and c.get("protein_id")}
    by_seq: dict[str, str] = {}
    for c in cds[BORROWER]:
        if not c.get("protein_id"):
            continue
        seq = prot[BORROWER].get(c["protein_id"])
        if seq:
            by_seq.setdefault(seq, c["protein_id"])

    copies = Counter((h, c["gene"]) for h in (GEM_HOST, BORROWER)
                     for c in cds[h] if c.get("gene") and c.get("protein_id"))
    symbol_for_pid = {c["protein_id"]: c.get("gene") for c in cds[GEM_HOST]
                      if c.get("protein_id")}

    pseudo = pseudo_names(cds[BORROWER])
    missing, changed, copy_diff, boundary, pseudogenised = [], [], [], [], []
    matched_by = Counter()
    for g in resolved:
        pid_a = idx[GEM_HOST][g]
        sa = prot[GEM_HOST].get(pid_a)
        sym = symbol_for_pid.get(pid_a)

        pid_b, how = None, None
        if sa is not None and sa in by_seq:
            pid_b, how = by_seq[sa], "sequence"
        elif sym and sym in by_symbol:
            pid_b, how = by_symbol[sym], "symbol"
        if pid_b is None:
            if sym in pseudo or g in pseudo:
                pseudogenised.append((g, sym))
            else:
                missing.append((g, sym, pid_a))
            continue
        matched_by[how] += 1

        if sym and copies[(GEM_HOST, sym)] != copies[(BORROWER, sym)]:
            copy_diff.append((g, sym, copies[(GEM_HOST, sym)], copies[(BORROWER, sym)]))
        sb = prot[BORROWER].get(pid_b)
        if sa is None or sb is None or sa == sb:
            continue
        d = abs(len(sa) - len(sb))
        (boundary if d <= BOUNDARY_AA else changed).append((g, sym, len(sa), len(sb)))

    print(f"  matched: {matched_by['sequence']:,} by exact protein sequence, "
          f"{matched_by['symbol']:,} by gene symbol")

    print(f"\n  1a. absent from {BORROWER}      : {len(missing):,}")
    print(f"  1b. PSEUDOGENE in {BORROWER}   : {len(pseudogenised):,}")
    print(f"  2. changed beyond {BOUNDARY_AA} aa        : {len(changed):,}")
    print(f"  3. copy number differs         : {len(copy_diff):,}")
    print(f"  4. annotation-boundary only    : {len(boundary):,}   (allowed)")
    for label, rows in (("absent", missing), ("pseudo", pseudogenised),
                        ("changed", changed), ("copy_diff", copy_diff)):
        for r in rows[:15]:
            print(f"     {label}: {r}")

    lost = []
    if pseudogenised or missing:
        broken = {g for g, _ in pseudogenised} | {g for g, _, _ in missing}
        broken |= {s for _, s in pseudogenised if s} | {s for _, s, _ in missing if s}
        full = set(genes)
        rules = model_rules(gems[0])
        for rid, rule in rules:
            if rule_holds(rule, full) and not rule_holds(rule, full - broken):
                lost.append((rid, rule))
        print(f"\n  reactions live under the model's own gene set that go dark once "
              f"{sorted(broken & full)} are removed: {len(lost)}")
        for rid, rule in lost[:20]:
            print(f"     {rid:<14} {rule}")

    problems = []
    if missing:
        problems.append(f"{len(missing)} model genes are absent from {BORROWER}; the "
                        f"borrowed model asserts reactions that strain cannot carry")
    found_broken = {s or g for g, s in pseudogenised} | {s or g for g, s, _ in missing}
    if found_broken != EXPECTED_BROKEN:
        problems.append(
            f"the broken-gene set changed: found {sorted(found_broken)}, declared "
            f"{sorted(EXPECTED_BROKEN)}. benchmark/host_gpr_gem.py drops a FIXED list of "
            f"reactions on the strength of this measurement, so the two have to agree.")
    found_lost = {rid for rid, _ in lost}
    if found_lost != EXPECTED_LOST_REACTIONS:
        problems.append(
            f"the lost-reaction set changed: found {sorted(found_lost)}, declared "
            f"{sorted(EXPECTED_LOST_REACTIONS)}. Update host_gpr_gem.py's EPI300_EDIT "
            f"together with this.")
    if changed:
        problems.append(f"{len(changed)} model genes differ by more than {BOUNDARY_AA} aa; "
                        f"those are not the same protein and the borrow is not licensed "
                        f"for them")
    if copy_diff:
        problems.append(f"{len(copy_diff)} model genes differ in copy number")

    if a.gpr is not None:
        import pandas as pd
        pa = a.gpr / "hosts" / GEM_HOST / "gpr_gem.parquet"
        pb = a.gpr / "hosts" / BORROWER / "gpr_gem.parquet"
        if not (pa.exists() and pb.exists()):
            problems.append(f"--gpr given but {pa} or {pb} is missing")
        else:
            da, db = pd.read_parquet(pa), pd.read_parquet(pb)
            cols = [c for c in da.columns if c != "host"]
            same = (da[cols].reset_index(drop=True)
                    .equals(db[cols].reset_index(drop=True)))
            print(f"\n  gpr tables: {len(da):,} ({GEM_HOST}) vs {len(db):,} ({BORROWER}) "
                  f"rows; identical on every column but `host`: {same}")
            if not same:
                problems.append(
                    f"{BORROWER}'s table is not {GEM_HOST}'s. They read the same model "
                    f"through the same resolver and the only measured difference is "
                    f"seven transport reactions, which are outside the benchmark's atom "
                    f"universe -- so any difference here is invented rather than "
                    f"measured.")

    print()
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print(f"the edit list over {gems[0].stem}'s gene set is exactly "
              f"{sorted(EXPECTED_BROKEN)}, costing {len(EXPECTED_LOST_REACTIONS)} "
              f"reactions -- ALL OF THEM TRANSPORT, which bench_universe subtracts from "
              f"the atom universe, so the borrow is licensed and EPI300's GPR table is "
              f"DH10B's read faithfully under the EPI300 tag")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

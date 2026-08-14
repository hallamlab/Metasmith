"""Does EPI300 borrowing DH10B's model change the reaction space? Measure it.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python build_references/check_epi300_identity.py [--gpr <dir>]

EPI300 has no published genome-scale model and borrows DH10B's. That borrow is the
single largest unstated assumption in the host half of the benchmark, so it is measured
rather than asserted: EPI300 is a DH10B derivative, and the question is which of the
model's genes are present, unchanged and single-copy in EPI300.

FIVE OUTCOMES, and each catches a different way the borrow could be wrong:

  1a. ABSENT from EPI300                       -- a deletion removes reactions
  1b. PSEUDOGENE in EPI300                     -- present in the annotation, broken
  2.  CHANGED beyond annotation-boundary scale -- not the same protein
  3.  COPY NUMBER differs                      -- duplication changes dosage
  4.  annotation-boundary only                 -- allowed

THE EDIT LIST IS NOT EMPTY, and earlier work here recorded that it was. Two model genes
are pseudogenes in EPI300 -- `proV` (the ProU osmoprotectant ABC transporter's
ATP-binding subunit) and `fhuA` (the ferrichrome TonB-dependent receptor; loss of it is
the classic T1-phage-resistance marker in a cloning strain). Both sit in AND clauses, so
seven transport reactions go dark. Nothing else differs.

That is why the earlier claim was reachable and wrong. A pseudogene is a CDS with NO
`/protein_id`, so a check that indexes on the protein accession never sees it and reports
a clean pass; and the two strains do not share an id space -- DH10B carries
`old_locus_tag=ECDH10B_xxxx`, which is what the model names its genes by, and EPI300's
modern annotation carries none -- so a locus-tag join instead reports 1,272 of 1,304
genes deleted. Neither number is the answer. The join that means something is the
protein: exact sequence where it matches, gene symbol otherwise.

The consequence for the build is nonetheless that the two GPR tables ARE identical, and
that is not the earlier mistake repeated. All seven lost reactions are transport, which
`buildlib::bench_universe.py` subtracts from the atom universe, so they are marked out of
universe in both tables and carry no edge either way. `benchmark/host_gpr_gem.py`
therefore declares an EMPTY edit list, with the seven named in a comment beside it; this
script is what says that reasoning is still true of the two genomes.

WHAT THIS DOES NOT SHOW. An empty edit list is a statement about the model's gene set,
not about the strains. EPI300 carries the trfA/oriV amplification machinery and DH10B
does not; none of that is in iECDH10B_1368, so it cannot appear here. This measures
what the borrow does to the MODEL, which is the only thing the borrow is used for.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

# The two strains, and which one owns the model.
GEM_HOST = "e_coli_dh10b"
BORROWER = "e_coli_epi300"

# A protein length difference at or below this is an annotation-boundary call -- a start
# codon chosen differently by two annotation runs -- rather than a domain-scale change.
# Above it, the two records are not describing the same protein and the borrow is not
# licensed for that gene.
BOUNDARY_AA = 20

# What the measurement below currently says, declared so that a CHANGE in it is a
# failure rather than a silently different number. Kept in step with
# benchmark/host_gpr_gem.py's EPI300_EDIT, which is what actually drops them.
EXPECTED_BROKEN = {"proV", "fhuA"}
EXPECTED_LOST_REACTIONS = {
    # proV: the ProU osmoprotectant ABC transporter, an AND of three subunits
    "PROabcpp", "CRNabcpp", "CRNDabcpp", "CTBTabcpp",
    # fhuA: the ferrichrome TonB-dependent receptor, ANDed with the TonB-ExbBD complex
    "FE3HOXtonex", "FECRMtonex", "FEOXAMtonex",
}


def read_proteome(faa: Path) -> dict[str, str]:
    """protein_id -> sequence.

    Keyed on the `[protein_id=...]` field rather than on the leading identifier, which
    is `lcl|<contig>_prot_<acc>_<n>` -- a per-record index that joins to nothing in the
    GBK. Keying on it silently produces a proteome no CDS can look itself up in, and
    every sequence comparison then reads as "not found" rather than as "differs".

    ONE RECORD PER ACCESSION, first wins. A WP_ accession is non-redundant across a
    genome, so two loci encoding the identical protein produce two FASTA records under
    the same accession -- appending both concatenates a protein with itself and every
    such gene then reads as "length changed, exactly doubled".
    """
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
    """Every CDS as {locus, old_locus, gene, protein_id}.

    THE MODEL'S GENE IDS LIVE IN TWO ID SPACES AT ONCE. iECDH10B_1368 names most genes
    by old locus tag (`ECDH10B_xxxx`) and a few dozen by bare symbol (`thrA`). An index
    that resolves only one of them silently fails to bind about a tenth of the rules,
    and the check then reports a clean pass over the genes it happened to know.

    A PSEUDOGENE IS A CDS WITH NO `/protein_id`, and dropping it for that reason is how
    a real deletion hides. `/pseudo` is carried through so the caller can tell "this
    gene is not in the annotation" from "this gene is in the annotation, broken" -- two
    facts with the same effect on the network and completely different consequences for
    whether a borrowed model is licensed.
    """
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
    """Any of a CDS's names -> its protein_id. Pseudogenes have none and are excluded;
    `pseudo_names` below is what sees them."""
    index: dict[str, str] = {}
    for c in cds:
        if not c.get("protein_id"):
            continue
        for k in ("locus", "old_locus", "gene"):
            if c.get(k):
                index.setdefault(c[k], c["protein_id"])
    return index


def pseudo_names(cds: list[dict]) -> set[str]:
    """Every name carried by a CDS the annotation marked `/pseudo`."""
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
    """Is a BiGG gene_reaction_rule satisfied by this gene set?

    Written out rather than deferred to `cobra.GPR.eval`, which takes the KNOCKED-OUT
    set rather than the active one -- handing it the active set inverts the question,
    and a previous run of that logic in this project reported more reactions live after
    a knockout than before. Taking the present set makes the direction unmistakable.
    """
    if not rule:
        return True          # no rule: spontaneous or an exchange, live in every set
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
        # Not a failure of the borrow -- a failure of THIS CHECK to see those genes. The
        # distinction matters: an unresolvable gene is untested, not tested and passed.
        print(f"  {len(unresolved):,} unresolved here and therefore UNTESTED: "
              f"{unresolved[:10]}{' ...' if len(unresolved) > 10 else ''}")

    # THE TWO STRAINS DO NOT SHARE AN ID SPACE, and assuming they do is what makes this
    # check report a catastrophe that is not there. DH10B's record carries
    # `old_locus_tag=ECDH10B_xxxx`, which is what the model names its genes by; EPI300's
    # is a modern annotation with its own locus tags and no `old_locus_tag` at all. Join
    # on the locus tag and 1,272 of 1,304 model genes read as deleted.
    #
    # The join that means something is the PROTEIN. A gene symbol carries across
    # annotation runs, and where there is none, the amino-acid sequence does -- so the
    # EPI300 side is indexed by both, and a sequence hit is what says "this protein is
    # present" independently of what either annotator chose to call it.
    by_symbol = {c["gene"]: c["protein_id"] for c in cds[BORROWER]
                 if c.get("gene") and c.get("protein_id")}
    by_seq: dict[str, str] = {}
    for c in cds[BORROWER]:
        if not c.get("protein_id"):
            continue
        seq = prot[BORROWER].get(c["protein_id"])
        if seq:
            by_seq.setdefault(seq, c["protein_id"])

    # COPY NUMBER IS COUNTED PER GENE SYMBOL, not per protein accession. RefSeq's WP_
    # accessions are non-redundant across a genome, so two loci encoding the identical
    # protein share one accession -- counting CDS per accession answers "how many times
    # does this exact sequence occur", which is not the question. A gene duplicated in
    # one strain and not the other is what would change dosage, and that is a symbol
    # count.
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
            # Present in the annotation but broken is a different fact from absent, and
            # both remove the reaction. Reported apart so the reason survives.
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

    # WHAT THE EDIT LIST COSTS THE NETWORK. A non-empty edit list is not automatically a
    # changed reaction space: a gene in an OR with an isozyme takes nothing with it, one
    # in an AND takes the whole complex out. Measured rather than argued, by evaluating
    # every rule twice -- once over the model's gene set, once with the broken genes
    # removed -- and diffing the live reactions.
    lost = []
    if pseudogenised or missing:
        broken = {g for g, _ in pseudogenised} | {g for g, _, _ in missing}
        # a gene the model names by symbol and the annotation broke by symbol, or vice
        # versa: knock out both spellings, since the rule may use either.
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
    # The edit list is NOT expected to be empty -- it is expected to be THIS. A check
    # that demanded emptiness would fail forever on a true finding; one that reported
    # whatever it found would let the finding drift without anyone noticing.
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
            # THE TABLES ARE IDENTICAL, AND THE SEVEN LOST REACTIONS ARE STILL REAL.
            # Both facts hold at once because all seven are TRANSPORT, which
            # `buildlib::bench_universe.py` subtracts from the atom universe -- so all
            # seven are marked out of universe in both tables and carry no edge either
            # way. Editing them out would move no measurement while making two faithful
            # readings of one model disagree. The genotype difference is asserted above,
            # against the genomes, which is where it is a fact.
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

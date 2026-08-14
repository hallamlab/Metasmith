"""Two recovered inserts against the long-read assembly of the same physical clone.

SPEC
  Per clone, a BLAST dot-plot: x is the Plasmidsaurus hybrid insert, y is the
  insert this pipeline recovered from pooled short reads. Each HSP is a segment,
  a dot below 500 bp. The reference is reverse-complemented and rolled so the
  dominant diagonal starts at the origin, which puts any un-recovered tail at the
  right edge; that tail is shaded between dashed lines and annotated with the
  fraction of the molecule it is.

  This is the only independent view of a recovered molecule in the run. Two
  clones, picked because they were pickable -- it says a recovered insert matches
  the thing it came from, not how often that is true.

WHICH INSERT IS WHICH CLONE IS MEASURED, NOT NAMED
  The archived version of this figure named its two inserts by an id space this
  run retired (`C00198` / `C00126`), and those ids do not map onto anything here.
  So each clone's counterpart is found by aligning the clone against all 170
  inserts and taking the one that covers the most of it -- which is the honest
  form of the claim anyway.

PREPARING THE REFERENCE
  Each Plasmidsaurus file holds the ~4.5 Mb host chromosome and the fosmid; the
  fosmid is the shorter record. Two things then happen to it before it can be
  compared:
    * one assembled as a 2x tandem dimer, an artefact of assembling a circular
      molecule, and is collapsed to a single unit;
    * both still carry the ~7.9 kb pCC1FOS backbone, which the recovered inserts
      have had cut off. It is located by BLAST and excised -- the insert is the
      largest circular gap between the backbone footprints, which keeps it
      contiguous whether the backbone sits inside it or across the origin.
  The vector file's FIRST record only: its second is a host chromosome, and using
  the pair excises host sequence from everything it touches.

INPUT   data/fabfos/runs/scadc_fosmids/plasmidsaurus/*.fasta
        data/fabfos/runs/scadc_fosmids/sequences/inserts/inserts.fna
        data/originals/vector/pcc1.fna   (record 1)
ENV     mamba run -n figure-net python main/figures/inserts/clone_alignment.py
        needs blastn on PATH; FABFOS_BLAST_BIN points at it
OUT     cache/clone_alignment.{png,svg}
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import numpy as np                   # noqa: E402

import identity                      # for the blast binary resolution
from _common import CACHE, INK, INSERTS, PLASMIDSAURUS, VECTOR, save
from pieces import fr

WORK = CACHE / "clone_alignment"
DOT = 500          # HSPs shorter than this are drawn as a dot, not a line
VEC_MIN = 100      # shortest vector HSP counted as a backbone footprint
OUTFMT = "6 qstart qend sstart send length"

# Panel order and labels. The well is the identity that survives; the clone
# number is the one the archived figure used, kept so the two can be compared.
CLONES = [
    ("Clone 1", "well A1", "L9PH9L_2_SCADC_C2_D19_2_wellA1.polished-assembly.fasta"),
    ("Clone 2", "well D4", "L9PH9L_1_SCADC_C1_D19_3_wellD4.polished-assembly.fasta"),
]


def blast(qseq, sseq, tag):
    """query (y) vs subject (x). -> [(qstart, qend, sstart, send, length)]."""
    ws = WORK / tag
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "q.fna").write_text(f">q\n{qseq}\n")
    (ws / "s.fna").write_text(f">s\n{sseq}\n")
    rows = fr.blast_hsps(ws / "q.fna", ws / "s.fna", ws / "blast",
                         evalue="1e-5", task="blastn", outfmt=OUTFMT)
    return [(int(r["qstart"]), int(r["qend"]), int(r["sstart"]), int(r["send"]),
             int(r["length"])) for r in rows]


def fosmid_record(path):
    """The fosmid, not the host chromosome: the shorter of the two records."""
    recs = fr.read_fasta(path)
    return min(recs, key=lambda r: len(r[2]))[2]


def collapse_tandem(seq, tag):
    """A circular molecule assembled as a 2x repeat -> one unit."""
    hsps = blast(seq, seq, f"self_{tag}")
    L = len(seq)
    for qs, qe, ss, se, ln in hsps:
        if ln >= L:
            continue
        if (qe > qs) == (se > ss) and ln >= 0.35 * L:
            period = abs(qs - ss)
            if 0.3 * L <= period <= 0.7 * L:
                print(f"  {tag}: {L:,} bp collapses to a {period:,} bp unit")
                return seq[:period]
    return seq


def strip_vector(seq, vecseq, tag):
    """Excise pCC1FOS: the insert is the largest circular gap between footprints."""
    L = len(seq)
    foot = fr.merge_intervals([(ss, se) for _q1, _q2, ss, se, ln
                               in blast(vecseq, seq, f"vec_{tag}") if ln >= VEC_MIN])
    if not foot:
        raise SystemExit(f"{tag}: no pCC1FOS footprint found -- is this a fosmid?")
    gaps = []
    for i, (_a, end_i) in enumerate(foot):
        if i + 1 < len(foot):
            start, stop = end_i + 1, foot[i + 1][0] - 1
            gaps.append((start, stop, stop - start + 1))
        else:                                   # the piece that wraps the origin
            start, stop = end_i + 1, foot[0][0] - 1
            gaps.append((start, stop, (L - end_i) + (foot[0][0] - 1)))
    start, stop, n = max(gaps, key=lambda g: g[2])
    print(f"  {tag}: {sum(b - a + 1 for a, b in foot):,} bp of backbone excised, "
          f"insert {n:,} bp")
    return seq[start - 1:stop] if start <= stop else seq[start - 1:] + seq[:stop]


def match_insert(sseq, inserts, tag):
    """The recovered insert that covers the most of this clone. -> (id, sequence).

    One blast of the clone against the whole insert set, so the comparison every
    candidate is judged on is the same one.
    """
    ws = WORK / f"match_{tag}"
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "clone.fna").write_text(f">clone\n{sseq}\n")
    rows = fr.blast_hsps(ws / "clone.fna", INSERTS / "inserts.fna", ws / "blast",
                         evalue="1e-5", task="blastn",
                         outfmt="6 sseqid qstart qend length")
    by_insert = {}
    for r in rows:
        by_insert.setdefault(r["sseqid"], []).append(
            (int(r["qstart"]), int(r["qend"])))
    if not by_insert:
        raise SystemExit(f"{tag}: no recovered insert aligns to this clone")
    covered, name = max(
        ((sum(b - a + 1 for a, b in fr.merge_intervals(iv)), k)
         for k, iv in by_insert.items()), key=lambda t: t[0])
    seq = next(s for n, _d, s in inserts if n == name)
    print(f"  {tag}: matches {name} ({len(seq):,} bp), covering "
          f"{covered / len(sseq):.1%} of the clone")
    return name, seq


def orient(qseq, sseq, tag):
    """Flip and roll the reference so the dominant diagonal starts at the origin."""
    hsps = blast(qseq, sseq, f"{tag}_o1")
    dom = max(hsps, key=lambda h: h[4])
    if dom[3] < dom[2]:                          # dominant HSP is reversed
        sseq = fr.revcomp(sseq)
        hsps = blast(qseq, sseq, f"{tag}_o2")
        dom = max(hsps, key=lambda h: h[4])
    qs, _qe, ss, _se, _ln = dom
    origin = (ss - qs) % len(sseq)
    return sseq[origin:] + sseq[:origin]


def missing_intervals(hsps, L):
    """Reference intervals no HSP covers."""
    merged = fr.merge_intervals([(ss, se) for _q1, _q2, ss, se, _l in hsps])
    gaps, prev = [], 1
    for a, b in merged:
        if a > prev:
            gaps.append((prev, a - 1))
        prev = max(prev, b + 1)
    if prev <= L:
        gaps.append((prev, L))
    return gaps


def generate():
    identity._with_blast_on_path()
    inserts = fr.read_fasta(INSERTS / "inserts.fna")
    vecseq = fr.read_fasta(VECTOR)[0][2]        # record 1: the pCC1FOS backbone
    print(f"vector: {len(vecseq):,} bp; {len(inserts)} recovered inserts")

    fig, axes = plt.subplots(1, 2, figsize=(8.6, 4.4), dpi=300)
    for col, (title, well, fname) in enumerate(CLONES):
        ax = axes[col]
        tag = title.lower().replace(" ", "")
        print(f"{title} ({well})")
        sseq = collapse_tandem(fosmid_record(PLASMIDSAURUS / fname), tag)
        sseq = strip_vector(sseq, vecseq, tag)
        _name, qseq = match_insert(sseq, inserts, tag)
        sseq = orient(qseq, sseq, tag)
        hsps = blast(qseq, sseq, tag)
        L = len(sseq)

        for qs, qe, ss, se, ln in hsps:
            if ln < DOT:
                ax.plot((ss + se) / 2e3, (qs + qe) / 2e3, "o", ms=3.2, color=INK)
            else:
                ax.plot([ss / 1e3, se / 1e3], [qs / 1e3, qe / 1e3], "-", lw=1.8,
                        color=INK)

        gaps = missing_intervals(hsps, L)
        pct = sum(b - a + 1 for a, b in gaps) / L * 100
        sub = [(a, b) for a, b in gaps if b - a + 1 >= 100]
        if sub:
            x0 = min(a for a, _b in sub) / 1e3
            x1 = max(b for _a, b in sub) / 1e3
            ax.axvspan(x0, x1, color="#9e9e9e", alpha=0.30, lw=0)
            for x in (x0, x1):
                ax.axvline(x, ls="--", lw=1.0, color="#616161")
            ax.text((x0 + x1) / 2, len(qseq) / 2e3, f"{pct:.0f}% not recovered",
                    rotation=90, ha="center", va="center", fontsize=8,
                    color="#424242")

        ax.set_xlim(0, L / 1e3)
        ax.set_ylim(0, len(qseq) / 1e3)
        ax.set_xticks(np.arange(0, L / 1e3 + 1e-9, 10))
        ax.set_yticks(np.arange(0, len(qseq) / 1e3 + 1e-9, 10))
        ax.set_xlabel(f"{title}, {well} (kb)", fontsize=11)
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)
        if col == 0:
            ax.set_ylabel("FabFos (kb)", fontsize=11)
        else:
            ax.tick_params(left=False, labelleft=False)
            ax.spines["left"].set_visible(False)

        print(f"  {len(hsps)} HSPs; {pct:.1f}% of the clone not recovered")

    fig.tight_layout()
    save(fig, "clone_alignment")
    plt.close(fig)


def main():
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    generate()


if __name__ == "__main__":
    main()

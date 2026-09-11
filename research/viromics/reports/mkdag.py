CW = 6.72
def w_of(lines, pad=22, minw=96):
    return max(minw, round(max(len(s) for s in lines) * CW) + pad)

parts = []
rects = []
def node(x, y, lines, kind="t", h=None, w=None, cx=None):
    w = w or w_of(lines)
    h = h or (34 if len(lines) == 1 else 46)
    if cx is not None: x = cx - w / 2
    r = 17 if kind in ("d", "f", "g") else 3
    rects.append((x, y, w, h, lines[0]))
    parts.append(f'<g class="nd {kind}"><rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="{h}" rx="{r}"/>')
    if len(lines) == 1:
        parts.append(f'<text class="l1" x="{x+w/2:.1f}" y="{y+h/2+4:.1f}">{lines[0]}</text>')
    else:
        parts.append(f'<text class="l1" x="{x+w/2:.1f}" y="{y+19}">{lines[0]}</text>')
        parts.append(f'<text class="l2" x="{x+w/2:.1f}" y="{y+34}">{lines[1]}</text>')
    parts.append('</g>')
    return (x, y, w, h)

def edge(d, cls="e"): parts.append(f'<path class="{cls}" d="{d}" marker-end="url(#ah)"/>')
def plain(d, cls="e"): parts.append(f'<path class="{cls}" d="{d}"/>')
def cx_(n): return n[0] + n[2] / 2
def bot(n): return n[1] + n[3]
def mid(n): return n[1] + n[3] / 2

def hop(a, b):
    edge(f"M {a[0]+a[2]:.1f},{mid(a):.1f} L {b[0]-6:.1f},{mid(b):.1f}")

def drop(a, b, jog=None, cls="e"):
    x1, y1, x2, y2 = cx_(a), bot(a), cx_(b), b[1]
    if abs(x1 - x2) < 0.6:
        edge(f"M {x1:.1f},{y1} L {x2:.1f},{y2-6}", cls)
    else:
        my = jog if jog is not None else (y1 + y2) / 2
        edge(f"M {x1:.1f},{y1} L {x1:.1f},{my:.1f} L {x2:.1f},{my:.1f} L {x2:.1f},{y2-6}", cls)

def band(y, text, x=14): parts.append(f'<text class="band" x="{x}" y="{y}">{text}</text>')

# ---------------------------------------------------------------- band 1
band(24, "PER SAMPLE — the metagenomics template the library already ships")
reads = node(0, 36, ["paired reads"], "g", cx=90)
bbduk = node(0, 36, ["bbduk"], "t", cx=236)
mega  = node(0, 36, ["megahit"], "t", cx=364)
asm   = node(0, 36, ["assembly"], "d", cx=502)
astat = node(0, 36, ["assembly_stats"], "t", cx=664)
depth = node(0, 36, ["coverage + bam"], "d", cx=800)
hop(reads, bbduk); hop(bbduk, mega); hop(mega, asm); hop(asm, astat); hop(astat, depth)

# ---------------------------------------------------------------- band 2
band(100, "GENOME RECOVERY — untouched")
band(100, "VIRAL CALLING AND PER-CONTIG ANNOTATION — still per sample", x=444)
plain("M 424,90 L 424,404", "sep")

bin3 = node(14,  134, ["3 binners", "metabat2 · semibin2 · comebin"], "t", w=192, h=46)
aggr = node(14,  200, ["aggregator + CheckM2", "quality_bin_fasta"], "t", w=192, h=46)
gtdb = node(14,  266, ["skANI + GTDB-Tk r232", "taxonomy::gtdbtk"], "t", w=192, h=46)
cct  = node(228, 200, ["CCTyper", "crispr_spacers"], "n", w=182, h=46)
iph  = node(228, 266, ["iPHoP add_to_db", "iphop_augmented_db"], "n", w=182, h=46)
drop(bin3, aggr); drop(aggr, gtdb); hop(aggr, cct); hop(gtdb, iph)

# Every caller consumes contig_batch, and that is the whole reason the frozen set
# is not reachable by one: it is what keeps the merge from eating its own output.
batch = node(444, 134, ["contig_batch  ·  ~5 Mbp shards, headers sample-prefixed"], "d", w=426)
gen = node(444, 190, ["geNomad", "genes · taxonomy"], "t", w=138, h=46)
vs2 = node(590, 190, ["VirSorter2", "affi_contigs"], "t", w=138, h=46)
vib = node(736, 190, ["VIBRANT", "lifestyle · AMGs"], "n", w=134, h=46)
dvv = node(590, 252, ["DRAM-v", "annotations · distill"], "t", w=138, h=46)
for n in (gen, vs2, vib): drop(batch, n)
drop(vs2, dvv)

cand  = node(444, 314, ["candidate_virus  ·  contig, start, end, caller, score"], "d", w=426)
merge = node(444, 370, ["merge_candidate_calls  (g)",
                        "dedup identical · union overlapping, min start → max end"], "n", w=426, h=46)
drop(gen, cand); drop(vib, cand)
# VirSorter2 to the bar, routed left of DRAM-v rather than through it
edge(f"M {cx_(vs2):.1f},{bot(vs2)} L {cx_(vs2):.1f},220 L 560,220 L 560,306 L {cx_(cand):.1f},306 "
     f"L {cx_(cand):.1f},{cand[1]-6}")
drop(cand, merge)

hop(asm, astat)
edge(f"M {cx_(asm):.1f},{bot(asm)} L {cx_(asm):.1f},112 L {cx_(bin3):.1f},112 L {cx_(bin3):.1f},{bin3[1]-6}")
edge(f"M {cx_(asm):.1f},{bot(asm)} L {cx_(asm):.1f},112 L {cx_(batch):.1f},112 L {cx_(batch):.1f},{batch[1]-6}")
edge(f"M {cx_(depth):.1f},{bot(depth)} L {cx_(depth):.1f},80 L 874,80 L 874,{mid(bin3):.1f} "
     f"L {bin3[0]+bin3[2]+6:.1f},{mid(bin3):.1f}")

froz = node(0, 436, ["dereplicated_candidate_virus", "the frozen reference"], "f", cx=657, w=340, h=54)
drop(merge, froz)

FR = 512
plain(f"M 14,{FR} L 866,{FR}", "freeze")
LBL = "FROZEN — nothing below writes sequence"
tw = len(LBL) * 6.05 + 26
parts.append(f'<rect class="freezechip" x="{657-tw/2:.1f}" y="{FR-11}" width="{tw:.1f}" height="22" rx="11"/>')
parts.append(f'<text class="freezelbl" x="657" y="{FR+4}">{LBL}</text>')

# ---------------------------------------------------------------- band 3
band(534, "ON THE FROZEN SET — the cross-sample tools only, and every product is a leaf")
COLS = [14, 234, 454, 674]; CWID = 192; ROWY = [560, 630]
items = [
    ("spacer BLASTn",     "spacer_host_links"),
    ("iPHoP predict",     "host_prediction"),
    ("CheckV end_to_end", "contamination + QA"),
    ("MMseqs2 cov-1",     "votu_cluster_table"),
    ("seqkit fx2tab",     "contig_length_table"),
    ("MMseqs2 cov-0",     "precluster_table"),
    ("vConTACT3",         "vcontact3_assignments"),
    ("prodigal-gv → kofamscan", "kofamscan_results"),
]
fans = []
for i, (m, s) in enumerate(items):
    r, c = divmod(i, 4)
    fans.append(node(COLS[c], ROWY[r], [m, s], "n", w=CWID, h=52))

BUSY = 540
plain(f"M {cx_(froz):.1f},{bot(froz)} L {cx_(froz):.1f},{BUSY}")
plain(f"M {COLS[0]-10},{BUSY} L {COLS[3]-10},{BUSY}")
for c in range(4):
    rail = COLS[c] - 10
    last = max((r for r in range(len(ROWY)) if r * 4 + c < len(items)), default=-1)
    if last < 0: continue
    plain(f"M {rail},{BUSY} L {rail},{ROWY[last]+26}")
    for r in range(last + 1):
        edge(f"M {rail},{ROWY[r]+26} L {COLS[c]-6},{ROWY[r]+26}")

# the two cross-links from the MAG branch
edge(f"M {cct[0]+cct[2]:.1f},{mid(cct):.1f} L 418,{mid(cct):.1f} L 418,492 L {cx_(fans[0]):.1f},492 "
     f"L {cx_(fans[0]):.1f},{ROWY[0]-6}", "e xlink")
edge(f"M {cx_(iph):.1f},{bot(iph)} L {cx_(iph):.1f},500 L {cx_(fans[1]):.1f},500 "
     f"L {cx_(fans[1]):.1f},{ROWY[1]-6}", "e xlink")

H = 712
svg = (f'<svg viewBox="0 0 880 {H}" role="img" aria-label="Topology of the planned viromics pipeline">'
       '<defs><marker id="ah" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" '
       'orient="auto-start-reverse"><path class="ahp" d="M 0,0.8 L 7.2,4 L 0,7.2 z"/></marker></defs>'
       + "\n".join(parts) + "</svg>")
open("dag.svg", "w").write(svg)

# geometry check -- a diagram that quietly overlaps is worse than none
bad = 0
for i in range(len(rects)):
    for j in range(i + 1, len(rects)):
        ax, ay, aw, ah, an = rects[i]
        bx, by, bw, bh, bn = rects[j]
        if ax < bx + bw and bx < ax + aw and ay < by + bh and by < ay + ah:
            print(f"OVERLAP: {an!r} / {bn!r}")
            bad += 1
mx = max(x + w for x, y, w, h, _ in rects)
my = max(y + h for x, y, w, h, _ in rects)
print(f"{len(svg)} bytes, {len(fans)} fan cards, {len(rects)} boxes, "
      f"{bad} overlaps, bounds {mx:.0f}x{my:.0f} in 880x{H}")

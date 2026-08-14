"""FabFos insert recovery: map the backbone, rectify against the assembly graph, dedup.

Three stages, one module, because they share the provenance convention and the
length accounting that makes the cut checkable.

    map       assemblies + pCC1fos backbone         -> junctions.tsv
    rectify   assemblies + junctions + graphs       -> split_contigs.fna
    dedup     split_contigs.fna                     -> inserts.fna + insert_metadata/

THE FOUR FILES THAT SURVIVE
Everything above is scratch except four artifacts, and the set is chosen so that
each one answers a question the others cannot:

    inserts.fna                        the insert set
    insert_metadata/inserts.csv        centroid, length, ends, absorbed
    insert_metadata/membership.csv     contig, centroid, mi
    insert_metadata/junctions.tsv      the backbone blast, verbatim

The intermediate tables that used to ship beside them -- the derived junction
map, the per-piece closure report, the per-contig accounting -- were each a
restatement of something the four still carry, or of a check that is enforced in
process and therefore does not need to be re-readable. `rectify` still raises
when the length accounting fails; writing the table it checked is now opt-in
(`--out-accounting`) rather than a product.

IDENTIFIERS ARE FULLY QUALIFIED
`k141_9` names a different contig in each of 35 pools and `NODE_1_...` names one
in each spades run, so an id is only meaningful with its pool and assembler
attached. Every id these four files use is therefore

    pool:assembler:contig                 a contig
    pool:assembler:contig:start-end       a piece cut out of one

which is what lets `membership.csv` carry provenance in the `contig` column
itself instead of in three more columns, and lets `centroid` be drawn from the
same space as `contig` -- a centroid IS one of the pieces. `start > end` is a
piece that wraps the origin of a circular contig.

WHY MAPPING AND CUTTING ARE NOW SEPARATE
They were one step, and the step decided both where the vector was and what that
meant. The meaning turned out to be the fragile half. Classifying each HSP
`terminal` or `internal` by its distance from a contig end called 78 contigs
chimeric across the 35 SCADC pools when only 3 were: the recurring shape is
`vector | insert | 154 bp backbone | 207 bp | vector`, whose small detached
fragment sits 1.5 kb inside a 53 kb contig and so reads `internal`, though it is
plainly part of the same terminal vector complex. The cut never used the label --
it merges every footprint and takes the complement -- so the product was right
and only the evidence column was wrong. Splitting the stages makes that
structural: `map` states where the backbone lands and nothing else, and every
interpretation of it lives in `rectify`, where the assembly graph is available to
check it against.

WHERE PROVENANCE COMES FROM
Never from a filename. `map` takes `--assembly PATH:ASSEMBLER:POOL`, so the
caller -- a metasmith transform, which knows the assembler from *which*
requirement a file arrived on and the pool from the read_metadata node it is
grouped by -- states it explicitly. `rectify` writes assembler and pool into
every emitted FASTA header, and `dedup` reads them back from there. Contig ids
collide across assemblers (`k141_144` vs `NODE_1_length_...`), so pooled records
are rekeyed to a stable synthetic id and the original is carried in `source=`.

THE MAP
The pCC1fos backbone is BLASTed against the pool's contigs, both assemblers in
one subject, and `junctions.tsv` is that blast's own tabular output with nothing
added and nothing dropped -- the only edit is that the subject id is written back
as the qualified contig id, since blast ran against a rekeyed copy. Columns are
blast's, in blast's order:

    qseqid sseqid sstart send pident length nident qlen slen

No strand column, no distance-from-end columns, no classification, no derived
junction position: each would be a restatement of two columns already there, and
the one time this file carried a derived column it carried a wrong one. The
`--min-hsp` floor is applied by `rectify` when it reads the file, not by the
blast that writes it, so re-cutting at a different floor does not mean re-mapping
and the evidence below the floor stays visible.

`--min-hsp` defaults to 50, not 100, and the difference is large. Measured over
the 35 SCADC pools on contigs >= 10 kb, backbone HSPs sort by length into two
populations and the SHORT one is the clean signal:

    aln 50-100 bp   527 HSPs   median identity 100.0%   100.0% within 500 bp of an end
    aln 100-200 bp  962 HSPs   median identity  96.1%    91.3% within 500 bp of an end
    aln >= 200 bp   554 HSPs   median identity  91.7%    86.3% within 500 bp of an end

A short PERFECT match flush against a contig end is exactly what a clone boundary
looks like when the assembly ran a little way into the vector and stopped; the
long ragged hits are the less specific ones. A floor of 100 discarded 527 of them
and took the number of >= 10 kb contigs carrying a terminal footprint from 512
down to 300. Nothing below 50 is left to gain (583 contigs at 50, 585 at 30 and
at 0) because blast's own e-value has already truncated there, and no 50-100 bp
hit on a long contig is mid-contig, so the lower floor adds no chimeric cuts.

The hit COUNT is also batch-dependent and the map cannot hide that: blast's
e-value scales with database size, so running all pools as one subject gives ~3.6k
terminal-proximal hits where the pipeline's own per-pool batching gives ~13k.
Retune `--min-hsp` against per-pool numbers, never against a convenience run.

RECTIFY
The interpretation stage. It merges the mapped footprints, decides circularity,
strips the join, cuts, and states how many ends of each emitted piece are closed.

  circularity   the assembler's own claim, read from its GRAPH, never inferred
                from a terminal repeat. spades: the walk in `contigs.paths` closes
                if a link joins its last node back to its first, and that link's
                overlap field states the exact join length to trim. megahit: a
                self-loop in the fastg, or the `flag` LOOP bit -- which is 0x2,
                not 0x1. 0x1 is `standalone`, and testing it called 3,012 of
                3,877 megahit contigs circular where 24 are: all 24 flag=3
                contigs carry a 141 bp terminal self-repeat (exactly k, zero
                variance), and of 2,988 flag=1 contigs eleven carry any repeat at
                all, mean 46 bp. Wrapping a linear contig fuses the two ends of
                one real insert, so the gate has to be right.

  the cut       footprints merged, complement emitted, vector dropped rather than
                handed to whichever neighbour is longer. A graph-confirmed circle
                is cut ROUND the origin so a clone linearised inside its own
                insert comes back as one insert instead of two halves. Pieces
                below `--min-piece` are dropped and recorded, so per contig
                    emitted bp + dropped-short bp + vector bp == contig length
                which `--out-accounting` writes and `verify` re-checks.

  closure       `closed_ends` in {0, 1, 2}, per emitted piece, with the evidence
                for each end named. An end is closed when the assembly ran into
                the backbone there -- either because a vector footprint was cut
                off it (`vector`), or because the graph puts a backbone node
                adjacent to it (`graph`). A circle is closed at both ends by
                construction (`circular`). A boundary the cut invented in the
                middle of a contig is open, and so is a contig end the graph
                leaves dangling.

                The two assemblers answer this differently and both answers are
                wanted. spades keeps the backbone as its own graph node -- one
                6,679 bp node at 99.985% identity in a 891-node pool -- so
                adjacency to it is a clean, assembler-stated closure claim that
                needs no vector bases inside the contig at all. megahit merges
                the vector into the contig it flanks (a 24,956 bp node carrying
                6,818 bp of backbone), so it has no backbone node to be adjacent
                to and its closure comes from the footprints instead. A clone
                closed by both is closed on two independent grounds.

GRAPH JOINS
  spades   `contigs.paths` states the walk each contig is, so the join is exact.
           The GFA's P lines are named for SCAFFOLDS and must not be used for it.
  megahit  no contig->node statement exists -- contig2fastg's `ID_` field is just
           the record index (2i-1) and carries no contig identity -- so a contig
           is located in the graph by its SEQUENCE. Verified lossless: all 196
           contigs of a real pool match a fastg node verbatim, forward or reverse
           complement, and the 14 unmatched nodes are k-iteration contigs megahit
           filtered out of its own final set. An unmatched contig is REPORTED,
           never guessed at.

DEDUP
All-vs-all blastn over the pieces, complete-linkage agglomerative clustering,
representative = the LONGEST member. Two changes from the scadc original, both
forced by measurement on the 35 pools:

  the metric        was `nident/qlen`, which is asymmetric in a way that biases
                    everything downstream of it. A short piece contained in a
                    long one scores ~1.0 against it while the long one scores
                    only len_short/len_long back, so max-summed-similarity --
                    the old representative rule -- systematically rewarded being
                    SHORT: the representative was the shortest member of 113 of
                    166 multi-member clusters and 632 kb, 9.5% of the insert set,
                    was discarded. It is now `nident/max(qlen, slen)`, which is
                    symmetric and penalises length disagreement, and the loss
                    falls to 9.4 kb.

  the absorb pass   the new metric refuses to merge a fragment into its own
                    container, which is correct for a similarity but wrong for a
                    dedup: 105 of 291 representatives, 2.3 Mb, were >=99%
                    contained inside a longer representative. Containment is
                    therefore its own pass, run after clustering, using the
                    asymmetric ratio for the one job it is actually right for.
                    Two passes each doing one thing, rather than one metric
                    asked to do both.

Both numbers are summed over non-overlapping query intervals, not read off the
best single HSP -- see `_accepted_nident`. A single indel splits an otherwise
perfect alignment into two HSPs neither of which clears the cut, which put one
picked clone into the insert set six times over.

The absorb pass has a second rule for the other half of that: a piece closed at
NEITHER end is bounded by where the assembly stopped, not by the vector, so it
cannot be a clone -- and >=90% inside a piece closed at BOTH ends, it is a
shorter rendering of one. Closure was recorded from the start and never used;
this is the one place it decides anything, and it decides only whether a piece
that is already mostly contained is allowed to stand on its own.

Two passes, two tables. `membership.csv` gives every piece its centroid and the
`mi` it scores against that centroid -- `nident/max(qlen, slen)`, the clustering
metric, reported even for the pieces that reached their centroid by containment
rather than by clustering. That is the point of reporting it: a member sitting at
`mi` 0.4 under a centroid did not cluster with it, it was absorbed into it, and
the value says so.

`inserts.csv`'s `absorbed` column is the other half of that story and the only
fact in the four files that nothing else states. Clustering produces one
representative per cluster; the containment pass then folds whole clusters into a
longer representative, and `absorbed` names the representatives that were folded
into this one (`;`-separated, empty for most). It is not a count of members --
`membership.csv` has that -- it is which OTHER clones this insert swallowed, and
therefore where to look when an insert's member list is more diverse than a
single clone should be.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

# blastn/makeblastdb are looked up on PATH; the transform supplies them from the
# blast container, a direct run from an env that has them.
BLAST_HSP_FMT = "6 qseqid sseqid sstart send pident length nident qlen slen"
# The all-vs-all carries the QUERY coordinates too, because its similarity sums
# HSPs and cannot tell a second alignment of the same region from a second region
# without them. It is deliberately a SECOND format: `junctions.tsv` is a shipped
# product written from the one above, pinned by checksum and declared in the type
# library, so widening that constant would silently widen the product.
AVA_HSP_FMT = "6 qseqid sseqid qstart qend sstart send pident length nident qlen slen"


# =====================================================================
# identifiers
# =====================================================================

def contig_key(pool, assembler, contig_id):
    """`pool:assembler:contig` -- the only globally meaningful name for a contig."""
    return f"{pool}:{assembler}:{contig_id}"


def piece_key(cid, start, end):
    """`pool:assembler:contig:start-end`. `start > end` wraps a circular origin."""
    return f"{cid}:{start}-{end}"


def split_contig_key(cid):
    """-> (pool, assembler, contig_id). Split from the LEFT twice: a contig id may
    contain anything, a pool name and an assembler name may not contain ':'."""
    parts = cid.split(":", 2)
    if len(parts) != 3:
        raise ValueError(f"not a qualified contig id: {cid!r}")
    return parts[0], parts[1], parts[2]


# =====================================================================
# fasta / intervals
# =====================================================================

def read_fasta(path):
    """-> [(id, description, sequence)], in file order."""
    out, name, desc, buf = [], None, "", []
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None:
                    out.append((name, desc, "".join(buf)))
                head = line[1:].rstrip("\n")
                parts = head.split(None, 1)
                name, desc, buf = parts[0], (parts[1] if len(parts) > 1 else ""), []
            else:
                buf.append(line.strip())
    if name is not None:
        out.append((name, desc, "".join(buf)))
    return out


def write_fasta(path, records, width=80):
    with open(path, "w") as fh:
        for name, desc, seq in records:
            fh.write(f">{name}" + (f" {desc}" if desc else "") + "\n")
            for i in range(0, len(seq), width):
                fh.write(seq[i:i + width] + "\n")


def revcomp(seq):
    return seq.translate(str.maketrans("ACGTNacgtn", "TGCANtgcan"))[::-1]


def merge_intervals(iv):
    """Merge 1-based inclusive intervals. Adjacency counts as overlap."""
    iv = sorted((min(a, b), max(a, b)) for a, b in iv)
    m = []
    for a, b in iv:
        if m and a <= m[-1][1] + 1:
            m[-1] = (m[-1][0], max(m[-1][1], b))
        else:
            m.append((a, b))
    return m


def complement_intervals(merged, length):
    """The 1-based inclusive gaps of `merged` over [1, length]. LINEAR, not circular."""
    gaps, prev = [], 1
    for a, b in merged:
        if a > prev:
            gaps.append((prev, a - 1))
        prev = max(prev, b + 1)
    if prev <= length:
        gaps.append((prev, length))
    return gaps


def complement_intervals_circular(merged, length):
    """The gaps of `merged` over a CIRCLE of circumference `length`.

    Returns (start, end) pairs where `start > end` means the arc wraps through the
    origin. Only ever applied to a contig the assembler's own GRAPH reported
    circular -- on a genuinely linear contig this fuses its two ends.
    """
    if not merged:
        return [(1, length)]
    n = len(merged)
    covered = sum(b - a + 1 for a, b in merged)
    gaps = []
    for i in range(n):
        cur_end = merged[i][1]
        nxt_start = merged[(i + 1) % n][0]
        # Modular gap length. This is what makes the three degenerate cases fall
        # out instead of needing to be special-cased: footprints adjacent through
        # the origin (ends at `length`, next starts at 1) and ordinary adjacent
        # footprints both give 0, and a single footprint covering the whole circle
        # gives 0 rather than a phantom full-length gap.
        glen = (nxt_start - cur_end - 1) % length
        if glen == 0:
            continue
        s = cur_end % length + 1
        e = (s + glen - 2) % length + 1
        gaps.append((s, e))
    total = sum(arc_length(a, b, length) for a, b in gaps)
    if total != length - covered:
        raise AssertionError(
            f"circular complement is {total} bp against {length - covered} expected "
            f"(circumference {length}, footprints {merged})"
        )
    return gaps


def arc_length(start, end, length):
    """Length of the 1-based inclusive arc start..end on a circle of circumference
    `length`; `start > end` wraps."""
    return (end - start + 1) if start <= end else (length - start + 1) + end


def arc_seq(seq, start, end):
    """Extract the arc start..end (1-based inclusive), wrapping if start > end."""
    return seq[start - 1:end] if start <= end else seq[start - 1:] + seq[:end]


def terminal_self_repeat(seq, lo=10, hi=400):
    """Longest W in [lo, hi] with seq[:W] == seq[-W:], else 0.

    Only a FALLBACK trim, for a contig the graph calls circular without stating an
    overlap. It is never the circularity gate: a 15-30 bp terminal self-repeat is
    common in linear sequence, and gating on one called 945 spades contigs
    circular where the graph calls far fewer.
    """
    for w in range(min(hi, len(seq) // 2), lo - 1, -1):
        if seq[:w] == seq[-w:]:
            return w
    return 0


def megahit_flag(description):
    """megahit's `flag` field, or None if the header does not carry one.

    Bit 0x1 is `standalone` and 0x2 is `loop`, so a closed isolated contig reads
    flag=3 and the LOOP bit is the circularity claim. Getting these the wrong way
    round is not cosmetic -- see the module docstring.
    """
    fields = dict(t.split("=", 1) for t in description.split() if "=" in t)
    if "flag" not in fields:
        return None
    try:
        return int(fields["flag"])
    except ValueError:
        return None


# =====================================================================
# blast
# =====================================================================

def _run(cmd, cwd):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({p.returncode}):\n{p.stderr[-4000:]}")
    return p.stdout


def blast_hsps(query_fa, subject_fa, work, threads=None, evalue="1e-5",
               perc_identity=None, task=None, outfmt=BLAST_HSP_FMT):
    """blastn query vs a db built from subject. -> list of dicts, one per HSP."""
    # blast runs with cwd=work, so every path it is handed must be absolute --
    # otherwise a relative --out-split resolves against the wrong directory and
    # makeblastdb reports a missing file that is sitting right there.
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    query_fa, subject_fa = Path(query_fa).resolve(), Path(subject_fa).resolve()
    _run(["makeblastdb", "-dbtype", "nucl", "-in", str(subject_fa), "-out", str(work / "db")], work)
    cmd = ["blastn", "-query", str(query_fa), "-db", str(work / "db"),
           "-evalue", str(evalue), "-outfmt", outfmt]
    if task:
        cmd += ["-task", task]
    if perc_identity is not None:
        cmd += ["-perc_identity", str(perc_identity)]
    if threads:
        cmd += ["-num_threads", str(threads)]
    keys = outfmt.split()[1:]
    rows = []
    for line in _run(cmd, work).splitlines():
        vals = line.split("\t")
        if len(vals) != len(keys):
            continue
        rows.append({k: v for k, v in zip(keys, vals)})
    return rows


def read_blast_tsv(path, outfmt=BLAST_HSP_FMT):
    """Read a blast -outfmt 6 table written by a separate step.

    A table written under a DIFFERENT `-outfmt` than the one asked for here reads
    as zero rows, not as an error, which is how a format change becomes an empty
    matrix downstream instead of a traceback. So a file with content but no
    parseable row raises: there are two formats in this module now and the wrong
    one has to fail loudly.
    """
    keys = outfmt.split()[1:]
    rows, seen = [], 0
    with open(path) as fh:
        for line in fh:
            if not line.strip():
                continue
            seen += 1
            vals = line.rstrip("\n").split("\t")
            if len(vals) == len(keys):
                rows.append(dict(zip(keys, vals)))
    if seen and not rows:
        raise ValueError(
            f"{path}: {seen:,} lines, none with {len(keys)} columns -- it was not "
            f"written under `-outfmt \"{outfmt}\"`")
    return rows


def check_backbone(backbone):
    """The vector reference must be ONE record.

    `vector/pcc1.fna` is 4.7 MB and carries the 7,930 bp backbone AND a 4.69 Mb
    EPI300 chromosome -- a host+vector depletion reference filed under the wrong
    name. BLASTing that as the backbone calls a junction on every host-derived
    contig, so this refuses rather than proceeding.
    """
    bb = read_fasta(backbone)
    if len(bb) != 1:
        raise ValueError(
            f"backbone {backbone} has {len(bb)} records, expected 1. A multi-record "
            f"'vector' file is a host+vector depletion reference, not the backbone."
        )
    print(f"[backbone] {bb[0][0]!r} {len(bb[0][2]):,} bp", flush=True)
    return bb[0][2]


# =====================================================================
# assembly graphs
# =====================================================================

def _flip(orient):
    return "-" if orient == "+" else "+"


def read_gfa(path):
    """spades GFA -> (nodes {id: seq}, adj {(id, orient): {(id, orient, overlap_bp)}}).

    `L a ao b bo <cigar>` joins a's ao-end to b's bo-start, and the same link read
    backwards joins b's flipped end to a's flipped one -- both directions are
    stored, so a walk can be extended from either of its ends.
    """
    nodes, adj = {}, {}
    with open(path) as fh:
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if not f:
                continue
            if f[0] == "S" and len(f) >= 3:
                nodes[f[1]] = f[2]
            elif f[0] == "L" and len(f) >= 6:
                a, ao, b, bo, cig = f[1], f[2], f[3], f[4], f[5]
                m = re.match(r"^(\d+)M$", cig)
                ov = int(m.group(1)) if m else 0
                adj.setdefault((a, ao), set()).add((b, bo, ov))
                adj.setdefault((b, _flip(bo)), set()).add((a, _flip(ao), ov))
    return nodes, adj


def read_spades_paths(path):
    """spades `contigs.paths` -> {contig_name: [(node_id, orient), ...]}.

    Forward strand only; the `NAME'` records are the reverse complement of the
    record above them and carry no extra information. A path spread over several
    lines is a scaffold spanning a gap -- the contig FASTA carries it as one
    record, so the subpaths are concatenated to match.
    """
    walks, name = {}, None
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("NODE") or line.startswith("EDGE"):
                name = line
                walks.setdefault(name, [])
                continue
            if name is None:
                continue
            walks[name].extend(
                (t[:-1], t[-1]) for t in line.rstrip(";").split(",") if t and t[-1] in "+-"
            )
    return {k: v for k, v in walks.items() if not k.endswith("'")}


def read_fastg(path):
    """megahit FASTG -> (nodes {name: seq}, adj {name: {name}}).

    Header shape is `>NODE:SUCC1,SUCC2;` or `>NODE;`. A name ending in `'` is the
    reverse-complement strand, and successors of `X` are what X's 3' end joins;
    successors of `X'` are therefore what X's 5' end joins.
    """
    nodes, adj, cur = {}, {}, None
    buf = []
    with open(path) as fh:
        for raw in fh:
            if raw.startswith(">"):
                if cur is not None:
                    nodes[cur] = "".join(buf)
                head = raw[1:].strip().rstrip(";")
                src, _sep, dst = head.partition(":")
                cur, buf = src, []
                adj[src] = {d for d in dst.split(",") if d}
            else:
                buf.append(raw.strip())
    if cur is not None:
        nodes[cur] = "".join(buf)
    return nodes, adj


def write_graph_nodes(nodes, work):
    """Write a graph's nodes out as the blast subject. -> the FASTA path.

    Separate from the call below so the blast between them can run in an image
    that has blastn while everything either side runs in one that has pandas --
    the same split `map-prep`/`map-write` exists for.
    """
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    node_fa = work / "graph_nodes.fna"
    write_fasta(node_fa, [(n, "", s) for n, s in nodes.items()])
    return node_fa


def backbone_graph_nodes(nodes, backbone, work, min_cover=0.9, min_pident=95.0,
                         threads=None, hits_path=None):
    """Which graph nodes ARE the backbone.

    A node qualifies only when the backbone covers at least `min_cover` of it: the
    node has to BE vector, not merely contain some. On a real pool the strict rule
    returns one 6,679 bp node out of 891; "carries any backbone HSP" returns 356,
    most of them incidental sub-200 bp similarity, and adjacency to those would
    call closure everywhere.

    `hits_path` is a blast table a separate step already produced; without one
    this blasts for itself, which is what a direct run wants.
    """
    work = Path(work)
    if hits_path is not None:
        hits = read_blast_tsv(hits_path)
    else:
        node_fa = write_graph_nodes(nodes, work)
        hits = blast_hsps(backbone, node_fa, work / "bb", threads=threads,
                          evalue="1e-10")
    cover = {}
    for h in hits:
        if float(h["pident"]) < min_pident:
            continue
        n = h["sseqid"]
        cover.setdefault(n, []).append((min(int(h["sstart"]), int(h["send"])),
                                        max(int(h["sstart"]), int(h["send"]))))
    keep = set()
    for n, iv in cover.items():
        covered = sum(b - a + 1 for a, b in merge_intervals(iv))
        if len(nodes[n]) and covered / len(nodes[n]) >= min_cover:
            keep.add(n)
    return keep


class GraphView:
    """What the assembler's graph says about one assembly's contigs.

    Answers two questions per contig -- is it a circle, and is either of its ends
    adjacent to the backbone -- over both assemblers, because the questions are
    the same and only the contig->node join differs. `unmatched` names the contigs
    the join could not place, so a silent miss is impossible.
    """

    def __init__(self, assembler, contigs, graph_path, paths_path=None,
                 backbone=None, work=None, threads=None, hits_path=None):
        self.assembler = assembler
        self.unmatched = []
        self._circ = {}     # contig -> (bool, overlap_bp, evidence)
        self._ends = {}     # contig -> (set 5' neighbours, set 3' neighbours)
        self._hits_path = hits_path
        if assembler == "spades":
            self._init_spades(contigs, graph_path, paths_path, backbone, work, threads)
        elif assembler == "megahit":
            self._init_megahit(contigs, graph_path, backbone, work, threads)
        else:
            raise ValueError(f"no graph reader for assembler {assembler!r}")

    def _backbone_nodes(self, nodes, backbone, work, threads):
        if not backbone and self._hits_path is None:
            return set()
        return backbone_graph_nodes(nodes, backbone, work, threads=threads,
                                    hits_path=self._hits_path)

    # -- spades: the walk is stated, so the join is exact -------------------
    def _init_spades(self, contigs, gfa, paths, backbone, work, threads):
        nodes, adj = read_gfa(gfa)
        walks = read_spades_paths(paths)
        self.backbone_nodes = self._backbone_nodes(nodes, backbone, work, threads)
        for name, _desc, _seq in contigs:
            w = walks.get(name)
            if not w:
                self.unmatched.append(name)
                self._circ[name] = (False, 0, "no_walk")
                self._ends[name] = (set(), set())
                continue
            first, last = w[0], w[-1]
            fwd = adj.get(last, set())
            back = adj.get((first[0], _flip(first[1])), set())
            closing = [ov for n, o, ov in fwd if n == first[0] and o == first[1]]
            if closing:
                self._circ[name] = (True, max(closing), "gfa_link")
            else:
                self._circ[name] = (False, 0, "gfa_open")
            self._ends[name] = ({n for n, _o, _ov in back}, {n for n, _o, _ov in fwd})

    # -- megahit: no stated join, so match on sequence ----------------------
    def _init_megahit(self, contigs, fastg, backbone, work, threads):
        nodes, adj = read_fastg(fastg)
        self.backbone_nodes = self._backbone_nodes(nodes, backbone, work, threads)
        by_seq = {}
        for n, s in nodes.items():
            by_seq.setdefault(s, n)
        for name, desc, seq in contigs:
            node = by_seq.get(seq)
            if node is None:
                node = by_seq.get(revcomp(seq))
                if node is not None:
                    node = node if node.endswith("'") else node + "'"
            flag = megahit_flag(desc)
            if node is None or node not in adj:
                self.unmatched.append(name)
                # The flag is still an assembler claim even with no node to stand
                # on, so it is not thrown away just because the join missed.
                loop = bool(flag & 0x2) if flag is not None else False
                self._circ[name] = (loop, 0, "flag_only" if loop else "no_node")
                self._ends[name] = (set(), set())
                continue
            rc = node[:-1] if node.endswith("'") else node + "'"
            fwd = {d.rstrip("'") for d in adj.get(node, set())}
            back = {d.rstrip("'") for d in adj.get(rc, set())}
            self_loop = node in adj.get(node, set())
            loop_bit = bool(flag & 0x2) if flag is not None else False
            if self_loop:
                self._circ[name] = (True, 0, "fastg_self_loop")
            elif loop_bit:
                self._circ[name] = (True, 0, "flag_loop")
            else:
                self._circ[name] = (False, 0, "fastg_open")
            self._ends[name] = (back, fwd)

    def circular(self, contig):
        return self._circ.get(contig, (False, 0, "no_graph"))

    def end_touches_backbone(self, contig):
        """-> (5' end adjacent to a backbone node, 3' end adjacent)."""
        back, fwd = self._ends.get(contig, (set(), set()))
        bb = {b.rstrip("'") for b in self.backbone_nodes}
        return (bool(back & bb), bool(fwd & bb))


# =====================================================================
# stage 1 -- map
# =====================================================================

JUNCTION_COLS = BLAST_HSP_FMT.split()[1:]   # blast's own columns, in blast's order


def parse_assembly_arg(spec):
    """`PATH:ASSEMBLER:POOL` -> (Path, assembler, pool). Split from the right so a
    path containing ':' is still readable."""
    parts = spec.rsplit(":", 2)
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"--assembly expects PATH:ASSEMBLER:POOL, got {spec!r}")
    path, assembler, pool = parts
    return Path(path), assembler, pool


def pool_contigs(assemblies, work):
    """Rekey one pool's contigs from every assembler into one FASTA.

    Ids collide across assemblers, so records are rekeyed `S000001`... and the
    stated assembler/pool plus the original id are carried in `contig_meta.json`.
    Nothing about the sequence is changed here -- the circular join is stripped in
    `rectify`, once the graph has said there is one.
    """
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    meta, pooled = {}, []
    i = 0
    for path, assembler, pool in assemblies:
        for name, desc, seq in read_fasta(path):
            i += 1
            key = f"S{i:06d}"
            meta[key] = dict(contig_id=name, description=desc, assembler=assembler,
                             source_pool=pool, length=len(seq))
            pooled.append((key, "", seq))
    if not pooled:
        raise ValueError("no contigs across the given assemblies")
    write_fasta(work / "pooled.fna", pooled)
    (work / "contig_meta.json").write_text(json.dumps(meta))
    print(f"[map] {len(pooled):,} contigs from {len(assemblies)} assemblies", flush=True)
    return work / "pooled.fna"


def write_junctions(work, out_junctions):
    """Re-key `backbone_hits.tsv` onto qualified contig ids. Nothing else.

    Written headerless and column-for-column as blast emitted it, because the
    file's contract is that it IS the blast output -- a header would be the first
    thing in it blast did not write. The pooled subject was rekeyed `S000001`...
    to keep ids unique across assemblers, so mapping `sseqid` back is the one
    substitution that has to happen for the table to mean anything outside the
    work directory it was produced in.
    """
    work = Path(work)
    meta = json.loads((work / "contig_meta.json").read_text())
    hsps = read_blast_tsv(work / "backbone_hits.tsv")
    cid = {k: contig_key(m["source_pool"], m["assembler"], m["contig_id"])
           for k, m in meta.items()}
    n = 0
    with open(out_junctions, "w") as fh:
        for h in hsps:
            row = dict(h)
            row["sseqid"] = cid[h["sseqid"]]
            fh.write("\t".join(row[k] for k in JUNCTION_COLS) + "\n")
            n += 1
    print(f"[map] {n:,} backbone HSPs -> {out_junctions}", flush=True)
    return n


def read_junctions(path, min_hsp=50):
    """`junctions.tsv` -> {qualified contig id: [(start, end), ...]}, filtered.

    This is where `--min-hsp` is applied: the file holds every HSP blast reported
    and the floor is a property of the cut, not of the evidence.
    """
    foot, kept, total = {}, 0, 0
    for h in read_blast_tsv(path):
        total += 1
        if int(h["length"]) < min_hsp:
            continue
        ss, se = int(h["sstart"]), int(h["send"])
        foot.setdefault(h["sseqid"], []).append((min(ss, se), max(ss, se)))
        kept += 1
    print(f"[rectify] {kept:,} of {total:,} backbone HSPs >= {min_hsp} bp "
          f"on {len(foot):,} contigs", flush=True)
    return foot


def call_map(assemblies, backbone, out_junctions, threads=None, work=None):
    """prep + blast + rekey in one process -- the direct-run entry point."""
    work = Path(work or "map_work")
    pooled = pool_contigs(assemblies, work)
    check_backbone(backbone)
    hsps = blast_hsps(backbone, pooled, work / "bb", threads=threads)
    with open(work / "backbone_hits.tsv", "w") as fh:
        for h in hsps:
            fh.write("\t".join(h[k] for k in JUNCTION_COLS) + "\n")
    return write_junctions(work, out_junctions)


# =====================================================================
# stage 2 -- rectify + closure
# =====================================================================

ACCT_COLS = ["contig", "raw_length", "join_overlap_bp",
             "contig_length", "circular", "circ_evidence", "graph_bb_5p", "graph_bb_3p",
             "vector_bp", "emitted_bp", "dropped_short_bp", "n_pieces", "n_dropped",
             "action", "balanced"]
CLOSURE_COLS = ["piece", "contig", "start", "end",
                "length", "action", "closed_ends", "evidence_5p", "evidence_3p"]


def graph_work(work, assembler):
    """Where one assembler's graph-node blast lives. One name, agreed by the prep
    step, the blast between them and the rectify that reads the result."""
    return Path(work) / f"graph_{assembler}"


def rectify_prep(graphs, work):
    """Write each assembler's graph nodes out for the backbone blast.

    The other half of the env split: this and `rectify` want pandas, the blast
    between them wants blastn. -> [(assembler, node FASTA path)].
    """
    out = []
    for assembler, (gpath, _ppath) in graphs.items():
        if assembler == "spades":
            nodes, _adj = read_gfa(gpath)
        elif assembler == "megahit":
            nodes, _adj = read_fastg(gpath)
        else:
            raise ValueError(f"no graph reader for assembler {assembler!r}")
        fa = write_graph_nodes(nodes, graph_work(work, assembler))
        print(f"[rectify-prep] {assembler}: {len(nodes):,} graph nodes -> {fa}",
              flush=True)
        out.append((assembler, fa))
    return out


def rectify(assemblies, junctions, out_split, out_closure=None, out_accounting=None,
            backbone=None, graphs=None, graph_hits=None, min_piece=1000, min_hsp=50,
            threads=None, work=None):
    """Merge the mapped footprints, cut, and state how many ends each piece closes.

    `graphs` is {assembler: (graph_path, paths_path_or_None)}. It is not optional:
    circularity and graph closure both come from it, and a rectify without it is
    the fragile FASTA-only inference this redesign removed.

    `graph_hits` is {assembler: blast table} from a separate blast step; without
    it each GraphView blasts the backbone against its own nodes.

    `out_closure` and `out_accounting` are debugging tables, not products. The
    accounting CHECK runs either way -- it raises below if the cut lost or
    duplicated a base -- so the file is only worth writing when something has
    already gone wrong and needs reading per contig.
    """
    import pandas as pd

    work = Path(work or "rectify_work")
    work.mkdir(parents=True, exist_ok=True)
    graphs = graphs or {}
    graph_hits = graph_hits or {}
    foot_by_cid = read_junctions(junctions, min_hsp=min_hsp)

    views, contigs_by_asm = {}, {}
    for path, assembler, pool in assemblies:
        recs = read_fasta(path)
        contigs_by_asm[assembler] = (recs, pool)
        if assembler not in graphs:
            raise ValueError(
                f"no assembly graph given for {assembler!r}; rectify reads circularity "
                f"and closure from the graph and will not infer them from the FASTA"
            )
        gpath, ppath = graphs[assembler]
        views[assembler] = GraphView(assembler, recs, gpath, ppath, backbone,
                                     graph_work(work, assembler), threads,
                                     hits_path=graph_hits.get(assembler))
        if views[assembler].unmatched:
            u = views[assembler].unmatched
            print(f"[rectify] {assembler}: {len(u):,} of {len(recs):,} contigs not "
                  f"located in the graph (e.g. {u[:3]})", flush=True)

    out_records, acct_rows, close_rows = [], [], []
    for assembler, (recs, pool) in contigs_by_asm.items():
        view = views[assembler]
        for name, desc, seq in recs:
            cid = contig_key(pool, assembler, name)
            raw_len = len(seq)
            circ, ov, circ_ev = view.circular(name)
            bb5, bb3 = view.end_touches_backbone(name)
            # The join overlap the graph states is stripped so footprints, the cut
            # and the accounting all run against the true circumference. Where the
            # graph calls a circle but states no overlap (megahit's fastg has no
            # overlap field), the terminal repeat is the fallback trim -- a trim,
            # never the gate.
            if circ and not ov:
                ov = terminal_self_repeat(seq)
            # A join overlap at or above half the contig is not a circumference,
            # it is a single-k-mer loop in the graph: spades emits 56 bp nodes
            # carrying a `55M` self-loop at k=55, and stripping the join off one
            # leaves a 1 bp "contig". 247 of 512 graph-circular contigs are this,
            # all <= 110 bp, none carrying vector and none reaching 10 kb -- so the
            # guard removes graph junk and cannot touch a fosmid. Recorded in
            # `circ_evidence` rather than dropped silently.
            if circ and ov * 2 >= raw_len:
                circ, ov, circ_ev = False, 0, f"{circ_ev}:degenerate_overlap"
            if circ and ov:
                seq = seq[:-ov]
            L = len(seq)
            iv = [(a, min(b, L)) for a, b in foot_by_cid.get(cid, []) if a <= L]
            merged = merge_intervals(iv) if iv else []
            vector_bp = sum(b - a + 1 for a, b in merged)

            if merged:
                gaps = (complement_intervals_circular(merged, L) if circ
                        else complement_intervals(merged, L))
                # `min_piece` guards against slivers the CUT created. It is
                # deliberately not a length filter on the assembly: this step cuts,
                # it does not select, and the dedup's own `--min-contig-length` is
                # where length policy lives.
                keep = [g for g in gaps if arc_length(g[0], g[1], L) >= min_piece]
                short = [g for g in gaps if arc_length(g[0], g[1], L) < min_piece]
                if any(a > b for a, b in keep):
                    action = "circularised"
                elif len(keep) > 1:
                    action = "split"
                elif len(keep) == 1:
                    action = "trimmed"
                else:
                    action = "dropped"
            else:
                # No footprint, so there is nothing to cut AT and nothing to
                # rotate to. `circularised` is reserved for a piece whose arc
                # actually wraps the origin; calling it that merely because the
                # graph says circle labelled 498 contigs circularised when 4 were,
                # 475 of them sub-200 bp graph loops that were never touched.
                keep, short = ([(1, L)] if L else []), []
                action = "kept"

            for a, b in keep:
                pid = piece_key(cid, a, b)
                e5, e3 = _piece_closure(a, b, L, merged, circ, bb5, bb3)
                n_closed = (e5 != "open") + (e3 != "open")
                out_records.append((
                    pid,
                    f"action={action} length={arc_length(a, b, L)} "
                    f"closed_ends={n_closed}",
                    arc_seq(seq, a, b),
                ))
                close_rows.append(dict(
                    piece=pid, contig=cid,
                    start=a, end=b, length=arc_length(a, b, L), action=action,
                    closed_ends=n_closed, evidence_5p=e5, evidence_3p=e3))

            emitted_bp = sum(arc_length(a, b, L) for a, b in keep)
            dropped_bp = sum(arc_length(a, b, L) for a, b in short)
            acct_rows.append(dict(
                contig=cid,
                raw_length=raw_len, join_overlap_bp=(ov if circ else 0),
                contig_length=L, circular=circ, circ_evidence=circ_ev,
                graph_bb_5p=bb5, graph_bb_3p=bb3,
                vector_bp=vector_bp, emitted_bp=emitted_bp, dropped_short_bp=dropped_bp,
                n_pieces=len(keep), n_dropped=len(short), action=action,
                balanced=(emitted_bp + dropped_bp + vector_bp == L)))

    write_fasta(out_split, out_records)
    clo = pd.DataFrame(close_rows, columns=CLOSURE_COLS)
    if out_closure:
        clo.to_csv(out_closure, index=False)
    acct = pd.DataFrame(acct_rows, columns=ACCT_COLS)
    if out_accounting:
        acct.to_csv(out_accounting, index=False)

    unbalanced = int((~acct["balanced"]).sum())
    print(f"[rectify] {len(out_records):,} pieces "
          f"({acct['action'].value_counts().to_dict()})", flush=True)
    if len(clo):
        print(f"[rectify] closed ends: {clo.closed_ends.value_counts().sort_index().to_dict()}",
              flush=True)
    print(f"[rectify] length accounting: {len(acct) - unbalanced:,}/{len(acct):,} balance",
          flush=True)
    if unbalanced:
        raise AssertionError(
            f"{unbalanced} contigs fail emitted + dropped_short + vector == length; "
            f"the cut lost or duplicated sequence"
        )
    return acct, clo


def _piece_closure(a, b, L, merged, circ, bb5, bb3):
    """Why each end of one emitted piece is closed, or `open`.

    Four values, no others: `vector`, `graph`, `circular`, `open`. A circle has no
    ends to leave open. Otherwise an end is closed when the assembly ran into the
    backbone there -- `vector` if a mapped footprint was cut off it, `graph` if it
    is an original contig end the graph puts next to a backbone node. A boundary
    the cut invented mid-contig is open, and so is a contig end the graph leaves
    dangling.

    There is deliberately no "near the end" category. A footprint within some
    tolerance of a contig end either got cut off that end -- in which case it is
    `vector` on its own evidence -- or it did not, in which case its proximity is
    a coincidence of coordinates. Tolerance-based closeness is exactly the
    reasoning the per-HSP terminal/internal call got wrong.
    """
    if circ and a > b:
        return "circular", "circular"

    def side(pos, is_5p):
        # a footprint immediately abutting this boundary is what created it
        if any((b_ + 1 == pos) if is_5p else (a_ - 1 == pos) for a_, b_ in merged):
            return "vector"
        at_contig_end = (pos <= 1) if is_5p else (pos >= L)
        if at_contig_end and (bb5 if is_5p else bb3):
            return "graph"
        return "open"

    return side(a, True), side(b, False)


# =====================================================================
# stage 3 -- dedup
# =====================================================================

INSERT_COLS = ["centroid", "length", "ends", "absorbed"]
MEMBER_COLS = ["contig", "centroid", "mi"]


def dedup_prep(split_fa, work, min_contig_len=10000):
    """Pool the long pieces under blast-safe keys, provenance off the piece id.

    The pieces are rekeyed `C00001`... for the all-vs-all only. Every table
    written at the end names them by their qualified piece id again -- the rekey
    exists because a blast subject id is a poor place to discover you have a
    length limit, not because the short name means anything.
    """
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    meta, pooled = {}, []
    i = 0
    for name, desc, seq in read_fasta(split_fa):
        if len(seq) < min_contig_len:
            continue
        f = dict(tok.split("=", 1) for tok in desc.split() if "=" in tok)
        i += 1
        key = f"C{i:05d}"
        # Not defaulted. Closure decides absorption now, and zero is the
        # absorb-ELIGIBLE value, so a missing token would quietly make every piece
        # a candidate fragment. `rectify` states it on every piece it emits.
        if "closed_ends" not in f:
            raise ValueError(
                f"{name}: no closed_ends= in the description -- pieces must come "
                f"from `rectify`, which states closure on every piece it emits")
        meta[key] = dict(piece=name, action=f.get("action", ""),
                         closed_ends=int(f["closed_ends"]),
                         length=len(seq))
        pooled.append((key, "", seq))
    if not pooled:
        raise ValueError(f"no pieces >= {min_contig_len} bp in {split_fa}")
    write_fasta(work / "pooled.fna", pooled)
    (work / "piece_meta.json").write_text(json.dumps(meta))
    print(f"[dedup] {len(pooled):,} pieces >= {min_contig_len:,} bp", flush=True)
    return work / "pooled.fna"


def _accepted_nident(hsps):
    """Identical bases per pair, summed over NON-OVERLAPPING query intervals.

    -> {(qseqid, sseqid): (nident, qlen, slen)}.

    The best single HSP is not enough. blast will not extend one alignment across
    a long gap, so two renderings of the same molecule that differ by a single
    indel come back as two HSPs and score as the larger fragment alone: the two
    assemblies of picked clone 1 differ by a 168 bp expansion of a G-rich tract
    and scored 0.900 against a 0.99 cut while being 99.98% identical over 31 kb.
    Summed they score 0.994 and merge, which is the whole fix.

    Non-overlapping and not simply summed, because a repeat aligns to the same
    query bases many times and summing those counts one region several times --
    that inflates identity above 1.0 and merges genuinely different molecules.
    Greedy from the largest HSP down rather than an optimal interval selection:
    the ordering only matters where alignments conflict, and taking the most
    identical bases first is the conservative resolution of a conflict.
    """
    by_pair = {}
    for h in hsps:
        by_pair.setdefault((h["qseqid"], h["sseqid"]), []).append(h)
    out = {}
    for k, group in by_pair.items():
        taken, total = [], 0
        for h in sorted(group, key=lambda x: -int(x["nident"])):
            a, b = sorted((int(h["qstart"]), int(h["qend"])))
            if any(a <= tb and ta <= b for ta, tb in taken):
                continue
            taken.append((a, b))
            total += int(h["nident"])
        g = group[0]
        out[k] = (total, int(g["qlen"]), int(g["slen"]))
    return out


def _similarity(hsps, index, mode):
    """Summed-HSP identity per pair, normalised two different ways.

    `mode="symmetric"` divides by max(qlen, slen): the clustering metric. It is
    symmetric and it penalises length disagreement, so a fragment does not read as
    identical to the thing that contains it.

    `mode="containment"` divides by qlen: how much OF THE QUERY the subject holds.
    Asymmetric on purpose -- it is the right question for the absorb pass and the
    wrong one for clustering.

    Both read the same accepted HSP set -- see `_accepted_nident`, which is where
    "how much of this pair actually aligns" is decided, once.
    """
    import numpy as np

    n = len(index)
    sim = np.zeros((n, n))
    pair = {}
    for (q, s), (nident, qlen, slen) in _accepted_nident(hsps).items():
        if q not in index or s not in index:
            continue
        denom = max(qlen, slen) if mode == "symmetric" else qlen
        # A summed count can exceed the denominator when the subject holds the
        # query twice over; identity is a fraction, so it is capped there.
        pair[(q, s)] = min(1.0, nident / denom) if denom else 0.0
    for (q, s), v in pair.items():
        i, j = index[q], index[s]
        if mode == "symmetric":
            sim[i, j] = max(sim[i, j], v)
            sim[j, i] = max(sim[j, i], v)
        else:
            sim[i, j] = max(sim[i, j], v)
    np.fill_diagonal(sim, 1.0)
    return sim


def _cluster_by_silhouette(sim):
    """Sweep the clustering's own threshold and keep whichever cut scores best.

    -> (labels, N, between_cluster_identity, silhouette).

    N is not chosen and then imposed -- the THRESHOLD is swept and N falls out of
    it, which is the only way to be sure every partition scored is one the
    clustering actually produces. Asking for k clusters instead (`maxclust`) is
    not the same sweep: the distance matrix is full of exact ties, every
    non-aligning pair sitting at 1.0, so many k are unrealisable and the ones that
    are get scored against a threshold that has to be recovered afterwards, off by
    a merge. Sweeping the distinct merge heights enumerates each distinct
    partition exactly once (291 of them here, against 554 nominally-reachable k)
    and gives its identity exactly, as `1 - height`.

    The tree is built ONCE, so this costs one linkage and one silhouette per
    partition rather than one clustering per candidate.

    The maximum is checked to be INTERIOR. An optimum sitting against either end
    of the range is an artifact of the range, not a choice, and the caller is told
    so rather than handed the edge value.
    """
    import numpy as np
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform
    from sklearn.metrics import silhouette_score

    n = sim.shape[0]
    dist = 1.0 - sim
    np.fill_diagonal(dist, 0.0)
    Z = linkage(squareform(dist, checks=False), method="complete")

    best = (-2.0, None, None, None)
    seen = []
    for h in np.unique(Z[:, 2]):
        assignment = fcluster(Z, h, criterion="distance")
        k = len(set(assignment))
        if k < 2 or k >= n:
            continue
        score = float(silhouette_score(dist, assignment, metric="precomputed"))
        seen.append((k, score))
        if score > best[0]:
            best = (score, assignment, k, float(1.0 - h))
    if best[1] is None:
        raise ValueError("no cut of the tree produced a scoreable clustering")
    score, assignment, k, thr = best
    ks = [x for x, _s in seen]
    if k in (min(ks), max(ks)):
        print(f"[dedup] WARNING: the silhouette optimum N={k} is at the edge of the "
              f"partition range {min(ks)}-{max(ks)}; it is a property of the range, "
              f"not a choice", flush=True)
    return assignment, k, thr, score


def _representatives(groups, meta, sim, cont, index, closure_margin=0.95,
                     containment=0.99, fragment_containment=0.90,
                     absorb=True, verbose=True):
    """One representative per cluster, then absorb the contained ones.

    -> (member2centroid, reps, absorbed_into). Split out of `dedup_cluster` so
    that a sweep over candidate partitions can score the representatives the
    pipeline would actually ship, rather than a restatement of these rules that
    is free to drift from them.

    Longest member wins, but only among members that are still essentially
    full length -- and within that band a CLOSED piece beats a longer open one.

    Length alone is the wrong rule and the data says so loudly: it handed 53 of
    the 101 clusters that contained a both-ends-closed fosmid an open
    representative instead, and the median length it bought by doing so was 103
    bp. Trading a closure claim for a hundred bases is not a trade worth making,
    and two of those clusters gained literally zero -- the closed and open pieces
    were the same length and the tie-break picked the open one.

    It is a band and not a plain closure-first rule because closure is not
    unconditionally worth more than length: one cluster's closed member is 38 kb
    shorter than its longest, which is a partial clone rather than a better
    rendering of the same one. `closure_margin` is where that line sits.
    """
    member2centroid, reps = {}, []
    for members in groups.values():
        longest = max(meta[x]["length"] for x in members)
        band = [x for x in members
                if meta[x]["length"] >= closure_margin * longest] or list(members)
        best = max(band, key=lambda x: (meta[x]["closed_ends"], meta[x]["length"],
                                        float(sim[index[x]].sum()), x))
        reps.append(best)
        for mm in members:
            member2centroid[mm] = best
    if verbose:
        print(f"[dedup] {len(reps):,} clusters from {len(meta):,} pieces", flush=True)

    # Absorption rewrites the centroid of every member of the absorbed cluster,
    # not just of its representative -- so a member can end up under a centroid
    # complete linkage never merged it with. `inserts.csv`'s `absorbed` column and
    # `membership.csv`'s `mi` are both readings of exactly that rewrite.
    # Two rules, because containment alone leaves assembly fragments standing.
    # A piece closed at NEITHER end has both its boundaries where the assembly
    # stopped rather than where the vector was, so it cannot be a clone -- and
    # when it sits mostly inside a piece closed at BOTH ends, it is a shorter
    # rendering of that clone. Six copies of picked clone 1 survived the strict
    # rule: three of them open at both ends, contained at 0.980-0.989 against a
    # 0.99 cut. The strict rule is probed first so a fully-contained host still
    # wins where both apply.
    absorbed_into, by_rule = {}, {"contained": 0, "fragment": 0}
    if absorb and len(reps) > 1:
        order = sorted(reps, key=lambda x: -meta[x]["length"])
        alive = []
        for r in order:
            host = next((h for h in alive
                         if cont[index[r], index[h]] >= containment), None)
            rule = "contained"
            if host is None and meta[r]["closed_ends"] == 0:
                host = next((h for h in alive
                             if meta[h]["closed_ends"] == 2
                             and cont[index[r], index[h]] >= fragment_containment), None)
                rule = "fragment"
            if host is None:
                alive.append(r)
            else:
                absorbed_into[r] = host
                by_rule[rule] += 1
        for member, cen in list(member2centroid.items()):
            while cen in absorbed_into:
                cen = absorbed_into[cen]
            member2centroid[member] = cen
        reps = [r for r in reps if r not in absorbed_into]
        if verbose:
            print(f"[dedup] absorbed {len(absorbed_into):,} representatives "
                  f"({by_rule['contained']:,} >= {containment:.0%} contained in a "
                  f"longer one, {by_rule['fragment']:,} open at both ends and >= "
                  f"{fragment_containment:.0%} inside a closed one) -> "
                  f"{len(reps):,} inserts", flush=True)
    return member2centroid, reps, absorbed_into


def dedup_cluster(work, out_inserts, out_metadata, identity=0.99,
                  containment=0.99, fragment_containment=0.90, absorb=True,
                  closure_margin=0.95, select_k="identity"):
    """Cluster the pieces, take the longest of each, then absorb the contained.

    Two passes with two different normalisations, each used for the one job it is
    right for -- see `_similarity`. The representative is the LONGEST member, not
    the most central: with a symmetric metric centrality no longer implies
    completeness, and completeness is what an insert is judged on.
    """
    import numpy as np
    import pandas as pd
    from sklearn.cluster import AgglomerativeClustering

    work = Path(work)
    meta = json.loads((work / "piece_meta.json").read_text())
    seqs = {n: s for n, _d, s in read_fasta(work / "pooled.fna")}
    hsps = read_blast_tsv(work / "ava_hits.tsv", outfmt=AVA_HSP_FMT)

    labels = list(meta.keys())
    index = {c: i for i, c in enumerate(labels)}
    sim = _similarity(hsps, index, "symmetric")

    if len(labels) == 1:
        groups = {0: [labels[0]]}
    elif select_k == "silhouette":
        assignment, k, thr, score = _cluster_by_silhouette(sim)
        print(f"[dedup] silhouette-selected k={k} (score {score:.4f}, "
              f"between-cluster identity {thr:.4f})", flush=True)
        groups = {}
        for lab, c in zip(assignment, labels):
            groups.setdefault(int(lab), []).append(c)
    else:
        m = AgglomerativeClustering(metric="precomputed", linkage="complete",
                                    n_clusters=None, distance_threshold=1 - identity)
        m.fit(1 - sim)
        groups = {}
        for lab, c in zip(m.labels_, labels):
            groups.setdefault(int(lab), []).append(c)

    cont = _similarity(hsps, index, "containment") if (absorb and len(groups) > 1) else None
    member2centroid, reps, absorbed_into = _representatives(
        groups, meta, sim, cont, index, closure_margin=closure_margin,
        containment=containment, fragment_containment=fragment_containment,
        absorb=absorb)

    # Every absorbed representative, resolved through any chain of absorptions to
    # the representative that actually survives. `absorbed_into` can point at a
    # rep that was itself absorbed later in the same pass.
    def _final_host(r):
        while r in absorbed_into:
            r = absorbed_into[r]
        return r

    swallowed = {}
    for r in absorbed_into:
        swallowed.setdefault(_final_host(r), []).append(r)

    reps = sorted(reps, key=lambda c: meta[c]["piece"])
    n_members = {}
    for _mm, cen in member2centroid.items():
        n_members[cen] = n_members.get(cen, 0) + 1

    key = lambda c: meta[c]["piece"]
    write_fasta(out_inserts, [
        (key(c), f"length={meta[c]['length']} ends={meta[c]['closed_ends']} "
                 f"members={n_members.get(c, 1)}",
         seqs[c]) for c in reps])

    out_metadata = Path(out_metadata)
    out_metadata.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [dict(centroid=key(c), length=meta[c]["length"],
              ends=meta[c]["closed_ends"],
              absorbed=";".join(sorted(key(x) for x in swallowed.get(c, []))))
         for c in reps],
        columns=INSERT_COLS).to_csv(out_metadata / "inserts.csv", index=False)

    # `mi` is the member's similarity TO ITS CENTROID under the clustering
    # metric, reported for absorbed members too even though they reached that
    # centroid by containment -- a low value is the visible trace of the absorb
    # pass, not a defect.
    pd.DataFrame(
        [dict(contig=key(mem), centroid=key(cen),
              mi=round(float(sim[index[mem], index[cen]]), 6))
         for mem, cen in sorted(member2centroid.items(), key=lambda kv: key(kv[0]))],
        columns=MEMBER_COLS).to_csv(out_metadata / "membership.csv", index=False)

    if len(member2centroid) != len(meta):
        raise AssertionError(
            f"membership accounts for {len(member2centroid)} of {len(meta)} pieces"
        )
    print(f"[dedup] {len(reps):,} inserts from {len(meta):,} pieces "
          f"(cross-assembler, cross-pool)", flush=True)
    return reps


AVA_COLS = AVA_HSP_FMT.split()[1:]


def dedup(split_fa, out_inserts, out_metadata, min_contig_len=10000,
          identity=0.99, containment=0.99, fragment_containment=0.90, absorb=True,
          closure_margin=0.95, select_k="identity", threads=None, work=None):
    """prep + all-vs-all blast + cluster in one process -- the direct-run entry point."""
    work = Path(work or "dedup_work")
    pooled = dedup_prep(split_fa, work, min_contig_len=min_contig_len)
    # scadc settings: evalue 1000, perc_identity 50 -- deliberately permissive,
    # since the similarity that matters is a long contiguous match and a strict
    # evalue would drop the HSPs that make it up.
    hsps = blast_hsps(pooled, pooled, work / "ava", threads=threads,
                      evalue=1000, perc_identity=50, outfmt=AVA_HSP_FMT)
    with open(work / "ava_hits.tsv", "w") as fh:
        for h in hsps:
            fh.write("\t".join(h[k] for k in AVA_COLS) + "\n")
    print(f"[dedup] {len(hsps):,} HSPs", flush=True)
    return dedup_cluster(work, out_inserts, out_metadata,
                         identity=identity, containment=containment,
                         fragment_containment=fragment_containment, absorb=absorb,
                         closure_margin=closure_margin, select_k=select_k)


# =====================================================================
# CLI
# =====================================================================

def _graph_arg(spec):
    """`ASSEMBLER:GRAPH[:PATHS]` -> (assembler, graph, paths|None)."""
    parts = spec.split(":")
    if len(parts) == 2:
        return parts[0], Path(parts[1]), None
    if len(parts) == 3:
        return parts[0], Path(parts[1]), Path(parts[2])
    raise ValueError(f"--graph expects ASSEMBLER:GRAPH[:PATHS], got {spec!r}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fabfos_recovery", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("map", help="report where the vector backbone lands")
    m.add_argument("--assembly", action="append", required=True, metavar="PATH:ASSEMBLER:POOL")
    m.add_argument("--backbone", type=Path, required=True, help="single-record pCC1fos FASTA")
    m.add_argument("--out-junctions", type=Path, required=True,
                   help="blast's own table, unfiltered; the --min-hsp floor is rectify's")
    m.add_argument("--threads", type=int, default=None)
    m.add_argument("--work", type=Path, default=None)

    r = sub.add_parser("rectify", help="cut at the mapped footprints, using the assembly graph")
    r.add_argument("--assembly", action="append", required=True, metavar="PATH:ASSEMBLER:POOL")
    r.add_argument("--graph", action="append", required=True, metavar="ASSEMBLER:GRAPH[:PATHS]",
                   help="spades needs its contigs.paths as the third field; megahit does not")
    r.add_argument("--junctions", type=Path, required=True)
    r.add_argument("--backbone", type=Path, required=True,
                   help="used to find the backbone NODE in each graph")
    r.add_argument("--out-split", type=Path, required=True)
    r.add_argument("--out-closure", type=Path, default=None,
                   help="per-piece closure table; debugging, not a product")
    r.add_argument("--out-accounting", type=Path, default=None,
                   help="per-contig length accounting; the CHECK runs either way")
    r.add_argument("--graph-hits", action="append", default=None, metavar="ASSEMBLER:TSV",
                   help="backbone-vs-graph-nodes blast from a separate step; "
                        "without it this blasts for itself")
    r.add_argument("--min-piece", type=int, default=1000)
    r.add_argument("--min-hsp", type=int, default=50,
                   help="floor on HSP length; 50 is where the evidence plateaus")
    r.add_argument("--threads", type=int, default=None)
    r.add_argument("--work", type=Path, default=None)

    d = sub.add_parser("dedup", help="cluster the pieces into the insert set")
    d.add_argument("--split", type=Path, required=True)
    d.add_argument("--out-inserts", type=Path, required=True)
    d.add_argument("--out-metadata", type=Path, required=True,
                   help="directory; inserts.csv + membership.csv are written into it")
    d.add_argument("--min-contig-length", type=int, default=10000)
    d.add_argument("--identity", type=float, default=0.99,
                   help="nident/max(qlen,slen); complete linkage at 1 - this")
    d.add_argument("--containment", type=float, default=0.99,
                   help="nident/qlen at or above which a representative is absorbed")
    d.add_argument("--fragment-containment", type=float, default=0.90,
                   help="nident/qlen at or above which a representative closed at "
                        "NEITHER end is absorbed into one closed at both")
    d.add_argument("--no-absorb", action="store_true", help="skip the containment pass")
    d.add_argument("--select-k", choices=["identity", "silhouette"], default="identity",
                   help="cut the tree at --identity (default), or at whichever k "
                        "maximises silhouette")
    d.add_argument("--closure-margin", type=float, default=0.95,
                   help="a closed piece beats a longer open one while it is at "
                        "least this fraction of the cluster's longest member")
    d.add_argument("--threads", type=int, default=None)
    d.add_argument("--work", type=Path, default=None)

    # The three-step forms. A transform uses these because the blast in the middle
    # needs a container with blastn and the two around it need one with pandas,
    # and no env in the library has both.
    mp = sub.add_parser("map-prep", help="pool one pool's contigs from every assembler")
    mp.add_argument("--assembly", action="append", required=True, metavar="PATH:ASSEMBLER:POOL")
    mp.add_argument("--backbone", type=Path, required=True)
    mp.add_argument("--work", type=Path, required=True)

    mw = sub.add_parser("map-write", help="re-key backbone_hits.tsv onto qualified contig ids")
    mw.add_argument("--work", type=Path, required=True)
    mw.add_argument("--out-junctions", type=Path, required=True)

    rp = sub.add_parser("rectify-prep", help="write each graph's nodes out for the backbone blast")
    rp.add_argument("--graph", action="append", required=True, metavar="ASSEMBLER:GRAPH[:PATHS]")
    rp.add_argument("--work", type=Path, required=True)

    dp = sub.add_parser("dedup-prep", help="pool the long pieces")
    dp.add_argument("--split", type=Path, required=True)
    dp.add_argument("--work", type=Path, required=True)
    dp.add_argument("--min-contig-length", type=int, default=10000)

    dc = sub.add_parser("dedup-cluster", help="cluster, absorb, pick representatives")
    dc.add_argument("--work", type=Path, required=True)
    dc.add_argument("--out-inserts", type=Path, required=True)
    dc.add_argument("--out-metadata", type=Path, required=True)
    dc.add_argument("--identity", type=float, default=0.99)
    dc.add_argument("--containment", type=float, default=0.99)
    dc.add_argument("--fragment-containment", type=float, default=0.90)
    dc.add_argument("--no-absorb", action="store_true")
    dc.add_argument("--select-k", choices=["identity", "silhouette"], default="identity",
                   help="cut the tree at --identity (default), or at whichever k "
                        "maximises silhouette")
    dc.add_argument("--closure-margin", type=float, default=0.95)

    v = sub.add_parser("verify", help="re-check an accounting table independently")
    v.add_argument("--accounting", type=Path, required=True)

    a = ap.parse_args(argv)
    if a.cmd == "map":
        call_map([parse_assembly_arg(s) for s in a.assembly], a.backbone,
                 a.out_junctions, threads=a.threads, work=a.work)
    elif a.cmd == "rectify":
        graphs = {}
        for spec in a.graph:
            asm, g, p = _graph_arg(spec)
            graphs[asm] = (g, p)
        hits = {}
        for spec in (a.graph_hits or []):
            asm, tsv = spec.split(":", 1)
            hits[asm] = Path(tsv)
        rectify([parse_assembly_arg(s) for s in a.assembly], a.junctions,
                a.out_split, a.out_closure, a.out_accounting, backbone=a.backbone,
                graphs=graphs, graph_hits=hits, min_piece=a.min_piece,
                min_hsp=a.min_hsp, threads=a.threads, work=a.work)
    elif a.cmd == "dedup":
        dedup(a.split, a.out_inserts, a.out_metadata,
              min_contig_len=a.min_contig_length, identity=a.identity,
              containment=a.containment,
              fragment_containment=a.fragment_containment, absorb=not a.no_absorb,
              closure_margin=a.closure_margin, select_k=a.select_k,
              threads=a.threads, work=a.work)
    elif a.cmd == "map-prep":
        check_backbone(a.backbone)
        pool_contigs([parse_assembly_arg(s) for s in a.assembly], a.work)
    elif a.cmd == "map-write":
        write_junctions(a.work, a.out_junctions)
    elif a.cmd == "rectify-prep":
        rectify_prep({asm: (g, p) for asm, g, p in
                      (_graph_arg(s) for s in a.graph)}, a.work)
    elif a.cmd == "dedup-prep":
        dedup_prep(a.split, a.work, min_contig_len=a.min_contig_length)
    elif a.cmd == "dedup-cluster":
        dedup_cluster(a.work, a.out_inserts, a.out_metadata,
                      identity=a.identity, containment=a.containment,
                      fragment_containment=a.fragment_containment,
                      absorb=not a.no_absorb, closure_margin=a.closure_margin,
                      select_k=a.select_k)
    elif a.cmd == "verify":
        import pandas as pd
        df = pd.read_csv(a.accounting)
        bad = df[df["emitted_bp"] + df["dropped_short_bp"] + df["vector_bp"] != df["contig_length"]]
        print(f"[verify] {len(df) - len(bad):,}/{len(df):,} contigs balance")
        print(json.dumps(df["action"].value_counts().to_dict(), indent=2))
        if len(bad):
            print(bad.head(20).to_string(), file=sys.stderr)
            raise SystemExit(1)


if __name__ == "__main__":
    main()

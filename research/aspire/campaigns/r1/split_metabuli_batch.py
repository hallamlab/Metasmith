#!/usr/bin/env python3
"""Split one batched Metabuli run back into per-sample tables.

The service classifies all 34 assemblies as a single job, because the cost of a
database pass is per job and not per query (METABULI_SERVICE.md section 3). One
job means one `*_classifications.tsv` and one `*_report.tsv` covering every
sample, so the split has to happen here.

The two files need different treatment, and that asymmetry is the whole point:

  classifications.tsv  is per contig. Its `name` column is the sample-prefixed
                       contig id, so splitting is a partition -- no arithmetic,
                       nothing to get wrong beyond the prefix.

  report.tsv           is an aggregate over the whole batch. It cannot be
                       partitioned, only RECOMPUTED, because every count in it
                       sums across all 34 samples.

Recomputation is possible without the taxonomy dump because `--lineage 1` puts
each contig's full lineage in-band, and because the batch report already names
every taxon any sample hit, with its rank, taxID and depth. So the batch report
supplies the tree and the classifications supply the per-sample counts.

Column layouts here were read off real Metabuli 1.2.0 output, not inferred:

  classifications: is_classified name taxID query_length score e_value rank
                   lineage taxID:match_count
  report:          clade_proportion clade_count taxon_count rank taxID name
                   (name indented two spaces per level of depth)

Usage:
    split_metabuli_batch.py <classifications.tsv> <report.tsv> <outdir>
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

SEP = "__"
INDENT = "  "


class Node:
    __slots__ = ("depth", "rank", "taxid", "name", "parent", "children", "line_no")

    def __init__(self, depth, rank, taxid, name, line_no):
        self.depth, self.rank, self.taxid, self.name = depth, rank, taxid, name
        self.line_no = line_no
        self.parent = None
        self.children = []


def parse_report(path: Path):
    """Return (header, roots, nodes_in_file_order, unclassified_node|None).

    Depth comes from the indentation of the name column, which is how Metabuli
    encodes the tree -- there is no parent column. A node's parent is the
    nearest preceding node one level shallower.
    """
    header = None
    nodes, stack, roots = [], {}, []
    unclassified = None

    for line_no, raw in enumerate(path.read_text().splitlines()):
        if not raw.strip():
            continue
        if raw.startswith("#"):
            header = raw
            continue
        cols = raw.split("\t")
        if len(cols) < 6:
            raise ValueError(f"{path}:{line_no}: expected 6 columns, got {len(cols)}")
        _prop, _clade, _taxon, rank, taxid, name_field = cols[:6]
        depth = (len(name_field) - len(name_field.lstrip(" "))) // len(INDENT)
        node = Node(depth, rank, taxid.strip(), name_field.strip(), line_no)
        nodes.append(node)
        # Metabuli emits unclassified, when there is any, as a top-level row.
        # Mirror whatever shape it used rather than inventing one: this branch
        # is reached only when the batch itself contained unclassified contigs.
        if node.name.lower() == "unclassified" or node.taxid == "0":
            unclassified = node
            continue
        if depth == 0:
            roots.append(node)
        else:
            parent = stack.get(depth - 1)
            if parent is None:
                raise ValueError(
                    f"{path}:{line_no}: '{node.name}' is at depth {depth} with no "
                    f"parent at depth {depth - 1}"
                )
            node.parent = parent
            parent.children.append(node)
        stack[depth] = node
    return header, roots, nodes, unclassified


def parse_classifications(path: Path):
    """Return (header, list[(sample, cols)]). Rows keep their columns verbatim."""
    header, rows = None, []
    for line_no, raw in enumerate(path.read_text().splitlines()):
        if not raw.strip():
            continue
        if raw.startswith("#"):
            header = raw
            continue
        cols = raw.split("\t")
        if len(cols) < 3:
            raise ValueError(f"{path}:{line_no}: expected >=3 columns, got {len(cols)}")
        name = cols[1]
        if SEP not in name:
            raise ValueError(
                f"{path}:{line_no}: sequence id '{name}' has no '{SEP}' sample "
                f"prefix. The batch was built without prefixed headers, and this "
                f"table cannot be split back apart -- see METABULI_SERVICE.md "
                f"section 3."
            )
        rows.append((name.split(SEP, 1)[0], cols))
    return header, rows


def recompute_report(sample_rows, header, roots, nodes, unclassified):
    """Rebuild one sample's report from its classification rows.

    `taxon_count` is the number of contigs assigned exactly at a node;
    `clade_count` is that summed over the node and everything beneath it.
    """
    taxon = defaultdict(int)
    n_unclassified = 0
    for cols in sample_rows:
        if cols[0] != "1":
            n_unclassified += 1
            continue
        taxon[cols[2]] += 1

    clade = {}

    def accumulate(node):
        total = taxon.get(node.taxid, 0)
        for child in node.children:
            total += accumulate(child)
        clade[node.taxid] = total
        return total

    for root in roots:
        accumulate(root)

    # Denominator: mirror the batch report's own convention rather than assume
    # one. If it carried an unclassified row, percentages are over classified +
    # unclassified; if it did not, root IS the total.
    total = sum(clade.get(r.taxid, 0) for r in roots)
    if unclassified is not None:
        total += n_unclassified
    if total == 0:
        return None

    out = [header] if header else []
    for node in nodes:
        if node is unclassified:
            if n_unclassified == 0:
                continue
            count, own = n_unclassified, n_unclassified
        else:
            count = clade.get(node.taxid, 0)
            if count == 0:
                continue
            own = taxon.get(node.taxid, 0)
        out.append(
            "\t".join([
                f"{100.0 * count / total:.4f}",
                str(count),
                str(own),
                node.rank,
                node.taxid,
                INDENT * node.depth + node.name,
            ])
        )
    return "\n".join(out) + "\n"


def split(classifications: Path, report: Path, outdir: Path) -> dict:
    c_header, rows = parse_classifications(classifications)
    r_header, roots, nodes, unclassified = parse_report(report)

    by_sample = defaultdict(list)
    for sample, cols in rows:
        by_sample[sample].append(cols)

    outdir.mkdir(parents=True, exist_ok=True)
    written = {}
    for sample, sample_rows in sorted(by_sample.items()):
        cdst = outdir / f"{sample}_classifications.tsv"
        lines = [c_header] if c_header else []
        for cols in sample_rows:
            cols = list(cols)
            # Strip the prefix: it exists to make the batch's id namespace
            # unique, and a consumer of one sample's table should see the
            # assembler's own contig names.
            cols[1] = cols[1].split(SEP, 1)[1]
            lines.append("\t".join(cols))
        cdst.write_text("\n".join(lines) + "\n")

        rep = recompute_report(sample_rows, r_header, roots, nodes, unclassified)
        rdst = outdir / f"{sample}_report.tsv"
        if rep is not None:
            rdst.write_text(rep)
        written[sample] = (len(sample_rows), cdst, rdst if rep else None)
    return written


def verify_roundtrip(report: Path, outdir: Path) -> list[str]:
    """Per-sample clade counts must sum back to the batch report's.

    A recomputation that is merely plausible is the failure mode worth guarding
    against here, and this catches it: every count in the batch report is a sum
    over samples, so the split is only correct if the parts add up.
    """
    _h, _roots, nodes, _u = parse_report(report)
    batch = {n.taxid: n for n in nodes}
    got = defaultdict(int)
    for f in sorted(outdir.glob("*_report.tsv")):
        for raw in f.read_text().splitlines():
            if raw.startswith("#") or not raw.strip():
                continue
            cols = raw.split("\t")
            got[cols[4].strip()] += int(cols[1])

    problems = []
    for taxid, node in batch.items():
        want = None
        for raw in report.read_text().splitlines():
            if raw.startswith("#") or not raw.strip():
                continue
            c = raw.split("\t")
            if c[4].strip() == taxid:
                want = int(c[1])
                break
        if want is not None and got.get(taxid, 0) != want:
            problems.append(
                f"{node.name} (taxID {taxid}): batch={want} sum-of-samples={got.get(taxid, 0)}"
            )
    return problems


def main(argv):
    if len(argv) != 4:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    classifications, report, outdir = (Path(a) for a in argv[1:])
    written = split(classifications, report, outdir)
    for sample, (n, cdst, rdst) in written.items():
        print(f"  {sample:12s} {n:>8} contigs -> {cdst.name}"
              f"{'' if rdst else '  (no report: nothing classified)'}")
    problems = verify_roundtrip(report, outdir)
    if problems:
        print(f"\n!! {len(problems)} clade count(s) do not sum back to the batch report:",
              file=sys.stderr)
        for p in problems[:10]:
            print(f"   {p}", file=sys.stderr)
        return 1
    print(f"\n{len(written)} sample(s); per-sample clade counts sum back to the batch report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

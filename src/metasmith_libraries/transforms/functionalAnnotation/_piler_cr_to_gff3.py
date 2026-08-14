#!/usr/bin/env python3
"""Parse PILER-CR output and append CRISPR entries to a bakta GFF3 file.

Usage:
    python3 _piler_cr_to_gff3.py <piler_cr_output.txt> <bakta.gff3>

Best-effort: exits 0 even if parsing fails (logs warnings to stderr).
Produces GFF3 entries matching bakta's CRISPR format:
  - Parent: {contig}_crispr_{N}  type=CRISPR  source=PILER-CR  strand=?
  - Child repeats: {parent}_repeat_{M}  type=crispr-repeat
  - Child spacers: {parent}_spacer_{M}  type=crispr-spacer  sequence=...
"""
import re
import sys
from urllib.parse import quote


def parse_piler_cr(text):
    """Parse PILER-CR text output into structured CRISPR arrays.

    Returns list of dicts:
        {contig, array_num, start, stop, repeats: [{pos, length, spacer_len, spacer_seq}], consensus}
    """
    arrays = []
    current_contig = None
    current_array_num = None
    current_repeats = []
    in_data = False

    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]

        # Contig header: >contig_name ...
        if line.startswith(">"):
            current_contig = line[1:].split()[0]
            i += 1
            continue

        # Array header: Array N
        m = re.match(r"^Array\s+(\d+)", line)
        if m:
            current_array_num = int(m.group(1))
            current_repeats = []
            in_data = False
            i += 1
            continue

        # Separator line starts data section
        if line.strip().startswith("=") and "=" * 5 in line:
            if in_data:
                # Second separator = end of array, summary line follows
                i += 1
                if i < len(lines):
                    summary = lines[i].strip()
                    # Summary: copies  repeat_len  spacer_len  consensus
                    parts = summary.split()
                    consensus = parts[-1] if parts else ""
                    if current_contig and current_repeats:
                        start = current_repeats[0]["pos"]
                        last = current_repeats[-1]
                        stop = last["pos"] + last["length"] - 1
                        arrays.append({
                            "contig": current_contig,
                            "array_num": current_array_num,
                            "start": start,
                            "stop": stop,
                            "repeats": current_repeats,
                            "consensus": consensus,
                        })
                in_data = False
            else:
                in_data = True
            i += 1
            continue

        # Data lines: position-based parsing
        if in_data and line.strip():
            # Format: Pos  Repeat  %id  Spacer  LeftFlank  RepeatSeq  SpacerSeq
            # Fields are whitespace-separated, but some may be missing on last repeat
            parts = line.split()
            if not parts:
                i += 1
                continue

            try:
                pos = int(parts[0])
            except (ValueError, IndexError):
                i += 1
                continue

            try:
                repeat_len = int(parts[1])
            except (ValueError, IndexError):
                i += 1
                continue

            # Parse %id
            pct_id = None
            spacer_len = None
            spacer_seq = ""

            idx = 2
            # %id field
            if idx < len(parts):
                try:
                    pct_id = float(parts[idx])
                    idx += 1
                except ValueError:
                    pass

            # Spacer length (may be absent on last repeat)
            if idx < len(parts):
                try:
                    spacer_len = int(parts[idx])
                    idx += 1
                except ValueError:
                    pass

            # Remaining parts: left_flank, repeat_seq, spacer_seq
            # Left flank is 10 chars of DNA, repeat_seq has dots/letters, spacer_seq is DNA
            remaining = parts[idx:]

            # The spacer sequence is the last element if spacer_len is set
            if spacer_len and remaining:
                spacer_seq = remaining[-1]
                # Verify it looks like DNA (not dots)
                if set(spacer_seq) <= set("ACGTNacgtn."):
                    if "." in spacer_seq and len(spacer_seq) > 5:
                        # This is probably the repeat dots, not spacer
                        spacer_seq = ""
                else:
                    spacer_seq = ""

            current_repeats.append({
                "pos": pos,
                "length": repeat_len,
                "spacer_len": spacer_len,
                "spacer_seq": spacer_seq,
            })

        i += 1

    return arrays


def url_encode_attr(s):
    """URL-encode special characters for GFF3 attributes (commas, semicolons)."""
    return s.replace("%", "%25").replace(",", "%2C").replace(";", "%3B")


def arrays_to_gff3_lines(arrays):
    """Convert parsed CRISPR arrays to GFF3 lines matching bakta format."""
    lines = []
    contig_counters = {}

    for array in arrays:
        contig = array["contig"]
        contig_counters.setdefault(contig, 0)
        contig_counters[contig] += 1
        n = contig_counters[contig]

        parent_id = f"{contig}_crispr_{n}"
        repeats = array["repeats"]
        num_repeats = len(repeats)
        repeat_len = repeats[0]["length"] if repeats else 0
        consensus = array["consensus"]

        # Calculate average spacer length from repeats that have spacers
        spacer_lens = [r["spacer_len"] for r in repeats if r["spacer_len"]]
        avg_spacer = round(sum(spacer_lens) / len(spacer_lens)) if spacer_lens else 0

        # Product description matching bakta format
        product = (
            f"CRISPR array with {num_repeats} repeats of length {repeat_len}"
            f", consensus sequence {consensus} and spacer length {avg_spacer}"
        )
        product_encoded = url_encode_attr(product)

        # Parent CRISPR feature
        lines.append(
            f"{contig}\tPILER-CR\tCRISPR\t{array['start']}\t{array['stop']}\t.\t?\t."
            f"\tID={parent_id};Name={product_encoded};product={product_encoded}"
        )

        # Child repeat/spacer features
        for j, rep in enumerate(repeats, 1):
            rep_start = rep["pos"]
            rep_stop = rep["pos"] + rep["length"] - 1

            lines.append(
                f"{contig}\tPILER-CR\tcrispr-repeat\t{rep_start}\t{rep_stop}\t.\t?\t."
                f"\tID={parent_id}_repeat_{j};Parent={parent_id}"
            )

            if rep["spacer_len"] and rep["spacer_seq"]:
                sp_start = rep_stop + 1
                sp_stop = sp_start + rep["spacer_len"] - 1
                lines.append(
                    f"{contig}\tPILER-CR\tcrispr-spacer\t{sp_start}\t{sp_stop}\t.\t?\t."
                    f"\tID={parent_id}_spacer_{j};Parent={parent_id}"
                    f";sequence={rep['spacer_seq']}"
                )

    return lines


def append_to_gff3(gff3_path, crispr_lines):
    """Append CRISPR GFF3 lines to existing bakta GFF3 file.

    Inserts before ##FASTA section if present, otherwise appends at end.
    """
    with open(gff3_path, "r") as f:
        content = f.read()

    crispr_block = "\n".join(crispr_lines) + "\n"

    if "##FASTA" in content:
        content = content.replace("##FASTA", crispr_block + "##FASTA", 1)
    else:
        if not content.endswith("\n"):
            content += "\n"
        content += crispr_block

    with open(gff3_path, "w") as f:
        f.write(content)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <piler_cr_output.txt> <bakta.gff3>", file=sys.stderr)
        sys.exit(0)  # Best-effort: don't fail the transform

    piler_path = sys.argv[1]
    gff3_path = sys.argv[2]

    try:
        with open(piler_path) as f:
            piler_text = f.read()

        arrays = parse_piler_cr(piler_text)
        if not arrays:
            print("piler_cr_to_gff3: no CRISPR arrays found in PILER-CR output", file=sys.stderr)
            sys.exit(0)

        crispr_lines = arrays_to_gff3_lines(arrays)
        append_to_gff3(gff3_path, crispr_lines)
        print(f"piler_cr_to_gff3: appended {len(arrays)} CRISPR arrays ({len(crispr_lines)} GFF3 lines)", file=sys.stderr)

    except Exception as e:
        print(f"piler_cr_to_gff3: failed ({e}), CRISPR annotations skipped", file=sys.stderr)

    sys.exit(0)


if __name__ == "__main__":
    main()

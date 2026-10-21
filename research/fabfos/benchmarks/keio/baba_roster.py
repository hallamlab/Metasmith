#!/usr/bin/env python3
"""Which genes the Keio collection actually contains -- read from Baba 2006's own
supplement, so a screen over the collection has a denominator rather than an assumption.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/keio/baba_roster.py

Any classifier claim about a Keio-based screen needs this. A gene the collection never
carried was never assayed, and counting it as a measured negative manufactures the
result: the screen said nothing about it, and silence is not evidence of no phenotype.
The roster is the difference between an unlabelled gene and a negative one.

Two of Baba's supplementary tables answer it between them, and neither alone:

  * Supplementary Table 2 (`msb4100050-s4.xls`) -- every ORF the deletion campaign
    TARGETED, with its primer extensions. 4,208 of them carry an MG1655 b-number.
  * Supplementary Table 6 (`msb4100050-s8.xls`) -- the essential-gene candidates, the
    300 b-numbered ORFs where no viable deletion mutant was obtained.

The collection is the first minus the second: 3,908 b-numbered mutants. Baba quotes
3,985, and the difference is ORFs annotated in W3110 with no MG1655 counterpart -- this
tree joins everything on b-numbers, so a gene without one cannot be scored either way
and is outside the roster by the same argument that puts essentials outside it.

WHY THIS FILE CARRIES A SPREADSHEET DECODER. The supplement is Excel 97 (OLE2/BIFF8) and
this host has no xlrd, no calamine, no gnumeric, no LibreOffice -- checked, not assumed.
The alternative to ~150 lines of format code is a dependency added to a shared
environment for one table read once, so the format code is here. It reads only what a
flat table needs: the directory of streams, the shared string table, and the four cell
record types that hold text and numbers. Anything richer -- formulas, dates, styles --
is not decoded, because none of it appears in a roster of gene names.
"""
from __future__ import annotations

import struct
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
KEIO = REPO / "data/fabfos/originals/benchmarks/keio"
SUPP = KEIO / "supp"
ZIP = KEIO / "keio_baba2006_supplementary.zip"

TARGETED_XLS = "msb4100050-s4.xls"      # Supplementary Table 2
ESSENTIAL_XLS = "msb4100050-s8.xls"     # Supplementary Table 6

# Baba's own counts, as published. These are guards, not parameters: a decoder that
# drifts silently is worse than one that fails, and a roster short by a few hundred
# genes would quietly inflate any AUC scored against it.
MIN_TARGETED = 4_100
MIN_ESSENTIAL = 290


# =====================================================================
# OLE2 compound file
# =====================================================================

def ole_streams(data: bytes) -> dict[str, bytes]:
    """Every named stream in an OLE2 compound file.

    A workbook is one stream (`Workbook`) inside a tiny filesystem: a header, a FAT
    naming each sector's successor, a directory of entries, and a second smaller
    allocation for streams under `mini_cutoff`. All four are walked here because the
    workbook is large and the directory is not, so both paths are exercised.
    """
    if data[:8] != b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        raise SystemExit("[roster] not an OLE2 compound file")
    ssz = 1 << struct.unpack_from("<H", data, 0x1E)[0]
    mssz = 1 << struct.unpack_from("<H", data, 0x20)[0]
    n_fat = struct.unpack_from("<I", data, 0x2C)[0]
    dir_start = struct.unpack_from("<I", data, 0x30)[0]
    mini_cutoff = struct.unpack_from("<I", data, 0x38)[0]
    mini_fat_start = struct.unpack_from("<I", data, 0x3C)[0]
    difat_start = struct.unpack_from("<I", data, 0x44)[0]
    n_difat = struct.unpack_from("<I", data, 0x48)[0]

    def sector(i: int) -> bytes:
        off = 512 + i * ssz
        return data[off:off + ssz]

    difat = list(struct.unpack_from("<109I", data, 0x4C))
    nxt = difat_start
    for _ in range(n_difat):
        if nxt in (0xFFFFFFFE, 0xFFFFFFFF):
            break
        s = sector(nxt)
        difat.extend(struct.unpack_from(f"<{ssz // 4 - 1}I", s, 0))
        nxt = struct.unpack_from("<I", s, ssz - 4)[0]

    fat: list[int] = []
    for d in (x for x in difat[:n_fat] if x != 0xFFFFFFFF):
        fat.extend(struct.unpack_from(f"<{ssz // 4}I", sector(d), 0))

    def chain(start: int, table: list[int]) -> list[int]:
        out, cur, seen = [], start, set()
        while cur not in (0xFFFFFFFE, 0xFFFFFFFF) and cur < len(table):
            if cur in seen:
                raise SystemExit("[roster] cyclic sector chain")
            seen.add(cur)
            out.append(cur)
            cur = table[cur]
        return out

    def read_chain(start: int) -> bytes:
        return b"".join(sector(i) for i in chain(start, fat))

    entries = []
    dir_bytes = read_chain(dir_start)
    for off in range(0, len(dir_bytes) - 127, 128):
        e = dir_bytes[off:off + 128]
        nlen = struct.unpack_from("<H", e, 0x40)[0]
        entries.append((e[:max(nlen - 2, 0)].decode("utf-16-le", "replace"),
                        e[0x42],
                        struct.unpack_from("<I", e, 0x74)[0],
                        struct.unpack_from("<Q", e, 0x78)[0]))

    root = next(e for e in entries if e[1] == 5)
    mini_stream = read_chain(root[2])[:root[3]] if root[2] != 0xFFFFFFFE else b""
    mini_fat: list[int] = []
    if mini_fat_start not in (0xFFFFFFFE, 0xFFFFFFFF):
        mf = read_chain(mini_fat_start)
        mini_fat = list(struct.unpack_from(f"<{len(mf) // 4}I", mf, 0))

    out: dict[str, bytes] = {}
    for name, etype, start, size in entries:
        if etype != 2:
            continue
        blob = (b"".join(mini_stream[i * mssz:(i + 1) * mssz]
                         for i in chain(start, mini_fat)) if size < mini_cutoff
                else read_chain(start))
        out[name] = blob[:size]
    return out


# =====================================================================
# BIFF8 workbook
# =====================================================================

def _records(stream: bytes) -> list[tuple[int, bytes]]:
    out, i = [], 0
    while i + 4 <= len(stream):
        rid, ln = struct.unpack_from("<HH", stream, i)
        out.append((rid, stream[i + 4:i + 4 + ln]))
        i += 4 + ln
    return out


def _shared_strings(records) -> list[str]:
    """The SST, reassembled across CONTINUE records.

    A BIFF record caps at 8,224 bytes, so the string table of a four-thousand-row sheet
    always spills. The subtlety that makes a naive reader produce mojibake: a CONTINUE
    restarts with its OWN 8-bit/16-bit flag byte, so a string split across the boundary
    can change width halfway through.
    """
    blobs, collecting = [], False
    for rid, body in records:
        if rid == 0x00FC:                       # SST
            blobs.append(body)
            collecting = True
        elif rid == 0x003C and collecting:      # CONTINUE
            blobs.append(body)
        elif collecting:
            break
    if not blobs:
        return []

    strings: list[str] = []
    total = struct.unpack_from("<I", blobs[0], 4)[0]
    bi, pos = 0, 8
    for _ in range(total):
        while pos >= len(blobs[bi]):
            pos -= len(blobs[bi])
            bi += 1
        nchar = struct.unpack_from("<H", blobs[bi], pos)[0]
        pos += 2
        flags = blobs[bi][pos]
        pos += 1
        rich = struct.unpack_from("<H", blobs[bi], pos)[0] if flags & 0x08 else 0
        pos += 2 if flags & 0x08 else 0
        ext = struct.unpack_from("<I", blobs[bi], pos)[0] if flags & 0x04 else 0
        pos += 4 if flags & 0x04 else 0

        chars, wide, left = [], bool(flags & 0x01), nchar
        while left:
            avail = (len(blobs[bi]) - pos) // (2 if wide else 1)
            if avail <= 0:
                bi += 1
                wide = bool(blobs[bi][0] & 0x01)
                pos = 1
                continue
            take = min(left, avail)
            raw = blobs[bi][pos:pos + take * (2 if wide else 1)]
            chars.append(raw.decode("utf-16-le" if wide else "latin1", "replace"))
            pos += take * (2 if wide else 1)
            left -= take
        strings.append("".join(chars))

        skip = rich * 4 + ext
        while skip:
            avail = len(blobs[bi]) - pos
            if avail <= 0:
                bi += 1
                pos = 0
                continue
            take = min(skip, avail)
            pos += take
            skip -= take
    return strings


def _rk(v: int) -> float:
    """An RK is a double with its low 34 mantissa bits thrown away, or a 30-bit signed
    integer, either optionally divided by 100 -- the two low bits say which."""
    if v & 0x02:
        iv = v >> 2
        n = float(iv - 0x40000000 if iv & 0x20000000 else iv)
    else:
        n = struct.unpack("<d", struct.pack("<Q", (v & 0xFFFFFFFC) << 32))[0]
    return n / 100.0 if v & 0x01 else n


def first_sheet(stream: bytes) -> list[list]:
    """The workbook's first worksheet as a dense list of rows.

    Only the record types a flat table needs are decoded: LABELSST and LABEL for text,
    NUMBER, RK and MULRK for numbers. A cell of any other kind reads as the empty
    string, which is what an undecoded cell should look like to a caller that then
    fails its own guard rather than silently seeing a gap.
    """
    records = _records(stream)
    sst = _shared_strings(records)
    sheets = []
    for rid, body in records:
        if rid == 0x0085:                       # BOUNDSHEET
            off = struct.unpack_from("<I", body, 0)[0]
            nlen, flags = body[6], body[7]
            sheets.append((body[8:8 + nlen * 2].decode("utf-16-le") if flags & 1
                           else body[8:8 + nlen].decode("latin1"), off))
    if not sheets:
        raise SystemExit("[roster] workbook declares no worksheet")

    cells: dict[tuple[int, int], object] = {}
    i, depth = sheets[0][1], 0
    while i + 4 <= len(stream):
        rid, ln = struct.unpack_from("<HH", stream, i)
        body = stream[i + 4:i + 4 + ln]
        i += 4 + ln
        if rid == 0x0809:                                    # BOF
            depth += 1
        elif rid == 0x000A:                                  # EOF
            depth -= 1
            if depth <= 0:
                break
        elif rid == 0x00FD and len(body) >= 10:              # LABELSST
            r, c, idx = struct.unpack_from("<HHxxI", body, 0)
            cells[(r, c)] = sst[idx] if idx < len(sst) else ""
        elif rid == 0x0204 and len(body) >= 9:               # LABEL
            r, c = struct.unpack_from("<HH", body, 0)
            nchar, flags, raw = struct.unpack_from("<H", body, 6)[0], body[8], body[9:]
            cells[(r, c)] = (raw[:nchar * 2].decode("utf-16-le", "replace") if flags & 1
                             else raw[:nchar].decode("latin1"))
        elif rid == 0x0203 and len(body) >= 14:              # NUMBER
            r, c = struct.unpack_from("<HH", body, 0)
            cells[(r, c)] = struct.unpack_from("<d", body, 6)[0]
        elif rid == 0x027E and len(body) >= 10:              # RK
            r, c = struct.unpack_from("<HH", body, 0)
            cells[(r, c)] = _rk(struct.unpack_from("<I", body, 6)[0])
        elif rid == 0x00BD and len(body) >= 6:               # MULRK
            r, c0 = struct.unpack_from("<HH", body, 0)
            for k in range((len(body) - 6) // 6):
                cells[(r, c0 + k)] = _rk(
                    struct.unpack_from("<I", body, 4 + k * 6 + 2)[0])

    if not cells:
        raise SystemExit(f"[roster] sheet {sheets[0][0]!r} decoded to no cells")
    nr = max(r for r, _ in cells) + 1
    nc = max(c for _, c in cells) + 1
    return [[cells.get((r, c), "") for c in range(nc)] for r in range(nr)]


# =====================================================================
# the roster
# =====================================================================

def _read_xls(name: str) -> list[list]:
    """The loose copy under `supp/` if it is there, else the same member of the zip.
    Both are DVC-pinned; the zip is the acquisition and `supp/` the unpacked form, and
    they have been checked to carry identical bytes for these two members."""
    loose = SUPP / name
    data = loose.read_bytes() if loose.exists() else zipfile.ZipFile(ZIP).read(name)
    streams = ole_streams(data)
    key = next(k for k in streams if k.lower().lstrip("\x05") in ("workbook", "book"))
    return first_sheet(streams[key])


def _b_numbers(rows: list[list], what: str) -> dict[str, str]:
    """`{b_number: gene}` from one of Baba's tables.

    The header row is FOUND, not counted: the two tables put it at different depths
    (Supplementary Table 2 has three header rows, Table 6 has four) because Table 6
    carries an extra merged banner. Counting rows would silently drop `thrL` from one
    table and nothing from the other, which is exactly the kind of off-by-one that
    survives every count check because it is one row wide.
    """
    hdr = next((i for i, r in enumerate(rows[:10])
                if any(str(c).strip().lower() == "b number" for c in r)), None)
    if hdr is None:
        raise SystemExit(f"[roster] {what}: no 'b number' header in the first 10 rows")
    bcol = next(c for c, v in enumerate(rows[hdr])
                if str(v).strip().lower() == "b number")
    out: dict[str, str] = {}
    for r in rows[hdr + 1:]:
        b = str(r[bcol]).strip()
        if len(b) == 5 and b[0] == "b" and b[1:].isdigit():
            out[b] = str(r[1]).strip()
    return out


def collection() -> tuple[dict[str, str], dict[str, str]]:
    """`(assayed, essential)` as `{b_number: gene}`. `assayed` is the Keio collection:
    targeted and not essential, which is exactly the set a screen over the collection
    could have reported a phenotype for."""
    targeted = _b_numbers(_read_xls(TARGETED_XLS), "Supplementary Table 2 (targeted)")
    essential = _b_numbers(_read_xls(ESSENTIAL_XLS), "Supplementary Table 6 (essential)")
    if len(targeted) < MIN_TARGETED:
        raise SystemExit(f"[roster] only {len(targeted)} targeted ORFs carry a "
                         f"b-number; Baba targeted 4,288 and >= {MIN_TARGETED} should "
                         f"survive the b-number join -- the decode is wrong")
    if len(essential) < MIN_ESSENTIAL:
        raise SystemExit(f"[roster] only {len(essential)} essential candidates; "
                         f"Baba reports 303, >= {MIN_ESSENTIAL} b-numbered")
    stray = set(essential) - set(targeted)
    if stray:
        raise SystemExit(f"[roster] {len(stray)} essential gene(s) are not in the "
                         f"targeted table, e.g. {sorted(stray)[:5]} -- the two tables "
                         f"disagree about what was attempted")
    return {b: g for b, g in targeted.items() if b not in essential}, essential


def main() -> int:
    assayed, essential = collection()
    print(f"targeted  {len(assayed) + len(essential):,} b-numbered ORFs\n"
          f"essential {len(essential):,} (no viable deletion mutant)\n"
          f"assayed   {len(assayed):,} -- the Keio collection, as a b-number set")
    print(f"\nfirst few assayed: {sorted(assayed.items())[:5]}")
    print(f"first few essential: {sorted(essential.items())[:5]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

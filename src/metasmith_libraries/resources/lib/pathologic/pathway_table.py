"""MetaPathways 4.0.3's per-PGDB pathway table (<tag>_pwy.tsv, written by its pgdb_build_wf.py
extract_pwy), plus three columns of Pathway Tools' pathways report.

One row per pathway that is not a super-pathway: SAMPLE, PWY_NAME (frame id), PWY_COMMON_NAME,
PWY_SCORE (from the pathway inference report), NUM_REACTIONS (REACTION-LIST length),
NUM_COVERED_REACTIONS (REACTIONS-PRESENT in EXPLANATION-CODE), ORF_COUNT and ORFS (camelot_frs's
genes_of_pathway), then Pathway Frequency Score, Pathway Abundance, Reason to Keep.

Runs where camelot_frs is installed (env::metapathways.env), and reads the PGDB through it as MP4
does: genes_of_pathway reaches enzymes through generic reactions camelot infers from compound
classes, which the flat files do not record.
"""
import argparse
import re
from pathlib import Path

PATHWAY_COLUMNS = ["SAMPLE", "PWY_NAME", "PWY_COMMON_NAME", "PWY_SCORE", "NUM_REACTIONS",
                   "NUM_COVERED_REACTIONS", "ORF_COUNT", "ORFS"]
REPORT_COLUMNS = ["Pathway Name", "Pathway Frame-id", "Pathway Class Name", "Pathway Class Frame-id",
                  "Pathway Score", "Pathway Frequency Score", "Pathway Abundance", "Reason to Keep",
                  "Pathway URL"]
EXTRA_COLUMNS = ["Pathway Frequency Score", "Pathway Abundance", "Reason to Keep"]


def sexp(text):
    tokens = re.findall(r'\(|\)|"[^"]*"|[^\s()]+', text)
    stack = [[]]
    for t in tokens:
        if t == "(":
            stack.append([])
        elif t == ")":
            done = stack.pop()
            stack[-1].append(done)
        else:
            stack[-1].append(t)
    return stack[0]


def latest(reports, pattern):
    files = sorted(Path(reports).glob(pattern), key=lambda p: (p.stat().st_mtime, p.name))
    return files[-1] if files else None


def inference_scores(reports):
    """Frame id -> score of every passing record in the latest inference report, skipping the
    pruned-pathway listing, as get_pwy_inf does."""
    path = latest(reports, "pwy-inference-report_*.txt")
    if path is None:
        return {}
    records, rec, skip = [], None, False
    for line in path.read_text(encoding="latin-1").split("\n"):
        if "List of pathways pruned" in line:
            skip = True
        if ":::" in line:
            skip = False
        if skip:
            continue
        if line[:2] == " (":
            if rec:
                records.append(rec)
            rec = line
        elif line and rec is not None and line[0] == " ":
            rec += line
    if rec:
        records.append(rec)
    scores = {}
    for r in records:
        parsed = sexp(r)[0]
        if len(parsed) > 5 and parsed[1] == "T":
            scores[parsed[0]] = parsed[5]
    return scores


def report_rows(reports):
    path = latest(reports, "pathways-report_*.txt")
    rows = {}
    if path is None:
        return rows
    for line in path.read_text(encoding="latin-1").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        cells = [c.strip() for c in line.split("|")]
        if cells == REPORT_COLUMNS:
            continue
        row = dict(zip(REPORT_COLUMNS, cells))
        rows.setdefault(row["Pathway Frame-id"], row)
    return rows


def reactions_present(code):
    if code == "PWY-HAS-NON-COMPUTATIONAL-EVIDENCE":
        return []
    for item in sexp(code)[0] if code.startswith("(") else []:
        if isinstance(item, list) and item and item[0] == "REACTIONS-PRESENT":
            return item[1] if len(item) > 1 and isinstance(item[1], list) else item[1:]
    return []


def verify_empty(pgdb):
    # pt_exports.write_verified_empty_pathways: a PGDB with no pathways is an empty table only when
    # the final report is header-only and the evidence list holds nothing.
    reports = Path(pgdb) / "reports"
    summaries = sorted(reports.glob("pathways-report_*.txt"))
    if not summaries:
        raise SystemExit("[pathway_table] no pathways and no pathway report to confirm it")
    for report in summaries:
        rows = [line.strip() for line in report.read_text(encoding="latin-1").splitlines()
                if line.strip() and not line.lstrip().startswith("#")]
        if len(rows) != 1 or [c.strip() for c in rows[0].split("|")] != REPORT_COLUMNS:
            raise SystemExit(f"[pathway_table] no pathways, yet {report.name} is not header-only")
    evidence = (reports / "pwy-evidence-list.dat").read_text(encoding="latin-1")
    if any(line.strip() and not line.lstrip().startswith(";;;") for line in evidence.splitlines()):
        raise SystemExit("[pathway_table] no pathways, yet pwy-evidence-list.dat lists some")


def has_pathways(data):
    path = data / "pathways.dat"
    lines = [line.strip() for line in path.read_text(encoding="latin-1").splitlines()
             if line.strip() and not line.lstrip().startswith("#")] if path.exists() else []
    return any(line.startswith("UNIQUE-ID") for line in lines)


def rows(pgdb, sample, work):
    data, reports = Path(pgdb) / "data", Path(pgdb) / "reports"
    if not has_pathways(data):
        verify_empty(pgdb)
        return
    from camelot_frs.camelot_frs import frame_parent_of_frame_p, get_frame, get_frame_all_children, get_kb
    from camelot_frs.pgdb_api import genes_of_pathway
    from camelot_frs.pgdb_loader import load_pgdb, make_camelot_file

    # make_camelot_file names the KB from version.dat, which MP4 copies in from default-version.
    if not (data / "version.dat").exists():
        (data / "version.dat").write_text(f"ORGID\t{sample}\n")
    Path(work).mkdir(parents=True, exist_ok=True)
    org = make_camelot_file(str(data), str(work))
    load_pgdb(f"{work}/{org}.camelot")
    kb = get_kb(org)
    scores, report = inference_scores(reports), report_rows(reports)
    for pwy in get_frame_all_children(get_frame(kb, "Pathways"), frame_types="instance"):
        if frame_parent_of_frame_p(get_frame(kb, "Super-Pathways"), pwy):
            continue
        orfs = [str(g.get_slot_values("COMMON-NAME")[0]).removeprefix("frame:") if g.get_slot_values("COMMON-NAME")
                else g.frame_id for g in genes_of_pathway(pwy)]
        code = (pwy.get_slot_values("EXPLANATION-CODE") or ["PWY-HAS-NON-COMPUTATIONAL-EVIDENCE"])[0]
        extra = report.get(pwy.frame_id, {})
        yield [sample, pwy.frame_id, pwy.get_slot_values("COMMON-NAME")[0], scores.get(pwy.frame_id, ""),
               str(len(pwy.get_slot_values("REACTION-LIST"))), str(len(reactions_present(str(code)))),
               str(len(orfs)), ",".join(orfs), *(extra.get(c, "") for c in EXTRA_COLUMNS)]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pgdb", required=True, help="the PGDB's version directory, holding data/ and reports/")
    p.add_argument("--sample", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--work", default="camelot", help="where camelot writes its KB pickle")
    a = p.parse_args()
    n = 0
    with open(a.out, "w") as fh:
        fh.write("\t".join(PATHWAY_COLUMNS + EXTRA_COLUMNS) + "\n")
        for row in rows(a.pgdb, a.sample, a.work):
            fh.write("\t".join(row) + "\n")
            n += 1
    print(f"[pathway_table] {a.sample}: {n:,} pathways", flush=True)


if __name__ == "__main__":
    main()

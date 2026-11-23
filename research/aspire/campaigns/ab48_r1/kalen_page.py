#!/usr/bin/env python3
"""Cut the lab timeline page down to Kalen's copy: the Purify Spirulina figures only.

    python research/aspire/campaigns/ab48_r1/kalen_page.py [--page PAGE] [--out OUT]

Reads the page report_data.py wrote and drops the AB48 section and its data, the July-lane
checks, the counts of other lab projects and the repository paths. Each cut names the text it
replaces and fails when that text is gone, so a template edit cannot slip a section through.
"""

import argparse
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
REPORT = REPO / "cache" / "aspire" / "report"

LEDE = ("The Purify Spirulina samples your sample sheet marks for plotting, profiled from 16S amplicons "
        "and placed at your sampling dates above the photobioreactor log. Every sample's reads are cut to "
        "the V4 region, so the 2026 V4-only run shares one set of ASVs with the earlier runs.")
METHODS = ("Every 16S sample in the lab was processed together with ASPIRE, the lab's 16S workflow, on "
           "Sockeye. Denoising all runs together means one ASV identifier is one sequence across every "
           "sample on this page.")
FOOTER = ("Built from the lab's V4 and V4–V5 ASV tables and Purify's reactor log export. The log is "
          "binned to 3-hour means. The logger holds a dose or harvest value for the length of the "
          "event, so those two panels show each bin's largest value.")


def cut(html: str, pattern: str, repl: str = "", flags=re.S) -> str:
    out, n = re.subn(pattern, lambda _: repl, html, count=1, flags=flags)
    assert n == 1, f"not found: {pattern[:80]}"
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--page", type=Path, default=REPORT / "lab_asv_timeline.html")
    ap.add_argument("--out", type=Path, default=REPORT / "spirulina_timeline.html")
    args = ap.parse_args()
    html = args.page.read_text()

    html = cut(html, r"<title>.*?</title>", "<title>Purify Spirulina over time</title>")
    html = cut(html, r"<h1>.*?</h1>", "<h1>Purify Spirulina: the community over time</h1>")
    html = cut(html, r'<p class="lede">.*?</p>', f'<p class="lede">{LEDE}</p>')
    html = cut(html, r"Kalen's sample sheet marks for plotting, placed at the sampling dates in that sheet",
               "your sample sheet marks for plotting, placed at your sampling dates")
    html = cut(html, r"the ones Kalen's sheet marks Plot = Y", "the ones your sheet marks Plot = Y")
    html = cut(html, r'\s*<section id="ab48">.*?</section>')
    html = cut(html, r'(<section id="methods">\s*<h2>.*?</h2>\s*)<p>.*?</p>', rf"\g<1><p>{METHODS}</p>")
    html = cut(html, r'\s*<div class="step"><h4>Link to MAGs</h4>.*?</div>')
    html = cut(html, r"\s*<h3>Checks against the July lane</h3>\s*<ul class=\"checks\">.*?</ul>")
    html = cut(html, r'\s*<div class="tablewrap" id="table-ab48"></div>')
    html = cut(html, r"<footer>.*?</footer>", f"<footer>\n  <p>{FOOTER}</p>\n</footer>")

    html = cut(html, r"\s*mk\(\"table-ab48\".*?;\n", "\n")
    html = cut(html, r"const st = DATA\.ab48\.stats, p = DATA\.purify;", "const p = DATA.purify;")
    html = cut(html, r'\s*\[st\.samples, "AB48 samples"\].*?with a MAG`\],')
    html = cut(html, r'\s*\[DATA\.study\.nostoc, "Nostoc and Anabaena samples"\], \[DATA\.study\.patrik, "of Patrik\'s 2026 cultures"\],',
               "")
    html = cut(html, r'\s*document\.getElementById\("cand-n"\)\.textContent = st\.paired_asvs;')
    html = cut(html, r"ab48Figure\(\); ")
    html = cut(html, r"\n  function ab48Figure\(\) \{.*?\n  \}\n", "\n")
    html = cut(html, r"Paired AB48 bins", "Paired bins")

    def strip_data(m):
        data = json.loads(m.group(1))
        del data["ab48"]
        for key in ("nostoc", "patrik"):
            del data["study"][key]
        keep = set(data["purify"]["rows"]) | set(data["purify"]["bars"])
        data["phyla"] = {t: v for t, v in data["phyla"].items() if t in keep}
        return f"<script>const DATA = {json.dumps(data, ensure_ascii=False)};</script>"
    html, n = re.subn(r"<script>const DATA = (.*?);</script>", strip_data, html, count=1, flags=re.S)
    assert n == 1

    for leak in ("AB48", "ab48", "Nostoc", "Patrik", "research/", "data/aspire", "Kalen"):
        assert leak not in html, leak
    args.out.write_text(html)
    print(f"{args.out} {len(html) / 1e6:.2f} MB")


if __name__ == "__main__":
    main()

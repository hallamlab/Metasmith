"""Check each upstream_definition in a dictionary CSV against the page its definition_url cites.

usage: verify.py <feature_descriptions.csv|class_descriptions.csv> <report.csv>
"""
import csv, hashlib, html, json, re, subprocess, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

CACHE = Path("pages"); CACHE.mkdir(exist_ok=True)
URL = re.compile(r"https?://[^\s)'\",]+")
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"


def norm(s):
    s = html.unescape(s)
    s = s.translate(str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-",
                                   "—": "-", " ": " ", "−": "-", "²": "2", "³": "3"}))
    s = re.sub(r"[^a-z0-9%./-]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def fetch(url):
    base = url.split("#")[0].strip()
    key = CACHE / hashlib.md5(base.encode()).hexdigest()
    if key.with_suffix(".txt").exists():
        return key.with_suffix(".txt").read_text()
    raw = key.with_suffix(".raw")
    r = subprocess.run(["curl", "-sL", "-m", "60", "-A", UA, "-o", str(raw), "-w", "%{http_code} %{content_type}", base],
                       capture_output=True, text=True)
    code, _, ctype = r.stdout.partition(" ")
    if not raw.exists() or not code.startswith("2"):
        txt = f"__FETCH_FAILED__ {code}"
    elif raw.read_bytes()[:4] == b"%PDF":
        txt = " ".join(subprocess.run(["pdftotext", *m, str(raw), "-"], capture_output=True, text=True).stdout
                       for m in (["-layout"], ["-raw"], []))
        txt = re.sub(r"-\s*\n\s*", "", txt) + " " + txt
    elif raw.read_bytes()[:2] == b"PK":
        import openpyxl
        import io; wb = openpyxl.load_workbook(io.BytesIO(raw.read_bytes()), read_only=True)
        txt = " ".join(str(c) for ws in wb for row in ws.iter_rows(values_only=True) for c in row if c is not None)
    elif raw.read_bytes()[:5] == b"{\\rtf":
        t = raw.read_bytes().decode("latin-1")
        t = re.sub(r"\\'([0-9a-f]{2})", lambda m: bytes.fromhex(m.group(1)).decode("cp1252"), t)
        txt = re.sub(r"\\[a-z]+-?\d* ?|[{}]", " ", t)
    else:
        t = raw.read_bytes().decode("utf-8", "replace")
        t = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", t)
        txt = re.sub(r"<[^>]+>", " ", t)
        txt = txt.replace("\\n", " ").replace('\\"', '"')
    key.with_suffix(".txt").write_text(txt)
    return txt


def score(quote, page):
    q, p = norm(quote), norm(page)
    if not q:
        return "no_quote", 1.0
    if q in p:
        return "exact", 1.0
    parts = [norm(x) for x in re.split(r"\.\.\.|…|\[\.\.\.\]", quote) if norm(x)]
    if parts and all(x in p for x in parts):
        return "exact_with_ellipsis", 1.0
    flat = p.replace(" ", "")
    sents = [x.replace(" ", "") for x in re.split(r"(?<=[a-z0-9])\. ", q) if len(x.split()) >= 3]
    if sents and all(x in flat for x in sents):
        return "sentences_found", 1.0
    w = q.split()
    n = 6 if len(w) >= 6 else len(w)
    sh = [" ".join(w[i:i + n]) for i in range(max(1, len(w) - n + 1))]
    frac = sum(s in p for s in sh) / len(sh)
    return ("partial" if frac >= 0.5 else "not_found"), round(frac, 2)


rows = list(csv.DictReader(open(sys.argv[1])))
urls = sorted({u for r in rows if r["upstream_definition"].strip() for u in URL.findall(r["definition_url"])})
with ThreadPoolExecutor(8) as ex:
    pages = dict(zip(urls, ex.map(fetch, urls)))

out = []
for r in rows:
    d = r["upstream_definition"].strip()
    if not d:
        r["verify"], r["verify_frac"] = "blank", ""
    else:
        us = URL.findall(r["definition_url"])
        texts = [pages[u] for u in us if u in pages]
        if not texts:
            r["verify"], r["verify_frac"] = "no_url", 0
        elif all(t.startswith("__FETCH_FAILED__") for t in texts):
            r["verify"], r["verify_frac"] = "fetch_failed " + (texts[0].split() + ["?", "?"])[1], 0
        else:
            best = max((score(d, t) for t in texts if not t.startswith("__FETCH_FAILED__")), key=lambda x: x[1])
            r["verify"], r["verify_frac"] = best
    out.append(r)

cols = list(rows[0].keys())
with open(sys.argv[2], "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(out)
from collections import Counter
print(Counter(r["verify"].split()[0] for r in out))
print("fetch failures:", Counter(r["verify"] for r in out if r["verify"].startswith("fetch")))

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::bench_keio"))

SUPP_BASE = "https://www.embopress.org/action/downloadSupplement"
DOI       = "10.1038/msb4100050"
PMC       = "PMC1681482"
PMC_OA    = "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"

TABLES = tuple(f"msb4100050-s{i}" for i in range(1, 11))

DRIVER = r'''
import sys, urllib.request
from pathlib import Path

OUT = Path("{out}")
(OUT / "supp").mkdir(parents=True, exist_ok=True)
(OUT / "fulltext").mkdir(parents=True, exist_ok=True)

# (magic prefix, what it means). A file whose first bytes match none of these is not
# what its extension claims, and the one shape we have actually been burned by --
# an HTML error page served with a 200 -- is caught by the first check rather than
# by the last.
MAGIC = {{
    ".pdf":  (b"%PDF",),
    ".xls":  (b"\xd0\xcf\x11\xe0", b"PK\x03\x04"),   # OLE2, or xlsx-in-.xls
    ".doc":  (b"\xd0\xcf\x11\xe0",),
    ".zip":  (b"PK\x03\x04",),
    ".xml":  (b"<?xml", b"<!DOC", b"<art"),
    ".gif":  (b"GIF8",),
    ".jpg":  (b"\xff\xd8\xff",),
}}

def check(p: Path):
    want = MAGIC.get(p.suffix.lower())
    if want is None:
        return
    head = p.read_bytes()[:8]
    if not any(head.startswith(w) for w in want):
        raise SystemExit(
            f"[keio] {{p.name}} is not a {{p.suffix}}: first bytes {{head!r}}. "
            f"A landing page served with a 200 looks exactly like this -- refusing "
            f"to write it rather than letting a parser find out later.")

def fetch(url: str, dest: Path):
    urllib.request.urlretrieve(url, dest)
    if dest.stat().st_size == 0:
        raise SystemExit(f"[keio] {{dest.name}} is empty")
    check(dest)
    print(f"[keio] {{dest.relative_to(OUT)}}  {{dest.stat().st_size:,}} B", flush=True)

fetch("{pmc_url}", OUT / "fulltext" / "keio_baba2006_{pmc}.xml")
for stem in {tables}:
    fetch(f"{supp_base}?doi={doi}&file={{stem}}.xls", OUT / "supp" / f"{{stem}}.xls")

print(f"[keio] {{sum(1 for _ in OUT.rglob('*') if _.is_file())}} files", flush=True)
'''


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    driver = DRIVER.format(
        out=iout.container, supp_base=SUPP_BASE, doi=DOI, pmc=PMC,
        pmc_url=f"{PMC_OA}?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:"
                f"{PMC[3:]}&metadataPrefix=pmc",
        tables=repr(TABLES),
    )
    context.LocalShell("cat > _bench_keio.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv(env=image, cmd="python3 _bench_keio.py")

    n = sum(1 for p in (iout.local / "supp").glob("*") if p.is_file())
    Log.Info(f"keio: {n} supplementary files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=n >= len(TABLES) and (iout.local / "fulltext").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)

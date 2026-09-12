from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::bench_eydallin"))

PMC       = "PMC2900218"
PMC_OA    = "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"
SUPPL_URL = ("https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}/bin/"
             "pone.0011505.s001.pdf")

DRIVER = r'''
import urllib.request
from pathlib import Path

OUT = Path("{out}")
OUT.mkdir(parents=True, exist_ok=True)

def fetch(url: str, dest: Path, magic: bytes):
    urllib.request.urlretrieve(url, dest)
    head = dest.read_bytes()[:8]
    if not head.startswith(magic):
        raise SystemExit(
            f"[eydallin] {{dest.name}} starts {{head!r}}, wanted {{magic!r}} -- a "
            f"landing page served with a 200 looks exactly like this")
    print(f"[eydallin] {{dest.name}}  {{dest.stat().st_size:,}} B", flush=True)

fetch("{suppl_url}", OUT / "eydallin2010_suppl_table_s1.pdf", b"%PDF")
fetch("{pmc_url}", OUT / "eydallin2010_fulltext.jats.xml", b"<?xml")
'''


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    driver = DRIVER.format(
        out=iout.container,
        suppl_url=SUPPL_URL.format(pmc=PMC),
        pmc_url=f"{PMC_OA}?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:"
                f"{PMC[3:]}&metadataPrefix=pmc",
    )
    context.LocalShell("cat > _bench_eydallin.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv(env=image, cmd="python3 _bench_eydallin.py")

    want = ("eydallin2010_suppl_table_s1.pdf", "eydallin2010_fulltext.jats.xml")
    missing = [w for w in want if not (iout.local / w).exists()]
    if missing:
        Log.Error(f"eydallin: missing {missing}")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=not missing,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=30)),
)

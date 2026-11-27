from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddRequirement(lib.GetType("ref::dbcan_sources"))
db      = model.AddProduct(lib.GetType("ref::dbcan_hmm"))

HMM = "dbCAN.hmm"
FAMILIES = "fam-substrate-mapping.tsv"


# The release ships no fam-activities table. Its family-substrate map lists one CAZy
# activity per row, `Name` and `EC_Number`, repeated once per substrate, so a family's
# activities are its distinct rows in CAZy's own `name (EC n); ...` form, behind the family
# the way DRAM read the old table. Glycosyltransferases have no substrate row and no entry.
def family_activities(path):
    import csv
    found = {}
    with open(path, newline="", errors="replace") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            fam, name, ec = (" ".join(row[k].split()) for k in ("Family", "Name", "EC_Number"))
            ec = "" if ec in ("", "NA", "-") else f" (EC {ec})"
            if fam and name:
                found.setdefault(fam, {})[name + ec] = None
    return {fam: f"{fam} " + "; ".join(acts) for fam, acts in found.items()}


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)

    # A server that answers with a web page passes the fetch, so read what landed.
    if open(isrc.local/HMM, "rb").read(6) != b"HMMER3":
        raise SystemExit(f"[dbcan] {HMM} is not an HMMER3 file")
    activities = family_activities(isrc.local/FAMILIES)
    if len(activities) < 100:
        raise SystemExit(f"[dbcan] {FAMILIES} names {len(activities)} families; "
                         f"it is not dbCAN's family-substrate map")

    idb.local.mkdir(parents=True, exist_ok=True)
    with open(idb.local/"descriptions.tsv", "w") as fh:
        for fam in sorted(activities):
            fh.write(f"{fam}\t{activities[fam]}\n")
    context.LocalShell(f"cp {isrc.local/HMM} {idb.local}/{HMM}")
    context.ExecWithEnv(env=image, cmd=f"hmmpress -f {idb.container}/{HMM}")

    pressed = all((idb.local/f"dbCAN.hmm.{ext}").exists() for ext in ("h3f", "h3i", "h3m", "h3p"))
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=pressed,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)

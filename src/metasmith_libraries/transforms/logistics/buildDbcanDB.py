from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddRequirement(lib.GetType("ref::dbcan_sources"))
db      = model.AddProduct(lib.GetType("ref::dbcan_hmm"))

HMM = "dbCAN-HMMdb-V11.txt"
ACTIVITIES = "CAZyDB.08062022.fam-activities.txt"


# DRAM 1.5's own reading of the family table (DatabaseHandler.process_dbcan_descriptions):
# the first field is the family, and a description that repeats it is not doubled.
def family_activities(path):
    out = {}
    with open(path, errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            words = line.strip().split()
            if len(words) == 1:
                out[words[0]] = words[0]
            elif words[0] == words[1]:
                out[words[0]] = " ".join(words[1:])
            else:
                out[words[0]] = " ".join(words)
    return out


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)

    # A server that answers with a web page passes the fetch, so read what landed.
    if open(isrc.local/HMM, "rb").read(6) != b"HMMER3":
        raise SystemExit(f"[dbcan] {HMM} is not an HMMER3 file")
    activities = family_activities(isrc.local/ACTIVITIES)
    if len(activities) < 100:
        raise SystemExit(f"[dbcan] {ACTIVITIES} names {len(activities)} families; "
                         f"it is not the CAZy family table")

    idb.local.mkdir(parents=True, exist_ok=True)
    with open(idb.local/"descriptions.tsv", "w") as fh:
        for fam in sorted(activities):
            fh.write(f"{fam}\t{activities[fam]}\n")
    context.LocalShell(f"cp {isrc.local/HMM} {idb.local}/dbCAN.hmm")
    context.ExecWithEnv(env=image, cmd=f"hmmpress -f {idb.container}/dbCAN.hmm")

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

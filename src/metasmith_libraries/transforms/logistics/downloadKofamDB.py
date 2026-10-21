from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
profiles = model.AddProduct(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddProduct(lib.GetType("ref::kofamscan_ko_list"))

# The top-level profiles.tar.gz and ko_list.gz are replaced at each KEGG release;
# archives/<date>/ keeps each pair, and the pair is what must agree.
BASE_URL = "https://www.genome.jp/ftp/db/kofam/archives"
RELEASE = "2026-08-02"

def protocol(context: ExecutionContext):
    iprofiles = context.Output(profiles)
    iko_list = context.Output(ko_list)
    base = context.params.get("kofam_base_url", BASE_URL).rstrip("/")
    release = context.params.get("kofam_release", RELEASE)
    Log.Info(f"KOfam release [{release}] from [{base}]")

    # `--strip-components=1` because the archive wraps everything in a
    # `profiles/` directory and kofamscan is handed a profile directory it does
    # not recurse into -- a nested level reads to it as zero profiles, which it
    # reports as zero annotations rather than as an error.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{release}/profiles.tar.gz", "profiles.tar.gz", label="KOfam profiles"),
        FetchCommand(f"{base}/{release}/ko_list.gz", "ko_list.gz", label="KOfam ko_list"),
        f"mkdir -p {iprofiles.local}",
        f"tar -xzf profiles.tar.gz -C {iprofiles.local} --strip-components=1",
        f"gunzip -c ko_list.gz > {iko_list.local}",
        "rm -f profiles.tar.gz ko_list.gz",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    n_hmm = len(list(iprofiles.local.glob("*.hmm"))) if iprofiles.local.is_dir() else 0
    n_ko = 0
    if iko_list.local.exists():
        with open(iko_list.local, errors="replace") as fh:
            n_ko = sum(1 for _ in fh) - 1
    Log.Info(f"unpacked {n_hmm:,} HMM profiles, {n_ko:,} scoring thresholds")
    if n_hmm and n_ko and abs(n_hmm - n_ko) > 0.05 * n_hmm:
        Log.Warn(f"{n_hmm:,} profiles against {n_ko:,} thresholds -- more than 5% apart, "
                 f"which is what a mismatched profiles/ko_list pair looks like")

    return ExecutionResult(
        manifest=[{profiles: iprofiles.local, ko_list: iko_list.local}],
        success=n_hmm > 0 and n_ko > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)

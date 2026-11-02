from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
sources  = model.AddRequirement(lib.GetType("ref::kofam_sources"))
profiles = model.AddProduct(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddProduct(lib.GetType("ref::kofamscan_ko_list"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    iprofiles = context.Output(profiles)
    iko_list = context.Output(ko_list)

    # `--strip-components=1` because the archive wraps everything in a
    # `profiles/` directory and kofamscan is handed a profile directory it does
    # not recurse into -- a nested level reads to it as zero profiles, which it
    # reports as zero annotations rather than as an error.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iprofiles.local}",
        f"tar -xzf {isrc.local}/profiles.tar.gz -C {iprofiles.local} --strip-components=1",
        f"gunzip -c {isrc.local}/ko_list.gz > {iko_list.local}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    hmms = sorted(iprofiles.local.glob("*.hmm"))
    with open(iko_list.local, errors="replace") as fh:
        header = fh.readline()
        n_ko = sum(1 for _ in fh)
    # A server that answers with a web page passes the fetch, so read what landed.
    profiles_ok = bool(hmms) and all(open(h, "rb").read(6) == b"HMMER3" for h in hmms[:: max(1, len(hmms)//50)])
    ko_list_ok = header.startswith("knum\t")
    Log.Info(f"unpacked {len(hmms):,} HMM profiles, {n_ko:,} scoring thresholds")
    if hmms and n_ko and abs(len(hmms) - n_ko) > 0.05 * len(hmms):
        Log.Warn(f"{len(hmms):,} profiles against {n_ko:,} thresholds -- more than 5% apart, "
                 f"which is what a mismatched profiles/ko_list pair looks like")

    return ExecutionResult(
        manifest=[{profiles: iprofiles.local, ko_list: iko_list.local}],
        success=profiles_ok and ko_list_ok and n_ko > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

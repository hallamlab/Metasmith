from pathlib import Path
from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
w_300m    = model.AddProduct(lib.GetType("ref::esm_c_300m_weights"))
w_600m    = model.AddProduct(lib.GetType("ref::esm_c_600m_weights"))

HF_ENDPOINT = "https://huggingface.co"
# The ESM-C repos moved from EvolutionaryScale/ to biohub/. The Hub redirects the old
# name for now; naming the new one does not depend on that lasting.
# esm_c.py runs with INFRA_PROVIDER=local, which reads data/weights/ under the
# unpacked directory, so the archive keeps the repo's layout.

# Each repo at a pinned revision, with the sha256 of each LFS object; the small
# git-tracked files have none.
REPOS = {
    w_300m: ("biohub/esmc-300m-2024-12", "7f10b20ae75017b2dbc884070e03434515709a8d", {
        "config.json":                           None,
        "data/weights/esmc_300m_2024_12_v0.pth": "323dff9fbf3fef297a74f4f18b6528e6f2e599b0bcf72b6927516804015becea",
    }),
    w_600m: ("biohub/esmc-600m-2024-12", "e4d83bc7e10fd55c92e598e545f4a76bf04a6e5c", {
        "config.json":                           None,
        "data/weights/esmc_600m_2024_12_v0.pth": "8ef856e1a237ee3f995442df997a962e70057faadecf38fc0c8561bd3c2f4324",
    }),
}


def fetch_repo(endpoint, repo, revision, files, into):
    lines = []
    for path, sha in files.items():
        dest = f"{into}/{path}"
        lines += [
            f"mkdir -p {Path(dest).parent}",
            FetchCommand(f"{endpoint}/{repo}/resolve/{revision}/{path}", dest, label=f"{repo}/{path}"),
        ]
        if sha is not None:
            lines.append(f'echo "{sha}  {dest}" | sha256sum -c --quiet - || {{ rm -f {dest}; exit 1; }}')
    return lines


def pack(files, into, tarball, listing):
    # The listing reads the whole gzip stream back, so a truncated archive fails here.
    return [
        f"tar -cf - -C {into} {' '.join(files)} | gzip -1 > {tarball}",
        f"tar -tzf {tarball} > {listing}",
        *(f"rm -f {into}/{path}" for path in files),
    ]


def packed(files, tarball, listing):
    return tarball.exists() and set(Path(listing).read_text().split()) >= set(files)


def staging(repo):
    return repo.replace("/", "__")


def protocol(context: ExecutionContext):
    endpoint = context.params.get("hf_endpoint", HF_ENDPOINT).rstrip("/")
    outputs  = {w_300m: context.Output(w_300m), w_600m: context.Output(w_600m)}

    shell = ["set -euo pipefail"]
    for product, (repo, revision, files) in REPOS.items():
        shell += [
            *fetch_repo(endpoint, repo, revision, files, staging(repo)),
            *pack(files, staging(repo), outputs[product].local, f"{staging(repo)}.list"),
        ]
    if context.LocalShell("\n".join(shell)) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{product: o.local for product, o in outputs.items()}],
        success=all(
            packed(files, outputs[product].local, f"{staging(repo)}.list")
            for product, (repo, _, files) in REPOS.items()
        ),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)

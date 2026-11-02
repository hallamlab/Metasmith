from pathlib import Path
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
weights  = model.AddProduct(lib.GetType("ref::esmfold_weights"))

HF_ENDPOINT = "https://huggingface.co"
HF_REPO     = "facebook/esmfold_v1"
REVISION    = "75a3841ee059df2bf4d56688166c8fb459ddd97a"
# The sha256 of each LFS object at REVISION; the small git-tracked files have none.
FILES = {
    "config.json":             None,
    "special_tokens_map.json": None,
    "tokenizer_config.json":   None,
    "vocab.txt":               None,
    "pytorch_model.bin":       "2ee07356b125d1e3e57503c204111fd7323347fc4735d41d3caac57c2a78e116",
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


def protocol(context: ExecutionContext):
    iweights = context.Output(weights)
    endpoint = context.params.get("hf_endpoint", HF_ENDPOINT).rstrip("/")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *fetch_repo(endpoint, HF_REPO, REVISION, FILES, "model"),
        *pack(FILES, "model", iweights.local, "model.list"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{weights: iweights.local}],
        success=packed(FILES, iweights.local, "model.list"),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=4),
    ),
)

from pathlib import Path
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
weights  = model.AddProduct(lib.GetType("ref::saprot_650m_weights"))

HF_ENDPOINT = "https://huggingface.co"
HF_REPO     = "westlake-repl/SaProt_650M_AF2"
REVISION    = "d9b9ad00ef61c0990e611b2b43f2231c7de24b38"
# The sha256 of each LFS object at REVISION; the small git-tracked files have none.
# SaProt_650M_AF2.pt is the same model as a raw checkpoint, which from_pretrained
# never reads, so it is left out.
FILES = {
    "config.json":             None,
    "special_tokens_map.json": None,
    "tokenizer_config.json":   None,
    "vocab.txt":               None,
    "pytorch_model.bin":       "513c39088804b74309d961174d5fc7aeabfb9b79d2814cdc7e465d4fe4b9f216",
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
        duration=Duration(hours=2),
    ),
)

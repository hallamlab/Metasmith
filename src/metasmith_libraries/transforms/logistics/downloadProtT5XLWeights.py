from pathlib import Path
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
weights  = model.AddProduct(lib.GetType("ref::prott5_xl_uniref50_weights"))

HF_ENDPOINT = "https://huggingface.co"
HF_REPO     = "Rostlab/prot_t5_xl_uniref50"
REVISION    = "973be27c52ee6474de9c945952a8008aeb2a1a73"
# The sha256 of each LFS object at REVISION; the small git-tracked files have none.
# pytorch_model_600k.bin and _723k.bin are earlier checkpoints, 11 GB each, that
# from_pretrained never reads, so they are left out.
FILES = {
    "config.json":             None,
    "special_tokens_map.json": None,
    "tokenizer_config.json":   None,
    "spiece.model":            "74da7b4afcde53faa570114b530c726135bdfcdb813dec3abfb27f9d44db7324",
    "pytorch_model.bin":       "c06141d42e93c12b5f6d95c701952156bd4079661f5f2d981d0d2036ba96dae9",
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

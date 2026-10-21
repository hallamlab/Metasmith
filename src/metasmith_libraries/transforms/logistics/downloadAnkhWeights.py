from pathlib import Path
from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
w_base    = model.AddProduct(lib.GetType("ref::ankh_base_weights"))
w_large   = model.AddProduct(lib.GetType("ref::ankh_large_weights"))

HF_ENDPOINT = "https://huggingface.co"
# Each repo at a pinned revision, with the sha256 of each LFS object; the small
# git-tracked files have none.
REPOS = {
    w_base: ("ElnaggarLab/ankh-base", "d99cb6b966530dfc2ae96bc69d9255c2a07308b0", {
        "config.json":             None,
        "special_tokens_map.json": None,
        "tokenizer.json":          None,
        "tokenizer_config.json":   None,
        "pytorch_model.bin":       "9b2a886374f0ff4a893f4e7a989deed76bb2458c8998bd5202ea8e97d92ddcc3",
    }),
    w_large: ("ElnaggarLab/ankh-large", "74b371dbfa3ee0a05d32ae74df0c2e0b82d6b9a6", {
        "config.json":             None,
        "special_tokens_map.json": None,
        "tokenizer.json":          None,
        "tokenizer_config.json":   None,
        "pytorch_model.bin":       "517b6e8b279dedcb477af240b35c46bd6eb3307723eb281e60d4b2c8a87b889b",
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
    outputs  = {w_base: context.Output(w_base), w_large: context.Output(w_large)}

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
        duration=Duration(hours=4),
    ),
)

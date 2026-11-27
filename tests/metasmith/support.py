from __future__ import annotations

import ast
import re
import contextlib
import functools
import http.server
import shutil
import threading
from pathlib import Path

import yaml

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary
from metasmith.models.solver import Endpoint

MOCK_TYPES = {
    "sample_metadata": {"sample_metadata"},
    "reads": {"reads"},
    "assembly": {"assembly"},
    "bam": {"bam"},
    "scattered": {"scattered"},
    "gathered": {"gathered"},
    "metabat2_bins": {"bins", "method:metabat2"},
    "maxbin2_bins": {"bins", "method:maxbin2"},
    "concoct_bins": {"bins", "method:concoct"},
    "container": {"container"},
    "annotated": {"annotated"},
    "branch_a": {"branch_a"},
    "branch_b": {"branch_b"},
    "merged": {"merged"},
}

_TRANSFORM_TYPES = """schema: v1
ontology:
  name: EDAM
  version: '1.25'
  doi: https://doi.org/10.1093/bioinformatics/btt113
  strict: false
types:
  transform:
    properties:
    - metasmith
    - transform
"""


def write_mock_types(root: Path) -> Path:
    types = DataTypeLibrary()
    for name, properties in MOCK_TYPES.items():
        types[name] = Endpoint(properties=set(properties))
    path = root / "mock_types.yml"
    types.Save(path)
    return path


def write_mock_samples(root: Path, mock_types: Path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(root / "samples.xgdb")
    lib.AddTypeLibrary(mock_types, namespace="mock")
    for i in range(3):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "metadata.json").write_text(f'{{"id": "{sid}"}}', encoding="utf-8")
        (sdir / "reads.fq").write_text(f">read_{i}\nACGT\n", encoding="utf-8")
        (sdir / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n", encoding="utf-8")
        m = lib.AddItem(Path(f"{sid}/metadata.json"), "mock::sample_metadata")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[r])
    lib.Save()
    # A plan refuses a given whose identity the calling process minted, and a driver
    # reaches a plan with a library read off disk, so the test gets the same shape.
    return DataInstanceLibrary.Load(lib.location)


def create_transform_library(base_dir: Path, mock_types: Path, transforms: dict[str, str]) -> TransformInstanceLibrary:
    tr_path = base_dir / "transforms.xgdb"
    types_dir = tr_path / "_metadata" / "types"
    types_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(mock_types, types_dir / "mock.yml")
    (types_dir / "transforms.yml").write_text(_TRANSFORM_TYPES, encoding="utf-8")

    manifest = {}
    for name, code in transforms.items():
        (tr_path / f"{name}.py").write_text(code, encoding="utf-8")
        manifest[f"{name}.py"] = {"type": "transforms::transform"}
    (tr_path / "_metadata" / "index.yml").write_text(
        yaml.dump({"manifest": manifest, "schema": "v1"}), encoding="utf-8",
    )
    return TransformInstanceLibrary.Load(tr_path)


def nxf_exec(container: str, workdir: Path, timeout: int) -> list[str]:
    # A timed-out client leaves its process running inside the container, so it carries its own limit.
    return [
        "docker", "exec", "-w", str(workdir),
        "-e", f"HOME={workdir}",
        "-e", f"NXF_HOME={workdir}/.nextflow",
        "-e", "NXF_DISABLE_CHECK_LATEST=true",
        container, "timeout", "-s", "KILL", str(timeout),
    ]


_FETCH_TOKENS = re.compile(
    r"\b(wget|curl|aria2c|prefetch|urlretrieve|urlopen|datasets download|virsorter setup)\b"
    r"|download-[a-z-]+|https?://|ftp://"
)


def source_downloads(source: str) -> bool:
    tree = ast.parse(source)
    docstrings = {id(n.value) for n in ast.walk(tree) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "FetchCommand":
            return True
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            if _FETCH_TOKENS.search(node.value):
                return True
    return False


def library_transforms(libraries_root: Path):
    transforms = libraries_root / "transforms"
    for d in sorted(p for p in transforms.iterdir() if (p / "_metadata").is_dir()):
        lib = TransformInstanceLibrary.Load(d)
        for key, tr in lib.IterateTransforms():
            yield f"{d.name}/{key}", tr, (d / tr._path).read_text()


@contextlib.contextmanager
def serve_directory(root: Path):
    hits: list[str] = []

    class _Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            super().do_GET()

        def log_message(self, *_):
            pass

    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(_Handler, directory=str(root)),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", hits
    finally:
        server.shutdown()
        server.server_close()

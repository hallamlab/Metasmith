"""What the database transforms tell a wget to do.

Pins I4 and I5 of the annotation-trio investigation. The reporter's second run
fetched the same 5.5 GB InterProScan archive three times, seventeen minutes and
then eleven and then fifty-one, and said nothing at all while it did -- because
the fetch is unconditional, unbounded, and quiet.

These read the command each transform emits rather than the transform's source,
so a rewrite that keeps the contract keeps the tests.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from metasmith.models.libraries import TransformInstanceLibrary


DOWNLOADERS = ["downloadUniRef50DB", "downloadKofamDB", "downloadInterProScanDB"]



@dataclass
class _RecordedPath:
    local: Path
    container: Path

    @property
    def external(self) -> Path:
        return self.local


@dataclass
class _RecordingContext:
    """Every surface the logistics protocols touch, and nothing else."""

    external_cwd: Path
    commands: list[str] = field(default_factory=list)
    local_shell_calls: list[str] = field(default_factory=list)
    params: dict = field(default_factory=dict)

    def StagingDir(self) -> _RecordedPath:
        d = self.external_cwd / "staging"
        d.mkdir(exist_ok=True)
        return _RecordedPath(local=d, container=d)

    def Output(self, dep) -> _RecordedPath:
        name = f"out_{getattr(dep, 'key', 'x')}"
        return _RecordedPath(
            local=self.external_cwd / name, container=Path("/ws") / name,
        )

    def ExecWithEnv(self, env=None, cmd="", **kw):
        self.commands.append(cmd)

    def LocalShell(self, cmd: str, **kw):
        self.local_shell_calls.append(cmd)
        return 0

    @property
    def emitted(self) -> list[str]:
        return self.local_shell_calls + self.commands


def _render(lib_root: Path, name: str, tmp_path: Path, url_override: str | None = None,
            staged: dict[str, str] | None = None):
    lib = TransformInstanceLibrary.Load(lib_root / "transforms" / "logistics")
    transform = lib.GetTransform(name)
    if url_override is not None:
        g = transform.protocol.__globals__
        for key, value in list(g.items()):
            if not key.isupper():
                continue
            if isinstance(value, str) and "://" in value:
                g[key] = url_override
            elif isinstance(value, list) and value and "://" in str(value[0]):
                g[key] = [url_override]
    ctx = _RecordingContext(external_cwd=tmp_path)
    for fname, text in (staged or {}).items():
        (ctx.StagingDir().local / fname).write_text(text)
    try:
        transform.protocol(ctx)
    except Exception:
        # The protocol's tail inspects products this never produced. The
        # commands it emitted before that point are the whole subject here.
        pass
    return ctx


def _wget_invocations(command: str) -> list[str]:
    out = []
    for chunk in re.split(r"(?:\|\||&&|;|\n)", command):
        chunk = chunk.replace("\\", " ").strip()
        if chunk.startswith("wget"):
            out.append(" ".join(chunk.split()))
    return out


def test_an_indexing_failure_cannot_discard_the_archive(metasmith_libraries_root):
    # I4: the download and the indexing were one transform, so an indexing failure
    # discarded the archive. They are two now, and the build reads the fetched fasta
    # as a product of its own, which a retry of the build does not fetch again.
    lib = TransformInstanceLibrary.Load(metasmith_libraries_root / "transforms" / "logistics")
    fetch = lib.GetTransform("downloadUniRef50DB.py")
    build = lib.GetTransform("buildUniRef50DB.py")
    fetched = {d.key for g in fetch.model.produces for d in g}
    assert "local" in fetch.labels and "local" not in build.labels
    assert any(d.key in fetched for d in build.model.requires)


@pytest.mark.parametrize("name", DOWNLOADERS)
def test_a_download_narrates_and_bounds_itself(metasmith_libraries_root, tmp_path, name):
    # I5: `wget -q` with no timeout and no retry cap. Ten to seventeen silent
    # minutes at a stretch, and a stalled transfer that looks like a working one.
    ctx = _render(metasmith_libraries_root, name, tmp_path)
    invocations = [w for c in ctx.emitted for w in _wget_invocations(c)]
    assert invocations, f"[{name}] emitted no wget"
    for w in invocations:
        assert "--progress=" in w, f"[{name}] fetches without saying anything: {w}"
        assert "timeout=" in w, f"[{name}] fetches with no timeout: {w}"
        assert "--tries=" in w, f"[{name}] fetches with unbounded retries: {w}"


# What a split fetch hands its build half. Each fetcher runs on the login node
# (`local`), so a check or unpack there costs the shared node hours.
SPLIT_FETCHERS = {
    "downloadKofamDB": None,
    "downloadInterProScanDB": None,
    "downloadEggnogDB": None,
    "downloadMetabuliDB": None,
    "downloadDramDB": None,
    "downloadGtdbDB": {"MD5SUM.txt": "abc  ./auxillary_files/gtdbtk_package/split_package/gtdbtk_r232_data.tar.gz.part_aa\n"},
}
_UNPACK = re.compile(r"tar [^|;]*-?x|gunzip|gzip -t|bunzip2|unzip |setup\.py|md5sum ")


@pytest.mark.parametrize("name", sorted(SPLIT_FETCHERS))
def test_a_split_fetch_neither_unpacks_nor_checks_on_the_login_node(metasmith_libraries_root, tmp_path, name):
    lib = TransformInstanceLibrary.Load(metasmith_libraries_root / "transforms" / "logistics")
    assert "local" in lib.GetTransform(name).labels
    ctx = _render(metasmith_libraries_root, name, tmp_path, staged=SPLIT_FETCHERS[name])
    fetched = [w for c in ctx.emitted for w in _wget_invocations(c)]
    assert fetched, f"[{name}] emitted no wget"
    # FetchCommand's own md5 of a part it fetched is the fetch's check, not a sweep.
    emitted = "\n".join(line for c in ctx.emitted for line in c.splitlines() if "md5sum {" not in line and ".part |" not in line)
    assert not _UNPACK.search(emitted), f"[{name}] unpacks or checks on the login node: {_UNPACK.search(emitted).group(0)}"

"""What the database transforms tell a wget to do.

Pins I4 and I5 of the annotation-trio investigation. The reporter's second run
fetched the same 5.5 GB InterProScan archive three times, seventeen minutes and
then eleven and then fifty-one, and said nothing at all while it did -- because
the fetch is unconditional, unbounded, and quiet.

These read the command each transform emits rather than the transform's source,
so a rewrite that keeps the contract keeps the tests.
"""

from __future__ import annotations

import http.server
import re
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from metasmith.models.libraries import TransformInstanceLibrary


DOWNLOADERS = ["downloadUniRef50DB", "downloadKofamDB", "downloadInterProScanDB"]

# Big enough that a repeat transfer is unmistakable, small enough to be free.
PAYLOAD = b"metasmith database payload\n" * 40_000


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

    def Output(self, dep) -> _RecordedPath:
        name = f"out_{getattr(dep, 'key', 'x')}"
        return _RecordedPath(
            local=self.external_cwd / name, container=Path("/ws") / name,
        )

    def ExecWithEnv(self, env=None, cmd="", **kw):
        self.commands.append(cmd)

    def LocalShell(self, cmd: str, **kw):
        self.local_shell_calls.append(cmd)


def _render(lib_root: Path, name: str, tmp_path: Path, url_override: str | None = None):
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


class _CountingHandler(http.server.BaseHTTPRequestHandler):
    served_bytes = 0

    def log_message(self, *a):
        pass

    def do_GET(self):
        start = 0
        rng = self.headers.get("Range")
        if rng:
            m = re.match(r"bytes=(\d+)-", rng)
            if m:
                start = int(m.group(1))
        if start >= len(PAYLOAD):
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{len(PAYLOAD)}")
            self.end_headers()
            return
        body = PAYLOAD[start:]
        self.send_response(206 if start else 200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        if start:
            self.send_header(
                "Content-Range",
                f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}",
            )
        self.end_headers()
        self.wfile.write(body)
        type(self).served_bytes += len(body)


@pytest.fixture
def counting_server():
    _CountingHandler.served_bytes = 0
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _CountingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/archive.tar.gz", _CountingHandler
    finally:
        server.shutdown()
        server.server_close()


def test_a_second_attempt_does_not_refetch_the_archive(
    metasmith_libraries_root, tmp_path, counting_server,
):
    # I4: the download and the indexing are one transform, so an indexing
    # failure discards the archive. Running the emitted command twice in the
    # same work directory must not put the payload on the wire twice.
    url, handler = counting_server
    work = tmp_path / "work"
    work.mkdir()
    ctx = _render(metasmith_libraries_root, "downloadUniRef50DB", work, url_override=url)
    assert ctx.commands, "the transform emitted no command"
    command = ctx.commands[0]

    for _ in range(2):
        # The indexing that follows the fetch is not installed here and is not
        # the subject; only what reached the wire is.
        subprocess.run(["bash", "-c", command], cwd=work, capture_output=True, timeout=300)

    assert handler.served_bytes < 1.5 * len(PAYLOAD), (
        f"the archive went over the wire twice: {handler.served_bytes} bytes "
        f"for a {len(PAYLOAD)} byte payload"
    )


@pytest.mark.parametrize("name", DOWNLOADERS)
def test_a_download_narrates_and_bounds_itself(metasmith_libraries_root, tmp_path, name):
    # I5: `wget -q` with no timeout and no retry cap. Ten to seventeen silent
    # minutes at a stretch, and a stalled transfer that looks like a working one.
    ctx = _render(metasmith_libraries_root, name, tmp_path)
    invocations = [w for c in ctx.commands for w in _wget_invocations(c)]
    assert invocations, f"[{name}] emitted no wget"
    for w in invocations:
        assert "--progress=" in w, f"[{name}] fetches without saying anything: {w}"
        assert "timeout=" in w, f"[{name}] fetches with no timeout: {w}"
        assert "--tries=" in w, f"[{name}] fetches with unbounded retries: {w}"

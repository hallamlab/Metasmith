"""Every URL a download contract declares still answers.

The addresses come from each transform's own `URLS`, the ones its protocol fetches from, so a moved link is
fixed in the contract and this test follows it. A list of URLs is mirrors of one file: one answering is enough.
"""
from __future__ import annotations

import base64
import ftplib
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest

from tests.metasmith.support import library_transforms

pytestmark = pytest.mark.network

LIBRARIES = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"
TIMEOUT_S = 30
PER_HOST = 4
ATTEMPTS = 2


def _http(url: str) -> str | None:
    parts = urlsplit(url)
    headers = {"User-Agent": "metasmith-url-smoke"}
    if parts.username:
        token = base64.b64encode(f"{parts.username}:{parts.password or ''}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
        host = parts.hostname + (f":{parts.port}" if parts.port else "")
        url = urlunsplit(parts._replace(netloc=host))
    why = ""
    # Some servers refuse HEAD but serve a one-byte range.
    for method, extra in (("HEAD", {}), ("GET", {"Range": "bytes=0-0"})):
        request = urllib.request.Request(url, method=method, headers={**headers, **extra})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_S):
                return None
        except urllib.error.HTTPError as e:
            if e.code == 416:
                return None
            why = f"{method} {e.code} {e.reason}"
        except (urllib.error.URLError, OSError) as e:
            why = f"{method} {type(e).__name__}: {getattr(e, 'reason', e)}"
    return why


def _ftp(url: str) -> str | None:
    parts = urlsplit(url)
    try:
        with ftplib.FTP(parts.hostname, timeout=TIMEOUT_S) as ftp:
            ftp.login()
            ftp.size(parts.path)
    except (ftplib.Error, OSError) as e:
        return f"FTP {type(e).__name__}: {e}"
    return None


def _probe(url: str, gates: dict[str, threading.Semaphore]) -> str | None:
    host = urlsplit(url).hostname
    for attempt in range(ATTEMPTS):
        with gates[host]:
            why = _ftp(url) if url.startswith("ftp://") else _http(url)
        if why is None:
            return None
        if attempt + 1 < ATTEMPTS:
            time.sleep(2)
    return why


def test_every_declared_url_responds():
    assert (LIBRARIES / "transforms" / "logistics" / "_metadata" / "index.yml").exists(), \
        "the standard library is not compiled; run dev/libraries.sh -bm"
    wanted: list[tuple[str, str, list[str]]] = []
    for name, tr, _ in library_transforms(LIBRARIES):
        for key, value in (tr.protocol.__globals__.get("URLS") or {}).items():
            wanted.append((name, key, [value] if isinstance(value, str) else list(value)))
    assert wanted, "no transform declares URLS"

    unique = sorted({u for _, _, mirrors in wanted for u in mirrors})
    gates = defaultdict(lambda: threading.Semaphore(PER_HOST))
    for u in unique:
        gates[urlsplit(u).hostname]
    with ThreadPoolExecutor(max_workers=32) as pool:
        answers = dict(zip(unique, pool.map(lambda u: _probe(u, gates), unique)))

    failed = [
        (name, key, "; ".join(f"{u} -> {answers[u]}" for u in mirrors if answers[u]))
        for name, key, mirrors in wanted
        if all(answers[u] for u in mirrors)
    ]
    table = "\n".join(f"  {name:45} {key:35} {what}" for name, key, what in failed)
    assert not failed, f"{len(failed)} of {len(wanted)} declared URLs do not respond ({len(unique)} probed):\n{table}"

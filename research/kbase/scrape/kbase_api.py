from __future__ import annotations

import hashlib
import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
CACHE = REPO / "data" / "kbase" / "raw"

CATALOG = "https://kbase.us/services/catalog"
NMS = "https://kbase.us/services/narrative_method_store/rpc"
WORKSPACE = "https://kbase.us/services/ws"
SEARCH = "https://kbase.us/services/searchapi2/rpc"

_MIN_INTERVAL = 0.05
_throttle = threading.Lock()
_last_call = [0.0]


class KBaseError(RuntimeError):
    pass


def _pace():
    with _throttle:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_call[0])
        if wait > 0: time.sleep(wait)
        _last_call[0] = time.monotonic()


def _post(url: str, payload: dict, timeout: int, retries: int) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    last = None
    for attempt in range(retries):
        _pace()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            # The Java services answer a bad request with HTTP 500 carrying a
            # JSON-RPC error body -- a retired type, a type name with no module.
            # That is deterministic, so retrying it only spends the backoff.
            try:
                return json.loads(e.read().decode())
            except Exception:
                last = e
                time.sleep(min(2 ** attempt, 30))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
            last = e
            time.sleep(min(2 ** attempt, 30))
    raise KBaseError(f"{url} failed after {retries} attempts: {last!r}")


def _cache_path(service: str, method: str, key: str) -> Path:
    return CACHE / service / method / f"{key}.json"


def _key(params) -> str:
    return hashlib.blake2b(
        json.dumps(params, sort_keys=True, default=str).encode(), digest_size=12
    ).hexdigest()


def call(service: str, url: str, method: str, params, *,
         jsonrpc2: bool = False, timeout: int = 90, retries: int = 4,
         refresh: bool = False):
    """One cached RPC. Returns the unwrapped result, or raises KBaseError."""
    path = _cache_path(service, method.split(".")[-1], _key(params))
    if path.exists() and not refresh:
        return json.loads(path.read_text())["result"]

    if jsonrpc2:
        payload = {"jsonrpc": "2.0", "method": method, "id": "1", "params": params}
    else:
        payload = {"method": method, "version": "1.1", "id": "1", "params": params}
    raw = _post(url, payload, timeout, retries)
    if "error" in raw and raw["error"] is not None:
        err = raw["error"]
        raise KBaseError(f"{method}: {err.get('message', err)}")
    result = raw.get("result")
    # The 1.1 services wrap every return in a single-element list; jsonrpc2 does not.
    if not jsonrpc2 and isinstance(result, list) and len(result) == 1:
        result = result[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"method": method, "params": params,
                                "fetched": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                "result": result}))
    return result


def catalog(method: str, params=None, **kw):
    return call("catalog", CATALOG, f"Catalog.{method}", [params or {}], **kw)


def nms(method: str, params=None, **kw):
    return call("nms", NMS, f"NarrativeMethodStore.{method}", [params or {}], **kw)


def workspace(method: str, params, **kw):
    return call("ws", WORKSPACE, f"Workspace.{method}", [params], **kw)


def search(method: str, params, **kw):
    return call("search", SEARCH, method, params, jsonrpc2=True, **kw)


def fanout(fn, items, workers: int = 8, label: str = ""):
    """Run fn over items with bounded concurrency; collect (item, result, error)."""
    out = []
    done = [0]
    lock = threading.Lock()

    def run(it):
        try:
            r, e = fn(it), None
        except Exception as exc:
            r, e = None, exc
        with lock:
            done[0] += 1
            if label and done[0] % 25 == 0:
                print(f"  {label}: {done[0]}/{len(items)}", flush=True)
        return it, r, e

    with ThreadPoolExecutor(max_workers=workers) as pool:
        out.extend(pool.map(run, items))
    return out


def image_digest(image: str, *, timeout: int = 30, refresh: bool = False):
    """Resolve an OCI image reference to its content digest via the registry.

    KBase tags embed the module's git commit, so a tag is already immutable by
    construction -- but a digest is what the library's pinning rule asks for,
    and the registry serves one unauthenticated.
    """
    path = _cache_path("registry", "digest", _key(image))
    if path.exists() and not refresh:
        return json.loads(path.read_text())["result"]

    host, _, rest = image.partition("/")
    repo, _, tag = rest.partition(":")
    url = f"https://{host}/v2/{repo}/manifests/{tag}"
    # The registry sits behind Cloudflare, which answers urllib's default
    # User-Agent with 403. Any ordinary client string is accepted.
    req = urllib.request.Request(url, method="HEAD", headers={
        "User-Agent": "curl/8.5.0",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json,"
                  "application/vnd.oci.image.manifest.v1+json,"
                  "application/vnd.docker.distribution.manifest.list.v2+json,"
                  "application/vnd.oci.image.index.v1+json",
    })
    _pace()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            result = {"digest": r.headers.get("docker-content-digest"), "error": None}
    except Exception as e:
        result = {"digest": None, "error": repr(e)[:200]}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"image": image, "result": result}))
    return result

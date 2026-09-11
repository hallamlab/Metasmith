#!/usr/bin/env python3
"""Fetch the KBase app catalog: modules, app specs and workspace types.

Every call is cached under data/kbase/raw/, so a re-run refetches nothing and an
interrupted run resumes where it stopped. Nothing here needs credentials.

    fetch.py modules      # 137 catalog modules, with git url / commit / image
    fetch.py apps         # 493 app specs, full per-parameter contract
    fetch.py types        # workspace type registry for every referenced type
    fetch.py images       # resolve each module image to a content digest
    fetch.py all
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import kbase_api as K

# Aggregates of raw specs, sized like the corpus they summarise. They live
# beside the cache under data/ and are pinned by DVC; only the small distilled
# tables distill.py writes belong in git.
OUT = K.REPO / "data" / "kbase" / "aggregate"

# get_method_spec resolves a spec at one release tag. Omitting the tag makes a
# dynamically registered module fail with "Repository X wasn't registered",
# which reads as a missing module rather than a missing argument. Legacy specs
# carry no module and accept no tag, so both forms are tried.
_SPEC_ATTEMPTS = ({"tag": "release"}, {"tag": "beta"}, {"tag": "dev"}, {})


def fetch_modules() -> dict:
    basic = K.catalog("list_basic_module_info")
    names = sorted(m["module_name"] for m in basic)
    print(f"catalog: {len(names)} modules")

    def one(name):
        return K.catalog("get_module_version", {"module_name": name})

    got = K.fanout(one, names, label="modules")
    modules = {}
    for name, ver, err in got:
        base = next(m for m in basic if m["module_name"] == name)
        modules[name] = {
            "module_name": name,
            "git_url": base.get("git_url"),
            "owners": base.get("owners"),
            "error": repr(err) if err else None,
            "version": (ver or {}).get("version"),
            "git_commit_hash": (ver or {}).get("git_commit_hash"),
            "docker_img_name": (ver or {}).get("docker_img_name"),
            "released": (ver or {}).get("released"),
            "dynamic_service": (ver or {}).get("dynamic_service"),
        }
    _write("modules.json", modules)
    failed = [n for n, m in modules.items() if m["error"]]
    print(f"  version records: {len(modules) - len(failed)} ok, {len(failed)} failed")
    return modules


def fetch_apps() -> dict:
    listed = K.nms("list_methods")
    print(f"nms: {len(listed)} apps listed")

    def one(entry):
        aid = entry["id"]
        last = None
        for extra in _SPEC_ATTEMPTS:
            try:
                got = K.nms("get_method_spec", {"ids": [aid], **extra})
                if got: return {"spec": got[0], "resolved_with": extra or None}
            except K.KBaseError as e:
                last = e
        raise K.KBaseError(f"no spec for [{aid}]: {last}")

    got = K.fanout(one, listed, workers=8, label="specs")
    apps = {}
    for entry, res, err in got:
        apps[entry["id"]] = {
            "listing": entry,
            "spec": (res or {}).get("spec"),
            "resolved_with": (res or {}).get("resolved_with"),
            "error": repr(err) if err else None,
        }
    _write("apps.json", apps)
    failed = [a for a, v in apps.items() if v["error"]]
    print(f"  full specs: {len(apps) - len(failed)} ok, {len(failed)} failed")
    return apps


def referenced_types(apps: dict) -> list[str]:
    seen = set()
    for v in apps.values():
        li = v.get("listing") or {}
        seen.update(li.get("input_types") or [])
        seen.update(li.get("output_types") or [])
        for p in ((v.get("spec") or {}).get("parameters") or []):
            seen.update(((p.get("text_options") or {}).get("valid_ws_types")) or [])
    return sorted(t for t in seen if t)


def fetch_types(apps: dict) -> dict:
    wanted = referenced_types(apps)
    print(f"ws: {len(wanted)} distinct types referenced")

    def one(t):
        return K.workspace("get_type_info", t)

    got = K.fanout(one, wanted, workers=6, label="types")
    types = {}
    for t, info, err in got:
        types[t] = {
            "requested": t,
            "error": repr(err) if err else None,
            "type_def": (info or {}).get("type_def"),
            "description": (info or {}).get("description"),
            "spec_def": (info or {}).get("spec_def"),
            "using_type_defs": (info or {}).get("using_type_defs"),
        }
    _write("types.json", types)
    failed = [t for t, v in types.items() if v["error"]]
    print(f"  registry: {len(types) - len(failed)} resolved, {len(failed)} unresolved")
    return types


def fetch_images(modules: dict) -> dict:
    wanted = sorted({m["docker_img_name"] for m in modules.values() if m.get("docker_img_name")})
    print(f"registry: {len(wanted)} module images")
    got = K.fanout(K.image_digest, wanted, workers=6, label="digests")
    out = {img: res for img, res, err in got}
    _write("images.json", out)
    failed = [i for i, r in out.items() if not r or not r.get("digest")]
    print(f"  digests: {len(out) - len(failed)} resolved, {len(failed)} failed")
    return out


def _write(name: str, obj) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    p = OUT / name
    p.write_text(json.dumps(obj, indent=1, sort_keys=True))
    print(f"  wrote {p.relative_to(K.REPO)} ({p.stat().st_size / 1e6:.1f} MB)")
    return p


def _load(name: str):
    p = OUT / name
    if not p.exists():
        sys.exit(f"missing {p} -- run the earlier phase first")
    return json.loads(p.read_text())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["modules", "apps", "types", "images", "all"])
    a = ap.parse_args()
    modules = fetch_modules() if a.phase in ("modules", "all") else None
    apps = fetch_apps() if a.phase in ("apps", "all") else None
    if a.phase in ("types", "all"): fetch_types(apps or _load("apps.json"))
    if a.phase in ("images", "all"): fetch_images(modules or _load("modules.json"))


if __name__ == "__main__":
    main()

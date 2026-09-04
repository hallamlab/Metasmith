#!/usr/bin/env python3
"""Distill the raw KBase scrape into the generator's input table and the census.

Writes catalog/apps.jsonl (one record per app, the only thing the generator
reads), catalog/types.jsonl, and catalog/census.json + census.md.

The census is the fixed denominator for the whole port. It is written before any
conversion decision so that no later filtering can move it.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

import kbase_api as K

CAT = K.REPO / "research" / "kbase" / "catalog"
AGG = K.REPO / "data" / "kbase" / "aggregate"

_TYPE_FAIL = (
    ("retired_type", "Unable to locate type"),
    ("retired_module", "Module doesn't exist"),
    ("malformed_reference", "could not be split into a module and name"),
)


def type_status(types: dict) -> dict:
    out = {}
    for name, rec in types.items():
        if not rec.get("error"):
            out[name] = {"status": "resolved", "type_def": rec.get("type_def")}
            continue
        reason = "fetch_failed"
        for tag, needle in _TYPE_FAIL:
            if needle in rec["error"]:
                reason = tag
                break
        out[name] = {"status": reason, "type_def": None}
    return out


def distill_app(aid: str, rec: dict, modules: dict, tstat: dict) -> dict:
    listing = rec.get("listing") or {}
    spec = rec.get("spec") or {}
    module = aid.split("/")[0] if "/" in aid else None
    m = modules.get(module or "", {})

    inputs, outputs, untyped_outputs = [], [], []
    for p in spec.get("parameters") or []:
        to = p.get("text_options") or {}
        ws = list(to.get("valid_ws_types") or [])
        entry = {
            "param_id": p.get("id"),
            "ws_types": ws,
            "optional": bool(p.get("optional")),
            "allow_multiple": bool(p.get("allow_multiple")),
            "advanced": bool(p.get("advanced")),
            "field_type": p.get("field_type"),
            "ui_name": p.get("ui_name"),
        }
        if to.get("is_output_name"):
            (outputs if ws else untyped_outputs).append(entry)
        elif ws:
            inputs.append(entry)

    behavior = spec.get("behavior") or {}
    # Three ways an app can name what it produces, and only the first is a
    # parameter flag. Every KBase app that emits a KBaseReport declares it in
    # the service output mapping instead, which is 79 of the 235 active apps --
    # dropping them for want of an is_output_name would halve the port.
    mapping = json.dumps(behavior.get("kb_service_output_mapping") or [])
    report_output = ("report_name" in mapping) or ("report_ref" in mapping)
    channel = ("typed" if outputs else "") + ("+report" if report_output and outputs else
                                              "report" if report_output else "")
    referenced = sorted({t for e in inputs + outputs for t in e["ws_types"]})
    return {
        "app_id": aid,
        "module": module,
        "legacy": module is None,
        "name": listing.get("name"),
        "subtitle": listing.get("subtitle"),
        "app_type": listing.get("app_type"),
        "categories": listing.get("categories") or [],
        "active": "active" in (listing.get("categories") or []),
        "authors": listing.get("authors") or [],
        "spec_version": (spec.get("info") or {}).get("ver") or listing.get("ver"),
        "resolved_with": rec.get("resolved_with"),
        "git_url": m.get("git_url"),
        "git_commit_hash": m.get("git_commit_hash"),
        "docker_img_name": m.get("docker_img_name"),
        "module_version": m.get("version"),
        "listing_input_types": listing.get("input_types") or [],
        "listing_output_types": listing.get("output_types") or [],
        "inputs": inputs,
        "outputs": outputs,
        "untyped_outputs": untyped_outputs,
        "report_output": report_output,
        "output_channel": channel or "none",
        "service": {
            "name": behavior.get("kb_service_name"),
            "method": behavior.get("kb_service_method"),
            "version": behavior.get("kb_service_version"),
        },
        "unresolved_types": [t for t in referenced if tstat.get(t, {}).get("status") != "resolved"],
    }


def census(apps: list[dict], tstat: dict, modules: dict) -> dict:
    def count(pred): return sum(1 for a in apps if pred(a))
    tfail = collections.Counter(v["status"] for v in tstat.values())
    hosts = collections.Counter()
    orgs = collections.Counter()
    for m in modules.values():
        u = m.get("git_url") or ""
        parts = u.split("/")
        hosts[parts[2] if len(parts) > 2 else "none"] += 1
        orgs[parts[3] if len(parts) > 3 else "none"] += 1

    return {
        "fetched": "2026-09-04",
        "apps": {
            "total": len(apps),
            "active": count(lambda a: a["active"]),
            "inactive": count(lambda a: not a["active"]),
            "runnable": count(lambda a: a["app_type"] == "app"),
            "viewer": count(lambda a: a["app_type"] == "viewer"),
            "with_listing_types": count(lambda a: a["listing_input_types"] or a["listing_output_types"]),
            "with_typed_inputs": count(lambda a: a["inputs"]),
            "with_typed_outputs": count(lambda a: a["outputs"]),
            "with_untyped_output_only": count(lambda a: not a["outputs"] and a["untyped_outputs"]),
            "with_report_output": count(lambda a: a["report_output"]),
            "with_any_output_channel": count(lambda a: a["output_channel"] != "none"),
            "with_no_output_channel": count(lambda a: a["output_channel"] == "none"),
            "by_output_channel": dict(collections.Counter(a["output_channel"] for a in apps).most_common()),
            "with_module": count(lambda a: a["module"] is not None),
            "legacy_no_module": count(lambda a: a["legacy"]),
            "with_docker_image": count(lambda a: a["docker_img_name"]),
            "referencing_a_dead_type": count(lambda a: a["unresolved_types"]),
        },
        "active_apps": {
            "total": count(lambda a: a["active"]),
            "runnable": count(lambda a: a["active"] and a["app_type"] == "app"),
            "viewer": count(lambda a: a["active"] and a["app_type"] == "viewer"),
            "with_typed_outputs": count(lambda a: a["active"] and a["outputs"]),
            "with_report_output": count(lambda a: a["active"] and a["report_output"]),
            "with_any_output_channel": count(lambda a: a["active"] and a["output_channel"] != "none"),
            "with_no_output_channel": count(lambda a: a["active"] and a["output_channel"] == "none"),
            "with_docker_image": count(lambda a: a["active"] and a["docker_img_name"]),
        },
        "types": {
            "referenced": len(tstat),
            "resolved": tfail.get("resolved", 0),
            "unresolved": sum(v for k, v in tfail.items() if k != "resolved"),
            "unresolved_by_reason": {k: v for k, v in sorted(tfail.items()) if k != "resolved"},
        },
        "modules": {
            "total": len(modules),
            "with_git_url": sum(1 for m in modules.values() if m.get("git_url")),
            "with_docker_image": sum(1 for m in modules.values() if m.get("docker_img_name")),
            "by_host": dict(hosts.most_common()),
            "by_org": dict(orgs.most_common()),
        },
    }


def census_md(c: dict) -> str:
    def block(title, d):
        rows = "\n".join(
            f"| {k} | {v if not isinstance(v, dict) else ', '.join(f'{a}: {b}' for a, b in v.items())} |"
            for k, v in d.items()
        )
        return f"### {title}\n\n| | |\n|---|---|\n{rows}\n"

    return (
        "# KBase census\n\n"
        "The fixed denominator for this port, written at scrape time on "
        f"{c['fetched']} before any conversion decision. `census.json` is the machine-readable\n"
        "form and wins over this file if the two disagree.\n\n"
        + block("Apps", c["apps"])
        + "\n" + block("Active apps", c["active_apps"])
        + "\n" + block("Workspace types", c["types"])
        + "\n" + block("Catalog modules", c["modules"])
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    modules = json.loads((AGG / "modules.json").read_text())
    raw_apps = json.loads((AGG / "apps.json").read_text())
    raw_types = json.loads((AGG / "types.json").read_text())

    tstat = type_status(raw_types)
    apps = [distill_app(a, r, modules, tstat) for a, r in sorted(raw_apps.items())]

    (CAT / "apps.jsonl").write_text("".join(json.dumps(a) + "\n" for a in apps))
    (CAT / "types.jsonl").write_text("".join(
        json.dumps({"ws_type": k, **v, "description": (raw_types[k].get("description") or "").strip()}) + "\n"
        for k, v in sorted(tstat.items())))

    c = census(apps, tstat, modules)
    (CAT / "census.json").write_text(json.dumps(c, indent=1))
    (CAT / "census.md").write_text(census_md(c))

    print(json.dumps(c, indent=1))


if __name__ == "__main__":
    main()

from __future__ import annotations

import re
from pathlib import Path

from metasmith.caching.keys import CACHE_KEY_VERSION, LIN_PAYLOAD_VERSION
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.nextflow_codegen import LIN_ECHO_EXPR
import metasmith.models.workflow as workflow_mod


_EMITTER_RE = re.compile(r"\[\s*v\s*:\s*(\d+)\s*,\s*entries\s*:")


def _hardcoded_wire_versions() -> list[int]:
    pkg = Path(workflow_mod.__file__).parent
    found: list[int] = []
    for src in sorted(pkg.rglob("*.py")):
        found += [int(m) for m in _EMITTER_RE.findall(src.read_text(encoding="utf-8"))]
    return found


def test_rendered_emitter_stamps_the_parser_version():
    found = _EMITTER_RE.findall(LIN_ECHO_EXPR)
    assert found, (
        f"no `[v:N, entries:...]` envelope in the rendered emitter "
        f"{LIN_ECHO_EXPR!r}; the guard is stale — update it to track the "
        f"current emitter so this invariant keeps being checked"
    )
    for n in found:
        assert int(n) == LIN_PAYLOAD_VERSION, (
            f"Groovy lin emitter stamps wire version {n} but the parser "
            f"(LinPayload) expects {LIN_PAYLOAD_VERSION}; a real deploy would "
            f"fail every task with 'unsupported lin payload version'."
        )


def test_no_module_hardcodes_the_wire_version():
    assert _hardcoded_wire_versions() == [], (
        "a module in metasmith.models.workflow spells the lin-envelope "
        "version as a literal. Interpolate LIN_PAYLOAD_VERSION instead — a "
        "literal is how R5 desynced the emitter from the parser and failed "
        "every containerized task with a green fast suite."
    )


def test_parser_classvar_tracks_module_constant():
    assert LinPayload.VERSION == LIN_PAYLOAD_VERSION


def test_cache_epoch_and_wire_version_are_independent():
    assert CACHE_KEY_VERSION == 3
    assert LIN_PAYLOAD_VERSION == 4
    assert CACHE_KEY_VERSION != LIN_PAYLOAD_VERSION, (
        "the two constants having drifted apart is the point; if a change ever "
        "makes them equal again, it must be a coincidence and not a re-merge"
    )


_GROOVY_KEY_RE = re.compile(
    r"public\s+static\s+final\s+String\s+(FILES_KEY|PROV_KEY)\s*=\s*\"([^\"]+)\""
)


def _orchestrator_source() -> str:
    src = Path(workflow_mod.__file__).parents[2] / "nextflow_config" / "Orchestrator.groovy"
    assert src.is_file(), f"Orchestrator.groovy not found at [{src}]"
    return src.read_text()


def test_groovy_reserved_keys_match_the_parser():
    found = dict(_GROOVY_KEY_RE.findall(_orchestrator_source()))
    assert found == {"FILES_KEY": LinPayload.FILES_KEY, "PROV_KEY": LinPayload.PROV_KEY}, (
        f"Orchestrator.groovy declares {found}, but the parser expects "
        f"FILES_KEY={LinPayload.FILES_KEY!r} PROV_KEY={LinPayload.PROV_KEY!r}. "
        "A renamed key on one side leaves the other reading a key nobody writes: "
        "FILES silently empties every input group, PROV silently disables "
        "provenance. Neither raises."
    )


def test_reserved_keys_are_the_set_lineage_index_filters():
    assert LinPayload.RESERVED_KEYS == {LinPayload.FILES_KEY, LinPayload.PROV_KEY}


def test_the_orchestrator_strips_every_reserved_key_on_the_way_out():
    src = _orchestrator_source()
    debatch = src.split("public def _debatch(")[1]
    assert "stripReserved(index)" in debatch, (
        "_debatch no longer routes its indexes through stripReserved; the "
        "strip is what keeps FILES and PROV out of every downstream index"
    )
    strip = src.split("public static Map stripReserved(")[1].split("\n    }\n")[0]
    for key in ("FILES_KEY", "PROV_KEY"):
        assert key in strip, (
            f"stripReserved does not strip {key}; it will propagate into "
            "every downstream index and into promoted shards"
        )

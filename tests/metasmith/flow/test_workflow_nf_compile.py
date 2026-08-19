from __future__ import annotations

from pathlib import Path

from metasmith.models import workflow as _wf


def _emitter_source() -> str:
    pkg = Path(_wf.__file__).parent
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(pkg.rglob("*.py"))
    )


def test_emitter_does_not_inject_groovy_import():
    src = _emitter_source()
    assert '"import groovy.json.JsonSlurper"' not in src, (
        "workflow.py must not emit a Groovy `import` declaration; "
        "Nextflow 26 strict parser rejects it. Use the fully-qualified "
        "name inline instead."
    )


def test_emitter_uses_fully_qualified_jsonslurper():
    src = _emitter_source()
    assert "new groovy.json.JsonSlurper()" in src, (
        "expected fully-qualified `new groovy.json.JsonSlurper()` "
        "instantiation in the emitted workflow.nf body"
    )
    assert "new JsonSlurper()" not in src, (
        "the bare-name `new JsonSlurper()` form requires an `import` "
        "which Nextflow 26 strict parser rejects"
    )

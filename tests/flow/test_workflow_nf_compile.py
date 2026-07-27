"""Regression test for Bug D — Nextflow 26.04.1 strict parser rejects
`import` declarations inside .nf scripts.

The emitter in `metasmith.models.workflow` used to inject
``import groovy.json.JsonSlurper`` at the top of every generated
workflow.nf and then call ``new JsonSlurper()`` later in the script.
Under Nextflow 26 strict parsing this fails with::

    workflow.nf:18:1: Groovy `import` declarations are not supported
                      -- use fully-qualified name inline instead

The fix removes the import and uses the fully-qualified class name
inline. This test pins the absence of the import and the presence of
the fully-qualified instantiation in the emitter source.
"""
from __future__ import annotations

import inspect

from metasmith.models import workflow as _wf


def _emitter_source() -> str:
    return inspect.getsource(_wf)


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

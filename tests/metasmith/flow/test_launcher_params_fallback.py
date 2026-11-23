from pathlib import Path

from metasmith import agents as agents_mod


def _agents_source() -> str:
    pkg = Path(agents_mod.__file__).parent
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(pkg.rglob("*.py"))
    )


def test_launcher_template_writes_valid_yaml_not_touch():
    src = _agents_source()
    assert "msm_write {AgentPaths.NXF_PARAMS} '{}'".replace("{}", "{{}}") in src, (
        "launcher source no longer writes `{}` as the NXF_PARAMS fallback"
    )
    assert "touch {AgentPaths.NXF_PARAMS}" not in src, (
        "launcher source still has the broken `touch NXF_PARAMS` fallback "
        "that produces a 0-byte file Nextflow can't parse"
    )

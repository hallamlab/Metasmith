import hashlib
import json
import os
import re
from pathlib import Path

import pytest

from tests.metasmith.cache.test_codegen import _stage

GOLDEN = Path(__file__).parent / "fixtures" / "template_golden.json"
REGEN = "MSM_REGEN_TEMPLATE_GOLDEN"


def _golden() -> dict:
    return json.loads(GOLDEN.read_text()) if GOLDEN.exists() else {}


def _normalized(path: Path, root: Path) -> str:
    text = path.read_text().replace(str(root), "ROOT")
    text = re.sub(r"msm-template-[A-Za-z0-9_]+", "msm-template-X", text)
    lines = text.split("\n")
    # bind lines come out of a set, so their order is not part of the plan
    idx = [i for i, ln in enumerate(lines) if re.match(r'^b\d+="', ln)]
    vals = sorted(lines[i].split("=", 1)[1] for i in idx)
    for i, v in zip(idx, vals):
        lines[i] = lines[i].split("=", 1)[0] + "=" + v
    return "\n".join(lines)


def _staged_digests(template, tmp_path: Path) -> dict[str, str]:
    ws = _stage(template.spec.Solve(), tmp_path)
    out = {}
    for p in sorted([*ws.glob("*.nf"), *ws.glob("*.json"), *ws.glob("*.meta")]):
        text = _normalized(p, tmp_path)
        (tmp_path / "normalized").mkdir(exist_ok=True)
        (tmp_path / "normalized" / p.name).write_text(text)
        out[p.name] = hashlib.sha256(text.encode()).hexdigest()
    return out


def _templates(root: Path) -> dict:
    from metasmith.agents import Template

    return {t.name: t for t in Template.Discover(root)}


def test_the_golden_names_every_shipped_template(metasmith_libraries_root):
    assert sorted(_templates(metasmith_libraries_root)) == sorted(_golden())


def _names() -> list[str]:
    if os.environ.get(REGEN):
        root = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"
        return sorted(p.parent.name for p in (root / "templates").glob("*/spec.yml"))
    return sorted(_golden())


@pytest.mark.parametrize("name", _names())
def test_a_shipped_template_stages_byte_identical_to_its_golden(
    name, metasmith_libraries_root, tmp_path
):
    template = _templates(metasmith_libraries_root)[name]
    got = _staged_digests(template, tmp_path)
    if os.environ.get(REGEN):
        golden = _golden()
        golden[name] = got
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return
    want = _golden()[name]
    differ = sorted(k for k in want.keys() | got.keys() if want.get(k) != got.get(k))
    assert not differ, (
        f"{name}: {differ} changed; normalized output is in {tmp_path / 'normalized'}. "
        f"If the change is intended, regenerate with {REGEN}=1."
    )

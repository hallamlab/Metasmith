import os
import stat
import sys
from pathlib import Path

import yaml

HERE = Path(os.path.realpath(__file__)).parent
PKG = HERE.parent.parent / "src" / "ecspr"
sys.path.insert(0, str(PKG.parent))

from ecspr.model import NAME, SHORT_SUMMARY, USER, ENTRY_POINTS, VERSION, BUILD_HASH  # noqa: E402

BUILD_STRING = f"py_{BUILD_HASH}" if BUILD_HASH else "py_0"

TEST_ONLY = {"pytest", "networkx", "pip"}


def _name(dep: str) -> str:
    for sep in ("=", "<", ">", "!", " "):
        dep = dep.split(sep)[0]
    return dep.strip()


raw = yaml.safe_load((PKG / "env.yml").read_text())
deps = [d for d in raw["dependencies"]
        if isinstance(d, str) and _name(d) not in TEST_ONLY]
reqs = "\n".join(f"    - {d}" for d in deps)
python_ver = "\n".join(f"    - {d}" for d in deps if d.startswith("python=")) \
    or "    - python=3.12"
entry = "\n".join(f"    - {e}" for e in ENTRY_POINTS)

template = (HERE / "meta_template.yaml").read_text()
for k, v in {"USER": USER, "NAME": NAME, "SHORT_SUMMARY": SHORT_SUMMARY,
             "VERSION": VERSION, "BUILD_STRING": BUILD_STRING,
             "ENTRY": entry, "REQUIREMENTS": reqs,
             "PYTHON": python_ver}.items():
    template = template.replace(f"<{k}>", v)
(HERE / "meta.yaml").write_text(template)

build_file = HERE / "call_build.sh"
channels = " ".join(f"-c {ch}" for ch in raw["channels"])
build_file.write_text(
    'HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )\n'
    f"conda mambabuild {channels} --output-folder $HERE/../../conda_build $HERE/\n")
build_file.chmod(build_file.stat().st_mode | stat.S_IEXEC)
print(f"wrote {HERE / 'meta.yaml'} and {build_file}")

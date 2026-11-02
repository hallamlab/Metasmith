import os
import stat
import sys
from pathlib import Path

import yaml

HERE = Path(os.path.realpath(__file__)).parent
PKG = HERE.parent.parent / "src" / "fabfos"
sys.path.insert(0, str(PKG.parent))

from fabfos import NAME, SHORT_SUMMARY, USER, ENTRY_POINTS, __version__ as VERSION  # noqa: E402

ENVS = HERE.parent.parent / "envs" / "fabfos" / "base.yml"


def _parse_deps(level: list, compiled: str, depth: int) -> str:
    tabs = "  " * depth
    for item in level:
        if not isinstance(item, str) or item == "pip":
            continue
        compiled += f"{tabs}- {item}\n"
    return compiled[:-1]


raw = yaml.safe_load(ENVS.read_text())
reqs = _parse_deps(raw["dependencies"], "", 2)
python_dep = [d for d in raw["dependencies"] if isinstance(d, str) and d.startswith("python=")]
python_ver = _parse_deps(python_dep or ["python=3.12"], "", 2)
entry = "\n".join(f"    - {e}" for e in ENTRY_POINTS)

template = (HERE / "meta_template.yaml").read_text()
for k, v in {"USER": USER, "NAME": NAME, "SHORT_SUMMARY": SHORT_SUMMARY,
             "VERSION": VERSION, "ENTRY": entry, "REQUIREMENTS": reqs,
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

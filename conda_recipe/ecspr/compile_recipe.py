import argparse
import os
import stat
import sys
from pathlib import Path

import yaml

HERE = Path(os.path.realpath(__file__)).parent
PKG = HERE.parent.parent / "src" / "ecspr"
ENV_SPEC = HERE.parent.parent / "envs" / "ecspr" / "base.yml"
sys.path.insert(0, str(PKG.parent))

from ecspr import NAME, SHORT_SUMMARY, USER, ENTRY_POINTS, VERSION, BUILD_HASH  # noqa: E402

TEST_ONLY = {"pytest", "networkx", "pyyaml", "pip"}


def _name(dep: str) -> str:
    for sep in ("=", "<", ">", "!", " "):
        dep = dep.split(sep)[0]
    return dep.strip()


def compile_recipe(out_dir: Path) -> tuple[Path, Path]:
    out_dir = out_dir.resolve()
    if not BUILD_HASH:
        raise SystemExit(
            f"refusing to compile a recipe from an unstamped tree: {PKG}/build_hash.txt "
            f"is missing, so the package would claim bare {VERSION} and stop being "
            f"traceable to a source state. Stamp it first:\n"
            f"    PYTHONPATH={PKG.parent} python -m ecspr._build_hash --write")

    raw = yaml.safe_load(ENV_SPEC.read_text())
    deps = [d for d in raw["dependencies"]
            if isinstance(d, str) and _name(d) not in TEST_ONLY]
    reqs = "\n".join(f"    - {d}" for d in deps)
    python_ver = "\n".join(f"    - {d}" for d in deps if d.startswith("python=")) \
        or "    - python=3.12"
    entry = "\n".join(f"    - {e}" for e in ENTRY_POINTS)

    template = (HERE / "meta_template.yaml").read_text()
    for k, v in {"USER": USER, "NAME": NAME, "SHORT_SUMMARY": SHORT_SUMMARY,
                 "VERSION": VERSION, "BUILD_STRING": f"py_{BUILD_HASH}",
                 "ENTRY": entry, "REQUIREMENTS": reqs,
                 "PYTHON": python_ver}.items():
        template = template.replace(f"<{k}>", v)

    # `source.path` and `license_file` are relative to the recipe directory, so a
    # recipe compiled anywhere but HERE has to be told where the tree is.
    if out_dir != HERE:
        template = (template
                    .replace("path: ../../src/ecspr", f"path: {PKG}")
                    .replace("license_file: ../../LICENSE",
                             f"license_file: {HERE.parent.parent / 'LICENSE'}"))

    out_dir.mkdir(parents=True, exist_ok=True)
    meta = out_dir / "meta.yaml"
    meta.write_text(template)

    build_file = out_dir / "call_build.sh"
    channels = " ".join(f"-c {ch}" for ch in raw["channels"])
    build_file.write_text(
        'HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )\n'
        f"conda mambabuild {channels} "
        f"--output-folder {HERE.parent.parent / 'conda_build'} $HERE/\n")
    build_file.chmod(build_file.stat().st_mode | stat.S_IEXEC)
    return meta, build_file


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="compile the ecspr conda recipe from src/ecspr's own constants")
    ap.add_argument("--out-dir", type=Path, default=HERE,
                    help="where to write meta.yaml and call_build.sh (default: here)")
    args = ap.parse_args(argv)
    meta, build_file = compile_recipe(args.out_dir)
    print(f"wrote {meta} and {build_file}")


if __name__ == "__main__":
    main()

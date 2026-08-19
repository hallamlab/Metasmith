import sys, shutil, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(ROOT))

from metasmith.python_api import (
    Agent, Source, Runtime,
    DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size, Duration,
)

DOMAINS = [
    "assembly",
    "functionalAnnotation",
    "logistics",
    "metabolicModelling",
    "metagenomics",
    "pangenome",
    "responseSurface",
    "transcriptomics",
]

passed = 0
failed = 0
errors = []

def check(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS  {name}")
        passed += 1
    except Exception as e:
        print(f"  FAIL  {name}: {e}")
        errors.append((name, e))
        failed += 1


print("\n[1] Loading data_types/")
for yml in sorted((ROOT / "data_types").glob("*.yml")):
    ns = yml.stem
    def _load(p=yml):
        tl = DataTypeLibrary.Load(p)
        n = len(list(tl))
        assert n > 0, f"no types in {p.name}"
    check(f"data_types/{ns} ({yml.name})", _load)


print("\n[2] Loading resources/")
for res in ["env", "lib"]:
    def _load(r=res):
        DataInstanceLibrary.Load(ROOT / f"resources/{r}")
    check(f"resources/{res}", _load)


print("\n[3] Loading and iterating transform libraries")
for domain in DOMAINS:
    def _load_iter(d=domain):
        tl = TransformInstanceLibrary.Load(ROOT / f"transforms/{d}")
        count = 0
        for path, tr in tl.IterateTransforms():
            assert tr.model is not None, f"{path} has no model"
            assert len(tr.model.requires) > 0, f"{path} has no requirements"
            assert len(tr.model.produces) > 0, f"{path} has no products"
            count += 1
        assert count > 0, f"no transforms in {d}"
    check(f"transforms/{domain}", _load_iter)


print("\n[4] Cross-domain workflow generation")

tmpdir = Path(tempfile.mkdtemp(prefix="msm_test_"))

resources = [
    DataInstanceLibrary.Load(ROOT / f"resources/{n}")
    for n in ["env", "lib"]
]
all_transforms = [
    TransformInstanceLibrary.Load(ROOT / f"transforms/{d}")
    for d in DOMAINS
]

def make_inputs(type_ns, type_file, item_type, item_path="/dev/null", label="test_input"):
    in_dir = tmpdir / f"inputs_{type_ns}"
    if in_dir.exists():
        shutil.rmtree(in_dir)
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(namespace=type_ns, lib=DataTypeLibrary.Load(ROOT / f"data_types/{type_file}"))
    inputs.AddItem(item_path, item_type)
    inputs.Save()
    return inputs

def make_params():
    p_dir = tmpdir / "params"
    if p_dir.exists():
        shutil.rmtree(p_dir)
    params = DataInstanceLibrary(p_dir)
    params.Purge()
    params.AddTypeLibrary(namespace="clustering", lib=DataTypeLibrary.Load(ROOT / "data_types/clustering.yml"))
    params.AddValue("min_identity.txt", "0.9", "clustering::min_identity")
    params.Save()
    return params

agent_home = Source.FromLocal(tmpdir / "agent_home")

test_cases = [
    (
        "assembly -> open_reading_frames",
        lambda: make_inputs("sequences", "sequences.yml", "sequences::assembly"),
        "sequences::orfs",
        "sequences::assembly",
    ),
    (
        "assembly -> augmented_centroids (multi-step chain)",
        lambda: make_inputs("sequences", "sequences.yml", "sequences::assembly"),
        "clustering::augmented_centroids",
        "sequences::assembly",
        make_params,
    ),
    (
        "assembly -> centroids (clustering)",
        lambda: make_inputs("sequences", "sequences.yml", "sequences::assembly"),
        "clustering::centroids",
        "sequences::assembly",
        make_params,
    ),
    (
        "assembly -> antismash_json",
        lambda: make_inputs("sequences", "sequences.yml", "sequences::assembly"),
        "annotation::antismash_json",
        "sequences::assembly",
    ),
    (
        "assembly -> genomad_virus_summary",
        lambda: make_inputs("sequences", "sequences.yml", "sequences::assembly"),
        "taxonomy::genomad_virus_summary",
        "sequences::assembly",
    ),
]

smith = Agent(home=agent_home, runtime=Runtime.DOCKER)

for case in test_cases:
    name, setup_fn, target_type, sample_type = case[:4]
    extra_fn = case[4] if len(case) > 4 else None
    def _gen(sf=setup_fn, tt=target_type, st=sample_type, xf=extra_fn):
        inputs = sf()
        targets = TargetBuilder()
        targets.Add(tt)
        task = smith.GenerateWorkflow(
            samples=inputs.AsSamples(st),
            resources=resources + ([xf()] if xf else []),
            transforms=all_transforms,
            targets=targets,
        )
        assert task.ok, f"workflow generation failed for target {tt}"
        assert len(task.plan.steps) > 0, f"no steps for target {tt}"
    check(name, _gen)


print("\n[5] Type cross-reference validation")

import re

def _check_type_refs():
    defined_types = set()
    for yml in (ROOT / "data_types").glob("*.yml"):
        ns = yml.stem
        tl = DataTypeLibrary.Load(yml)
        for name, _ in tl:
            defined_types.add(f"{ns}::{name}")

    missing = []
    for py_file in ROOT.glob("transforms/**/*.py"):
        if py_file.name.startswith("_"):
            continue
        if "_disabled" in str(py_file):
            continue
        content = py_file.read_text()
        refs = re.findall(r'GetType\("([^"]+)"\)', content)
        for ref in refs:
            if ref not in defined_types:
                missing.append((py_file.relative_to(ROOT), ref))

    if missing:
        msg = "\n".join(f"  {path}: {ref}" for path, ref in missing)
        raise AssertionError(f"Unresolved type references:\n{msg}")

check("all GetType() references resolve to defined types", _check_type_refs)


shutil.rmtree(tmpdir, ignore_errors=True)

print(f"\n{'='*50}")
print(f"Results: {passed} passed, {failed} failed")
if errors:
    print("\nFailures:")
    for name, e in errors:
        print(f"  {name}: {e}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)

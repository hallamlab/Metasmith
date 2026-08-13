from typing import Any
from pathlib import Path
import yaml
import os
from metasmith.python_api import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary

print("compiling std library")

HERE = Path(__file__).parent
OUTPUT = (HERE/"../../../src/metasmith/std").resolve()

def verify_and_sync(src: Path, dest: Path, items: list[tuple]):
    items_actual = set()
    for f in src.iterdir():
        items_actual.add(f.name)
    items_planned = {dest for src, dest, tp in items}
    missing = items_planned-items_actual
    extra = items_actual-items_planned
    assert len(missing)==0, missing
    assert len(extra)==0, extra
    os.system(f"rsync -acP {src}/ {dest}")
    staged = set()
    for x in dest.iterdir():
        if x.name == "_metadata": continue
        staged.add(x.name)
    junk = staged-items_actual
    if len(junk)>0:
        for x in junk:
            if x in {"__pycache__"}: continue
            print(f"cleaning [{x}]")
            os.system(f"rm -r {dest/x}")

# -----------------------
# std data types
print(f"data types")
dtypes = DataTypeLibrary.Load(HERE/"dtypes.yml")
os.system(f"rsync -acP {HERE/'dtypes.yml'} {OUTPUT}")

# -----------------------
# containers
print(f"containers")
with open(HERE/"index_containers.yml") as f:
    d = yaml.safe_load("".join(f.readlines()))
items: list[Any] = [
    (HERE/f"containers/{k}", k, v['type'])
    for k, v in d["manifest"].items()
]
containers = DataInstanceLibrary(OUTPUT/"containers.xgdb")
containers.AddTypeLibrary("std", dtypes)
for s, d, t in items: containers.AddItem(d, t)
containers.Save()
verify_and_sync(HERE/"containers", containers.location, items)

# -----------------------
# transforms
print(f"transforms")
transforms = TransformInstanceLibrary(OUTPUT/"transforms.xgdb")
transforms.AddTypeLibrary("std", dtypes)
def check_protocol_file(p: Path):
    assert p.is_file() and p.suffix == ".py"
    return True
procedures: list[Any] = [
    (p, p.name, "transforms::transform")
    for p in (HERE/"transforms").iterdir() if check_protocol_file(p)
]
for s, d, t in procedures: transforms.AddItem(d, t)
transforms.Save()
verify_and_sync(HERE/"transforms", transforms.location, procedures)

# -----------------------
print("completed compilation of std library")

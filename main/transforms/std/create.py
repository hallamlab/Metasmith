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
    assert items_actual == items_planned
    os.system(f"rsync -acP {src}/ {dest}")
    staged = set()
    for x in dest.iterdir():
        if x.name == "_metadata": continue
        staged.add(x.name)
    junk = staged-items_actual
    if len(junk)>0:
        for x in junk:
            print(f"cleaning [{x}]")
            os.system(f"rm -r {dest/x}")

# -----------------------
# std data types
dtypes = DataTypeLibrary.Load(HERE/"dtypes.yml")

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
containers.Add(items, transfer_method=None)
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
transforms.Add(procedures, transfer_method=None)
transforms.Save()
verify_and_sync(HERE/"transforms", transforms.location, procedures)

# -----------------------
print("completed compilation of std library")

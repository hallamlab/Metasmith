from pathlib import Path
import os
from metasmith.python_api import DataInstanceLibrary

res_path= Path("./cache/local_home/runs/nlq8TEBZ/results")
reslib = DataInstanceLibrary.Load(res_path)
to_rename = {}
for path, name, type in reslib.Iterate():
    if path.is_absolute(): continue
    print(name, path)
    for parent in reslib.parents[path]:
        if parent.name != "sequences::read_pair": continue
        print(" ", parent.name)
        print(" ", parent.path)
        print()
        to_rename[path] = path.parent/f"{parent.path.stem}.fq.gz"
        break

for a, b in to_rename.items():
    if b in reslib: continue
    print(f"{a} -> {b}")
    reslib.Rename(a, b, _save=False)
reslib.Save()

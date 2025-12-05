from pathlib import Path
import yaml

from ..models.solver import Endpoint
from ..models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary

def Build(data_type_dirs: list[Path], transform_dirs: list[Path], unique_dirs: list[Path]):

    # print(data_type_dirs)
    # print(transform_dirs)
    # print(unique_dirs)

    def dir_ok(d: Path):
        if not d.is_dir(): return False
        if any(d.name.startswith(x) for x in [".", "_"]): return False
        return True

    def file_ok(f: Path):
        if f.is_dir(): return False
        if any(f.name.startswith(x) for x in [".", "_"]): return False
        return True

    dtypes: dict[str, DataTypeLibrary] = {}
    for d in data_type_dirs:
        if not dir_ok(d): continue
        for f in d.iterdir():
            namespace = f.stem
            if f.suffix not in {".yml", ".yaml"}: continue
            if not file_ok(f): continue
            dtypes[namespace]=DataTypeLibrary.Load(f)

    for d in unique_dirs:
        if not dir_ok(d): continue
        lib = DataInstanceLibrary(d)
        namespace = d.name
        lib.AddTypeLibrary(namespace, dtypes[namespace])
        for f in d.iterdir():
            if not file_ok(f) and not dir_ok(f): continue
            lib.AddItem(f.name, f"{namespace}::{f.name}")
        lib.PruneTypes() # saves

    for d in transform_dirs:
        if not dir_ok(d): continue
        lib = TransformInstanceLibrary(d)
        for k, dlib in dtypes.items():
            lib.AddTypeLibrary(k, dlib)
        count = 0
        for f in d.iterdir():
            if f.suffix != ".py": continue
            if not file_ok(f): continue
            count += 1
            lib.AddItem(f.name, "transforms::transform")
        if count>0:
            lib.Save()
            lib.PruneTypes() # saves
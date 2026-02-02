from pathlib import Path

from ..models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary
from ..logging import Log

def Build(data_type_dirs: list[Path], transform_dirs: list[Path], unique_dirs: list[Path]):

    # print(data_type_dirs)
    # print(transform_dirs)
    # print(unique_dirs)
    DISABLE = '_disabled'

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
            if f.name.startswith(DISABLE):
                Log.Warn(f"skipping: [{f.stem}]")
                continue
            namespace = f.stem
            if f.suffix not in {".yml", ".yaml"}: continue
            if not file_ok(f): continue
            dtypes[namespace]=DataTypeLibrary.Load(f)
            Log.Info(f"adding [{len(dtypes[namespace].types)}] types from [{namespace}]")

    for d in unique_dirs:
        if not dir_ok(d): continue
        lib = DataInstanceLibrary(d)
        namespace = d.name
        lib.AddTypeLibrary(namespace=namespace, lib=dtypes[namespace])
        c = 0
        for f in d.iterdir():
            if f.name.startswith(DISABLE):
                Log.Warn(f"skipping: [{f.stem}]")
                continue
            if not file_ok(f) and not dir_ok(f): continue
            lib.AddItem(f.name, f"{namespace}::{f.name}")
            c += 1
        lib.Save()
        Log.Info(f"compiled [{c}] data resources from [{namespace}]")

    for d in transform_dirs:
        if not dir_ok(d): continue
        lib = TransformInstanceLibrary(d)
        for k, dlib in dtypes.items():
            lib.AddTypeLibrary(namespace=k, lib=dlib)
        count = 0
        # for f in d.iterdir():
        for f in d.glob("**/*.py"):
            rel_f = f.relative_to(d)
            if DISABLE in str(f):
                # Log.Warn(f"skipping: [{d.name}/{f.stem}]")
                Log.Warn(f"skipping: [{rel_f}]")
                continue
            if f.suffix != ".py": continue
            if not file_ok(f): continue
            count += 1
            lib.AddItem(rel_f, "transforms::transform")
        if count>0:
            lib.Save()
            lib.PruneTypes(save=True) # saves
            Log.Info(f"compiled [{count}] transforms from [{d.name}]")
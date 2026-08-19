from pathlib import Path

from ..models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary
from ..logging import Log


DISABLE_PREFIX = "_disabled"


def _dir_ok(d: Path) -> bool:
    if not d.is_dir(): return False
    if any(d.name.startswith(x) for x in [".", "_"]): return False
    return True


def _file_ok(f: Path) -> bool:
    if f.is_dir(): return False
    if any(f.name.startswith(x) for x in [".", "_"]): return False
    return True


def _is_disabled(path: Path) -> bool:
    return any(part.startswith(DISABLE_PREFIX) for part in path.parts)


def LoadTypeLibraries(data_type_dirs: list[Path]) -> dict[str, DataTypeLibrary]:
    dtypes: dict[str, DataTypeLibrary] = {}
    sources: dict[str, Path] = {}
    for d in data_type_dirs:
        if not _dir_ok(d): continue
        for f in d.iterdir():
            if _is_disabled(f):
                Log.Warn(f"skipping: [{f.stem}]")
                continue
            if f.suffix not in {".yml", ".yaml"}: continue
            if not _file_ok(f): continue
            namespace = f.stem
            if namespace in dtypes:
                raise ValueError(
                    f"duplicate type namespace [{namespace}]: "
                    f"already loaded from [{sources[namespace]}], also found at [{f}]"
                )
            dtypes[namespace] = DataTypeLibrary.Load(f)
            sources[namespace] = f
            Log.Info(f"adding [{len(dtypes[namespace].types)}] types from [{namespace}]")
    return dtypes


def CompileUniqueLibrary(unique_dir: Path, types: dict[str, DataTypeLibrary]) -> dict:
    if not _dir_ok(unique_dir):
        return {"library": str(unique_dir), "namespace": unique_dir.name, "count": 0, "skipped": "filtered"}
    namespace = unique_dir.name
    if namespace not in types:
        raise KeyError(
            f"unique dir [{unique_dir}] has no matching type library "
            f"(looked for namespace [{namespace}]); available: {sorted(types.keys())}"
        )
    lib = DataInstanceLibrary(unique_dir)
    lib.AddTypeLibrary(namespace=namespace, lib=types[namespace])
    count = 0
    for f in unique_dir.iterdir():
        if _is_disabled(f):
            Log.Warn(f"skipping: [{f.stem}]")
            continue
        if not _file_ok(f) and not _dir_ok(f): continue
        lib.AddItem(f.name, f"{namespace}::{f.name}")
        count += 1
    lib.Save()
    Log.Info(f"compiled [{count}] data resources from [{namespace}]")
    return {"library": str(unique_dir), "namespace": namespace, "count": count}


def CompileTransformLibrary(transform_dir: Path, types: dict[str, DataTypeLibrary]) -> dict:
    if not _dir_ok(transform_dir):
        return {"library": str(transform_dir), "count": 0, "skipped": "filtered"}
    lib = TransformInstanceLibrary(transform_dir)
    for k, dlib in types.items():
        lib.AddTypeLibrary(namespace=k, lib=dlib)
    count = 0
    for f in transform_dir.glob("**/*.py"):
        rel_f = f.relative_to(transform_dir)
        if _is_disabled(rel_f):
            Log.Warn(f"skipping: [{rel_f}]")
            continue
        if not _file_ok(f): continue
        count += 1
        lib.AddItem(rel_f, "transforms::transform")
    if count > 0:
        # PruneTypes walks IterateTransforms, which loads each .py via
        # ResolveParentLibrary — that needs _metadata on disk, so save first.
        lib.Save()
        lib.PruneTypes(save=True)
        Log.Info(f"compiled [{count}] transforms from [{transform_dir.name}]")
    return {"library": str(transform_dir), "count": count}


def Build(
    data_type_dirs: list[Path],
    transform_dirs: list[Path],
    unique_dirs: list[Path],
) -> dict:
    types = LoadTypeLibraries(data_type_dirs)

    missing = [d for d in unique_dirs if _dir_ok(d) and d.name not in types]
    if missing:
        raise KeyError(
            f"unique dirs without matching type namespaces: "
            f"{[str(d) for d in missing]}; available: {sorted(types.keys())}"
        )

    unique_results = [CompileUniqueLibrary(d, types) for d in unique_dirs]
    transform_results = [CompileTransformLibrary(d, types) for d in transform_dirs]

    return {
        "types": {ns: len(lib.types) for ns, lib in types.items()},
        "uniques": unique_results,
        "transforms": transform_results,
    }

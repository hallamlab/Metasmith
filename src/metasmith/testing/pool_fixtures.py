"""A hand-built data library, given the identities a pool assigned it.

A plan refuses a given whose identity this process minted. A driver avoids that
by importing once and referencing thereafter; a fixture that builds a library
file by file has no pool behind it at all. This imports what the fixture built
and reads the identities back, so the library a test plans against references
real pool entries rather than claiming to be one.
"""

from __future__ import annotations

from pathlib import Path


def pool_backed(lib, cache_root: Path | str | None = None):
    """Import every item in `lib` and re-identify it from the pool.

    An item the pool already holds under the same name is referenced rather
    than imported again. That is what a driver does -- import at setup, cite
    thereafter -- and it is what lets a fixture be built twice and plan to one
    key, which several cache tests are entirely about.

    The library keeps its own relative paths. A projection would key on the
    absolute path the pool records, which is right for a driver and wrong for
    a fixture whose whole shape is `sample_00/reads.fq`.
    """
    from ..ops.cache import list_cache
    from ..ops.data import import_item

    root = Path(cache_root) if cache_root is not None else (
        lib.location.parent / f"{lib.location.name}._pool"
    )
    # Newest first, so the first row for a name is the one a reference resolves
    # to -- the same rule the projection applies.
    known: dict[str, str] = {}
    for row in list_cache(str(root), origin="imported").get("entries", []):
        known.setdefault(row["name"], row["instance_id"])

    for path, dtype in list(lib.manifest.items()):
        name = str(path)
        instance_id = known.get(name)
        if instance_id is None:
            abs_path = path if path.is_absolute() else lib.location / path
            instance_id = import_item(
                str(abs_path), dtype, cache_root=str(root), name=name,
            )["instance_id"]
            known[name] = instance_id
        lib.SetLineageInstance(
            path,
            instance_id=instance_id,
            lineage_payload=None,
            origin="imported",
        )
    return lib


def reimport(lib, cache_root: Path | str | None = None):
    """Import every item again, as a second act, and take the new identities.

    How a driver says the data behind a given changed. An import is assigned
    rather than derived, so there is nothing to re-derive: the caller imports
    again and the pool records a second entry, which supersedes the first
    wherever a path is what resolves the reference.
    """
    from ..ops.data import import_item

    root = Path(cache_root) if cache_root is not None else (
        lib.location.parent / f"{lib.location.name}._pool"
    )
    for path, dtype in list(lib.manifest.items()):
        abs_path = path if path.is_absolute() else lib.location / path
        record = import_item(
            str(abs_path), dtype, cache_root=str(root), name=str(path),
        )
        lib.SetLineageInstance(
            path,
            instance_id=record["instance_id"],
            lineage_payload=None,
            origin="imported",
        )
    return lib

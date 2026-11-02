"""Promotion links a product into the cache when a task container binds its work dir and the agent home separately."""

from pathlib import Path

from metasmith.caching.fs import MountEntry, mount_view
from metasmith.caching.layout import shard_dir
from metasmith.caching.promote import promote_members

from .test_promote import KEY, META, _entry, _task_dir

LUSTRE = "0:52"
TASK_BINDS = (
    MountEntry("/", "overlay", "0:40", "/"),
    MountEntry("/ws", "lustre", LUSTRE, "/phyberos/cami/metasmith/runs/WfOlaqLT/nxf_work/ab/cdef"),
    MountEntry("/msm_home", "lustre", LUSTRE, "/phyberos/cami/metasmith"),
    MountEntry("/tmp", "ext4", "8:3", "/localscratch/job"),
)


def test_a_work_dir_inside_the_home_is_spelled_under_the_home_bind():
    view = mount_view(Path("/ws"), Path("/msm_home/task_cache"), TASK_BINDS)
    assert view == Path("/msm_home/runs/WfOlaqLT/nxf_work/ab/cdef")


def test_a_work_dir_on_another_device_has_no_view():
    assert mount_view(Path("/tmp/x"), Path("/msm_home/task_cache"), TASK_BINDS) is None


def test_a_work_dir_outside_the_home_bind_has_no_view():
    binds = TASK_BINDS[:1] + (
        MountEntry("/ws", "lustre", LUSTRE, "/phyberos/bench/e1/work/ab"),
    ) + TASK_BINDS[2:]
    assert mount_view(Path("/ws"), Path("/msm_home/task_cache"), binds) is None


def test_a_product_on_the_cache_mount_is_linked_not_copied(tmp_path):
    cwd = _task_dir(tmp_path, "t1")
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=cwd, entries=[_entry()], meta=META, cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "promoted"
    stored = shard_dir(cache_root, KEY) / "out" / "1-1-1.abcdef-step_a.txt"
    assert stored.samefile(cwd / "1-1-1.abcdef-step_a.txt")

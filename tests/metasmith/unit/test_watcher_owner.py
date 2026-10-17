import os

from metasmith.coms.via_file_watcher import _pid_namespace, _watcher_alive


def _dead_pid() -> int:
    pid = os.getpid() + 100000
    while os.path.exists(f"/proc/{pid}"):
        pid += 1
    return pid


def test_a_watcher_in_this_namespace_is_probed(tmp_path):
    (tmp_path/"active").write_text(f"{_dead_pid()} {_pid_namespace()}\n")
    assert not _watcher_alive(tmp_path)


def test_a_host_watcher_seen_from_a_container_counts_as_alive(tmp_path):
    # Inside a docker container the host watcher's pid names nothing, so probing it would
    # strand every job the step hands the relay.
    (tmp_path/"active").write_text(f"{_dead_pid()} pid:[1]\n")
    assert _watcher_alive(tmp_path)


def test_a_missing_owner_is_dead(tmp_path):
    assert not _watcher_alive(tmp_path)

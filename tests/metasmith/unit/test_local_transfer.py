from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from metasmith.models.remote import Logistics, Source, SourceType


def _transfer(src: Path, dest: Path, **kwargs):
    lg = Logistics()
    lg.QueueTransfer(Source.FromLocal(src), Source.FromLocal(dest))
    res = lg.ExecuteTransfers(**kwargs)
    assert res.errors == [], res.errors
    return res


@pytest.fixture(params=["in process", "rsync"])
def transfer(request, monkeypatch):
    if request.param == "rsync":
        monkeypatch.setattr("metasmith.models.remote._LOCAL_TREE_LIMIT", -1)
    return _transfer


def _tree(root: Path) -> dict[str, str]:
    found = {}
    for here, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            p = Path(here)/name
            rel = str(p.relative_to(root))
            if p.is_symlink():
                found[rel] = f"-> {os.readlink(p)}"
            elif p.is_dir():
                found[rel] = "dir/"
            else:
                found[rel] = p.read_text()
    return found


class TestDirectories:
    def test_a_tree_arrives_whole(self, tmp_path, transfer):
        src = tmp_path/"src"
        (src/"nested"/"deeper").mkdir(parents=True)
        (src/"top.txt").write_text("top")
        (src/"nested"/"mid.txt").write_text("mid")
        (src/"nested"/"deeper"/"leaf.txt").write_text("leaf")

        transfer(src, tmp_path/"dest")
        assert _tree(tmp_path/"dest") == _tree(src)

    def test_an_empty_directory_is_a_directory(self, tmp_path, transfer):
        src = tmp_path/"src"
        (src/"empty").mkdir(parents=True)
        transfer(src, tmp_path/"dest")
        assert (tmp_path/"dest"/"empty").is_dir()

    def test_a_symlink_is_copied_as_a_symlink(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"real.txt").write_text("real")
        (src/"link.txt").symlink_to("real.txt")

        transfer(src, tmp_path/"dest")
        link = tmp_path/"dest"/"link.txt"
        assert link.is_symlink()
        assert os.readlink(link) == "real.txt"

    def test_resolving_symlinks_follows_them(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        outside = tmp_path/"outside.txt"
        outside.write_text("outside")
        (src/"link.txt").symlink_to(outside)

        transfer(src, tmp_path/"dest", resolve_symlinks=True)
        copied = tmp_path/"dest"/"link.txt"
        assert not copied.is_symlink()
        assert copied.read_text() == "outside"

    def test_a_newer_destination_is_left_alone(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"f.txt").write_text("old")
        os.utime(src/"f.txt", (1_600_000_000, 1_600_000_000))

        dest = tmp_path/"dest"
        dest.mkdir()
        (dest/"f.txt").write_text("newer")

        transfer(src, dest)
        assert (dest/"f.txt").read_text() == "newer"

    def test_an_older_destination_is_overwritten(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"f.txt").write_text("new")

        dest = tmp_path/"dest"
        dest.mkdir()
        (dest/"f.txt").write_text("stale")
        os.utime(dest/"f.txt", (1_600_000_000, 1_600_000_000))

        transfer(src, dest)
        assert (dest/"f.txt").read_text() == "new"

    def test_extraneous_destination_files_survive(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"kept.txt").write_text("kept")

        dest = tmp_path/"dest"
        dest.mkdir()
        (dest/"theirs.txt").write_text("theirs")

        transfer(src, dest)
        assert (dest/"theirs.txt").read_text() == "theirs"
        assert (dest/"kept.txt").read_text() == "kept"

    def test_nothing_is_left_behind(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"f.txt").write_text("body")

        transfer(src, tmp_path/"dest")
        assert sorted(p.name for p in (tmp_path/"dest").iterdir()) == ["f.txt"]

    def test_an_entry_that_changed_kind_is_refused_the_same_way(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        (src/"thing").write_text("now a file")
        (src/"fine.txt").write_text("fine")

        dest = tmp_path/"dest"
        (dest/"thing").mkdir(parents=True)
        (dest/"thing"/"inside.txt").write_text("stale")

        lg = Logistics()
        lg.QueueTransfer(Source.FromLocal(src), Source.FromLocal(dest))
        res = lg.ExecuteTransfers()
        assert any("could not make way" in e for e in res.errors), res.errors
        assert (dest/"thing"/"inside.txt").read_text() == "stale"


class TestExclusions:
    def test_an_excluded_path_is_left_behind(self, tmp_path, transfer):
        src = tmp_path/"src"
        (src/"_metadata").mkdir(parents=True)
        (src/"_metadata"/"keep.txt").write_text("keep")
        (src/"_metadata"/"skip.txt").write_text("skip")
        (src/"data.txt").write_text("data")

        transfer(src, tmp_path/"dest", exclude=["/_metadata/skip.txt"])
        dest = tmp_path/"dest"
        assert (dest/"data.txt").read_text() == "data"
        assert (dest/"_metadata"/"keep.txt").read_text() == "keep"
        assert not (dest/"_metadata"/"skip.txt").exists()

    def test_a_pattern_is_anchored_at_the_transfer_root(self, tmp_path, transfer):
        src = tmp_path/"src"
        (src/"_metadata").mkdir(parents=True)
        (src/"_metadata"/"logs.latest").write_text("alias")
        (src/"deep"/"_metadata").mkdir(parents=True)
        (src/"deep"/"_metadata"/"logs.latest").write_text("someone else's")

        transfer(src, tmp_path/"dest", exclude=["/_metadata/logs.latest"])
        dest = tmp_path/"dest"
        assert not (dest/"_metadata"/"logs.latest").exists()
        assert (dest/"deep"/"_metadata"/"logs.latest").read_text() == "someone else's"

    def test_the_in_process_arm_declines_rather_than_half_copying(self, tmp_path):
        src = tmp_path/"src"
        src.mkdir()
        (src/"a.txt").write_text("a")
        (src/"b.txt").write_text("b")

        _transfer(src, tmp_path/"dest", exclude=["/b.txt"])
        assert (tmp_path/"dest"/"a.txt").read_text() == "a"
        assert not (tmp_path/"dest"/"b.txt").exists()

    def test_following_links_and_excluding_compose(self, tmp_path, transfer):
        src = tmp_path/"src"
        src.mkdir()
        outside = tmp_path/"outside"
        outside.mkdir()
        (outside/"real.txt").write_text("out there")
        (src/"followed").symlink_to(outside)
        (src/"alias").symlink_to(outside)

        transfer(src, tmp_path/"dest", resolve_symlinks=True, exclude=["/alias"])
        dest = tmp_path/"dest"
        assert (dest/"followed"/"real.txt").read_text() == "out there"
        assert not (dest/"alias").exists()


class TestFiles:
    def test_a_plain_file(self, tmp_path, transfer):
        src = tmp_path/"f.txt"
        src.write_text("body")
        transfer(src, tmp_path/"copy.txt")
        assert (tmp_path/"copy.txt").read_text() == "body"

    def test_into_a_directory_that_does_not_exist_yet(self, tmp_path, transfer):
        src = tmp_path/"f.txt"
        src.write_text("body")
        transfer(src, tmp_path/"a"/"b"/"f.txt")
        assert (tmp_path/"a"/"b"/"f.txt").read_text() == "body"


class TestFallback:
    def test_a_fifo_sends_the_tree_to_rsync(self, tmp_path):
        src = tmp_path/"src"
        src.mkdir()
        (src/"f.txt").write_text("body")
        os.mkfifo(src/"pipe")

        _transfer(src, tmp_path/"dest")
        assert (tmp_path/"dest"/"f.txt").read_text() == "body"

    def test_a_tree_over_the_limit_sends_it_to_rsync(self, tmp_path, monkeypatch):
        monkeypatch.setattr("metasmith.models.remote._LOCAL_TREE_LIMIT", 1)
        src = tmp_path/"src"
        (src/"a").mkdir(parents=True)
        (src/"b").mkdir()
        for i in range(4):
            (src/"a"/f"{i}.txt").write_text(str(i))

        _transfer(src, tmp_path/"dest")
        assert _tree(tmp_path/"dest") == _tree(src)

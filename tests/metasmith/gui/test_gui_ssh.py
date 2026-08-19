from __future__ import annotations

import stat
from pathlib import Path

import pytest

from metasmith.gui.sshconfig import (
    BEGIN_MARKER,
    END_MARKER,
    SshConfig,
    SshConfigError,
)

pytestmark = pytest.mark.gui

@pytest.fixture
def cfg_path(tmp_path):
    return tmp_path / "ssh" / "config"


@pytest.fixture
def cfg(cfg_path):
    return SshConfig(cfg_path)


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


class TestParsing:
    def test_reads_hosts_and_keywords(self, cfg, cfg_path):
        _write(cfg_path, """
Host alpha
    HostName alpha.example.org
    User tony
    Port 2222

Host beta
    HostName 10.0.0.2
    ProxyJump alpha
""")
        hosts = {h["alias"]: h for h in cfg.hosts()}
        assert hosts["alpha"]["hostname"] == "alpha.example.org"
        assert hosts["alpha"]["user"] == "tony"
        assert hosts["alpha"]["port"] == "2222"
        assert hosts["beta"]["proxy_jump"] == "alpha"

    def test_one_line_declares_several_hosts(self, cfg, cfg_path):
        _write(cfg_path, "Host a b c\n    User shared\n")
        assert {h["alias"] for h in cfg.hosts()} == {"a", "b", "c"}
        assert all(h["user"] == "shared" for h in cfg.hosts())

    def test_wildcards_are_not_destinations(self, cfg, cfg_path):
        _write(cfg_path, """
Host *
    ServerAliveInterval 30

Host *.internal
    User svc

Host real
    HostName real.example.org
""")
        assert [h["alias"] for h in cfg.hosts()] == ["real"]

    def test_key_equals_value_form(self, cfg, cfg_path):
        _write(cfg_path, "Host alpha\n    HostName=alpha.example.org\n")
        assert cfg.hosts()[0]["hostname"] == "alpha.example.org"

    def test_match_block_keywords_are_not_the_hosts(self, cfg, cfg_path):
        _write(cfg_path, """
Host alpha
    HostName alpha.example.org

Match host beta
    User someone-else
""")
        assert cfg.hosts()[0]["user"] is None

    def test_first_value_wins(self, cfg, cfg_path):
        _write(cfg_path, "Host alpha\n    User first\n    User second\n")
        assert cfg.hosts()[0]["user"] == "first"


class TestDuplicateAliases:
    def test_repeated_alias_is_one_host(self, cfg, cfg_path):
        _write(cfg_path, """
Host alpha
    HostName alpha.example.org

Host alpha
    HostName other.example.org
""")
        assert [h["alias"] for h in cfg.hosts()] == ["alpha"]

    def test_repeated_alias_merges_first_wins(self, cfg, cfg_path):
        _write(cfg_path, """
Host alpha
    HostName alpha.example.org

Host alpha
    HostName ignored.example.org
    User tony
    Port 2222
""")
        h = cfg.hosts()[0]
        assert h["hostname"] == "alpha.example.org"
        assert h["user"] == "tony"
        assert h["port"] == "2222"

    def test_overlapping_host_lines_collapse(self, cfg, cfg_path):
        _write(cfg_path, "Host a b\n    User one\n\nHost b c\n    User two\n")
        hosts = {h["alias"]: h for h in cfg.hosts()}
        assert set(hosts) == {"a", "b", "c"}
        assert hosts["b"]["user"] == "one"

    def test_duplicate_across_an_include_collapses(self, cfg, cfg_path):
        _write(cfg_path.parent / "extra", "Host alpha\n    User from-include\n")
        _write(cfg_path, "Include extra\n\nHost alpha\n    HostName a\n")
        hosts = cfg.hosts()
        assert [h["alias"] for h in hosts] == ["alpha"]
        assert hosts[0]["user"] == "from-include"
        assert hosts[0]["hostname"] == "a"

    def test_aliases_are_unique(self, cfg, cfg_path):
        _write(cfg_path, """
Host alpha beta
Host beta gamma
Host alpha
Include missing-on-purpose
""")
        aliases = [h["alias"] for h in cfg.hosts()]
        assert len(aliases) == len(set(aliases))

    def test_repeated_pattern_collapses_in_shadowing(self, cfg, cfg_path):
        _write(cfg_path, "Host *\n    User one\n\nHost *\n    Port 22\n")
        patterns = cfg.shadowing_patterns("anything")
        assert [p["alias"] for p in patterns] == ["*"]

    def test_definition_site_is_the_first_occurrence(self, cfg, cfg_path):
        _write(cfg_path, "Host alpha\n    HostName first\n\nHost alpha\n    User u\n")
        assert cfg.find("alpha").line == 1


class TestIncludes:
    def test_follows_include(self, cfg, cfg_path):
        _write(cfg_path.parent / "extra", "Host gamma\n    HostName gamma.example.org\n")
        _write(cfg_path, "Include extra\n\nHost alpha\n    HostName a\n")
        assert {h["alias"] for h in cfg.hosts()} == {"alpha", "gamma"}

    def test_include_is_relative_to_the_config_directory(self, cfg, cfg_path):
        (cfg_path.parent / "conf.d").mkdir(parents=True)
        _write(cfg_path.parent / "conf.d" / "work", "Host work\n    HostName w\n")
        _write(cfg_path, "Include conf.d/work\n")
        assert [h["alias"] for h in cfg.hosts()] == ["work"]

    def test_include_glob(self, cfg, cfg_path):
        (cfg_path.parent / "conf.d").mkdir(parents=True)
        _write(cfg_path.parent / "conf.d" / "a.conf", "Host one\n")
        _write(cfg_path.parent / "conf.d" / "b.conf", "Host two\n")
        _write(cfg_path, "Include conf.d/*.conf\n")
        assert {h["alias"] for h in cfg.hosts()} == {"one", "two"}

    def test_include_cycle_terminates(self, cfg, cfg_path):
        _write(cfg_path.parent / "other", "Include config\nHost gamma\n")
        _write(cfg_path, "Include other\nHost alpha\n")
        assert {h["alias"] for h in cfg.hosts()} == {"alpha", "gamma"}


class TestCollisions:
    def test_refuses_an_alias_already_defined(self, cfg, cfg_path):
        _write(cfg_path, "Host sockeye\n    HostName sockeye.example.org\n")
        with pytest.raises(SshConfigError) as exc:
            cfg.add_host("sockeye", "elsewhere.example.org")
        assert str(cfg_path) in str(exc.value)
        assert "already defined" in str(exc.value)

    def test_refusal_names_the_included_file(self, cfg, cfg_path):
        included = cfg_path.parent / "work.conf"
        _write(included, "Host sockeye\n    HostName s\n")
        _write(cfg_path, "Include work.conf\n")
        with pytest.raises(SshConfigError) as exc:
            cfg.add_host("sockeye", "x")
        assert str(included) in str(exc.value)

    def test_refuses_a_pattern_as_an_alias(self, cfg):
        with pytest.raises(AssertionError, match="not a host"):
            cfg.add_host("*.example.org", "x")

    def test_a_wildcard_block_does_not_block_a_new_alias(self, cfg, cfg_path):
        _write(cfg_path, "Host *\n    ServerAliveInterval 30\n")
        host = cfg.add_host("brand-new", "new.example.org")
        assert host["alias"] == "brand-new"


class TestManagedBlock:
    def test_block_is_written_first(self, cfg, cfg_path):
        _write(cfg_path, "Host *\n    User wrong-user\n\nHost old\n    HostName o\n")
        cfg.add_host("fresh", "fresh.example.org", user="right-user")

        lines = [ln for ln in cfg.read().splitlines() if ln.strip()]
        assert lines[0] == BEGIN_MARKER
        assert lines.index("Host fresh") < lines.index("Host *")

    def test_existing_content_is_preserved(self, cfg, cfg_path):
        _write(cfg_path, "Host old\n    HostName old.example.org\n    User someone\n")
        cfg.add_host("fresh", "fresh.example.org")
        text = cfg.read()
        assert "Host old" in text
        assert "User someone" in text
        assert {h["alias"] for h in cfg.hosts()} == {"old", "fresh"}

    def test_managed_hosts_are_marked(self, cfg, cfg_path):
        _write(cfg_path, "Host old\n    HostName o\n")
        cfg.add_host("fresh", "f")
        by_alias = {h["alias"]: h for h in cfg.hosts()}
        assert by_alias["fresh"]["managed"] is True
        assert by_alias["old"]["managed"] is False

    def test_adding_a_second_host_keeps_the_first(self, cfg):
        cfg.add_host("one", "one.example.org", user="a")
        cfg.add_host("two", "two.example.org", proxy_jump="one")
        by_alias = {h["alias"]: h for h in cfg.hosts()}
        assert by_alias["one"]["user"] == "a"
        assert by_alias["two"]["proxy_jump"] == "one"
        assert cfg.read().count(BEGIN_MARKER) == 1
        assert cfg.read().count(END_MARKER) == 1

    def test_split_exposes_the_three_regions(self, cfg, cfg_path):
        _write(cfg_path, "Host old\n    HostName o\n")
        cfg.add_host("fresh", "f")
        before, managed, after = cfg.split()
        assert before.strip() == ""
        assert "Host fresh" in managed
        assert "Host old" in after

    def test_round_tripping_the_editor_does_not_stack_preambles(self, cfg):
        cfg.add_host("one", "one.example.org")
        for _ in range(3):
            _before, body, _after = cfg.split()
            cfg.write_managed_block(body)
        assert cfg.read().count("# Managed by metasmith.") == 1
        assert [h["alias"] for h in cfg.hosts()] == ["one"]

    def test_editing_the_block_directly(self, cfg):
        cfg.add_host("one", "one.example.org")
        cfg.write_managed_block("Host hand-written\n    HostName manual.example.org\n")
        assert [h["alias"] for h in cfg.hosts()] == ["hand-written"]

    def test_file_is_private(self, cfg, cfg_path):
        cfg.add_host("one", "one.example.org")
        mode = stat.S_IMODE(cfg_path.stat().st_mode)
        assert mode == 0o600


class TestOwnership:
    def test_update_only_what_we_own(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        with pytest.raises(SshConfigError, match="outside the"):
            cfg.update_host("theirs", hostname="hijacked")

    def test_remove_only_what_we_own(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        with pytest.raises(SshConfigError, match="not metasmith's to remove"):
            cfg.remove_host("theirs")

    def test_update_a_managed_host(self, cfg):
        cfg.add_host("one", "one.example.org", user="a")
        updated = cfg.update_host("one", user="b", port="2222")
        assert updated["user"] == "b"
        assert updated["port"] == "2222"
        assert updated["hostname"] == "one.example.org"

    def test_clearing_a_field(self, cfg):
        cfg.add_host("one", "one.example.org", user="a")
        assert cfg.update_host("one", user="")["user"] is None

    def test_remove_a_managed_host(self, cfg):
        cfg.add_host("one", "one.example.org")
        cfg.add_host("two", "two.example.org")
        cfg.remove_host("one")
        assert [h["alias"] for h in cfg.hosts()] == ["two"]

    def test_unknown_host(self, cfg):
        with pytest.raises(SshConfigError, match="no host named"):
            cfg.remove_host("nope")


class TestRenamingAHost:
    def test_rename_keeps_the_keywords(self, cfg):
        cfg.add_host("one", "one.example.org", user="a", port="2222")
        renamed = cfg.update_host("one", alias="uno")
        assert renamed["alias"] == "uno"
        assert renamed["user"] == "a"
        assert renamed["port"] == "2222"
        assert [h["alias"] for h in cfg.hosts()] == ["uno"]

    def test_rename_and_edit_in_one_write(self, cfg):
        cfg.add_host("one", "one.example.org")
        out = cfg.update_host("one", alias="uno", hostname="uno.example.org")
        assert (out["alias"], out["hostname"]) == ("uno", "uno.example.org")

    def test_rename_onto_a_taken_alias_is_refused(self, cfg):
        cfg.add_host("one", "one.example.org")
        cfg.add_host("two", "two.example.org")
        with pytest.raises(SshConfigError, match="already defined"):
            cfg.update_host("one", alias="two")
        assert [h["alias"] for h in cfg.hosts()] == ["one", "two"]

    def test_rename_onto_a_native_alias_is_refused(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        cfg.add_host("one", "one.example.org")
        with pytest.raises(SshConfigError, match="already defined"):
            cfg.update_host("one", alias="theirs")

    def test_rename_to_a_pattern_is_refused(self, cfg):
        cfg.add_host("one", "one.example.org")
        with pytest.raises(AssertionError, match="not a host"):
            cfg.update_host("one", alias="*.example.org")

    def test_same_alias_is_not_a_rename(self, cfg):
        cfg.add_host("one", "one.example.org")
        assert cfg.update_host("one", alias="one", user="b")["user"] == "b"


class TestShadowing:
    def test_reports_patterns_that_apply(self, cfg, cfg_path):
        _write(cfg_path, "Host *.example.org\n    User svc\n")
        cfg.add_host("box.example.org", "10.0.0.9")
        shadows = cfg.shadowing_patterns("box.example.org")
        assert [s["alias"] for s in shadows] == ["*.example.org"]

    def test_nothing_to_report_when_nothing_matches(self, cfg, cfg_path):
        _write(cfg_path, "Host *.internal\n    User svc\n")
        cfg.add_host("box.example.org", "10.0.0.9")
        assert cfg.shadowing_patterns("box.example.org") == []


class TestIdentityKeys:
    def test_identity_is_a_form_field(self, cfg):
        cfg.add_host("one", "one.example.org", identity_file="~/.ssh/metasmith/one")
        host = cfg.hosts()[0]
        assert host["identity_file"] == "~/.ssh/metasmith/one"
        assert "IdentityFile ~/.ssh/metasmith/one" in cfg.read()

    def test_identity_can_be_updated_and_cleared(self, cfg):
        cfg.add_host("one", "one.example.org")
        assert cfg.update_host("one", identity_file="~/.ssh/k")["identity_file"] == "~/.ssh/k"
        assert cfg.update_host("one", identity_file="")["identity_file"] is None

    def test_generate_writes_a_keypair(self, cfg):
        out = cfg.generate_identity("one")
        assert out["created"] is True
        assert out["exists"] is True
        assert Path(out["path"]).parent == cfg.key_dir
        assert Path(out["path"] + ".pub").is_file()
        assert out["public_key"].startswith("ssh-ed25519 ")

    def test_generated_key_is_private(self, cfg):
        out = cfg.generate_identity("one")
        assert stat.S_IMODE(Path(out["path"]).stat().st_mode) == 0o600

    def test_generate_never_overwrites(self, cfg):
        first = cfg.generate_identity("one")
        original = Path(first["path"]).read_bytes()
        again = cfg.generate_identity("one")
        assert again["created"] is False
        assert again["public_key"] == first["public_key"]
        assert Path(first["path"]).read_bytes() == original

    def test_generated_path_is_written_home_relative(self, cfg, monkeypatch, tmp_path):
        monkeypatch.setattr(Path, "home", classmethod(lambda _cls: tmp_path))
        out = cfg.generate_identity("one")
        assert out["value"].startswith("~/")
        assert "metasmith/one" in out["value"]

    def test_read_identity_reports_a_missing_key(self, cfg):
        info = cfg.read_identity("~/.ssh/nope-not-here")
        assert info["exists"] is False
        assert info["public_key"] is None

    def test_read_identity_of_nothing(self, cfg):
        assert cfg.read_identity(None) is None
        assert cfg.read_identity("") is None

    def test_generate_refuses_a_pattern(self, cfg):
        with pytest.raises(AssertionError):
            cfg.generate_identity("not/a/name")

    def test_delete_removes_both_halves(self, cfg):
        out = cfg.generate_identity("one")
        priv = Path(out["path"])
        cfg.delete_identity("one")
        assert not priv.exists()
        assert not priv.with_name(priv.name + ".pub").exists()

    def test_delete_is_repeatable_only_once(self, cfg):
        cfg.generate_identity("one")
        cfg.delete_identity("one")
        with pytest.raises(SshConfigError, match="no generated key"):
            cfg.delete_identity("one")

    def test_delete_never_touches_a_key_we_did_not_generate(self, cfg, cfg_path):
        theirs = cfg_path.parent / "id_ed25519"
        _write(theirs, "PRIVATE KEY")
        with pytest.raises(SshConfigError, match="no generated key"):
            cfg.delete_identity("id_ed25519")
        assert theirs.read_text() == "PRIVATE KEY"

    def test_generate_after_delete_makes_a_fresh_key(self, cfg):
        first = cfg.generate_identity("one")["public_key"]
        cfg.delete_identity("one")
        second = cfg.generate_identity("one")
        assert second["created"] is True
        assert second["public_key"] != first


class TestEditingBothHalves:
    def test_native_half_is_editable(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        cfg.add_host("ours", "o")
        cfg.write_all(cfg.split()[1], "Host theirs\n    HostName edited.example.org\n")
        by_alias = {h["alias"]: h for h in cfg.hosts()}
        assert by_alias["theirs"]["hostname"] == "edited.example.org"
        assert by_alias["ours"]["hostname"] == "o"

    def test_managed_block_stays_first(self, cfg, cfg_path):
        cfg.add_host("ours", "o")
        cfg.write_all(cfg.split()[1], "Host *\n    User wrong\n")
        lines = [ln for ln in cfg.read().splitlines() if ln.strip()]
        assert lines[0] == BEGIN_MARKER
        assert lines.index("Host ours") < lines.index("Host *")

    def test_markers_in_the_native_half_are_refused(self, cfg):
        cfg.add_host("ours", "o")
        with pytest.raises(SshConfigError, match="markers belong to"):
            cfg.write_all("", f"{BEGIN_MARKER}\nHost sneaky\n{END_MARKER}\n")

    def test_native_joins_both_sides_of_the_block(self, cfg, cfg_path):
        _write(cfg_path, f"Host above\n\n{BEGIN_MARKER}\n{END_MARKER}\n\nHost below\n")
        native = cfg.native()
        assert "Host above" in native
        assert "Host below" in native
        assert BEGIN_MARKER not in native

    def test_round_trip_is_stable(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        cfg.add_host("ours", "o")
        for _ in range(3):
            cfg.write_all(cfg.split()[1], cfg.native())
        assert cfg.read().count(BEGIN_MARKER) == 1
        assert cfg.read().count("# Managed by metasmith.") == 1
        assert {h["alias"] for h in cfg.hosts()} == {"ours", "theirs"}

    def test_writing_only_the_block_leaves_the_rest(self, cfg, cfg_path):
        _write(cfg_path, "Host theirs\n    HostName t\n")
        cfg.add_host("ours", "o")
        cfg.write_managed_block("Host renamed\n    HostName r\n")
        assert "Host theirs" in cfg.read()


class TestEmptyConfig:
    def test_no_file_yet(self, cfg, cfg_path):
        assert cfg.hosts() == []
        assert cfg.split() == ("", "", "")
        cfg.add_host("first", "first.example.org")
        assert cfg_path.is_file()
        assert [h["alias"] for h in cfg.hosts()] == ["first"]

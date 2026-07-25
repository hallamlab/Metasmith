"""The SSH config manager: what metasmith owns, and what it refuses to touch."""
from __future__ import annotations

import stat

import pytest

from metasmith.gui.sshconfig import (
    BEGIN_MARKER,
    END_MARKER,
    SshConfig,
    SshConfigError,
)


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
        """A pattern block sets defaults for other hosts; you cannot connect to it."""
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
        """A Match block is conditional; its settings do not belong to the host above."""
        _write(cfg_path, """
Host alpha
    HostName alpha.example.org

Match host beta
    User someone-else
""")
        assert cfg.hosts()[0]["user"] is None

    def test_first_value_wins(self, cfg, cfg_path):
        """ssh takes the first value it finds for a keyword; so does this."""
        _write(cfg_path, "Host alpha\n    User first\n    User second\n")
        assert cfg.hosts()[0]["user"] == "first"


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
        """A host may live in a file the main config merely pulls in."""
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
        """`Host *` matches everything; refusing on that basis would refuse everything."""
        _write(cfg_path, "Host *\n    ServerAliveInterval 30\n")
        host = cfg.add_host("brand-new", "new.example.org")
        assert host["alias"] == "brand-new"


class TestManagedBlock:
    def test_block_is_written_first(self, cfg, cfg_path):
        """Load-bearing placement.

        ssh uses the first value it finds for each keyword, so a wildcard block
        above the managed one would set User or IdentityFile for a brand-new
        alias -- an entry that parses cleanly and connects as the wrong user.
        """
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
        """split() hands back the preamble too; writing it straight back must not double it."""
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


class TestEmptyConfig:
    def test_no_file_yet(self, cfg, cfg_path):
        assert cfg.hosts() == []
        assert cfg.split() == ("", "", "")
        cfg.add_host("first", "first.example.org")
        assert cfg_path.is_file()
        assert [h["alias"] for h in cfg.hosts()] == ["first"]

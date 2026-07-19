"""Unit tests for the bwrap jail argv builder — no bwrap binary required.

The argv builder is a pure function; these tests pin the namespace flags, the
bind ordering (shared-auth on top of the sandbox), the no-net / no-userns
defaults, and the enablement decision logic.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e_agentic.harness import jail as J


# ---------------------------------------------------------------------------
# BindSpec
# ---------------------------------------------------------------------------


def test_bindspec_modes() -> None:
    assert J.BindSpec("/a", "/b", "ro").to_argv() == ["--ro-bind", "/a", "/b"]
    assert J.BindSpec("/a", "/b", "rw").to_argv() == ["--bind", "/a", "/b"]
    assert J.BindSpec("/a", "/b", "dev").to_argv() == ["--dev-bind", "/a", "/b"]
    assert J.BindSpec("/a", "/b", "ro", optional=True).to_argv() == [
        "--ro-bind-try", "/a", "/b"]


# ---------------------------------------------------------------------------
# build_bwrap_argv (pure)
# ---------------------------------------------------------------------------


def test_build_argv_namespaces_and_defaults() -> None:
    argv = J.build_bwrap_argv(["claude", "--print", "hi"],
                              binds=[J.BindSpec("/sb", "/sb", "rw")],
                              bwrap="bwrap")
    assert argv[0] == "bwrap"
    # filesystem + process jail, but NOT net, NOT user (apptainer owns userns).
    assert "--unshare-pid" in argv
    assert "--unshare-ipc" in argv
    assert "--unshare-net" not in argv
    assert "--unshare-user" not in argv
    assert "--unshare-user-try" not in argv
    # inner argv sits after the `--` terminator, verbatim + in order.
    dd = argv.index("--")
    assert argv[dd + 1:] == ["claude", "--print", "hi"]


def test_build_argv_unshare_user_optin() -> None:
    argv = J.build_bwrap_argv(["true"], binds=[], unshare_user=True)
    assert "--unshare-user-try" in argv


def test_build_argv_tmpfs_default_vs_bind() -> None:
    a = J.build_bwrap_argv(["true"], binds=[])
    assert "--tmpfs" in a and "/tmp" in a
    b = J.build_bwrap_argv(["true"], binds=[],
                           tmp_bind=J.BindSpec("/sb/tmp", "/tmp", "rw"))
    assert "--tmpfs" not in b
    i = b.index("--bind")
    assert b[i:i + 3] == ["--bind", "/sb/tmp", "/tmp"]


def test_build_argv_chdir_and_bind_order() -> None:
    binds = [J.BindSpec("/usr", "/usr", "ro"),
             J.BindSpec("/sb", "/sb", "rw"),
             J.BindSpec("/host/.claude", "/sb/home/.claude", "rw")]
    argv = J.build_bwrap_argv(["true"], binds=binds, chdir="/sb")
    assert "--chdir" in argv and argv[argv.index("--chdir") + 1] == "/sb"
    # binds appear in the given order (shared-auth last so it wins).
    usr = argv.index("/usr")
    sb = argv.index("/sb", usr)
    claude = argv.index("/host/.claude")
    assert usr < sb < claude


# ---------------------------------------------------------------------------
# default_binds — layout → bind set
# ---------------------------------------------------------------------------


def test_default_binds_sandbox_rw_and_system_ro(tmp_path: Path) -> None:
    sb = tmp_path / "sb"
    home = sb / "home"
    binds = J.default_binds(sb, home, share_claude_auth=False)
    modes = {(b.src, b.mode) for b in binds}
    assert ("/usr", "ro") in modes
    assert (str(sb), "rw") in modes
    # sandbox rw bind must come AFTER the system ro binds (override a ro parent).
    srcs = [b.src for b in binds]
    assert srcs.index("/usr") < srcs.index(str(sb))


def test_default_binds_shares_claude_auth_on_top(tmp_path: Path, monkeypatch) -> None:
    fake_home = tmp_path / "fakehost"
    (fake_home / ".claude").mkdir(parents=True)
    (fake_home / ".claude" / ".credentials.json").write_text("{}")
    monkeypatch.setattr(J.Path, "home", classmethod(lambda cls: fake_home))

    sb = tmp_path / "sb"
    home = sb / "home"
    binds = J.default_binds(sb, home, share_claude_auth=True)
    # the shared-auth bind targets the in-jail HOME/.claude, rw, and is LAST
    # (after the sandbox-root bind) so it lands on top.
    claude_binds = [b for b in binds if b.dst == str(home / ".claude")]
    assert len(claude_binds) == 1
    cb = claude_binds[0]
    assert cb.mode == "rw"
    assert cb.src == str(fake_home / ".claude")
    assert binds.index(cb) > [b.src for b in binds].index(str(sb))


def test_default_binds_extra_ro_dedup(tmp_path: Path) -> None:
    d = tmp_path / "mats"
    d.mkdir()
    binds = J.default_binds(tmp_path / "sb", tmp_path / "sb" / "home",
                            extra_ro=[d, d], share_claude_auth=False)
    hits = [b for b in binds if b.src == str(d)]
    assert len(hits) == 1 and hits[0].mode == "ro" and hits[0].optional


# ---------------------------------------------------------------------------
# enablement
# ---------------------------------------------------------------------------


def test_jail_enabled_force_off(monkeypatch) -> None:
    monkeypatch.setenv("MSM_E2E_JAIL", "0")
    monkeypatch.setattr(J, "resolve_bwrap", lambda: "/usr/bin/bwrap")
    assert J.jail_enabled({"MSM_E2E_RUNTIME": "APPTAINER"}) is False


def test_jail_enabled_force_on(monkeypatch) -> None:
    monkeypatch.setenv("MSM_E2E_JAIL", "1")
    assert J.jail_enabled({}) is True


def test_jail_enabled_auto_apptainer(monkeypatch) -> None:
    monkeypatch.delenv("MSM_E2E_JAIL", raising=False)
    monkeypatch.setattr(J, "resolve_bwrap", lambda: "/usr/bin/bwrap")
    assert J.jail_enabled({"MSM_E2E_RUNTIME": "APPTAINER"}) is True
    assert J.jail_enabled({"MSM_E2E_RUNTIME": "DOCKER"}) is False


def test_jail_enabled_auto_no_bwrap(monkeypatch) -> None:
    monkeypatch.delenv("MSM_E2E_JAIL", raising=False)
    monkeypatch.setattr(J, "resolve_bwrap", lambda: None)
    assert J.jail_enabled({"MSM_E2E_RUNTIME": "APPTAINER"}) is False


def test_require_bwrap_raises_when_absent(monkeypatch) -> None:
    monkeypatch.setattr(J, "resolve_bwrap", lambda: None)
    with pytest.raises(RuntimeError):
        J.require_bwrap()


def test_resolve_bwrap_explicit(monkeypatch, tmp_path: Path) -> None:
    b = tmp_path / "bwrap"
    b.write_text("#!/bin/sh\n")
    monkeypatch.setenv("MSM_E2E_BWRAP", str(b))
    assert J.resolve_bwrap() == str(b)
    monkeypatch.setenv("MSM_E2E_BWRAP", str(tmp_path / "nope"))
    assert J.resolve_bwrap() is None

"""Tests for "what a pull produced is actually usable", and the record of it.

Antonio's report from Sockeye: several SIFs (metaphlan, kraken2, sylph) arrived
with a bad squashfs superblock. Every arm of the materialise chain gated on
`[ -e <sif> ]`, which a corrupt download satisfies, so the broken artifact was
trusted on the run that fetched it and on every later run on that host -- the
failure surfaced hops away, inside a tool that could not read its own rootfs.

The probe is `apptainer exec <artifact> true`: it exercises the squashfs mount,
which is the exact code path that produced those errors. A header-only
inspection (`sif list`) would have passed on his images, and `apptainer verify`
checks cryptographic signatures biocontainers do not carry.

Verifying on every task would cost a container start per task, so a success
writes a sibling stamp and the already-materialised test requires both. That
also makes the fix self-healing for artifacts that predate it: no stamp means
verify once -- adopting the artifact if it mounts, replacing it if it does not.

Nothing here pulls a real image or starts a real container. The behavioural
tests run the *emitted* shell against a stub `apptainer` on PATH that can be
told to produce a good artifact, a corrupt one, or to fail outright -- so the
thing under test is the chain's logic, which is where the defect lived.
"""

import os
import subprocess
from pathlib import Path

import pytest

from metasmith.env import ContainerDef, Environment, Rootfs, Runtime
from metasmith.models.libraries.execution import _materialised_test


IMAGE = "docker://quay.io/example/tool:1.0"


def _apptainer(cache: Path, **kw) -> Environment:
    return Environment(
        image=IMAGE, runtime=Runtime.APPTAINER,
        container=ContainerDef(cache=cache, workdir=Path("/ws")),
        **kw,
    )


def _stamp_of(artifact: Path) -> Path:
    """The sibling marker a verified artifact carries.

    Spelled out here rather than read off the Environment so the test pins the
    convention instead of agreeing with whatever the code currently does. It has
    to be a *sibling*: the sandbox artifact is a directory, and a stamp inside it
    would be swallowed by the `rm -rf` that replaces it.
    """
    return Path(f"{artifact}.verified")


# --------------------------------------------------------------------------
# the stub runtime
# --------------------------------------------------------------------------

_STUB = r"""#!/bin/bash
echo "$@" >> "$MSM_STUB_LOG"

_write() {  # $1 = one of good|corrupt|fail, $2 = dest, $3 = 1 if a directory
    case "$1" in
        fail) return 1 ;;
        good) _c=GOOD ;;
        *)    _c=CORRUPT ;;
    esac
    if [ "$3" = "1" ]; then
        rm -rf "$2"; mkdir -p "$2"; printf '%s' "$_c" > "$2/rootfs"
    else
        printf '%s' "$_c" > "$2"
    fi
    return 0
}

sub="$1"; shift
case "$sub" in
    pull)
        # apptainer pull <dest> <image>
        [ -e "$1" ] && { echo "image file already exists" >&2; exit 255; }
        _write "$MSM_PULL" "$1" 0 || exit 1
        exit 0
        ;;
    build)
        # apptainer build --force [--mksquashfs-args A] [--sandbox] <dest> <src>
        _sandbox=0
        while [ $# -gt 0 ]; do
            case "$1" in
                --force) shift ;;
                --sandbox) _sandbox=1; shift ;;
                --mksquashfs-args) shift 2 ;;
                *) break ;;
            esac
        done
        if [ "$_sandbox" = "1" ]; then
            _write "$MSM_SANDBOX" "$1" 1 || exit 1
        else
            _write "$MSM_BUILD" "$1" 0 || exit 1
        fi
        exit 0
        ;;
    exec)
        # apptainer exec [flags] <artifact> true -- mounting is what we prove
        while [ $# -gt 0 ]; do
            case "$1" in --*) shift ;; *) break ;; esac
        done
        if [ -d "$1" ]; then _c=$(cat "$1/rootfs" 2>/dev/null)
        else _c=$(cat "$1" 2>/dev/null); fi
        [ "$_c" = "GOOD" ] && exit 0
        echo "FATAL: while mounting image: squashfs: bad superblock" >&2
        exit 1
        ;;
esac
exit 1
"""


class _Host:
    """One fake execution host: a store directory and a stub apptainer."""

    def __init__(self, root: Path):
        self.store = root / "container_images"
        self.store.mkdir(parents=True)
        self.log = root / "apptainer.log"
        self.log.write_text("")
        bindir = root / "bin"
        bindir.mkdir()
        stub = bindir / "apptainer"
        stub.write_text(_STUB)
        stub.chmod(0o755)
        self._bindir = bindir

    def run(self, cmd: str, *, pull="good", build="good", sandbox="good"):
        env = {
            k: v for k, v in os.environ.items()
            # unset so the store root's ${APPTAINER_CACHEDIR:-<cache>} falls
            # through to the cache this host was built with
            if k != "APPTAINER_CACHEDIR"
        }
        env.update(
            PATH=f"{self._bindir}{os.pathsep}{os.environ['PATH']}",
            MSM_STUB_LOG=str(self.log),
            MSM_PULL=pull, MSM_BUILD=build, MSM_SANDBOX=sandbox,
        )
        return subprocess.run(
            ["bash", "-c", cmd], env=env, capture_output=True, text=True,
        )

    @property
    def calls(self) -> list[str]:
        return [ln for ln in self.log.read_text().splitlines() if ln.strip()]

    def subcommands(self, name: str) -> list[str]:
        return [c for c in self.calls if c.split(" ", 1)[0] == name]


@pytest.fixture
def host(tmp_path) -> _Host:
    return _Host(tmp_path)


def _paths(env: Environment, host: _Host) -> tuple[Path, Path, Path, Path]:
    """The four on-disk names, with the store expression already resolved.

    The emitted command carries `${APPTAINER_CACHEDIR:-<cache>}` verbatim -- it
    is expanded on the execution host, deliberately, so the fetch and the exec
    cannot disagree. The test resolves it the same way the shell will.
    """
    def _real(p: Path) -> Path:
        return host.store / p.name
    sif, sandbox = env.GetLocalPath(), env.GetSandboxPath()
    return _real(sif), _real(sandbox), _stamp_of(_real(sif)), _stamp_of(_real(sandbox))


# --------------------------------------------------------------------------
# what the emitted command says
# --------------------------------------------------------------------------

class TestVerificationIsEmitted:
    @pytest.mark.parametrize("rootfs", [Rootfs.AUTO, Rootfs.SIF, Rootfs.SANDBOX])
    def test_every_mode_verifies_the_artifact_it_names(self, rootfs, tmp_path):
        env = _apptainer(tmp_path, rootfs=rootfs)
        cmd = env.MakeMaterialiseCommand()
        wanted = env.GetSandboxPath() if rootfs is Rootfs.SANDBOX else env.GetLocalPath()
        assert "apptainer exec" in cmd and f"{wanted} true" in cmd, (
            f"[{rootfs.value}] materialises {wanted} without ever mounting it"
        )

    def test_forced_modes_do_not_verify_the_other_artifact(self, tmp_path):
        sif_env = _apptainer(tmp_path, rootfs=Rootfs.SIF)
        assert f"{sif_env.GetSandboxPath()} true" not in sif_env.MakeMaterialiseCommand()
        box_env = _apptainer(tmp_path, rootfs=Rootfs.SANDBOX)
        assert f"{box_env.GetLocalPath()} true" not in box_env.MakeMaterialiseCommand()

    def test_force_clears_the_stamp_with_the_artifact(self, tmp_path):
        # Otherwise an assertive deploy re-pulls into a stale "verified" claim,
        # which is worse than the state it was clearing.
        env = _apptainer(tmp_path)
        cmd = env.MakeMaterialiseCommand(force=True)
        assert "rm -rf " in cmd
        clearing = cmd.split("rm -rf ", 1)[1].split(";", 1)[0]
        for artifact in (env.GetLocalPath(), env.GetSandboxPath()):
            assert str(_stamp_of(artifact)) in clearing, (
                f"force does not clear the stamp beside {artifact}"
            )

    def test_docker_arm_is_untouched(self, tmp_path):
        env = Environment(
            image=IMAGE, runtime=Runtime.DOCKER,
            container=ContainerDef(cache=tmp_path),
        )
        cmd = env.MakeMaterialiseCommand()
        # Docker keeps its own image store and its own integrity guarantees;
        # there is no artifact of ours to mount.
        assert "apptainer" not in cmd
        assert ".verified" not in cmd


class TestMaterialisedTestRequiresStamp:
    """The per-task reuse gate, consulted before every container invocation."""

    def test_auto_requires_a_stamp_beside_either_artifact(self, tmp_path):
        env = _apptainer(tmp_path)
        test = _materialised_test(env)
        assert str(_stamp_of(env.GetLocalPath())) in test
        assert str(_stamp_of(env.GetSandboxPath())) in test

    def test_forced_sif_requires_the_sif_stamp_only(self, tmp_path):
        env = _apptainer(tmp_path, rootfs=Rootfs.SIF)
        test = _materialised_test(env)
        assert str(_stamp_of(env.GetLocalPath())) in test
        assert ".sandbox" not in test

    def test_forced_sandbox_requires_the_sandbox_stamp_only(self, tmp_path):
        env = _apptainer(tmp_path, rootfs=Rootfs.SANDBOX)
        test = _materialised_test(env)
        assert str(_stamp_of(env.GetSandboxPath())) in test
        assert ".sif" not in test


# --------------------------------------------------------------------------
# what the emitted command does
# --------------------------------------------------------------------------

class TestChainBehaviour:
    def test_corrupt_pull_falls_through_to_the_build_rung(self, host, tmp_path):
        """Antonio's case: the pull succeeds and produces an unusable image."""
        env = _apptainer(host.store)
        res = host.run(env.MakeMaterialiseCommand(), pull="corrupt", build="good")
        sif, _, sif_stamp, _ = _paths(env, host)
        assert res.returncode == 0, res.stderr
        assert sif.read_text() == "GOOD", "the corrupt pull was kept"
        assert sif_stamp.is_file(), "no record that the artifact was verified"

    def test_corrupt_build_falls_through_to_the_sandbox_rung(self, host):
        env = _apptainer(host.store)
        res = host.run(
            env.MakeMaterialiseCommand(),
            pull="corrupt", build="corrupt", sandbox="good",
        )
        sif, sandbox, sif_stamp, sandbox_stamp = _paths(env, host)
        assert res.returncode == 0, res.stderr
        assert (sandbox / "rootfs").read_text() == "GOOD"
        assert sandbox_stamp.is_file()
        assert not sif.exists(), "a SIF proven unmountable was left on disk"
        assert not sif_stamp.exists()

    def test_every_rung_corrupt_fails_loudly(self, host):
        env = _apptainer(host.store)
        res = host.run(
            env.MakeMaterialiseCommand(),
            pull="corrupt", build="corrupt", sandbox="corrupt",
        )
        _, _, sif_stamp, sandbox_stamp = _paths(env, host)
        assert res.returncode != 0, (
            "the chain reported success with no usable artifact anywhere"
        )
        assert not sif_stamp.exists() and not sandbox_stamp.exists()

    def test_a_good_unstamped_artifact_is_adopted_without_refetching(self, host):
        """An image that predates this fix must not cost a re-download.

        `pull` is wired to fail outright, so any attempt to re-fetch shows up as
        a non-zero exit rather than as a silent success.
        """
        env = _apptainer(host.store)
        sif, _, sif_stamp, _ = _paths(env, host)
        sif.write_text("GOOD")
        res = host.run(env.MakeMaterialiseCommand(), pull="fail", build="fail")
        assert res.returncode == 0, res.stderr
        assert sif_stamp.is_file()
        assert host.subcommands("pull") == [], "re-fetched an artifact that was fine"

    def test_a_corrupt_unstamped_artifact_is_replaced(self, host):
        """The self-healing half: the store already holds one of Antonio's SIFs."""
        env = _apptainer(host.store)
        sif, _, sif_stamp, _ = _paths(env, host)
        sif.write_text("CORRUPT")
        res = host.run(env.MakeMaterialiseCommand(), pull="good")
        assert res.returncode == 0, res.stderr
        assert sif.read_text() == "GOOD"
        assert sif_stamp.is_file()

    def test_verification_costs_one_container_start_per_host(self, host):
        """Not one per task -- which is what the stamp buys.

        A workflow of N tasks over one image consults the materialised test N
        times and materialises once; the second invocation here stands for every
        task after the first.
        """
        env = _apptainer(host.store)
        assert host.run(env.MakeMaterialiseCommand()).returncode == 0
        first = len(host.subcommands("exec"))
        assert first == 1, f"expected exactly one mount test, got {first}"
        res = host.run(env.MakeMaterialiseCommand())
        assert res.returncode == 0, res.stderr
        assert len(host.subcommands("exec")) == 1, "re-verified an already-stamped artifact"

    def test_force_rematerialises_a_stamped_artifact(self, host):
        env = _apptainer(host.store)
        assert host.run(env.MakeMaterialiseCommand()).returncode == 0
        res = host.run(env.MakeMaterialiseCommand(force=True))
        sif, _, sif_stamp, _ = _paths(env, host)
        assert res.returncode == 0, res.stderr
        assert len(host.subcommands("pull")) == 2, "force did not re-fetch"
        assert sif.read_text() == "GOOD" and sif_stamp.is_file()


class TestForcedModeBehaviour:
    def test_forced_sif_never_reaches_for_a_sandbox(self, host):
        env = _apptainer(host.store, rootfs=Rootfs.SIF)
        res = host.run(
            env.MakeMaterialiseCommand(),
            pull="corrupt", build="corrupt", sandbox="good",
        )
        _, sandbox, _, _ = _paths(env, host)
        assert res.returncode != 0, "a mode that ruled out the sandbox still succeeded"
        assert not sandbox.exists()

    def test_forced_sandbox_verifies_the_directory(self, host):
        env = _apptainer(host.store, rootfs=Rootfs.SANDBOX)
        res = host.run(env.MakeMaterialiseCommand(), sandbox="corrupt")
        _, sandbox, _, sandbox_stamp = _paths(env, host)
        assert res.returncode != 0
        assert not sandbox_stamp.exists()
        assert not sandbox.exists(), "an unmountable sandbox was left on disk"

    def test_forced_sandbox_adopts_a_good_unstamped_tree(self, host):
        env = _apptainer(host.store, rootfs=Rootfs.SANDBOX)
        _, sandbox, _, sandbox_stamp = _paths(env, host)
        (sandbox / "rootfs").parent.mkdir(parents=True)
        (sandbox / "rootfs").write_text("GOOD")
        res = host.run(env.MakeMaterialiseCommand(), sandbox="fail")
        assert res.returncode == 0, res.stderr
        assert sandbox_stamp.is_file()
        assert host.subcommands("build") == []

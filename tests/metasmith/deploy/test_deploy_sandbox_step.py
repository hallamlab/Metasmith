"""Pin Agent.Deploy()'s rootfs-delivery step.

Deploy materialises exactly one artifact, trying the cheap form first and
falling back only on real failure:

  pull sif -> build sif (-no-fragments) -> unpack sandbox

Nothing inspects the host first. A declared `rootfs` mode short-circuits the
chain: `sif` drops the unpack rung, `sandbox` goes straight to the registry.

These assert on the *emitted command* rather than on the source text: the
decision moved between functions once already, and a source-pattern test
pins the spelling instead of the behaviour.
"""
from pathlib import Path

from metasmith.env.environment import Environment, ContainerDef, Rootfs
from metasmith.models.libraries.execution import Runtime

IMAGE = "docker://quay.io/example/tool:1.0"
STORE = "${APPTAINER_CACHEDIR:-/cache}"
SIF = f"{STORE}/docker..quay.io_example_tool..1.0.sif"
SANDBOX = f"{STORE}/docker..quay.io_example_tool..1.0.sandbox"


def _cmd(*, rootfs: Rootfs = Rootfs.AUTO, **kw) -> str:
    env = Environment(image=IMAGE, runtime=Runtime.APPTAINER, rootfs=rootfs,
                      container=ContainerDef(cache=Path("/cache")))
    return env.ProvisionSteps(agent_home=Path("/agent"), **kw)[0][0]


def test_nothing_inspects_the_host():
    """The version/setuid probe is gone, not merely unused.

    It predicted wrong in both directions -- unpacking on hosts that run a
    real workflow on SIFs alone, and unable to foresee the one failure that
    actually matters (an mksquashfs that segfaults on large images).
    """
    cmd = _cmd()
    for probe in ("apptainer --version", "starter-suid", "/proc/version", "VERDICT"):
        assert probe not in cmd
    assert cmd.index("mkdir -p") < cmd.index("apptainer pull")


def test_sif_is_tried_first_then_the_mksquashfs_workaround_then_the_sandbox():
    cmd = _cmd()
    pull = cmd.index(f"apptainer pull {SIF} {IMAGE}")
    workaround = cmd.index('--mksquashfs-args "-no-fragments"')
    unpack = cmd.index(f"--sandbox {SANDBOX} {IMAGE}", workaround)
    assert pull < workaround < unpack, "fallback chain is out of order"
    # each arm clears its own partial output: a half-written SIF still
    # satisfies the `[ -e ]` that the run command checks
    assert cmd.count(f"rm -f {SIF}") == 2


def test_auto_leaves_an_existing_sandbox_alone():
    """A sandbox under `auto` is not a leftover to tidy away.

    It is the record that a pull *and* a build already failed on this host,
    so rebuilding would re-run an mksquashfs known to segfault here. Either
    artifact therefore counts as materialised.
    """
    cmd = _cmd()
    assert f"[ ! -e {SIF} ] && [ ! -d {SANDBOX} ]" in cmd
    assert f"rm -rf {SANDBOX}" not in cmd


def test_sandbox_mode_never_packs_a_squashfs():
    """The point of the sandbox arm is that mksquashfs is never invoked --
    that is what makes it usable on a host whose mksquashfs segfaults."""
    cmd = _cmd(rootfs=Rootfs.SANDBOX)
    assert f"[ -d {SANDBOX} ] || apptainer build --force --sandbox {SANDBOX} {IMAGE}" in cmd
    assert "pull" not in cmd and "mksquashfs" not in cmd


def test_sif_mode_refuses_to_unpack_and_drops_a_stale_sandbox():
    """`rootfs=sif` rules the directory rootfs out, so failing to build the
    SIF must fail rather than silently produce the artifact that was ruled
    out -- and the 2.4 GB tree nothing will now read goes."""
    cmd = _cmd(rootfs=Rootfs.SIF)
    assert f"rm -rf {SANDBOX}" in cmd
    assert cmd.index(f"rm -rf {SANDBOX}") < cmd.index("apptainer pull")
    assert "--sandbox" not in cmd
    assert '--mksquashfs-args "-no-fragments"' in cmd


def test_assertive_clears_both_artifacts():
    """`assertive=True` is the only way to refresh a corrupted artifact short
    of rm -rf by hand, so it must invalidate the sandbox AND the sif."""
    for mode in Rootfs:
        cmd = _cmd(rootfs=mode, assertive=True)
        assert f"rm -rf {SANDBOX} {SIF}" in cmd
        assert cmd.index("rm -rf") < cmd.index("apptainer build")
        assert f"rm -rf {SANDBOX} {SIF}" not in _cmd(rootfs=mode)

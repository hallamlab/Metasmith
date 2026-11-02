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
    assert cmd.count(f"rm -f {SIF}") == 2


def test_auto_leaves_an_existing_sandbox_alone():
    cmd = _cmd()
    short_circuit = f"{{ [ -d {SANDBOX} ] && [ -e {SANDBOX}.verified ]; }}"
    assert short_circuit in cmd
    assert cmd.index(short_circuit) < cmd.index(f"rm -rf {SANDBOX}")


def test_sandbox_mode_never_packs_a_squashfs():
    cmd = _cmd(rootfs=Rootfs.SANDBOX)
    build = f"apptainer build --force --sandbox {SANDBOX} {IMAGE}"
    assert build in cmd
    assert cmd.index(f"[ -d {SANDBOX} ]") < cmd.index(build)
    assert "pull" not in cmd and "mksquashfs" not in cmd
    assert SIF not in cmd


def test_sif_mode_refuses_to_unpack_and_drops_a_stale_sandbox():
    cmd = _cmd(rootfs=Rootfs.SIF)
    assert f"rm -rf {SANDBOX}" in cmd
    assert cmd.index(f"rm -rf {SANDBOX}") < cmd.index("apptainer pull")
    assert "--sandbox" not in cmd
    assert '--mksquashfs-args "-no-fragments"' in cmd


def test_assertive_clears_both_artifacts():
    for mode in Rootfs:
        cmd = _cmd(rootfs=mode, assertive=True)
        assert f"rm -rf {SANDBOX} {SIF}" in cmd
        assert cmd.index("rm -rf") < cmd.index("apptainer build")
        assert f"rm -rf {SANDBOX} {SIF}" not in _cmd(rootfs=mode)

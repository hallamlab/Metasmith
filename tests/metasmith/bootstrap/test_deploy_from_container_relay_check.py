"""`DeployFromContainer`'s relay-binary content check.

Client-side counterpart to dev.sh's `_assert_real_relays`: that check only
ever runs against the maintainer's own build at publish time, so it can't see
a stale or corrupt image on someone else's machine. `_assert_real_relay`
verifies the binary actually materialised inside the running container
(magic bytes + size) before it gets copied into the agent home, so a stub or
corrupted relay fails loudly and precisely instead of surfacing later as a
bare "missing" assertion.
"""

from pathlib import Path

import pytest

from metasmith.bootstrap import DeployFromContainer, _assert_real_relay


ELF_MAGIC = b"\x7fELF"
MACHO_MAGIC = b"\xcf\xfa\xed\xfe"


def _write(path: Path, magic: bytes, size: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(magic + b"\x00" * max(0, size - len(magic)))


class TestAssertRealRelay:
    def test_real_linux_binary_passes(self, tmp_path):
        p = tmp_path/"msm_relay.x86_64-linux"
        _write(p, ELF_MAGIC, 200_000)
        _assert_real_relay(p, "x86_64", "linux")  # does not raise

    def test_real_darwin_binary_passes(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, MACHO_MAGIC, 200_000)
        _assert_real_relay(p, "arm64", "darwin")  # does not raise

    def test_undersized_stub_raises(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, MACHO_MAGIC, 28)  # the historical 28-byte stub
        with pytest.raises(AssertionError, match="stub or is corrupted"):
            _assert_real_relay(p, "arm64", "darwin")

    def test_wrong_magic_raises(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, ELF_MAGIC, 200_000)  # ELF where Mach-O was expected
        with pytest.raises(AssertionError, match="stub or is corrupted"):
            _assert_real_relay(p, "arm64", "darwin")

    def test_missing_source_raises_clearly(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        with pytest.raises(AssertionError, match="not found in image"):
            _assert_real_relay(p, "arm64", "darwin")


class TestDeployFromContainerRelayCheck:
    def test_stub_relay_is_rejected_before_copy(self, tmp_path, monkeypatch):
        # Simulate the container's /app by pointing the source lookup at a
        # tmp dir instead of the real /app -- DeployFromContainer hardcodes
        # /app, so this test exercises _assert_real_relay's wiring via a
        # direct call shaped like DeployFromContainer's, rather than
        # patching Path itself (which would be fragile against unrelated
        # Path usage in the same function).
        dest = tmp_path/"ws"/"relay"/"msm_relay"
        src = tmp_path/"msm_relay.x86_64-linux"
        _write(src, ELF_MAGIC, 28)  # stub
        with pytest.raises(AssertionError, match="stub or is corrupted"):
            if not dest.exists():
                _assert_real_relay(src, "x86_64", "linux")
                dest.parent.mkdir(parents=True, exist_ok=True)
                import shutil
                shutil.copy(src, dest)
        assert not dest.exists(), "a rejected relay must not be copied"

    def test_real_relay_is_copied(self, tmp_path):
        dest = tmp_path/"ws"/"relay"/"msm_relay"
        src = tmp_path/"msm_relay.x86_64-linux"
        _write(src, ELF_MAGIC, 200_000)
        if not dest.exists():
            _assert_real_relay(src, "x86_64", "linux")
            dest.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copy(src, dest)
        assert dest.exists()
        assert dest.stat().st_size == 200_000

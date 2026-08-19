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
        _assert_real_relay(p, "x86_64", "linux")

    def test_real_darwin_binary_passes(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, MACHO_MAGIC, 200_000)
        _assert_real_relay(p, "arm64", "darwin")

    def test_undersized_stub_raises(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, MACHO_MAGIC, 28)
        with pytest.raises(AssertionError, match="stub or is corrupted"):
            _assert_real_relay(p, "arm64", "darwin")

    def test_wrong_magic_raises(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        _write(p, ELF_MAGIC, 200_000)
        with pytest.raises(AssertionError, match="stub or is corrupted"):
            _assert_real_relay(p, "arm64", "darwin")

    def test_missing_source_raises_clearly(self, tmp_path):
        p = tmp_path/"msm_relay.arm64-darwin"
        with pytest.raises(AssertionError, match="not found in image"):
            _assert_real_relay(p, "arm64", "darwin")


class TestDeployFromContainerRelayCheck:
    def test_stub_relay_is_rejected_before_copy(self, tmp_path, monkeypatch):
        dest = tmp_path/"ws"/"relay"/"msm_relay"
        src = tmp_path/"msm_relay.x86_64-linux"
        _write(src, ELF_MAGIC, 28)
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

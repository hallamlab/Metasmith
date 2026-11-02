import pytest

from metasmith.ops.build import (
    vendor_library,
    check_vendor_library,
    _library_content_hash,
    _parse_vendor_srcs,
)

MANIFEST = "manifest:\n  a.py:\n    type: transforms::transform\n"


def _tree(root, index_body: str | None):
    types = root / "data_types"
    types.mkdir(parents=True)
    (types / "demo.yml").write_text("demo:\n  thing: {}\n")

    lib = root / "transforms" / "demo"
    lib.mkdir(parents=True)
    (lib / "a.py").write_text("# a transform\n")
    if index_body is not None:
        meta = lib / "_metadata"
        (meta / "types").mkdir(parents=True)
        (meta / "types" / "demo.yml").write_text("demo:\n  thing: {}\n")
        (meta / "index.yml").write_text(index_body)

    return [f"data_types={types}", f"transforms={root / 'transforms'}"]


def test_vendoring_an_uncompiled_tree_is_refused(tmp_path):
    srcs = _tree(tmp_path / "src", index_body=None)
    with pytest.raises(ValueError, match="no _metadata/ at all"):
        vendor_library(srcs, str(tmp_path / "dst"))


def test_vendoring_an_empty_manifest_is_refused(tmp_path):
    srcs = _tree(tmp_path / "src", index_body="manifest: {}\n")
    with pytest.raises(ValueError, match="index.yml lists nothing"):
        vendor_library(srcs, str(tmp_path / "dst"))


def test_a_compiled_tree_vendors_and_checks_clean(tmp_path):
    srcs = _tree(tmp_path / "src", index_body=MANIFEST)
    dst = str(tmp_path / "dst")
    stamped = vendor_library(srcs, dst)
    assert stamped["content_hash"]
    assert check_vendor_library(srcs, dst)["ok"]


def test_check_refuses_a_stamped_but_hollow_bundle(tmp_path):
    src = tmp_path / "src"
    srcs = _tree(src, index_body="manifest: {}\n")
    dst = tmp_path / "dst"

    (dst / "transforms" / "demo" / "_metadata").mkdir(parents=True)
    (dst / "transforms" / "demo" / "_metadata" / "index.yml").write_text("manifest: {}\n")
    (dst / "VENDOR_HASH").write_text(_library_content_hash(_parse_vendor_srcs(srcs)))

    with pytest.raises(ValueError, match="unusable metadata"):
        check_vendor_library(srcs, str(dst))

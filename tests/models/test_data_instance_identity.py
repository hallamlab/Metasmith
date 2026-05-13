from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


def test_data_instance_id_is_stable_across_lineage_retyping(tmp_path):
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "assembly.fa").write_text(">c1\nACGT\n")
    lib.AddItem(path=Path("assembly.fa"), dtype="mock::assembly")

    inst = lib.Get(Path("assembly.fa"))
    lineage_ep = Endpoint(properties=inst.dtype.properties, parents={inst.dtype})
    remapped = inst.WithDType(lineage_ep)

    assert remapped.instance_id == inst.instance_id
    assert remapped.legacy_key != inst.legacy_key
    assert remapped.dtype.key != inst.dtype.key


def test_data_instance_pack_unpack_preserves_instance_id(tmp_path):
    lib = DataInstanceLibrary(tmp_path / "samples_pack.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "assembly.fa").write_text(">c1\nACGT\n")
    lib.AddItem(path=Path("assembly.fa"), dtype="mock::assembly")

    inst = lib.Get(Path("assembly.fa"))
    packed = inst.Pack()
    unpacked = inst.Unpack(packed, {lib.GetKey(): lib})

    assert unpacked.instance_id == inst.instance_id
    assert unpacked._key == inst.instance_id

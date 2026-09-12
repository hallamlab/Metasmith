from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ...hashing import KeyGenerator
from ...logging import Log
from ._atomic import write_yaml_atomic
from ..remote import Logistics, Source, SourceType
from ..solver import Endpoint


def yaml_safe_load(p: Path):
    MAX = 5
    for i in range(MAX):
        try:
            with open(p) as f:
                s = f.read()
            d = yaml.safe_load(s)
        except OSError as e:
            Log.Warn(f"{i+1} of {MAX}, error reading yaml [{p}]: {e}")
            time.sleep(1)
            continue
        if d is not None: return d
        Log.Warn(f"{i+1} of {MAX}, failed to load yaml [{p}]")
        time.sleep(1)
    assert False, f"failed to load yaml [{p}]"

@dataclass
class DataTypeOntology:
    name: str
    version: str
    doi: str
    strict: bool

    def Pack(self):
        d = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"): continue
            d[k] = v
        return d

    @classmethod
    def Unpack(cls, d: dict):
        return cls(**d)

class DataTypeOntologies:
    EDAM = DataTypeOntology(
        name = "EDAM",
        version = "1.25",
        doi = "https://doi.org/10.1093/bioinformatics/btt113",
        strict = False,
    )


@dataclass
class DataTypeLibrary:
    schema: str = "v1"
    ontology: DataTypeOntology = field(default_factory=lambda: DataTypeOntologies.EDAM)
    types: dict[str, Endpoint] = field(default_factory=dict)


    def __getitem__(self, key: str) -> Endpoint:
        return self.types[key]

    def __setitem__(self, key: str, value: Endpoint):
        assert isinstance(value, Endpoint)
        assert isinstance(key, str)
        self.types[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.types

    def __iter__(self):
        for k, v in self.types.items():
            yield k, v

    def __len__(self) -> int:
        return len(self.types)

    @classmethod
    def Unpack(cls, d: dict):
        raw_types = {}
        def pluralize(vv):
            def _fix(_v):
                if isinstance(_v, set): return _v
                if isinstance(_v, list): return set(_v)
                return {_v}
            return {k:_fix(v) for k, v in vv.items()}
        for type_name, type_raw in d["types"].items():
            extends = type_raw.get("extends", [])
            if isinstance(extends, str): extends = [extends]
            assert Endpoint.PROPERTY_FIELD in type_raw, f"[{type_name}] is missing [{Endpoint.PROPERTY_FIELD}]"
            props = type_raw[Endpoint.PROPERTY_FIELD]
            if isinstance(props, list) or isinstance(props, set):
                props = set(props)
                for pk in extends:
                    props |= raw_types[pk][Endpoint.PROPERTY_FIELD]
                raw_types[type_name] = props
            else:
                props = {}
                todo = [
                    raw_types[pk][Endpoint.PROPERTY_FIELD]
                    for pk in extends
                ]+[
                    pluralize(type_raw[Endpoint.PROPERTY_FIELD])
                ]
                for vk, vv in [entry for _props in todo for entry in _props.items()]:
                    props[vk] = props.get(type_name, set())|set(vv)
                props = {k:list(v) for k, v in props.items()}
            raw_types[type_name] = {Endpoint.PROPERTY_FIELD:props}
        params: dict = dict(
            types={k: Endpoint.Unpack(v) for k, v in raw_types.items()},
        )
        if "schema" in d:
            params["schema"] = str(d["schema"])
        if "ontology" in d:
            params["ontology"] = DataTypeOntology.Unpack(d["ontology"])
        return cls(**params)

    @classmethod
    def Load(cls, path: Source|str|Path) -> DataTypeLibrary:
        def _load(path: Path):
            return cls.Unpack(yaml_safe_load(path))

        if isinstance(path, Source):
            src = path
            if src.type not in {SourceType.DIRECT, SourceType.SYMLINK}:
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir = Path(tmpdir)
                    mover = Logistics()
                    _, salt = KeyGenerator.FromStr(src.address)
                    dest = Source.FromLocal(tmpdir/f"metasmith_datatypes.{salt}")
                    mover.QueueTransfer(src, dest)
                    res = mover.ExecuteTransfers()
                    assert len(res.completed) == 1, f"failed to load datatype library from [{src.address}], [{res.errors}]"
                    return _load(dest.GetPath())
            else:
                return _load(src.GetPath())
        else:
            return _load(Path(path))

    def Pack(self):
        return dict(
            schema=self.schema,
            ontology=self.ontology.Pack(),
            types={k: v.Pack() for k, v in self.types.items()},
        )

    def Save(self, path: Path):
        # os.replace, not a truncating write: it is atomic for a concurrent
        # reader, and it breaks a hardlink instead of writing through it -- a
        # staged image links its metadata from the library it images.
        write_yaml_atomic(Path(path), self.Pack())

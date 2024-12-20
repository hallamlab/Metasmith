from __future__ import annotations
import inspect
from json.decoder import JSONDecodeError
from typing import Callable, TypeVar, Union, Any, get_type_hints
import json

Primitive = type(Union[str, int, float, bool, list['Primitive'], dict[str, 'Primitive']])

T = TypeVar('T')
TypeDesc = Union[str, tuple[str, list['TypeDesc']]]
class _SerializableTypes:
    # all need to be strict uppercase for _members to see
    # constructor, type
    STR = str, str
    BOOL = lambda x: _SerializableTypes._parseBool(x), bool
    INT = int, int
    FLOAT = lambda x: _SerializableTypes._parseFloat(x), float
    PRIMITIVE = lambda x: _SerializableTypes._parseCustomType(Primitive, x, False), 'Primitive'
    TYPE = lambda x: _SerializableTypes._parseTypeAsString(x), type

    DefinedModels: dict[str, type[SerializableModel]] = {}

    @classmethod
    def _parseFloat(cls, x) -> float:
        if isinstance(x, float):
            return x
        f = float(x)
        if f.is_integer():
            raise ValueError
        else: 
            return f

    @classmethod
    def _parseBool(cls, x) -> bool:
        if isinstance(x, bool):
            return x
        elif isinstance(x, str):
            s = x.lower()
            if s == 'false':
                return False
            elif s == 'true':
                return True
        raise ValueError

    @classmethod
    def _parseCustomType(cls, customType: type, val, default: T) -> T:
        type = cls.ExtractTypes(str(customType))
        success, value = cls.Parse(type, val)
        if success:
            return value
        else:
            return default # default value

    @classmethod
    def _parseTypeAsString(cls, x) -> str:
        x = str(x)
        if x.startswith('<class'):
            return cls._typeStr(x)
        else:
            raise ValueError

    @classmethod
    def _typeStr(cls, t) -> str:
        s = str(t).lower()
        if s.startswith('<class'):
            return s[8:-2]
        if s.startswith("'") and s.endswith("'"):
            return s[1:-1]
        return s 

    __typesMap = {} # this may not be necessary if children to overwrite this
    @classmethod
    def _members(cls) -> list[dict[str, Callable[[str], Any]]]:
        def get(c: type):
            allMap = _SerializableTypes.__typesMap
            m = allMap.get(c, None)
            if m == None:
                d = {}
                for name, items in [i for i in filter(lambda i: i[0].isupper(), c.__dict__.items())]:
                    # d[str(type(default))[8:-2]] = makeSafeConstr(constr, default)
                    constr, theType = items
                    d[cls._typeStr(theType)] = constr
                allMap[c] = d
                m = d
            return m
        all = []
        # print(inspect.getmro(cls))
        for c in inspect.getmro(cls):
            all.append(get(c))
            if c == _SerializableTypes: break
        # print([[d.keys()] for d in all])
        return all

    @classmethod
    def _registerModel(cls, child):
        if str(child) == "<class 'limes_common.models.Model'>" or (inspect.isclass(child) and issubclass(child, SerializableModel)):
            name = cls._typeStr(child)
            if name not in cls.DefinedModels:
                cls.DefinedModels[name] = child

    @classmethod
    def ConstrFromStr(cls, string: str) -> Callable | None:
        string = cls._typeStr(string)
        for d in cls._members():
            # print([d.keys()])
            if string in d:
                return d[string]
        return None

    @classmethod
    def ConstrFromType(cls, type: type) -> Callable | None:
        string = str(type)
        return cls.ConstrFromStr(string)

    @classmethod
    def ExtractTypes(cls, hint:str, start:int = 0) -> TypeDesc:
        def extractTypes(start:int = 0) -> tuple[list[TypeDesc], int]:
            if hint.startswith('<class'):
                return [hint[8:-2]], start
            else:
                types = []
                token = ''
                def reset():
                    nonlocal token
                    token = ''

                i = start-1
                while i < len(hint) - 1:
                    i+=1
                    c = hint[i]
                    if c == ',':
                        types.append(token)
                        reset()
                        continue
                    elif c == '[':
                        subs, i = extractTypes(i+1)
                        types.append((token, subs))
                        reset()
                        continue
                    elif c == ']':
                        types.append(token)
                        return types, i+1
                    if c != ' ': token += c

                if len(token) > 0:
                    types.append(token)
                return types, i
        t, _ = extractTypes()
        return t[0]

    @classmethod
    def Parse(cls, type: TypeDesc, value, silent=False) -> tuple[bool, Any]:
        if value is None: return False, None

        if isinstance(type, tuple):
            base, subs = type
        else:
            base, subs = type, []

        if base == 'dict' and isinstance(value, dict):
            if len(subs) == 2:
                keyType, valType = subs
                d = {}
                success = True
                for k, v in value.items():
                    s1, key = cls.Parse(keyType, k)
                    s2, d[key] = cls.Parse(valType, v)
                    success = s1 and s2 and success
                return success, d
            else:
                return True, value

        if base == 'list' and isinstance(value, list):
            if len(subs) == 1:
                itemType = subs[0]
                l = []
                success = True
                for i in value:
                    s, v = cls.Parse(itemType, i)
                    l.append(v)
                    success = success and s
                return success, l
            else:
                return True, value

        if base == 'typing.Union' or base == 'typing._UnionGenericAlias':
            order = ['dict', 'tuple', 'list', 'bool', 'float', 'int', 'str']
            sorted: list[Union[None, TypeDesc]] = [None] * len(order)
            others = []
            success = False
            for t in subs:
                try:
                    i = order.index(str(t))
                    sorted[i] = t
                except ValueError:
                    others.append(t)
            for t in others + sorted:
                if t is None: continue
                s, parsed = cls.Parse(t, value, silent=True)
                if s: return True, parsed

        baseLower = base.lower()
        if base.lower() in cls.DefinedModels:
            theModel = cls.DefinedModels[baseLower]
            if isinstance(value, dict) or isinstance(value, bytes) or isinstance(value, str):
                return True, theModel.Parse(value)

        constr = cls.ConstrFromStr(base)
        if constr is not None:
            try:
                if base == 'type': print(base, value)
                return True, constr(value)
            except (TypeError, ValueError):
                pass

        if not silent:
            if len(subs) > 0:
                subPrint = []
                for s in subs:
                    if isinstance(s, tuple):
                        subPrint.append(s[0])
                    else:
                        subPrint.append(s)
            else:
                subPrint = ''
            print('defaulting to [%s]\nfor unknown type <%s%s>'%(value, base, subPrint))
            print('types inheriting Model must call the super-constructor if implimenting __init__')
            print('custom types must use a custom <SerializableTypes>')
        return False, value


class AutoRegisterChildren(type):
    def __new__(cls, name, bases, classdict):
        new_cls = type.__new__(cls, name, bases, classdict)
        _SerializableTypes._registerModel(new_cls)
        return new_cls

class SerializableModel(metaclass=AutoRegisterChildren):
    GetTypesDict: Callable[..., type[_SerializableTypes]] = lambda: _SerializableTypes

    # INHERITING CLASSES MUST SET DEFAULTS TO ALL CONSTRUCTOR PARAMATERS
    def __init__(self) -> None:
        self._jsonLoadSuccess = False
        self._raw = ''

    @classmethod
    def _getTypes(cls, typesDict: type[_SerializableTypes]) -> dict[str, TypeDesc]:
        hints = get_type_hints(cls)
        d = {}
        for k, hint in hints.items():
            hint = str(hint)
            type = typesDict.ExtractTypes(hint)
            d[k] = type
        return d

    @classmethod
    def Parse(cls, raw: bytes | str | list[str] | dict, base: SerializableModel=None):
        try:
            model = cls()
        except TypeError:
            raise TypeError('%s inheriting <Model> must have no requred paramaters in constructor' % str(cls))

        try:
            if isinstance(raw, dict):
                d = raw
            else:
                if isinstance(raw, list): raw = "\n".join(raw)
                d = json.loads(raw)
                model._jsonLoadSuccess = True
        except JSONDecodeError:
            model._raw = raw
            return model

        typesDict = cls.GetTypesDict()
        hints = cls._getTypes(typesDict)
        # print([hints.keys()])
        for k, t in hints.items():
            if k.startswith('_') or k == 'GetTypesDict': continue
            _, val = typesDict.Parse(t, d.get(k, None))
            if val is None and base is not None:
                val = base.__dict__.get(k, None)
            model.__setattr__(k, val)
        return model

    def _toDict(self, v):
        if isinstance(v, SerializableModel):
            return v.ToDict()
        elif isinstance(v, list):
            l = []
            for i in v:
                l.append(self._toDict(i))
            return l
        elif isinstance(v, dict):
            d = {}
            for k, vv in v.items():
                d[k] = self._toDict(vv)
            return d
        elif isinstance(v, type):
            return str(v)
        else:
            return v

    def ToDict(self) -> dict:
        d = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'): continue
            d[k] = self._toDict(v)
        return d
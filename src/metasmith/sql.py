from __future__ import annotations
import sqlite3
import atexit
import json
from enum import Enum
from pathlib import Path
from typing import Iterable, Iterator, Literal, Any

def jdumps(obj: Any):
    return json.dumps(obj, separators=(',', ':'))

jloads = json.loads

def dictAppend(d:dict, k, v, unique=False):
    arr = d.get(k, list())
    arr.append(v)
    if unique: arr = list(set(arr))
    d[k] = arr

def toLetters(i: int):
    base=26
    off = 97
    c = lambda x: chr(x+off-1) if x != 0 else 'z'
    if i > base-1:
        next = (i//base)*base
        rem = i - next
        print(next, rem)
        return f'{toLetters(next//base)}{c(rem)}'
    else:
        return f'{c(i)}'

class SecondaryIndex:
    def __init__(self, table: str, key: str, target: str, key_in_index: str='') -> None:
        """specifying a unique @key_in_index will make tracing much faster"""
        if key_in_index == '': key_in_index = key
        self.key_in_secondary_index = key_in_index
        self.table_name = table
        self.original_key = key
        self.target_name = target

class TraceStep:
    def __init__(self, forward: bool, index: SecondaryIndex) -> None:
        self.forward = forward
        self.secondary_index = index
        self.index_name = index.original_key
        self.table_name = index.table_name
        self.tuple = (forward, index.key_in_secondary_index, self.table_name)

class TraceResult:
    def __init__(self, result: Iterable, steps: list[TraceStep], sql: str) -> None:
        self.results = result
        self.steps = steps
        self.explanation = []
        for s in steps:
            a = s.secondary_index.table_name
            b = s.secondary_index.target_name
            self.explanation.append((f"{a} -> {b}" if s.forward else f"{b} -> {a}", f"via {s.table_name}.{s.index_name}"))
        self.sql = sql

    def __iter__(self):
        return self.results.__iter__()
    
    def ToMaps(self) -> tuple[dict, dict]:
        map, r_map = {}, {}
        for t in self:
            ka, kb = t[0], t[-1]
            map[ka] = map.get(ka, [])+[kb]
            r_map[kb] = r_map.get(kb, [])+[ka]
        return map, r_map

class _dat_statics:
    fwd = {}
    rev = {}
    from_table = {}

class Dat(Enum):
    def __init__(self, val: str) -> None:
        super().__init__()
        self.file = val
        self.table_name = val.replace('.', '_').replace('-', '_')
        self.secondary_indexes: list[SecondaryIndex] = list()
        _dat_statics.from_table[self.table_name] = self

    @classmethod
    def GetSILinks(cls):
        return dict(_dat_statics.fwd), dict(_dat_statics.rev)

    @classmethod
    def FromTableName(cls, table_name: str) -> Dat:
        return _dat_statics.from_table.get(table_name, None)

    def SetSIs(self, secondary_indexes: list[tuple]=list()):
        def makesi(t):
            k, targ = t[:2]
            si = SecondaryIndex(self.table_name, k, str(targ))
            if len(t) == 3: si.key_in_secondary_index = t[2] # added alt_key
            return si

        self.secondary_indexes = list()
        for tup in secondary_indexes:
            si = makesi(tup)
            dictAppend(_dat_statics.fwd, self.table_name, si.target_name, unique=True)
            dictAppend(_dat_statics.rev, si.target_name, self.table_name, unique=True)
            self.secondary_indexes.append(si)

    def GetSecondaryIndexes(self):
        return 

    def __str__(self) -> str:
        return self.table_name

class Traceable(Enum):
    def __init__(self, val) -> None:
        super().__init__()
        if isinstance(val, Dat):
            self.alt_value = val
        else:
            self.alt_value = None

    def __str__(self):
        return self.alt_value.__str__() if self.alt_value is not None else self.name


STAR: Literal['*'] = '*'
class DType(Enum):
    TEXT = 'TEXT'
    INT = 'INTEGER'
    REAL = 'REAL'

class Field:
    def __init__(self, name: str, is_pk: bool=False, dtype: DType=DType.TEXT, not_null:bool=True) -> None:
        self.name = name
        self.is_pk = is_pk
        self.dtype = dtype
        self.not_null = not_null
        self.sql = f'{self.name} {dtype.value} NOT NULL'

class Table:
    def __init__(self, db, name, fields, type='') -> None:
        """use Database.MakeTable() instead of initializer"""
        assert any([f.is_pk for f in fields]), "primary key not specified"
        self.name: str = name
        self.fields: dict[str, Field] = dict((f.name, f) for f in fields)
        self._db = db
        self._cur = db._cur
        self.type: str = type
        self.is_new: bool = False

        # foreign_keys = [f for f in self.fields if isinstance(f, ForeignKey)]
        self.sql = f'''
        CREATE TABLE {self.name} (
            {','.join([f.sql for f in self.fields.values()])},
            PRIMARY KEY ({','.join([f.name for f in self.fields.values() if f.is_pk])})
        )'''

    def _exists(self) -> bool:
        sql = f"SELECT name FROM sqlite_schema WHERE name='{self.name}' AND type='table'"
        return len(list(self._cur.execute(sql))) > 0

    def Select(self, columns: str|list[str]|Literal['*'], unique:bool=False, where='') -> Iterator[tuple]:
        if isinstance(columns, str): columns = [columns]
        _columns = STAR if columns == STAR else ','.join([f for f in columns])
        sql = f"SELECT {'DISTINCT' if unique else ''} {_columns} FROM {self.name} {'WHERE' if where != '' else ''} {where}"
        return self._cur.execute(sql)

    def _insert_helper(self, fn, values):
        fn(F'''INSERT INTO {self.name} 
            ({','.join([f.name for f in self.fields.values()])})
            VALUES ({','.join(['?' for f in self.fields.values()])})
            ''', values)

    def _insert(self, values:tuple):
        assert len(values) == len(self.fields), f'expected {len(self.fields)} columns, got {len(values)}'
        self._insert_helper(self._cur.execute, values)

    def _insert_many(self, values:list[tuple]):
        self._insert_helper(self._cur.executemany, values)

class RegistryTable(Table):
    NAME='tables'
    TYPE='registry'
    def __init__(self, db: Database) -> None:
        self.F_table_name = Field('name', is_pk=True)
        self.F_fields = Field('fields', is_pk=True)
        self.F_type = Field('type', is_pk=False)
        self.db = db
        super().__init__(db, RegistryTable.NAME, [self.F_table_name, self.F_type, self.F_fields], type=RegistryTable.TYPE)

        if not self._exists():
            self._cur.execute(self.sql)

    def _register(self, table: Table):
        fields_json = jdumps([[f.name, f.is_pk, f.dtype.value, f.not_null] for f in table.fields.values()])
        self._insert((table.name, table.type, fields_json))

    def GetFields(self, table_name: str) -> list[Field]:
        fres = list(self.Select('fields', where=f"name='{table_name}'"))
        assert len(fres) > 0, f"{table_name} is not in the registry"
        fs = jloads(fres[0][0])
        return [Field(name, is_pk, dtype, not_null) for name, is_pk, dtype, not_null in fs]

    def GetTypes(self) -> list[str]:
        types = []
        for row in self._cur.execute(F"SELECT DISTINCT {self.F_type.name} FROM {self.name}"):
            types.append(row[0])
        return types

    def _getTables(self, where: str):
        tables = []
        for name, fields, t in self.Select([self.F_table_name.name, self.F_fields.name, self.F_type.name], False, where):
            fs = [Field(name, is_pk, DType[dtype], not_null) for name, is_pk, dtype, not_null in jloads(fields)]
            tables.append(Table(self.db, name, fs, t))
        return tables

    def GetTables(self, type:str|Literal['*']=STAR) -> list[Table]:
        where = '' if type == STAR else f'{self.F_type.name}="{type}"'
        return self._getTables(where)

    def GetTable(self, table_name: str) -> Table:
        where = f'{self.F_table_name.name}="{table_name}"'
        result = self._getTables(where)
        assert len(result) > 0 , f"{table_name} doesn't exist"
        return result[0]

    def HasTable(self, table_name: str) -> bool:
        where = f'{self.F_table_name.name}="{table_name}"'
        return len(self._getTables(where)) > 0

class Database:
    DATA_KEY = 'key'
    DATA_TYPE = 'json'
    SI_NAME = 'si_name'
    SI_KEY = 'si_key'
    SI_TARGET = 'parent_key' # value should match DATA_KEY
    def __init__(self, db_path: str|Path, ext:str='db') -> None:
        db_path = str(db_path)
        toks = db_path.split('.')
        if toks[-1] != ext:
            toks.append(ext)
        db_path = '.'.join(toks)
        self.path = db_path

        self._con = sqlite3.connect(db_path)
        self._cur = self._con.cursor()

        self.registry = RegistryTable(self)

        def create_if_not_exists(name, fields, ttype):
            r = self.registry
            if r.HasTable(name):
                return r.GetTable(name)
            else:
                return self.AttachTable(name, fields, ttype)

        self._cached_tables: dict[str, dict] = {}

        self.data_table_keys = create_if_not_exists('data_keys', [
            Field('key', is_pk=True),
            Field('table_name', is_pk=True),
        ], 'data_table_keys')

        self.info = create_if_not_exists('info', [
            Field('key', is_pk=True),
            Field('val'),
        ], 'db_metadata')

        def onExit():
            self._con.close()
        atexit.register(onExit)

    def __del__(self):
        self._cur.close()
        self._con.close()
        del self.registry
        del self._cached_tables

    def GetInfo(self):
        info = {}
        for k, v in self.info.Select(STAR):
            info[k] = v
        return info

    def _addTable(self, table: Table):
        with self._con as con: # transaction
            con.execute(table.sql)
            self.registry._register(table)

    def AttachTable(self, name: str, fields: list[Field], type: str='') -> Table:
        table = Table(self, name, fields, type)
        self._addTable(table)
        return table

    def _si_of_table(self, table_name: str):
        return f'{table_name}_si'

    def ImportDataTable(self, table_name: str, data: dict, secondary_indexes:list[SecondaryIndex]=list(), silent=False):
        if not silent:
            print(f"loading [{table_name}]")
            for si in secondary_indexes:
                print(f"\t secondary index: [{si.original_key}] as [{si.target_name}]")

        table = self.AttachTable(table_name, [
            Field(self.DATA_KEY, is_pk=True),
            Field(self.DATA_TYPE)
        ], self.DATA_TYPE)
        si_table = self.AttachTable(self._si_of_table(table_name), [
            Field(self.SI_NAME, is_pk=True),
            Field(self.SI_KEY, is_pk=True),
            Field(self.SI_TARGET, is_pk=True),
        ], self.DATA_TYPE)
        # for si where reference is tuple (DBLINKS, for ex)
        def parse(v):
            return '_'.join(v) if isinstance(v, list) else str(v)

        # just using a dict because not enough useful links between tables to justify
        entries: list[tuple] = []
        json_keys = set()
        sis: list[tuple] = []
        for k, val in data.items():
            entries.append((k, jdumps(val)))
            json_keys = json_keys.union(set(val.keys()))
            for si in secondary_indexes:
                sis += [(si.key_in_secondary_index, parse(v), k) for v in val.get(si.original_key, list())]

        table._insert_many(entries)
        si_table._insert_many(list(set(sis)))
        self.data_table_keys._insert_many([(k, table_name) for k in json_keys])
        return table

    def ListEntries(self, table_name: Dat|str):
        table = self.registry.GetTable(str(table_name))
        return [i[0] for i in table.Select(self.DATA_KEY)]

    def GetKeysOfDataTable(self, table_name: Dat|str) -> set[str]:
        return set([x[0] for x in self.data_table_keys.Select(self.DATA_KEY, where=f"table_name='{table_name}'")])

    def GetSiOfDataTable(self, table_name: Dat|str) -> set[str]:
        si_name = self._si_of_table(str(table_name))
        si_table = self.registry.GetTable(si_name)
        return set(e[0] for e in list(si_table.Select(self.SI_NAME, unique=True)))

    def GetEntry(self, table_name: Dat|str, key: str):
        table = self.registry.GetTable(str(table_name))
        res = list(table.Select(self.DATA_TYPE, where=f"{self.DATA_KEY}='{key}'"))
        assert len(res) > 0, f"[{key}] not in {table_name}"
        return jloads(res[0][0])

    def GetEntriesBySI(self, table_name: Dat|str, si_key: str, si_name: str|None=None):
        si_table = self.registry.GetTable(self._si_of_table(str(table_name)))
        table = self.registry.GetTable(str(table_name))
        if STAR in si_key or "%" in si_key:
            si_key = si_key.replace(STAR, "%")
            where = f"{self.SI_KEY} LIKE '{si_key}'"
        else:
            where = f"{self.SI_KEY}='{si_key}'"
        if si_name is not None: where += f" AND {self.SI_NAME}='{si_name}'" # need check for si_name in table, omitted for performance
        entry_keys = [e[0] for e in list(si_table.Select(self.SI_TARGET, where=where))]
        res = [(key, jloads(e[0])) for key, group in [
            (key, list(table.Select(self.DATA_TYPE, where=f"{self.DATA_KEY}='{key}'"))) for key in entry_keys
        ] for e in group]
        return res

    def GetDataTable(self, table_name: Dat|str):
        table_name = str(table_name)
        if table_name in self._cached_tables:
            return self._cached_tables[table_name]

        table = self.registry.GetTable(table_name)
        assert table.type == self.DATA_TYPE, \
            f"[{table}] has type [{table.type}] is not a data table\nuse the registry instead"
        fields = table.fields.values()
        pks = [f.name for f in fields if f.is_pk]
        data = {}
        for entry in table.Select(pks+[self.DATA_TYPE]):
            j = entry[-1]
            k = '_'.join(entry[:-1])
            v = jloads(j)
            data[k] = v
        self._cached_tables[table_name] = data
        return data

    def _performTrace(self, steps: list[TraceStep], intermediates=False) -> TraceResult:
        if len(steps) == 0: return TraceResult([], steps, "")
        def makeSql():
            pk = self.SI_TARGET
            sk = self.SI_KEY
            def recurse(m, i):
                fwd, key, table = m[0]
                x = toLetters(i)
                # print(fwd, key, table, i)
                if len(m) == 1:
                    j = f"{self._si_of_table(table)} AS {x}"
                    w = f"{x}.{self.SI_NAME}='{key}'"
                    n = f"{x}.{pk if fwd else sk}, {x}.{sk if fwd else pk}"
                    return n, j, w

                f2, k2, t2 = m[1]
                y = toLetters(i+1)
                link = f"{x}.{sk if fwd else pk}={y}.{pk if f2 else sk}"
                ka = pk if fwd else sk
                kb = sk if fwd else pk # kc = ka
                if len(m) == 2:
                    names = f"{x}.{ka}, {x}.{kb}, {y}.{ka}"
                    joins = f"{self._si_of_table(table)} AS {x} INNER JOIN {self._si_of_table(t2)} AS {y} ON {link}"
                    where = f"{x}.{self.SI_NAME}='{key}' AND {y}.{self.SI_NAME}='{k2}'"
                else:
                    pnames, pjoins, pwhere = recurse(m[1:], i+1)
                    names = f"{x}.{ka}, {pnames}"
                    joins = f"{self._si_of_table(table)} AS {x} INNER JOIN ({pjoins}) ON {link}"
                    where = f"{x}.{self.SI_NAME}='{key}'"
                    where += f" AND {pwhere}"
                return names, joins, where

            n, j, w = recurse([ts.tuple for ts in steps], 1)
            if intermediates:
                return f"SELECT DISTINCT {n} FROM ({j}) WHERE {w}"
                # return f"SELECT DISTINCT {n} FROM ({j})"
            else:
                ka = pk if steps[0].forward else sk
                kb = pk if not steps[-1].forward else sk
                return f"SELECT DISTINCT {toLetters(1)}.{ka}, {toLetters(len(steps))}.{kb} FROM ({j}) WHERE {w}"
                # return f"SELECT DISTINCT {toLetters(1)}.{ka}, {toLetters(len(steps))}.{kb} FROM ({j})"
        
        # use_table_names = len(set([ts.index_name for ts in steps])) != len(steps) # if indexes are not sufficiently unique
        sql = makeSql()
        # print('x')
        # # return sql
        # return self._cur.execute(sql)
        # results: list[tuple[str, str]] = list(self._cur.execute(sql))
        return TraceResult(self._cur.execute(sql), steps, sql)

    def _calc_trace(self, source: Traceable, target: Traceable):
        links, rev_links = Dat.GetSILinks()

        t_str = str(target)
        def search(curr, path, dirs):
            if curr == t_str: return path+[curr], dirs
            if curr in path: return None

            nexts:list[tuple[str, bool]] = [(l, True) for l in links.get(curr, [])]
            nexts += [(l, False) for l in rev_links.get(curr, [])]
            for n, fwd in nexts:
                res = search(n, path+[curr], dirs+[fwd])
                if res is not None: return res
            return None
        res = search(str(source), [], [])
        
        assert res is not None, f"no conversion found between [{source}] and [{target}]"
        path, dirs = res
        trace = []
        for i, (p, d) in enumerate(zip(path, dirs)):
            dat = Dat.FromTableName(p if d else path[i+1])
            # gets the matching set of p, p+1 in dat.si
            si = dict((str(sorted((s.table_name, s.target_name))), s) for s in dat.secondary_indexes)
            k = str(sorted((p, path[i+1])))
            if k not in si:
                print(k)
                print(si)
            si = si[k]
            trace.append(TraceStep(d, si))

        return trace

    def Trace(self, source: Traceable, target: Traceable, intermediates=False):
        trace = self._calc_trace(source, target)
        return self._performTrace(trace, intermediates)

    def Commit(self):
        self._con.commit()

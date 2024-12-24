from __future__ import annotations
from typing import Any, List
from common import ColumnType

class TableColumnDefinition:
    def __init__(self,
                 name: str, type: ColumnType,
                 primary_key: bool = False,
                 reference: TableColumnDefinition = None,
                 unique: bool = False,
                 null_allowed: bool = True,
                 default: Any = None,
                 auto_increment: bool = False,
                 check: str = None):
        self._name = name
        self._type = type
        self._primary_key: bool = primary_key
        self._reference: TableColumnDefinition = reference
        self._unique: bool = unique
        self._null_allowed: bool = null_allowed
        self._default: Any = default
        self._auto_increment: bool = auto_increment
        self._check: str = check

class TableColumn:
    def __init__(self):
        self._definition: TableColumnDefinition = None
        self._reference: Table

class Table:
    name: str = None
    columns: TableColumn = []

    def __init_subclass__(cls):
        cls.define_columns()

    @classmethod
    def name(cls) -> str: raise NotImplementedError(f'name not implemented in Table {type(cls).__name__}')

    @classmethod
    def define_columns(cls) -> List[TableColumn]: raise NotImplementedError(f'columns not implemented in Table {type(cls).__name__}')

    @classmethod
    def add_column(cls, column: TableColumn) -> None:
        cls._columns.append(column)
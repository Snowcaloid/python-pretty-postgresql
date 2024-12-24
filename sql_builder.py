from __future__ import annotations
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from os import getenv
from typing import List, Self, Tuple, Union, get_type_hints, override

from utils import filter_empty

SQL_ENABLE_CUSTOM_HANDLING = getenv('SQL_ENABLE_CUSTOM_HANDLING') or False
SQL_TABLE_ALIAS_NEEDS_AS = getenv('SQL_TABLE_ALIAS_NEEDS_AS') or False

def timestamp_string(timestamp: datetime):
    return timestamp.strftime("\'%Y-%m-%d %H:%M:%S\'")

def ensure_type(*classes):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            type_hints = get_type_hints(func)
            new_args = []
            for arg, (name, hint) in zip(args, type_hints.items()):
                for cls in classes:
                    if hint == cls and arg is not None and not isinstance(arg, cls):
                        arg = cls(arg)
                        break
                new_args.append(arg)
            new_kwargs = {}
            for key, value in kwargs.items():
                for cls in classes:
                    if key in type_hints and type_hints[key] == cls and value is not None and not isinstance(value, cls):
                        value = cls(value)
                new_kwargs[key] = value
            return func(self, *new_args, **new_kwargs)
        return wrapper
    return decorator

class SQLKeyword(Enum):
    ADD = 'ADD'
    ADD_CONSTRAINT = 'ADD CONSTRAINT'
    ALL = 'ALL'
    ALTER = 'ALTER'
    ALTER_COLUMN = 'ALTER COLUMN'
    ALTER_TABLE = 'ALTER TABLE'
    AND = 'AND'
    ANY = 'ANY'
    AS = 'AS'
    ASC = 'ASC'
    BACKUP_DATABASE = 'BACKUP DATABASE'
    BETWEEN = 'BETWEEN'
    CASE = 'CASE'
    CHECK = 'CHECK'
    COLUMN = 'COLUMN'
    CONSTRAINT = 'CONSTRAINT'
    CREATE = 'CREATE'
    CREATE_DATABASE = 'CREATE DATABASE'
    CREATE_INDEX = 'CREATE INDEX'
    CREATE_OR_REPLACE_VIEW = 'CREATE OR REPLACE VIEW'
    CREATE_TABLE = 'CREATE TABLE'
    CREATE_PROCEDURE = 'CREATE PROCEDURE'
    CREATE_UNIQUE_INDEX = 'CREATE UNIQUE INDEX'
    CREATE_VIEW = 'CREATE VIEW'
    DATABASE = 'DATABASE'
    DEFAULT = 'DEFAULT'
    DELETE = 'DELETE'
    DESC = 'DESC'
    DISTINCT = 'DISTINCT'
    DROP = 'DROP'
    DROP_COLUMN = 'DROP COLUMN'
    DROP_CONSTRAINT = 'DROP CONSTRAINT'
    DROP_DATABASE = 'DROP DATABASE'
    DROP_DEFAULT = 'DROP DEFAULT'
    DROP_INDEX = 'DROP INDEX'
    DROP_TABLE = 'DROP TABLE'
    DROP_VIEW = 'DROP VIEW'
    EXEC = 'EXEC'
    EXISTS = 'EXISTS'
    FOREIGN_KEY = 'FOREIGN KEY'
    FROM = 'FROM'
    FULL_OUTER_JOIN = 'FULL OUTER JOIN'
    GROUP_BY = 'GROUP BY'
    HAVING = 'HAVING'
    IN = 'IN'
    INDEX = 'INDEX'
    INNER_JOIN = 'INNER JOIN'
    INSERT_INTO = 'INSERT INTO'
    INSERT_INTO_SELECT = 'INSERT INTO SELECT'
    IS_NULL = 'IS NULL'
    IS_NOT_NULL = 'IS NOT NULL'
    JOIN = 'JOIN'
    LEFT_JOIN = 'LEFT JOIN'
    LIKE = 'LIKE'
    LIMIT = 'LIMIT'
    NOT = 'NOT'
    NOT_NULL = 'NOT NULL'
    OR = 'OR'
    ORDER_BY = 'ORDER BY'
    OUTER_JOIN = 'OUTER JOIN'
    PRIMARY_KEY = 'PRIMARY KEY'
    PROCEDURE = 'PROCEDURE'
    RIGHT_JOIN = 'RIGHT JOIN'
    ROWNUM = 'ROWNUM'
    SELECT = 'SELECT'
    SELECT_DISTINCT = 'SELECT DISTINCT'
    SELECT_INTO = 'SELECT INTO'
    SELECT_TOP = 'SELECT TOP'
    SET = 'SET'
    TABLE = 'TABLE'
    TOP = 'TOP'
    TRUNCATE_TABLE = 'TRUNCATE TABLE'
    UNION = 'UNION'
    UNION_ALL = 'UNION ALL'
    UNIQUE = 'UNIQUE'
    UPDATE = 'UPDATE'
    VALUES = 'VALUES'
    VIEW = 'VIEW'
    WHERE = 'WHERE'

JoinKeyWords = [
    SQLKeyword.INNER_JOIN,
    SQLKeyword.OUTER_JOIN,
    SQLKeyword.LEFT_JOIN,
    SQLKeyword.RIGHT_JOIN,
    SQLKeyword.FULL_OUTER_JOIN
]

KW = SQLKeyword

class SQLExpressionType(Enum):
    EQUATION = -1
    KEYWORD = 0
    STRING = 1
    VALUE = 2
    FIELD = 3
    TABLE = 4
    ALIAS = 5
    FUNCTION = 6
    DATE = 7

class SQLExpression:
    value: str

    def __init__(self, value):
        self.value = str(value)

    @abstractmethod
    def expression_type(self) -> SQLExpressionType: pass
    def __eq__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '=', other)

    def __ne__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '!=', other)

    def __lt__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '<', other)

    def __le__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '<=', other)

    def __gt__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '>', other)

    def __ge__(self, other: SQLExpression) -> SQLEquation:
        return SQLEquation(self, '>=', other)

    def __and__(self, other: SQLExpression) -> SQLLogicalExpression:
        return SQLLogicalExpression(self, KW.AND.value, other)

    def __or__(self, other: SQLExpression) -> SQLLogicalExpression:
        return SQLLogicalExpression(self, KW.OR.value, other)

    def __invert__(self) -> SQLLogicalExpression:
        return SQLLogicalExpression(self, KW.NOT.value, self)

class SQLEquation(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.EQUATION
    def __init__(self, left: SQLExpression, operator: str, right: SQLExpression):
        self.value = f"({left.value} {operator} {right.value})"

class SQLLogicalExpression(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.EQUATION
    def __init__(self, left: SQLExpression, operator: str, right: SQLExpression = None):
        if right:
            self.value = f"({left.value} {operator} {right.value})"
        else:
            self.value = f"({operator} {left.value})"

Exp = SQLExpression

class SQLReservedWord(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.KEYWORD
    def __init__(self, kw: SQLKeyword):
        self.value = kw.value

class SQLString(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.STRING
    def __init__(self, val: str):
        self.value = f"'{val}'"

class SQLValue(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.VALUE
    def __init__(self, val: str):
        self.value = val

class SQLField(SQLValue):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.FIELD

class SQLTable(SQLValue):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.TABLE

class SQLAlias(SQLValue):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.ALIAS

class SQLFunction(SQLExpression):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.FUNCTION
    def __init__(self, name: str, parameters: List[SQLExpression] = []):
      self.value = f'{name}({", ".join([param.value for param in parameters])})'

class SQLDate(SQLString):
    def expression_type(self) -> SQLExpressionType: return SQLExpressionType.DATE
    def __init__(self, val: datetime):
        self.value = timestamp_string(val)

class SQLOn(SQLExpression):
    @ensure_type(SQLAlias, SQLField)
    def __init__(self, table_alias: SQLAlias, field: SQLField):
        self.table_alias = table_alias
        self.field = field

    def expression_type(self) -> SQLExpressionType:
        return SQLExpressionType.EQUATION

    @property
    def value(self) -> str:
        return f'{self.table_alias.value}.{self.field.value}'

AF = SQLOn # Alias Field

class SQLStatement:
    @ensure_type(SQLExpression, SQLAlias)
    def __init__(self, expression: SQLExpression, alias: SQLAlias = None):
      self.expression = expression
      self.alias = alias
      self.value = ''

    def compile(self) -> str:
        self.value = self.expression.value
        return self.value

    def value_to_sql(self, value) -> str:
        if isinstance(value, datetime):
            return timestamp_string(value)
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (int, float, bool)):
            return str(value)
        elif SQL_ENABLE_CUSTOM_HANDLING:
            return str(value)
        else:
            raise ValueError(f'SQL Value {value} of type not convertable into string')

class SQLSelectField(SQLStatement):
    @override
    def compile(self) -> str:
        self.value = self.expression.value
        if self.alias:
            self.value = f'{self.value} {KW.AS.value} {self.alias.value}'

        return self.value

SF = SQLSelectField

SQLOrderByLiteral = Union[Union[SQLField, SQLAlias], Tuple[Union[SQLField, SQLAlias], bool]]

class SQLJoin:
    def __init__(self, table: Union[str, Tuple[str,str]], on: SQLEquation, join_type: SQLKeyword = SQLKeyword.INNER_JOIN):
        if not join_type in JoinKeyWords: raise ValueError(f'{join_type.value} is not a valid join keyword.')
        self.table = table
        self.on = on
        self.join_type = join_type

    def compile(self) -> str:
        table = f'{self.table[0]} {self.table[1]}' if isinstance(self.table, tuple) else self.table
        return f'{self.join_type.value} {table} ON {self.on.value}'

class SQLSelect(SQLStatement):
    @ensure_type(SQLAlias)
    def __init__(self, alias: SQLAlias = None):
        super().__init__(expression=SQLExpression(None), alias=alias)
        self._fields: List[SQLSelectField] = []
        self._table: str = None
        self._table_alias: SQLAlias = None
        self._where: SQLExpression = None
        self._order_by: List[SQLOrderByLiteral] = []
        self._joins: List[SQLJoin] = []
        self._limit = None
        self._top_limit = None

    def fields(self, _fields: List[SQLSelectField]) -> Self:
        self._fields = self._fields + _fields
        return self

    @ensure_type(SQLAlias)
    def table(self, name: str, alias: SQLAlias = None) -> Self:
        self._table = name
        self._table_alias = alias
        return self

    @ensure_type(SQLExpression)
    def where(self, expression: SQLExpression) -> Self:
        self._where = expression
        return self

    def order_by(self, _fields: List[SQLOrderByLiteral]) -> Self:
        @ensure_type(SQLField)
        def fix_field(field: SQLField):
            return field
        @ensure_type(SQLAlias)
        def fix_alias(field: SQLAlias):
            return field
        # Transform fields into tuples if they are not already
        _fields = [field if isinstance(field, tuple) else (field, True) for field in _fields]
        # Ensure that the fields are either SQLField or SQLAlias objects
        _fields = [(fix_field(field), asc) for field, asc in _fields]
        _fields = [(fix_alias(field), asc) for field, asc in _fields]
        self._order_by = self._order_by + _fields
        return self

    @ensure_type(SQLExpression)
    def join(self, table: Union[str, Tuple[str,str]], on: SQLExpression, join_type: SQLKeyword = SQLKeyword.INNER_JOIN) -> Self:
        self._joins.append(SQLJoin(table, on, join_type))
        return self

    def limit(self, limit: int) -> Self:
        self._limit = limit
        return self

    def top(self, limit: int) -> Self:
        self._top_limit = limit
        return self

    def group_by(self, fields: List[SQLExpression]) -> Self:
        self._group_by = fields
        return self

    def compile(self) -> str:
        def order_by_compile(order_by: SQLOrderByLiteral) -> str:
            field, asc = order_by
            field = field.compile() if isinstance(field, SQLStatement) else field.value
            return f'{field} {'' if asc else KW.DESC.value}'.strip(' ')
        table_alias = f'{self._table_alias.value}' if self._table_alias else ''
        where = f'{KW.WHERE.value} {self._where.value}' if self._where else ''
        order_by = ''
        if self._order_by:
            order_by = KW.ORDER_BY.value + ' ' + ', '.join(order_by_compile((order_by, asc)) for order_by, asc in self._order_by)
        group_by = ''
        if self._group_by:
            group_by = KW.GROUP_BY.value + ' ' + ', '.join(expr.compile() if isinstance(expr, SQLStatement) else expr.value for expr in self._group_by)
        result = filter_empty([
            KW.SELECT.value,
            KW.TOP.value if self._top_limit else '',
            ', '.join(field.compile() for field in self._fields),
            KW.FROM.value,
            self._table,
            table_alias,
            ' '.join(join.compile() for join in self._joins),
            where,
            group_by,
            order_by,
            f'{KW.LIMIT.value} {self._limit}' if self._limit else ''
        ])
        result = ' '.join(result)
        if self.alias:
            result = f'({result}) {KW.AS.value + ' ' if SQL_TABLE_ALIAS_NEEDS_AS else ''}{self.alias.value}'
        return result

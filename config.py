from enum import Enum
from os import getenv

class SqlEngine(Enum):
    POSTGRESQL = 'postgresql'
    MYSQL = 'mysql'
    SQLITE = 'sqlite'

SQL_ENABLE_CUSTOM_HANDLING = getenv('SQL_ENABLE_CUSTOM_HANDLING') or False
SQL_TABLE_ALIAS_NEEDS_AS = getenv('SQL_TABLE_ALIAS_NEEDS_AS') or False
SQL_ENGINE = SqlEngine(getenv('SQL_ENGINE', 'postgresql'))


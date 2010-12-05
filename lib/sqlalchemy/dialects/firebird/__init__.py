from sqlalchemy.dialects.firebird import base, kinterbasdb

base.dialect = kinterbasdb.dialect

from sqlalchemy.dialects.firebird.base import \
    SMALLINT, BIGINT, FLOAT, FLOAT, DATE, TIME, \
    TEXT, NUMERIC, FLOAT, TIMESTAMP, VARCHAR, CHAR, BLOB,\
    dialect
    
__all__ = (
    'SMALLINT', 'BIGINT', 'FLOAT', 'FLOAT', 'DATE', 'TIME', 
    'TEXT', 'NUMERIC', 'FLOAT', 'TIMESTAMP', 'VARCHAR', 'CHAR', 'BLOB',
    'dialect'
)
    
    

from sqlalchemy.dialects.sqlite import base, pysqlite

# default dialect
base.dialect = pysqlite.dialect


from sqlalchemy.dialects.sqlite.base import \
    BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, FLOAT, INTEGER,\
    NUMERIC, SMALLINT, TEXT, TIME, TIMESTAMP, VARCHAR, dialect

__all__ = (
    'BLOB', 'BOOLEAN', 'CHAR', 'DATE', 'DATETIME', 'DECIMAL', 'FLOAT', 'INTEGER',
    'NUMERIC', 'SMALLINT', 'TEXT', 'TIME', 'TIMESTAMP', 'VARCHAR', 'dialect'
)
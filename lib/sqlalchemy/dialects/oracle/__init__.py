from sqlalchemy.dialects.oracle import base, cx_oracle, zxjdbc

base.dialect = cx_oracle.dialect

from sqlalchemy.dialects.oracle.base import \
    VARCHAR, NVARCHAR, CHAR, DATE, DATETIME, NUMBER,\
    BLOB, BFILE, CLOB, NCLOB, TIMESTAMP, RAW,\
    FLOAT, DOUBLE_PRECISION, LONG, dialect, INTERVAL,\
    VARCHAR2, NVARCHAR2


__all__ = (
'VARCHAR', 'NVARCHAR', 'CHAR', 'DATE', 'DATETIME', 'NUMBER',
'BLOB', 'BFILE', 'CLOB', 'NCLOB', 'TIMESTAMP', 'RAW',
'FLOAT', 'DOUBLE_PRECISION', 'LONG', 'dialect', 'INTERVAL',
'VARCHAR2', 'NVARCHAR2'
)

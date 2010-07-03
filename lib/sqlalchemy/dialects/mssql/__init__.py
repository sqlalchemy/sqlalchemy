from sqlalchemy.dialects.mssql import base, pyodbc, adodbapi, \
                                    pymssql, zxjdbc, mxodbc

base.dialect = pyodbc.dialect

from sqlalchemy.dialects.mssql.base import \
    INTEGER, BIGINT, SMALLINT, TINYINT, VARCHAR, NVARCHAR, CHAR, \
    NCHAR, TEXT, NTEXT, DECIMAL, NUMERIC, FLOAT, DATETIME,\
    DATETIME2, DATETIMEOFFSET, DATE, TIME, SMALLDATETIME, \
    BINARY, VARBINARY, BIT, REAL, IMAGE, TIMESTAMP,\
    MONEY, SMALLMONEY, UNIQUEIDENTIFIER, SQL_VARIANT, dialect


__all__ = (
    'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'VARCHAR', 'NVARCHAR', 'CHAR', 
    'NCHAR', 'TEXT', 'NTEXT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DATETIME',
    'DATETIME2', 'DATETIMEOFFSET', 'DATE', 'TIME', 'SMALLDATETIME', 
    'BINARY', 'VARBINARY', 'BIT', 'REAL', 'IMAGE', 'TIMESTAMP',
    'MONEY', 'SMALLMONEY', 'UNIQUEIDENTIFIER', 'SQL_VARIANT', 'dialect'
)
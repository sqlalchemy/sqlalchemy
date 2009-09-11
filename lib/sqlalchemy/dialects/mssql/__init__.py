from sqlalchemy.dialects.mssql import base, pyodbc, adodbapi, pymssql, zxjdbc

base.dialect = pyodbc.dialect

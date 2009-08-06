from sqlalchemy.dialects.mssql import base, pyodbc, adodbapi, pymssql

base.dialect = pyodbc.dialect
from sqlalchemy.dialects.mssql import base, pyodbc

base.dialect = pyodbc.dialect
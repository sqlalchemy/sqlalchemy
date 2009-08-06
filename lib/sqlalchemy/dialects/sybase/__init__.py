from sqlalchemy.dialects.sybase import base, pyodbc

# default dialect
base.dialect = pyodbc.dialect
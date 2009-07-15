from sqlalchemy.dialects.mysql import base, mysqldb, pyodbc, zxjdbc

# default dialect
base.dialect = mysqldb.dialect
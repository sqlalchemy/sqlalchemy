from sqlalchemy.dialects.mysql import base, mysqldb

# default dialect
base.dialect = mysqldb.dialect
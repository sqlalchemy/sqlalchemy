from sqlalchemy.dialects.sqlite import base, pysqlite

# default dialect
base.dialect = pysqlite.dialect
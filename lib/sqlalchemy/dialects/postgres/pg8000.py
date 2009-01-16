"""Support for the PostgreSQL database via the pg8000.

Connecting
----------

URLs are of the form `postgres+pg8000://user@password@host:port/dbname[?key=value&key=value...]`.

Unicode
-------

Unicode data which contains non-ascii characters don't seem to be supported yet.  non-ascii
schema identifiers though *are* supported, if you set the client_encoding=utf8 in the postgresql.conf 
file.

Interval
--------

Passing data from/to the Interval type is not supported as of yet.

"""

import decimal, random, re, string

from sqlalchemy import sql, schema, exc, util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgres.base import PGDialect, PGInet, PGCidr, PGMacAddr, PGArray, \
 PGBigInteger, PGInterval

class PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if self.asdecimal:
            return None
        else:
            def process(value):
                if isinstance(value, decimal.Decimal):
                    return float(value)
                else:
                    return value
            return process

class Postgres_pg8000ExecutionContext(default.DefaultExecutionContext):
    pass

class Postgres_pg8000(PGDialect):
    driver = 'pg8000'

    supports_unicode_statements = False #True
    
    # this one doesn't matter, cant pass non-ascii through
    # pending further investigation
    supports_unicode_binds = False #True
    
    default_paramstyle = 'format'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = Postgres_pg8000ExecutionContext
    
    @classmethod
    def dbapi(cls):
        return __import__('pg8000').dbapi

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e):
        return "connection is closed" in e

dialect = Postgres_pg8000

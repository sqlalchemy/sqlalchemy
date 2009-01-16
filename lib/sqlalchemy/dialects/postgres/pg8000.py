"""Support for the PostgreSQL database via the pg8000.

Connecting
----------

URLs are of the form `postgres+pg8000://user@password@host:port/dbname[?key=value&key=value...]`.

Unicode
-------

pg8000 requires that the postgres client encoding be configured in the postgresql.conf file
in order to use encodings other than ascii.  Set this value to the same value as 
the "encoding" parameter on create_engine(), usually "utf-8".

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
    
    supports_unicode_binds = True
    
    default_paramstyle = 'format'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = Postgres_pg8000ExecutionContext
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : PGNumeric,
            sqltypes.Float: sqltypes.Float,  # prevents PGNumeric from being used
        }
    )
    
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

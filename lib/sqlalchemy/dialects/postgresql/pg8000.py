"""Support for the PostgreSQL database via the pg8000 driver.

Connecting
----------

URLs are of the form `postgresql+pg8000://user@password@host:port/dbname[?key=value&key=value...]`.

Unicode
-------

pg8000 requires that the postgresql client encoding be configured in the postgresql.conf file
in order to use encodings other than ascii.  Set this value to the same value as 
the "encoding" parameter on create_engine(), usually "utf-8".

Interval
--------

Passing data from/to the Interval type is not supported as of yet.

"""
from sqlalchemy.engine import default
import decimal
from sqlalchemy import util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGDialect, PGCompiler

class _PGNumeric(sqltypes.Numeric):
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

class PostgreSQL_pg8000ExecutionContext(default.DefaultExecutionContext):
    pass

class PostgreSQL_pg8000Compiler(PGCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)
    
    
class PostgreSQL_pg8000(PGDialect):
    driver = 'pg8000'

    supports_unicode_statements = True
    
    supports_unicode_binds = True
    
    default_paramstyle = 'format'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PostgreSQL_pg8000ExecutionContext
    statement_compiler = PostgreSQL_pg8000Compiler
    
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : _PGNumeric,
            sqltypes.Float: sqltypes.Float,  # prevents _PGNumeric from being used
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
        return "connection is closed" in str(e)

dialect = PostgreSQL_pg8000

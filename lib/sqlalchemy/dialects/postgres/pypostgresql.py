"""Support for the PostgreSQL database via py-postgresql.

Connecting
----------

URLs are of the form `postgres+pypostgresql://user@password@host:port/dbname[?key=value&key=value...]`.


"""
from sqlalchemy.engine import default
import decimal
from sqlalchemy import util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgres.base import PGDialect, PGDefaultRunner

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

class Postgres_pypostgresqlExecutionContext(default.DefaultExecutionContext):
    pass

class Postgres_pypostgresqlDefaultRunner(PGDefaultRunner):
    def execute_string(self, stmt, params=None):
        return PGDefaultRunner.execute_string(self, stmt, params or ())
        
class Postgres_pypostgresql(PGDialect):
    driver = 'pypostgresql'

    supports_unicode_statements = True
    
    supports_unicode_binds = True
    description_encoding = None
    
    defaultrunner = Postgres_pypostgresqlDefaultRunner
    
    default_paramstyle = 'format'
    
    supports_sane_rowcount = False  # alas....posting a bug now
    
    supports_sane_multi_rowcount = False
    
    execution_ctx_cls = Postgres_pypostgresqlExecutionContext
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : PGNumeric,
            sqltypes.Float: sqltypes.Float,  # prevents PGNumeric from being used
        }
    )
    
    @classmethod
    def dbapi(cls):
        from postgresql.driver import dbapi20
        return dbapi20

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        else:
            opts['port'] = 5432
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e):
        return "connection is closed" in str(e)

dialect = Postgres_pypostgresql

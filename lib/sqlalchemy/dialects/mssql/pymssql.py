"""
Support for the pymssql dialect.

Going forward we will be supporting the 1.0 release of pymssql.

"""
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy import types as sqltypes


class MSDialect_pymssql(MSDialect):
    supports_sane_rowcount = False
    max_identifier_length = 30
    driver = 'pymssql'

    @classmethod
    def dbapi(cls):
        import pymssql as module
        # pymmsql doesn't have a Binary method.  we use string
        # TODO: monkeypatching here is less than ideal
        module.Binary = lambda st: str(st)
        return module

    def __init__(self, **params):
        super(MSDialect_pymssql, self).__init__(**params)
        self.use_scope_identity = True


    def create_connect_args(self, url):
        keys = url.query
        if keys.get('port'):
            # pymssql expects port as host:port, not a separate arg
            keys['host'] = ''.join([keys.get('host', ''), ':', str(keys['port'])])
            del keys['port']
        return [[], keys]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.DatabaseError) and "Error 10054" in str(e)

dialect = MSDialect_pymssql
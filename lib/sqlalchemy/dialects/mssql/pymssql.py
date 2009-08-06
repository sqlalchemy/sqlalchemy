from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy import types as sqltypes


class MSDialect_pymssql(MSDialect):
    supports_sane_rowcount = False
    max_identifier_length = 30
    driver = 'pymssql'

    @classmethod
    def import_dbapi(cls):
        import pymssql as module
        # pymmsql doesn't have a Binary method.  we use string
        # TODO: monkeypatching here is less than ideal
        module.Binary = lambda st: str(st)
        return module

    def __init__(self, **params):
        super(MSSQLDialect_pymssql, self).__init__(**params)
        self.use_scope_identity = True

        # pymssql understands only ascii
        if self.convert_unicode:
            util.warn("pymssql does not support unicode")
            self.encoding = params.get('encoding', 'ascii')


    def create_connect_args(self, url):
        if hasattr(self, 'query_timeout'):
            # ick, globals ?   we might want to move this....
            self.dbapi._mssql.set_query_timeout(self.query_timeout)

        keys = url.query
        if keys.get('port'):
            # pymssql expects port as host:port, not a separate arg
            keys['host'] = ''.join([keys.get('host', ''), ':', str(keys['port'])])
            del keys['port']
        return [[], keys]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.DatabaseError) and "Error 10054" in str(e)

    def do_begin(self, connection):
        pass

dialect = MSDialect_pymssql
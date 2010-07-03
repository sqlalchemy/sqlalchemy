from sqlalchemy.dialects.informix.base import InformixDialect
from sqlalchemy.engine import default

class InformixExecutionContext_informixdb(default.DefaultExecutionContext):
    def post_exec(self):
        if self.isinsert:
            self._lastrowid = [self.cursor.sqlerrd[1]]


class InformixDialect_informixdb(InformixDialect):
    driver = 'informixdb'
    default_paramstyle = 'qmark'
    execution_context_cls = InformixExecutionContext_informixdb

    @classmethod
    def dbapi(cls):
        return __import__('informixdb')

    def create_connect_args(self, url):
        if url.host:
            dsn = '%s@%s' % (url.database, url.host)
        else:
            dsn = url.database

        if url.username:
            opt = {'user': url.username, 'password': url.password}
        else:
            opt = {}

        return ([dsn], opt)

    def _get_server_version_info(self, connection):
        # http://informixdb.sourceforge.net/manual.html#inspecting-version-numbers
        vers = connection.dbms_version
        
        # TODO: not tested
        return tuple([int(x) for x in vers.split('.')])

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) \
                    or 'connection not open' in str(e)
        else:
            return False


dialect = InformixDialect_informixdb

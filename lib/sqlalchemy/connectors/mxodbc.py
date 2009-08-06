from sqlalchemy.connectors import Connector

class MxODBCConnector(Connector):
    driver='mxodbc'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_statements = False
    supports_unicode_binds = False

    @classmethod
    def import_dbapi(cls):
        import mxODBC as module
        return module

    def create_connect_args(self, url):
        '''Return a tuple of *args,**kwargs'''
        # FIXME: handle mx.odbc.Windows proprietary args
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        argsDict = {}
        argsDict['user'] = opts['user']
        argsDict['password'] = opts['password']
        connArgs = [[opts['dsn']], argsDict]
        return connArgs

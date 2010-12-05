"""
Provide an SQLALchemy connector for the eGenix mxODBC commercial
Python adapter for ODBC. This is not a free product, but eGenix
provides SQLAlchemy with a license for use in continuous integration
testing.

This has been tested for use with mxODBC 3.1.2 on SQL Server 2005
and 2008, using the SQL Server Native driver. However, it is
possible for this to be used on other database platforms.

For more info on mxODBC, see http://www.egenix.com/

"""

import sys
import re
import warnings
from decimal import Decimal

from sqlalchemy.connectors import Connector
from sqlalchemy import types as sqltypes
import sqlalchemy.processors as processors

class MxODBCConnector(Connector):
    driver='mxodbc'
    
    supports_sane_multi_rowcount = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    
    supports_native_decimal = True
    
    @classmethod
    def dbapi(cls):
        # this classmethod will normally be replaced by an instance
        # attribute of the same name, so this is normally only called once.
        cls._load_mx_exceptions()
        platform = sys.platform
        if platform == 'win32':
            from mx.ODBC import Windows as module
        # this can be the string "linux2", and possibly others
        elif 'linux' in platform:
            from mx.ODBC import unixODBC as module
        elif platform == 'darwin':
            from mx.ODBC import iODBC as module
        else:
            raise ImportError, "Unrecognized platform for mxODBC import"
        return module

    @classmethod
    def _load_mx_exceptions(cls):
        """ Import mxODBC exception classes into the module namespace,
        as if they had been imported normally. This is done here
        to avoid requiring all SQLAlchemy users to install mxODBC.
        """
        global InterfaceError, ProgrammingError
        from mx.ODBC import InterfaceError
        from mx.ODBC import ProgrammingError

    def on_connect(self):
        def connect(conn):
            conn.stringformat = self.dbapi.MIXED_STRINGFORMAT
            conn.datetimeformat = self.dbapi.PYDATETIME_DATETIMEFORMAT
            conn.decimalformat = self.dbapi.DECIMAL_DECIMALFORMAT
            conn.errorhandler = self._error_handler()
        return connect
    
    def _error_handler(self):
        """ Return a handler that adjusts mxODBC's raised Warnings to
        emit Python standard warnings.
        """
        from mx.ODBC.Error import Warning as MxOdbcWarning
        def error_handler(connection, cursor, errorclass, errorvalue):

            if issubclass(errorclass, MxOdbcWarning):
                errorclass.__bases__ = (Warning,)
                warnings.warn(message=str(errorvalue),
                          category=errorclass,
                          stacklevel=2)
            else:
                raise errorclass, errorvalue
        return error_handler

    def create_connect_args(self, url):
        """ Return a tuple of *args,**kwargs for creating a connection.

        The mxODBC 3.x connection constructor looks like this:

            connect(dsn, user='', password='',
                    clear_auto_commit=1, errorhandler=None)

        This method translates the values in the provided uri
        into args and kwargs needed to instantiate an mxODBC Connection.

        The arg 'errorhandler' is not used by SQLAlchemy and will
        not be populated.
        
        """
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        args = opts.pop('host')
        opts.pop('port', None)
        opts.pop('database', None)
        return (args,), opts

    def is_disconnect(self, e):
        # eGenix recommends checking connection.closed here,
        # but how can we get a handle on the current connection?
        if isinstance(e, self.dbapi.ProgrammingError):
            return "connection already closed" in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False

    def _get_server_version_info(self, connection):
        # eGenix suggests using conn.dbms_version instead 
        # of what we're doing here
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        # 18 == pyodbc.SQL_DBMS_VER
        for n in r.split(dbapi_con.getinfo(18)[1]):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    def do_execute(self, cursor, statement, parameters, context=None):
        if context:
            native_odbc_execute = context.execution_options.\
                                        get('native_odbc_execute', 'auto')
            if native_odbc_execute is True:
                # user specified native_odbc_execute=True
                cursor.execute(statement, parameters)
            elif native_odbc_execute is False:
                # user specified native_odbc_execute=False
                cursor.executedirect(statement, parameters)
            elif context.is_crud:
                # statement is UPDATE, DELETE, INSERT
                cursor.execute(statement, parameters)
            else:
                # all other statements
                cursor.executedirect(statement, parameters)
        else:
            cursor.executedirect(statement, parameters)

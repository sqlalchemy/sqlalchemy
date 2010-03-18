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

    def on_connect(self):
        def connect(conn):
            conn.stringformat = self.dbapi.MIXED_STRINGFORMAT
            conn.datetimeformat = self.dbapi.PYDATETIME_DATETIMEFORMAT
            conn.decimalformat = self.dbapi.DECIMAL_DECIMALFORMAT
            conn.errorhandler = self._error_handler()
        return connect
    
    def _error_handler(self):
        """Return a handler that adjusts mxODBC's raised Warnings to
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
        args = opts['host'],
        kwargs = {'user':opts['user'],
                  'password': opts['password']}
        return args, kwargs

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


class MxNumeric(sqltypes.Numeric):
    """
    Handle Numeric types between SQLAlchemy and mxODBC.
    """
    def bind_processor(self, dialect):
        """
        SQLAlchemy can accept a Python Decimal for bind
        variables, so no special bind_processor is needed.
        """
        return None

    def result_processor(self, dialect, coltype):
        """
        By default, the SQLAlchemy mxODBC connector is
        configured to return Decimal values from 
        Numeric columns. In addition, the SQLAlchemy's
        Numeric type is expected to return a Python Numeric,
        so by default no action is needed.
        
        However, if the user specifies asdecimal=False
        on a Decimal column, it is expected to return
        a Python float.
        """
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class MxFloat(sqltypes.Float):
    """
    Handle Numeric types between SQLAlchemy and mxODBC.
    """
    def bind_processor(self, dialect):
        """
        SQLAlchemy can accept a Python Decimal for bind
        variables, so no special bind_processor is needed.
        """
        return None

    def result_processor(self, dialect, coltype):
        """
        mxODBC returns Python float values for REAL, FLOAT, and
        DOUBLE column types, so if the user specifies 'asdecimal',
        SQLAlchemy needs to coerce the value to a Decimal type.
        Otherwise, no special action is needed.
        """
        if self.asdecimal:
            return processors.to_decimal_processor_factory(Decimal)
        else:
            return None

        

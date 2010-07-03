"""
Support for MS-SQL via pyodbc.

pyodbc is available at:

    http://pypi.python.org/pypi/pyodbc/

Connecting
^^^^^^^^^^

Examples of pyodbc connection string URLs:

* ``mssql+pyodbc://mydsn`` - connects using the specified DSN named ``mydsn``.
  The connection string that is created will appear like::

    dsn=mydsn;Trusted_Connection=Yes

* ``mssql+pyodbc://user:pass@mydsn`` - connects using the DSN named
  ``mydsn`` passing in the ``UID`` and ``PWD`` information. The
  connection string that is created will appear like::

    dsn=mydsn;UID=user;PWD=pass

* ``mssql+pyodbc://user:pass@mydsn/?LANGUAGE=us_english`` - connects
  using the DSN named ``mydsn`` passing in the ``UID`` and ``PWD``
  information, plus the additional connection configuration option
  ``LANGUAGE``. The connection string that is created will appear
  like::

    dsn=mydsn;UID=user;PWD=pass;LANGUAGE=us_english

* ``mssql+pyodbc://user:pass@host/db`` - connects using a connection string
  dynamically created that would appear like::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass

* ``mssql+pyodbc://user:pass@host:123/db`` - connects using a connection
  string that is dynamically created, which also includes the port
  information using the comma syntax. If your connection string
  requires the port information to be passed as a ``port`` keyword
  see the next example. This will create the following connection
  string::

    DRIVER={SQL Server};Server=host,123;Database=db;UID=user;PWD=pass

* ``mssql+pyodbc://user:pass@host/db?port=123`` - connects using a connection
  string that is dynamically created that includes the port
  information as a separate ``port`` keyword. This will create the
  following connection string::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass;port=123

If you require a connection string that is outside the options
presented above, use the ``odbc_connect`` keyword to pass in a
urlencoded connection string. What gets passed in will be urldecoded
and passed directly.

For example::

    mssql+pyodbc:///?odbc_connect=dsn%3Dmydsn%3BDatabase%3Ddb

would create the following connection string::

    dsn=mydsn;Database=db

Encoding your connection string can be easily accomplished through
the python shell. For example::

    >>> import urllib
    >>> urllib.quote_plus('dsn=mydsn;Database=db')
    'dsn%3Dmydsn%3BDatabase%3Ddb'


"""

from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy import types as sqltypes, util
import decimal

class _MSNumeric_pyodbc(sqltypes.Numeric):
    """Turns Decimals with adjusted() < 0 or > 7 into strings.
    
    This is the only method that is proven to work with Pyodbc+MSSQL
    without crashing (floats can be used but seem to cause sporadic
    crashes).
    
    """

    def bind_processor(self, dialect):
        super_process = super(_MSNumeric_pyodbc, self).bind_processor(dialect)

        def process(value):
            if self.asdecimal and \
                    isinstance(value, decimal.Decimal):
                
                adjusted = value.adjusted()
                if adjusted < 0:
                    return self._small_dec_to_string(value)
                elif adjusted > 7:
                    return self._large_dec_to_string(value)

            if super_process:
                return super_process(value)
            else:
                return value
        return process
    
    def _small_dec_to_string(self, value):
        return "%s0.%s%s" % (
                    (value < 0 and '-' or ''),
                    '0' * (abs(value.adjusted()) - 1),
                    "".join([str(nint) for nint in value._int]))

    def _large_dec_to_string(self, value):
        if 'E' in str(value):
            result = "%s%s%s" % (
                    (value < 0 and '-' or ''),
                    "".join([str(s) for s in value._int]),
                    "0" * (value.adjusted() - (len(value._int)-1)))
        else:
            if (len(value._int) - 1) > value.adjusted():
                result = "%s%s.%s" % (
                (value < 0 and '-' or ''),
                "".join(
                    [str(s) for s in value._int][0:value.adjusted() + 1]),
                "".join(
                    [str(s) for s in value._int][value.adjusted() + 1:]))
            else:
                result = "%s%s" % (
                (value < 0 and '-' or ''),
                "".join(
                    [str(s) for s in value._int][0:value.adjusted() + 1]))
        return result
    
    
class MSExecutionContext_pyodbc(MSExecutionContext):
    _embedded_scope_identity = False
    
    def pre_exec(self):
        """where appropriate, issue "select scope_identity()" in the same
        statement.
        
        Background on why "scope_identity()" is preferable to "@@identity":
        http://msdn.microsoft.com/en-us/library/ms190315.aspx
        
        Background on why we attempt to embed "scope_identity()" into the same
        statement as the INSERT:
        http://code.google.com/p/pyodbc/wiki/FAQs#How_do_I_retrieve_autogenerated/identity_values?
        
        """
        
        super(MSExecutionContext_pyodbc, self).pre_exec()

        # don't embed the scope_identity select into an 
        # "INSERT .. DEFAULT VALUES"
        if self._select_lastrowid and \
                self.dialect.use_scope_identity and \
                len(self.parameters[0]):
            self._embedded_scope_identity = True
            
            self.statement += "; select scope_identity()"

    def post_exec(self):
        if self._embedded_scope_identity:
            # Fetch the last inserted id from the manipulated statement
            # We may have to skip over a number of result sets with 
            # no data (due to triggers, etc.)
            while True:
                try:
                    # fetchall() ensures the cursor is consumed 
                    # without closing it (FreeTDS particularly)
                    row = self.cursor.fetchall()[0]  
                    break
                except self.dialect.dbapi.Error, e:
                    # no way around this - nextset() consumes the previous set
                    # so we need to just keep flipping
                    self.cursor.nextset()
                    
            self._lastrowid = int(row[0])
        else:
            super(MSExecutionContext_pyodbc, self).post_exec()


class MSDialect_pyodbc(PyODBCConnector, MSDialect):

    execution_ctx_cls = MSExecutionContext_pyodbc

    pyodbc_driver_name = 'SQL Server'
    
    colspecs = util.update_copy(
        MSDialect.colspecs,
        {
            sqltypes.Numeric:_MSNumeric_pyodbc
        }
    )
    
    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_pyodbc, self).__init__(**params)
        self.description_encoding = description_encoding
        self.use_scope_identity = self.dbapi and \
                        hasattr(self.dbapi.Cursor, 'nextset')
        
dialect = MSDialect_pyodbc

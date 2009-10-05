# mssql.py

"""Support for the Microsoft SQL Server database.

Driver
------

The MSSQL dialect will work with three different available drivers:

* *pyodbc* - http://pyodbc.sourceforge.net/. This is the recommeded
  driver.

* *pymssql* - http://pymssql.sourceforge.net/

* *adodbapi* - http://adodbapi.sourceforge.net/

Drivers are loaded in the order listed above based on availability.

If you need to load a specific driver pass ``module_name`` when
creating the engine::

    engine = create_engine('mssql://dsn', module_name='pymssql')

``module_name`` currently accepts: ``pyodbc``, ``pymssql``, and
``adodbapi``.

Currently the pyodbc driver offers the greatest level of
compatibility.

Connecting
----------

Connecting with create_engine() uses the standard URL approach of
``mssql://user:pass@host/dbname[?key=value&key=value...]``.

If the database name is present, the tokens are converted to a
connection string with the specified values. If the database is not
present, then the host token is taken directly as the DSN name.

Examples of pyodbc connection string URLs:

* *mssql://mydsn* - connects using the specified DSN named ``mydsn``.
  The connection string that is created will appear like::

    dsn=mydsn;Trusted_Connection=Yes

* *mssql://user:pass@mydsn* - connects using the DSN named
  ``mydsn`` passing in the ``UID`` and ``PWD`` information. The
  connection string that is created will appear like::

    dsn=mydsn;UID=user;PWD=pass

* *mssql://user:pass@mydsn/?LANGUAGE=us_english* - connects
  using the DSN named ``mydsn`` passing in the ``UID`` and ``PWD``
  information, plus the additional connection configuration option
  ``LANGUAGE``. The connection string that is created will appear
  like::

    dsn=mydsn;UID=user;PWD=pass;LANGUAGE=us_english

* *mssql://user:pass@host/db* - connects using a connection string
  dynamically created that would appear like::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass

* *mssql://user:pass@host:123/db* - connects using a connection
  string that is dynamically created, which also includes the port
  information using the comma syntax. If your connection string
  requires the port information to be passed as a ``port`` keyword
  see the next example. This will create the following connection
  string::

    DRIVER={SQL Server};Server=host,123;Database=db;UID=user;PWD=pass

* *mssql://user:pass@host/db?port=123* - connects using a connection
  string that is dynamically created that includes the port
  information as a separate ``port`` keyword. This will create the
  following connection string::

    DRIVER={SQL Server};Server=host;Database=db;UID=user;PWD=pass;port=123

If you require a connection string that is outside the options
presented above, use the ``odbc_connect`` keyword to pass in a
urlencoded connection string. What gets passed in will be urldecoded
and passed directly.

For example::

    mssql:///?odbc_connect=dsn%3Dmydsn%3BDatabase%3Ddb

would create the following connection string::

    dsn=mydsn;Database=db

Encoding your connection string can be easily accomplished through
the python shell. For example::

    >>> import urllib
    >>> urllib.quote_plus('dsn=mydsn;Database=db')
    'dsn%3Dmydsn%3BDatabase%3Ddb'

Additional arguments which may be specified either as query string
arguments on the URL, or as keyword argument to
:func:`~sqlalchemy.create_engine()` are:

* *auto_identity_insert* - enables support for IDENTITY inserts by
  automatically turning IDENTITY INSERT ON and OFF as required.
  Defaults to ``True``.

* *query_timeout* - allows you to override the default query timeout.
  Defaults to ``None``. This is only supported on pymssql.

* *text_as_varchar* - if enabled this will treat all TEXT column
  types as their equivalent VARCHAR(max) type. This is often used if
  you need to compare a VARCHAR to a TEXT field, which is not
  supported directly on MSSQL. Defaults to ``False``.

* *use_scope_identity* - allows you to specify that SCOPE_IDENTITY
  should be used in place of the non-scoped version @@IDENTITY.
  Defaults to ``False``. On pymssql this defaults to ``True``, and on
  pyodbc this defaults to ``True`` if the version of pyodbc being
  used supports it.

* *has_window_funcs* - indicates whether or not window functions
  (LIMIT and OFFSET) are supported on the version of MSSQL being
  used. If you're running MSSQL 2005 or later turn this on to get
  OFFSET support. Defaults to ``False``.

* *max_identifier_length* - allows you to se the maximum length of
  identfiers supported by the database. Defaults to 128. For pymssql
  the default is 30.

* *schema_name* - use to set the schema name. Defaults to ``dbo``.

Auto Increment Behavior
-----------------------

``IDENTITY`` columns are supported by using SQLAlchemy
``schema.Sequence()`` objects. In other words::

    Table('test', mss_engine,
           Column('id', Integer,
                  Sequence('blah',100,10), primary_key=True),
           Column('name', String(20))
         ).create()

would yield::

   CREATE TABLE test (
     id INTEGER NOT NULL IDENTITY(100,10) PRIMARY KEY,
     name VARCHAR(20) NULL,
     )

Note that the ``start`` and ``increment`` values for sequences are
optional and will default to 1,1.

* Support for ``SET IDENTITY_INSERT ON`` mode (automagic on / off for
  ``INSERT`` s)

* Support for auto-fetching of ``@@IDENTITY/@@SCOPE_IDENTITY()`` on
  ``INSERT``

Collation Support
-----------------

MSSQL specific string types support a collation parameter that
creates a column-level specific collation for the column. The
collation parameter accepts a Windows Collation Name or a SQL
Collation Name. Supported types are MSChar, MSNChar, MSString,
MSNVarchar, MSText, and MSNText. For example::

    Column('login', String(32, collation='Latin1_General_CI_AS'))

will yield::

    login VARCHAR(32) COLLATE Latin1_General_CI_AS NULL

LIMIT/OFFSET Support
--------------------

MSSQL has no support for the LIMIT or OFFSET keysowrds. LIMIT is
supported directly through the ``TOP`` Transact SQL keyword::

    select.limit

will yield::

    SELECT TOP n

If the ``has_window_funcs`` flag is set then LIMIT with OFFSET
support is available through the ``ROW_NUMBER OVER`` construct. This
construct requires an ``ORDER BY`` to be specified as well and is
only available on MSSQL 2005 and later.

Nullability
-----------
MSSQL has support for three levels of column nullability. The default
nullability allows nulls and is explicit in the CREATE TABLE
construct::

    name VARCHAR(20) NULL

If ``nullable=None`` is specified then no specification is made. In
other words the database's configured default is used. This will
render::

    name VARCHAR(20)

If ``nullable`` is ``True`` or ``False`` then the column will be
``NULL` or ``NOT NULL`` respectively.

Date / Time Handling
--------------------
For MSSQL versions that support the ``DATE`` and ``TIME`` types
(MSSQL 2008+) the data type is used. For versions that do not
support the ``DATE`` and ``TIME`` types a ``DATETIME`` type is used
instead and the MSSQL dialect handles converting the results
properly. This means ``Date()`` and ``Time()`` are fully supported
on all versions of MSSQL. If you do not desire this behavior then
do not use the ``Date()`` or ``Time()`` types.

Compatibility Levels
--------------------
MSSQL supports the notion of setting compatibility levels at the
database level. This allows, for instance, to run a database that
is compatibile with SQL2000 while running on a SQL2005 database
server. ``server_version_info`` will always retrun the database
server version information (in this case SQL2005) and not the
compatibiility level information. Because of this, if running under
a backwards compatibility mode SQAlchemy may attempt to use T-SQL
statements that are unable to be parsed by the database server.

Known Issues
------------

* No support for more than one ``IDENTITY`` column per table

* pymssql has problems with binary and unicode data that this module
  does **not** work around

"""
import datetime, decimal, inspect, operator, re, sys, urllib

from sqlalchemy import sql, schema, exc, util
from sqlalchemy import Table, MetaData, Column, ForeignKey, String, Integer
from sqlalchemy.sql import select, compiler, expression, operators as sql_operators, functions as sql_functions
from sqlalchemy.engine import default, base
from sqlalchemy import types as sqltypes
from decimal import Decimal as _python_Decimal


RESERVED_WORDS = set(
    ['add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'authorization',
     'backup', 'begin', 'between', 'break', 'browse', 'bulk', 'by', 'cascade',
     'case', 'check', 'checkpoint', 'close', 'clustered', 'coalesce',
     'collate', 'column', 'commit', 'compute', 'constraint', 'contains',
     'containstable', 'continue', 'convert', 'create', 'cross', 'current',
     'current_date', 'current_time', 'current_timestamp', 'current_user',
     'cursor', 'database', 'dbcc', 'deallocate', 'declare', 'default',
     'delete', 'deny', 'desc', 'disk', 'distinct', 'distributed', 'double',
     'drop', 'dump', 'else', 'end', 'errlvl', 'escape', 'except', 'exec',
     'execute', 'exists', 'exit', 'external', 'fetch', 'file', 'fillfactor',
     'for', 'foreign', 'freetext', 'freetexttable', 'from', 'full',
     'function', 'goto', 'grant', 'group', 'having', 'holdlock', 'identity',
     'identity_insert', 'identitycol', 'if', 'in', 'index', 'inner', 'insert',
     'intersect', 'into', 'is', 'join', 'key', 'kill', 'left', 'like',
     'lineno', 'load', 'merge', 'national', 'nocheck', 'nonclustered', 'not',
     'null', 'nullif', 'of', 'off', 'offsets', 'on', 'open', 'opendatasource',
     'openquery', 'openrowset', 'openxml', 'option', 'or', 'order', 'outer',
     'over', 'percent', 'pivot', 'plan', 'precision', 'primary', 'print',
     'proc', 'procedure', 'public', 'raiserror', 'read', 'readtext',
     'reconfigure', 'references', 'replication', 'restore', 'restrict',
     'return', 'revert', 'revoke', 'right', 'rollback', 'rowcount',
     'rowguidcol', 'rule', 'save', 'schema', 'securityaudit', 'select',
     'session_user', 'set', 'setuser', 'shutdown', 'some', 'statistics',
     'system_user', 'table', 'tablesample', 'textsize', 'then', 'to', 'top',
     'tran', 'transaction', 'trigger', 'truncate', 'tsequal', 'union',
     'unique', 'unpivot', 'update', 'updatetext', 'use', 'user', 'values',
     'varying', 'view', 'waitfor', 'when', 'where', 'while', 'with',
     'writetext',
    ])


class _StringType(object):
    """Base for MSSQL string types."""

    def __init__(self, collation=None, **kwargs):
        self.collation = kwargs.get('collate', collation)

    def _extend(self, spec):
        """Extend a string-type declaration with standard SQL
        COLLATE annotations.
        """

        if self.collation:
            collation = 'COLLATE %s' % self.collation
        else:
            collation = None

        return ' '.join([c for c in (spec, collation)
                         if c is not None])

    def __repr__(self):
        attributes = inspect.getargspec(self.__init__)[0][1:]
        attributes.extend(inspect.getargspec(_StringType.__init__)[0][1:])

        params = {}
        for attr in attributes:
            val = getattr(self, attr)
            if val is not None and val is not False:
                params[attr] = val

        return "%s(%s)" % (self.__class__.__name__,
                           ', '.join(['%s=%r' % (k, params[k]) for k in params]))

    def bind_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            if self.assert_unicode is None:
                assert_unicode = dialect.assert_unicode
            else:
                assert_unicode = self.assert_unicode

            if not assert_unicode:
                return None

            def process(value):
                if not isinstance(value, (unicode, sqltypes.NoneType)):
                    if assert_unicode == 'warn':
                        util.warn("Unicode type received non-unicode bind "
                                  "param value %r" % value)
                        return value
                    else:
                        raise exc.InvalidRequestError("Unicode type received non-unicode bind param value %r" % value)
                else:
                    return value
            return process
        else:
            return None


class MSNumeric(sqltypes.Numeric):
    def result_processor(self, dialect):
        if self.asdecimal:
            def process(value):
                if value is not None:
                    return _python_Decimal(str(value))
                else:
                    return value
            return process
        else:
            def process(value):
                return float(value)
            return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                # Not sure that this exception is needed
                return value

            elif isinstance(value, decimal.Decimal):
                if value.adjusted() < 0:
                    result = "%s0.%s%s" % (
                            (value < 0 and '-' or ''),
                            '0' * (abs(value.adjusted()) - 1),
                            "".join([str(nint) for nint in value._int]))

                else:
                    if 'E' in str(value):
                        result = "%s%s%s" % (
                                (value < 0 and '-' or ''),
                                "".join([str(s) for s in value._int]),
                                "0" * (value.adjusted() - (len(value._int)-1)))
                    else:
                        if (len(value._int) - 1) > value.adjusted():
                            result = "%s%s.%s" % (
                                    (value < 0 and '-' or ''),
                                    "".join([str(s) for s in value._int][0:value.adjusted() + 1]),
                                    "".join([str(s) for s in value._int][value.adjusted() + 1:]))
                        else:
                            result = "%s%s" % (
                                    (value < 0 and '-' or ''),
                                    "".join([str(s) for s in value._int][0:value.adjusted() + 1]))

                return result

            else:
                return value

        return process

    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': self.precision, 'scale' : self.scale}


class MSFloat(sqltypes.Float):
    def get_col_spec(self):
        if self.precision is None:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}


class MSReal(MSFloat):
    """A type for ``real`` numbers."""

    def __init__(self):
        """
        Construct a Real.

        """
        super(MSReal, self).__init__(precision=24)

    def adapt(self, impltype):
        return impltype()

    def get_col_spec(self):
        return "REAL"


class MSInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"


class MSBigInteger(MSInteger):
    def get_col_spec(self):
        return "BIGINT"


class MSTinyInteger(MSInteger):
    def get_col_spec(self):
        return "TINYINT"


class MSSmallInteger(MSInteger):
    def get_col_spec(self):
        return "SMALLINT"


class _DateTimeType(object):
    """Base for MSSQL datetime types."""

    def bind_processor(self, dialect):
        # if we receive just a date we can manipulate it
        # into a datetime since the db-api may not do this.
        def process(value):
            if type(value) is datetime.date:
                return datetime.datetime(value.year, value.month, value.day)
            return value
        return process


class MSDateTime(_DateTimeType, sqltypes.DateTime):
    def get_col_spec(self):
        return "DATETIME"


class MSDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"


class MSTime(sqltypes.Time):
    def __init__(self, precision=None, **kwargs):
        self.precision = precision
        super(MSTime, self).__init__()

    def get_col_spec(self):
        if self.precision:
            return "TIME(%s)" % self.precision
        else:
            return "TIME"


class MSSmallDateTime(_DateTimeType, sqltypes.TypeEngine):
    def get_col_spec(self):
        return "SMALLDATETIME"


class MSDateTime2(_DateTimeType, sqltypes.TypeEngine):
    def __init__(self, precision=None, **kwargs):
        self.precision = precision

    def get_col_spec(self):
        if self.precision:
            return "DATETIME2(%s)" % self.precision
        else:
            return "DATETIME2"


class MSDateTimeOffset(_DateTimeType, sqltypes.TypeEngine):
    def __init__(self, precision=None, **kwargs):
        self.precision = precision

    def get_col_spec(self):
        if self.precision:
            return "DATETIMEOFFSET(%s)" % self.precision
        else:
            return "DATETIMEOFFSET"


class MSDateTimeAsDate(_DateTimeType, MSDate):
    """ This is an implementation of the Date type for versions of MSSQL that
    do not support that specific type. In order to make it work a ``DATETIME``
    column specification is used and the results get converted back to just
    the date portion.

    """

    def get_col_spec(self):
        return "DATETIME"

    def result_processor(self, dialect):
        def process(value):
            # If the DBAPI returns the value as datetime.datetime(), truncate
            # it back to datetime.date()
            if type(value) is datetime.datetime:
                return value.date()
            return value
        return process


class MSDateTimeAsTime(MSTime):
    """ This is an implementation of the Time type for versions of MSSQL that
    do not support that specific type. In order to make it work a ``DATETIME``
    column specification is used and the results get converted back to just
    the time portion.

    """

    __zero_date = datetime.date(1900, 1, 1)

    def get_col_spec(self):
        return "DATETIME"

    def bind_processor(self, dialect):
        def process(value):
            if type(value) is datetime.datetime:
                value = datetime.datetime.combine(self.__zero_date, value.time())
            elif type(value) is datetime.time:
                value = datetime.datetime.combine(self.__zero_date, value)
            return value
        return process

    def result_processor(self, dialect):
        def process(value):
            if type(value) is datetime.datetime:
                return value.time()
            elif type(value) is datetime.date:
                return datetime.time(0, 0, 0)
            return value
        return process


class MSDateTime_adodbapi(MSDateTime):
    def result_processor(self, dialect):
        def process(value):
            # adodbapi will return datetimes with empty time values as datetime.date() objects.
            # Promote them back to full datetime.datetime()
            if type(value) is datetime.date:
                return datetime.datetime(value.year, value.month, value.day)
            return value
        return process


class MSText(_StringType, sqltypes.Text):
    """MSSQL TEXT type, for variable-length text up to 2^31 characters."""

    def __init__(self, *args, **kwargs):
        """Construct a TEXT.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.Text.__init__(self, None,
                convert_unicode=kwargs.get('convert_unicode', False),
                assert_unicode=kwargs.get('assert_unicode', None))

    def get_col_spec(self):
        if self.dialect.text_as_varchar:
            return self._extend("VARCHAR(max)")
        else:
            return self._extend("TEXT")


class MSNText(_StringType, sqltypes.UnicodeText):
    """MSSQL NTEXT type, for variable-length unicode text up to 2^30
    characters."""

    def __init__(self, *args, **kwargs):
        """Construct a NTEXT.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.UnicodeText.__init__(self, None,
                convert_unicode=kwargs.get('convert_unicode', True),
                assert_unicode=kwargs.get('assert_unicode', 'warn'))

    def get_col_spec(self):
        if self.dialect.text_as_varchar:
            return self._extend("NVARCHAR(max)")
        else:
            return self._extend("NTEXT")


class MSString(_StringType, sqltypes.String):
    """MSSQL VARCHAR type, for variable-length non-Unicode data with a maximum
    of 8,000 characters."""

    def __init__(self, length=None, convert_unicode=False, assert_unicode=None, **kwargs):
        """Construct a VARCHAR.

        :param length: Optinal, maximum data length, in characters.

        :param convert_unicode: defaults to False.  If True, convert
          ``unicode`` data sent to the database to a ``str``
          bytestring, and convert bytestrings coming back from the
          database into ``unicode``.

          Bytestrings are encoded using the dialect's
          :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which
          defaults to `utf-8`.

          If False, may be overridden by
          :attr:`sqlalchemy.engine.base.Dialect.convert_unicode`.

        :param assert_unicode:

          If None (the default), no assertion will take place unless
          overridden by :attr:`sqlalchemy.engine.base.Dialect.assert_unicode`.

          If 'warn', will issue a runtime warning if a ``str``
          instance is used as a bind value.

          If true, will raise an :exc:`sqlalchemy.exc.InvalidRequestError`.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.String.__init__(self, length=length,
                convert_unicode=convert_unicode,
                assert_unicode=assert_unicode)

    def get_col_spec(self):
        if self.length:
            return self._extend("VARCHAR(%s)" % self.length)
        else:
            return self._extend("VARCHAR")


class MSNVarchar(_StringType, sqltypes.Unicode):
    """MSSQL NVARCHAR type.

    For variable-length unicode character data up to 4,000 characters."""

    def __init__(self, length=None, **kwargs):
        """Construct a NVARCHAR.

        :param length: Optional, Maximum data length, in characters.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.Unicode.__init__(self, length=length,
                convert_unicode=kwargs.get('convert_unicode', True),
                assert_unicode=kwargs.get('assert_unicode', 'warn'))

    def adapt(self, impltype):
        return impltype(length=self.length,
                        convert_unicode=self.convert_unicode,
                        assert_unicode=self.assert_unicode,
                        collation=self.collation)

    def get_col_spec(self):
        if self.length:
            return self._extend("NVARCHAR(%(length)s)" % {'length' : self.length})
        else:
            return self._extend("NVARCHAR")


class MSChar(_StringType, sqltypes.CHAR):
    """MSSQL CHAR type, for fixed-length non-Unicode data with a maximum
    of 8,000 characters."""

    def __init__(self, length=None, convert_unicode=False, assert_unicode=None, **kwargs):
        """Construct a CHAR.

        :param length: Optinal, maximum data length, in characters.

        :param convert_unicode: defaults to False.  If True, convert
          ``unicode`` data sent to the database to a ``str``
          bytestring, and convert bytestrings coming back from the
          database into ``unicode``.

          Bytestrings are encoded using the dialect's
          :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which
          defaults to `utf-8`.

          If False, may be overridden by
          :attr:`sqlalchemy.engine.base.Dialect.convert_unicode`.

        :param assert_unicode:

          If None (the default), no assertion will take place unless
          overridden by :attr:`sqlalchemy.engine.base.Dialect.assert_unicode`.

          If 'warn', will issue a runtime warning if a ``str``
          instance is used as a bind value.

          If true, will raise an :exc:`sqlalchemy.exc.InvalidRequestError`.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.CHAR.__init__(self, length=length,
                convert_unicode=convert_unicode,
                assert_unicode=assert_unicode)

    def get_col_spec(self):
        if self.length:
            return self._extend("CHAR(%s)" % self.length)
        else:
            return self._extend("CHAR")


class MSNChar(_StringType, sqltypes.NCHAR):
    """MSSQL NCHAR type.

    For fixed-length unicode character data up to 4,000 characters."""

    def __init__(self, length=None, **kwargs):
        """Construct an NCHAR.

        :param length: Optional, Maximum data length, in characters.

        :param collation: Optional, a column-level collation for this string
          value. Accepts a Windows Collation Name or a SQL Collation Name.

        """
        _StringType.__init__(self, **kwargs)
        sqltypes.NCHAR.__init__(self, length=length,
                convert_unicode=kwargs.get('convert_unicode', True),
                assert_unicode=kwargs.get('assert_unicode', 'warn'))

    def get_col_spec(self):
        if self.length:
            return self._extend("NCHAR(%(length)s)" % {'length' : self.length})
        else:
            return self._extend("NCHAR")


class MSGenericBinary(sqltypes.Binary):
    """The Binary type assumes that a Binary specification without a length
    is an unbound Binary type whereas one with a length specification results
    in a fixed length Binary type.

    If you want standard MSSQL ``BINARY`` behavior use the ``MSBinary`` type.

    """

    def get_col_spec(self):
        if self.length:
            return "BINARY(%s)" % self.length
        else:
            return "IMAGE"


class MSBinary(MSGenericBinary):
    def get_col_spec(self):
        if self.length:
            return "BINARY(%s)" % self.length
        else:
            return "BINARY"


class MSVarBinary(MSGenericBinary):
    def get_col_spec(self):
        if self.length:
            return "VARBINARY(%s)" % self.length
        else:
            return "VARBINARY"


class MSImage(MSGenericBinary):
    def get_col_spec(self):
        return "IMAGE"


class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BIT"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is True:
                return 1
            elif value is False:
                return 0
            elif value is None:
                return None
            else:
                return value and True or False
        return process


class MSTimeStamp(sqltypes.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"


class MSMoney(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "MONEY"


class MSSmallMoney(MSMoney):
    def get_col_spec(self):
        return "SMALLMONEY"


class MSUniqueIdentifier(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "UNIQUEIDENTIFIER"


class MSVariant(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "SQL_VARIANT"

ischema = MetaData()

schemata = Table("SCHEMATA", ischema,
    Column("CATALOG_NAME", String, key="catalog_name"),
    Column("SCHEMA_NAME", String, key="schema_name"),
    Column("SCHEMA_OWNER", String, key="schema_owner"),
    schema="INFORMATION_SCHEMA")

tables = Table("TABLES", ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("TABLE_TYPE", String, key="table_type"),
    schema="INFORMATION_SCHEMA")

columns = Table("COLUMNS", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("IS_NULLABLE", Integer, key="is_nullable"),
    Column("DATA_TYPE", String, key="data_type"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    Column("CHARACTER_MAXIMUM_LENGTH", Integer, key="character_maximum_length"),
    Column("NUMERIC_PRECISION", Integer, key="numeric_precision"),
    Column("NUMERIC_SCALE", Integer, key="numeric_scale"),
    Column("COLUMN_DEFAULT", Integer, key="column_default"),
    Column("COLLATION_NAME", String, key="collation_name"),
    schema="INFORMATION_SCHEMA")

constraints = Table("TABLE_CONSTRAINTS", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("CONSTRAINT_TYPE", String, key="constraint_type"),
    schema="INFORMATION_SCHEMA")

column_constraints = Table("CONSTRAINT_COLUMN_USAGE", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    schema="INFORMATION_SCHEMA")

key_constraints = Table("KEY_COLUMN_USAGE", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    schema="INFORMATION_SCHEMA")

ref_constraints = Table("REFERENTIAL_CONSTRAINTS", ischema,
    Column("CONSTRAINT_CATALOG", String, key="constraint_catalog"),
    Column("CONSTRAINT_SCHEMA", String, key="constraint_schema"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("UNIQUE_CONSTRAINT_CATLOG", String, key="unique_constraint_catalog"),
    Column("UNIQUE_CONSTRAINT_SCHEMA", String, key="unique_constraint_schema"),
    Column("UNIQUE_CONSTRAINT_NAME", String, key="unique_constraint_name"),
    Column("MATCH_OPTION", String, key="match_option"),
    Column("UPDATE_RULE", String, key="update_rule"),
    Column("DELETE_RULE", String, key="delete_rule"),
    schema="INFORMATION_SCHEMA")

def _has_implicit_sequence(column):
    return column.primary_key and  \
        column.autoincrement and \
        isinstance(column.type, sqltypes.Integer) and \
        not column.foreign_keys and \
        (
            column.default is None or
            (
                isinstance(column.default, schema.Sequence) and
                column.default.optional)
            )

def _table_sequence_column(tbl):
    if not hasattr(tbl, '_ms_has_sequence'):
        tbl._ms_has_sequence = None
        for column in tbl.c:
            if getattr(column, 'sequence', False) or _has_implicit_sequence(column):
                tbl._ms_has_sequence = column
                break
    return tbl._ms_has_sequence

class MSSQLExecutionContext(default.DefaultExecutionContext):
    IINSERT = False
    HASIDENT = False

    def pre_exec(self):
        """Activate IDENTITY_INSERT if needed."""

        if self.compiled.isinsert:
            tbl = self.compiled.statement.table
            seq_column = _table_sequence_column(tbl)
            self.HASIDENT = bool(seq_column)
            if self.dialect.auto_identity_insert and self.HASIDENT:
                self.IINSERT = tbl._ms_has_sequence.key in self.compiled_parameters[0]
            else:
                self.IINSERT = False

            if self.IINSERT:
                self.cursor.execute("SET IDENTITY_INSERT %s ON" %
                    self.dialect.identifier_preparer.format_table(self.compiled.statement.table))

    def handle_dbapi_exception(self, e):
        if self.IINSERT:
            try:
                self.cursor.execute("SET IDENTITY_INSERT %s OFF" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))
            except:
                pass

    def post_exec(self):
        """Disable IDENTITY_INSERT if enabled."""

        if self.compiled.isinsert and not self.executemany and self.HASIDENT and not self.IINSERT:
            if not self._last_inserted_ids or self._last_inserted_ids[0] is None:
                if self.dialect.use_scope_identity:
                    self.cursor.execute("SELECT scope_identity() AS lastrowid")
                else:
                    self.cursor.execute("SELECT @@identity AS lastrowid")
                row = self.cursor.fetchone()
                self._last_inserted_ids = [int(row[0])] + self._last_inserted_ids[1:]

        if self.IINSERT:
            self.cursor.execute("SET IDENTITY_INSERT %s OFF" % self.dialect.identifier_preparer.format_table(self.compiled.statement.table))


class MSSQLExecutionContext_pyodbc (MSSQLExecutionContext):
    def pre_exec(self):
        """where appropriate, issue "select scope_identity()" in the same statement"""
        super(MSSQLExecutionContext_pyodbc, self).pre_exec()
        if self.compiled.isinsert and self.HASIDENT and not self.IINSERT \
                and len(self.parameters) == 1 and self.dialect.use_scope_identity:
            self.statement += "; select scope_identity()"

    def post_exec(self):
        if self.HASIDENT and not self.IINSERT and self.dialect.use_scope_identity and not self.executemany:
            import pyodbc
            # Fetch the last inserted id from the manipulated statement
            # We may have to skip over a number of result sets with no data (due to triggers, etc.)
            while True:
                try:
                    row = self.cursor.fetchone()
                    break
                except pyodbc.Error, e:
                    self.cursor.nextset()
            self._last_inserted_ids = [int(row[0])]
        else:
            super(MSSQLExecutionContext_pyodbc, self).post_exec()

class MSSQLDialect(default.DefaultDialect):
    name = 'mssql'
    supports_default_values = True
    supports_empty_insert = False
    auto_identity_insert = True
    execution_ctx_cls = MSSQLExecutionContext
    text_as_varchar = False
    use_scope_identity = False
    has_window_funcs = False
    max_identifier_length = 128
    schema_name = "dbo"

    colspecs = {
        sqltypes.Unicode : MSNVarchar,
        sqltypes.Integer : MSInteger,
        sqltypes.Smallinteger: MSSmallInteger,
        sqltypes.Numeric : MSNumeric,
        sqltypes.Float : MSFloat,
        sqltypes.DateTime : MSDateTime,
        sqltypes.Date : MSDate,
        sqltypes.Time : MSTime,
        sqltypes.String : MSString,
        sqltypes.Binary : MSGenericBinary,
        sqltypes.Boolean : MSBoolean,
        sqltypes.Text : MSText,
        sqltypes.UnicodeText : MSNText,
        sqltypes.CHAR: MSChar,
        sqltypes.NCHAR: MSNChar,
        sqltypes.TIMESTAMP: MSTimeStamp,
    }

    ischema_names = {
        'int' : MSInteger,
        'bigint': MSBigInteger,
        'smallint' : MSSmallInteger,
        'tinyint' : MSTinyInteger,
        'varchar' : MSString,
        'nvarchar' : MSNVarchar,
        'char' : MSChar,
        'nchar' : MSNChar,
        'text' : MSText,
        'ntext' : MSNText,
        'decimal' : MSNumeric,
        'numeric' : MSNumeric,
        'float' : MSFloat,
        'datetime' : MSDateTime,
        'datetime2' : MSDateTime2,
        'datetimeoffset' : MSDateTimeOffset,
        'date': MSDate,
        'time': MSTime,
        'smalldatetime' : MSSmallDateTime,
        'binary' : MSBinary,
        'varbinary' : MSVarBinary,
        'bit': MSBoolean,
        'real' : MSFloat,
        'image' : MSImage,
        'timestamp': MSTimeStamp,
        'money': MSMoney,
        'smallmoney': MSSmallMoney,
        'uniqueidentifier': MSUniqueIdentifier,
        'sql_variant': MSVariant,
    }

    def __new__(cls, *args, **kwargs):
        if cls is not MSSQLDialect:
            # this gets called with the dialect specific class
            return super(MSSQLDialect, cls).__new__(cls)
        dbapi = kwargs.get('dbapi', None)
        if dbapi:
            dialect = dialect_mapping.get(dbapi.__name__)
            return dialect(**kwargs)
        else:
            return object.__new__(cls)

    def __init__(self,
                 auto_identity_insert=True, query_timeout=None,
                 text_as_varchar=False, use_scope_identity=False,
                 has_window_funcs=False, max_identifier_length=None,
                 schema_name="dbo", **opts):
        self.auto_identity_insert = bool(auto_identity_insert)
        self.query_timeout = int(query_timeout or 0)
        self.schema_name = schema_name

        # to-do: the options below should use server version introspection to set themselves on connection
        self.text_as_varchar = bool(text_as_varchar)
        self.use_scope_identity = bool(use_scope_identity)
        self.has_window_funcs =  bool(has_window_funcs)
        self.max_identifier_length = int(max_identifier_length or 0) or \
                self.max_identifier_length
        super(MSSQLDialect, self).__init__(**opts)

    @classmethod
    def dbapi(cls, module_name=None):
        if module_name:
            try:
                dialect_cls = dialect_mapping[module_name]
                return dialect_cls.import_dbapi()
            except KeyError:
                raise exc.InvalidRequestError("Unsupported MSSQL module '%s' requested (must be adodbpi, pymssql or pyodbc)" % module_name)
        else:
            for dialect_cls in [MSSQLDialect_pyodbc, MSSQLDialect_pymssql, MSSQLDialect_adodbapi]:
                try:
                    return dialect_cls.import_dbapi()
                except ImportError, e:
                    pass
            else:
                raise ImportError('No DBAPI module detected for MSSQL - please install pyodbc, pymssql, or adodbapi')

    @base.connection_memoize(('mssql', 'server_version_info'))
    def server_version_info(self, connection):
        """A tuple of the database server version.

        Formats the remote server version as a tuple of version values,
        e.g. ``(9, 0, 1399)``.  If there are strings in the version number
        they will be in the tuple too, so don't count on these all being
        ``int`` values.

        This is a fast check that does not require a round trip.  It is also
        cached per-Connection.
        """
        return connection.dialect._server_version_info(connection.connection)

    def _server_version_info(self, dbapi_con):
        """Return a tuple of the database's version number."""
        raise NotImplementedError()

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        if 'auto_identity_insert' in opts:
            self.auto_identity_insert = bool(int(opts.pop('auto_identity_insert')))
        if 'query_timeout' in opts:
            self.query_timeout = int(opts.pop('query_timeout'))
        if 'text_as_varchar' in opts:
            self.text_as_varchar = bool(int(opts.pop('text_as_varchar')))
        if 'use_scope_identity' in opts:
            self.use_scope_identity = bool(int(opts.pop('use_scope_identity')))
        if 'has_window_funcs' in opts:
            self.has_window_funcs =  bool(int(opts.pop('has_window_funcs')))
        return self.make_connect_string(opts, url.query)

    def type_descriptor(self, typeobj):
        newobj = sqltypes.adapt_type(typeobj, self.colspecs)
        # Some types need to know about the dialect
        if isinstance(newobj, (MSText, MSNText)):
            newobj.dialect = self
        return newobj

    def do_savepoint(self, connection, name):
        util.warn("Savepoint support in mssql is experimental and may lead to data loss.")
        connection.execute("IF @@TRANCOUNT = 0 BEGIN TRANSACTION")
        connection.execute("SAVE TRANSACTION %s" % name)

    def do_release_savepoint(self, connection, name):
        pass

    @base.connection_memoize(('dialect', 'default_schema_name'))
    def get_default_schema_name(self, connection):
        query = "SELECT user_name() as user_name;"
        user_name = connection.scalar(sql.text(query))
        if user_name is not None:
            # now, get the default schema
            query = """
            SELECT default_schema_name FROM
            sys.database_principals
            WHERE name = :user_name
            AND type = 'S'
            """
            try:
                default_schema_name = connection.scalar(sql.text(query),
                                                    user_name=user_name)
                if default_schema_name is not None:
                    return default_schema_name
            except:
                pass
        return self.schema_name

    def table_names(self, connection, schema):
        s = select([tables.c.table_name], tables.c.table_schema==schema)
        return [row[0] for row in connection.execute(s)]


    def has_table(self, connection, tablename, schema=None):

        current_schema = schema or self.get_default_schema_name(connection)
        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==tablename, columns.c.table_schema==current_schema)
                       or columns.c.table_name==tablename,
                   )

        c = connection.execute(s)
        row  = c.fetchone()
        return row is not None

    def reflecttable(self, connection, table, include_columns):
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.get_default_schema_name(connection)

        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==table.name, columns.c.table_schema==current_schema)
                       or columns.c.table_name==table.name,
                   order_by=[columns.c.ordinal_position])

        c = connection.execute(s)
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True
            (name, type, nullable, charlen, numericprec, numericscale, default, collation) = (
                row[columns.c.column_name],
                row[columns.c.data_type],
                row[columns.c.is_nullable] == 'YES',
                row[columns.c.character_maximum_length],
                row[columns.c.numeric_precision],
                row[columns.c.numeric_scale],
                row[columns.c.column_default],
                row[columns.c.collation_name]
            )
            if include_columns and name not in include_columns:
                continue

            coltype = self.ischema_names.get(type, None)

            kwargs = {}
            if coltype in (MSString, MSChar, MSNVarchar, MSNChar, MSText, MSNText, MSBinary, MSVarBinary, sqltypes.Binary):
                kwargs['length'] = charlen
                if collation:
                    kwargs['collation'] = collation
                if coltype == MSText or (coltype in (MSString, MSNVarchar) and charlen == -1):
                    kwargs.pop('length')

            if issubclass(coltype, sqltypes.Numeric):
                kwargs['scale'] = numericscale
                kwargs['precision'] = numericprec

            if coltype is None:
                util.warn("Did not recognize type '%s' of column '%s'" % (type, name))
                coltype = sqltypes.NULLTYPE

            coltype = coltype(**kwargs)
            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))
            table.append_column(schema.Column(name, coltype, nullable=nullable, autoincrement=False, *colargs))

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # We also run an sp_columns to check for identity columns:
        cursor = connection.execute("sp_columns @table_name = '%s', @table_owner = '%s'" % (table.name, current_schema))
        ic = None
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            col_name, type_name = row[3], row[5]
            if type_name.endswith("identity") and col_name in table.c:
                ic = table.c[col_name]
                ic.autoincrement = True
                # setup a psuedo-sequence to represent the identity attribute - we interpret this at table.create() time as the identity attribute
                ic.sequence = schema.Sequence(ic.name + '_identity', 1, 1)
                # MSSQL: only one identity per table allowed
                cursor.close()
                break
        if not ic is None:
            try:
                cursor = connection.execute("select ident_seed(?), ident_incr(?)", table.fullname, table.fullname)
                row = cursor.fetchone()
                cursor.close()
                if not row is None:
                    ic.sequence.start = int(row[0])
                    ic.sequence.increment = int(row[1])
            except:
                # ignoring it, works just like before
                pass

        # Add constraints
        RR = ref_constraints
        TC = constraints
        C  = key_constraints.alias('C') #information_schema.constraint_column_usage: the constrained column
        R  = key_constraints.alias('R') #information_schema.constraint_column_usage: the referenced column

        # Primary key constraints
        s = sql.select([C.c.column_name, TC.c.constraint_type], sql.and_(TC.c.constraint_name == C.c.constraint_name,
                                                                         C.c.table_name == table.name,
                                                                         C.c.table_schema == (table.schema or current_schema)))
        c = connection.execute(s)
        for row in c:
            if 'PRIMARY' in row[TC.c.constraint_type.name] and row[0] in table.c:
                table.primary_key.add(table.c[row[0]])

        # Foreign key constraints
        s = sql.select([C.c.column_name,
                        R.c.table_schema, R.c.table_name, R.c.column_name,
                        RR.c.constraint_name, RR.c.match_option, RR.c.update_rule, RR.c.delete_rule],
                       sql.and_(C.c.table_name == table.name,
                                C.c.table_schema == (table.schema or current_schema),
                                C.c.constraint_name == RR.c.constraint_name,
                                R.c.constraint_name == RR.c.unique_constraint_name,
                                C.c.ordinal_position == R.c.ordinal_position
                                ),
                       order_by = [RR.c.constraint_name, R.c.ordinal_position])
        rows = connection.execute(s).fetchall()

        def _gen_fkref(table, rschema, rtbl, rcol):
            if rschema == current_schema and not table.schema:
                return '.'.join([rtbl, rcol])
            else:
                return '.'.join([rschema, rtbl, rcol])

        # group rows by constraint ID, to handle multi-column FKs
        fknm, scols, rcols = (None, [], [])
        for r in rows:
            scol, rschema, rtbl, rcol, rfknm, fkmatch, fkuprule, fkdelrule = r
            # if the reflected schema is the default schema then don't set it because this will
            # play into the metadata key causing duplicates.
            if rschema == current_schema and not table.schema:
                schema.Table(rtbl, table.metadata, autoload=True, autoload_with=connection)
            else:
                schema.Table(rtbl, table.metadata, schema=rschema, autoload=True, autoload_with=connection)
            if rfknm != fknm:
                if fknm:
                    table.append_constraint(schema.ForeignKeyConstraint(scols, [_gen_fkref(table, s, t, c) for s, t, c in rcols], fknm, link_to_name=True))
                fknm, scols, rcols = (rfknm, [], [])
            if not scol in scols:
                scols.append(scol)
            if not (rschema, rtbl, rcol) in rcols:
                rcols.append((rschema, rtbl, rcol))

        if fknm and scols:
            table.append_constraint(schema.ForeignKeyConstraint(scols, [_gen_fkref(table, s, t, c) for s, t, c in rcols], fknm, link_to_name=True))


class MSSQLDialect_pymssql(MSSQLDialect):
    supports_sane_rowcount = False
    max_identifier_length = 30

    @classmethod
    def import_dbapi(cls):
        import pymssql as module
        # pymmsql doesn't have a Binary method.  we use string
        # TODO: monkeypatching here is less than ideal
        module.Binary = lambda st: str(st)
        try:
            module.version_info = tuple(map(int, module.__version__.split('.')))
        except:
            module.version_info = (0, 0, 0)
        return module

    def __init__(self, **params):
        super(MSSQLDialect_pymssql, self).__init__(**params)
        self.use_scope_identity = True

        # pymssql understands only ascii
        if self.convert_unicode:
            util.warn("pymssql does not support unicode")
            self.encoding = params.get('encoding', 'ascii')

        self.colspecs = MSSQLDialect.colspecs.copy()
        self.ischema_names = MSSQLDialect.ischema_names.copy()
        self.ischema_names['date'] = MSDateTimeAsDate
        self.colspecs[sqltypes.Date] = MSDateTimeAsDate
        self.ischema_names['time'] = MSDateTimeAsTime
        self.colspecs[sqltypes.Time] = MSDateTimeAsTime

    def create_connect_args(self, url):
        r = super(MSSQLDialect_pymssql, self).create_connect_args(url)
        if hasattr(self, 'query_timeout'):
            if self.dbapi.version_info > (0, 8, 0):
                r[1]['timeout'] = self.query_timeout
            else:
                self.dbapi._mssql.set_query_timeout(self.query_timeout)
        return r

    def make_connect_string(self, keys, query):
        if keys.get('port'):
            # pymssql expects port as host:port, not a separate arg
            keys['host'] = ''.join([keys.get('host', ''), ':', str(keys['port'])])
            del keys['port']
        return [[], keys]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.DatabaseError) and "Error 10054" in str(e)

    def do_begin(self, connection):
        pass


class MSSQLDialect_pyodbc(MSSQLDialect):
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    # PyODBC unicode is broken on UCS-4 builds
    supports_unicode = sys.maxunicode == 65535
    supports_unicode_statements = supports_unicode
    execution_ctx_cls = MSSQLExecutionContext_pyodbc

    def __init__(self, description_encoding='latin-1', **params):
        super(MSSQLDialect_pyodbc, self).__init__(**params)
        self.description_encoding = description_encoding

        if self.server_version_info < (10,):
            self.colspecs = MSSQLDialect.colspecs.copy()
            self.ischema_names = MSSQLDialect.ischema_names.copy()
            self.ischema_names['date'] = MSDateTimeAsDate
            self.colspecs[sqltypes.Date] = MSDateTimeAsDate
            self.ischema_names['time'] = MSDateTimeAsTime
            self.colspecs[sqltypes.Time] = MSDateTimeAsTime

        # FIXME: scope_identity sniff should look at server version, not the ODBC driver
        # whether use_scope_identity will work depends on the version of pyodbc
        try:
            import pyodbc
            self.use_scope_identity = hasattr(pyodbc.Cursor, 'nextset')
        except:
            pass

    @classmethod
    def import_dbapi(cls):
        import pyodbc as module
        return module

    def make_connect_string(self, keys, query):
        if 'max_identifier_length' in keys:
            self.max_identifier_length = int(keys.pop('max_identifier_length'))

        if 'odbc_connect' in keys:
            connectors = [urllib.unquote_plus(keys.pop('odbc_connect'))]
        else:
            dsn_connection = 'dsn' in keys or ('host' in keys and 'database' not in keys)
            if dsn_connection:
                connectors= ['dsn=%s' % (keys.pop('host', '') or keys.pop('dsn', ''))]
            else:
                port = ''
                if 'port' in keys and not 'port' in query:
                    port = ',%d' % int(keys.pop('port'))

                connectors = ["DRIVER={%s}" % keys.pop('driver', 'SQL Server'),
                              'Server=%s%s' % (keys.pop('host', ''), port),
                              'Database=%s' % keys.pop('database', '') ]

            user = keys.pop("user", None)
            if user:
                connectors.append("UID=%s" % user)
                connectors.append("PWD=%s" % keys.pop('password', ''))
            else:
                connectors.append("Trusted_Connection=Yes")

            # if set to 'Yes', the ODBC layer will try to automagically convert
            # textual data from your database encoding to your client encoding
            # This should obviously be set to 'No' if you query a cp1253 encoded
            # database from a latin1 client...
            if 'odbc_autotranslate' in keys:
                connectors.append("AutoTranslate=%s" % keys.pop("odbc_autotranslate"))

            connectors.extend(['%s=%s' % (k,v) for k,v in keys.iteritems()])

        return [[";".join (connectors)], {}]

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or 'Attempt to use a closed connection.' in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False


    def _server_version_info(self, dbapi_con):
        """Convert a pyodbc SQL_DBMS_VER string into a tuple."""
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.getinfo(self.dbapi.SQL_DBMS_VER)):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

class MSSQLDialect_adodbapi(MSSQLDialect):
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_unicode = sys.maxunicode == 65535
    supports_unicode_statements = True

    @classmethod
    def import_dbapi(cls):
        import adodbapi as module
        return module

    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.DateTime] = MSDateTime_adodbapi

    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['datetime'] = MSDateTime_adodbapi

    def make_connect_string(self, keys, query):
        connectors = ["Provider=SQLOLEDB"]
        if 'port' in keys:
            connectors.append ("Data Source=%s, %s" % (keys.get("host"), keys.get("port")))
        else:
            connectors.append ("Data Source=%s" % keys.get("host"))
        connectors.append ("Initial Catalog=%s" % keys.get("database"))
        user = keys.get("user")
        if user:
            connectors.append("User Id=%s" % user)
            connectors.append("Password=%s" % keys.get("password", ""))
        else:
            connectors.append("Integrated Security=SSPI")
        return [[";".join (connectors)], {}]

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.adodbapi.DatabaseError) and "'connection failure'" in str(e)


dialect_mapping = {
    'pymssql':  MSSQLDialect_pymssql,
    'pyodbc':   MSSQLDialect_pyodbc,
    'adodbapi': MSSQLDialect_adodbapi
    }


class MSSQLCompiler(compiler.DefaultCompiler):
    operators = compiler.OPERATORS.copy()
    operators.update({
        sql_operators.concat_op: '+',
        sql_operators.match_op: lambda x, y: "CONTAINS (%s, %s)" % (x, y)
    })

    functions = compiler.DefaultCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now: 'CURRENT_TIMESTAMP',
            sql_functions.current_date: 'GETDATE()',
            'length': lambda x: "LEN(%s)" % x,
            sql_functions.char_length: lambda x: "LEN(%s)" % x
        }
    )

    extract_map = compiler.DefaultCompiler.extract_map.copy()
    extract_map.update ({
        'doy': 'dayofyear',
        'dow': 'weekday',
        'milliseconds': 'millisecond',
        'microseconds': 'microsecond'
    })

    def __init__(self, *args, **kwargs):
        super(MSSQLCompiler, self).__init__(*args, **kwargs)
        self.tablealiases = {}

    def get_select_precolumns(self, select):
        """ MS-SQL puts TOP, it's version of LIMIT here """
        if select._distinct or select._limit:
            s = select._distinct and "DISTINCT " or ""

            if select._limit:
                if not select._offset:
                    s += "TOP %s " % (select._limit,)
                else:
                    if not self.dialect.has_window_funcs:
                        raise exc.InvalidRequestError('MSSQL does not support LIMIT with an offset')
            return s
        return compiler.DefaultCompiler.get_select_precolumns(self, select)

    def limit_clause(self, select):
        # Limit in mssql is after the select keyword
        return ""

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        if self.dialect.has_window_funcs and not getattr(select, '_mssql_visit', None) and select._offset:
            # to use ROW_NUMBER(), an ORDER BY is required.
            orderby = self.process(select._order_by_clause)
            if not orderby:
                raise exc.InvalidRequestError('MSSQL requires an order_by when using an offset.')

            _offset = select._offset
            _limit = select._limit
            select._mssql_visit = True
            select = select.column(sql.literal_column("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("mssql_rn")).order_by(None).alias()

            limitselect = sql.select([c for c in select.c if c.key!='mssql_rn'])
            limitselect.append_whereclause("mssql_rn>%d" % _offset)
            if _limit is not None:
                limitselect.append_whereclause("mssql_rn<=%d" % (_limit + _offset))
            return self.process(limitselect, iswrapper=True, **kwargs)
        else:
            return compiler.DefaultCompiler.visit_select(self, select, **kwargs)

    def _schema_aliased_table(self, table):
        if getattr(table, 'schema', None) is not None:
            if table not in self.tablealiases:
                self.tablealiases[table] = table.alias()
            return self.tablealiases[table]
        else:
            return None

    def visit_table(self, table, mssql_aliased=False, **kwargs):
        if mssql_aliased:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

        # alias schema-qualified tables
        alias = self._schema_aliased_table(table)
        if alias is not None:
            return self.process(alias, mssql_aliased=True, **kwargs)
        else:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

    def visit_alias(self, alias, **kwargs):
        # translate for schema-qualified table aliases
        self.tablealiases[alias.original] = alias
        kwargs['mssql_aliased'] = True
        return super(MSSQLCompiler, self).visit_alias(alias, **kwargs)

    def visit_extract(self, extract):
        field = self.extract_map.get(extract.field, extract.field)
        return 'DATEPART("%s", %s)' % (field, self.process(extract.expr))

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TRANSACTION %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_column(self, column, result_map=None, **kwargs):
        if column.table is not None and \
            (not self.isupdate and not self.isdelete) or self.is_subquery():
            # translate for schema-qualified table aliases
            t = self._schema_aliased_table(column.table)
            if t is not None:
                converted = expression._corresponding_column_or_error(t, column)

                if result_map is not None:
                    result_map[column.name.lower()] = (column.name, (column, ), column.type)

                return super(MSSQLCompiler, self).visit_column(converted, result_map=None, **kwargs)

        return super(MSSQLCompiler, self).visit_column(column, result_map=result_map, **kwargs)

    def visit_binary(self, binary, **kwargs):
        """Move bind parameters to the right-hand side of an operator, where
        possible.

        """
        if isinstance(binary.left, expression._BindParamClause) and binary.operator == operator.eq \
            and not isinstance(binary.right, expression._BindParamClause):
            return self.process(expression._BinaryExpression(binary.right, binary.left, binary.operator), **kwargs)
        else:
            if (binary.operator is operator.eq or binary.operator is operator.ne) and (
                (isinstance(binary.left, expression._FromGrouping) and isinstance(binary.left.element, expression._ScalarSelect)) or \
                (isinstance(binary.right, expression._FromGrouping) and isinstance(binary.right.element, expression._ScalarSelect)) or \
                 isinstance(binary.left, expression._ScalarSelect) or isinstance(binary.right, expression._ScalarSelect)):
                op = binary.operator == operator.eq and "IN" or "NOT IN"
                return self.process(expression._BinaryExpression(binary.left, binary.right, op), **kwargs)
            return super(MSSQLCompiler, self).visit_binary(binary, **kwargs)

    def visit_insert(self, insert_stmt):
        insert_select = False
        if insert_stmt.parameters:
            insert_select = [p for p in insert_stmt.parameters.values() if isinstance(p, sql.Select)]
        if insert_select:
            self.isinsert = True
            colparams = self._get_colparams(insert_stmt)
            preparer = self.preparer

            insert = ' '.join(["INSERT"] +
                              [self.process(x) for x in insert_stmt._prefixes])

            if not colparams and not self.dialect.supports_default_values and not self.dialect.supports_empty_insert:
                raise exc.CompileError(
                    "The version of %s you are using does not support empty inserts." % self.dialect.name)
            elif not colparams and self.dialect.supports_default_values:
                return (insert + " INTO %s DEFAULT VALUES" % (
                    (preparer.format_table(insert_stmt.table),)))
            else:
                return (insert + " INTO %s (%s) SELECT %s" %
                    (preparer.format_table(insert_stmt.table),
                     ', '.join([preparer.format_column(c[0])
                               for c in colparams]),
                     ', '.join([c[1] for c in colparams])))
        else:
            return super(MSSQLCompiler, self).visit_insert(insert_stmt)

    def label_select_column(self, select, column, asfrom):
        if isinstance(column, expression.Function):
            return column.label(None)
        else:
            return super(MSSQLCompiler, self).label_select_column(select, column, asfrom)

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select):
        order_by = self.process(select._order_by_clause)

        # MSSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""


class MSSQLSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()

        if column.nullable is not None:
            if not column.nullable or column.primary_key:
                colspec += " NOT NULL"
            else:
                colspec += " NULL"

        if not column.table:
            raise exc.InvalidRequestError("mssql requires Table-bound columns in order to generate DDL")

        seq_col = _table_sequence_column(column.table)

        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if seq_col is column:
            sequence = getattr(column, 'sequence', None)
            if sequence:
                start, increment = sequence.start or 1, sequence.increment or 1
            else:
                start, increment = 1, 1
            colspec += " IDENTITY(%s,%s)" % (start, increment)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

class MSSQLSchemaDropper(compiler.SchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX %s.%s" % (
            self.preparer.quote_identifier(index.table.name),
            self.preparer.quote(self._validate_identifier(index.name, False), index.quote)
            ))
        self.execute()


class MSSQLIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(self, dialect):
        super(MSSQLIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')

    def _escape_identifier(self, value):
        #TODO: determine MSSQL's escaping rules
        return value

    def quote_schema(self, schema, force=True):
        """Prepare a quoted table and schema name."""
        result = '.'.join([self.quote(x, force) for x in schema.split('.')])
        return result

dialect = MSSQLDialect
dialect.statement_compiler = MSSQLCompiler
dialect.schemagenerator = MSSQLSchemaGenerator
dialect.schemadropper = MSSQLSchemaDropper
dialect.preparer = MSSQLIdentifierPreparer


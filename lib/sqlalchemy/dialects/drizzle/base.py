# drizzle/base.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
# Copyright (C) 2010-2011 Monty Taylor <mordred@inaugust.com>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the Drizzle database.

Supported Versions and Features
-------------------------------

SQLAlchemy supports the Drizzle database starting with 2010.08.
with capabilities increasing with more modern servers.

Most available DBAPI drivers are supported; see below.

=====================================  ===============
Feature                                Minimum Version
=====================================  ===============
sqlalchemy.orm                         2010.08
Table Reflection                       2010.08
DDL Generation                         2010.08
utf8/Full Unicode Connections          2010.08
Transactions                           2010.08
Two-Phase Transactions                 2010.08
Nested Transactions                    2010.08
=====================================  ===============

See the official Drizzle documentation for detailed information about features
supported in any given server release.

Connecting
----------

See the API documentation on individual drivers for details on connecting.

Connection Timeouts
-------------------

Drizzle features an automatic connection close behavior, for connections that
have been idle for eight hours or more.   To circumvent having this issue, use
the ``pool_recycle`` option which controls the maximum age of any connection::

    engine = create_engine('drizzle+mysqldb://...', pool_recycle=3600)

Storage Engines
---------------

Drizzle defaults to the ``InnoDB`` storage engine, which is transactional.

Storage engines can be elected when creating tables in SQLAlchemy by supplying
a ``drizzle_engine='whatever'`` to the ``Table`` constructor.  Any Drizzle table
creation option can be specified in this syntax::

  Table('mytable', metadata,
        Column('data', String(32)),
        drizzle_engine='InnoDB',
       )

Keys
----

Not all Drizzle storage engines support foreign keys.  For ``BlitzDB`` and
similar engines, the information loaded by table reflection will not include
foreign keys.  For these tables, you may supply a
:class:`~sqlalchemy.ForeignKeyConstraint` at reflection time::

  Table('mytable', metadata,
        ForeignKeyConstraint(['other_id'], ['othertable.other_id']),
        autoload=True
       )

When creating tables, SQLAlchemy will automatically set ``AUTO_INCREMENT`` on
an integer primary key column::

  >>> t = Table('mytable', metadata,
  ...   Column('mytable_id', Integer, primary_key=True)
  ... )
  >>> t.create()
  CREATE TABLE mytable (
          id INTEGER NOT NULL AUTO_INCREMENT,
          PRIMARY KEY (id)
  )

You can disable this behavior by supplying ``autoincrement=False`` to the
:class:`~sqlalchemy.Column`.  This flag can also be used to enable
auto-increment on a secondary column in a multi-column key for some storage
engines::

  Table('mytable', metadata,
        Column('gid', Integer, primary_key=True, autoincrement=False),
        Column('id', Integer, primary_key=True)
       )

Drizzle SQL Extensions
----------------------

Many of the Drizzle SQL extensions are handled through SQLAlchemy's generic
function and operator support::

  table.select(table.c.password==func.md5('plaintext'))
  table.select(table.c.username.op('regexp')('^[a-d]'))

And of course any valid Drizzle statement can be executed as a string as well.

Some limited direct support for Drizzle extensions to SQL is currently
available.

* SELECT pragma::

    select(..., prefixes=['HIGH_PRIORITY', 'SQL_SMALL_RESULT'])

* UPDATE with LIMIT::

    update(..., drizzle_limit=10)

"""

import datetime, inspect, re, sys

from sqlalchemy import schema as sa_schema
from sqlalchemy import exc, log, sql, util
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql import functions as sql_functions
from sqlalchemy.sql import compiler
from array import array as _array

from sqlalchemy.engine import reflection
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.mysql import base as mysql_dialect

from sqlalchemy.types import DATE, DATETIME, BOOLEAN, TIME, \
                                BLOB, BINARY, VARBINARY

class _NumericType(object):
    """Base for Drizzle numeric types."""

    def __init__(self, **kw):
        super(_NumericType, self).__init__(**kw)

class _FloatType(_NumericType, sqltypes.Float):
    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        if isinstance(self, (REAL, DOUBLE)) and \
            (
                (precision is None and scale is not None) or
                (precision is not None and scale is None)
            ):
           raise exc.ArgumentError(
               "You must specify both precision and scale or omit "
               "both altogether.")

        super(_FloatType, self).__init__(precision=precision, asdecimal=asdecimal, **kw)
        self.scale = scale

class _StringType(mysql_dialect._StringType):
    """Base for Drizzle string types."""

    def __init__(self, collation=None,
                 binary=False,
                 **kw):
        kw['national'] = False
        super(_StringType, self).__init__(collation=collation, 
                                        binary=binary, 
                                        **kw)


class NUMERIC(_NumericType, sqltypes.NUMERIC):
    """Drizzle NUMERIC type."""

    __visit_name__ = 'NUMERIC'

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a NUMERIC.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        """
        super(NUMERIC, self).__init__(precision=precision, scale=scale, asdecimal=asdecimal, **kw)


class DECIMAL(_NumericType, sqltypes.DECIMAL):
    """Drizzle DECIMAL type."""

    __visit_name__ = 'DECIMAL'

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a DECIMAL.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        """
        super(DECIMAL, self).__init__(precision=precision, scale=scale,
                                      asdecimal=asdecimal, **kw)


class DOUBLE(_FloatType):
    """Drizzle DOUBLE type."""

    __visit_name__ = 'DOUBLE'

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a DOUBLE.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        """
        super(DOUBLE, self).__init__(precision=precision, scale=scale,
                                     asdecimal=asdecimal, **kw)

class REAL(_FloatType, sqltypes.REAL):
    """Drizzle REAL type."""

    __visit_name__ = 'REAL'

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        """Construct a REAL.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        """
        super(REAL, self).__init__(precision=precision, scale=scale,
                                   asdecimal=asdecimal, **kw)

class FLOAT(_FloatType, sqltypes.FLOAT):
    """Drizzle FLOAT type."""

    __visit_name__ = 'FLOAT'

    def __init__(self, precision=None, scale=None, asdecimal=False, **kw):
        """Construct a FLOAT.

        :param precision: Total digits in this number.  If scale and precision
          are both None, values are stored to limits allowed by the server.

        :param scale: The number of digits after the decimal point.

        """
        super(FLOAT, self).__init__(precision=precision, scale=scale,
                                    asdecimal=asdecimal, **kw)

    def bind_processor(self, dialect):
        return None

class INTEGER(sqltypes.INTEGER):
    """Drizzle INTEGER type."""

    __visit_name__ = 'INTEGER'

    def __init__(self, **kw):
        """Construct an INTEGER.

        """
        super(INTEGER, self).__init__(**kw)

class BIGINT(sqltypes.BIGINT):
    """Drizzle BIGINTEGER type."""

    __visit_name__ = 'BIGINT'

    def __init__(self, **kw):
        """Construct a BIGINTEGER.

        """
        super(BIGINT, self).__init__(**kw)


class _DrizzleTime(mysql_dialect._MSTime):
    """Drizzle TIME type."""

class TIMESTAMP(sqltypes.TIMESTAMP):
    """Drizzle TIMESTAMP type."""
    __visit_name__ = 'TIMESTAMP'

class TEXT(_StringType, sqltypes.TEXT):
    """Drizzle TEXT type, for text up to 2^16 characters."""

    __visit_name__ = 'TEXT'

    def __init__(self, length=None, **kw):
        """Construct a TEXT.

        :param length: Optional, if provided the server may optimize storage
          by substituting the smallest TEXT type sufficient to store
          ``length`` characters.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super(TEXT, self).__init__(length=length, **kw)

class VARCHAR(_StringType, sqltypes.VARCHAR):
    """Drizzle VARCHAR type, for variable-length character data."""

    __visit_name__ = 'VARCHAR'

    def __init__(self, length=None, **kwargs):
        """Construct a VARCHAR.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        """
        super(VARCHAR, self).__init__(length=length, **kwargs)

class CHAR(_StringType, sqltypes.CHAR):
    """Drizzle CHAR type, for fixed-length character data."""

    __visit_name__ = 'CHAR'

    def __init__(self, length=None, **kwargs):
        """Construct a CHAR.

        :param length: Maximum data length, in characters.

        :param binary: Optional, use the default binary collation for the
          national character set.  This does not affect the type of data
          stored, use a BINARY type for binary data.

        :param collation: Optional, request a particular collation.  Must be
          compatible with the national character set.

        """
        super(CHAR, self).__init__(length=length, **kwargs)

class ENUM(mysql_dialect.ENUM):
    """Drizzle ENUM type."""

    def __init__(self, *enums, **kw):
        """Construct an ENUM.

        Example:

          Column('myenum', ENUM("foo", "bar", "baz"))

        :param enums: The range of valid values for this ENUM.  Values will be
          quoted when generating the schema according to the quoting flag (see
          below).

        :param strict: Defaults to False: ensure that a given value is in this
          ENUM's range of permissible values when inserting or updating rows.
          Note that Drizzle will not raise a fatal error if you attempt to store
          an out of range value- an alternate value will be stored instead.
          (See Drizzle ENUM documentation.)

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        :param quoting: Defaults to 'auto': automatically determine enum value
          quoting.  If all enum values are surrounded by the same quoting
          character, then use 'quoted' mode.  Otherwise, use 'unquoted' mode.

          'quoted': values in enums are already quoted, they will be used
          directly when generating the schema - this usage is deprecated.

          'unquoted': values in enums are not quoted, they will be escaped and
          surrounded by single quotes when generating the schema.

          Previous versions of this type always required manually quoted
          values to be supplied; future versions will always quote the string
          literals for you.  This is a transitional option.

        """
        super(ENUM, self).__init__(*enums, **kw)

class _DrizzleBoolean(sqltypes.Boolean):
    def get_dbapi_type(self, dbapi):
        return dbapi.NUMERIC

colspecs = {
    sqltypes.Numeric: NUMERIC,
    sqltypes.Float: FLOAT,
    sqltypes.Time: _DrizzleTime,
    sqltypes.Enum: ENUM,
    sqltypes.Boolean: _DrizzleBoolean,
}

# All the types we have in Drizzle
ischema_names = {
    'BIGINT': BIGINT,
    'BINARY': BINARY,
    'BLOB': BLOB,
    'BOOLEAN': BOOLEAN,
    'CHAR': CHAR,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'DECIMAL': DECIMAL,
    'DOUBLE': DOUBLE,
    'ENUM': ENUM,
    'FLOAT': FLOAT,
    'INT': INTEGER,
    'INTEGER': INTEGER,
    'NUMERIC': NUMERIC,
    'TEXT': TEXT,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARBINARY': VARBINARY,
    'VARCHAR': VARCHAR,
}

class DrizzleCompiler(mysql_dialect.MySQLCompiler):

    def visit_typeclause(self, typeclause):
        type_ = typeclause.type.dialect_impl(self.dialect)
        if isinstance(type_, sqltypes.Integer):
            return 'INTEGER'
        else:
            return super(DrizzleCompiler, self).visit_typeclause(typeclause)

    def visit_cast(self, cast, **kwargs):
        type_ = self.process(cast.typeclause)
        if type_ is None:
             return self.process(cast.clause)

        return 'CAST(%s AS %s)' % (self.process(cast.clause), type_)


class DrizzleDDLCompiler(mysql_dialect.MySQLDDLCompiler):
    pass

class DrizzleTypeCompiler(mysql_dialect.MySQLTypeCompiler):
    def _extend_numeric(self, type_, spec):
        return spec

    def _extend_string(self, type_, defaults, spec):
        """Extend a string-type declaration with standard SQL 
        COLLATE annotations and Drizzle specific extensions.

        """

        def attr(name):
            return getattr(type_, name, defaults.get(name))

        if attr('collation'):
            collation = 'COLLATE %s' % type_.collation
        elif attr('binary'):
            collation = 'BINARY'
        else:
            collation = None

        return ' '.join([c for c in (spec, collation)
                         if c is not None])

    def visit_NCHAR(self, type):
        raise NotImplementedError("Drizzle does not support NCHAR")

    def visit_NVARCHAR(self, type):
        raise NotImplementedError("Drizzle does not support NVARCHAR")

    def visit_FLOAT(self, type_):
        if type_.scale is not None and type_.precision is not None:
            return "FLOAT(%s, %s)" % (type_.precision, type_.scale)
        else:
            return "FLOAT"

    def visit_BOOLEAN(self, type_):
        return "BOOLEAN"

    def visit_BLOB(self, type_):
        return "BLOB"


class DrizzleExecutionContext(mysql_dialect.MySQLExecutionContext):
    pass

class DrizzleIdentifierPreparer(mysql_dialect.MySQLIdentifierPreparer):
    pass

class DrizzleDialect(mysql_dialect.MySQLDialect):
    """Details of the Drizzle dialect.  Not used directly in application code."""

    name = 'drizzle'

    _supports_cast = True
    supports_sequences = False
    supports_native_boolean = True
    supports_views = False


    default_paramstyle = 'format'
    colspecs = colspecs

    statement_compiler = DrizzleCompiler
    ddl_compiler = DrizzleDDLCompiler
    type_compiler = DrizzleTypeCompiler
    ischema_names = ischema_names
    preparer = DrizzleIdentifierPreparer

    def on_connect(self):
        """Force autocommit - Drizzle Bug#707842 doesn't set this
        properly"""
        def connect(conn):
            conn.autocommit(False)
        return connect

    def do_commit(self, connection):
        """Execute a COMMIT."""

        connection.commit()

    def do_rollback(self, connection):
        """Execute a ROLLBACK."""

        connection.rollback()

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """Return a Unicode SHOW TABLES from a given schema."""
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name

        charset = 'utf8'
        rp = connection.execute("SHOW TABLES FROM %s" %
            self.identifier_preparer.quote_identifier(current_schema))
        return [row[0] for row in self._compat_fetchall(rp, charset=charset)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        raise NotImplementedError

    def _detect_casing(self, connection):
        """Sniff out identifier case sensitivity.

        Cached per-connection. This value can not change without a server
        restart.

        """
        return 0

    def _detect_collations(self, connection):
        """Pull the active COLLATIONS list from the server.

        Cached per-connection.
        """

        collations = {}
        charset = self._connection_charset
        rs = connection.execute('SELECT CHARACTER_SET_NAME, COLLATION_NAME from data_dictionary.COLLATIONS')
        for row in self._compat_fetchall(rs, charset):
            collations[row[0]] = row[1]
        return collations

    def _detect_ansiquotes(self, connection):
        """Detect and adjust for the ANSI_QUOTES sql mode."""

        self._server_ansiquotes = False

        self._backslash_escapes = False

log.class_logger(DrizzleDialect)


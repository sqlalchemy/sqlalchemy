# mysql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the MySQL database.

SQLAlchemy supports 6 major MySQL versions: 3.23, 4.0, 4.1, 5.0, 5.1 and 6.0,
with capablities increasing with more modern servers. 

Versions 4.1 and higher support the basic SQL functionality that SQLAlchemy
uses in the ORM and SQL expressions.  These versions pass the applicable
tests in the suite 100%.  No heroic measures are taken to work around major
missing SQL features- if your server version does not support sub-selects, for
example, they won't work in SQLAlchemy either.

Currently, the only DB-API driver supported is `MySQL-Python` (also referred to
as `MySQLdb`).  Either 1.2.1 or 1.2.2 are recommended.  The alpha, beta and
gamma releases of 1.2.1 and 1.2.2 should be avoided.  Support for Jython and
IronPython is planned.

=====================================  ===============
Feature                                Minimum Version
=====================================  ===============
sqlalchemy.orm                         4.1.1
Table Reflection                       3.23.x
DDL Generation                         4.1.1
utf8/Full Unicode Connections          4.1.1
Transactions                           3.23.15
Two-Phase Transactions                 5.0.3
Nested Transactions                    5.0.3
=====================================  ===============

See the official MySQL documentation for detailed information about features
supported in any given server release.

Many MySQL server installations default to a ``latin1`` encoding for client
connections.  All data sent through the connection will be converted
into ``latin1``, even if you have ``utf8`` or another character set on your
tables and columns.  With versions 4.1 and higher, you can change the
connection character set either through server configuration or by passing
the  ``charset`` parameter to  ``create_engine``.  The ``charset`` option is
passed through to MySQL-Python and has the side-effect of also enabling
``use_unicode`` in the driver by default.  For regular encoded strings, also
pass ``use_unicode=0`` in the connection arguments.

Most MySQL server installations have a default table type of `MyISAM`, a
non-transactional table type.  During a transaction, non-transactional
storage engines do not participate and continue to store table changes in
autocommit mode.  For fully atomic transactions, all participating tables
must use a transactional engine such as `InnoDB`, `Falcon`, `SolidDB`,
`PBXT`, etc.  Storage engines can be elected when creating tables in
SQLAlchemy by supplying a ``mysql_engine='whatever'`` to the ``Table``
constructor.  Any MySQL table creation option can be specified in this syntax.

Not all MySQL storage engines support foreign keys.  For `MyISAM` and similar
engines, the information loaded by table reflection will not include foreign
keys.  For these tables, you may supply ``ForeignKeyConstraints`` at reflection
time::

  Table('mytable', metadata, autoload=True,
        ForeignKeyConstraint(['other_id'], ['othertable.other_id']))

When creating tables, SQLAlchemy will automatically set AUTO_INCREMENT on an
integer primary key column::

  >>> t = Table('mytable', metadata,
  ...   Column('mytable_id', Integer, primary_key=True))
  >>> t.create()
  CREATE TABLE mytable (
          id INTEGER NOT NULL AUTO_INCREMENT, 
          PRIMARY KEY (id)
  )

You can disable this behavior by supplying ``autoincrement=False`` in addition.
This can also be used to enable auto-increment on a secondary column in a
multi-column key for some storage engines::

  Table('mytable', metadata,
        Column('gid', Integer, primary_key=True, autoincrement=False),
        Column('id', Integer, primary_key=True))

MySQL SQL modes are supported.  Modes that enable ``ANSI_QUOTE`` (such as
``ANSI``) require an engine option to modify SQLAlchemy's quoting style.
When using an ANSI-quoting mode, supply ``use_ansiquotes=True`` when
creating your ``Engine``::

  create_engine('mysql://localhost/test', use_ansiquotes=True)

This is an engine-wide option and is not toggleable on a per-connection basis.
SQLAlchemy does not presume to ``SET sql_mode`` for you with this option.
For the best performance, set the quoting style server-wide in ``my.cnf`` or
by supplying ``--sql-mode`` to ``mysqld``.  You can also use a ``Pool`` hook
to issue a ``SET SESSION sql_mode='...'`` on connect to configure each
connection.

For normal SQLAlchemy usage, loading this module is unnescesary.  It will be
loaded on-demand when a MySQL connection is needed.  The generic column types
like ``String`` and ``Integer`` will automatically be adapted to the optimal
matching MySQL column type.

But if you would like to use one of the MySQL-specific or enhanced column
types when creating tables with your ``Table`` definitions, then you will
need to import them from this module::

  from sqlalchemy.databases import mysql

  Table('mytable', metadata,
        Column('id', Integer, primary_key=True),
        Column('ittybittyblob', mysql.MSTinyBlob),
        Column('biggy', mysql.MSBigInteger(unsigned=True)))

All standard MySQL column types are supported.  The OpenGIS types are
available for use via table reflection but have no special support or
mapping to Python classes.  If you're using these types and have opinions
about how OpenGIS can be smartly integrated into SQLAlchemy please join
the mailing list!

If you have problems that seem server related, first check that you are
using the most recent stable MySQL-Python package available.  The Database
Notes page on the wiki at http://sqlalchemy.org is a good resource for timely
information affecting MySQL in SQLAlchemy.
"""

import re, datetime, inspect, warnings, sys
from array import array as _array

from sqlalchemy import exceptions, logging, schema, sql, util
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql import compiler

from sqlalchemy.engine import base as engine_base, default
from sqlalchemy import types as sqltypes


__all__ = (
    'MSBigInteger', 'MSBinary', 'MSBit', 'MSBlob', 'MSBoolean', 
    'MSChar', 'MSDate', 'MSDateTime', 'MSDecimal', 'MSDouble', 
    'MSEnum', 'MSFloat', 'MSInteger', 'MSLongBlob', 'MSLongText', 
    'MSMediumBlob', 'MSMediumText', 'MSNChar', 'MSNVarChar', 
    'MSNumeric', 'MSSet', 'MSSmallInteger', 'MSString', 'MSText', 
    'MSTime', 'MSTimeStamp', 'MSTinyBlob', 'MSTinyInteger', 
    'MSTinyText', 'MSVarBinary', 'MSYear' )


RESERVED_WORDS = util.Set(
    ['accessible', 'add', 'all', 'alter', 'analyze','and', 'as', 'asc',
     'asensitive', 'before', 'between', 'bigint', 'binary', 'blob', 'both',
     'by', 'call', 'cascade', 'case', 'change', 'char', 'character', 'check',
     'collate', 'column', 'condition', 'constraint', 'continue', 'convert',
     'create', 'cross', 'current_date', 'current_time', 'current_timestamp',
     'current_user', 'cursor', 'database', 'databases', 'day_hour',
     'day_microsecond', 'day_minute', 'day_second', 'dec', 'decimal',
     'declare', 'default', 'delayed', 'delete', 'desc', 'describe',
     'deterministic', 'distinct', 'distinctrow', 'div', 'double', 'drop',
     'dual', 'each', 'else', 'elseif', 'enclosed', 'escaped', 'exists',
     'exit', 'explain', 'false', 'fetch', 'float', 'float4', 'float8',
     'for', 'force', 'foreign', 'from', 'fulltext', 'grant', 'group', 'having',
     'high_priority', 'hour_microsecond', 'hour_minute', 'hour_second', 'if',
     'ignore', 'in', 'index', 'infile', 'inner', 'inout', 'insensitive',
     'insert', 'int', 'int1', 'int2', 'int3', 'int4', 'int8', 'integer',
     'interval', 'into', 'is', 'iterate', 'join', 'key', 'keys', 'kill',
     'leading', 'leave', 'left', 'like', 'limit', 'linear', 'lines', 'load',
     'localtime', 'localtimestamp', 'lock', 'long', 'longblob', 'longtext',
     'loop', 'low_priority', 'master_ssl_verify_server_cert', 'match',
     'mediumblob', 'mediumint', 'mediumtext', 'middleint',
     'minute_microsecond', 'minute_second', 'mod', 'modifies', 'natural',
     'not', 'no_write_to_binlog', 'null', 'numeric', 'on', 'optimize',
     'option', 'optionally', 'or', 'order', 'out', 'outer', 'outfile',
     'precision', 'primary', 'procedure', 'purge', 'range', 'read', 'reads',
     'read_only', 'read_write', 'real', 'references', 'regexp', 'release',
     'rename', 'repeat', 'replace', 'require', 'restrict', 'return',
     'revoke', 'right', 'rlike', 'schema', 'schemas', 'second_microsecond',
     'select', 'sensitive', 'separator', 'set', 'show', 'smallint', 'spatial',
     'specific', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning',
     'sql_big_result', 'sql_calc_found_rows', 'sql_small_result', 'ssl',
     'starting', 'straight_join', 'table', 'terminated', 'then', 'tinyblob',
     'tinyint', 'tinytext', 'to', 'trailing', 'trigger', 'true', 'undo',
     'union', 'unique', 'unlock', 'unsigned', 'update', 'usage', 'use',
     'using', 'utc_date', 'utc_time', 'utc_timestamp', 'values', 'varbinary',
     'varchar', 'varcharacter', 'varying', 'when', 'where', 'while', 'with',
     'write', 'x509', 'xor', 'year_month', 'zerofill', # 5.0
     'fields', # 4.1
     'accessible', 'linear', 'master_ssl_verify_server_cert', 'range',
     'read_only', 'read_write', # 5.1
     ])

AUTOCOMMIT_RE = re.compile(
    r'\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER|LOAD +DATA)',
    re.I | re.UNICODE)
SELECT_RE = re.compile(
    r'\s*(?:SELECT|SHOW|DESCRIBE|XA RECOVER)',
    re.I | re.UNICODE)

class _NumericType(object):
    """Base for MySQL numeric types."""

    def __init__(self, unsigned=False, zerofill=False, **kw):
        self.unsigned = unsigned
        self.zerofill = zerofill

    def _extend(self, spec):
        "Extend a numeric-type declaration with MySQL specific extensions."
        
        if self.unsigned:
            spec += ' UNSIGNED'
        if self.zerofill:
            spec += ' ZEROFILL'
        return spec


class _StringType(object):
    """Base for MySQL string types."""

    def __init__(self, charset=None, collation=None,
                 ascii=False, unicode=False, binary=False,
                 national=False, **kwargs):
        self.charset = charset
        # allow collate= or collation= 
        self.collation = kwargs.get('collate', collation)
        self.ascii = ascii
        self.unicode = unicode
        self.binary = binary
        self.national = national

    def _extend(self, spec):
        """Extend a string-type declaration with standard SQL CHARACTER SET /
        COLLATE annotations and MySQL specific extensions.
        """
        
        if self.charset:
            charset = 'CHARACTER SET %s' % self.charset
        elif self.ascii:
            charset = 'ASCII'
        elif self.unicode:
            charset = 'UNICODE'
        else:
            charset = None

        if self.collation:
            collation = 'COLLATE %s' % self.collation
        elif self.binary:
            collation = 'BINARY'
        else:
            collation = None
            
        if self.national:
            # NATIONAL (aka NCHAR/NVARCHAR) trumps charsets.
            return ' '.join([c for c in ('NATIONAL', spec, collation)
                             if c is not None])
        return ' '.join([c for c in (spec, charset, collation)
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
                           ','.join(['%s=%s' % (k, params[k]) for k in params]))


class MSNumeric(sqltypes.Numeric, _NumericType):
    """MySQL NUMERIC type."""
    
    def __init__(self, precision=10, length=2, asdecimal=True, **kw):
        """Construct a NUMERIC.

        precision
          Total digits in this number.  If length and precision are both
          None, values are stored to limits allowed by the server.

        length
          The number of digits after the decimal point.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        _NumericType.__init__(self, **kw)
        sqltypes.Numeric.__init__(self, precision, length, asdecimal=asdecimal)
        
    def get_col_spec(self):
        if self.precision is None:
            return self._extend("NUMERIC")
        else:
            return self._extend("NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if not self.asdecimal:
            def process(value):
                if isinstance(value, util.decimal_type):
                    return float(value)
                else:
                    return value
            return process
        else:
            return None
            


class MSDecimal(MSNumeric):
    """MySQL DECIMAL type."""

    def __init__(self, precision=10, length=2, asdecimal=True, **kw):
        """Construct a DECIMAL.

        precision
          Total digits in this number.  If length and precision are both None,
          values are stored to limits allowed by the server.

        length
          The number of digits after the decimal point.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        super(MSDecimal, self).__init__(precision, length, asdecimal=asdecimal, **kw)
    
    def get_col_spec(self):
        if self.precision is None:
            return self._extend("DECIMAL")
        elif self.length is None:
            return self._extend("DECIMAL(%(precision)s)" % {'precision': self.precision})
        else:
            return self._extend("DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})


class MSDouble(MSNumeric):
    """MySQL DOUBLE type."""

    def __init__(self, precision=10, length=2, asdecimal=True, **kw):
        """Construct a DOUBLE.

        precision
          Total digits in this number.  If length and precision are both None,
          values are stored to limits allowed by the server.

        length
          The number of digits after the decimal point.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        if ((precision is None and length is not None) or
            (precision is not None and length is None)):
            raise exceptions.ArgumentError("You must specify both precision and length or omit both altogether.")
        super(MSDouble, self).__init__(precision, length, asdecimal=asdecimal, **kw)

    def get_col_spec(self):
        if self.precision is not None and self.length is not None:
            return self._extend("DOUBLE(%(precision)s, %(length)s)" %
                                {'precision': self.precision,
                                 'length' : self.length})
        else:
            return self._extend('DOUBLE')


class MSFloat(sqltypes.Float, _NumericType):
    """MySQL FLOAT type."""

    def __init__(self, precision=10, length=None, asdecimal=False, **kw):
        """Construct a FLOAT.
          
        precision
          Total digits in this number.  If length and precision are both None,
          values are stored to limits allowed by the server.

        length
          The number of digits after the decimal point.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        if length is not None:
            self.length=length
        _NumericType.__init__(self, **kw)
        sqltypes.Float.__init__(self, precision, asdecimal=asdecimal)

    def get_col_spec(self):
        if hasattr(self, 'length') and self.length is not None:
            return self._extend("FLOAT(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})
        elif self.precision is not None:
            return self._extend("FLOAT(%(precision)s)" % {'precision': self.precision})
        else:
            return self._extend("FLOAT")

    def bind_processor(self, dialect):
        return None


class MSInteger(sqltypes.Integer, _NumericType):
    """MySQL INTEGER type."""

    def __init__(self, length=None, **kw):
        """Construct an INTEGER.

        length
          Optional, maximum display width for this number.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        self.length = length
        _NumericType.__init__(self, **kw)
        sqltypes.Integer.__init__(self)

    def get_col_spec(self):
        if self.length is not None:
            return self._extend("INTEGER(%(length)s)" % {'length': self.length})
        else:
            return self._extend("INTEGER")


class MSBigInteger(MSInteger):
    """MySQL BIGINTEGER type."""

    def __init__(self, length=None, **kw):
        """Construct a BIGINTEGER.

        length
          Optional, maximum display width for this number.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        super(MSBigInteger, self).__init__(length, **kw)

    def get_col_spec(self):
        if self.length is not None:
            return self._extend("BIGINT(%(length)s)" % {'length': self.length})
        else:
            return self._extend("BIGINT")


class MSTinyInteger(MSInteger):
    """MySQL TINYINT type."""

    def __init__(self, length=None, **kw):
        """Construct a TINYINT.

        Note: following the usual MySQL conventions, TINYINT(1) columns
        reflected during Table(..., autoload=True) are treated as
        Boolean columns.

        length
          Optional, maximum display width for this number.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        super(MSTinyInteger, self).__init__(length, **kw)

    def get_col_spec(self):
        if self.length is not None:
            return self._extend("TINYINT(%s)" % self.length)
        else:
            return self._extend("TINYINT")


class MSSmallInteger(sqltypes.Smallinteger, _NumericType):
    """MySQL SMALLINTEGER type."""

    def __init__(self, length=None, **kw):
        """Construct a SMALLINTEGER.

        length
          Optional, maximum display width for this number.

        unsigned
          Optional.

        zerofill
          Optional. If true, values will be stored as strings left-padded with
          zeros. Note that this does not effect the values returned by the
          underlying database API, which continue to be numeric.
        """

        self.length = length
        _NumericType.__init__(self, **kw)
        sqltypes.Smallinteger.__init__(self, length)

    def get_col_spec(self):
        if self.length is not None:
            return self._extend("SMALLINT(%(length)s)" % {'length': self.length})
        else:
            return self._extend("SMALLINT")


class MSBit(sqltypes.TypeEngine):
    """MySQL BIT type.

    This type is for MySQL 5.0.3 or greater for MyISAM, and 5.0.5 or greater for
    MyISAM, MEMORY, InnoDB and BDB.  For older versions, use a MSTinyInteger(1)
    type.  
    """
    
    def __init__(self, length=None):
        self.length = length
 
    def result_processor(self, dialect):
        """Convert a MySQL's 64 bit, variable length binary string to a long."""
        def process(value):
            if value is not None:
                v = 0L
                for i in map(ord, value):
                    v = v << 8 | i
                value = v
            return value
        return process
        
    def get_col_spec(self):
        if self.length is not None:
            return "BIT(%s)" % self.length
        else:
            return "BIT"


class MSDateTime(sqltypes.DateTime):
    """MySQL DATETIME type."""

    def get_col_spec(self):
        return "DATETIME"


class MSDate(sqltypes.Date):
    """MySQL DATE type."""

    def get_col_spec(self):
        return "DATE"


class MSTime(sqltypes.Time):
    """MySQL TIME type."""

    def get_col_spec(self):
        return "TIME"

    def result_processor(self, dialect):
        def process(value):
            # convert from a timedelta value
            if value is not None:
                return datetime.time(value.seconds/60/60, value.seconds/60%60, value.seconds - (value.seconds/60*60))
            else:
                return None
        return process

class MSTimeStamp(sqltypes.TIMESTAMP):
    """MySQL TIMESTAMP type.

    To signal the orm to automatically re-select modified rows to retrieve
    the updated timestamp, add a PassiveDefault to your column specification::

        from sqlalchemy.databases import mysql
        Column('updated', mysql.MSTimeStamp,
               PassiveDefault(sql.text('CURRENT_TIMESTAMP')))

    The full range of MySQL 4.1+ TIMESTAMP defaults can be specified in
    the PassiveDefault::

        PassiveDefault(sql.text('CURRENT TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    """

    def get_col_spec(self):
        return "TIMESTAMP"


class MSYear(sqltypes.TypeEngine):
    """MySQL YEAR type, for single byte storage of years 1901-2155."""

    def __init__(self, length=None):
        self.length = length

    def get_col_spec(self):
        if self.length is None:
            return "YEAR"
        else:
            return "YEAR(%s)" % self.length

class MSText(_StringType, sqltypes.TEXT):
    """MySQL TEXT type, for text up to 2^16 characters.""" 
    
    def __init__(self, length=None, **kwargs):
        """Construct a TEXT.
        
        length
          Optional, if provided the server may optimize storage by
          subsitituting the smallest TEXT type sufficient to store
          ``length`` characters.

        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        national
          Optional. If true, use the server's configured national
          character set.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """

        _StringType.__init__(self, **kwargs)
        sqltypes.TEXT.__init__(self, length,
                               kwargs.get('convert_unicode', False))

    def get_col_spec(self):
        if self.length:
            return self._extend("TEXT(%d)" % self.length)
        else:
            return self._extend("TEXT")
            

class MSTinyText(MSText):
    """MySQL TINYTEXT type, for text up to 2^8 characters.""" 

    def __init__(self, **kwargs):
        """Construct a TINYTEXT.
        
        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        national
          Optional. If true, use the server's configured national
          character set.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """

        super(MSTinyText, self).__init__(**kwargs)

    def get_col_spec(self):
        return self._extend("TINYTEXT")


class MSMediumText(MSText):
    """MySQL MEDIUMTEXT type, for text up to 2^24 characters.""" 

    def __init__(self, **kwargs):
        """Construct a MEDIUMTEXT.
        
        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        national
          Optional. If true, use the server's configured national
          character set.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """

        super(MSMediumText, self).__init__(**kwargs)

    def get_col_spec(self):
        return self._extend("MEDIUMTEXT")


class MSLongText(MSText):
    """MySQL LONGTEXT type, for text up to 2^32 characters.""" 

    def __init__(self, **kwargs):
        """Construct a LONGTEXT.
        
        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        national
          Optional. If true, use the server's configured national
          character set.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """

        super(MSLongText, self).__init__(**kwargs)

    def get_col_spec(self):
        return self._extend("LONGTEXT")


class MSString(_StringType, sqltypes.String):
    """MySQL VARCHAR type, for variable-length character data."""

    def __init__(self, length=None, **kwargs):
        """Construct a VARCHAR.
        
        length
          Maximum data length, in characters.

        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        national
          Optional. If true, use the server's configured national
          character set.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """

        _StringType.__init__(self, **kwargs)
        sqltypes.String.__init__(self, length,
                                 kwargs.get('convert_unicode', False))

    def get_col_spec(self):
        if self.length:
            return self._extend("VARCHAR(%d)" % self.length)
        else:
            return self._extend("TEXT")


class MSChar(_StringType, sqltypes.CHAR):
    """MySQL CHAR type, for fixed-length character data."""
    
    def __init__(self, length, **kwargs):
        """Construct an NCHAR.
        
        length
          Maximum data length, in characters.

        binary
          Optional, use the default binary collation for the national character
          set.  This does not affect the type of data stored, use a BINARY
          type for binary data.

        collation
          Optional, request a particular collation.  Must be compatibile
          with the national character set.
        """
        _StringType.__init__(self, **kwargs)
        sqltypes.CHAR.__init__(self, length,
                               kwargs.get('convert_unicode', False))

    def get_col_spec(self):
        return self._extend("CHAR(%(length)s)" % {'length' : self.length})


class MSNVarChar(_StringType, sqltypes.String):
    """MySQL NVARCHAR type.

    For variable-length character data in the server's configured national
    character set.
    """

    def __init__(self, length=None, **kwargs):
        """Construct an NVARCHAR.
        
        length
          Maximum data length, in characters.

        binary
          Optional, use the default binary collation for the national character
          set.  This does not affect the type of data stored, use a VARBINARY
          type for binary data.

        collation
          Optional, request a particular collation.  Must be compatibile
          with the national character set.
        """

        kwargs['national'] = True
        _StringType.__init__(self, **kwargs)
        sqltypes.String.__init__(self, length,
                                 kwargs.get('convert_unicode', False))

    def get_col_spec(self):
        # We'll actually generate the equiv. "NATIONAL VARCHAR" instead
        # of "NVARCHAR".
        return self._extend("VARCHAR(%(length)s)" % {'length': self.length})
    

class MSNChar(_StringType, sqltypes.CHAR):
    """MySQL NCHAR type.

    For fixed-length character data in the server's configured national
    character set.
    """

    def __init__(self, length=None, **kwargs):
        """Construct an NCHAR.  Arguments are:

        length
          Maximum data length, in characters.

        binary
          Optional, request the default binary collation for the
          national character set.

        collation
          Optional, request a particular collation.  Must be compatibile
          with the national character set.
        """

        kwargs['national'] = True
        _StringType.__init__(self, **kwargs)
        sqltypes.CHAR.__init__(self, length,
                               kwargs.get('convert_unicode', False))
    def get_col_spec(self):
        # We'll actually generate the equiv. "NATIONAL CHAR" instead of "NCHAR".
        return self._extend("CHAR(%(length)s)" % {'length': self.length})


class _BinaryType(sqltypes.Binary):
    """Base for MySQL binary types."""

    def get_col_spec(self):
        if self.length:
            return "BLOB(%d)" % self.length
        else:
            return "BLOB"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            else:
                return buffer(value)
        return process

class MSVarBinary(_BinaryType):
    """MySQL VARBINARY type, for variable length binary data."""

    def __init__(self, length=None, **kw):
        """Construct a VARBINARY.  Arguments are:

        length
          Maximum data length, in bytes.
        """
        super(MSVarBinary, self).__init__(length, **kw)

    def get_col_spec(self):
        if self.length:
            return "VARBINARY(%d)" % self.length
        else:
            return "BLOB"


class MSBinary(_BinaryType):
    """MySQL BINARY type, for fixed length binary data"""

    def __init__(self, length=None, **kw):
        """Construct a BINARY.  This is a fixed length type, and short
        values will be right-padded with a server-version-specific
        pad value.

        length
          Maximum data length, in bytes.  If length is not specified, this
          will generate a BLOB.  This usage is deprecated.
        """

        super(MSBinary, self).__init__(length, **kw)

    def get_col_spec(self):
        if self.length:
            return "BINARY(%d)" % self.length
        else:
            return "BLOB"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            else:
                return buffer(value)
        return process

class MSBlob(_BinaryType):
    """MySQL BLOB type, for binary data up to 2^16 bytes""" 

    def __init__(self, length=None, **kw):
        """Construct a BLOB.  Arguments are:

        length
          Optional, if provided the server may optimize storage by
          subsitituting the smallest TEXT type sufficient to store
          ``length`` characters.
        """

        super(MSBlob, self).__init__(length, **kw)

    def get_col_spec(self):
        if self.length:
            return "BLOB(%d)" % self.length
        else:
            return "BLOB"
    
    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            else:
                return buffer(value)
        return process
        
    def __repr__(self):
        return "%s()" % self.__class__.__name__


class MSTinyBlob(MSBlob):
    """MySQL TINYBLOB type, for binary data up to 2^8 bytes.""" 

    def get_col_spec(self):
        return "TINYBLOB"


class MSMediumBlob(MSBlob): 
    """MySQL MEDIUMBLOB type, for binary data up to 2^24 bytes."""

    def get_col_spec(self):
        return "MEDIUMBLOB"


class MSLongBlob(MSBlob):
    """MySQL LONGBLOB type, for binary data up to 2^32 bytes."""

    def get_col_spec(self):
        return "LONGBLOB"


class MSEnum(MSString):
    """MySQL ENUM type."""
    
    def __init__(self, *enums, **kw):
        """
        Construct an ENUM.

        Example:

          Column('myenum', MSEnum("'foo'", "'bar'", "'baz'"))

        Arguments are:
        
        enums
          The range of valid values for this ENUM.  Values will be used
          exactly as they appear when generating schemas

        strict
          Defaults to False: ensure that a given value is in this ENUM's
          range of permissible values when inserting or updating rows.
          Note that MySQL will not raise a fatal error if you attempt to
          store an out of range value- an alternate value will be stored
          instead.  (See MySQL ENUM documentation.)

        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """
        
        self.__ddl_values = enums

        strip_enums = []
        for a in enums:
            if a[0:1] == '"' or a[0:1] == "'":
                a = a[1:-1]
            strip_enums.append(a)
            
        self.enums = strip_enums
        self.strict = kw.pop('strict', False)
        length = max([len(v) for v in strip_enums])
        super(MSEnum, self).__init__(length, **kw)

    def bind_processor(self, dialect):
        super_convert = super(MSEnum, self).bind_processor(dialect)
        def process(value):
            if self.strict and value is not None and value not in self.enums:
                raise exceptions.InvalidRequestError('"%s" not a valid value for '
                                                     'this enum' % value)
            if super_convert:
                return super_convert(value)
            else:
                return value
        return process
        
    def get_col_spec(self):
        return self._extend("ENUM(%s)" % ",".join(self.__ddl_values))


class MSSet(MSString):
    """MySQL SET type."""
    
    def __init__(self, *values, **kw):
        """Construct a SET.

        Example::

          Column('myset', MSSet("'foo'", "'bar'", "'baz'"))

        Arguments are:
        
        values
          The range of valid values for this SET.  Values will be used
          exactly as they appear when generating schemas.

        charset
          Optional, a column-level character set for this string
          value.  Takes precendence to 'ascii' or 'unicode' short-hand.

        collation
          Optional, a column-level collation for this string value.
          Takes precedence to 'binary' short-hand.

        ascii
          Defaults to False: short-hand for the ``latin1`` character set,
          generates ASCII in schema.

        unicode
          Defaults to False: short-hand for the ``ucs2`` character set,
          generates UNICODE in schema.

        binary
          Defaults to False: short-hand, pick the binary collation type
          that matches the column's character set.  Generates BINARY in
          schema.  This does not affect the type of data stored, only the
          collation of character data.
        """
        
        self.__ddl_values = values

        strip_values = []
        for a in values:
            if a[0:1] == '"' or a[0:1] == "'":
                a = a[1:-1]
            strip_values.append(a)
            
        self.values = strip_values
        length = max([len(v) for v in strip_values] + [0])
        super(MSSet, self).__init__(length, **kw)

    def result_processor(self, dialect):
        def process(value):
            # The good news:
            #   No ',' quoting issues- commas aren't allowed in SET values
            # The bad news:
            #   Plenty of driver inconsistencies here.
            if isinstance(value, util.set_types):
                # ..some versions convert '' to an empty set
                if not value:
                    value.add('')
                # ..some return sets.Set, even for pythons that have __builtin__.set
                if not isinstance(value, util.Set):
                    value = util.Set(value)
                return value
            # ...and some versions return strings
            if value is not None:
                return util.Set(value.split(','))
            else:
                return value
        return process
        
    def bind_processor(self, dialect):
        super_convert = super(MSSet, self).bind_processor(dialect)
        def process(value):
            if value is None or isinstance(value, (int, long, basestring)):
                pass
            else:
                if None in value:
                    value = util.Set(value)
                    value.remove(None)
                    value.add('')
                value = ','.join(value)
            if super_convert:
                return super_convert(value)
            else:
                return value
        return process
        
    def get_col_spec(self):
        return self._extend("SET(%s)" % ",".join(self.__ddl_values))


class MSBoolean(sqltypes.Boolean):
    """MySQL BOOLEAN type."""

    def get_col_spec(self):
        return "BOOL"

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

colspecs = {
    sqltypes.Integer: MSInteger,
    sqltypes.Smallinteger: MSSmallInteger,
    sqltypes.Numeric: MSNumeric,
    sqltypes.Float: MSFloat,
    sqltypes.DateTime: MSDateTime,
    sqltypes.Date: MSDate,
    sqltypes.Time: MSTime,
    sqltypes.String: MSString,
    sqltypes.Binary: MSBlob,
    sqltypes.Boolean: MSBoolean,
    sqltypes.TEXT: MSText,
    sqltypes.CHAR: MSChar,
    sqltypes.NCHAR: MSNChar,
    sqltypes.TIMESTAMP: MSTimeStamp,
    sqltypes.BLOB: MSBlob,
    _BinaryType: _BinaryType,
}

# Everything 3.23 through 5.1 excepting OpenGIS types.
ischema_names = {
    'bigint': MSBigInteger,
    'binary': MSBinary,
    'bit': MSBit,
    'blob': MSBlob,    
    'boolean':MSBoolean,
    'char': MSChar,
    'date': MSDate,
    'datetime': MSDateTime,
    'decimal': MSDecimal,
    'double': MSDouble,
    'enum': MSEnum,
    'fixed': MSDecimal,
    'float': MSFloat,
    'int': MSInteger,
    'integer': MSInteger,
    'longblob': MSLongBlob,
    'longtext': MSLongText,
    'mediumblob': MSMediumBlob,
    'mediumint': MSInteger,
    'mediumtext': MSMediumText,
    'nchar': MSNChar,
    'nvarchar': MSNVarChar,
    'numeric': MSNumeric,
    'set': MSSet,
    'smallint': MSSmallInteger,
    'text': MSText,
    'time': MSTime,
    'timestamp': MSTimeStamp,
    'tinyblob': MSTinyBlob,
    'tinyint': MSTinyInteger,
    'tinytext': MSTinyText,
    'varbinary': MSVarBinary,
    'varchar': MSString,
    'year': MSYear,
}

def descriptor():
    return {'name':'mysql',
    'description':'MySQL',
    'arguments':[
        ('username',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}


class MySQLExecutionContext(default.DefaultExecutionContext):
    _my_is_select = re.compile(r'\s*(?:SELECT|SHOW|DESCRIBE|XA +RECOVER)',
                               re.I | re.UNICODE)

    def post_exec(self):
        if self.compiled.isinsert and not self.executemany:
            if (not len(self._last_inserted_ids) or
                self._last_inserted_ids[0] is None):
                self._last_inserted_ids = ([self.cursor.lastrowid] +
                                           self._last_inserted_ids[1:])
            
    def is_select(self):
        return SELECT_RE.match(self.statement)

    def should_autocommit(self):
        return AUTOCOMMIT_RE.match(self.statement)


class MySQLDialect(default.DefaultDialect):
    """Details of the MySQL dialect.  Not used directly in application code."""

    supports_alter = True
    supports_unicode_statements = False
    # identifiers are 64, however aliases can be 255...
    max_identifier_length = 255
    supports_sane_rowcount = True

    def __init__(self, use_ansiquotes=False, **kwargs):
        self.use_ansiquotes = use_ansiquotes
        kwargs.setdefault('default_paramstyle', 'format')
        if self.use_ansiquotes:
            self.preparer = MySQLANSIIdentifierPreparer
        else:
            self.preparer = MySQLIdentifierPreparer
        default.DefaultDialect.__init__(self, **kwargs)

    def dbapi(cls):
        import MySQLdb as mysql
        return mysql
    dbapi = classmethod(dbapi)
    
    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'db', 'user', 'passwd', 'port'])
        opts.update(url.query)

        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'connect_timeout', int)
        util.coerce_kw_type(opts, 'client_flag', int)
        util.coerce_kw_type(opts, 'local_infile', int)
        # Note: using either of the below will cause all strings to be returned
        # as Unicode, both in raw SQL operations and with column types like
        # String and MSString.
        util.coerce_kw_type(opts, 'use_unicode', bool)   
        util.coerce_kw_type(opts, 'charset', str)

        # Rich values 'cursorclass' and 'conv' are not supported via
        # query string.
        
        ssl = {}
        for key in ['ssl_ca', 'ssl_key', 'ssl_cert', 'ssl_capath', 'ssl_cipher']:
            if key in opts:
                ssl[key[4:]] = opts[key]
                util.coerce_kw_type(ssl, key[4:], str)
                del opts[key]
        if ssl:
            opts['ssl'] = ssl
        
        # FOUND_ROWS must be set in CLIENT_FLAGS to enable
        # supports_sane_rowcount.
        client_flag = opts.get('client_flag', 0)
        if self.dbapi is not None:
            try:
                import MySQLdb.constants.CLIENT as CLIENT_FLAGS
                client_flag |= CLIENT_FLAGS.FOUND_ROWS
            except:
                pass
            opts['client_flag'] = client_flag
        return [[], opts]

    def create_execution_context(self, connection, **kwargs):
        return MySQLExecutionContext(self, connection, **kwargs)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def compiler(self, statement, bindparams, **kwargs):
        return MySQLCompiler(statement, bindparams, dialect=self, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return MySQLSchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return MySQLSchemaDropper(self, *args, **kwargs)

    def do_executemany(self, cursor, statement, parameters, context=None):
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount
    
    def supports_unicode_statements(self):
        return True
                
    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_commit(self, connection):
        """Execute a COMMIT."""

        # COMMIT/ROLLBACK were introduced in 3.23.15.
        # Yes, we have at least one user who has to talk to these old versions!
        #
        # Ignore commit/rollback if support isn't present, otherwise even basic
        # operations via autocommit fail.
        try:
            connection.commit()
        except:
            if self._server_version_info(connection) < (3, 23, 15):
                args = sys.exc_info()[1].args
                if args and args[0] == 1064:
                    return
            raise

    def do_rollback(self, connection):
        """Execute a ROLLBACK."""
        
        try:
            connection.rollback()
        except:
            if self._server_version_info(connection) < (3, 23, 15):
                args = sys.exc_info()[1].args
                if args and args[0] == 1064:
                    return
            raise

    def do_begin_twophase(self, connection, xid):
        connection.execute("XA BEGIN %s", xid)

    def do_prepare_twophase(self, connection, xid):
        connection.execute("XA END %s", xid)
        connection.execute("XA PREPARE %s", xid)

    def do_rollback_twophase(self, connection, xid, is_prepared=True,
                             recover=False):
        if not is_prepared:
            connection.execute("XA END %s", xid)
        connection.execute("XA ROLLBACK %s", xid)

    def do_commit_twophase(self, connection, xid, is_prepared=True,
                           recover=False):
        if not is_prepared:
            self.do_prepare_twophase(connection, xid)
        connection.execute("XA COMMIT %s", xid)
    
    def do_recover_twophase(self, connection):
        resultset = connection.execute("XA RECOVER")
        return [row['data'][0:row['gtrid_length']] for row in resultset]

    def do_ping(self, connection):
        connection.ping()

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.OperationalError) and \
               e.args[0] in (2006, 2013, 2014, 2045, 2055)

    def get_default_schema_name(self, connection):
        try:
            return self._default_schema_name
        except AttributeError:
            name = self._default_schema_name = \
              connection.execute('SELECT DATABASE()').scalar()
            return name

    def table_names(self, connection, schema):
        """Return a Unicode SHOW TABLES from a given schema."""

        charset = self._detect_charset(connection)
        rp = connection.execute("SHOW TABLES FROM %s" %
            self.identifier_preparer.quote_identifier(schema))
        return [row[0] for row in _compat_fetchall(rp, charset=charset)]

    def has_table(self, connection, table_name, schema=None):
        # SHOW TABLE STATUS LIKE and SHOW TABLES LIKE do not function properly
        # on macosx (and maybe win?) with multibyte table names.
        #
        # TODO: if this is not a problem on win, make the strategy swappable
        # based on platform.  DESCRIBE is slower.

        # [ticket:726]
        # full_name = self.identifier_preparer.format_table(table, use_schema=True)

        full_name = '.'.join(self.identifier_preparer._quote_free_identifiers(
            schema, table_name))
        
        st = "DESCRIBE %s" % full_name
        rs = None
        try:
            try:
                rs = connection.execute(st)
                have = rs.rowcount > 0
                rs.close()
                return have
            except exceptions.SQLError, e:
                if e.orig.args[0] == 1146:
                    return False
                raise
        finally:
            if rs:
                rs.close()

    def server_version_info(self, connection):
        """A tuple of the database server version.

        Formats the remote server version as a tuple of version values,
        e.g. ``(5, 0, 44)``.  If there are strings in the version number
        they will be in the tuple too, so don't count on these all being
        ``int`` values.

        This is a fast check that does not require a round trip.  It is also
        cached per-Connection.
        """

        try:
            return connection.properties['_mysql_server_version_info']
        except KeyError:
            version = connection.properties['_mysql_server_version_info'] = \
              self._server_version_info(connection.connection.connection)
            return version

    def _server_version_info(self, dbapi_con):
        """Convert a MySQL-python server_info string into a tuple."""

        version = []
        for n in dbapi_con.get_server_info().split('.'):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    # @deprecated
    def get_version_info(self, connectable):
        """A tuple of the database server version.

        Deprecated, use ``server_version_info()``.
        """

        if isinstance(connectable, engine_base.Engine):
            connectable = connectable.contextual_connect()

        return self.server_version_info(connectable)
    get_version_info = util.deprecated(get_version_info)

    def reflecttable(self, connection, table, include_columns):
        """Load column definitions from the server."""

        charset = self._detect_charset(connection)
        casing = self._detect_casing(connection, charset)
        # is this really needed?
        if casing == 1 and table.name != table.name.lower():
            table.name = table.name.lower()
            lc_alias = schema._get_table_key(table.name, table.schema)
            table.metadata.tables[lc_alias] = table

        sql = self._show_create_table(connection, table, charset)

        try:
            reflector = self.reflector
        except AttributeError:
            self.reflector = reflector = \
                MySQLSchemaReflector(self.identifier_preparer)

        reflector.reflect(connection, table, sql, charset, only=include_columns)

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""

        # Allow user override, won't sniff if force_charset is set.
        if 'force_charset' in connection.properties:
            return connection.properties['force_charset']

        # Note: MySQL-python 1.2.1c7 seems to ignore changes made
        # on a connection via set_character_set()
        if self.server_version_info(connection) < (4, 1, 0):
            try:
                return connection.connection.character_set_name()
            except AttributeError:
                # < 1.2.1 final MySQL-python drivers have no charset support.
                # a query is needed.
                pass

        # Prefer 'character_set_results' for the current connection over the
        # value in the driver.  SET NAMES or individual variable SETs will
        # change the charset without updating the driver's view of the world.
        # 
        # If it's decided that issuing that sort of SQL leaves you SOL, then
        # this can prefer the driver value.
        rs = connection.execute("SHOW VARIABLES LIKE 'character_set%%'")
        opts = dict([(row[0], row[1]) for row in _compat_fetchall(rs)])

        if 'character_set_results' in opts:
            return opts['character_set_results']
        try:
            return connection.connection.character_set_name()
        except AttributeError:
            # Still no charset on < 1.2.1 final...
            if 'character_set' in opts:
                return opts['character_set']
            else:
                warnings.warn(RuntimeWarning(
                    "Could not detect the connection character set with this "
                    "combination of MySQL server and MySQL-python. "
                    "MySQL-python >= 1.2.2 is recommended.  Assuming latin1."))
                return 'latin1'

    def _detect_casing(self, connection, charset=None):
        """Sniff out identifier case sensitivity.

        Cached per-connection. This value can not change without a server
        restart.
        """

        # http://dev.mysql.com/doc/refman/5.0/en/name-case-sensitivity.html

        try:
            return connection.properties['lower_case_table_names']
        except KeyError:
            row = _compat_fetchone(connection.execute(
                    "SHOW VARIABLES LIKE 'lower_case_table_names'"),
                                   charset=charset)
            if not row:
                cs = 0
            else:
                # 4.0.15 returns OFF or ON according to [ticket:489]
                # 3.23 doesn't, 4.0.27 doesn't..
                if row[1] == 'OFF':
                    cs = 0
                elif row[1] == 'ON':
                    cs = 1
                else:
                    cs = int(row[1])
                row.close()
            connection.properties['lower_case_table_names'] = cs
            return cs

    def _detect_collations(self, connection, charset=None):
        """Pull the active COLLATIONS list from the server.

        Cached per-connection.
        """
        
        try:
            return connection.properties['collations']
        except KeyError:
            collations = {}
            if self.server_version_info(connection) < (4, 1, 0):
                pass
            else:
                rs = connection.execute('SHOW COLLATION')
                for row in _compat_fetchall(rs, charset):
                    collations[row[0]] = row[1]
            connection.properties['collations'] = collations
            return collations

    def _show_create_table(self, connection, table, charset=None,
                           full_name=None):
        """Run SHOW CREATE TABLE for a ``Table``."""

        if full_name is None:
            full_name = self.identifier_preparer.format_table(table)
        st = "SHOW CREATE TABLE %s" % full_name

        rp = None
        try:
            try:
                rp = connection.execute(st)
            except exceptions.SQLError, e:
                if e.orig.args[0] == 1146:
                    raise exceptions.NoSuchTableError(full_name)
                else:
                    raise
            row = _compat_fetchone(rp, charset=charset)
            if not row:
                raise exceptions.NoSuchTableError(full_name)
            return row[1].strip()
        finally:
            if rp:
                rp.close()

        return sql


class _MySQLPythonRowProxy(object):
    """Return consistent column values for all versions of MySQL-python (esp. alphas) and Unicode settings."""

    # Some MySQL-python versions can return some columns as
    # sets.Set(['value']) (seriously) but thankfully that doesn't
    # seem to come up in DDL queries.

    def __init__(self, rowproxy, charset):
        self.rowproxy = rowproxy
        self.charset = charset
    def __getitem__(self, index):
        item = self.rowproxy[index]
        if isinstance(item, _array):
            item = item.tostring()
        if self.charset and isinstance(item, str):
            return item.decode(self.charset)
        else:
            return item
    def __getattr__(self, attr):
        item = getattr(self.rowproxy, attr)
        if isinstance(item, _array):
            item = item.tostring()
        if self.charset and isinstance(item, str):
            return item.decode(self.charset)
        else:
            return item


class MySQLCompiler(compiler.DefaultCompiler):
    operators = compiler.DefaultCompiler.operators.copy()
    operators.update(
        {
            sql_operators.concat_op: \
              lambda x, y: "concat(%s, %s)" % (x, y),
            sql_operators.mod: '%%'
        }
    )

    def visit_cast(self, cast, **kwargs):
        if isinstance(cast.type, (sqltypes.Date, sqltypes.Time,
                                  sqltypes.DateTime)):
            return super(MySQLCompiler, self).visit_cast(cast, **kwargs)
        else:
            # so just skip the CAST altogether for now.
            # TODO: put whatever MySQL does for CAST here.
            return self.process(cast.clause)

    def get_select_precolumns(self, select):
        if isinstance(select._distinct, basestring):
            return select._distinct.upper() + " "
        elif select._distinct:
            return "DISTINCT "
        else:
            return ""

    def for_update_clause(self, select):
        if select.for_update == 'read':
             return ' LOCK IN SHARE MODE'
        else:
            return super(MySQLCompiler, self).for_update_clause(select)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                # straight from the MySQL docs, I kid you not
                text += " \n LIMIT 18446744073709551615"
            text += " OFFSET " + str(select._offset)
        return text
        

# ug.  "InnoDB needs indexes on foreign keys and referenced keys [...].
#       Starting with MySQL 4.1.2, these indexes are created automatically.
#       In older versions, the indexes must be created explicitly or the
#       creation of foreign key constraints fails."

class MySQLSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, first_pk=False):
        """Builds column DDL."""
        
        colspec = [self.preparer.format_column(column),
                   column.type.dialect_impl(self.dialect).get_col_spec()]

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append('DEFAULT ' + default)

        if not column.nullable:
            colspec.append('NOT NULL')

        if column.primary_key and column.autoincrement:
            try:
                first = [c for c in column.table.primary_key.columns
                         if (c.autoincrement and
                             isinstance(c.type, sqltypes.Integer) and
                             not c.foreign_keys)].pop(0)
                if column is first:
                    colspec.append('AUTO_INCREMENT')
            except IndexError:
                pass

        return ' '.join(colspec)

    def post_create_table(self, table):
        """Build table-level CREATE options like ENGINE and COLLATE."""

        table_opts = []
        for k in table.kwargs:
            if k.startswith('mysql_'):
                opt = k[6:].upper()
                joiner = '='
                if opt in ('TABLESPACE', 'DEFAULT CHARACTER SET',
                           'CHARACTER SET', 'COLLATE'):
                    joiner = ' '
                
                table_opts.append(joiner.join((opt, table.kwargs[k])))
        return ' '.join(table_opts)


class MySQLSchemaDropper(compiler.SchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX %s ON %s" %
                    (self.preparer.format_index(index),
                     self.preparer.format_table(index.table)))
        self.execute()

    def drop_foreignkey(self, constraint):
        self.append("ALTER TABLE %s DROP FOREIGN KEY %s" %
                    (self.preparer.format_table(constraint.table),
                     self.preparer.format_constraint(constraint)))
        self.execute()


class MySQLSchemaReflector(object):
    """Parses SHOW CREATE TABLE output."""

    def __init__(self, identifier_preparer):
        """Construct a MySQLSchemaReflector.

        identifier_preparer
          An ANSIIdentifierPreparer type, used to determine the identifier
          quoting style in effect.
        """
        
        self.preparer = identifier_preparer
        self._prep_regexes()

    def reflect(self, connection, table, show_create, charset, only=None):
        """Parse MySQL SHOW CREATE TABLE and fill in a ''Table''.

        show_create
          Unicode output of SHOW CREATE TABLE

        table
          A ''Table'', to be loaded with Columns, Indexes, etc.
          table.name will be set if not already

        charset
          FIXME, some constructed values (like column defaults)
          currently can't be Unicode.  ''charset'' will convert them
          into the connection character set.

        only
           An optional sequence of column names.  If provided, only
           these columns will be reflected, and any keys or constraints
           that include columns outside this set will also be omitted.
           That means that if ``only`` includes only one column in a
           2 part primary key, the entire primary key will be omitted.
        """

        keys, constraints = [], []

        if only:
            only = util.Set(only)

        for line in re.split(r'\r?\n', show_create):
            if line.startswith('  ' + self.preparer.initial_quote):
                self._add_column(table, line, charset, only)
            elif line.startswith(') '):
                self._set_options(table, line)
            elif line.startswith('CREATE '):
                self._set_name(table, line)
            # Not present in real reflection, but may be if loading from a file.
            elif not line:
                pass
            else:
                type_, spec = self.constraints(line)
                if type_ is None:
                    warnings.warn(
                        RuntimeWarning("Unknown schema content: %s" %
                                       repr(line)))
                elif type_ == 'key':
                    keys.append(spec)
                elif type_ == 'constraint':
                    constraints.append(spec)
                else:
                    pass

        self._set_keys(table, keys, only)
        self._set_constraints(table, constraints, connection, only)

    def _set_name(self, table, line):
        """Override a Table name with the reflected name.

        table
          A ``Table``

        line
          The first line of SHOW CREATE TABLE output.
        """

        # Don't override by default.
        if table.name is None:
            table.name = self.name(line)

    def _add_column(self, table, line, charset, only=None):
        spec = self.column(line)
        if not spec:
            warnings.warn(RuntimeWarning(
                "Unknown column definition %s" % line))
            return
        if not spec['full']:
            warnings.warn(RuntimeWarning(
                "Incomplete reflection of column definition %s" % line))

        name, type_, args, notnull = \
              spec['name'], spec['coltype'], spec['arg'], spec['notnull']

        if only and name.lower() not in only:
            self.logger.info("Omitting reflected column %s.%s" %
                             (table.name, name))
            return

        # Convention says that TINYINT(1) columns == BOOLEAN
        if type_ == 'tinyint' and args == '1':
            type_ = 'boolean'
            args = None

        try:
            col_type = ischema_names[type_]
        except KeyError:
            warnings.warn(RuntimeWarning(
                "Did not recognize type '%s' of column '%s'" %
                (type_, name)))
            col_type = sqltypes.NULLTYPE
        
        # Column type positional arguments eg. varchar(32)
        if args is None or args == '':
            type_args = []
        elif args[0] == "'" and args[-1] == "'":
            type_args = self._re_csv_str.findall(args)
        else:
            type_args = [int(v) for v in self._re_csv_int.findall(args)]

        # Column type keyword options
        type_kw = {}
        for kw in ('unsigned', 'zerofill'):
            if spec.get(kw, False):
                type_kw[kw] = True
        for kw in ('charset', 'collate'):
            if spec.get(kw, False):
                type_kw[kw] = spec[kw]

        type_instance = col_type(*type_args, **type_kw)

        col_args, col_kw = [], {}

        # NOT NULL
        if spec.get('notnull', False):
            col_kw['nullable'] = False
            
        # AUTO_INCREMENT
        if spec.get('autoincr', False):
            col_kw['autoincrement'] = True
        elif issubclass(col_type, sqltypes.Integer):
            col_kw['autoincrement'] = False

        # DEFAULT
        default = spec.get('default', None)
        if default is not None and default != 'NULL':
            # Defaults should be in the native charset for the moment
            default = default.decode(charset)
            if type_ == 'timestamp':
                # can't be NULL for TIMESTAMPs
                if (default[0], default[-1]) != ("'", "'"):
                    default = sql.text(default)
            else:
                default = default[1:-1]
            col_args.append(schema.PassiveDefault(default))

        table.append_column(schema.Column(name, type_instance,
                                          *col_args, **col_kw))

    def _set_keys(self, table, keys, only):
        """Add ``Index`` and ``PrimaryKeyConstraint`` items to a ``Table``.

        Most of the information gets dropped here- more is reflected than
        the schema objects can currently represent.

        table
          A ``Table``

        keys
          A sequence of key specifications produced by `constraints`

        only
          Optional `set` of column names.  If provided, keys covering
          columns not in this set will be omitted.
        """
        
        for spec in keys:
            flavor = spec['type']
            col_names = [s[0] for s in spec['columns']]

            if only and not util.Set(col_names).issubset(only):
                if flavor is None:
                    flavor = 'index'
                self.logger.info(
                    "Omitting %s KEY for (%s), key covers ommitted columns." %
                    (flavor, ', '.join(col_names)))
                continue

            constraint = False
            if flavor == 'PRIMARY':
                key = schema.PrimaryKeyConstraint()
                constraint = True
            elif flavor == 'UNIQUE':
                key = schema.Index(spec['name'], unique=True)
            elif flavor in (None, 'FULLTEXT', 'SPATIAL'):
                key = schema.Index(spec['name'])
            else:
                self.logger.info(
                    "Converting unknown KEY type %s to a plain KEY" % flavor)
                key = schema.Index(spec['name'])

            for col in [table.c[name] for name in col_names]:
                key.append_column(col)

            if constraint:
                table.append_constraint(key)

    def _set_constraints(self, table, constraints, connection, only):
        """Apply constraints to a ``Table``."""

        for spec in constraints:
            # only FOREIGN KEYs
            ref_name = spec['table'][-1]
            ref_schema = len(spec['table']) > 1 and spec['table'][-2] or None
        
            loc_names = spec['local']
            if only and not util.Set(loc_names).issubset(only):
                self.logger.info(
                    "Omitting FOREIGN KEY for (%s), key covers ommitted "
                    "columns." % (', '.join(loc_names)))
                continue

            ref_key = schema._get_table_key(ref_name, ref_schema)
            if ref_key in table.metadata.tables:
                ref_table = table.metadata.tables[ref_key]
            else:
                ref_table = schema.Table(
                    ref_name, table.metadata, schema=ref_schema,
                    autoload=True, autoload_with=connection)

            ref_names = spec['foreign']
            if not util.Set(ref_names).issubset(
                util.Set([c.name for c in ref_table.c])):
                raise exceptions.InvalidRequestError(
                    "Foreign key columns (%s) are not present on "
                    "foreign table %s" %
                    (', '.join(ref_names), ref_table.fullname()))
            ref_columns = [ref_table.c[name] for name in ref_names]

            con_kw = {}
            for opt in ('name', 'onupdate', 'ondelete'):
                if spec.get(opt, False):
                    con_kw[opt] = spec[opt]
                    
            key = schema.ForeignKeyConstraint([], [], **con_kw)
            table.append_constraint(key)
            for pair in zip(loc_names, ref_columns):
                key.append_element(*pair)
            
    def _set_options(self, table, line):
        """Apply safe reflected table options to a ``Table``.

        table
          A ``Table``

        line
          The final line of SHOW CREATE TABLE output.
        """

        options = self.table_options(line)
        for nope in ('auto_increment', 'data_directory', 'index_directory'):
            options.pop(nope, None)

        for opt, val in options.items():
            table.kwargs['mysql_%s' % opt] = val

    def _prep_regexes(self):
        """Pre-compile regular expressions."""

        self._re_columns = []
        self._pr_options = []
        self._re_options_util = {}

        _final = self.preparer.final_quote
        
        quotes = dict(zip(('iq', 'fq', 'esc_fq'),
                          [re.escape(s) for s in
                           (self.preparer.initial_quote,
                            _final,
                            self.preparer._escape_identifier(_final))]))

        self._pr_name = _pr_compile(
            r'^CREATE TABLE +'
            r'%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s +\($' % quotes,
            self.preparer._unescape_identifier)

        # `col`,`col2`(32),`col3`(15) DESC
        #
        # Note: ASC and DESC aren't reflected, so we'll punt...
        self._re_keyexprs = _re_compile(
            r'(?:'
            r'(?:%(iq)s((?:%(esc_fq)s|[^%(fq)s])+)%(fq)s)'
            r'(?:\((\d+)\))?(?=\,|$))+' % quotes)

        # 'foo' or 'foo','bar' or 'fo,o','ba''a''r'
        self._re_csv_str = _re_compile(r'\x27(?:\x27\x27|[^\x27])+\x27')

        # 123 or 123,456
        self._re_csv_int = _re_compile(r'\d+')


        # `colname` <type> [type opts]
        #  (NOT NULL | NULL)
        #   DEFAULT ('value' | CURRENT_TIMESTAMP...)
        #   COMMENT 'comment'
        #  COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
        #  STORAGE (DISK|MEMORY)
        self._re_column = _re_compile(
            r'  '
            r'%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s +'
            r'(?P<coltype>\w+)'
            r'(?:\((?P<arg>(?:\d+|\d+,\d+|'
              r'(?:\x27(?:\x27\x27|[^\x27])+\x27,?)+))\))?'
            r'(?: +(?P<unsigned>UNSIGNED))?'
            r'(?: +(?P<zerofill>ZEROFILL))?'
            r'(?: +CHARACTER SET +(?P<charset>\w+))?'
            r'(?: +COLLATE +(P<collate>\w+))?'
            r'(?: +(?P<notnull>NOT NULL))?'
            r'(?: +DEFAULT +(?P<default>'
              r'(?:NULL|\x27(?:\x27\x27|[^\x27])+\x27|\w+)'
              r'(?:ON UPDATE \w+)?'
            r'))?'
            r'(?: +(?P<autoincr>AUTO_INCREMENT))?'
            r'(?: +COMMENT +(P<comment>(?:\x27\x27|[^\x27])+))?'
            r'(?: +COLUMN_FORMAT +(?P<colfmt>\w+))?'
            r'(?: +STORAGE +(?P<storage>\w+))?'
            r'(?: +(?P<extra>.*))?'
            r',?$'
            % quotes
            )

        # Fallback, try to parse as little as possible
        self._re_column_loose = _re_compile(
            r'  '
            r'%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s +'
            r'(?P<coltype>\w+)'
            r'(?:\((?P<arg>(?:\d+|\d+,\d+|\x27(?:\x27\x27|[^\x27])+\x27))\))?'
            r'.*?(?P<notnull>NOT NULL)?'
            % quotes
            )

        # (PRIMARY|UNIQUE|FULLTEXT|SPATIAL) INDEX `name` (USING (BTREE|HASH))?
        # (`col` (ASC|DESC)?, `col` (ASC|DESC)?)
        # KEY_BLOCK_SIZE size | WITH PARSER name
        self._re_key = _re_compile(
            r'  '
            r'(?:(?P<type>\S+) )?KEY +'
            r'(?:%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s)?'
            r'(?: +USING +(?P<using>\S+) +)?'
            r' +\((?P<columns>.+?)\)'
            r'(?: +KEY_BLOCK_SIZE +(?P<keyblock>\S+))?'
            r'(?: +WITH PARSER +(?P<parser>\S+))?'
            r',?$'
            % quotes
            )

        # CONSTRAINT `name` FOREIGN KEY (`local_col`)
        # REFERENCES `remote` (`remote_col`)
        # MATCH FULL | MATCH PARTIAL | MATCH SIMPLE
        # ON DELETE CASCADE ON UPDATE RESTRICT
        #
        # unique constraints come back as KEYs
        kw = quotes.copy()
        kw['on'] = 'RESTRICT|CASCASDE|SET NULL|NOACTION'
        self._re_constraint = _re_compile(
            r'  '
            r'CONSTRAINT +'
            r'%(iq)s(?P<name>(?:%(esc_fq)s|[^%(fq)s])+)%(fq)s +'
            r'FOREIGN KEY +'
            r'\((?P<local>[^\)]+?)\) REFERENCES +'
            r'(?P<table>%(iq)s[^%(fq)s]+%(fq)s) +'
            r'\((?P<foreign>[^\)]+?)\)'
            r'(?: +(?P<match>MATCH \w+))?'
            r'(?: +ON DELETE (?P<ondelete>%(on)s))?'
            r'(?: +ON UPDATE (?P<onupdate>%(on)s))?'
            % kw
            )

        # PARTITION
        #
        # punt!
        self._re_partition = _re_compile(
            r'  '
            r'(?:SUB)?PARTITION')

        # Table-level options (COLLATE, ENGINE, etc.)
        for option in ('ENGINE', 'TYPE', 'AUTO_INCREMENT',
                       'AVG_ROW_LENGTH', 'CHARACTER SET',
                       'DEFAULT CHARSET', 'CHECKSUM',
                       'COLLATE', 'DELAY_KEY_WRITE', 'INSERT_METHOD',
                       'MAX_ROWS', 'MIN_ROWS', 'PACK_KEYS', 'ROW_FORMAT',
                       'KEY_BLOCK_SIZE'):
            self._add_option_word(option)

        for option in (('COMMENT', 'DATA_DIRECTORY', 'INDEX_DIRECTORY',
                        'PASSWORD', 'CONNECTION')):
            self._add_option_string(option)

        self._add_option_regex('UNION', r'\([^\)]+\)')
        self._add_option_regex('TABLESPACE', r'.*? STORAGE DISK')
        self._add_option_regex('RAID_TYPE',
          r'\w+\s+RAID_CHUNKS\s*\=\s*\w+RAID_CHUNKSIZE\s*=\s*\w+')
        self._re_options_util['='] = _re_compile(r'\s*=\s*$')

    def _add_option_string(self, directive):
        regex = (r'(?P<directive>%s\s*(?:=\s*)?)'
                 r'(?:\x27.(?P<val>.*?)\x27(?!\x27)\x27)' %
                 re.escape(directive))
        self._pr_options.append(
            _pr_compile(regex, lambda v: v.replace("''", "'")))

    def _add_option_word(self, directive):
        regex = (r'(?P<directive>%s\s*(?:=\s*)?)'
                 r'(?P<val>\w+)' % re.escape(directive))
        self._pr_options.append(_pr_compile(regex))

    def _add_option_regex(self, directive, regex):
        regex = (r'(?P<directive>%s\s*(?:=\s*)?)'
                 r'(?P<val>%s)' % (re.escape(directive), regex))
        self._pr_options.append(_pr_compile(regex))


    def name(self, line):
        """Extract the table name.

        line
          The first line of SHOW CREATE TABLE
        """

        regex, cleanup = self._pr_name
        m = regex.match(line)
        if not m:
            return None
        return cleanup(m.group('name'))

    def column(self, line):
        """Extract column details.

        Falls back to a 'minimal support' variant if full parse fails.

        line
          Any column-bearing line from SHOW CREATE TABLE
        """

        m = self._re_column.match(line)
        if m:
            spec = m.groupdict()
            spec['full'] = True
            return spec
        m = self._re_column_loose.match(line)
        if m:
            spec = m.groupdict()
            spec['full'] = False
            return spec
        return None

    def constraints(self, line):
        """Parse a KEY or CONSTRAINT line.

        line
          A line of SHOW CREATE TABLE output
        """

        # KEY
        m = self._re_key.match(line)
        if m:
            spec = m.groupdict()
            # convert columns into name, length pairs
            spec['columns'] = self._parse_keyexprs(spec['columns'])
            return 'key', spec

        # CONSTRAINT
        m = self._re_constraint.match(line)
        if m:
            spec = m.groupdict()
            spec['table'] = \
              self.preparer.unformat_identifiers(spec['table'])
            spec['local'] = [c[0]
                             for c in self._parse_keyexprs(spec['local'])]
            spec['foreign'] = [c[0]
                               for c in self._parse_keyexprs(spec['foreign'])]
            return 'constraint', spec

        # PARTITION and SUBPARTITION
        m = self._re_partition.match(line)
        if m:
            # Punt!
            return 'partition', line

        # No match.
        return (None, line)
        
    def table_options(self, line):
        """Build a dictionary of all reflected table-level options.

        line
          The final line of SHOW CREATE TABLE output.
        """
        
        options = {}

        if not line or line == ')':
            return options

        r_eq_trim = self._re_options_util['=']

        for regex, cleanup in self._pr_options:
            m = regex.search(line)
            if not m:
                continue
            directive, value = m.group('directive'), m.group('val')
            directive = r_eq_trim.sub('', directive).lower()
            if cleanup:
                value = cleanup(value)
            options[directive] = value

        return options

    def _parse_keyexprs(self, identifiers):
        """Unpack '"col"(2),"col" ASC'-ish strings into components."""

        return self._re_keyexprs.findall(identifiers)

MySQLSchemaReflector.logger = logging.class_logger(MySQLSchemaReflector)


class _MySQLIdentifierPreparer(compiler.IdentifierPreparer):
    """MySQL-specific schema identifier configuration."""

    reserved_words = RESERVED_WORDS
    
    def __init__(self, dialect, **kw):
        super(_MySQLIdentifierPreparer, self).__init__(dialect, **kw)

    def _fold_identifier_case(self, value):
        # TODO: determine MySQL's case folding rules
        #
        # For compatability with sql.text() issued statements, maybe it's best
        # to just leave things as-is.  When lower_case_table_names > 0 it
        # looks a good idea to lc everything, but more importantly the casing
        # of all identifiers in an expression must be consistent.  So for now,
        # just leave everything as-is.
        return value

    def _quote_free_identifiers(self, *ids):
        """Unilaterally identifier-quote any number of strings."""

        return tuple([self.quote_identifier(i) for i in ids if i is not None])


class MySQLIdentifierPreparer(_MySQLIdentifierPreparer):
    """Traditional MySQL-specific schema identifier configuration."""

    def __init__(self, dialect):
        super(MySQLIdentifierPreparer, self).__init__(dialect, initial_quote="`")

    def _escape_identifier(self, value):
        return value.replace('`', '``')

    def _unescape_identifier(self, value):
        return value.replace('``', '`')


class MySQLANSIIdentifierPreparer(_MySQLIdentifierPreparer):
    """ANSI_QUOTES MySQL schema identifier configuration."""

    pass


def _compat_fetchall(rp, charset=None):
    """Proxy result rows to smooth over MySQL-Python driver inconsistencies."""

    return [_MySQLPythonRowProxy(row, charset) for row in rp.fetchall()]

def _compat_fetchone(rp, charset=None):
    """Proxy a result row to smooth over MySQL-Python driver inconsistencies."""

    return _MySQLPythonRowProxy(rp.fetchone(), charset)

def _pr_compile(regex, cleanup=None):
    """Prepare a 2-tuple of compiled regex and callable."""

    return (_re_compile(regex), cleanup)

def _re_compile(regex):
    """Compile a string to regex, I and UNICODE."""

    return re.compile(regex, re.I | re.UNICODE)

dialect = MySQLDialect
dialect.statement_compiler = MySQLCompiler
dialect.schemagenerator = MySQLSchemaGenerator
dialect.schemadropper = MySQLSchemaDropper

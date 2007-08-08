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

For normal SQLAlchemy usage, loading this module is unnescesary.  It will be
loaded on-demand when a MySQL connection is needed.  If you would like to use
one of the MySQL-specific or enhanced column types when creating tables with
your ``Table`` definitions, then you will need to import them from this module::

  from sqlalchemy.databases import mysql

  Table('mytable', metadata,
        Column('id', Integer, primary_key=True),
        Column('ittybittyblob', mysql.MSTinyBlob),
        Column('biggy', mysql.MSBigInteger(unsigned=True)))

If you have problems that seem server related, first check that you are
using the most recent stable MySQL-Python package available.  The Database
Notes page on the wiki at http://sqlalchemy.org is a good resource for timely
information affecting MySQL in SQLAlchemy.
"""

import re, datetime, inspect, warnings, operator, sys
from array import array as _array

from sqlalchemy import sql, schema, ansisql
from sqlalchemy.engine import base as engine_base, default
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions
import sqlalchemy.util as util


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
    """MySQL NUMERIC type"""
    
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

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        if not self.asdecimal and isinstance(value, util.decimal_type):
            return float(value)
        else:
            return value


class MSDecimal(MSNumeric):
    """MySQL DECIMAL type"""

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
    """MySQL DOUBLE type"""

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
    """MySQL FLOAT type"""

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

    def convert_bind_param(self, value, dialect):
        return value


class MSInteger(sqltypes.Integer, _NumericType):
    """MySQL INTEGER type"""

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
    """MySQL BIGINTEGER type"""

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
    """MySQL TINYINT type"""

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
    """MySQL SMALLINTEGER type"""

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
    """MySQL BIT type

    This type is for MySQL 5.0.3 or greater for MyISAM, and 5.0.5 or greater for
    MyISAM, MEMORY, InnoDB and BDB.  For older versions, use a MSTinyInteger(1)
    type.  
    """
    
    def __init__(self, length=None):
        self.length = length
 
    def convert_result_value(self, value, dialect):
        """Converts MySQL's 64 bit, variable length binary string to a long."""

        if value is not None:
            v = 0L
            for i in map(ord, value):
                v = v << 8 | i
            value = v
        return value

    def get_col_spec(self):
        if self.length is not None:
            return "BIT(%s)" % self.length
        else:
            return "BIT"


class MSDateTime(sqltypes.DateTime):
    """MySQL DATETIME type"""

    def get_col_spec(self):
        return "DATETIME"


class MSDate(sqltypes.Date):
    """MySQL DATE type"""

    def get_col_spec(self):
        return "DATE"


class MSTime(sqltypes.Time):
    """MySQL TIME type"""

    def get_col_spec(self):
        return "TIME"

    def convert_result_value(self, value, dialect):
        # convert from a timedelta value
        if value is not None:
            return datetime.time(value.seconds/60/60, value.seconds/60%60, value.seconds - (value.seconds/60*60))
        else:
            return None


class MSTimeStamp(sqltypes.TIMESTAMP):
    """MySQL TIMESTAMP type

    To signal the orm to automatically re-select modified rows to retrieve
    the timestamp, add a PassiveDefault to your column specification::

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
    """MySQL YEAR type, for single byte storage of years 1901-2155"""

    def get_col_spec(self):
        return "YEAR"


class MSText(_StringType, sqltypes.TEXT):
    """MySQL TEXT type, for text up to 2^16 characters""" 
    
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
    """MySQL TINYTEXT type, for text up to 2^8 characters""" 

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
    """MySQL MEDIUMTEXT type, for text up to 2^24 characters""" 

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
    """MySQL LONGTEXT type, for text up to 2^32 characters""" 

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
    """MySQL NVARCHAR type, for variable-length character data in the
    server's configured national character set.
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
    """MySQL NCHAR type, for fixed-length character data in the
    server's configured national character set.
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
    """MySQL binary types"""

    def get_col_spec(self):
        if self.length:
            return "BLOB(%d)" % self.length
        else:
            return "BLOB"

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return buffer(value)


class MSVarBinary(_BinaryType):
    """MySQL VARBINARY type, for variable length binary data"""

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

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return buffer(value)


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

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return buffer(value)

    def __repr__(self):
        return "%s()" % self.__class__.__name__


class MSTinyBlob(MSBlob):
    """MySQL TINYBLOB type, for binary data up to 2^8 bytes""" 

    def get_col_spec(self):
        return "TINYBLOB"


class MSMediumBlob(MSBlob): 
    """MySQL MEDIUMBLOB type, for binary data up to 2^24 bytes"""

    def get_col_spec(self):
        return "MEDIUMBLOB"


class MSLongBlob(MSBlob):
    """MySQL LONGBLOB type, for binary data up to 2^32 bytes"""

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

    def convert_bind_param(self, value, engine): 
        if self.strict and value is not None and value not in self.enums:
            raise exceptions.InvalidRequestError('"%s" not a valid value for '
                                                 'this enum' % value)
        return super(MSEnum, self).convert_bind_param(value, engine)

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

    def convert_result_value(self, value, dialect):
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

    def convert_bind_param(self, value, engine): 
        if value is None or isinstance(value, (int, long, basestring)):
            pass
        else:
            if None in value:
                value = util.Set(value)
                value.remove(None)
                value.add('')
            value = ','.join(value)
        return super(MSSet, self).convert_bind_param(value, engine)

    def get_col_spec(self):
        return self._extend("SET(%s)" % ",".join(self.__ddl_values))


class MSBoolean(sqltypes.Boolean):
    """MySQL BOOLEAN type."""

    def get_col_spec(self):
        return "BOOL"

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        return value and True or False

    def convert_bind_param(self, value, dialect):
        if value is True:
            return 1
        elif value is False:
            return 0
        elif value is None:
            return None
        else:
            return value and True or False


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
    _my_is_select = re.compile(r'\s*(?:SELECT|SHOW|DESCRIBE|XA RECOVER)',
                               re.I | re.UNICODE)

    def post_exec(self):
        if self.compiled.isinsert:
            if (not len(self._last_inserted_ids) or
                self._last_inserted_ids[0] is None):
                self._last_inserted_ids = ([self.cursor.lastrowid] +
                                           self._last_inserted_ids[1:])
            
    def is_select(self):
        return self._my_is_select.match(self.statement) is not None


class MySQLDialect(ansisql.ANSIDialect):
    def __init__(self, **kwargs):
        ansisql.ANSIDialect.__init__(self, default_paramstyle='format',
                                     **kwargs)

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

    def create_execution_context(self, *args, **kwargs):
        return MySQLExecutionContext(self, *args, **kwargs)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    # identifiers are 64, however aliases can be 255...
    def max_identifier_length(self):
        return 255;

    def supports_sane_rowcount(self):
        return True

    def compiler(self, statement, bindparams, **kwargs):
        return MySQLCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return MySQLSchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return MySQLSchemaDropper(self, *args, **kwargs)

    def preparer(self):
        return MySQLIdentifierPreparer(self)

    def do_executemany(self, cursor, statement, parameters,
                       context=None, **kwargs):
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount
    
    def supports_unicode_statements(self):
        return True
                
    def do_execute(self, cursor, statement, parameters, **kwargs):
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
                                 self.preparer().quote_identifier(schema))
        return [row[0].decode(charset)
                for row in _compat_fetchall(rp, charset=charset)]

    def has_table(self, connection, table_name, schema=None):
        # SHOW TABLE STATUS LIKE and SHOW TABLES LIKE do not function properly
        # on macosx (and maybe win?) with multibyte table names.
        #
        # TODO: if this is not a problem on win, make the strategy swappable
        # based on platform.  DESCRIBE is slower.
        if schema is not None:
            st = "DESCRIBE `%s`.`%s`" % (schema, table_name)
        else:
            st = "DESCRIBE `%s`" % table_name
        try:
            rs = connection.execute(st)
            have = rs.rowcount > 0
            rs.close()
            return have
        except exceptions.SQLError, e:
            if e.orig.args[0] == 1146:
                return False
            raise

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

        decode_from = self._detect_charset(connection)
        casing = self._detect_casing(connection, decode_from)
        if casing == 1:
            # fixme: is this really needed?
            table.name = table.name.lower()
            table.metadata.tables[table.name]= table

        table_name = '.'.join(self.identifier_preparer.format_table_seq(table))
        try:
            rp = connection.execute("DESCRIBE " + table_name)
        except exceptions.SQLError, e:
            if e.orig.args[0] == 1146:
                raise exceptions.NoSuchTableError(table.fullname)
            raise

        for row in _compat_fetchall(rp, charset=decode_from):
            (name, type, nullable, primary_key, default) = \
                   (row[0], row[1], row[2] == 'YES', row[3] == 'PRI', row[4])

            # leave column names as unicode
            name = name.decode(decode_from)
            
            if include_columns and name not in include_columns:
                continue

            match = re.match(r'(\w+)(\(.*?\))?\s*(\w+)?\s*(\w+)?', type)
            col_type = match.group(1)
            args = match.group(2)
            extra_1 = match.group(3)
            extra_2 = match.group(4)

            if col_type == 'tinyint' and args == '(1)':
                col_type = 'boolean'
                args = None
            try:
                coltype = ischema_names[col_type]
            except KeyError:
                warnings.warn(RuntimeWarning(
                        "Did not recognize type '%s' of column '%s'" %
                        (col_type, name)))
                coltype = sqltypes.NULLTYPE

            kw = {}
            if extra_1 is not None:
                kw[extra_1] = True
            if extra_2 is not None:
                kw[extra_2] = True

            if args is not None and coltype is not sqltypes.NULLTYPE:
                if col_type in ('enum', 'set'):
                    args= args[1:-1]
                    argslist = args.split(',')
                    coltype = coltype(*argslist, **kw)
                else:
                    argslist = re.findall(r'(\d+)', args)
                    coltype = coltype(*[int(a) for a in argslist], **kw)

            colargs= []
            if default:
                if col_type == 'timestamp' and default == 'CURRENT_TIMESTAMP':
                    default = sql.text(default)
                colargs.append(schema.PassiveDefault(default))
            table.append_column(schema.Column(name, coltype, *colargs,
                                            **dict(primary_key=primary_key,
                                                   nullable=nullable,
                                                   )))

        table_options = self.moretableinfo(connection, table, decode_from)
        table.kwargs.update(table_options)

    def moretableinfo(self, connection, table, charset=None):
        """SHOW CREATE TABLE to get foreign key/table options."""

        table_name = '.'.join(self.identifier_preparer.format_table_seq(table))
        rp = connection.execute("SHOW CREATE TABLE " + table_name)
        row = _compat_fetchone(rp, charset=charset)
        if not row:
            raise exceptions.NoSuchTableError(table.fullname)
        desc = row[1].strip()
        row.close()

        table_options = {}

        lastparen = re.search(r'\)[^\)]*\Z', desc)
        if lastparen:
            match = re.search(r'\b(?P<spec>TYPE|ENGINE)=(?P<ttype>.+)\b', desc[lastparen.start():], re.I)
            if match:
                table_options["mysql_%s" % match.group('spec')] = \
                    match.group('ttype')

        # \x27 == ' (single quote)  (avoid xemacs syntax highlighting issue)
        fkpat = r'''CONSTRAINT [`"\x27](?P<name>.+?)[`"\x27] FOREIGN KEY \((?P<columns>.+?)\) REFERENCES [`"\x27](?P<reftable>.+?)[`"\x27] \((?P<refcols>.+?)\)'''
        for match in re.finditer(fkpat, desc):
            columns = re.findall(r'''[`"\x27](.+?)[`"\x27]''', match.group('columns'))
            refcols = [match.group('reftable') + "." + x for x in re.findall(r'''[`"\x27](.+?)[`"\x27]''', match.group('refcols'))]
            schema.Table(match.group('reftable'), table.metadata, autoload=True, autoload_with=connection)
            constraint = schema.ForeignKeyConstraint(columns, refcols, name=match.group('name'))
            table.append_constraint(constraint)

        return table_options

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

def _compat_fetchall(rp, charset=None):
    """Proxy result rows to smooth over MySQL-Python driver inconsistencies."""

    return [_MySQLPythonRowProxy(row, charset) for row in rp.fetchall()]

def _compat_fetchone(rp, charset=None):
    """Proxy a result row to smooth over MySQL-Python driver inconsistencies."""

    return _MySQLPythonRowProxy(rp.fetchone(), charset)
        

class _MySQLPythonRowProxy(object):
    """Return consistent column values for all versions of MySQL-python (esp. alphas) and unicode settings."""

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
        if self.charset and isinstance(item, unicode):
            return item.encode(self.charset)
        else:
            return item
    def __getattr__(self, attr):
        item = getattr(self.rowproxy, attr)
        if isinstance(item, _array):
            item = item.tostring()
        if self.charset and isinstance(item, unicode):
            return item.encode(self.charset)
        else:
            return item


class MySQLCompiler(ansisql.ANSICompiler):
    operators = ansisql.ANSICompiler.operators.copy()
    operators.update(
        {
            sql.ColumnOperators.concat_op: \
              lambda x, y: "concat(%s, %s)" % (x, y),
            operator.mod: '%%'
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

class MySQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False,
                                 first_pk=False):
        """Builds column DDL."""
        
        colspec = [self.preparer.format_column(column),
                   column.type.dialect_impl(self.dialect).get_col_spec()]

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append('DEFAULT ' + default)

        if not column.nullable:
            colspec.append('NOT NULL')

        # FIXME: #649, also #612 with regard to SHOW CREATE
        if column.primary_key:
            if (len(column.foreign_keys)==0
                and first_pk
                and column.autoincrement
                and isinstance(column.type, sqltypes.Integer)):
                colspec.append('AUTO_INCREMENT')

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


class MySQLSchemaDropper(ansisql.ANSISchemaDropper):
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


class MySQLIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    """MySQL-specific schema identifier configuration."""
    
    def __init__(self, dialect):
        super(MySQLIdentifierPreparer, self).__init__(dialect,
                                                      initial_quote='`')

    def _reserved_words(self):
        return RESERVED_WORDS

    def _escape_identifier(self, value):
        return value.replace('`', '``')

    def _fold_identifier_case(self, value):
        # TODO: determine MySQL's case folding rules
        #
        # For compatability with sql.text() issued statements, maybe it's best
        # to just leave things as-is.  When lower_case_table_names > 0 it
        # looks a good idea to lc everything, but more importantly the casing
        # of all identifiers in an expression must be consistent.  So for now,
        # just leave everything as-is.
        return value


dialect = MySQLDialect

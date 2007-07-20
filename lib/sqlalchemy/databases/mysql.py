# mysql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import re, datetime, inspect, warnings, weakref

from sqlalchemy import sql, schema, ansisql
from sqlalchemy.engine import default
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions
import sqlalchemy.util as util
from array import array as _array

try:
    from threading import Lock
except ImportError:
    from dummy_threading import Lock


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

_per_connection_mutex = Lock()

class _NumericType(object):
    "Base for MySQL numeric types."

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
    "Base for MySQL string types."

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
    
    def __init__(self, precision = 10, length = 2, **kw):
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
        sqltypes.Numeric.__init__(self, precision, length)

    def get_col_spec(self):
        if self.precision is None:
            return self._extend("NUMERIC")
        else:
            return self._extend("NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})

class MSDecimal(MSNumeric):
    """MySQL DECIMAL type"""

    def __init__(self, precision=10, length=2, **kw):
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

        super(MSDecimal, self).__init__(precision, length, **kw)
    
    def get_col_spec(self):
        if self.precision is None:
            return self._extend("DECIMAL")
        elif self.length is None:
            return self._extend("DECIMAL(%(precision)s)" % {'precision': self.precision})
        else:
            return self._extend("DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})

class MSDouble(MSNumeric):
    """MySQL DOUBLE type"""

    def __init__(self, precision=10, length=2, **kw):
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
        super(MSDouble, self).__init__(precision, length, **kw)

    def get_col_spec(self):
        if self.precision is not None and self.length is not None:
            return self._extend("DOUBLE(%(precision)s, %(length)s)" %
                                {'precision': self.precision,
                                 'length' : self.length})
        else:
            return self._extend('DOUBLE')

class MSFloat(sqltypes.Float, _NumericType):
    """MySQL FLOAT type"""

    def __init__(self, precision=10, length=None, **kw):
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
        sqltypes.Float.__init__(self, precision)

    def get_col_spec(self):
        if hasattr(self, 'length') and self.length is not None:
            return self._extend("FLOAT(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})
        elif self.precision is not None:
            return self._extend("FLOAT(%(precision)s)" % {'precision': self.precision})
        else:
            return self._extend("FLOAT")

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
    the timestamp, add a PassiveDefault to your column specification:

        from sqlalchemy.databases import mysql
        Column('updated', mysql.MSTimeStamp, PassiveDefault(text('CURRENT_TIMESTAMP()')))
    """

    def get_col_spec(self):
        return "TIMESTAMP"

class MSYear(sqltypes.String):
    """MySQL YEAR type, for single byte storage of years 1901-2155"""

    def get_col_spec(self):
        if self.length is None:
            return "YEAR"
        else:
            return "YEAR(%d)" % self.length

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
          Maximum data length, in bytes.  If not length is specified, this
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

class MSBoolean(sqltypes.Boolean):
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

# TODO: SET, BIT

colspecs = {
    sqltypes.Integer : MSInteger,
    sqltypes.Smallinteger : MSSmallInteger,
    sqltypes.Numeric : MSNumeric,
    sqltypes.Float : MSFloat,
    sqltypes.DateTime : MSDateTime,
    sqltypes.Date : MSDate,
    sqltypes.Time : MSTime,
    sqltypes.String : MSString,
    sqltypes.Binary : MSBlob,
    sqltypes.Boolean : MSBoolean,
    sqltypes.TEXT : MSText,
    sqltypes.CHAR: MSChar,
    sqltypes.NCHAR: MSNChar,
    sqltypes.TIMESTAMP: MSTimeStamp,
    sqltypes.BLOB: MSBlob,
    _BinaryType: _BinaryType,
}


ischema_names = {
    'bigint' : MSBigInteger,
    'binary' : MSBinary,
    'blob' : MSBlob,
    'boolean':MSBoolean,
    'char' : MSChar,
    'date' : MSDate,
    'datetime' : MSDateTime,
    'decimal' : MSDecimal,
    'double' : MSDouble,
    'enum': MSEnum,
    'fixed': MSDecimal,
    'float' : MSFloat,
    'int' : MSInteger,
    'integer' : MSInteger,
    'longblob': MSLongBlob,
    'longtext': MSLongText,
    'mediumblob': MSMediumBlob,
    'mediumint' : MSInteger,
    'mediumtext': MSMediumText,
    'nchar': MSNChar,
    'nvarchar': MSNVarChar,
    'numeric' : MSNumeric,
    'smallint' : MSSmallInteger,
    'text' : MSText,
    'time' : MSTime,
    'timestamp' : MSTimeStamp,
    'tinyblob': MSTinyBlob,
    'tinyint' : MSSmallInteger,
    'tinytext' : MSTinyText,
    'varbinary' : MSVarBinary,
    'varchar' : MSString,
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
    def post_exec(self):
        if self.compiled.isinsert:
            if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                self._last_inserted_ids = [self.cursor.lastrowid] + self._last_inserted_ids[1:]
                
class MySQLDialect(ansisql.ANSIDialect):
    def __init__(self, **kwargs):
        ansisql.ANSIDialect.__init__(self, default_paramstyle='format', **kwargs)
        self.per_connection = weakref.WeakKeyDictionary()

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
        # note: these two could break SA Unicode type
        util.coerce_kw_type(opts, 'use_unicode', bool)   
        util.coerce_kw_type(opts, 'charset', str)
        # TODO: cursorclass and conv:  support via query string or punt?
        
        # ssl
        ssl = {}
        for key in ['ssl_ca', 'ssl_key', 'ssl_cert', 'ssl_capath', 'ssl_cipher']:
            if key in opts:
                ssl[key[4:]] = opts[key]
                util.coerce_kw_type(ssl, key[4:], str)
                del opts[key]
        if len(ssl):
            opts['ssl'] = ssl
        
        # FOUND_ROWS must be set in CLIENT_FLAGS for to enable
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

    def do_executemany(self, cursor, statement, parameters, context=None, **kwargs):
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount
    
    def supports_unicode_statements(self):
        return True
                
    def do_execute(self, cursor, statement, parameters, **kwargs):
        cursor.execute(statement, parameters)

    def do_rollback(self, connection):
        # MySQL without InnoDB doesnt support rollback()
        try:
            connection.rollback()
        except:
            pass

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.OperationalError) and e.args[0] in (2006, 2013, 2014, 2045, 2055)

    def get_default_schema_name(self, connection):
        try:
            return self._default_schema_name
        except AttributeError:
            name = self._default_schema_name = \
              connection.execute('SELECT DATABASE()').scalar()
            return name

    def has_table(self, connection, table_name, schema=None):
        # SHOW TABLE STATUS LIKE and SHOW TABLES LIKE do not function properly
        # on macosx (and maybe win?) with multibyte table names.
        #
        # TODO: if this is not a problem on win, make the strategy swappable
        # based on platform.  DESCRIBE is much slower.
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

    def get_version_info(self, connectable):
        if hasattr(connectable, 'connect'):
            con = connectable.connect().connection
        else:
            con = connectable
        version = []
        for n in con.get_server_info().split('.'):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    def reflecttable(self, connection, table):
        """Load column definitions from the server."""

        decode_from = self._detect_charset(connection)
        case_sensitive = self._detect_case_sensitive(connection, decode_from)

        if not case_sensitive:
            table.name = table.name.lower()
            table.metadata.tables[table.name]= table

        try:
            rp = connection.execute("DESCRIBE " + self._escape_table_name(table))
        except exceptions.SQLError, e:
            if e.orig.args[0] == 1146:
                raise exceptions.NoSuchTableError(table.fullname)
            raise

        for row in _compat_fetchall(rp, charset=decode_from):
            (name, type, nullable, primary_key, default) = \
                   (row[0], row[1], row[2] == 'YES', row[3] == 'PRI', row[4])

            # leave column names as unicode
            name = name.decode(decode_from)

            match = re.match(r'(\w+)(\(.*?\))?\s*(\w+)?\s*(\w+)?', type)
            col_type = match.group(1)
            args = match.group(2)
            extra_1 = match.group(3)
            extra_2 = match.group(4)

            coltype = ischema_names.get(col_type, MSString)

            kw = {}
            if extra_1 is not None:
                kw[extra_1] = True
            if extra_2 is not None:
                kw[extra_2] = True

            if args is not None:
                if col_type == 'enum':
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

        tabletype = self.moretableinfo(connection, table, decode_from)
        table.kwargs['mysql_engine'] = tabletype

    def moretableinfo(self, connection, table, charset=None):
        """SHOW CREATE TABLE to get foreign key/table options."""

        rp = connection.execute("SHOW CREATE TABLE " + self._escape_table_name(table), {})
        row = _compat_fetchone(rp, charset=charset)
        if not row:
            raise exceptions.NoSuchTableError(table.fullname)
        desc = row[1].strip()
        row.close()

        tabletype = ''
        lastparen = re.search(r'\)[^\)]*\Z', desc)
        if lastparen:
            match = re.search(r'\b(?:TYPE|ENGINE)=(?P<ttype>.+)\b', desc[lastparen.start():], re.I)
            if match:
                tabletype = match.group('ttype')

        # \x27 == ' (single quote)  (avoid xemacs syntax highlighting issue)
        fkpat = r'''CONSTRAINT [`"\x27](?P<name>.+?)[`"\x27] FOREIGN KEY \((?P<columns>.+?)\) REFERENCES [`"\x27](?P<reftable>.+?)[`"\x27] \((?P<refcols>.+?)\)'''
        for match in re.finditer(fkpat, desc):
            columns = re.findall(r'''[`"\x27](.+?)[`"\x27]''', match.group('columns'))
            refcols = [match.group('reftable') + "." + x for x in re.findall(r'''[`"\x27](.+?)[`"\x27]''', match.group('refcols'))]
            schema.Table(match.group('reftable'), table.metadata, autoload=True, autoload_with=connection)
            constraint = schema.ForeignKeyConstraint(columns, refcols, name=match.group('name'))
            table.append_constraint(constraint)

        return tabletype

    def _escape_table_name(self, table):
        if table.schema is not None:
            return '`%s`.`%s`' % (table.schema, table.name)
        else:
            return '`%s`' % table.name

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""

        # Note: MySQL-python 1.2.1c7 seems to ignore changes made
        # on a connection via set_character_set()
        
        rs = connection.execute("show variables like 'character_set%%'")
        opts = dict([(row[0], row[1]) for row in _compat_fetchall(rs)])

        if 'character_set_results' in opts:
            return opts['character_set_results']
        try:
            return connection.connection.character_set_name()
        except AttributeError:
            # < 1.2.1 final MySQL-python drivers have no charset support
            if 'character_set' in opts:
                return opts['character_set']
            else:
                warnings.warn(RuntimeWarning("Could not detect the connection character set with this combination of MySQL server and MySQL-python.  MySQL-python >= 1.2.2 is recommended.  Assuming latin1."))
                return 'latin1'

    def _detect_case_sensitive(self, connection, charset=None):
        """Sniff out identifier case sensitivity.

        Cached per-connection. This value can not change without a server
        restart.
        """
        # http://dev.mysql.com/doc/refman/5.0/en/name-case-sensitivity.html

        _per_connection_mutex.acquire()
        try:
            raw_connection = connection.connection.connection
            cache = self.per_connection.get(raw_connection, {})
            if 'lower_case_table_names' not in cache:
                row = _compat_fetchone(connection.execute(
                        "SHOW VARIABLES LIKE 'lower_case_table_names'"),
                        charset=charset)
                if not row:
                    cs = True
                else:
                    cs = row[1] in ('0', 'OFF' 'off')
                    row.close()
                cache['lower_case_table_names'] = cs
                self.per_connection[raw_connection] = cache
            return cache.get('lower_case_table_names')
        finally:
            _per_connection_mutex.release()

def _compat_fetchall(rp, charset=None):
    """Proxy result rows to smooth over MySQL-Python driver inconsistencies."""

    return [_MySQLPythonRowProxy(row, charset) for row in rp.fetchall()]

def _compat_fetchone(rp, charset=None):
    """Proxy a result row to smooth over MySQL-Python driver inconsistencies."""

    return _MySQLPythonRowProxy(rp.fetchone(), charset)
        

class _MySQLPythonRowProxy(object):
    """Return consistent column values for all versions of MySQL-python (esp. alphas) and unicode settings."""

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
    def visit_cast(self, cast):

        if isinstance(cast.type, (sqltypes.Date, sqltypes.Time, sqltypes.DateTime)):
            return super(MySQLCompiler, self).visit_cast(cast)
        else:
            # so just skip the CAST altogether for now.
            # TODO: put whatever MySQL does for CAST here.
            self.strings[cast] = self.strings[cast.clause]

    def for_update_clause(self, select):
        if select.for_update == 'read':
             return ' LOCK IN SHARE MODE'
        else:
            return super(MySQLCompiler, self).for_update_clause(select)

    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                # striaght from the MySQL docs, I kid you not
                text += " \n LIMIT 18446744073709551615"
            text += " OFFSET " + str(select.offset)
        return text
        
    def binary_operator_string(self, binary):
        if binary.operator == '%':
            return '%%'
        else:
            return ansisql.ANSICompiler.binary_operator_string(self, binary)   

class MySQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, first_pk=False):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            if len(column.foreign_keys)==0 and first_pk and column.autoincrement and isinstance(column.type, sqltypes.Integer):
                colspec += " AUTO_INCREMENT"
        return colspec

    def post_create_table(self, table):
        args = ""
        for k in table.kwargs:
            if k.startswith('mysql_'):
                opt = k[6:]
                args += " %s=%s" % (opt.upper(), table.kwargs[k])
        return args

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
    def __init__(self, dialect):
        super(MySQLIdentifierPreparer, self).__init__(dialect, initial_quote='`')

    def _reserved_words(self):
        return RESERVED_WORDS

    def _escape_identifier(self, value):
        return value.replace('`', '``')

    def _fold_identifier_case(self, value):
        #TODO: determine MySQL's case folding rules
        return value

dialect = MySQLDialect

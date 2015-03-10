# sql/sqltypes.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL specific types.

"""

import datetime as dt
import codecs

from .type_api import TypeEngine, TypeDecorator, to_instance
from .elements import quoted_name, type_coerce, _defer_name
from .default_comparator import _DefaultColumnComparator
from .. import exc, util, processors
from .base import _bind_or_error, SchemaEventTarget
from . import operators
from .. import event
from ..util import pickle
import decimal

if util.jython:
    import array


class _DateAffinity(object):

    """Mixin date/time specific expression adaptations.

    Rules are implemented within Date,Time,Interval,DateTime, Numeric,
    Integer. Based on http://www.postgresql.org/docs/current/static
    /functions-datetime.html.

    """

    @property
    def _expression_adaptations(self):
        raise NotImplementedError()

    class Comparator(TypeEngine.Comparator):
        _blank_dict = util.immutabledict()

        def _adapt_expression(self, op, other_comparator):
            othertype = other_comparator.type._type_affinity
            return (
                op, to_instance(
                    self.type._expression_adaptations.
                    get(op, self._blank_dict).
                    get(othertype, NULLTYPE))
            )
    comparator_factory = Comparator


class Concatenable(object):

    """A mixin that marks a type as supporting 'concatenation',
    typically strings."""

    class Comparator(TypeEngine.Comparator):

        def _adapt_expression(self, op, other_comparator):
            if (op is operators.add and
                    isinstance(
                        other_comparator,
                        (Concatenable.Comparator, NullType.Comparator)
                    )):
                return operators.concat_op, self.expr.type
            else:
                return op, self.expr.type

    comparator_factory = Comparator


class String(Concatenable, TypeEngine):

    """The base for all string and character types.

    In SQL, corresponds to VARCHAR.  Can also take Python unicode objects
    and encode to the database's encoding in bind params (and the reverse for
    result sets.)

    The `length` field is usually required when the `String` type is
    used within a CREATE TABLE statement, as VARCHAR requires a length
    on most databases.

    """

    __visit_name__ = 'string'

    def __init__(self, length=None, collation=None,
                 convert_unicode=False,
                 unicode_error=None,
                 _warn_on_bytestring=False
                 ):
        """
        Create a string-holding type.

        :param length: optional, a length for the column for use in
          DDL and CAST expressions.  May be safely omitted if no ``CREATE
          TABLE`` will be issued.  Certain databases may require a
          ``length`` for use in DDL, and will raise an exception when
          the ``CREATE TABLE`` DDL is issued if a ``VARCHAR``
          with no length is included.  Whether the value is
          interpreted as bytes or characters is database specific.

        :param collation: Optional, a column-level collation for
          use in DDL and CAST expressions.  Renders using the
          COLLATE keyword supported by SQLite, MySQL, and Postgresql.
          E.g.::

            >>> from sqlalchemy import cast, select, String
            >>> print select([cast('some string', String(collation='utf8'))])
            SELECT CAST(:param_1 AS VARCHAR COLLATE utf8) AS anon_1

          .. versionadded:: 0.8 Added support for COLLATE to all
             string types.

        :param convert_unicode: When set to ``True``, the
          :class:`.String` type will assume that
          input is to be passed as Python ``unicode`` objects,
          and results returned as Python ``unicode`` objects.
          If the DBAPI in use does not support Python unicode
          (which is fewer and fewer these days), SQLAlchemy
          will encode/decode the value, using the
          value of the ``encoding`` parameter passed to
          :func:`.create_engine` as the encoding.

          When using a DBAPI that natively supports Python
          unicode objects, this flag generally does not
          need to be set.  For columns that are explicitly
          intended to store non-ASCII data, the :class:`.Unicode`
          or :class:`.UnicodeText`
          types should be used regardless, which feature
          the same behavior of ``convert_unicode`` but
          also indicate an underlying column type that
          directly supports unicode, such as ``NVARCHAR``.

          For the extremely rare case that Python ``unicode``
          is to be encoded/decoded by SQLAlchemy on a backend
          that does natively support Python ``unicode``,
          the value ``force`` can be passed here which will
          cause SQLAlchemy's encode/decode services to be
          used unconditionally.

        :param unicode_error: Optional, a method to use to handle Unicode
          conversion errors. Behaves like the ``errors`` keyword argument to
          the standard library's ``string.decode()`` functions.   This flag
          requires that ``convert_unicode`` is set to ``force`` - otherwise,
          SQLAlchemy is not guaranteed to handle the task of unicode
          conversion.   Note that this flag adds significant performance
          overhead to row-fetching operations for backends that already
          return unicode objects natively (which most DBAPIs do).  This
          flag should only be used as a last resort for reading
          strings from a column with varied or corrupted encodings.

        """
        if unicode_error is not None and convert_unicode != 'force':
            raise exc.ArgumentError("convert_unicode must be 'force' "
                                    "when unicode_error is set.")

        self.length = length
        self.collation = collation
        self.convert_unicode = convert_unicode
        self.unicode_error = unicode_error
        self._warn_on_bytestring = _warn_on_bytestring

    def literal_processor(self, dialect):
        def process(value):
            value = value.replace("'", "''")
            return "'%s'" % value
        return process

    def bind_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            if dialect.supports_unicode_binds and \
                    self.convert_unicode != 'force':
                if self._warn_on_bytestring:
                    def process(value):
                        if isinstance(value, util.binary_type):
                            util.warn("Unicode type received non-unicode"
                                      "bind param value.")
                        return value
                    return process
                else:
                    return None
            else:
                encoder = codecs.getencoder(dialect.encoding)
                warn_on_bytestring = self._warn_on_bytestring

                def process(value):
                    if isinstance(value, util.text_type):
                        return encoder(value, self.unicode_error)[0]
                    elif warn_on_bytestring and value is not None:
                        util.warn("Unicode type received non-unicode bind "
                                  "param value")
                    return value
            return process
        else:
            return None

    def result_processor(self, dialect, coltype):
        wants_unicode = self.convert_unicode or dialect.convert_unicode
        needs_convert = wants_unicode and \
            (dialect.returns_unicode_strings is not True or
             self.convert_unicode in ('force', 'force_nocheck'))
        needs_isinstance = (
            needs_convert and
            dialect.returns_unicode_strings and
            self.convert_unicode != 'force_nocheck'
        )
        if needs_convert:
            to_unicode = processors.to_unicode_processor_factory(
                dialect.encoding, self.unicode_error)

            if needs_isinstance:
                return processors.to_conditional_unicode_processor_factory(
                    dialect.encoding, self.unicode_error)
            else:
                return processors.to_unicode_processor_factory(
                    dialect.encoding, self.unicode_error)
        else:
            return None

    @property
    def python_type(self):
        if self.convert_unicode:
            return util.text_type
        else:
            return str

    def get_dbapi_type(self, dbapi):
        return dbapi.STRING


class Text(String):

    """A variably sized string type.

    In SQL, usually corresponds to CLOB or TEXT. Can also take Python
    unicode objects and encode to the database's encoding in bind
    params (and the reverse for result sets.)  In general, TEXT objects
    do not have a length; while some databases will accept a length
    argument here, it will be rejected by others.

    """
    __visit_name__ = 'text'


class Unicode(String):

    """A variable length Unicode string type.

    The :class:`.Unicode` type is a :class:`.String` subclass
    that assumes input and output as Python ``unicode`` data,
    and in that regard is equivalent to the usage of the
    ``convert_unicode`` flag with the :class:`.String` type.
    However, unlike plain :class:`.String`, it also implies an
    underlying column type that is explicitly supporting of non-ASCII
    data, such as ``NVARCHAR`` on Oracle and SQL Server.
    This can impact the output of ``CREATE TABLE`` statements
    and ``CAST`` functions at the dialect level, and can
    also affect the handling of bound parameters in some
    specific DBAPI scenarios.

    The encoding used by the :class:`.Unicode` type is usually
    determined by the DBAPI itself; most modern DBAPIs
    feature support for Python ``unicode`` objects as bound
    values and result set values, and the encoding should
    be configured as detailed in the notes for the target
    DBAPI in the :ref:`dialect_toplevel` section.

    For those DBAPIs which do not support, or are not configured
    to accommodate Python ``unicode`` objects
    directly, SQLAlchemy does the encoding and decoding
    outside of the DBAPI.   The encoding in this scenario
    is determined by the ``encoding`` flag passed to
    :func:`.create_engine`.

    When using the :class:`.Unicode` type, it is only appropriate
    to pass Python ``unicode`` objects, and not plain ``str``.
    If a plain ``str`` is passed under Python 2, a warning
    is emitted.  If you notice your application emitting these warnings but
    you're not sure of the source of them, the Python
    ``warnings`` filter, documented at
    http://docs.python.org/library/warnings.html,
    can be used to turn these warnings into exceptions
    which will illustrate a stack trace::

      import warnings
      warnings.simplefilter('error')

    For an application that wishes to pass plain bytestrings
    and Python ``unicode`` objects to the ``Unicode`` type
    equally, the bytestrings must first be decoded into
    unicode.  The recipe at :ref:`coerce_to_unicode` illustrates
    how this is done.

    See also:

        :class:`.UnicodeText` - unlengthed textual counterpart
        to :class:`.Unicode`.

    """

    __visit_name__ = 'unicode'

    def __init__(self, length=None, **kwargs):
        """
        Create a :class:`.Unicode` object.

        Parameters are the same as that of :class:`.String`,
        with the exception that ``convert_unicode``
        defaults to ``True``.

        """
        kwargs.setdefault('convert_unicode', True)
        kwargs.setdefault('_warn_on_bytestring', True)
        super(Unicode, self).__init__(length=length, **kwargs)


class UnicodeText(Text):

    """An unbounded-length Unicode string type.

    See :class:`.Unicode` for details on the unicode
    behavior of this object.

    Like :class:`.Unicode`, usage the :class:`.UnicodeText` type implies a
    unicode-capable type being used on the backend, such as
    ``NCLOB``, ``NTEXT``.

    """

    __visit_name__ = 'unicode_text'

    def __init__(self, length=None, **kwargs):
        """
        Create a Unicode-converting Text type.

        Parameters are the same as that of :class:`.Text`,
        with the exception that ``convert_unicode``
        defaults to ``True``.

        """
        kwargs.setdefault('convert_unicode', True)
        kwargs.setdefault('_warn_on_bytestring', True)
        super(UnicodeText, self).__init__(length=length, **kwargs)


class Integer(_DateAffinity, TypeEngine):

    """A type for ``int`` integers."""

    __visit_name__ = 'integer'

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

    @property
    def python_type(self):
        return int

    def literal_processor(self, dialect):
        def process(value):
            return str(value)
        return process

    @util.memoized_property
    def _expression_adaptations(self):
        # TODO: need a dictionary object that will
        # handle operators generically here, this is incomplete
        return {
            operators.add: {
                Date: Date,
                Integer: self.__class__,
                Numeric: Numeric,
            },
            operators.mul: {
                Interval: Interval,
                Integer: self.__class__,
                Numeric: Numeric,
            },
            operators.div: {
                Integer: self.__class__,
                Numeric: Numeric,
            },
            operators.truediv: {
                Integer: self.__class__,
                Numeric: Numeric,
            },
            operators.sub: {
                Integer: self.__class__,
                Numeric: Numeric,
            },
        }


class SmallInteger(Integer):

    """A type for smaller ``int`` integers.

    Typically generates a ``SMALLINT`` in DDL, and otherwise acts like
    a normal :class:`.Integer` on the Python side.

    """

    __visit_name__ = 'small_integer'


class BigInteger(Integer):

    """A type for bigger ``int`` integers.

    Typically generates a ``BIGINT`` in DDL, and otherwise acts like
    a normal :class:`.Integer` on the Python side.

    """

    __visit_name__ = 'big_integer'


class Numeric(_DateAffinity, TypeEngine):

    """A type for fixed precision numbers, such as ``NUMERIC`` or ``DECIMAL``.

    This type returns Python ``decimal.Decimal`` objects by default, unless
    the :paramref:`.Numeric.asdecimal` flag is set to False, in which case
    they are coerced to Python ``float`` objects.

    .. note::

        The :class:`.Numeric` type is designed to receive data from a database
        type that is explicitly known to be a decimal type
        (e.g. ``DECIMAL``, ``NUMERIC``, others) and not a floating point
        type (e.g. ``FLOAT``, ``REAL``, others).
        If the database column on the server is in fact a floating-point type
        type, such as ``FLOAT`` or ``REAL``, use the :class:`.Float`
        type or a subclass, otherwise numeric coercion between
        ``float``/``Decimal`` may or may not function as expected.

    .. note::

       The Python ``decimal.Decimal`` class is generally slow
       performing; cPython 3.3 has now switched to use the `cdecimal
       <http://pypi.python.org/pypi/cdecimal/>`_ library natively. For
       older Python versions, the ``cdecimal`` library can be patched
       into any application where it will replace the ``decimal``
       library fully, however this needs to be applied globally and
       before any other modules have been imported, as follows::

           import sys
           import cdecimal
           sys.modules["decimal"] = cdecimal

       Note that the ``cdecimal`` and ``decimal`` libraries are **not
       compatible with each other**, so patching ``cdecimal`` at the
       global level is the only way it can be used effectively with
       various DBAPIs that hardcode to import the ``decimal`` library.

    """

    __visit_name__ = 'numeric'

    _default_decimal_return_scale = 10

    def __init__(self, precision=None, scale=None,
                 decimal_return_scale=None, asdecimal=True):
        """
        Construct a Numeric.

        :param precision: the numeric precision for use in DDL ``CREATE
          TABLE``.

        :param scale: the numeric scale for use in DDL ``CREATE TABLE``.

        :param asdecimal: default True.  Return whether or not
          values should be sent as Python Decimal objects, or
          as floats.   Different DBAPIs send one or the other based on
          datatypes - the Numeric type will ensure that return values
          are one or the other across DBAPIs consistently.

        :param decimal_return_scale: Default scale to use when converting
         from floats to Python decimals.  Floating point values will typically
         be much longer due to decimal inaccuracy, and most floating point
         database types don't have a notion of "scale", so by default the
         float type looks for the first ten decimal places when converting.
         Specfiying this value will override that length.  Types which
         do include an explicit ".scale" value, such as the base
         :class:`.Numeric` as well as the MySQL float types, will use the
         value of ".scale" as the default for decimal_return_scale, if not
         otherwise specified.

         .. versionadded:: 0.9.0

        When using the ``Numeric`` type, care should be taken to ensure
        that the asdecimal setting is apppropriate for the DBAPI in use -
        when Numeric applies a conversion from Decimal->float or float->
        Decimal, this conversion incurs an additional performance overhead
        for all result columns received.

        DBAPIs that return Decimal natively (e.g. psycopg2) will have
        better accuracy and higher performance with a setting of ``True``,
        as the native translation to Decimal reduces the amount of floating-
        point issues at play, and the Numeric type itself doesn't need
        to apply any further conversions.  However, another DBAPI which
        returns floats natively *will* incur an additional conversion
        overhead, and is still subject to floating point data loss - in
        which case ``asdecimal=False`` will at least remove the extra
        conversion overhead.

        """
        self.precision = precision
        self.scale = scale
        self.decimal_return_scale = decimal_return_scale
        self.asdecimal = asdecimal

    @property
    def _effective_decimal_return_scale(self):
        if self.decimal_return_scale is not None:
            return self.decimal_return_scale
        elif getattr(self, "scale", None) is not None:
            return self.scale
        else:
            return self._default_decimal_return_scale

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

    def literal_processor(self, dialect):
        def process(value):
            return str(value)
        return process

    @property
    def python_type(self):
        if self.asdecimal:
            return decimal.Decimal
        else:
            return float

    def bind_processor(self, dialect):
        if dialect.supports_native_decimal:
            return None
        else:
            return processors.to_float

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if dialect.supports_native_decimal:
                # we're a "numeric", DBAPI will give us Decimal directly
                return None
            else:
                util.warn('Dialect %s+%s does *not* support Decimal '
                          'objects natively, and SQLAlchemy must '
                          'convert from floating point - rounding '
                          'errors and other issues may occur. Please '
                          'consider storing Decimal numbers as strings '
                          'or integers on this platform for lossless '
                          'storage.' % (dialect.name, dialect.driver))

                # we're a "numeric", DBAPI returns floats, convert.
                return processors.to_decimal_processor_factory(
                    decimal.Decimal,
                    self.scale if self.scale is not None
                    else self._default_decimal_return_scale)
        else:
            if dialect.supports_native_decimal:
                return processors.to_float
            else:
                return None

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.mul: {
                Interval: Interval,
                Numeric: self.__class__,
                Integer: self.__class__,
            },
            operators.div: {
                Numeric: self.__class__,
                Integer: self.__class__,
            },
            operators.truediv: {
                Numeric: self.__class__,
                Integer: self.__class__,
            },
            operators.add: {
                Numeric: self.__class__,
                Integer: self.__class__,
            },
            operators.sub: {
                Numeric: self.__class__,
                Integer: self.__class__,
            }
        }


class Float(Numeric):

    """Type representing floating point types, such as ``FLOAT`` or ``REAL``.

    This type returns Python ``float`` objects by default, unless the
    :paramref:`.Float.asdecimal` flag is set to True, in which case they
    are coerced to ``decimal.Decimal`` objects.

    .. note::

        The :class:`.Float` type is designed to receive data from a database
        type that is explicitly known to be a floating point type
        (e.g. ``FLOAT``, ``REAL``, others)
        and not a decimal type (e.g. ``DECIMAL``, ``NUMERIC``, others).
        If the database column on the server is in fact a Numeric
        type, such as ``DECIMAL`` or ``NUMERIC``, use the :class:`.Numeric`
        type or a subclass, otherwise numeric coercion between
        ``float``/``Decimal`` may or may not function as expected.

    """

    __visit_name__ = 'float'

    scale = None

    def __init__(self, precision=None, asdecimal=False,
                 decimal_return_scale=None, **kwargs):
        """
        Construct a Float.

        :param precision: the numeric precision for use in DDL ``CREATE
           TABLE``.

        :param asdecimal: the same flag as that of :class:`.Numeric`, but
          defaults to ``False``.   Note that setting this flag to ``True``
          results in floating point conversion.

        :param decimal_return_scale: Default scale to use when converting
         from floats to Python decimals.  Floating point values will typically
         be much longer due to decimal inaccuracy, and most floating point
         database types don't have a notion of "scale", so by default the
         float type looks for the first ten decimal places when converting.
         Specfiying this value will override that length.  Note that the
         MySQL float types, which do include "scale", will use "scale"
         as the default for decimal_return_scale, if not otherwise specified.

         .. versionadded:: 0.9.0

        :param \**kwargs: deprecated.  Additional arguments here are ignored
         by the default :class:`.Float` type.  For database specific
         floats that support additional arguments, see that dialect's
         documentation for details, such as
         :class:`sqlalchemy.dialects.mysql.FLOAT`.

        """
        self.precision = precision
        self.asdecimal = asdecimal
        self.decimal_return_scale = decimal_return_scale
        if kwargs:
            util.warn_deprecated("Additional keyword arguments "
                                 "passed to Float ignored.")

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return processors.to_decimal_processor_factory(
                decimal.Decimal,
                self._effective_decimal_return_scale)
        else:
            return None

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.mul: {
                Interval: Interval,
                Numeric: self.__class__,
            },
            operators.div: {
                Numeric: self.__class__,
            },
            operators.truediv: {
                Numeric: self.__class__,
            },
            operators.add: {
                Numeric: self.__class__,
            },
            operators.sub: {
                Numeric: self.__class__,
            }
        }


class DateTime(_DateAffinity, TypeEngine):

    """A type for ``datetime.datetime()`` objects.

    Date and time types return objects from the Python ``datetime``
    module.  Most DBAPIs have built in support for the datetime
    module, with the noted exception of SQLite.  In the case of
    SQLite, date and time types are stored as strings which are then
    converted back to datetime objects when rows are returned.

    """

    __visit_name__ = 'datetime'

    def __init__(self, timezone=False):
        """Construct a new :class:`.DateTime`.

        :param timezone: boolean.  If True, and supported by the
         backend, will produce 'TIMESTAMP WITH TIMEZONE'. For backends
         that don't support timezone aware timestamps, has no
         effect.

        """
        self.timezone = timezone

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @property
    def python_type(self):
        return dt.datetime

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add: {
                Interval: self.__class__,
            },
            operators.sub: {
                Interval: self.__class__,
                DateTime: Interval,
            },
        }


class Date(_DateAffinity, TypeEngine):

    """A type for ``datetime.date()`` objects."""

    __visit_name__ = 'date'

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @property
    def python_type(self):
        return dt.date

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add: {
                Integer: self.__class__,
                Interval: DateTime,
                Time: DateTime,
            },
            operators.sub: {
                # date - integer = date
                Integer: self.__class__,

                # date - date = integer.
                Date: Integer,

                Interval: DateTime,

                # date - datetime = interval,
                # this one is not in the PG docs
                # but works
                DateTime: Interval,
            },
        }


class Time(_DateAffinity, TypeEngine):

    """A type for ``datetime.time()`` objects."""

    __visit_name__ = 'time'

    def __init__(self, timezone=False):
        self.timezone = timezone

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @property
    def python_type(self):
        return dt.time

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add: {
                Date: DateTime,
                Interval: self.__class__
            },
            operators.sub: {
                Time: Interval,
                Interval: self.__class__,
            },
        }


class _Binary(TypeEngine):

    """Define base behavior for binary types."""

    def __init__(self, length=None):
        self.length = length

    def literal_processor(self, dialect):
        def process(value):
            value = value.decode(dialect.encoding).replace("'", "''")
            return "'%s'" % value
        return process

    @property
    def python_type(self):
        return util.binary_type

    # Python 3 - sqlite3 doesn't need the `Binary` conversion
    # here, though pg8000 does to indicate "bytea"
    def bind_processor(self, dialect):
        if dialect.dbapi is None:
            return None

        DBAPIBinary = dialect.dbapi.Binary

        def process(value):
            if value is not None:
                return DBAPIBinary(value)
            else:
                return None
        return process

    # Python 3 has native bytes() type
    # both sqlite3 and pg8000 seem to return it,
    # psycopg2 as of 2.5 returns 'memoryview'
    if util.py2k:
        def result_processor(self, dialect, coltype):
            if util.jython:
                def process(value):
                    if value is not None:
                        if isinstance(value, array.array):
                            return value.tostring()
                        return str(value)
                    else:
                        return None
            else:
                process = processors.to_str
            return process
    else:
        def result_processor(self, dialect, coltype):
            def process(value):
                if value is not None:
                    value = bytes(value)
                return value
            return process

    def coerce_compared_value(self, op, value):
        """See :meth:`.TypeEngine.coerce_compared_value` for a description."""

        if isinstance(value, util.string_types):
            return self
        else:
            return super(_Binary, self).coerce_compared_value(op, value)

    def get_dbapi_type(self, dbapi):
        return dbapi.BINARY


class LargeBinary(_Binary):

    """A type for large binary byte data.

    The Binary type generates BLOB or BYTEA when tables are created,
    and also converts incoming values using the ``Binary`` callable
    provided by each DB-API.

    """

    __visit_name__ = 'large_binary'

    def __init__(self, length=None):
        """
        Construct a LargeBinary type.

        :param length: optional, a length for the column for use in
          DDL statements, for those BLOB types that accept a length
          (i.e. MySQL).  It does *not* produce a small BINARY/VARBINARY
          type - use the BINARY/VARBINARY types specifically for those.
          May be safely omitted if no ``CREATE
          TABLE`` will be issued.  Certain databases may require a
          *length* for use in DDL, and will raise an exception when
          the ``CREATE TABLE`` DDL is issued.

        """
        _Binary.__init__(self, length=length)


class Binary(LargeBinary):

    """Deprecated.  Renamed to LargeBinary."""

    def __init__(self, *arg, **kw):
        util.warn_deprecated('The Binary type has been renamed to '
                             'LargeBinary.')
        LargeBinary.__init__(self, *arg, **kw)


class SchemaType(SchemaEventTarget):

    """Mark a type as possibly requiring schema-level DDL for usage.

    Supports types that must be explicitly created/dropped (i.e. PG ENUM type)
    as well as types that are complimented by table or schema level
    constraints, triggers, and other rules.

    :class:`.SchemaType` classes can also be targets for the
    :meth:`.DDLEvents.before_parent_attach` and
    :meth:`.DDLEvents.after_parent_attach` events, where the events fire off
    surrounding the association of the type object with a parent
    :class:`.Column`.

    .. seealso::

        :class:`.Enum`

        :class:`.Boolean`


    """

    def __init__(self, name=None, schema=None, metadata=None,
                 inherit_schema=False, quote=None):
        if name is not None:
            self.name = quoted_name(name, quote)
        else:
            self.name = None
        self.schema = schema
        self.metadata = metadata
        self.inherit_schema = inherit_schema

        if self.metadata:
            event.listen(
                self.metadata,
                "before_create",
                util.portable_instancemethod(self._on_metadata_create)
            )
            event.listen(
                self.metadata,
                "after_drop",
                util.portable_instancemethod(self._on_metadata_drop)
            )

    def _set_parent(self, column):
        column._on_table_attach(util.portable_instancemethod(self._set_table))

    def _set_table(self, column, table):
        if self.inherit_schema:
            self.schema = table.schema

        event.listen(
            table,
            "before_create",
            util.portable_instancemethod(
                self._on_table_create)
        )
        event.listen(
            table,
            "after_drop",
            util.portable_instancemethod(self._on_table_drop)
        )
        if self.metadata is None:
            # TODO: what's the difference between self.metadata
            # and table.metadata here ?
            event.listen(
                table.metadata,
                "before_create",
                util.portable_instancemethod(self._on_metadata_create)
            )
            event.listen(
                table.metadata,
                "after_drop",
                util.portable_instancemethod(self._on_metadata_drop)
            )

    def copy(self, **kw):
        return self.adapt(self.__class__)

    def adapt(self, impltype, **kw):
        schema = kw.pop('schema', self.schema)

        # don't associate with MetaData as the hosting type
        # is already associated with it, avoid creating event
        # listeners
        metadata = kw.pop('metadata', None)
        return impltype(name=self.name,
                        schema=schema,
                        metadata=metadata,
                        inherit_schema=self.inherit_schema,
                        **kw)

    @property
    def bind(self):
        return self.metadata and self.metadata.bind or None

    def create(self, bind=None, checkfirst=False):
        """Issue CREATE ddl for this type, if applicable."""

        if bind is None:
            bind = _bind_or_error(self)
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t.create(bind=bind, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=False):
        """Issue DROP ddl for this type, if applicable."""

        if bind is None:
            bind = _bind_or_error(self)
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t.drop(bind=bind, checkfirst=checkfirst)

    def _on_table_create(self, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t._on_table_create(target, bind, **kw)

    def _on_table_drop(self, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t._on_table_drop(target, bind, **kw)

    def _on_metadata_create(self, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t._on_metadata_create(target, bind, **kw)

    def _on_metadata_drop(self, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t.__class__ is not self.__class__ and isinstance(t, SchemaType):
            t._on_metadata_drop(target, bind, **kw)


class Enum(String, SchemaType):

    """Generic Enum Type.

    The Enum type provides a set of possible string values which the
    column is constrained towards.

    By default, uses the backend's native ENUM type if available,
    else uses VARCHAR + a CHECK constraint.

    .. seealso::

        :class:`~.postgresql.ENUM` - PostgreSQL-specific type,
        which has additional functionality.

    """

    __visit_name__ = 'enum'

    def __init__(self, *enums, **kw):
        """Construct an enum.

        Keyword arguments which don't apply to a specific backend are ignored
        by that backend.

        :param \*enums: string or unicode enumeration labels. If unicode
           labels are present, the `convert_unicode` flag is auto-enabled.

        :param convert_unicode: Enable unicode-aware bind parameter and
           result-set processing for this Enum's data. This is set
           automatically based on the presence of unicode label strings.

        :param metadata: Associate this type directly with a ``MetaData``
           object. For types that exist on the target database as an
           independent schema construct (Postgresql), this type will be
           created and dropped within ``create_all()`` and ``drop_all()``
           operations. If the type is not associated with any ``MetaData``
           object, it will associate itself with each ``Table`` in which it is
           used, and will be created when any of those individual tables are
           created, after a check is performed for its existence. The type is
           only dropped when ``drop_all()`` is called for that ``Table``
           object's metadata, however.

        :param name: The name of this type. This is required for Postgresql
           and any future supported database which requires an explicitly
           named type, or an explicitly named constraint in order to generate
           the type and/or a table that uses it.

        :param native_enum: Use the database's native ENUM type when
           available. Defaults to True. When False, uses VARCHAR + check
           constraint for all backends.

        :param schema: Schema name of this type. For types that exist on the
           target database as an independent schema construct (Postgresql),
           this parameter specifies the named schema in which the type is
           present.

           .. note::

                The ``schema`` of the :class:`.Enum` type does not
                by default make use of the ``schema`` established on the
                owning :class:`.Table`.  If this behavior is desired,
                set the ``inherit_schema`` flag to ``True``.

        :param quote: Set explicit quoting preferences for the type's name.

        :param inherit_schema: When ``True``, the "schema" from the owning
           :class:`.Table` will be copied to the "schema" attribute of this
           :class:`.Enum`, replacing whatever value was passed for the
           ``schema`` attribute.   This also takes effect when using the
           :meth:`.Table.tometadata` operation.

           .. versionadded:: 0.8

        """
        self.enums = enums
        self.native_enum = kw.pop('native_enum', True)
        convert_unicode = kw.pop('convert_unicode', None)
        if convert_unicode is None:
            for e in enums:
                if isinstance(e, util.text_type):
                    convert_unicode = True
                    break
            else:
                convert_unicode = False

        if self.enums:
            length = max(len(x) for x in self.enums)
        else:
            length = 0
        String.__init__(self,
                        length=length,
                        convert_unicode=convert_unicode,
                        )
        SchemaType.__init__(self, **kw)

    def __repr__(self):
        return util.generic_repr(self,
                                 additional_kw=[('native_enum', True)],
                                 to_inspect=[Enum, SchemaType],
                                 )

    def _should_create_constraint(self, compiler):
        return not self.native_enum or \
            not compiler.dialect.supports_native_enum

    @util.dependencies("sqlalchemy.sql.schema")
    def _set_table(self, schema, column, table):
        if self.native_enum:
            SchemaType._set_table(self, column, table)

        e = schema.CheckConstraint(
            type_coerce(column, self).in_(self.enums),
            name=_defer_name(self.name),
            _create_rule=util.portable_instancemethod(
                self._should_create_constraint)
        )
        assert e.table is table

    def adapt(self, impltype, **kw):
        schema = kw.pop('schema', self.schema)
        metadata = kw.pop('metadata', None)
        if issubclass(impltype, Enum):
            return impltype(name=self.name,
                            schema=schema,
                            metadata=metadata,
                            convert_unicode=self.convert_unicode,
                            native_enum=self.native_enum,
                            inherit_schema=self.inherit_schema,
                            *self.enums,
                            **kw)
        else:
            return super(Enum, self).adapt(impltype, **kw)


class PickleType(TypeDecorator):

    """Holds Python objects, which are serialized using pickle.

    PickleType builds upon the Binary type to apply Python's
    ``pickle.dumps()`` to incoming objects, and ``pickle.loads()`` on
    the way out, allowing any pickleable Python object to be stored as
    a serialized binary field.

    To allow ORM change events to propagate for elements associated
    with :class:`.PickleType`, see :ref:`mutable_toplevel`.

    """

    impl = LargeBinary

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL,
                 pickler=None, comparator=None):
        """
        Construct a PickleType.

        :param protocol: defaults to ``pickle.HIGHEST_PROTOCOL``.

        :param pickler: defaults to cPickle.pickle or pickle.pickle if
          cPickle is not available.  May be any object with
          pickle-compatible ``dumps` and ``loads`` methods.

        :param comparator: a 2-arg callable predicate used
          to compare values of this type.  If left as ``None``,
          the Python "equals" operator is used to compare values.

        """
        self.protocol = protocol
        self.pickler = pickler or pickle
        self.comparator = comparator
        super(PickleType, self).__init__()

    def __reduce__(self):
        return PickleType, (self.protocol,
                            None,
                            self.comparator)

    def bind_processor(self, dialect):
        impl_processor = self.impl.bind_processor(dialect)
        dumps = self.pickler.dumps
        protocol = self.protocol
        if impl_processor:
            def process(value):
                if value is not None:
                    value = dumps(value, protocol)
                return impl_processor(value)
        else:
            def process(value):
                if value is not None:
                    value = dumps(value, protocol)
                return value
        return process

    def result_processor(self, dialect, coltype):
        impl_processor = self.impl.result_processor(dialect, coltype)
        loads = self.pickler.loads
        if impl_processor:
            def process(value):
                value = impl_processor(value)
                if value is None:
                    return None
                return loads(value)
        else:
            def process(value):
                if value is None:
                    return None
                return loads(value)
        return process

    def compare_values(self, x, y):
        if self.comparator:
            return self.comparator(x, y)
        else:
            return x == y


class Boolean(TypeEngine, SchemaType):

    """A bool datatype.

    Boolean typically uses BOOLEAN or SMALLINT on the DDL side, and on
    the Python side deals in ``True`` or ``False``.

    """

    __visit_name__ = 'boolean'

    def __init__(self, create_constraint=True, name=None):
        """Construct a Boolean.

        :param create_constraint: defaults to True.  If the boolean
          is generated as an int/smallint, also create a CHECK constraint
          on the table that ensures 1 or 0 as a value.

        :param name: if a CHECK constraint is generated, specify
          the name of the constraint.

        """
        self.create_constraint = create_constraint
        self.name = name

    def _should_create_constraint(self, compiler):
        return not compiler.dialect.supports_native_boolean

    @util.dependencies("sqlalchemy.sql.schema")
    def _set_table(self, schema, column, table):
        if not self.create_constraint:
            return

        e = schema.CheckConstraint(
            type_coerce(column, self).in_([0, 1]),
            name=_defer_name(self.name),
            _create_rule=util.portable_instancemethod(
                self._should_create_constraint)
        )
        assert e.table is table

    @property
    def python_type(self):
        return bool

    def literal_processor(self, dialect):
        if dialect.supports_native_boolean:
            def process(value):
                return "true" if value else "false"
        else:
            def process(value):
                return str(1 if value else 0)
        return process

    def bind_processor(self, dialect):
        if dialect.supports_native_boolean:
            return None
        else:
            return processors.boolean_to_int

    def result_processor(self, dialect, coltype):
        if dialect.supports_native_boolean:
            return None
        else:
            return processors.int_to_boolean


class Interval(_DateAffinity, TypeDecorator):

    """A type for ``datetime.timedelta()`` objects.

    The Interval type deals with ``datetime.timedelta`` objects.  In
    PostgreSQL, the native ``INTERVAL`` type is used; for others, the
    value is stored as a date which is relative to the "epoch"
    (Jan. 1, 1970).

    Note that the ``Interval`` type does not currently provide date arithmetic
    operations on platforms which do not support interval types natively. Such
    operations usually require transformation of both sides of the expression
    (such as, conversion of both sides into integer epoch values first) which
    currently is a manual procedure (such as via
    :attr:`~sqlalchemy.sql.expression.func`).

    """

    impl = DateTime
    epoch = dt.datetime.utcfromtimestamp(0)

    def __init__(self, native=True,
                 second_precision=None,
                 day_precision=None):
        """Construct an Interval object.

        :param native: when True, use the actual
          INTERVAL type provided by the database, if
          supported (currently Postgresql, Oracle).
          Otherwise, represent the interval data as
          an epoch value regardless.

        :param second_precision: For native interval types
          which support a "fractional seconds precision" parameter,
          i.e. Oracle and Postgresql

        :param day_precision: for native interval types which
          support a "day precision" parameter, i.e. Oracle.

        """
        super(Interval, self).__init__()
        self.native = native
        self.second_precision = second_precision
        self.day_precision = day_precision

    def adapt(self, cls, **kw):
        if self.native and hasattr(cls, '_adapt_from_generic_interval'):
            return cls._adapt_from_generic_interval(self, **kw)
        else:
            return self.__class__(
                native=self.native,
                second_precision=self.second_precision,
                day_precision=self.day_precision,
                **kw)

    @property
    def python_type(self):
        return dt.timedelta

    def bind_processor(self, dialect):
        impl_processor = self.impl.bind_processor(dialect)
        epoch = self.epoch
        if impl_processor:
            def process(value):
                if value is not None:
                    value = epoch + value
                return impl_processor(value)
        else:
            def process(value):
                if value is not None:
                    value = epoch + value
                return value
        return process

    def result_processor(self, dialect, coltype):
        impl_processor = self.impl.result_processor(dialect, coltype)
        epoch = self.epoch
        if impl_processor:
            def process(value):
                value = impl_processor(value)
                if value is None:
                    return None
                return value - epoch
        else:
            def process(value):
                if value is None:
                    return None
                return value - epoch
        return process

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add: {
                Date: DateTime,
                Interval: self.__class__,
                DateTime: DateTime,
                Time: Time,
            },
            operators.sub: {
                Interval: self.__class__
            },
            operators.mul: {
                Numeric: self.__class__
            },
            operators.truediv: {
                Numeric: self.__class__
            },
            operators.div: {
                Numeric: self.__class__
            }
        }

    @property
    def _type_affinity(self):
        return Interval

    def coerce_compared_value(self, op, value):
        """See :meth:`.TypeEngine.coerce_compared_value` for a description."""

        return self.impl.coerce_compared_value(op, value)


class REAL(Float):

    """The SQL REAL type."""

    __visit_name__ = 'REAL'


class FLOAT(Float):

    """The SQL FLOAT type."""

    __visit_name__ = 'FLOAT'


class NUMERIC(Numeric):

    """The SQL NUMERIC type."""

    __visit_name__ = 'NUMERIC'


class DECIMAL(Numeric):

    """The SQL DECIMAL type."""

    __visit_name__ = 'DECIMAL'


class INTEGER(Integer):

    """The SQL INT or INTEGER type."""

    __visit_name__ = 'INTEGER'
INT = INTEGER


class SMALLINT(SmallInteger):

    """The SQL SMALLINT type."""

    __visit_name__ = 'SMALLINT'


class BIGINT(BigInteger):

    """The SQL BIGINT type."""

    __visit_name__ = 'BIGINT'


class TIMESTAMP(DateTime):

    """The SQL TIMESTAMP type."""

    __visit_name__ = 'TIMESTAMP'

    def get_dbapi_type(self, dbapi):
        return dbapi.TIMESTAMP


class DATETIME(DateTime):

    """The SQL DATETIME type."""

    __visit_name__ = 'DATETIME'


class DATE(Date):

    """The SQL DATE type."""

    __visit_name__ = 'DATE'


class TIME(Time):

    """The SQL TIME type."""

    __visit_name__ = 'TIME'


class TEXT(Text):

    """The SQL TEXT type."""

    __visit_name__ = 'TEXT'


class CLOB(Text):

    """The CLOB type.

    This type is found in Oracle and Informix.
    """

    __visit_name__ = 'CLOB'


class VARCHAR(String):

    """The SQL VARCHAR type."""

    __visit_name__ = 'VARCHAR'


class NVARCHAR(Unicode):

    """The SQL NVARCHAR type."""

    __visit_name__ = 'NVARCHAR'


class CHAR(String):

    """The SQL CHAR type."""

    __visit_name__ = 'CHAR'


class NCHAR(Unicode):

    """The SQL NCHAR type."""

    __visit_name__ = 'NCHAR'


class BLOB(LargeBinary):

    """The SQL BLOB type."""

    __visit_name__ = 'BLOB'


class BINARY(_Binary):

    """The SQL BINARY type."""

    __visit_name__ = 'BINARY'


class VARBINARY(_Binary):

    """The SQL VARBINARY type."""

    __visit_name__ = 'VARBINARY'


class BOOLEAN(Boolean):

    """The SQL BOOLEAN type."""

    __visit_name__ = 'BOOLEAN'


class NullType(TypeEngine):

    """An unknown type.

    :class:`.NullType` is used as a default type for those cases where
    a type cannot be determined, including:

    * During table reflection, when the type of a column is not recognized
      by the :class:`.Dialect`
    * When constructing SQL expressions using plain Python objects of
      unknown types (e.g. ``somecolumn == my_special_object``)
    * When a new :class:`.Column` is created, and the given type is passed
      as ``None`` or is not passed at all.

    The :class:`.NullType` can be used within SQL expression invocation
    without issue, it just has no behavior either at the expression
    construction level or at the bind-parameter/result processing level.
    :class:`.NullType` will result in a :exc:`.CompileError` if the compiler
    is asked to render the type itself, such as if it is used in a
    :func:`.cast` operation or within a schema creation operation such as that
    invoked by :meth:`.MetaData.create_all` or the :class:`.CreateTable`
    construct.

    """
    __visit_name__ = 'null'

    _isnull = True

    def literal_processor(self, dialect):
        def process(value):
            return "NULL"
        return process

    class Comparator(TypeEngine.Comparator):

        def _adapt_expression(self, op, other_comparator):
            if isinstance(other_comparator, NullType.Comparator) or \
                    not operators.is_commutative(op):
                return op, self.expr.type
            else:
                return other_comparator._adapt_expression(op, self)
    comparator_factory = Comparator


NULLTYPE = NullType()
BOOLEANTYPE = Boolean()
STRINGTYPE = String()
INTEGERTYPE = Integer()

_type_map = {
    int: Integer(),
    float: Numeric(),
    bool: BOOLEANTYPE,
    decimal.Decimal: Numeric(),
    dt.date: Date(),
    dt.datetime: DateTime(),
    dt.time: Time(),
    dt.timedelta: Interval(),
    util.NoneType: NULLTYPE
}

if util.py3k:
    _type_map[bytes] = LargeBinary()
    _type_map[str] = Unicode()
else:
    _type_map[unicode] = Unicode()
    _type_map[str] = String()


# back-assign to type_api
from . import type_api
type_api.BOOLEANTYPE = BOOLEANTYPE
type_api.STRINGTYPE = STRINGTYPE
type_api.INTEGERTYPE = INTEGERTYPE
type_api.NULLTYPE = NULLTYPE
type_api._type_map = _type_map

# this one, there's all kinds of ways to play it, but at the EOD
# there's just a giant dependency cycle between the typing system and
# the expression element system, as you might expect.   We can use
# importlaters or whatnot, but the typing system just necessarily has
# to have some kind of connection like this.  right now we're injecting the
# _DefaultColumnComparator implementation into the TypeEngine.Comparator
# interface.  Alternatively TypeEngine.Comparator could have an "impl"
# injected, though just injecting the base is simpler, error free, and more
# performant.


class Comparator(_DefaultColumnComparator):
    BOOLEANTYPE = BOOLEANTYPE

TypeEngine.Comparator.__bases__ = (
    Comparator, ) + TypeEngine.Comparator.__bases__

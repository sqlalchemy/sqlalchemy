# types.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines genericized SQL types, each represented by a subclass of
:class:`~sqlalchemy.types.AbstractType`.  Dialects define further subclasses of these
types.

For more information see the SQLAlchemy documentation on types.

"""
__all__ = [ 'TypeEngine', 'TypeDecorator', 'AbstractType', 'UserDefinedType',
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR','TEXT', 'Text',
            'FLOAT', 'NUMERIC', 'DECIMAL', 'TIMESTAMP', 'DATETIME', 'CLOB',
            'BLOB', 'BOOLEAN', 'SMALLINT', 'INTEGER', 'DATE', 'TIME',
            'String', 'Integer', 'SmallInteger', 'BigInteger', 'Numeric',
            'Float', 'DateTime', 'Date', 'Time', 'LargeBinary', 'Binary', 'Boolean',
            'Unicode', 'MutableType', 'Concatenable', 'UnicodeText',
            'PickleType', 'Interval', 'type_map', 'Enum' ]

import inspect
import datetime as dt
from decimal import Decimal as _python_Decimal
import codecs

from sqlalchemy import exc, schema
from sqlalchemy.sql import expression, operators
import sys
schema.types = expression.sqltypes =sys.modules['sqlalchemy.types']
from sqlalchemy.util import pickle
from sqlalchemy.sql.visitors import Visitable
from sqlalchemy import util
from sqlalchemy import processors
import collections

NoneType = type(None)
if util.jython:
    import array

class AbstractType(Visitable):
    
    def __init__(self, *args, **kwargs):
        pass

    def compile(self, dialect):
        return dialect.type_compiler.process(self)

    def copy_value(self, value):
        return value

    def bind_processor(self, dialect):
        """Defines a bind parameter processing function.
        
        :param dialect: Dialect instance in use.

        """

        return None

    def result_processor(self, dialect, coltype):
        """Defines a result-column processing function.
        
        :param dialect: Dialect instance in use.

        :param coltype: DBAPI coltype argument received in cursor.description.
        
        """

        return None

    def compare_values(self, x, y):
        """Compare two values for equality."""

        return x == y

    def is_mutable(self):
        """Return True if the target Python type is 'mutable'.

        This allows systems like the ORM to know if a column value can
        be considered 'not changed' by comparing the identity of
        objects alone.

        Use the :class:`MutableType` mixin or override this method to
        return True in custom types that hold mutable values such as
        ``dict``, ``list`` and custom objects.

        """
        return False

    def get_dbapi_type(self, dbapi):
        """Return the corresponding type object from the underlying DB-API, if
        any.
        
         This can be useful for calling ``setinputsizes()``, for example.

        """
        return None

    def _adapt_expression(self, op, othertype):
        """evaluate the return type of <self> <op> <othertype>,
        and apply any adaptations to the given operator.
        
        """
        return op, self
        
    @util.memoized_property
    def _type_affinity(self):
        """Return a rudimental 'affinity' value expressing the general class of type."""

        typ = None
        for t in self.__class__.__mro__:
            if t is TypeEngine or t is UserDefinedType:
                return typ
            elif issubclass(t, TypeEngine):
                typ = t
        else:
            return self.__class__
    
    def _coerce_compared_value(self, op, value):
        _coerced_type = type_map.get(type(value), NULLTYPE)
        if _coerced_type is NULLTYPE or _coerced_type._type_affinity is self._type_affinity:
            return self
        else:
            return _coerced_type
        
    def _compare_type_affinity(self, other):
        return self._type_affinity is other._type_affinity

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join("%s=%r" % (k, getattr(self, k, None))
                      for k in inspect.getargspec(self.__init__)[0][1:]))

class TypeEngine(AbstractType):
    """Base for built-in types."""

    @util.memoized_property
    def _impl_dict(self):
        return {}

    def dialect_impl(self, dialect, **kwargs):
        key = (dialect.__class__, dialect.server_version_info)
        
        try:
            return self._impl_dict[key]
        except KeyError:
            return self._impl_dict.setdefault(key, dialect.type_descriptor(self))

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('_impl_dict', None)
        return d

    def bind_processor(self, dialect):
        """Return a conversion function for processing bind values.

        Returns a callable which will receive a bind parameter value
        as the sole positional argument and will return a value to
        send to the DB-API.

        If processing is not necessary, the method should return ``None``.

        """
        return None

    def result_processor(self, dialect, coltype):
        """Return a conversion function for processing result row values.

        Returns a callable which will receive a result row column
        value as the sole positional argument and will return a value
        to return to the user.

        If processing is not necessary, the method should return ``None``.

        """
        return None

    def adapt(self, cls):
        return cls()

class UserDefinedType(TypeEngine):
    """Base for user defined types.

    This should be the base of new types.  Note that
    for most cases, :class:`TypeDecorator` is probably
    more appropriate::

      import sqlalchemy.types as types

      class MyType(types.UserDefinedType):
          def __init__(self, precision = 8):
              self.precision = precision

          def get_col_spec(self):
              return "MYTYPE(%s)" % self.precision

          def bind_processor(self, dialect):
              def process(value):
                  return value
              return process

          def result_processor(self, dialect, coltype):
              def process(value):
                  return value
              return process

    Once the type is made, it's immediately usable::

      table = Table('foo', meta,
          Column('id', Integer, primary_key=True),
          Column('data', MyType(16))
          )

    """
    __visit_name__ = "user_defined"

    def _adapt_expression(self, op, othertype):
        """evaluate the return type of <self> <op> <othertype>,
        and apply any adaptations to the given operator.
        
        """
        return self.adapt_operator(op), self

    def adapt_operator(self, op):
        """A hook which allows the given operator to be adapted
        to something new. 
        
        See also UserDefinedType._adapt_expression(), an as-yet-
        semi-public method with greater capability in this regard.
        
        """
        return op

class TypeDecorator(AbstractType):
    """Allows the creation of types which add additional functionality
    to an existing type.
    
    This method is preferred to direct subclassing of SQLAlchemy's
    built-in types as it ensures that all required functionality of 
    the underlying type is kept in place.

    Typical usage::

      import sqlalchemy.types as types

      class MyType(types.TypeDecorator):
          '''Prefixes Unicode values with "PREFIX:" on the way in and
          strips it off on the way out.
          '''

          impl = types.Unicode
          
          def process_bind_param(self, value, dialect):
              return "PREFIX:" + value

          def process_result_value(self, value, dialect):
              return value[7:]

          def copy(self):
              return MyType(self.impl.length)

    The class-level "impl" variable is required, and can reference any
    TypeEngine class.  Alternatively, the load_dialect_impl() method
    can be used to provide different type classes based on the dialect
    given; in this case, the "impl" variable can reference
    ``TypeEngine`` as a placeholder.

    Types that receive a Python type that isn't similar to the 
    ultimate type used may want to define the :meth:`TypeDecorator.coerce_compared_value`
    method.  This is used to give the expression system a hint 
    when coercing Python objects into bind parameters within expressions.  
    Consider this expression::
    
        mytable.c.somecol + datetime.date(2009, 5, 15)
        
    Above, if "somecol" is an ``Integer`` variant, it makes sense that 
    we're doing date arithmetic, where above is usually interpreted
    by databases as adding a number of days to the given date. 
    The expression system does the right thing by not attempting to
    coerce the "date()" value into an integer-oriented bind parameter.
    
    However, in the case of ``TypeDecorator``, we are usually changing
    an incoming Python type to something new - ``TypeDecorator`` by 
    default will "coerce" the non-typed side to be the same type as itself.
    Such as below, we define an "epoch" type that stores a date value as an integer::
    
        class MyEpochType(types.TypeDecorator):
            impl = types.Integer
            
            epoch = datetime.date(1970, 1, 1)
            
            def process_bind_param(self, value, dialect):
                return (value - self.epoch).days
            
            def process_result_value(self, value, dialect):
                return self.epoch + timedelta(days=value)

    Our expression of ``somecol + date`` with the above type will coerce the
    "date" on the right side to also be treated as ``MyEpochType``.  
    
    This behavior can be overridden via the :meth:`~TypeDecorator.coerce_compared_value`
    method, which returns a type that should be used for the value of the expression.
    Below we set it such that an integer value will be treated as an ``Integer``,
    and any other value is assumed to be a date and will be treated as a ``MyEpochType``::
    
        def coerce_compared_value(self, op, value):
            if isinstance(value, int):
                return Integer()
            else:
                return self

    """

    __visit_name__ = "type_decorator"

    def __init__(self, *args, **kwargs):
        if not hasattr(self.__class__, 'impl'):
            raise AssertionError("TypeDecorator implementations require a class-level "
                        "variable 'impl' which refers to the class of type being decorated")
        self.impl = self.__class__.impl(*args, **kwargs)
    
    def adapt(self, cls):
        return cls()
        
    def dialect_impl(self, dialect):
        key = (dialect.__class__, dialect.server_version_info)
        try:
            return self._impl_dict[key]
        except KeyError:
            pass

        # adapt the TypeDecorator first, in
        # the case that the dialect maps the TD
        # to one of its native types (i.e. PGInterval)
        adapted = dialect.type_descriptor(self)
        if adapted is not self:
            self._impl_dict[key] = adapted
            return adapted

        # otherwise adapt the impl type, link
        # to a copy of this TypeDecorator and return
        # that.
        typedesc = self.load_dialect_impl(dialect)
        tt = self.copy()
        if not isinstance(tt, self.__class__):
            raise AssertionError("Type object %s does not properly implement the copy() "
                    "method, it must return an object of type %s" % (self, self.__class__))
        tt.impl = typedesc
        self._impl_dict[key] = tt
        return tt

    @util.memoized_property
    def _type_affinity(self):
        return self.impl._type_affinity

    def type_engine(self, dialect):
        impl = self.dialect_impl(dialect)
        if not isinstance(impl, TypeDecorator):
            return impl
        else:
            return impl.impl

    def load_dialect_impl(self, dialect):
        """Loads the dialect-specific implementation of this type.

        by default calls dialect.type_descriptor(self.impl), but
        can be overridden to provide different behavior.

        """
        if isinstance(self.impl, TypeDecorator):
            return self.impl.dialect_impl(dialect)
        else:
            return dialect.type_descriptor(self.impl)

    def __getattr__(self, key):
        """Proxy all other undefined accessors to the underlying implementation."""

        return getattr(self.impl, key)

    def process_bind_param(self, value, dialect):
        raise NotImplementedError()

    def process_result_value(self, value, dialect):
        raise NotImplementedError()

    def bind_processor(self, dialect):
        if self.__class__.process_bind_param.func_code is not TypeDecorator.process_bind_param.func_code:
            process_param = self.process_bind_param
            impl_processor = self.impl.bind_processor(dialect)
            if impl_processor:
                def process(value):
                    return impl_processor(process_param(value, dialect))
            else:
                def process(value):
                    return process_param(value, dialect)
            return process
        else:
            return self.impl.bind_processor(dialect)

    def result_processor(self, dialect, coltype):
        if self.__class__.process_result_value.func_code is not TypeDecorator.process_result_value.func_code:
            process_value = self.process_result_value
            impl_processor = self.impl.result_processor(dialect, coltype)
            if impl_processor:
                def process(value):
                    return process_value(impl_processor(value), dialect)
            else:
                def process(value):
                    return process_value(value, dialect)
            return process
        else:
            return self.impl.result_processor(dialect, coltype)
    
    def coerce_compared_value(self, op, value):
        """Suggest a type for a 'coerced' Python value in an expression.
        
        By default, returns self.   This method is called by
        the expression system when an object using this type is 
        on the left or right side of an expression against a plain Python
        object which does not yet have a SQLAlchemy type assigned::
        
            expr = table.c.somecolumn + 35
            
        Where above, if ``somecolumn`` uses this type, this method will
        be called with the value ``operator.add``
        and ``35``.  The return value is whatever SQLAlchemy type should
        be used for ``35`` for this particular operation.
        
        """
        return self

    def _coerce_compared_value(self, op, value):
        return self.coerce_compared_value(op, value)
        
    def copy(self):
        instance = self.__class__.__new__(self.__class__)
        instance.__dict__.update(self.__dict__)
        instance._impl_dict = {}
        return instance

    def get_dbapi_type(self, dbapi):
        return self.impl.get_dbapi_type(dbapi)

    def copy_value(self, value):
        return self.impl.copy_value(value)

    def compare_values(self, x, y):
        return self.impl.compare_values(x, y)

    def is_mutable(self):
        return self.impl.is_mutable()

    def _adapt_expression(self, op, othertype):
        return self.impl._adapt_expression(op, othertype)



class MutableType(object):
    """A mixin that marks a :class:`TypeEngine` as representing
    a mutable Python object type.

    "mutable" means that changes can occur in place to a value 
    of this type.   Examples includes Python lists, dictionaries,
    and sets, as well as user-defined objects.  The primary
    need for identification of "mutable" types is by the ORM, 
    which applies special rules to such values in order to guarantee 
    that changes are detected.  These rules may have a significant 
    performance impact, described below.
    
    A :class:`MutableType` usually allows a flag called
    ``mutable=True`` to enable/disable the "mutability" flag,
    represented on this class by :meth:`is_mutable`.  Examples 
    include :class:`PickleType` and 
    :class:`~sqlalchemy.dialects.postgresql.base.ARRAY`.  Setting
    this flag to ``False`` effectively disables any mutability-
    specific behavior by the ORM.
    
    :meth:`copy_value` and :meth:`compare_values` represent a copy
    and compare function for values of this type - implementing
    subclasses should override these appropriately.

    The usage of mutable types has significant performance
    implications when using the ORM. In order to detect changes, the
    ORM must create a copy of the value when it is first
    accessed, so that changes to the current value can be compared
    against the "clean" database-loaded value. Additionally, when the
    ORM checks to see if any data requires flushing, it must scan
    through all instances in the session which are known to have
    "mutable" attributes and compare the current value of each
    one to its "clean"
    value. So for example, if the Session contains 6000 objects (a
    fairly large amount) and autoflush is enabled, every individual
    execution of :class:`Query` will require a full scan of that subset of
    the 6000 objects that have mutable attributes, possibly resulting
    in tens of thousands of additional method calls for every query.
    
    Note that for small numbers (< 100 in the Session at a time)
    of objects with "mutable" values, the performance degradation is 
    negligible.  In most cases it's likely that the convenience allowed 
    by "mutable" change detection outweighs the performance penalty.
    
    It is perfectly fine to represent "mutable" data types with the
    "mutable" flag set to False, which eliminates any performance
    issues. It means that the ORM will only reliably detect changes
    for values of this type if a newly modified value is of a different 
    identity (i.e., ``id(value)``) than what was present before - 
    i.e., instead of operations like these::
    
        myobject.somedict['foo'] = 'bar'
        myobject.someset.add('bar')
        myobject.somelist.append('bar')
        
    You'd instead say::
    
        myobject.somevalue = {'foo':'bar'}
        myobject.someset = myobject.someset.union(['bar'])
        myobject.somelist = myobject.somelist + ['bar']
        
    A future release of SQLAlchemy will include instrumented
    collection support for mutable types, such that at least usage of
    plain Python datastructures will be able to emit events for
    in-place changes, removing the need for pessimistic scanning for
    changes.

    """

    def is_mutable(self):
        """Return True, mutable."""
        return True

    def copy_value(self, value):
        """Unimplemented."""
        raise NotImplementedError()

    def compare_values(self, x, y):
        """Compare *x* == *y*."""
        return x == y

def to_instance(typeobj):
    if typeobj is None:
        return NULLTYPE

    if util.callable(typeobj):
        return typeobj()
    else:
        return typeobj

def adapt_type(typeobj, colspecs):
    if isinstance(typeobj, type):
        typeobj = typeobj()
    for t in typeobj.__class__.__mro__[0:-1]:
        try:
            impltype = colspecs[t]
            break
        except KeyError:
            pass
    else:
        # couldnt adapt - so just return the type itself
        # (it may be a user-defined type)
        return typeobj
    # if we adapted the given generic type to a database-specific type,
    # but it turns out the originally given "generic" type
    # is actually a subclass of our resulting type, then we were already
    # given a more specific type than that required; so use that.
    if (issubclass(typeobj.__class__, impltype)):
        return typeobj
    return typeobj.adapt(impltype)

class NullType(TypeEngine):
    """An unknown type.

    NullTypes will stand in if :class:`~sqlalchemy.Table` reflection
    encounters a column data type unknown to SQLAlchemy.  The
    resulting columns are nearly fully usable: the DB-API adapter will
    handle all translation to and from the database data type.

    NullType does not have sufficient information to particpate in a
    ``CREATE TABLE`` statement and will raise an exception if
    encountered during a :meth:`~sqlalchemy.Table.create` operation.

    """
    __visit_name__ = 'null'

    def _adapt_expression(self, op, othertype):
        if othertype is NULLTYPE or not operators.is_commutative(op):
            return op, self
        else:
            return othertype._adapt_expression(op, self)

NullTypeEngine = NullType

class Concatenable(object):
    """A mixin that marks a type as supporting 'concatenation', typically strings."""

    def _adapt_expression(self, op, othertype):
        if op is operators.add and issubclass(othertype._type_affinity, (Concatenable, NullType)):
            return operators.concat_op, self
        else:
            return op, self

class _DateAffinity(object):
    """Mixin date/time specific expression adaptations.
    
    Rules are implemented within Date,Time,Interval,DateTime, Numeric, Integer.
    Based on http://www.postgresql.org/docs/current/static/functions-datetime.html.
    
    """
    
    @property
    def _expression_adaptations(self):
        raise NotImplementedError()

    _blank_dict = util.frozendict()
    def _adapt_expression(self, op, othertype):
        othertype = othertype._type_affinity
        return op, \
                self._expression_adaptations.get(op, self._blank_dict).\
                get(othertype, NULLTYPE)

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

    def __init__(self, length=None, convert_unicode=False, 
                        assert_unicode=None, unicode_error=None,
                        _warn_on_bytestring=False
                        ):
        """
        Create a string-holding type.

        :param length: optional, a length for the column for use in
          DDL statements.  May be safely omitted if no ``CREATE
          TABLE`` will be issued.  Certain databases may require a
          *length* for use in DDL, and will raise an exception when
          the ``CREATE TABLE`` DDL is issued.  Whether the value is
          interpreted as bytes or characters is database specific.

        :param convert_unicode: defaults to False.  If True, the 
          type will do what is necessary in order to accept 
          Python Unicode objects as bind parameters, and to return
          Python Unicode objects in result rows.   This may
          require SQLAlchemy to explicitly coerce incoming Python 
          unicodes into an encoding, and from an encoding 
          back to Unicode, or it may not require any interaction
          from SQLAlchemy at all, depending on the DBAPI in use.

          When SQLAlchemy performs the encoding/decoding, 
          the encoding used is configured via
          :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which
          defaults to `utf-8`.

          The "convert_unicode" behavior can also be turned on
          for all String types by setting 
          :attr:`sqlalchemy.engine.base.Dialect.convert_unicode`
          on create_engine().
          
          To instruct SQLAlchemy to perform Unicode encoding/decoding
          even on a platform that already handles Unicode natively,
          set convert_unicode='force'.  This will incur significant
          performance overhead when fetching unicode result columns.
          
        :param assert_unicode: Deprecated.  A warning is raised in all cases when a non-Unicode
          object is passed when SQLAlchemy would coerce into an encoding
          (note: but **not** when the DBAPI handles unicode objects natively).
          To suppress or raise this warning to an 
          error, use the Python warnings filter documented at:
          http://docs.python.org/library/warnings.html

        :param unicode_error: Optional, a method to use to handle Unicode
          conversion errors. Behaves like the 'errors' keyword argument to
          the standard library's string.decode() functions.   This flag
          requires that `convert_unicode` is set to `"force"` - otherwise,
          SQLAlchemy is not guaranteed to handle the task of unicode
          conversion.   Note that this flag adds significant performance
          overhead to row-fetching operations for backends that already
          return unicode objects natively (which most DBAPIs do).  This
          flag should only be used as an absolute last resort for reading
          strings from a column with varied or corrupted encodings,
          which only applies to databases that accept invalid encodings 
          in the first place (i.e. MySQL.  *not* PG, Sqlite, etc.)

        """
        if unicode_error is not None and convert_unicode != 'force':
            raise exc.ArgumentError("convert_unicode must be 'force' "
                                        "when unicode_error is set.")
        
        if assert_unicode:
            util.warn_deprecated("assert_unicode is deprecated. "
                                "SQLAlchemy emits a warning in all cases where it "
                                "would otherwise like to encode a Python unicode object "
                                "into a specific encoding but a plain bytestring is received. "
                                "This does *not* apply to DBAPIs that coerce Unicode natively."
                                )
        self.length = length
        self.convert_unicode = convert_unicode
        self.unicode_error = unicode_error
        self._warn_on_bytestring = _warn_on_bytestring
        
    def adapt(self, impltype):
        return impltype(
                    length=self.length,
                    convert_unicode=self.convert_unicode,
                    unicode_error=self.unicode_error,
                    _warn_on_bytestring=True,
                    )

    def bind_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            if dialect.supports_unicode_binds and self.convert_unicode != 'force':
                if self._warn_on_bytestring:
                    def process(value):
                        # Py3K
                        #if isinstance(value, bytes):
                        # Py2K
                        if isinstance(value, str):
                        # end Py2K
                            util.warn("Unicode type received non-unicode bind "
                                      "param value.")
                        return value
                    return process
                else:
                    return None
            else:
                encoder = codecs.getencoder(dialect.encoding)
                def process(value):
                    if isinstance(value, unicode):
                        return encoder(value, self.unicode_error)[0]
                    elif value is not None:
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
                        self.convert_unicode == 'force')
       
        if needs_convert:
            to_unicode = processors.to_unicode_processor_factory(
                                    dialect.encoding, self.unicode_error)
            
            if dialect.returns_unicode_strings:
                # we wouldn't be here unless convert_unicode='force'
                # was specified, or the driver has erratic unicode-returning
                # habits.  since we will be getting back unicode
                # in most cases, we check for it (decode will fail).   
                def process(value):
                    if isinstance(value, unicode):
                        return value
                    else:
                        return to_unicode(value)
                return process
            else:
                # here, we assume that the object is not unicode,
                # avoiding expensive isinstance() check.
                return to_unicode
        else:
            return None

    def get_dbapi_type(self, dbapi):
        return dbapi.STRING

class Text(String):
    """A variably sized string type.

    In SQL, usually corresponds to CLOB or TEXT. Can also take Python
    unicode objects and encode to the database's encoding in bind
    params (and the reverse for result sets.)

    """
    __visit_name__ = 'text'

class Unicode(String):
    """A variable length Unicode string.

    The ``Unicode`` type is a :class:`String` which converts Python
    ``unicode`` objects (i.e., strings that are defined as
    ``u'somevalue'``) into encoded bytestrings when passing the value
    to the database driver, and similarly decodes values from the
    database back into Python ``unicode`` objects.
    
    It's roughly equivalent to using a ``String`` object with
    ``convert_unicode=True``, however
    the type has other significances in that it implies the usage 
    of a unicode-capable type being used on the backend, such as NVARCHAR.
    This may affect what type is emitted when issuing CREATE TABLE
    and also may effect some DBAPI-specific details, such as type
    information passed along to ``setinputsizes()``.
    
    When using the ``Unicode`` type, it is only appropriate to pass
    Python ``unicode`` objects, and not plain ``str``.  If a
    bytestring (``str``) is passed, a runtime warning is issued.  If
    you notice your application raising these warnings but you're not
    sure where, the Python ``warnings`` filter can be used to turn
    these warnings into exceptions which will illustrate a stack
    trace::

      import warnings
      warnings.simplefilter('error')

    Bytestrings sent to and received from the database are encoded
    using the dialect's
    :attr:`~sqlalchemy.engine.base.Dialect.encoding`, which defaults
    to `utf-8`.

    """

    __visit_name__ = 'unicode'
    
    def __init__(self, length=None, **kwargs):
        """
        Create a Unicode-converting String type.

        :param length: optional, a length for the column for use in
          DDL statements.  May be safely omitted if no ``CREATE
          TABLE`` will be issued.  Certain databases may require a
          *length* for use in DDL, and will raise an exception when
          the ``CREATE TABLE`` DDL is issued.  Whether the value is
          interpreted as bytes or characters is database specific.
        
        :param \**kwargs: passed through to the underlying ``String``
          type.
          
        """
        kwargs.setdefault('convert_unicode', True)
        kwargs.setdefault('_warn_on_bytestring', True)
        super(Unicode, self).__init__(length=length, **kwargs)

class UnicodeText(Text):
    """An unbounded-length Unicode string.

    See :class:`Unicode` for details on the unicode
    behavior of this object.

    Like ``Unicode``, usage the ``UnicodeText`` type implies a 
    unicode-capable type being used on the backend, such as NCLOB.

    """

    __visit_name__ = 'unicode_text'

    def __init__(self, length=None, **kwargs):
        """
        Create a Unicode-converting Text type.

        :param length: optional, a length for the column for use in
          DDL statements.  May be safely omitted if no ``CREATE
          TABLE`` will be issued.  Certain databases may require a
          *length* for use in DDL, and will raise an exception when
          the ``CREATE TABLE`` DDL is issued.  Whether the value is
          interpreted as bytes or characters is database specific.

        """
        kwargs.setdefault('convert_unicode', True)
        kwargs.setdefault('_warn_on_bytestring', True)
        super(UnicodeText, self).__init__(length=length, **kwargs)


class Integer(_DateAffinity, TypeEngine):
    """A type for ``int`` integers."""

    __visit_name__ = 'integer'

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER
    
    @util.memoized_property
    def _expression_adaptations(self):
        # TODO: need a dictionary object that will
        # handle operators generically here, this is incomplete
        return {
            operators.add:{
                Date:Date,
                Integer:Integer,
                Numeric:Numeric,
            },
            operators.mul:{
                Interval:Interval,
                Integer:Integer,
                Numeric:Numeric,
            },
            # Py2K
            operators.div:{
                Integer:Integer,
                Numeric:Numeric,
            },
            # end Py2K
            operators.truediv:{
                Integer:Integer,
                Numeric:Numeric,
            },
            operators.sub:{
                Integer:Integer,
                Numeric:Numeric,
            },
        }

class SmallInteger(Integer):
    """A type for smaller ``int`` integers.

    Typically generates a ``SMALLINT`` in DDL, and otherwise acts like
    a normal :class:`Integer` on the Python side.

    """

    __visit_name__ = 'small_integer'

class BigInteger(Integer):
    """A type for bigger ``int`` integers.

    Typically generates a ``BIGINT`` in DDL, and otherwise acts like
    a normal :class:`Integer` on the Python side.

    """

    __visit_name__ = 'big_integer'

class Numeric(_DateAffinity, TypeEngine):
    """A type for fixed precision numbers.

    Typically generates DECIMAL or NUMERIC.  Returns
    ``decimal.Decimal`` objects by default, applying
    conversion as needed.

    """

    __visit_name__ = 'numeric'

    def __init__(self, precision=None, scale=None, asdecimal=True):
        """
        Construct a Numeric.

        :param precision: the numeric precision for use in DDL ``CREATE TABLE``.

        :param scale: the numeric scale for use in DDL ``CREATE TABLE``.

        :param asdecimal: default True.  Return whether or not
          values should be sent as Python Decimal objects, or
          as floats.   Different DBAPIs send one or the other based on
          datatypes - the Numeric type will ensure that return values
          are one or the other across DBAPIs consistently.  
          
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
        self.asdecimal = asdecimal

    def adapt(self, impltype):
        return impltype(
                precision=self.precision, 
                scale=self.scale, 
                asdecimal=self.asdecimal)

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

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
                util.warn("Dialect %s+%s does *not* support Decimal objects natively, "
                            "and SQLAlchemy must convert from floating point - "
                            "rounding errors and other issues may occur. "
                            "Please consider storing Decimal numbers as strings or "
                            "integers on this platform for lossless storage." % 
                            (dialect.name, dialect.driver))
                
                # we're a "numeric", DBAPI returns floats, convert.
                if self.scale is not None:
                    return processors.to_decimal_processor_factory(_python_Decimal, self.scale)
                else:
                    return processors.to_decimal_processor_factory(_python_Decimal)
        else:
            if dialect.supports_native_decimal:
                return processors.to_float
            else:
                return None

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.mul:{
                Interval:Interval,
                Numeric:Numeric,
            },
            # Py2K
            operators.div:{
                Numeric:Numeric,
            },
            # end Py2K
            operators.truediv:{
                Numeric:Numeric,
            },
            operators.add:{
                Numeric:Numeric,
            },
            operators.sub:{
                Numeric:Numeric,
            }
        }

class Float(Numeric):
    """A type for ``float`` numbers.  
    
    Returns Python ``float`` objects by default, applying
    conversion as needed.
    
    """

    __visit_name__ = 'float'

    def __init__(self, precision=None, asdecimal=False, **kwargs):
        """
        Construct a Float.

        :param precision: the numeric precision for use in DDL ``CREATE TABLE``.
        
        :param asdecimal: the same flag as that of :class:`Numeric`, but
          defaults to ``False``.   Note that setting this flag to ``True``
          results in floating point conversion.

        """
        self.precision = precision
        self.asdecimal = asdecimal

    def adapt(self, impltype):
        return impltype(precision=self.precision, asdecimal=self.asdecimal)

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return processors.to_decimal_processor_factory(_python_Decimal)
        else:
            return None

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.mul:{
                Interval:Interval,
                Numeric:Float,
            },
            # Py2K
            operators.div:{
                Numeric:Float,
            },
            # end Py2K
            operators.truediv:{
                Numeric:Float,
            },
            operators.add:{
                Numeric:Float,
            },
            operators.sub:{
                Numeric:Float,
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
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add:{
                Interval:DateTime,
            },
            operators.sub:{
                Interval:DateTime,
                DateTime:Interval,
            },
        }
        

class Date(_DateAffinity,TypeEngine):
    """A type for ``datetime.date()`` objects."""

    __visit_name__ = 'date'

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add:{
                Integer:Date,
                Interval:DateTime,
                Time:DateTime,
            },
            operators.sub:{
                # date - integer = date
                Integer:Date,
                
                # date - date = integer.
                Date:Integer,

                Interval:DateTime,
                
                # date - datetime = interval,
                # this one is not in the PG docs 
                # but works
                DateTime:Interval,
            },
        }


class Time(_DateAffinity,TypeEngine):
    """A type for ``datetime.time()`` objects."""

    __visit_name__ = 'time'

    def __init__(self, timezone=False):
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

    @util.memoized_property
    def _expression_adaptations(self):
        return {
            operators.add:{
                Date:DateTime,
                Interval:Time
            },
            operators.sub:{
                Time:Interval,
                Interval:Time,
            },
        }


class _Binary(TypeEngine):
    """Define base behavior for binary types."""

    def __init__(self, length=None):
        self.length = length

    # Python 3 - sqlite3 doesn't need the `Binary` conversion
    # here, though pg8000 does to indicate "bytea"
    def bind_processor(self, dialect):
        DBAPIBinary = dialect.dbapi.Binary
        def process(value):
            if value is not None:
                return DBAPIBinary(value)
            else:
                return None
        return process

    # Python 3 has native bytes() type 
    # both sqlite3 and pg8000 seem to return it
    # (i.e. and not 'memoryview')
    # Py2K
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
    # end Py2K

    def _coerce_compared_value(self, op, value):
        if isinstance(value, basestring):
            return self
        else:
            return super(_Binary, self)._coerce_compared_value(op, value)
    
    def adapt(self, impltype):
        return impltype(length=self.length)

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
        util.warn_deprecated("The Binary type has been renamed to LargeBinary.")
        LargeBinary.__init__(self, *arg, **kw)

class SchemaType(object):
    """Mark a type as possibly requiring schema-level DDL for usage.
    
    Supports types that must be explicitly created/dropped (i.e. PG ENUM type)
    as well as types that are complimented by table or schema level
    constraints, triggers, and other rules.
    
    """
    
    def __init__(self, **kw):
        self.name = kw.pop('name', None)
        self.quote = kw.pop('quote', None)
        self.schema = kw.pop('schema', None)
        self.metadata = kw.pop('metadata', None)
        if self.metadata:
            self.metadata.append_ddl_listener(
                                    'before-create',
                                    util.portable_instancemethod(self._on_metadata_create)
                                    )
            self.metadata.append_ddl_listener(
                                    'after-drop',
                                    util.portable_instancemethod(self._on_metadata_drop)
                                    )
            
    def _set_parent(self, column):
        column._on_table_attach(util.portable_instancemethod(self._set_table))
        
    def _set_table(self, table, column):
        table.append_ddl_listener(
                            'before-create', 
                            util.portable_instancemethod(self._on_table_create)
                            )
        table.append_ddl_listener(
                            'after-drop', 
                            util.portable_instancemethod(self._on_table_drop)
                            )
        if self.metadata is None:
            table.metadata.append_ddl_listener(
                            'before-create',
                            util.portable_instancemethod(self._on_metadata_create)
                            )
            table.metadata.append_ddl_listener(
                            'after-drop',
                            util.portable_instancemethod(self._on_metadata_drop)
                            )
    
    @property
    def bind(self):
        return self.metadata and self.metadata.bind or None
        
    def create(self, bind=None, checkfirst=False):
        """Issue CREATE ddl for this type, if applicable."""
        
        if bind is None:
            bind = schema._bind_or_error(self)
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t.create(bind=bind, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=False):
        """Issue DROP ddl for this type, if applicable."""

        if bind is None:
            bind = schema._bind_or_error(self)
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t.drop(bind=bind, checkfirst=checkfirst)
        
    def _on_table_create(self, event, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t._on_table_create(event, target, bind, **kw)

    def _on_table_drop(self, event, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t._on_table_drop(event, target, bind, **kw)

    def _on_metadata_create(self, event, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t._on_metadata_create(event, target, bind, **kw)

    def _on_metadata_drop(self, event, target, bind, **kw):
        t = self.dialect_impl(bind.dialect)
        if t is not self and isinstance(t, SchemaType):
            t._on_metadata_drop(event, target, bind, **kw)
    
class Enum(String, SchemaType):
    """Generic Enum Type.
    
    The Enum type provides a set of possible string values which the 
    column is constrained towards.
    
    By default, uses the backend's native ENUM type if available, 
    else uses VARCHAR + a CHECK constraint.
    """
    
    __visit_name__ = 'enum'
    
    def __init__(self, *enums, **kw):
        """Construct an enum.
        
        Keyword arguments which don't apply to a specific backend are ignored
        by that backend.

        :param \*enums: string or unicode enumeration labels. If unicode labels
            are present, the `convert_unicode` flag is auto-enabled.

        :param convert_unicode: Enable unicode-aware bind parameter and result-set
            processing for this Enum's data. This is set automatically based on
            the presence of unicode label strings.

        :param metadata: Associate this type directly with a ``MetaData`` object.
            For types that exist on the target database as an independent schema
            construct (Postgresql), this type will be created and dropped within
            ``create_all()`` and ``drop_all()`` operations. If the type is not
            associated with any ``MetaData`` object, it will associate itself with
            each ``Table`` in which it is used, and will be created when any of
            those individual tables are created, after a check is performed for
            it's existence. The type is only dropped when ``drop_all()`` is called
            for that ``Table`` object's metadata, however.

        :param name: The name of this type. This is required for Postgresql and
            any future supported database which requires an explicitly named type,
            or an explicitly named constraint in order to generate the type and/or
            a table that uses it.

        :param native_enum: Use the database's native ENUM type when available.
            Defaults to True.  When False, uses VARCHAR + check constraint
            for all backends.

        :param schema: Schemaname of this type. For types that exist on the target
            database as an independent schema construct (Postgresql), this
            parameter specifies the named schema in which the type is present.

        :param quote: Force quoting to be on or off on the type's name. If left as
            the default of `None`, the usual schema-level "case
            sensitive"/"reserved name" rules are used to determine if this type's
            name should be quoted.

        """
        self.enums = enums
        self.native_enum = kw.pop('native_enum', True)
        convert_unicode= kw.pop('convert_unicode', None)
        if convert_unicode is None:
            for e in enums:
                if isinstance(e, unicode):
                    convert_unicode = True
                    break
            else:
                convert_unicode = False
        
        if self.enums:
            length =max(len(x) for x in self.enums)
        else:
            length = 0
        String.__init__(self, 
                        length =length,
                        convert_unicode=convert_unicode, 
                        )
        SchemaType.__init__(self, **kw)

    def _should_create_constraint(self, compiler):
        return not self.native_enum or \
                    not compiler.dialect.supports_native_enum
    
    def _set_table(self, table, column):
        if self.native_enum:
            SchemaType._set_table(self, table, column)
            

        e = schema.CheckConstraint(
                        column.in_(self.enums),
                        name=self.name,
                        _create_rule=util.portable_instancemethod(self._should_create_constraint)
                    )
        table.append_constraint(e)
        
    def adapt(self, impltype):
        if issubclass(impltype, Enum):
            return impltype(name=self.name, 
                        quote=self.quote, 
                        schema=self.schema, 
                        metadata=self.metadata,
                        convert_unicode=self.convert_unicode,
                        native_enum=self.native_enum,
                        *self.enums
                        )
        else:
            return super(Enum, self).adapt(impltype)

class PickleType(MutableType, TypeDecorator):
    """Holds Python objects, which are serialized using pickle.

    PickleType builds upon the Binary type to apply Python's
    ``pickle.dumps()`` to incoming objects, and ``pickle.loads()`` on
    the way out, allowing any pickleable Python object to be stored as
    a serialized binary field.

    **Note:** be sure to read the notes for :class:`MutableType` regarding
    ORM performance implications.
    
    """

    impl = LargeBinary

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL, pickler=None, mutable=True, comparator=None):
        """
        Construct a PickleType.

        :param protocol: defaults to ``pickle.HIGHEST_PROTOCOL``.

        :param pickler: defaults to cPickle.pickle or pickle.pickle if
          cPickle is not available.  May be any object with
          pickle-compatible ``dumps` and ``loads`` methods.

        :param mutable: defaults to True; implements
          :meth:`AbstractType.is_mutable`.   When ``True``, incoming
          objects should provide an ``__eq__()`` method which
          performs the desired deep comparison of members, or the
          ``comparator`` argument must be present.  

        :param comparator: optional. a 2-arg callable predicate used
          to compare values of this type.  Otherwise, 
          the == operator is used to compare values.

        """
        self.protocol = protocol
        self.pickler = pickler or pickle
        self.mutable = mutable
        self.comparator = comparator
        super(PickleType, self).__init__()

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

    def copy_value(self, value):
        if self.mutable:
            return self.pickler.loads(self.pickler.dumps(value, self.protocol))
        else:
            return value

    def compare_values(self, x, y):
        if self.comparator:
            return self.comparator(x, y)
        else:
            return x == y

    def is_mutable(self):
        return self.mutable


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
        
    def _set_table(self, table, column):
        if not self.create_constraint:
            return
            
        e = schema.CheckConstraint(
                        column.in_([0, 1]),
                        name=self.name,
                        _create_rule=util.portable_instancemethod(self._should_create_constraint)
                    )
        table.append_constraint(e)
    
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

    Note that the ``Interval`` type does not currently provide 
    date arithmetic operations on platforms which do not support 
    interval types natively.   Such operations usually require
    transformation of both sides of the expression (such as, conversion
    of both sides into integer epoch values first) which currently
    is a manual procedure (such as via :attr:`~sqlalchemy.sql.expression.func`).
    
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

    def adapt(self, cls):
        if self.native:
            return cls._adapt_from_generic_interval(self)
        else:
            return self
    
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
            operators.add:{
                Date:DateTime,
                Interval:Interval,
                DateTime:DateTime,
                Time:Time,
            },
            operators.sub:{
                Interval:Interval
            },
            operators.mul:{
                Numeric:Interval
            },
            operators.truediv: {
                Numeric:Interval
            },
            # Py2K
            operators.div: {
                Numeric:Interval
            }
            # end Py2K
        }

    @property
    def _type_affinity(self):
        return Interval

    def _coerce_compared_value(self, op, value):
        return self.impl._coerce_compared_value(op, value)


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

NULLTYPE = NullType()
BOOLEANTYPE = Boolean()
STRINGTYPE = String()

# using VARCHAR/NCHAR so that we dont get the genericized "String"
# type which usually resolves to TEXT/CLOB
type_map = {
    str: String(),
    # Py3K
    #bytes : LargeBinary(),
    # Py2K
    unicode : Unicode(),
    # end Py2K
    int : Integer(),
    float : Numeric(),
    bool: BOOLEANTYPE,
    _python_Decimal : Numeric(),
    dt.date : Date(),
    dt.datetime : DateTime(),
    dt.time : Time(),
    dt.timedelta : Interval(),
    NoneType: NULLTYPE
}


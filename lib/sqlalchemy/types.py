# types.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = [ 'TypeEngine', 'TypeDecorator', 'NullTypeEngine',
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'TEXT', 'FLOAT', 'DECIMAL',
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'SmallInteger','Smallinteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'Binary', 'Boolean', 'Unicode', 'PickleType', 'NULLTYPE',
        'SMALLINT', 'DATE', 'TIME','Interval'
            ]

from sqlalchemy import util, exceptions
import inspect, weakref
import datetime as dt
try:
    import cPickle as pickle
except:
    import pickle

_impl_cache = weakref.WeakKeyDictionary()

class AbstractType(object):
    def _get_impl_dict(self):
        try:
            return _impl_cache[self]
        except KeyError:
            return _impl_cache.setdefault(self, {})

    impl_dict = property(_get_impl_dict)

    def copy_value(self, value):
        return value

    def compare_values(self, x, y):
        return x is y

    def is_mutable(self):
        return False

    def get_dbapi_type(self, dbapi):
        """Return the corresponding type object from the underlying DBAPI, if any.

        This can be useful for calling ``setinputsizes()``, for example.
        """

        return None

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, ",".join(["%s=%s" % (k, getattr(self, k)) for k in inspect.getargspec(self.__init__)[0][1:]]))

class TypeEngine(AbstractType):
    def __init__(self, *args, **params):
        pass

    def dialect_impl(self, dialect):
        try:
            return self.impl_dict[dialect]
        except KeyError:
            return self.impl_dict.setdefault(dialect, dialect.type_descriptor(self))

    def get_col_spec(self):
        raise NotImplementedError()

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        return value

    def adapt(self, cls):
        return cls()
    
    def get_search_list(self):
        """return a list of classes to test for a match 
        when adapting this type to a dialect-specific type.
        
        """
        
        return self.__class__.__mro__[0:-1]
        
class TypeDecorator(AbstractType):
    def __init__(self, *args, **kwargs):
        if not hasattr(self.__class__, 'impl'):
            raise exceptions.AssertionError("TypeDecorator implementations require a class-level variable 'impl' which refers to the class of type being decorated")
        self.impl = self.__class__.impl(*args, **kwargs)

    def dialect_impl(self, dialect):
        try:
            return self.impl_dict[dialect]
        except:
            typedesc = self.load_dialect_impl(dialect)
            tt = self.copy()
            if not isinstance(tt, self.__class__):
                raise exceptions.AssertionError("Type object %s does not properly implement the copy() method, it must return an object of type %s" % (self, self.__class__))
            tt.impl = typedesc
            self.impl_dict[dialect] = tt
            return tt

    def load_dialect_impl(self, dialect):
        """loads the dialect-specific implementation of this type.
        
        by default calls dialect.type_descriptor(self.impl), but
        can be overridden to provide different behavior.
        """

        return dialect.type_descriptor(self.impl)
        
    def __getattr__(self, key):
        """Proxy all other undefined accessors to the underlying implementation."""

        return getattr(self.impl, key)

    def get_col_spec(self):
        return self.impl.get_col_spec()

    def convert_bind_param(self, value, dialect):
        return self.impl.convert_bind_param(value, dialect)

    def convert_result_value(self, value, dialect):
        return self.impl.convert_result_value(value, dialect)

    def copy(self):
        instance = self.__class__.__new__(self.__class__)
        instance.__dict__.update(self.__dict__)
        return instance

    def get_dbapi_type(self, dbapi):
        return self.impl.get_dbapi_type(dbapi)

    def copy_value(self, value):
        return self.impl.copy_value(value)

    def compare_values(self, x, y):
        return self.impl.compare_values(x,y)

    def is_mutable(self):
        return self.impl.is_mutable()

class MutableType(object):
    """A mixin that marks a Type as holding a mutable object."""

    def is_mutable(self):
        return True

    def copy_value(self, value):
        raise NotImplementedError()

    def compare_values(self, x, y):
        return x == y

def to_instance(typeobj):
    if typeobj is None:
        return NULLTYPE
    elif isinstance(typeobj, type):
        return typeobj()
    else:
        return typeobj

def adapt_type(typeobj, colspecs):
    if isinstance(typeobj, type):
        typeobj = typeobj()
    for t in typeobj.get_search_list():
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

class NullTypeEngine(TypeEngine):
    def get_col_spec(self):
        raise NotImplementedError()

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        return value

class String(TypeEngine):
    def __init__(self, length=None, convert_unicode=False):
        self.length = length
        self.convert_unicode = convert_unicode

    def adapt(self, impltype):
        return impltype(length=self.length, convert_unicode=self.convert_unicode)

    def convert_bind_param(self, value, dialect):
        if not (self.convert_unicode or dialect.convert_unicode) or value is None or not isinstance(value, unicode):
            return value
        else:
            return value.encode(dialect.encoding)

    def get_search_list(self):
        l = super(String, self).get_search_list()
        if self.length is None:
            return (TEXT,) + l
        else:
            return l

    def convert_result_value(self, value, dialect):
        if not (self.convert_unicode or dialect.convert_unicode) or value is None or isinstance(value, unicode):
            return value
        else:
            return value.decode(dialect.encoding)

    def get_dbapi_type(self, dbapi):
        return dbapi.STRING

    def compare_values(self, x, y):
        return x == y

class Unicode(String):
    def __init__(self, length=None, **kwargs):
        kwargs['convert_unicode'] = True
        super(Unicode, self).__init__(length=length, **kwargs)
    
class Integer(TypeEngine):
    """Integer datatype."""

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

class SmallInteger(Integer):
    """Smallint datatype."""

    pass

Smallinteger = SmallInteger

class Numeric(TypeEngine):
    def __init__(self, precision = 10, length = 2):
        self.precision = precision
        self.length = length

    def adapt(self, impltype):
        return impltype(precision=self.precision, length=self.length)

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER

class Float(Numeric):
    def __init__(self, precision = 10):
        self.precision = precision

    def adapt(self, impltype):
        return impltype(precision=self.precision)

class DateTime(TypeEngine):
    """Implement a type for ``datetime.datetime()`` objects."""

    def __init__(self, timezone=False):
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Date(TypeEngine):
    """Implement a type for ``datetime.date()`` objects."""

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Time(TypeEngine):
    """Implement a type for ``datetime.time()`` objects."""

    def __init__(self, timezone=False):
        self.timezone = timezone

    def adapt(self, impltype):
        return impltype(timezone=self.timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Binary(TypeEngine):
    def __init__(self, length=None):
        self.length = length

    def convert_bind_param(self, value, dialect):
        if value is not None:
            return dialect.dbapi.Binary(value)
        else:
            return None

    def convert_result_value(self, value, dialect):
        return value

    def adapt(self, impltype):
        return impltype(length=self.length)

    def get_dbapi_type(self, dbapi):
        return dbapi.BINARY

class PickleType(MutableType, TypeDecorator):
    impl = Binary

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL, pickler=None, mutable=True):
        self.protocol = protocol
        self.pickler = pickler or pickle
        self.mutable = mutable
        super(PickleType, self).__init__()

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        buf = self.impl.convert_result_value(value, dialect)
        return self.pickler.loads(str(buf))

    def convert_bind_param(self, value, dialect):
        if value is None:
            return None
        return self.impl.convert_bind_param(self.pickler.dumps(value, self.protocol), dialect)

    def copy_value(self, value):
        if self.mutable:
            return self.pickler.loads(self.pickler.dumps(value, self.protocol))
        else:
            return value

    def compare_values(self, x, y):
        if self.mutable:
            return self.pickler.dumps(x, self.protocol) == self.pickler.dumps(y, self.protocol)
        else:
            return x is y

    def is_mutable(self):
        return self.mutable

class Boolean(TypeEngine):
    pass
    
class Interval(TypeDecorator):
    """Type to be used in Column statements to store python timedeltas.

        If it's possible it uses native engine features to store timedeltas
        (now it's only PostgreSQL Interval type), if there is no such it
        fallbacks to DateTime storage with converting from/to timedelta on the fly

        Converting is very simple - just use epoch(zero timestamp, 01.01.1970) as
        base, so if we need to store timedelta = 1 day (24 hours) in database it
        will be stored as DateTime = '2nd Jan 1970 00:00', see convert_bind_param
        and convert_result_value to actual conversion code
    """
    #Empty useless type, because at the moment of creation of instance we don't
    #know what type will be decorated - it depends on used dialect.
    impl = TypeEngine

    def load_dialect_impl(self, dialect):
        """Checks if engine has native implementation of timedelta python type,
        if so it returns right class to handle it, if there is no native support, 
        it fallback to engine's DateTime implementation class
        """
        if not hasattr(self,'__supported'):
            import sqlalchemy.databases.postgres as pg
            self.__supported = {pg.PGDialect:pg.PGInterval}
            del pg
            
        if self.__hasNativeImpl(dialect):
            #For now, only PostgreSQL has native timedelta types support
            return self.__supported[dialect.__class__]()
        else:
            #All others should fallback to DateTime
            return dialect.type_descriptor(DateTime)
        
    def __hasNativeImpl(self,dialect):
        return dialect.__class__ in self.__supported
            
    def convert_bind_param(self, value, dialect):
        if value is None:
            return None
        if not self.__hasNativeImpl(dialect):
            tmpval = dt.datetime.utcfromtimestamp(0) + value
            return self.impl.convert_bind_param(tmpval,dialect)
        else:
            return self.impl.convert_bind_param(value,dialect)

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        retval = self.impl.convert_result_value(value,dialect)
        if not self.__hasNativeImpl(dialect):
            return retval - dt.datetime.utcfromtimestamp(0)
        else:
            return retval

class FLOAT(Float):pass
class TEXT(String):pass
class DECIMAL(Numeric):pass
class INT(Integer):pass
INTEGER = INT
class SMALLINT(Smallinteger):pass
class TIMESTAMP(DateTime): pass
class DATETIME(DateTime): pass
class DATE(Date): pass
class TIME(Time): pass
class CLOB(TEXT): pass
class VARCHAR(String): pass
class CHAR(String):pass
class NCHAR(Unicode):pass
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass

NULLTYPE = NullTypeEngine()

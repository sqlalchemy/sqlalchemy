# types.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = [ 'TypeEngine', 'TypeDecorator', 'NullTypeEngine',
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'Smallinteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'Binary', 'Boolean', 'Unicode', 'PickleType', 'NULLTYPE',
        'SMALLINT', 'DATE', 'TIME'
            ]

import sqlalchemy.util as util
import sqlalchemy.exceptions as exceptions
try:
    import cPickle as pickle
except:
    import pickle

class AbstractType(object):
    def _get_impl_dict(self):
        try:
            return self._impl_dict
        except AttributeError:
            self._impl_dict = {}
            return self._impl_dict
    impl_dict = property(_get_impl_dict)

    def copy_value(self, value):
        return value
    def compare_values(self, x, y):
        return x is y
    def is_mutable(self):
        return False
    def get_dbapi_type(self, dbapi):
        """return the corresponding type object from the underlying DBAPI, if any.
        
        this can be useful for calling setinputsizes(), for example."""
        return None
            
class TypeEngine(AbstractType):
    def __init__(self, *args, **params):
        pass
    def engine_impl(self, engine):
        """deprecated; call dialect_impl with a dialect directly."""
        return self.dialect_impl(engine.dialect)
    def dialect_impl(self, dialect):
        try:
            return self.impl_dict[dialect]
        except KeyError:
            return self.impl_dict.setdefault(dialect, dialect.type_descriptor(self))
    def _get_impl(self):
        if hasattr(self, '_impl'):
            return self._impl
        else:
            return NULLTYPE
    def _set_impl(self, impl):
        self._impl = impl
    impl = property(_get_impl, _set_impl)
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value, dialect):
        return value
    def convert_result_value(self, value, dialect):
        return value
    def adapt(self, cls):
        return cls()


class TypeDecorator(AbstractType):
    def __init__(self, *args, **kwargs):
        if not hasattr(self.__class__, 'impl'):
            raise exceptions.AssertionError("TypeDecorator implementations require a class-level variable 'impl' which refers to the class of type being decorated")
        self.impl = self.__class__.impl(*args, **kwargs)
    def engine_impl(self, engine):
        return self.dialect_impl(engine.dialect)
    def dialect_impl(self, dialect):
        try:
            return self.impl_dict[dialect]
        except:
            typedesc = dialect.type_descriptor(self.impl)
            tt = self.copy()
            if not isinstance(tt, self.__class__):
                raise exceptions.AssertionError("Type object %s does not properly implement the copy() method, it must return an object of type %s" % (self, self.__class__))
            tt.impl = typedesc
            self.impl_dict[dialect] = tt
            return tt
    def __getattr__(self, key):
        """proxy all other undefined accessors to the underlying implementation."""
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
    """a mixin that marks a Type as holding a mutable object"""
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
    
    for t in typeobj.__class__.__mro__[0:-1]:
        try:
            impltype = colspecs[t]
            break
        except KeyError:
            pass
    else:
        # couldnt adapt...raise exception ?
        return typeobj
    # if we adapted the given generic type to a database-specific type, 
    # but it turns out the originally given "generic" type
    # is actually a subclass of our resulting type, then we were already
    # were given a more specific type than that required; so use that.
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
    def __new__(cls, *args, **kwargs):
        if cls is not String or len(args) > 0 or kwargs.has_key('length'):
            return super(String, cls).__new__(cls, *args, **kwargs)
        else:
            return super(String, TEXT).__new__(TEXT, *args, **kwargs)
    def __init__(self, length = None):
        self.length = length
    def adapt(self, impltype):
        return impltype(length=self.length)
    def convert_bind_param(self, value, dialect):
        if not dialect.convert_unicode or value is None or not isinstance(value, unicode):
            return value
        else:
            return value.encode(dialect.encoding)
    def convert_result_value(self, value, dialect):
        if not dialect.convert_unicode or value is None or isinstance(value, unicode):
            return value
        else:
            return value.decode(dialect.encoding)
    def get_dbapi_type(self, dbapi):
        return dbapi.STRING
    def compare_values(self, x, y):
        return x == y
        
class Unicode(TypeDecorator):
    impl = String
    def convert_bind_param(self, value, dialect):
         if value is not None and isinstance(value, unicode):
              return value.encode(dialect.encoding)
         else:
              return value
    def convert_result_value(self, value, dialect):
         if value is not None and not isinstance(value, unicode):
             return value.decode(dialect.encoding)
         else:
             return value
        
class Integer(TypeEngine):
    """integer datatype"""
    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER
        
class SmallInteger(Integer):
    """ smallint datatype """
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
    """implements a type for datetime.datetime() objects"""
    def __init__(self, timezone=True):
        self.timezone = timezone
    def adapt(self, impltype):
        return impltype(timezone=self.timezone)
    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME
        
class Date(TypeEngine):
    """implements a type for datetime.date() objects"""
    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME

class Time(TypeEngine):
    """implements a type for datetime.time() objects"""
    def __init__(self, timezone=True):
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
            return dialect.dbapi().Binary(value)
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
class CLOB(String): pass
class VARCHAR(String): pass
class CHAR(String):pass
class NCHAR(Unicode):pass
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass

NULLTYPE = NullTypeEngine()

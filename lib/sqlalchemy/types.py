# types.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = [ 'TypeEngine', 'TypeDecorator', 'NullTypeEngine',
            'INT', 'CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'Smallinteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'Binary', 'Boolean', 'Unicode', 'NULLTYPE',
        'SMALLINT', 'DATE', 'TIME'
            ]

import sqlalchemy.util as util

class TypeEngine(object):
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value, engine):
        raise NotImplementedError()
    def convert_result_value(self, value, engine):
        raise NotImplementedError()
    def adapt(self, typeobj):
        """given a class that is a subclass of this TypeEngine's class, produces a new
        instance of that class with an equivalent state to this TypeEngine.  The given
        class is a database-specific subclass which is obtained via a lookup dictionary,
        mapped against the class returned by the class_to_adapt() method."""
        return typeobj()
    def adapt_args(self):
        """Returns an instance of this TypeEngine instance's class, adapted according
        to the constructor arguments of this TypeEngine.  Default return value is 
        just this object instance."""
        return self
    def class_to_adapt(self):
        """returns the class that should be sent to the adapt() method.  This class
        will be used to lookup an approprate database-specific subclass."""
        return self.__class__
#    def __repr__(self):
 #       return util.generic_repr(self)
        
def adapt_type(typeobj, colspecs):
    """given a generic type from this package, and a dictionary of 
    "conversion" specs from a DB-specific package, adapts the type
    to a correctly-configured type instance from the DB-specific package."""
    if type(typeobj) is type:
        typeobj = typeobj()
    # if the type is not a base type, i.e. not from our module, or its Null, 
    # we return the type as is
    if (typeobj.__module__ != 'sqlalchemy.types' or typeobj.__class__ is NullTypeEngine) and not isinstance(typeobj, TypeDecorator):
        return typeobj
    typeobj = typeobj.adapt_args()
    t = typeobj.class_to_adapt()
    for t in t.__mro__[0:-1]:
        try:
            return typeobj.adapt(colspecs[t])
        except KeyError, e:
            pass
    return typeobj.adapt(typeobj.__class__)
    
class NullTypeEngine(TypeEngine):
    def __init__(self, *args, **kwargs):
        pass
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value, engine):
        return value
    def convert_result_value(self, value, engine):
        return value

class TypeDecorator(object):
    def get_col_spec(self):
        return self.extended.get_col_spec()
    def adapt(self, typeobj):
        if self.extended is self:
            t = self.__class__.__mro__[2]
            self.extended = t.adapt(self, typeobj)
        else:
            self.extended = self.extended.adapt(typeobj)
        return self
    def adapt_args(self):
        t = self.__class__.__mro__[2]
        self.extended = t.adapt_args(self)
        return self
    def class_to_adapt(self):
        return self.extended.__class__
    
class String(NullTypeEngine):
    def __init__(self, length = None):
        self.length = length
    def adapt(self, typeobj):
        return typeobj(self.length)
    def adapt_args(self):
        if self.length is None:
            return TEXT()
        else:
            return self
    def convert_bind_param(self, value, engine):
        if not engine.convert_unicode or value is None or not isinstance(value, unicode):
            return value
        else:
            return value.encode('utf-8')
    def convert_result_value(self, value, engine):
        if not engine.convert_unicode or value is None or isinstance(value, unicode):
            return value
        else:
            return value.decode('utf-8')

class Unicode(TypeDecorator,String):
    def __init__(self, length=None):
        String.__init__(self, length)
    def convert_bind_param(self, value, engine):
         if isinstance(value, unicode):
              return value.encode('utf-8')
         else:
              return value
    def convert_result_value(self, value, engine):
         if not isinstance(value, unicode):
             return value.decode('utf-8')
         else:
             return value
              
class Integer(NullTypeEngine):
    """integer datatype"""
    # TODO: do string bind params need int(value) performed before sending ?  
    # seems to be not needed with SQLite, Postgres
    pass

class Smallinteger(Integer):
    """ smallint datatype """
    pass

class Numeric(NullTypeEngine):
    def __init__(self, precision = 10, length = 2):
        self.precision = precision
        self.length = length
    def adapt(self, typeobj):
        return typeobj(self.precision, self.length)

class Float(NullTypeEngine):
    def __init__(self, precision = 10):
        self.precision = precision
    def adapt(self, typeobj):
        return typeobj(self.precision)

class DateTime(NullTypeEngine):
    pass

class Date(NullTypeEngine):
    pass

class Time(NullTypeEngine):
    pass

class Binary(NullTypeEngine):
    def __init__(self, length=None):
        self.length = length
    def convert_bind_param(self, value, engine):
        return engine.dbapi().Binary(value)
    def convert_result_value(self, value, engine):
        return value
    def adapt(self, typeobj):
        return typeobj(self.length)

class Boolean(NullTypeEngine):
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
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass

NULLTYPE = NullTypeEngine()

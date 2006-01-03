# types.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = [ 'TypeEngine', 'TypeDecorator', 'NullTypeEngine',
            'INT', 'CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'Numeric', 'Float', 'DateTime', 'Binary', 'Boolean', 'Unicode', 'NULLTYPE'
            ]


class TypeEngine(object):
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value, engine):
        raise NotImplementedError()
    def convert_result_value(self, value, engine):
        raise NotImplementedError()
    def adapt(self, typeobj):
        return typeobj()
    def adapt_args(self):
        return self
        
def adapt_type(typeobj, colspecs):
    """given a generic type from this package, and a dictionary of 
    "conversion" specs from a DB-specific package, adapts the type
    to a correctly-configured type instance from the DB-specific package."""
    if type(typeobj) is type:
        typeobj = typeobj()
    typeobj = typeobj.adapt_args()
    t = typeobj.__class__
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
        t = self.__class__.__mro__[2]
        print repr(t)
        c = self.__class__()
        c.extended = t.adapt(self, typeobj)
        return c
    
class String(NullTypeEngine):
    def __init__(self, length = None, is_unicode=False):
        self.length = length
        self.is_unicode = is_unicode
    def adapt(self, typeobj):
        return typeobj(self.length)
    def adapt_args(self):
        if self.length is None:
            return TEXT(is_unicode=self.is_unicode)
        else:
            return self

class Unicode(String):
    def __init__(self, length=None):
        String.__init__(self, length, is_unicode=True)
    def adapt(self, typeobj):
        return typeobj(self.length, is_unicode=True)
        
class Integer(NullTypeEngine):
    """integer datatype"""
    # TODO: do string bind params need int(value) performed before sending ?  
    # seems to be not needed with SQLite, Postgres
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
class TIMESTAMP(DateTime): pass
class DATETIME(DateTime): pass
class CLOB(String): pass
class VARCHAR(String): pass
class CHAR(String):pass
class BLOB(Binary): pass
class BOOLEAN(Boolean): pass

NULLTYPE = NullTypeEngine()

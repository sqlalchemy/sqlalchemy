__ALL__ = [
            'INT', 'CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'Numeric', 'DateTime', 'Binary', 'Boolean'
            ]


class TypeEngineMeta(type):
    typeself = property(lambda cls:cls.singleton)        
    typeclass = property(lambda cls: cls)
    
class TypeEngine(object):
    __metaclass__ = TypeEngineMeta
    def get_col_spec(self, typeobj):
        raise NotImplementedError()
    def convert_bind_param(self, value):
        raise NotImplementedError()
    def convert_result_value(self, value):
        raise NotImplementedError()
    def adapt(self, typeobj):
        return typeobj()

    typeclass = property(lambda s: s.__class__)
    typeself = property(lambda s:s)
    
class NullTypeEngine(TypeEngine):
    def get_col_spec(self, typeobj):
        raise NotImplementedError()
    def convert_bind_param(self, value):
        return value
    def convert_result_value(self, value):
        return value

class String(TypeEngine):
    def __init__(self, length):
        self.length = length
    def adapt(self, typeobj):
        return typeobj(self.length)
String.singleton = String(-1)

class Integer(TypeEngine):
    """integer datatype"""
    pass
Integer.singleton = Integer()

class Numeric(TypeEngine):
    def __init__(self, precision, length):
        self.precision = precision
        self.length = length
    def adapt(self, typeobj):
        return typeobj(self.precision, self.length)
Numeric.singleton = Numeric(10, 2)

class DateTime(TypeEngine):
    pass
DateTime.singleton = DateTime()

class Binary(TypeEngine):
    pass
Binary.singleton = Binary()

class Boolean(TypeEngine):
    pass
Boolean.singleton = Boolean()

class FLOAT(Numeric):pass
class TEXT(String): pass
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



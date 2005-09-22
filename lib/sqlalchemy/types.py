__ALL__ = [
            'INT', 'CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN', 'String', 'Integer', 'Numeric', 'DateTime', 'Binary', 
            ]


class TypeEngineMeta(type):
    def convert_bind_param(cls, value):
        return cls.singleton.convert_bind_param(value)
    def convert_result_value(cls, value):
        return cls.singleton.convert_result_value(value)
        
class TypeEngine(object):
    __metaclass__ = TypeEngineMeta
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value):
        raise NotImplementedError()
    def convert_result_value(self, value):
        raise NotImplementedError()

class NullTypeEngine(TypeEngine):
    def get_col_spec(self):
        raise NotImplementedError()
    def convert_bind_param(self, value):
        return value
    def convert_result_value(self, value):
        return value

class String(TypeEngine):
    def __init__(self, length):
        self.length = length
String.singleton = String(-1)

class Integer(TypeEngine):
    """integer datatype"""
    pass
Integer.singleton = Integer()

class Numeric(TypeEngine):
    def __init__(self, precision, length):
        self.precision = precision
        self.length = length
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



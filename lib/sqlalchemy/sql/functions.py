from sqlalchemy import types as sqltypes
from sqlalchemy.sql.expression import _Function, _literal_as_binds, \
                                      ClauseList, _FigureVisitName
from sqlalchemy.sql import operators


class _GenericMeta(_FigureVisitName):
    def __init__(cls, clsname, bases, dict):
        cls.__visit_name__ = 'function'
        type.__init__(cls, clsname, bases, dict)

    def __call__(self, *args, **kwargs):
        args = [_literal_as_binds(c) for c in args]
        return type.__call__(self, *args, **kwargs)

class GenericFunction(_Function):
    __metaclass__ = _GenericMeta

    def __init__(self, type_=None, group=True, args=(), **kwargs):
        self.packagenames = []
        self.name = self.__class__.__name__
        self._bind = kwargs.get('bind', None)
        if group:
            self.clause_expr = ClauseList(
                operator=operators.comma_op,
                group_contents=True, *args).self_group()
        else:
            self.clause_expr = ClauseList(
                operator=operators.comma_op,
                group_contents=True, *args)
        self.type = sqltypes.to_instance(
            type_ or getattr(self, '__return_type__', None))

class AnsiFunction(GenericFunction):
    def __init__(self, **kwargs):
        GenericFunction.__init__(self, **kwargs)


class coalesce(GenericFunction):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('type_', _type_from_args(args))
        GenericFunction.__init__(self, args=args, **kwargs)

class now(GenericFunction):
    __return_type__ = sqltypes.DateTime

class concat(GenericFunction):
    __return_type__ = sqltypes.String
    def __init__(self, *args, **kwargs):
        GenericFunction.__init__(self, args=args, **kwargs)

class char_length(GenericFunction):
    __return_type__ = sqltypes.Integer

    def __init__(self, arg, **kwargs):
        GenericFunction.__init__(self, args=[arg], **kwargs)

class random(GenericFunction):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('type_', None)
        GenericFunction.__init__(self, args=args, **kwargs)

class current_date(AnsiFunction):
    __return_type__ = sqltypes.Date

class current_time(AnsiFunction):
    __return_type__ = sqltypes.Time

class current_timestamp(AnsiFunction):
    __return_type__ = sqltypes.DateTime

class current_user(AnsiFunction):
    __return_type__ = sqltypes.String

class localtime(AnsiFunction):
    __return_type__ = sqltypes.DateTime

class localtimestamp(AnsiFunction):
    __return_type__ = sqltypes.DateTime

class session_user(AnsiFunction):
    __return_type__ = sqltypes.String

class sysdate(AnsiFunction):
    __return_type__ = sqltypes.DateTime

class user(AnsiFunction):
    __return_type__ = sqltypes.String

def _type_from_args(args):
    for a in args:
        if not isinstance(a.type, sqltypes.NullType):
            return a.type
    else:
        return sqltypes.NullType

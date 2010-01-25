from sqlalchemy import types as sqltypes
from sqlalchemy.sql.expression import (
    ClauseList, Function, _literal_as_binds, text, _type_from_args
    )
from sqlalchemy.sql import operators
from sqlalchemy.sql.visitors import VisitableType

class _GenericMeta(VisitableType):
    def __call__(self, *args, **kwargs):
        args = [_literal_as_binds(c) for c in args]
        return type.__call__(self, *args, **kwargs)

class GenericFunction(Function):
    __metaclass__ = _GenericMeta

    def __init__(self, type_=None, args=(), **kwargs):
        self.packagenames = []
        self.name = self.__class__.__name__
        self._bind = kwargs.get('bind', None)
        self.clause_expr = ClauseList(
                operator=operators.comma_op,
                group_contents=True, *args).self_group()
        self.type = sqltypes.to_instance(
            type_ or getattr(self, '__return_type__', None))

class AnsiFunction(GenericFunction):
    def __init__(self, **kwargs):
        GenericFunction.__init__(self, **kwargs)

class ReturnTypeFromArgs(GenericFunction):
    """Define a function whose return type is the same as its arguments."""
    
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('type_', _type_from_args(args))
        GenericFunction.__init__(self, args=args, **kwargs)

class coalesce(ReturnTypeFromArgs):
    pass

class max(ReturnTypeFromArgs):
    pass

class min(ReturnTypeFromArgs):
    pass

class sum(ReturnTypeFromArgs):
    pass

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

class count(GenericFunction):
    """The ANSI COUNT aggregate function.  With no arguments, emits COUNT \*."""

    __return_type__ = sqltypes.Integer

    def __init__(self, expression=None, **kwargs):
        if expression is None:
            expression = text('*')
        GenericFunction.__init__(self, args=(expression,), **kwargs)

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


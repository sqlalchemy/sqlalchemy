from __future__ import annotations

import sys
import typing
from typing import Any
from typing import Callable  # noqa
from typing import cast
from typing import Dict
from typing import ForwardRef
from typing import Generic
from typing import overload
from typing import Type
from typing import TypeVar
from typing import Union

from typing_extensions import NotRequired as NotRequired  # noqa

from . import compat

_T = TypeVar("_T", bound=Any)

if compat.py310:
    # why they took until py310 to put this in stdlib is beyond me,
    # I've been wanting it since py27
    from types import NoneType
else:
    NoneType = type(None)  # type: ignore

if typing.TYPE_CHECKING or compat.py310:
    from typing import Annotated as Annotated
else:
    from typing_extensions import Annotated as Annotated  # noqa F401

if typing.TYPE_CHECKING or compat.py38:
    from typing import Literal as Literal
    from typing import Protocol as Protocol
    from typing import TypedDict as TypedDict
else:
    from typing_extensions import Literal as Literal  # noqa F401
    from typing_extensions import Protocol as Protocol  # noqa F401
    from typing_extensions import TypedDict as TypedDict  # noqa F401

# work around https://github.com/microsoft/pyright/issues/3025
_LiteralStar = Literal["*"]

if typing.TYPE_CHECKING or not compat.py310:
    from typing_extensions import Concatenate as Concatenate
    from typing_extensions import ParamSpec as ParamSpec
else:
    from typing import Concatenate as Concatenate  # noqa F401
    from typing import ParamSpec as ParamSpec  # noqa F401


class _TypeToInstance(Generic[_T]):
    """describe a variable that moves between a class and an instance of
    that class.

    """

    @overload
    def __get__(self, instance: None, owner: Any) -> Type[_T]:
        ...

    @overload
    def __get__(self, instance: object, owner: Any) -> _T:
        ...

    def __get__(self, instance: object, owner: Any) -> Union[Type[_T], _T]:
        ...

    @overload
    def __set__(self, instance: None, value: Type[_T]) -> None:
        ...

    @overload
    def __set__(self, instance: object, value: _T) -> None:
        ...

    def __set__(self, instance: object, value: Union[Type[_T], _T]) -> None:
        ...


def de_stringify_annotation(
    cls: Type[Any], annotation: Union[str, Type[Any]]
) -> Union[str, Type[Any]]:
    """Resolve annotations that may be string based into real objects.

    This is particularly important if a module defines "from __future__ import
    annotations", as everything inside of __annotations__ is a string. We want
    to at least have generic containers like ``Mapped``, ``Union``, ``List``,
    etc.

    """

    # looked at typing.get_type_hints(), looked at pydantic.  We need much
    # less here, and we here try to not use any private typing internals
    # or construct ForwardRef objects which is documented as something
    # that should be avoided.

    if (
        is_fwd_ref(annotation)
        and not cast(ForwardRef, annotation).__forward_evaluated__
    ):
        annotation = cast(ForwardRef, annotation).__forward_arg__

    if isinstance(annotation, str):
        base_globals: "Dict[str, Any]" = getattr(
            sys.modules.get(cls.__module__, None), "__dict__", {}
        )
        try:
            annotation = eval(annotation, base_globals, None)
        except NameError:
            pass
    return annotation


def is_fwd_ref(type_):
    return isinstance(type_, ForwardRef)


def de_optionalize_union_types(type_):
    """Given a type, filter out ``Union`` types that include ``NoneType``
    to not include the ``NoneType``.

    """
    if is_optional(type_):
        typ = set(type_.__args__)

        typ.discard(NoneType)

        return make_union_type(*typ)

    else:
        return type_


def make_union_type(*types):
    """Make a Union type.

    This is needed by :func:`.de_optionalize_union_types` which removes
    ``NoneType`` from a ``Union``.

    """
    return cast(Any, Union).__getitem__(types)


def expand_unions(type_, include_union=False, discard_none=False):
    """Return a type as as a tuple of individual types, expanding for
    ``Union`` types."""

    if is_union(type_):
        typ = set(type_.__args__)

        if discard_none:
            typ.discard(NoneType)

        if include_union:
            return (type_,) + tuple(typ)
        else:
            return tuple(typ)
    else:
        return (type_,)


def is_optional(type_):
    return is_origin_of(
        type_,
        "Optional",
        "Union",
    )


def is_union(type_):
    return is_origin_of(type_, "Union")


def is_origin_of(type_, *names, module=None):
    """return True if the given type has an __origin__ with the given name
    and optional module."""

    origin = getattr(type_, "__origin__", None)
    if origin is None:
        return False

    return _get_type_name(origin) in names and (
        module is None or origin.__module__.startswith(module)
    )


def _get_type_name(type_):
    if compat.py310:
        return type_.__name__
    else:
        typ_name = getattr(type_, "__name__", None)
        if typ_name is None:
            typ_name = getattr(type_, "_name", None)

        return typ_name

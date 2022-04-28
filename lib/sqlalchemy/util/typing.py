# mypy: allow-untyped-defs, allow-untyped-calls

from __future__ import annotations

import sys
import typing
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import ForwardRef
from typing import Generic
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from typing_extensions import NotRequired as NotRequired

from . import compat

_T = TypeVar("_T", bound=Any)
_KT = TypeVar("_KT")
_KT_co = TypeVar("_KT_co", covariant=True)
_KT_contra = TypeVar("_KT_contra", contravariant=True)
_VT = TypeVar("_VT")
_VT_co = TypeVar("_VT_co", covariant=True)

Self = TypeVar("Self", bound=Any)

if compat.py310:
    # why they took until py310 to put this in stdlib is beyond me,
    # I've been wanting it since py27
    from types import NoneType
else:
    NoneType = type(None)  # type: ignore

if compat.py310:
    from typing import TypeGuard as TypeGuard
    from typing import TypeAlias as TypeAlias
else:
    from typing_extensions import TypeGuard as TypeGuard
    from typing_extensions import TypeAlias as TypeAlias

if typing.TYPE_CHECKING or compat.py38:
    from typing import SupportsIndex as SupportsIndex
else:
    from typing_extensions import SupportsIndex as SupportsIndex

if typing.TYPE_CHECKING or compat.py310:
    from typing import Annotated as Annotated
else:
    from typing_extensions import Annotated as Annotated  # noqa: F401

if typing.TYPE_CHECKING or compat.py38:
    from typing import Literal as Literal
    from typing import Protocol as Protocol
    from typing import TypedDict as TypedDict
    from typing import Final as Final
else:
    from typing_extensions import Literal as Literal  # noqa: F401
    from typing_extensions import Protocol as Protocol  # noqa: F401
    from typing_extensions import TypedDict as TypedDict  # noqa: F401
    from typing_extensions import Final as Final  # noqa: F401

# copied from TypeShed, required in order to implement
# MutableMapping.update()


class SupportsKeysAndGetItem(Protocol[_KT, _VT_co]):
    def keys(self) -> Iterable[_KT]:
        ...

    def __getitem__(self, __k: _KT) -> _VT_co:
        ...


# work around https://github.com/microsoft/pyright/issues/3025
_LiteralStar = Literal["*"]

if typing.TYPE_CHECKING or not compat.py310:
    from typing_extensions import Concatenate as Concatenate
    from typing_extensions import ParamSpec as ParamSpec
else:
    from typing import Concatenate as Concatenate  # noqa: F401
    from typing import ParamSpec as ParamSpec  # noqa: F401


def de_stringify_annotation(
    cls: Type[Any],
    annotation: Union[str, Type[Any]],
    str_cleanup_fn: Optional[Callable[[str], str]] = None,
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
        if str_cleanup_fn:
            annotation = str_cleanup_fn(annotation)

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


def expand_unions(
    type_: Type[Any], include_union: bool = False, discard_none: bool = False
) -> Tuple[Type[Any], ...]:
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


def is_origin_of(
    type_: Any, *names: str, module: Optional[str] = None
) -> bool:
    """return True if the given type has an __origin__ with the given name
    and optional module."""

    origin = getattr(type_, "__origin__", None)
    if origin is None:
        return False

    return _get_type_name(origin) in names and (
        module is None or origin.__module__.startswith(module)
    )


def _get_type_name(type_: Type[Any]) -> str:
    if compat.py310:
        return type_.__name__
    else:
        typ_name = getattr(type_, "__name__", None)
        if typ_name is None:
            typ_name = getattr(type_, "_name", None)

        return typ_name  # type: ignore


class DescriptorProto(Protocol):
    def __get__(self, instance: object, owner: Any) -> Any:
        ...

    def __set__(self, instance: Any, value: Any) -> None:
        ...

    def __delete__(self, instance: Any) -> None:
        ...


_DESC = TypeVar("_DESC", bound=DescriptorProto)


class DescriptorReference(Generic[_DESC]):
    """a descriptor that refers to a descriptor.

    used for cases where we need to have an instance variable referring to an
    object that is itself a descriptor, which typically confuses typing tools
    as they don't know when they should use ``__get__`` or not when referring
    to the descriptor assignment as an instance variable. See
    sqlalchemy.orm.interfaces.PropComparator.prop

    """

    def __get__(self, instance: object, owner: Any) -> _DESC:
        ...

    def __set__(self, instance: Any, value: _DESC) -> None:
        ...

    def __delete__(self, instance: Any) -> None:
        ...


# $def ro_descriptor_reference(fn: Callable[])

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
from typing import NoReturn
from typing import Optional
from typing import overload
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from . import compat

if True:  # zimports removes the tailing comments
    from typing_extensions import Annotated as Annotated  # 3.8
    from typing_extensions import Concatenate as Concatenate  # 3.10
    from typing_extensions import (
        dataclass_transform as dataclass_transform,  # 3.11,
    )
    from typing_extensions import Final as Final  # 3.8
    from typing_extensions import final as final  # 3.8
    from typing_extensions import get_args as get_args  # 3.10
    from typing_extensions import get_origin as get_origin  # 3.10
    from typing_extensions import Literal as Literal  # 3.8
    from typing_extensions import NotRequired as NotRequired  # 3.11
    from typing_extensions import ParamSpec as ParamSpec  # 3.10
    from typing_extensions import Protocol as Protocol  # 3.8
    from typing_extensions import SupportsIndex as SupportsIndex  # 3.8
    from typing_extensions import TypeAlias as TypeAlias  # 3.10
    from typing_extensions import TypedDict as TypedDict  # 3.8
    from typing_extensions import TypeGuard as TypeGuard  # 3.10


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
    from types import NoneType as NoneType
else:
    NoneType = type(None)  # type: ignore

typing_get_args = get_args
typing_get_origin = get_origin

# copied from TypeShed, required in order to implement
# MutableMapping.update()

_AnnotationScanType = Union[Type[Any], str]


class SupportsKeysAndGetItem(Protocol[_KT, _VT_co]):
    def keys(self) -> Iterable[_KT]:
        ...

    def __getitem__(self, __k: _KT) -> _VT_co:
        ...


# work around https://github.com/microsoft/pyright/issues/3025
_LiteralStar = Literal["*"]


def de_stringify_annotation(
    cls: Type[Any],
    annotation: _AnnotationScanType,
    str_cleanup_fn: Optional[Callable[[str], str]] = None,
) -> Type[Any]:
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
        except NameError as err:
            raise NameError(
                f"Could not de-stringify annotation {annotation}"
            ) from err
    return annotation  # type: ignore


def de_stringify_union_elements(
    cls: Type[Any],
    annotation: _AnnotationScanType,
    str_cleanup_fn: Optional[Callable[[str], str]] = None,
) -> Type[Any]:
    return make_union_type(
        *[
            de_stringify_annotation(cls, anno, str_cleanup_fn)
            for anno in annotation.__args__  # type: ignore
        ]
    )


def is_pep593(type_: Optional[_AnnotationScanType]) -> bool:
    return type_ is not None and typing_get_origin(type_) is Annotated


def is_fwd_ref(type_: _AnnotationScanType) -> bool:
    return isinstance(type_, ForwardRef)


@overload
def de_optionalize_union_types(type_: str) -> str:
    ...


@overload
def de_optionalize_union_types(type_: Type[Any]) -> Type[Any]:
    ...


def de_optionalize_union_types(
    type_: _AnnotationScanType,
) -> _AnnotationScanType:
    """Given a type, filter out ``Union`` types that include ``NoneType``
    to not include the ``NoneType``.

    """
    if is_optional(type_):
        typ = set(type_.__args__)  # type: ignore

        typ.discard(NoneType)

        return make_union_type(*typ)

    else:
        return type_


def make_union_type(*types: _AnnotationScanType) -> Type[Any]:
    """Make a Union type.

    This is needed by :func:`.de_optionalize_union_types` which removes
    ``NoneType`` from a ``Union``.

    """
    return cast(Any, Union).__getitem__(types)  # type: ignore


def expand_unions(
    type_: Type[Any], include_union: bool = False, discard_none: bool = False
) -> Tuple[Type[Any], ...]:
    """Return a type as a tuple of individual types, expanding for
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


def is_optional(type_: Any) -> bool:
    return is_origin_of(
        type_,
        "Optional",
        "Union",
        "UnionType",
    )


def is_optional_union(type_: Any) -> bool:
    return is_optional(type_) and NoneType in typing_get_args(type_)


def is_union(type_: Any) -> bool:
    return is_origin_of(type_, "Union")


def is_origin_of_cls(
    type_: Any, class_obj: Union[Tuple[Type[Any], ...], Type[Any]]
) -> bool:
    """return True if the given type has an __origin__ that shares a base
    with the given class"""

    origin = typing_get_origin(type_)
    if origin is None:
        return False

    return isinstance(origin, type) and issubclass(origin, class_obj)


def is_origin_of(
    type_: Any, *names: str, module: Optional[str] = None
) -> bool:
    """return True if the given type has an __origin__ with the given name
    and optional module."""

    origin = typing_get_origin(type_)
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


_DESC_co = TypeVar("_DESC_co", bound=DescriptorProto, covariant=True)


class RODescriptorReference(Generic[_DESC_co]):
    """a descriptor that refers to a descriptor.

    same as :class:`.DescriptorReference` but is read-only, so that subclasses
    can define a subtype as the generically contained element

    """

    def __get__(self, instance: object, owner: Any) -> _DESC_co:
        ...

    def __set__(self, instance: Any, value: Any) -> NoReturn:
        ...

    def __delete__(self, instance: Any) -> NoReturn:
        ...


_FN = TypeVar("_FN", bound=Optional[Callable[..., Any]])


class CallableReference(Generic[_FN]):
    """a descriptor that refers to a callable.

    works around mypy's limitation of not allowing callables assigned
    as instance variables


    """

    def __get__(self, instance: object, owner: Any) -> _FN:
        ...

    def __set__(self, instance: Any, value: _FN) -> None:
        ...

    def __delete__(self, instance: Any) -> None:
        ...


# $def ro_descriptor_reference(fn: Callable[])

import typing
from typing import Any
from typing import Callable  # noqa
from typing import Generic
from typing import overload
from typing import Type
from typing import TypeVar
from typing import Union

from typing_extensions import NotRequired  # noqa

from . import compat

_T = TypeVar("_T", bound=Any)

if typing.TYPE_CHECKING or not compat.py38:
    from typing_extensions import Literal  # noqa F401
    from typing_extensions import Protocol  # noqa F401
    from typing_extensions import TypedDict  # noqa F401
else:
    from typing import Literal  # noqa F401
    from typing import Protocol  # noqa F401
    from typing import TypedDict  # noqa F401

if typing.TYPE_CHECKING or not compat.py310:
    from typing_extensions import Concatenate  # noqa F401
    from typing_extensions import ParamSpec  # noqa F401
else:
    from typing import Concatenate  # noqa F401
    from typing import ParamSpec  # noqa F401


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


class ReadOnlyInstanceDescriptor(Protocol[_T]):
    """protocol representing an instance-only descriptor"""

    @overload
    def __get__(
        self, instance: None, owner: Any
    ) -> "ReadOnlyInstanceDescriptor[_T]":
        ...

    @overload
    def __get__(self, instance: object, owner: Any) -> _T:
        ...

    def __get__(
        self, instance: object, owner: Any
    ) -> Union["ReadOnlyInstanceDescriptor[_T]", _T]:
        ...

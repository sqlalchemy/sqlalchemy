from typing import Any
from typing import Callable  # noqa
from typing import Generic
from typing import overload
from typing import Type
from typing import TypeVar
from typing import Union

from . import compat

_T = TypeVar("_T", bound=Any)

if compat.py38:
    from typing import Literal
    from typing import Protocol
    from typing import TypedDict
else:
    from typing_extensions import Literal  # noqa
    from typing_extensions import Protocol  # noqa
    from typing_extensions import TypedDict  # noqa

if compat.py310:
    from typing import Concatenate
    from typing import ParamSpec
else:
    from typing_extensions import Concatenate  # noqa
    from typing_extensions import ParamSpec  # noqa

if compat.py311:
    from typing import NotRequired  # noqa
else:
    from typing_extensions import NotRequired  # noqa


_T = TypeVar("_T")


class _TypeToInstance(Generic[_T]):
    @overload
    def __get__(self, instance: None, owner: Any) -> Type[_T]:
        ...

    @overload
    def __get__(self, instance: object, owner: Any) -> _T:
        ...

    @overload
    def __set__(self, instance: None, value: Type[_T]) -> None:
        ...

    @overload
    def __set__(self, instance: object, value: _T) -> None:
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

from __future__ import annotations

import operator
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from sqlalchemy.orm.interfaces import UserDefinedOption
from ..util.typing import Protocol
from ..util.typing import TypeGuard

if TYPE_CHECKING:
    from .attributes import AttributeImpl
    from .attributes import CollectionAttributeImpl
    from .base import PassiveFlag
    from .descriptor_props import _CompositeClassProto
    from .mapper import Mapper
    from .state import InstanceState
    from .util import AliasedClass
    from .util import AliasedInsp
    from ..sql.base import ExecutableOption

_T = TypeVar("_T", bound=Any)

_O = TypeVar("_O", bound=Any)
"""The 'ORM mapped object' type.
I would have preferred this were bound=object however it seems
to not travel in all situations when defined in that way.
"""

_InternalEntityType = Union["Mapper[_T]", "AliasedInsp[_T]"]

_EntityType = Union[_T, "AliasedClass[_T]", "Mapper[_T]", "AliasedInsp[_T]"]


_InstanceDict = Dict[str, Any]

_IdentityKeyType = Tuple[Type[_T], Tuple[Any, ...], Optional[Any]]


class _LoaderCallable(Protocol):
    def __call__(self, state: InstanceState[Any], passive: PassiveFlag) -> Any:
        ...


def is_user_defined_option(
    opt: ExecutableOption,
) -> TypeGuard[UserDefinedOption]:
    return not opt._is_core and opt._is_user_defined  # type: ignore


def is_composite_class(obj: Any) -> TypeGuard[_CompositeClassProto]:
    return hasattr(obj, "__composite_values__")


if TYPE_CHECKING:

    def is_collection_impl(
        impl: AttributeImpl,
    ) -> TypeGuard[CollectionAttributeImpl]:
        ...

else:
    is_collection_impl = operator.attrgetter("collection")

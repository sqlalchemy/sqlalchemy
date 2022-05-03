from __future__ import annotations

import operator
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from ..sql import roles
from ..sql._typing import _HasClauseElement
from ..sql.elements import ColumnElement
from ..util.typing import Protocol
from ..util.typing import TypeGuard

if TYPE_CHECKING:
    from .attributes import AttributeImpl
    from .attributes import CollectionAttributeImpl
    from .base import PassiveFlag
    from .decl_api import registry as _registry_type
    from .descriptor_props import _CompositeClassProto
    from .interfaces import MapperProperty
    from .interfaces import UserDefinedOption
    from .mapper import Mapper
    from .relationships import Relationship
    from .state import InstanceState
    from .util import AliasedClass
    from .util import AliasedInsp
    from ..sql.base import ExecutableOption

_T = TypeVar("_T", bound=Any)


# I would have preferred this were bound=object however it seems
# to not travel in all situations when defined in that way.
_O = TypeVar("_O", bound=Any)
"""The 'ORM mapped object' type.

"""

if TYPE_CHECKING:
    _RegistryType = _registry_type

_InternalEntityType = Union["Mapper[_T]", "AliasedInsp[_T]"]

_ExternalEntityType = Union[Type[_T], "AliasedClass[_T]"]

_EntityType = Union[
    Type[_T], "AliasedClass[_T]", "Mapper[_T]", "AliasedInsp[_T]"
]


_InstanceDict = Dict[str, Any]

_IdentityKeyType = Tuple[Type[_T], Tuple[Any, ...], Optional[Any]]

_ORMColumnExprArgument = Union[
    ColumnElement[_T],
    _HasClauseElement,
    roles.ExpressionElementRole[_T],
]

# somehow Protocol didn't want to work for this one
_ORMAdapterProto = Callable[
    [_ORMColumnExprArgument[_T], Optional[str]], _ORMColumnExprArgument[_T]
]


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

    def insp_is_mapper_property(obj: Any) -> TypeGuard[MapperProperty[Any]]:
        ...

    def insp_is_mapper(obj: Any) -> TypeGuard[Mapper[Any]]:
        ...

    def insp_is_aliased_class(obj: Any) -> TypeGuard[AliasedInsp[Any]]:
        ...

    def prop_is_relationship(
        prop: MapperProperty[Any],
    ) -> TypeGuard[Relationship[Any]]:
        ...

    def is_collection_impl(
        impl: AttributeImpl,
    ) -> TypeGuard[CollectionAttributeImpl]:
        ...

else:
    insp_is_mapper_property = operator.attrgetter("is_property")
    insp_is_mapper = operator.attrgetter("is_mapper")
    insp_is_aliased_class = operator.attrgetter("is_aliased_class")
    is_collection_impl = operator.attrgetter("collection")
    prop_is_relationship = operator.attrgetter("_is_relationship")

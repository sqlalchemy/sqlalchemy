from __future__ import annotations

from typing import Any
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from . import roles
from .. import util
from ..inspection import Inspectable
from ..util.typing import Literal

if TYPE_CHECKING:
    from .elements import quoted_name
    from .schema import DefaultGenerator
    from .schema import Sequence
    from .selectable import FromClause
    from .selectable import NamedFromClause
    from .selectable import TableClause
    from .sqltypes import TupleType
    from .type_api import TypeEngine
    from ..util.typing import TypeGuard

_T = TypeVar("_T", bound=Any)

_ColumnsClauseElement = Union[
    Literal["*", 1],
    roles.ColumnsClauseRole,
    Type[Any],
    Inspectable[roles.HasColumnElementClauseElement],
]
_FromClauseElement = Union[
    roles.FromClauseRole, Type[Any], Inspectable[roles.HasFromClauseElement]
]

_ColumnExpression = Union[
    roles.ExpressionElementRole[_T],
    Inspectable[roles.HasColumnElementClauseElement],
]

_PropagateAttrsType = util.immutabledict[str, Any]

_TypeEngineArgument = Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"]


def is_named_from_clause(t: FromClause) -> TypeGuard[NamedFromClause]:
    return t.named_with_column


def has_schema_attr(t: FromClause) -> TypeGuard[TableClause]:
    return hasattr(t, "schema")


def is_quoted_name(s: str) -> TypeGuard[quoted_name]:
    return hasattr(s, "quote")


def is_tuple_type(t: TypeEngine[Any]) -> TypeGuard[TupleType]:
    return t._is_tuple_type


def is_has_clause_element(s: object) -> TypeGuard[roles.HasClauseElement]:
    return hasattr(s, "__clause_element__")


def is_has_column_element_clause_element(
    s: object,
) -> TypeGuard[roles.HasColumnElementClauseElement]:
    return hasattr(s, "__clause_element__")

from __future__ import annotations

from typing import Any
from typing import Iterable
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from . import roles
from .. import util
from ..inspection import Inspectable
from ..util.typing import Literal
from ..util.typing import Protocol

if TYPE_CHECKING:
    from .elements import ClauseElement
    from .elements import ColumnClause
    from .elements import ColumnElement
    from .elements import quoted_name
    from .elements import SQLCoreOperations
    from .elements import TextClause
    from .roles import ColumnsClauseRole
    from .roles import FromClauseRole
    from .schema import DefaultGenerator
    from .schema import Sequence
    from .selectable import FromClause
    from .selectable import NamedFromClause
    from .selectable import TableClause
    from .sqltypes import TupleType
    from .type_api import TypeEngine
    from ..util.typing import TypeGuard

_T = TypeVar("_T", bound=Any)


class _HasClauseElement(Protocol):
    """indicates a class that has a __clause_element__() method"""

    def __clause_element__(self) -> ColumnsClauseRole:
        ...


# convention:
# XYZArgument - something that the end user is passing to a public API method
# XYZElement - the internal representation that we use for the thing.
# the coercions system is responsible for converting from XYZArgument to
# XYZElement.

_ColumnsClauseArgument = Union[
    Literal["*", 1],
    roles.ColumnsClauseRole,
    Type[Any],
    Inspectable[_HasClauseElement],
    _HasClauseElement,
]

_SelectIterable = Iterable[Union["ColumnElement[Any]", "TextClause"]]

_FromClauseArgument = Union[
    roles.FromClauseRole,
    Type[Any],
    Inspectable[_HasClauseElement],
    _HasClauseElement,
]

_ColumnExpressionArgument = Union[
    "ColumnElement[_T]", _HasClauseElement, roles.ExpressionElementRole[_T]
]

_DMLColumnArgument = Union[str, "ColumnClause[Any]", _HasClauseElement]

_PropagateAttrsType = util.immutabledict[str, Any]

_TypeEngineArgument = Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"]


def is_named_from_clause(t: FromClauseRole) -> TypeGuard[NamedFromClause]:
    return t.named_with_column


def is_column_element(c: ClauseElement) -> TypeGuard[ColumnElement[Any]]:
    return c._is_column_element


def is_text_clause(c: ClauseElement) -> TypeGuard[TextClause]:
    return c._is_text_clause


def has_schema_attr(t: FromClauseRole) -> TypeGuard[TableClause]:
    return hasattr(t, "schema")


def is_quoted_name(s: str) -> TypeGuard[quoted_name]:
    return hasattr(s, "quote")


def is_tuple_type(t: TypeEngine[Any]) -> TypeGuard[TupleType]:
    return t._is_tuple_type


def is_has_clause_element(s: object) -> TypeGuard[_HasClauseElement]:
    return hasattr(s, "__clause_element__")

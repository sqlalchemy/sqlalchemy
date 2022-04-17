from __future__ import annotations

import operator
from typing import Any
from typing import Dict
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from sqlalchemy.sql.base import Executable
from . import roles
from .. import util
from ..inspection import Inspectable
from ..util.typing import Literal
from ..util.typing import Protocol

if TYPE_CHECKING:
    from .compiler import Compiled
    from .compiler import DDLCompiler
    from .compiler import SQLCompiler
    from .dml import UpdateBase
    from .dml import ValuesBase
    from .elements import ClauseElement
    from .elements import ColumnClause
    from .elements import ColumnElement
    from .elements import quoted_name
    from .elements import SQLCoreOperations
    from .elements import TextClause
    from .roles import ColumnsClauseRole
    from .roles import FromClauseRole
    from .schema import Column
    from .schema import DefaultGenerator
    from .schema import Sequence
    from .selectable import Alias
    from .selectable import FromClause
    from .selectable import Join
    from .selectable import NamedFromClause
    from .selectable import ReturnsRows
    from .selectable import Select
    from .selectable import SelectBase
    from .selectable import Subquery
    from .selectable import TableClause
    from .sqltypes import TableValueType
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

_TextCoercedExpressionArgument = Union[
    str,
    "TextClause",
    "ColumnElement[_T]",
    _HasClauseElement,
    roles.ExpressionElementRole[_T],
]

_ColumnsClauseArgument = Union[
    Literal["*", 1],
    roles.ColumnsClauseRole,
    Type[Any],
    Inspectable[_HasClauseElement],
    _HasClauseElement,
]
"""open-ended SELECT columns clause argument.

Includes column expressions, tables, ORM mapped entities, a few literal values.

This type is used for lists of columns  / entities to be returned in result
sets; select(...), insert().returning(...), etc.


"""

_ColumnExpressionArgument = Union[
    "ColumnElement[_T]", _HasClauseElement, roles.ExpressionElementRole[_T]
]
"""narrower "column expression" argument.

This type is used for all the other "column" kinds of expressions that
typically represent a single SQL column expression, not a set of columns the
way a table or ORM entity does.

This includes ColumnElement, or ORM-mapped attributes that will have a
`__clause_element__()` method, it also has the ExpressionElementRole
overall which brings in the TextClause object also.

"""

_InfoType = Dict[Any, Any]
"""the .info dictionary accepted and used throughout Core /ORM"""

_FromClauseArgument = Union[
    roles.FromClauseRole,
    Type[Any],
    Inspectable[_HasClauseElement],
    _HasClauseElement,
]
"""A FROM clause, like we would send to select().select_from().

Also accommodates ORM entities and related constructs.

"""

_JoinTargetArgument = Union[_FromClauseArgument, roles.JoinTargetRole]
"""target for join() builds on _FromClauseArgument to include additional
join target roles such as those which come from the ORM.

"""

_OnClauseArgument = Union[_ColumnExpressionArgument[Any], roles.OnClauseRole]
"""target for an ON clause, includes additional roles such as those which
come from the ORM.

"""

_SelectStatementForCompoundArgument = Union[
    "SelectBase", roles.CompoundElementRole
]
"""SELECT statement acceptable by ``union()`` and other SQL set operations"""

_DMLColumnArgument = Union[
    str, "ColumnClause[Any]", _HasClauseElement, roles.DMLColumnRole
]
"""A DML column expression.  This is a "key" inside of insert().values(),
update().values(), and related.

These are usually strings or SQL table columns.

There's also edge cases like JSON expression assignment, which we would want
the DMLColumnRole to be able to accommodate.

"""


_DDLColumnArgument = Union[str, "Column[Any]", roles.DDLConstraintColumnRole]
"""DDL column accepting string or `Column` references.

used for :class:`.PrimaryKeyConstraint`, :class:`.UniqueConstraint`, etc.

"""

_DMLTableArgument = Union[
    "TableClause",
    "Join",
    "Alias",
    Type[Any],
    Inspectable[_HasClauseElement],
    _HasClauseElement,
]

_PropagateAttrsType = util.immutabledict[str, Any]

_TypeEngineArgument = Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"]

if TYPE_CHECKING:

    def is_sql_compiler(c: Compiled) -> TypeGuard[SQLCompiler]:
        ...

    def is_ddl_compiler(c: Compiled) -> TypeGuard[DDLCompiler]:
        ...

    def is_named_from_clause(t: FromClauseRole) -> TypeGuard[NamedFromClause]:
        ...

    def is_column_element(c: ClauseElement) -> TypeGuard[ColumnElement[Any]]:
        ...

    def is_text_clause(c: ClauseElement) -> TypeGuard[TextClause]:
        ...

    def is_from_clause(c: ClauseElement) -> TypeGuard[FromClause]:
        ...

    def is_tuple_type(t: TypeEngine[Any]) -> TypeGuard[TupleType]:
        ...

    def is_table_value_type(t: TypeEngine[Any]) -> TypeGuard[TableValueType]:
        ...

    def is_select_base(
        t: Union[Executable, ReturnsRows]
    ) -> TypeGuard[SelectBase]:
        ...

    def is_select_statement(
        t: Union[Executable, ReturnsRows]
    ) -> TypeGuard[Select]:
        ...

    def is_table(t: FromClause) -> TypeGuard[TableClause]:
        ...

    def is_subquery(t: FromClause) -> TypeGuard[Subquery]:
        ...

    def is_dml(c: ClauseElement) -> TypeGuard[UpdateBase]:
        ...

else:

    is_sql_compiler = operator.attrgetter("is_sql")
    is_ddl_compiler = operator.attrgetter("is_ddl")
    is_named_from_clause = operator.attrgetter("named_with_column")
    is_column_element = operator.attrgetter("_is_column_element")
    is_text_clause = operator.attrgetter("_is_text_clause")
    is_from_clause = operator.attrgetter("_is_from_clause")
    is_tuple_type = operator.attrgetter("_is_tuple_type")
    is_table_value_type = operator.attrgetter("_is_table_value")
    is_select_base = operator.attrgetter("_is_select_base")
    is_select_statement = operator.attrgetter("_is_select_statement")
    is_table = operator.attrgetter("_is_table")
    is_subquery = operator.attrgetter("_is_subquery")
    is_dml = operator.attrgetter("is_dml")


def has_schema_attr(t: FromClauseRole) -> TypeGuard[TableClause]:
    return hasattr(t, "schema")


def is_quoted_name(s: str) -> TypeGuard[quoted_name]:
    return hasattr(s, "quote")


def is_has_clause_element(s: object) -> TypeGuard[_HasClauseElement]:
    return hasattr(s, "__clause_element__")


def is_insert_update(c: ClauseElement) -> TypeGuard[ValuesBase]:
    return c.is_dml and (c.is_insert or c.is_update)  # type: ignore

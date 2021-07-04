# sql/expression.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Defines the public namespace for SQL expression constructs.

Prior to version 0.9, this module contained all of "elements", "dml",
"default_comparator" and "selectable".   The module was broken up
and most "factory" functions were moved to be grouped with their associated
class.

"""

__all__ = [
    "Alias",
    "AliasedReturnsRows",
    "any_",
    "all_",
    "CacheKey",
    "ClauseElement",
    "ColumnCollection",
    "ColumnElement",
    "CompoundSelect",
    "Delete",
    "FromClause",
    "Insert",
    "Join",
    "Lateral",
    "LambdaElement",
    "StatementLambdaElement",
    "Select",
    "Selectable",
    "TableClause",
    "TableValuedAlias",
    "Update",
    "Values",
    "alias",
    "and_",
    "asc",
    "between",
    "bindparam",
    "case",
    "cast",
    "column",
    "custom_op",
    "cte",
    "delete",
    "desc",
    "distinct",
    "except_",
    "except_all",
    "exists",
    "extract",
    "func",
    "modifier",
    "collate",
    "insert",
    "intersect",
    "intersect_all",
    "join",
    "label",
    "lateral",
    "lambda_stmt",
    "literal",
    "literal_column",
    "not_",
    "null",
    "nulls_first",
    "nulls_last",
    "or_",
    "outparam",
    "outerjoin",
    "over",
    "select",
    "table",
    "text",
    "tuple_",
    "type_coerce",
    "quoted_name",
    "union",
    "union_all",
    "update",
    "quoted_name",
    "within_group",
    "Subquery",
    "TableSample",
    "tablesample",
    "values",
]


from .base import _from_objects
from .base import _select_iterables
from .base import ColumnCollection
from .base import Executable
from .base import PARSE_AUTOCOMMIT
from .dml import Delete
from .dml import Insert
from .dml import Update
from .dml import UpdateBase
from .dml import ValuesBase
from .elements import _truncated_label
from .elements import between
from .elements import BinaryExpression
from .elements import BindParameter
from .elements import BooleanClauseList
from .elements import Case
from .elements import Cast
from .elements import ClauseElement
from .elements import ClauseList
from .elements import collate
from .elements import CollectionAggregate
from .elements import ColumnClause
from .elements import ColumnElement
from .elements import Extract
from .elements import False_
from .elements import FunctionFilter
from .elements import Grouping
from .elements import Label
from .elements import literal
from .elements import literal_column
from .elements import not_
from .elements import Null
from .elements import outparam
from .elements import Over
from .elements import quoted_name
from .elements import ReleaseSavepointClause
from .elements import RollbackToSavepointClause
from .elements import SavepointClause
from .elements import TextClause
from .elements import True_
from .elements import Tuple
from .elements import TypeClause
from .elements import TypeCoerce
from .elements import UnaryExpression
from .elements import WithinGroup
from .functions import func
from .functions import Function
from .functions import FunctionElement
from .functions import modifier
from .lambdas import lambda_stmt
from .lambdas import LambdaElement
from .lambdas import StatementLambdaElement
from .operators import ColumnOperators
from .operators import custom_op
from .operators import Operators
from .selectable import Alias
from .selectable import AliasedReturnsRows
from .selectable import CompoundSelect
from .selectable import CTE
from .selectable import Exists
from .selectable import FromClause
from .selectable import FromGrouping
from .selectable import GenerativeSelect
from .selectable import HasCTE
from .selectable import HasPrefixes
from .selectable import HasSuffixes
from .selectable import Join
from .selectable import LABEL_STYLE_DEFAULT
from .selectable import LABEL_STYLE_DISAMBIGUATE_ONLY
from .selectable import LABEL_STYLE_NONE
from .selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from .selectable import Lateral
from .selectable import ReturnsRows
from .selectable import ScalarSelect
from .selectable import Select
from .selectable import Selectable
from .selectable import SelectBase
from .selectable import Subquery
from .selectable import subquery
from .selectable import TableClause
from .selectable import TableSample
from .selectable import TableValuedAlias
from .selectable import TextAsFrom
from .selectable import TextualSelect
from .selectable import Values
from .traversals import CacheKey
from .visitors import Visitable
from ..util.langhelpers import public_factory

# factory functions - these pull class-bound constructors and classmethods
# from SQL elements and selectables into public functions.  This allows
# the functions to be available in the sqlalchemy.sql.* namespace and
# to be auto-cross-documenting from the function to the class itself.

all_ = public_factory(CollectionAggregate._create_all, ".sql.expression.all_")
any_ = public_factory(CollectionAggregate._create_any, ".sql.expression.any_")
and_ = public_factory(BooleanClauseList.and_, ".sql.expression.and_")
alias = public_factory(Alias._factory, ".sql.expression.alias")
tablesample = public_factory(
    TableSample._factory, ".sql.expression.tablesample"
)
lateral = public_factory(Lateral._factory, ".sql.expression.lateral")
or_ = public_factory(BooleanClauseList.or_, ".sql.expression.or_")
bindparam = public_factory(BindParameter, ".sql.expression.bindparam")
select = public_factory(Select._create, ".sql.expression.select")
text = public_factory(TextClause._create_text, ".sql.expression.text")
table = public_factory(TableClause, ".sql.expression.table")
column = public_factory(ColumnClause, ".sql.expression.column")
over = public_factory(Over, ".sql.expression.over")
within_group = public_factory(WithinGroup, ".sql.expression.within_group")
label = public_factory(Label, ".sql.expression.label")
case = public_factory(Case, ".sql.expression.case")
cast = public_factory(Cast, ".sql.expression.cast")
cte = public_factory(CTE._factory, ".sql.expression.cte")
values = public_factory(Values, ".sql.expression.values")
extract = public_factory(Extract, ".sql.expression.extract")
tuple_ = public_factory(Tuple, ".sql.expression.tuple_")
except_ = public_factory(
    CompoundSelect._create_except, ".sql.expression.except_"
)
except_all = public_factory(
    CompoundSelect._create_except_all, ".sql.expression.except_all"
)
intersect = public_factory(
    CompoundSelect._create_intersect, ".sql.expression.intersect"
)
intersect_all = public_factory(
    CompoundSelect._create_intersect_all, ".sql.expression.intersect_all"
)
union = public_factory(CompoundSelect._create_union, ".sql.expression.union")
union_all = public_factory(
    CompoundSelect._create_union_all, ".sql.expression.union_all"
)
exists = public_factory(Exists, ".sql.expression.exists")
nulls_first = public_factory(
    UnaryExpression._create_nulls_first, ".sql.expression.nulls_first"
)
nullsfirst = nulls_first  # deprecated 1.4; see #5435
nulls_last = public_factory(
    UnaryExpression._create_nulls_last, ".sql.expression.nulls_last"
)
nullslast = nulls_last  # deprecated 1.4; see #5435
asc = public_factory(UnaryExpression._create_asc, ".sql.expression.asc")
desc = public_factory(UnaryExpression._create_desc, ".sql.expression.desc")
distinct = public_factory(
    UnaryExpression._create_distinct, ".sql.expression.distinct"
)
type_coerce = public_factory(TypeCoerce, ".sql.expression.type_coerce")
true = public_factory(True_._instance, ".sql.expression.true")
false = public_factory(False_._instance, ".sql.expression.false")
null = public_factory(Null._instance, ".sql.expression.null")
join = public_factory(Join._create_join, ".sql.expression.join")
outerjoin = public_factory(Join._create_outerjoin, ".sql.expression.outerjoin")
insert = public_factory(Insert, ".sql.expression.insert")
update = public_factory(Update, ".sql.expression.update")
delete = public_factory(Delete, ".sql.expression.delete")
funcfilter = public_factory(FunctionFilter, ".sql.expression.funcfilter")


# internal functions still being called from tests and the ORM,
# these might be better off in some other namespace


# old names for compatibility
_Executable = Executable
_BindParamClause = BindParameter
_Label = Label
_SelectBase = SelectBase
_BinaryExpression = BinaryExpression
_Cast = Cast
_Null = Null
_False = False_
_True = True_
_TextClause = TextClause
_UnaryExpression = UnaryExpression
_Case = Case
_Tuple = Tuple
_Over = Over
_TypeClause = TypeClause
_Extract = Extract
_Exists = Exists
_Grouping = Grouping
_FromGrouping = FromGrouping
_ScalarSelect = ScalarSelect

# sql/expression.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

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


from .base import _from_objects  # noqa
from .base import _select_iterables  # noqa
from .base import ColumnCollection  # noqa
from .base import Executable  # noqa
from .base import PARSE_AUTOCOMMIT  # noqa
from .dml import Delete  # noqa
from .dml import Insert  # noqa
from .dml import Update  # noqa
from .dml import UpdateBase  # noqa
from .dml import ValuesBase  # noqa
from .elements import _truncated_label  # noqa
from .elements import between  # noqa
from .elements import BinaryExpression  # noqa
from .elements import BindParameter  # noqa
from .elements import BooleanClauseList  # noqa
from .elements import Case  # noqa
from .elements import Cast  # noqa
from .elements import ClauseElement  # noqa
from .elements import ClauseList  # noqa
from .elements import collate  # noqa
from .elements import CollectionAggregate  # noqa
from .elements import ColumnClause  # noqa
from .elements import ColumnElement  # noqa
from .elements import Extract  # noqa
from .elements import False_  # noqa
from .elements import FunctionFilter  # noqa
from .elements import Grouping  # noqa
from .elements import Label  # noqa
from .elements import literal  # noqa
from .elements import literal_column  # noqa
from .elements import not_  # noqa
from .elements import Null  # noqa
from .elements import outparam  # noqa
from .elements import Over  # noqa
from .elements import quoted_name  # noqa
from .elements import ReleaseSavepointClause  # noqa
from .elements import RollbackToSavepointClause  # noqa
from .elements import SavepointClause  # noqa
from .elements import TextClause  # noqa
from .elements import True_  # noqa
from .elements import Tuple  # noqa
from .elements import TypeClause  # noqa
from .elements import TypeCoerce  # noqa
from .elements import UnaryExpression  # noqa
from .elements import WithinGroup  # noqa
from .functions import func  # noqa
from .functions import Function  # noqa
from .functions import FunctionElement  # noqa
from .functions import modifier  # noqa
from .lambdas import lambda_stmt  # noqa
from .lambdas import LambdaElement  # noqa
from .lambdas import StatementLambdaElement  # noqa
from .operators import ColumnOperators  # noqa
from .operators import custom_op  # noqa
from .operators import Operators  # noqa
from .selectable import Alias  # noqa
from .selectable import AliasedReturnsRows  # noqa
from .selectable import CompoundSelect  # noqa
from .selectable import CTE  # noqa
from .selectable import Exists  # noqa
from .selectable import FromClause  # noqa
from .selectable import FromGrouping  # noqa
from .selectable import GenerativeSelect  # noqa
from .selectable import HasCTE  # noqa
from .selectable import HasPrefixes  # noqa
from .selectable import HasSuffixes  # noqa
from .selectable import Join  # noqa
from .selectable import LABEL_STYLE_DEFAULT  # noqa
from .selectable import LABEL_STYLE_DISAMBIGUATE_ONLY  # noqa
from .selectable import LABEL_STYLE_NONE  # noqa
from .selectable import LABEL_STYLE_TABLENAME_PLUS_COL  # noqa
from .selectable import Lateral  # noqa
from .selectable import ReturnsRows  # noqa
from .selectable import ScalarSelect  # noqa
from .selectable import Select  # noqa
from .selectable import Selectable  # noqa
from .selectable import SelectBase  # noqa
from .selectable import Subquery  # noqa
from .selectable import subquery  # noqa
from .selectable import TableClause  # noqa
from .selectable import TableSample  # noqa
from .selectable import TableValuedAlias  # noqa
from .selectable import TextAsFrom  # noqa
from .selectable import TextualSelect  # noqa
from .selectable import Values  # noqa
from .traversals import CacheKey  # noqa
from .visitors import Visitable  # noqa
from ..util.langhelpers import public_factory  # noqa

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

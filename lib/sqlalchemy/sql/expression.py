# sql/expression.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
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
    "any_",
    "all_",
    "ClauseElement",
    "ColumnCollection",
    "ColumnElement",
    "CompoundSelect",
    "Delete",
    "FromClause",
    "Insert",
    "Join",
    "Lateral",
    "Select",
    "Selectable",
    "TableClause",
    "Update",
    "alias",
    "and_",
    "asc",
    "between",
    "bindparam",
    "case",
    "cast",
    "column",
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
    "literal",
    "literal_column",
    "not_",
    "null",
    "nullsfirst",
    "nullslast",
    "or_",
    "outparam",
    "outerjoin",
    "over",
    "select",
    "subquery",
    "table",
    "text",
    "tuple_",
    "type_coerce",
    "quoted_name",
    "union",
    "union_all",
    "update",
    "within_group",
    "TableSample",
    "tablesample",
]


from .base import _from_objects  # noqa
from .base import ColumnCollection  # noqa
from .base import Executable  # noqa
from .base import Generative  # noqa
from .base import PARSE_AUTOCOMMIT  # noqa
from .dml import Delete  # noqa
from .dml import Insert  # noqa
from .dml import Update  # noqa
from .dml import UpdateBase  # noqa
from .dml import ValuesBase  # noqa
from .elements import _clause_element_as_expr  # noqa
from .elements import _clone  # noqa
from .elements import _cloned_difference  # noqa
from .elements import _cloned_intersection  # noqa
from .elements import _column_as_key  # noqa
from .elements import _corresponding_column_or_error  # noqa
from .elements import _expression_literal_as_text  # noqa
from .elements import _is_column  # noqa
from .elements import _labeled  # noqa
from .elements import _literal_as_binds  # noqa
from .elements import _literal_as_column  # noqa
from .elements import _literal_as_label_reference  # noqa
from .elements import _literal_as_text  # noqa
from .elements import _only_column_elements  # noqa
from .elements import _select_iterables  # noqa
from .elements import _string_or_unprintable  # noqa
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
from .selectable import _interpret_as_from  # noqa
from .selectable import Alias  # noqa
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
from .selectable import Lateral  # noqa
from .selectable import ScalarSelect  # noqa
from .selectable import Select  # noqa
from .selectable import Selectable  # noqa
from .selectable import SelectBase  # noqa
from .selectable import subquery  # noqa
from .selectable import TableClause  # noqa
from .selectable import TableSample  # noqa
from .selectable import TextAsFrom  # noqa
from .visitors import Visitable  # noqa
from ..util.langhelpers import public_factory  # noqa


# factory functions - these pull class-bound constructors and classmethods
# from SQL elements and selectables into public functions.  This allows
# the functions to be available in the sqlalchemy.sql.* namespace and
# to be auto-cross-documenting from the function to the class itself.

all_ = public_factory(CollectionAggregate._create_all, ".expression.all_")
any_ = public_factory(CollectionAggregate._create_any, ".expression.any_")
and_ = public_factory(BooleanClauseList.and_, ".expression.and_")
alias = public_factory(Alias._factory, ".expression.alias")
tablesample = public_factory(TableSample._factory, ".expression.tablesample")
lateral = public_factory(Lateral._factory, ".expression.lateral")
or_ = public_factory(BooleanClauseList.or_, ".expression.or_")
bindparam = public_factory(BindParameter, ".expression.bindparam")
select = public_factory(Select, ".expression.select")
text = public_factory(TextClause._create_text, ".expression.text")
table = public_factory(TableClause, ".expression.table")
column = public_factory(ColumnClause, ".expression.column")
over = public_factory(Over, ".expression.over")
within_group = public_factory(WithinGroup, ".expression.within_group")
label = public_factory(Label, ".expression.label")
case = public_factory(Case, ".expression.case")
cast = public_factory(Cast, ".expression.cast")
cte = public_factory(CTE._factory, ".expression.cte")
extract = public_factory(Extract, ".exp  # noqaression.extract")
tuple_ = public_factory(Tuple, ".expression.tuple_")
except_ = public_factory(CompoundSelect._create_except, ".expression.except_")
except_all = public_factory(
    CompoundSelect._create_except_all, ".expression.except_all"
)
intersect = public_factory(
    CompoundSelect._create_intersect, ".expression.intersect"
)
intersect_all = public_factory(
    CompoundSelect._create_intersect_all, ".expression.intersect_all"
)
union = public_factory(CompoundSelect._create_union, ".expression.union")
union_all = public_factory(
    CompoundSelect._create_union_all, ".expression.union_all"
)
exists = public_factory(Exists, ".expression.exists")
nullsfirst = public_factory(
    UnaryExpression._create_nullsfirst, ".expression.nullsfirst"
)
nullslast = public_factory(
    UnaryExpression._create_nullslast, ".expression.nullslast"
)
asc = public_factory(UnaryExpression._create_asc, ".expression.asc")
desc = public_factory(UnaryExpression._create_desc, ".expression.desc")
distinct = public_factory(
    UnaryExpression._create_distinct, ".expression.distinct"
)
type_coerce = public_factory(TypeCoerce, ".expression.type_coerce")
true = public_factory(True_._instance, ".expression.true")
false = public_factory(False_._instance, ".expression.false")
null = public_factory(Null._instance, ".expression.null")
join = public_factory(Join._create_join, ".expression.join")
outerjoin = public_factory(Join._create_outerjoin, ".expression.outerjoin")
insert = public_factory(Insert, ".expression.insert")
update = public_factory(Update, ".expression.update")
delete = public_factory(Delete, ".expression.delete")
funcfilter = public_factory(FunctionFilter, ".expression.funcfilter")


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
_Generative = Generative
_TypeClause = TypeClause
_Extract = Extract
_Exists = Exists
_Grouping = Grouping
_FromGrouping = FromGrouping
_ScalarSelect = ScalarSelect

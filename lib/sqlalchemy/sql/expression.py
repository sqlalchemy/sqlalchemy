# sql/expression.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
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


from .base import ColumnCollection
from .base import Executable
from .base import Generative
from .dml import Delete
from .dml import Insert
from .dml import Update
from .elements import between
from .elements import BinaryExpression
from .elements import BindParameter
from .elements import BooleanClauseList
from .elements import Case
from .elements import Cast
from .elements import ClauseElement
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
from .elements import TextClause
from .elements import True_
from .elements import Tuple
from .elements import TypeClause
from .elements import TypeCoerce
from .elements import UnaryExpression
from .elements import WithinGroup
from .functions import func
from .functions import modifier
from .selectable import Alias
from .selectable import alias
from .selectable import CompoundSelect
from .selectable import Exists
from .selectable import FromClause
from .selectable import FromGrouping
from .selectable import Join
from .selectable import Lateral
from .selectable import lateral
from .selectable import ScalarSelect
from .selectable import Select
from .selectable import Selectable
from .selectable import SelectBase
from .selectable import subquery
from .selectable import TableClause
from .selectable import TableSample
from .selectable import tablesample
from ..util.langhelpers import public_factory

# factory functions - these pull class-bound constructors and classmethods
# from SQL elements and selectables into public functions.  This allows
# the functions to be available in the sqlalchemy.sql.* namespace and
# to be auto-cross-documenting from the function to the class itself.

all_ = public_factory(CollectionAggregate._create_all, ".expression.all_")
any_ = public_factory(CollectionAggregate._create_any, ".expression.any_")
and_ = public_factory(BooleanClauseList.and_, ".expression.and_")
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
extract = public_factory(Extract, ".expression.extract")
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

# sql/expression.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
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
    'Alias', 'ClauseElement', 'ColumnCollection', 'ColumnElement',
    'CompoundSelect', 'Delete', 'FromClause', 'Insert', 'Join', 'Select',
    'Selectable', 'TableClause', 'Update', 'alias', 'and_', 'asc', 'between',
    'bindparam', 'case', 'cast', 'column', 'delete', 'desc', 'distinct',
    'except_', 'except_all', 'exists', 'extract', 'func', 'modifier',
    'collate', 'insert', 'intersect', 'intersect_all', 'join', 'label',
    'literal', 'literal_column', 'not_', 'null', 'nullsfirst', 'nullslast',
    'or_', 'outparam', 'outerjoin', 'over', 'select', 'subquery',
    'table', 'text',
    'tuple_', 'type_coerce', 'union', 'union_all', 'update']


from .visitors import Visitable
from .functions import func, modifier, FunctionElement
from ..util.langhelpers import public_factory
from .elements import ClauseElement, ColumnElement,\
  BindParameter, UnaryExpression, BooleanClauseList, \
  Label, Cast, Case, ColumnClause, TextClause, Over, Null, \
  True_, False_, BinaryExpression, Tuple, TypeClause, Extract, \
  Grouping, not_, \
  collate, literal_column, between,\
  literal, outparam, type_coerce, ClauseList

from .elements import SavepointClause, RollbackToSavepointClause, \
  ReleaseSavepointClause

from .base import ColumnCollection, Generative, Executable, \
          PARSE_AUTOCOMMIT

from .selectable import Alias, Join, Select, Selectable, TableClause, \
        CompoundSelect, CTE, FromClause, FromGrouping, SelectBase, \
        alias, GenerativeSelect, \
        subquery, HasPrefixes, Exists, ScalarSelect, TextAsFrom


from .dml import Insert, Update, Delete, UpdateBase, ValuesBase

# factory functions - these pull class-bound constructors and classmethods
# from SQL elements and selectables into public functions.  This allows
# the functions to be available in the sqlalchemy.sql.* namespace and
# to be auto-cross-documenting from the function to the class itself.

and_ = public_factory(BooleanClauseList.and_, ".expression.and_")
or_ = public_factory(BooleanClauseList.or_, ".expression.or_")
bindparam = public_factory(BindParameter, ".expression.bindparam")
select = public_factory(Select, ".expression.select")
text = public_factory(TextClause._create_text, ".expression.text")
table = public_factory(TableClause, ".expression.table")
column = public_factory(ColumnClause, ".expression.column")
over = public_factory(Over, ".expression.over")
label = public_factory(Label, ".expression.label")
case = public_factory(Case, ".expression.case")
cast = public_factory(Cast, ".expression.cast")
extract = public_factory(Extract, ".expression.extract")
tuple_ = public_factory(Tuple, ".expression.tuple_")
except_ = public_factory(CompoundSelect._create_except, ".expression.except_")
except_all = public_factory(CompoundSelect._create_except_all, ".expression.except_all")
intersect = public_factory(CompoundSelect._create_intersect, ".expression.intersect")
intersect_all = public_factory(CompoundSelect._create_intersect_all, ".expression.intersect_all")
union = public_factory(CompoundSelect._create_union, ".expression.union")
union_all = public_factory(CompoundSelect._create_union_all, ".expression.union_all")
exists = public_factory(Exists, ".expression.exists")
nullsfirst = public_factory(UnaryExpression._create_nullsfirst, ".expression.nullsfirst")
nullslast = public_factory(UnaryExpression._create_nullslast, ".expression.nullslast")
asc = public_factory(UnaryExpression._create_asc, ".expression.asc")
desc = public_factory(UnaryExpression._create_desc, ".expression.desc")
distinct = public_factory(UnaryExpression._create_distinct, ".expression.distinct")
true = public_factory(True_._singleton, ".expression.true")
false = public_factory(False_._singleton, ".expression.false")
null = public_factory(Null._singleton, ".expression.null")
join = public_factory(Join._create_join, ".expression.join")
outerjoin = public_factory(Join._create_outerjoin, ".expression.outerjoin")
insert = public_factory(Insert, ".expression.insert")
update = public_factory(Update, ".expression.update")
delete = public_factory(Delete, ".expression.delete")


# internal functions still being called from tests and the ORM,
# these might be better off in some other namespace
from .base import _from_objects
from .elements import _literal_as_text, _clause_element_as_expr,\
  _is_column, _labeled, _only_column_elements, _string_or_unprintable, \
    _truncated_label, _clone, _cloned_difference, _cloned_intersection,\
    _column_as_key, _literal_as_binds, _select_iterables, \
    _corresponding_column_or_error
from .selectable import _interpret_as_from



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

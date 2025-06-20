# dialects/sqlite/dml.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .._typing import _OnConflictIndexElementsT
from .._typing import _OnConflictIndexWhereT
from .._typing import _OnConflictSetT
from .._typing import _OnConflictWhereT
from ... import util
from ...sql import coercions
from ...sql import roles
from ...sql import schema
from ...sql._typing import _DMLTableArgument
from ...sql.base import _exclusive_against
from ...sql.base import ColumnCollection
from ...sql.base import ReadOnlyColumnCollection
from ...sql.base import SyntaxExtension
from ...sql.dml import _DMLColumnElement
from ...sql.dml import Insert as StandardInsert
from ...sql.elements import ClauseElement
from ...sql.elements import ColumnElement
from ...sql.elements import KeyedColumnElement
from ...sql.elements import TextClause
from ...sql.expression import alias
from ...sql.sqltypes import NULLTYPE
from ...sql.visitors import InternalTraversal
from ...util.typing import Self

__all__ = ("Insert", "insert")


def insert(table: _DMLTableArgument) -> Insert:
    """Construct a sqlite-specific variant :class:`_sqlite.Insert`
    construct.

    .. container:: inherited_member

        The :func:`sqlalchemy.dialects.sqlite.insert` function creates
        a :class:`sqlalchemy.dialects.sqlite.Insert`.  This class is based
        on the dialect-agnostic :class:`_sql.Insert` construct which may
        be constructed using the :func:`_sql.insert` function in
        SQLAlchemy Core.

    The :class:`_sqlite.Insert` construct includes additional methods
    :meth:`_sqlite.Insert.on_conflict_do_update`,
    :meth:`_sqlite.Insert.on_conflict_do_nothing`.

    """
    return Insert(table)


class Insert(StandardInsert):
    """SQLite-specific implementation of INSERT.

    Adds methods for SQLite-specific syntaxes such as ON CONFLICT.

    The :class:`_sqlite.Insert` object is created using the
    :func:`sqlalchemy.dialects.sqlite.insert` function.

    .. versionadded:: 1.4

    .. seealso::

        :ref:`sqlite_on_conflict_insert`

    """

    stringify_dialect = "sqlite"
    inherit_cache = True

    @util.memoized_property
    def excluded(
        self,
    ) -> ReadOnlyColumnCollection[str, KeyedColumnElement[Any]]:
        """Provide the ``excluded`` namespace for an ON CONFLICT statement

        SQLite's ON CONFLICT clause allows reference to the row that would
        be inserted, known as ``excluded``.  This attribute provides
        all columns in this row to be referenceable.

        .. tip::  The :attr:`_sqlite.Insert.excluded` attribute is an instance
            of :class:`_expression.ColumnCollection`, which provides an
            interface the same as that of the :attr:`_schema.Table.c`
            collection described at :ref:`metadata_tables_and_columns`.
            With this collection, ordinary names are accessible like attributes
            (e.g. ``stmt.excluded.some_column``), but special names and
            dictionary method names should be accessed using indexed access,
            such as ``stmt.excluded["column name"]`` or
            ``stmt.excluded["values"]``.  See the docstring for
            :class:`_expression.ColumnCollection` for further examples.

        """
        return alias(self.table, name="excluded").columns

    _on_conflict_exclusive = _exclusive_against(
        "_post_values_clause",
        msgs={
            "_post_values_clause": "This Insert construct already has "
            "an ON CONFLICT clause established"
        },
    )

    @_on_conflict_exclusive
    def on_conflict_do_update(
        self,
        index_elements: _OnConflictIndexElementsT = None,
        index_where: _OnConflictIndexWhereT = None,
        set_: _OnConflictSetT = None,
        where: _OnConflictWhereT = None,
    ) -> Self:
        r"""
        Specifies a DO UPDATE SET action for ON CONFLICT clause.

        :param index_elements:
         A sequence consisting of string column names, :class:`_schema.Column`
         objects, or other column expression objects that will be used
         to infer a target index or unique constraint.

        :param index_where:
         Additional WHERE criterion that can be used to infer a
         conditional target index.

        :param set\_:
         A dictionary or other mapping object
         where the keys are either names of columns in the target table,
         or :class:`_schema.Column` objects or other ORM-mapped columns
         matching that of the target table, and expressions or literals
         as values, specifying the ``SET`` actions to take.

         .. versionadded:: 1.4 The
            :paramref:`_sqlite.Insert.on_conflict_do_update.set_`
            parameter supports :class:`_schema.Column` objects from the target
            :class:`_schema.Table` as keys.

         .. warning:: This dictionary does **not** take into account
            Python-specified default UPDATE values or generation functions,
            e.g. those specified using :paramref:`_schema.Column.onupdate`.
            These values will not be exercised for an ON CONFLICT style of
            UPDATE, unless they are manually specified in the
            :paramref:`.Insert.on_conflict_do_update.set_` dictionary.

        :param where:
         Optional argument. An expression object representing a ``WHERE``
         clause that restricts the rows affected by ``DO UPDATE SET``. Rows not
         meeting the ``WHERE`` condition will not be updated (effectively a
         ``DO NOTHING`` for those rows).

        """

        return self.ext(
            OnConflictDoUpdate(index_elements, index_where, set_, where)
        )

    @_on_conflict_exclusive
    def on_conflict_do_nothing(
        self,
        index_elements: _OnConflictIndexElementsT = None,
        index_where: _OnConflictIndexWhereT = None,
    ) -> Self:
        """
        Specifies a DO NOTHING action for ON CONFLICT clause.

        :param index_elements:
         A sequence consisting of string column names, :class:`_schema.Column`
         objects, or other column expression objects that will be used
         to infer a target index or unique constraint.

        :param index_where:
         Additional WHERE criterion that can be used to infer a
         conditional target index.

        """

        return self.ext(OnConflictDoNothing(index_elements, index_where))


class OnConflictClause(SyntaxExtension, ClauseElement):
    stringify_dialect = "sqlite"

    inferred_target_elements: Optional[List[Union[str, schema.Column[Any]]]]
    inferred_target_whereclause: Optional[
        Union[ColumnElement[Any], TextClause]
    ]

    _traverse_internals = [
        ("inferred_target_elements", InternalTraversal.dp_multi_list),
        ("inferred_target_whereclause", InternalTraversal.dp_clauseelement),
    ]

    def __init__(
        self,
        index_elements: _OnConflictIndexElementsT = None,
        index_where: _OnConflictIndexWhereT = None,
    ):
        if index_elements is not None:
            self.inferred_target_elements = [
                coercions.expect(roles.DDLConstraintColumnRole, column)
                for column in index_elements
            ]
            self.inferred_target_whereclause = (
                coercions.expect(
                    roles.WhereHavingRole,
                    index_where,
                )
                if index_where is not None
                else None
            )
        else:
            self.inferred_target_elements = (
                self.inferred_target_whereclause
            ) = None

    def apply_to_insert(self, insert_stmt: StandardInsert) -> None:
        insert_stmt.apply_syntax_extension_point(
            self.append_replacing_same_type, "post_values"
        )


class OnConflictDoNothing(OnConflictClause):
    __visit_name__ = "on_conflict_do_nothing"

    inherit_cache = True


class OnConflictDoUpdate(OnConflictClause):
    __visit_name__ = "on_conflict_do_update"

    update_values_to_set: Dict[_DMLColumnElement, ColumnElement[Any]]
    update_whereclause: Optional[ColumnElement[Any]]

    _traverse_internals = OnConflictClause._traverse_internals + [
        ("update_values_to_set", InternalTraversal.dp_dml_values),
        ("update_whereclause", InternalTraversal.dp_clauseelement),
    ]

    def __init__(
        self,
        index_elements: _OnConflictIndexElementsT = None,
        index_where: _OnConflictIndexWhereT = None,
        set_: _OnConflictSetT = None,
        where: _OnConflictWhereT = None,
    ):
        super().__init__(
            index_elements=index_elements,
            index_where=index_where,
        )

        if isinstance(set_, dict):
            if not set_:
                raise ValueError("set parameter dictionary must not be empty")
        elif isinstance(set_, ColumnCollection):
            set_ = dict(set_)
        else:
            raise ValueError(
                "set parameter must be a non-empty dictionary "
                "or a ColumnCollection such as the `.c.` collection "
                "of a Table object"
            )
        self.update_values_to_set = {
            coercions.expect(roles.DMLColumnRole, k): coercions.expect(
                roles.ExpressionElementRole, v, type_=NULLTYPE, is_crud=True
            )
            for k, v in set_.items()
        }
        self.update_whereclause = (
            coercions.expect(roles.WhereHavingRole, where)
            if where is not None
            else None
        )

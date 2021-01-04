# postgresql/on_conflict.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from . import ext
from ... import util
from ...sql import schema
from ...sql.base import _generative
from ...sql.dml import Insert as StandardInsert
from ...sql.elements import ClauseElement
from ...sql.expression import alias
from ...util.langhelpers import public_factory


__all__ = ("Insert", "insert")


class Insert(StandardInsert):
    """PostgreSQL-specific implementation of INSERT.

    Adds methods for PG-specific syntaxes such as ON CONFLICT.

    The :class:`_postgresql.Insert` object is created using the
    :func:`sqlalchemy.dialects.postgresql.insert` function.

    .. versionadded:: 1.1

    """

    @util.memoized_property
    def excluded(self):
        """Provide the ``excluded`` namespace for an ON CONFLICT statement

        PG's ON CONFLICT clause allows reference to the row that would
        be inserted, known as ``excluded``.  This attribute provides
        all columns in this row to be referenceable.

        .. seealso::

            :ref:`postgresql_insert_on_conflict` - example of how
            to use :attr:`_expression.Insert.excluded`

        """
        return alias(self.table, name="excluded").columns

    @_generative
    def on_conflict_do_update(
        self,
        constraint=None,
        index_elements=None,
        index_where=None,
        set_=None,
        where=None,
    ):
        r"""
        Specifies a DO UPDATE SET action for ON CONFLICT clause.

        Either the ``constraint`` or ``index_elements`` argument is
        required, but only one of these can be specified.

        :param constraint:
         The name of a unique or exclusion constraint on the table,
         or the constraint object itself if it has a .name attribute.

        :param index_elements:
         A sequence consisting of string column names, :class:`_schema.Column`
         objects, or other column expression objects that will be used
         to infer a target index.

        :param index_where:
         Additional WHERE criterion that can be used to infer a
         conditional target index.

        :param set\_:
         Required argument. A dictionary or other mapping object
         with column names as keys and expressions or literals as values,
         specifying the ``SET`` actions to take.
         If the target :class:`_schema.Column` specifies a ".
         key" attribute distinct
         from the column name, that key should be used.

         .. warning:: This dictionary does **not** take into account
            Python-specified default UPDATE values or generation functions,
            e.g. those specified using :paramref:`_schema.Column.onupdate`.
            These values will not be exercised for an ON CONFLICT style of
            UPDATE, unless they are manually specified in the
            :paramref:`.Insert.on_conflict_do_update.set_` dictionary.

        :param where:
         Optional argument. If present, can be a literal SQL
         string or an acceptable expression for a ``WHERE`` clause
         that restricts the rows affected by ``DO UPDATE SET``. Rows
         not meeting the ``WHERE`` condition will not be updated
         (effectively a ``DO NOTHING`` for those rows).

         .. versionadded:: 1.1


        .. seealso::

            :ref:`postgresql_insert_on_conflict`

        """
        self._post_values_clause = OnConflictDoUpdate(
            constraint, index_elements, index_where, set_, where
        )
        return self

    @_generative
    def on_conflict_do_nothing(
        self, constraint=None, index_elements=None, index_where=None
    ):
        """
        Specifies a DO NOTHING action for ON CONFLICT clause.

        The ``constraint`` and ``index_elements`` arguments
        are optional, but only one of these can be specified.

        :param constraint:
         The name of a unique or exclusion constraint on the table,
         or the constraint object itself if it has a .name attribute.

        :param index_elements:
         A sequence consisting of string column names, :class:`_schema.Column`
         objects, or other column expression objects that will be used
         to infer a target index.

        :param index_where:
         Additional WHERE criterion that can be used to infer a
         conditional target index.

         .. versionadded:: 1.1

        .. seealso::

            :ref:`postgresql_insert_on_conflict`

        """
        self._post_values_clause = OnConflictDoNothing(
            constraint, index_elements, index_where
        )
        return self


insert = public_factory(
    Insert, ".dialects.postgresql.insert", ".dialects.postgresql.Insert"
)


class OnConflictClause(ClauseElement):
    def __init__(self, constraint=None, index_elements=None, index_where=None):

        if constraint is not None:
            if not isinstance(constraint, util.string_types) and isinstance(
                constraint,
                (schema.Index, schema.Constraint, ext.ExcludeConstraint),
            ):
                constraint = getattr(constraint, "name") or constraint

        if constraint is not None:
            if index_elements is not None:
                raise ValueError(
                    "'constraint' and 'index_elements' are mutually exclusive"
                )

            if isinstance(constraint, util.string_types):
                self.constraint_target = constraint
                self.inferred_target_elements = None
                self.inferred_target_whereclause = None
            elif isinstance(constraint, schema.Index):
                index_elements = constraint.expressions
                index_where = constraint.dialect_options["postgresql"].get(
                    "where"
                )
            elif isinstance(constraint, ext.ExcludeConstraint):
                index_elements = constraint.columns
                index_where = constraint.where
            else:
                index_elements = constraint.columns
                index_where = constraint.dialect_options["postgresql"].get(
                    "where"
                )

        if index_elements is not None:
            self.constraint_target = None
            self.inferred_target_elements = index_elements
            self.inferred_target_whereclause = index_where
        elif constraint is None:
            self.constraint_target = (
                self.inferred_target_elements
            ) = self.inferred_target_whereclause = None


class OnConflictDoNothing(OnConflictClause):
    __visit_name__ = "on_conflict_do_nothing"


class OnConflictDoUpdate(OnConflictClause):
    __visit_name__ = "on_conflict_do_update"

    def __init__(
        self,
        constraint=None,
        index_elements=None,
        index_where=None,
        set_=None,
        where=None,
    ):
        super(OnConflictDoUpdate, self).__init__(
            constraint=constraint,
            index_elements=index_elements,
            index_where=index_where,
        )

        if (
            self.inferred_target_elements is None
            and self.constraint_target is None
        ):
            raise ValueError(
                "Either constraint or index_elements, "
                "but not both, must be specified unless DO NOTHING"
            )

        if not isinstance(set_, dict) or not set_:
            raise ValueError("set parameter must be a non-empty dictionary")
        self.update_values_to_set = [
            (key, value) for key, value in set_.items()
        ]
        self.update_whereclause = where

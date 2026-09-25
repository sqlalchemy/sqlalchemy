# sql/naming.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: allow-untyped-defs, allow-untyped-calls

"""Establish constraint and index naming conventions."""

from __future__ import annotations

import re

from . import events  # noqa
from .base import _NONE_NAME
from .elements import conv as conv
from .schema import CheckConstraint
from .schema import Column
from .schema import Constraint
from .schema import ForeignKeyConstraint
from .schema import Index
from .schema import PrimaryKeyConstraint
from .schema import Table
from .schema import UniqueConstraint
from .. import event
from .. import exc


class ConventionDict:
    def __init__(self, const, table, convention):
        self.const = const
        self._is_fk = isinstance(const, ForeignKeyConstraint)
        self.table = table
        self.convention = convention
        self._const_name = const.name

    def _key_table_name(self):
        return self.table.name

    def _column_X(self, idx, attrname):
        if self._is_fk:
            try:
                fk = self.const.elements[idx]
            except IndexError:
                return ""
            else:
                return getattr(fk.parent, attrname)
        else:
            cols = list(self.const.columns)
            try:
                col = cols[idx]
            except IndexError:
                return ""
            else:
                return getattr(col, attrname)

    def _key_constraint_name(self):
        if self._const_name in (None, _NONE_NAME):
            raise exc.InvalidRequestError(
                "Naming convention including "
                "%(constraint_name)s token requires that "
                "constraint is explicitly named."
            )
        if not isinstance(self._const_name, conv):
            self.const.name = None
        return self._const_name

    def _key_column_X_key(self, idx):
        # note this method was missing before
        # [ticket:3989], meaning tokens like ``%(column_0_key)s`` weren't
        # working even though documented.
        return self._column_X(idx, "key")

    def _key_column_X_name(self, idx):
        return self._column_X(idx, "name")

    def _key_column_X_label(self, idx):
        return self._column_X(idx, "_ddl_label")

    def _key_referred_table_name(self):
        fk = self.const.elements[0]
        return fk.target_tokens.table_name

    def _key_referred_column_X_name(self, idx):
        fk = self.const.elements[idx]
        # note that before [ticket:3989], this method was returning
        # the specification for the :class:`.ForeignKey` itself, which normally
        # would be using the ``.key`` of the column, not the name.
        try:
            return fk.column.name
        except exc.NoReferenceError as err:
            # ``fk.column`` resolves the target on access. The name is built
            # when the constraint is attached to its table, which for a
            # string-based ForeignKey can be before the referenced Table is
            # in the MetaData -- every other path resolves lazily, so the
            # plain "could not find table" message gives no hint that the
            # naming convention is what forced the resolution.
            raise self._referred_resolution_error(err) from err

    def _referred_resolution_error(self, err):
        """Re-raise a failed target resolution as the same error type, saying
        what actually required the foreign key to be resolvable.

        The type is preserved so that existing
        ``except NoReferencedTableError`` handling keeps working.

        """
        message = (
            "Naming convention including the "
            "%%(referred_column_0_name)s token requires the foreign key "
            "target to be resolvable when the constraint is attached to "
            "table '%s'; %s. Either define the referenced table before this "
            "one, pass the target Column object to ForeignKey() rather than "
            "a string, or use the %%(referred_table_name)s token, which is "
            "read from the string and does not require resolution."
            % (self.table.name, err.args[0])
        )
        if isinstance(err, exc.NoReferencedColumnError):
            return exc.NoReferencedColumnError(
                message, err.table_name, err.column_name
            )
        elif isinstance(err, exc.NoReferencedTableError):
            return exc.NoReferencedTableError(message, err.table_name)
        else:
            return type(err)(message)

    def __getitem__(self, key):
        if key in self.convention:
            return self.convention[key](self.const, self.table)
        elif hasattr(self, "_key_%s" % key):
            return getattr(self, "_key_%s" % key)()
        else:
            col_template = re.match(r".*_?column_(\d+)(_?N)?_.+", key)
            if col_template:
                idx = col_template.group(1)
                multiples = col_template.group(2)

                if multiples:
                    if self._is_fk:
                        elems = self.const.elements
                    else:
                        elems = list(self.const.columns)
                    tokens = []
                    for idx, elem in enumerate(elems):
                        attr = "_key_" + key.replace("0" + multiples, "X")
                        try:
                            tokens.append(getattr(self, attr)(idx))
                        except AttributeError:
                            raise KeyError(key)
                    sep = "_" if multiples.startswith("_") else ""
                    return sep.join(tokens)
                else:
                    attr = "_key_" + key.replace(idx, "X")
                    idx = int(idx)
                    if hasattr(self, attr):
                        return getattr(self, attr)(idx)
        raise KeyError(key)


_prefix_dict = {
    Index: "ix",
    PrimaryKeyConstraint: "pk",
    CheckConstraint: "ck",
    UniqueConstraint: "uq",
    ForeignKeyConstraint: "fk",
}


def _get_convention(dict_, key):
    for super_ in key.__mro__:
        if super_ in _prefix_dict and _prefix_dict[super_] in dict_:
            return dict_[_prefix_dict[super_]]
        elif super_ in dict_:
            return dict_[super_]
    else:
        return None


def _constraint_name_for_table(const, table):
    metadata = table.metadata
    convention = _get_convention(metadata.naming_convention, type(const))

    if isinstance(const.name, conv):
        return const.name
    elif (
        convention is not None
        and not isinstance(const.name, conv)
        and (
            const.name is None
            or "constraint_name" in convention
            or const.name is _NONE_NAME
        )
    ):
        return conv(
            convention
            % ConventionDict(const, table, metadata.naming_convention)
        )
    elif convention is _NONE_NAME:
        return None


@event.listens_for(
    PrimaryKeyConstraint, "_sa_event_column_added_to_pk_constraint"
)
def _column_added_to_pk_constraint(pk_constraint, col):
    if pk_constraint._implicit_generated:
        # only operate upon the "implicit" pk constraint for now,
        # as we have to force the name to None to reset it.  the
        # "implicit" constraint will only have a naming convention name
        # if at all.
        table = pk_constraint.table
        pk_constraint.name = None
        newname = _constraint_name_for_table(pk_constraint, table)
        if newname:
            pk_constraint.name = newname


@event.listens_for(Constraint, "after_parent_attach")
@event.listens_for(Index, "after_parent_attach")
def _constraint_name(const, table):
    if isinstance(table, Column):
        # this path occurs for a CheckConstraint linked to a Column

        # for column-attached constraint, set another event
        # to link the column attached to the table as this constraint
        # associated with the table.
        event.listen(
            table,
            "after_parent_attach",
            lambda col, table: _constraint_name(const, table),
        )

    elif isinstance(table, Table):
        if isinstance(const.name, conv) or const.name is _NONE_NAME:
            return

        newname = _constraint_name_for_table(const, table)
        if newname:
            const.name = newname

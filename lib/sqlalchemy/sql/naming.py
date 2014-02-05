# sqlalchemy/naming.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Establish constraint and index naming conventions.


"""

from .schema import Constraint, ForeignKeyConstraint, PrimaryKeyConstraint, \
                UniqueConstraint, CheckConstraint, Index, Table
from .. import event, events
from .. import exc
from .elements import _truncated_label
import re

class ConventionDict(object):
    def __init__(self, const, table, convention):
        self.const = const
        self._is_fk = isinstance(const, ForeignKeyConstraint)
        self.table = table
        self.convention = convention
        self._const_name = const.name

    def _key_table_name(self):
        return self.table.name

    def _column_X(self, idx):
        if self._is_fk:
            fk = self.const.elements[idx]
            return fk.parent
        else:
            return list(self.const.columns)[idx]

    def _key_constraint_name(self):
        if not self._const_name:
            raise exc.InvalidRequestError(
                    "Naming convention including "
                    "%(constraint_name)s token requires that "
                    "constraint is explicitly named."
                )
        # they asked for a name that's derived from the existing
        # name, so set the existing name to None
        self.const.name = None
        return self._const_name

    def _key_column_X_name(self, idx):
        return self._column_X(idx).name

    def _key_column_X_label(self, idx):
        return self._column_X(idx)._label

    def _key_referred_table_name(self):
        fk = self.const.elements[0]
        refs = fk.target_fullname.split(".")
        if len(refs) == 3:
            refschema, reftable, refcol = refs
        else:
            reftable, refcol = refs
        return reftable

    def _key_referred_column_X_name(self, idx):
        fk = self.const.elements[idx]
        refs = fk.target_fullname.split(".")
        if len(refs) == 3:
            refschema, reftable, refcol = refs
        else:
            reftable, refcol = refs
        return refcol

    def __getitem__(self, key):
        if key in self.convention:
            return self.convention[key](self.const, self.table)
        elif hasattr(self, '_key_%s' % key):
            return getattr(self, '_key_%s' % key)()
        else:
            col_template = re.match(r".*_?column_(\d+)_.+", key)
            if col_template:
                idx = col_template.group(1)
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
    ForeignKeyConstraint: "fk"
}

def _get_convention(dict_, key):

    for super_ in key.__mro__:
        if super_ in _prefix_dict and _prefix_dict[super_] in dict_:
            return dict_[_prefix_dict[super_]]
        elif super_ in dict_:
            return dict_[super_]
    else:
        return None


@event.listens_for(Constraint, "after_parent_attach")
@event.listens_for(Index, "after_parent_attach")
def _constraint_name(const, table):
    if isinstance(table, Table):
        metadata = table.metadata
        convention = _get_convention(metadata.naming_convention, type(const))
        if convention is not None:
            newname = _truncated_label(
                        convention % ConventionDict(const, table, metadata.naming_convention)
                        )
            if const.name is None:
                const.name = newname

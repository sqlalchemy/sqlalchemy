# sqlalchemy/naming.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Establish constraint and index naming conventions.


"""

from .schema import Constraint, ForeignKeyConstraint, PrimaryKeyConstraint, \
                UniqueConstraint, CheckConstraint, Index, Table, Column
from .. import event, events
from .. import exc
from .elements import _truncated_label
import re

class conv(_truncated_label):
    """Mark a string indicating that a name has already been converted
    by a naming convention.

    This is a string subclass that indicates a name that should not be
    subject to any further naming conventions.

    E.g. when we create a :class:`.Constraint` using a naming convention
    as follows::

        m = MetaData(naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"})
        t = Table('t', m, Column('x', Integer),
                        CheckConstraint('x > 5', name='x5'))

    The name of the above constraint will be rendered as ``"ck_t_x5"``.  That is,
    the existing name ``x5`` is used in the naming convention as the ``constraint_name``
    token.

    In some situations, such as in migration scripts, we may be rendering
    the above :class:`.CheckConstraint` with a name that's already been
    converted.  In order to make sure the name isn't double-modified, the
    new name is applied using the :func:`.schema.conv` marker.  We can
    use this explicitly as follows::


        m = MetaData(naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"})
        t = Table('t', m, Column('x', Integer),
                        CheckConstraint('x > 5', name=conv('ck_t_x5')))

    Where above, the :func:`.schema.conv` marker indicates that the constraint
    name here is final, and the name will render as ``"ck_t_x5"`` and not
    ``"ck_t_ck_t_x5"``

    .. versionadded:: 0.9.4

    .. seealso::

        :ref:`constraint_naming_conventions`

    """

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
        if not isinstance(self._const_name, conv):
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
    if isinstance(table, Column):
        # for column-attached constraint, set another event
        # to link the column attached to the table as this constraint
        # associated with the table.
        event.listen(table, "after_parent_attach",
                    lambda col, table: _constraint_name(const, table)
                )
    elif isinstance(table, Table):
        metadata = table.metadata
        convention = _get_convention(metadata.naming_convention, type(const))
        if convention is not None:
            newname = conv(
                        convention % ConventionDict(const, table, metadata.naming_convention)
                        )
            if const.name is None:
                const.name = newname

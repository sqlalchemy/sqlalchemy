from ... import util
from ...sql.base import _generative
from ...sql.dml import Insert as StandardInsert
from ...sql.elements import ClauseElement
from ...sql.expression import alias
from ...util.langhelpers import public_factory

__all__ = ("Insert", "insert")


class Insert(StandardInsert):
    """MySQL-specific implementation of INSERT.

    Adds methods for MySQL-specific syntaxes such as ON DUPLICATE KEY UPDATE.

    .. versionadded:: 1.2

    """

    @property
    def inserted(self):
        """Provide the "inserted" namespace for an ON DUPLICATE KEY UPDATE statement

        MySQL's ON DUPLICATE KEY UPDATE clause allows reference to the row
        that would be inserted, via a special function called ``VALUES()``.
        This attribute provides all columns in this row to be referenceable
        such that they will render within a ``VALUES()`` function inside the
        ON DUPLICATE KEY UPDATE clause.    The attribute is named ``.inserted``
        so as not to conflict with the existing :meth:`.Insert.values` method.

        .. seealso::

            :ref:`mysql_insert_on_duplicate_key_update` - example of how
            to use :attr:`.Insert.inserted`

        """
        return self.inserted_alias.columns

    @util.memoized_property
    def inserted_alias(self):
        return alias(self.table, name="inserted")

    @_generative
    def on_duplicate_key_update(self, **kw):
        r"""
        Specifies the ON DUPLICATE KEY UPDATE clause.

        :param \**kw:  Column keys linked to UPDATE values.  The
         values may be any SQL expression or supported literal Python
         values.

        .. warning:: This dictionary does **not** take into account
           Python-specified default UPDATE values or generation functions,
           e.g. those specified using :paramref:`.Column.onupdate`.
           These values will not be exercised for an ON DUPLICATE KEY UPDATE
           style of UPDATE, unless values are manually specified here.

        .. versionadded:: 1.2

        .. seealso::

            :ref:`mysql_insert_on_duplicate_key_update`

        """
        inserted_alias = getattr(self, "inserted_alias", None)
        self._post_values_clause = OnDuplicateClause(inserted_alias, kw)
        return self


insert = public_factory(Insert, ".dialects.mysql.insert")


class OnDuplicateClause(ClauseElement):
    __visit_name__ = "on_duplicate_key_update"

    def __init__(self, inserted_alias, update):
        self.inserted_alias = inserted_alias
        if not update or not isinstance(update, dict):
            raise ValueError("update parameter must be a non-empty dictionary")
        self.update = update

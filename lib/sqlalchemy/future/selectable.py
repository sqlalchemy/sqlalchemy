from ..sql import coercions
from ..sql import roles
from ..sql.base import _generative
from ..sql.selectable import GenerativeSelect
from ..sql.selectable import Select as _LegacySelect
from ..sql.selectable import SelectState
from ..sql.util import _entity_namespace_key


class Select(_LegacySelect):
    _is_future = True
    _setup_joins = ()
    _legacy_setup_joins = ()
    inherit_cache = True

    @classmethod
    def _create_select(cls, *entities):
        raise NotImplementedError("use _create_future_select")

    @classmethod
    def _create_future_select(cls, *entities):
        r"""Construct a new :class:`_expression.Select` using the 2.
        x style API.

        .. versionadded:: 2.0 - the :func:`_future.select` construct is
           the same construct as the one returned by
           :func:`_expression.select`, except that the function only
           accepts the "columns clause" entities up front; the rest of the
           state of the SELECT should be built up using generative methods.

        Similar functionality is also available via the
        :meth:`_expression.FromClause.select` method on any
        :class:`_expression.FromClause`.

        .. seealso::

            :ref:`coretutorial_selecting` - Core Tutorial description of
            :func:`_expression.select`.

        :param \*entities:
          Entities to SELECT from.  For Core usage, this is typically a series
          of :class:`_expression.ColumnElement` and / or
          :class:`_expression.FromClause`
          objects which will form the columns clause of the resulting
          statement.   For those objects that are instances of
          :class:`_expression.FromClause` (typically :class:`_schema.Table`
          or :class:`_expression.Alias`
          objects), the :attr:`_expression.FromClause.c`
          collection is extracted
          to form a collection of :class:`_expression.ColumnElement` objects.

          This parameter will also accept :class:`_expression.TextClause`
          constructs as
          given, as well as ORM-mapped classes.

        """

        self = cls.__new__(cls)
        self._raw_columns = [
            coercions.expect(
                roles.ColumnsClauseRole, ent, apply_propagate_attrs=self
            )
            for ent in entities
        ]

        GenerativeSelect.__init__(self)

        return self

    def filter(self, *criteria):
        """A synonym for the :meth:`_future.Select.where` method."""

        return self.where(*criteria)

    def _exported_columns_iterator(self):
        meth = SelectState.get_plugin_class(self).exported_columns_iterator
        return meth(self)

    def _filter_by_zero(self):
        if self._setup_joins:
            meth = SelectState.get_plugin_class(
                self
            ).determine_last_joined_entity
            _last_joined_entity = meth(self)
            if _last_joined_entity is not None:
                return _last_joined_entity

        if self._from_obj:
            return self._from_obj[0]

        return self._raw_columns[0]

    def filter_by(self, **kwargs):
        r"""apply the given filtering criterion as a WHERE clause
        to this select.

        """
        from_entity = self._filter_by_zero()

        clauses = [
            _entity_namespace_key(from_entity, key) == value
            for key, value in kwargs.items()
        ]
        return self.filter(*clauses)

    @property
    def column_descriptions(self):
        """Return a 'column descriptions' structure which may be
        plugin-specific.

        """
        meth = SelectState.get_plugin_class(self).get_column_descriptions
        return meth(self)

    @_generative
    def join(self, target, onclause=None, isouter=False, full=False):
        r"""Create a SQL JOIN against this :class:`_expresson.Select`
        object's criterion
        and apply generatively, returning the newly resulting
        :class:`_expression.Select`.


        """
        target = coercions.expect(
            roles.JoinTargetRole, target, apply_propagate_attrs=self
        )
        self._setup_joins += (
            (target, onclause, None, {"isouter": isouter, "full": full}),
        )

    @_generative
    def join_from(
        self, from_, target, onclause=None, isouter=False, full=False
    ):
        r"""Create a SQL JOIN against this :class:`_expresson.Select`
        object's criterion
        and apply generatively, returning the newly resulting
        :class:`_expression.Select`.


        """
        # note the order of parsing from vs. target is important here, as we
        # are also deriving the source of the plugin (i.e. the subject mapper
        # in an ORM query) which should favor the "from_" over the "target"

        from_ = coercions.expect(
            roles.FromClauseRole, from_, apply_propagate_attrs=self
        )
        target = coercions.expect(
            roles.JoinTargetRole, target, apply_propagate_attrs=self
        )

        self._setup_joins += (
            (target, onclause, from_, {"isouter": isouter, "full": full}),
        )

    def outerjoin(self, target, onclause=None, full=False):
        """Create a left outer join.



        """
        return self.join(target, onclause=onclause, isouter=True, full=full,)

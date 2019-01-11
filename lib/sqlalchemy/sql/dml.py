# sql/dml.py
# Copyright (C) 2009-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
Provide :class:`.Insert`, :class:`.Update` and :class:`.Delete`.

"""

from .base import _from_objects
from .base import _generative
from .base import DialectKWArgs
from .base import Executable
from .elements import _clone
from .elements import _column_as_key
from .elements import _literal_as_text
from .elements import and_
from .elements import ClauseElement
from .elements import Null
from .selectable import _interpret_as_from
from .selectable import _interpret_as_select
from .selectable import HasCTE
from .selectable import HasPrefixes
from .. import exc
from .. import util


class UpdateBase(
    HasCTE, DialectKWArgs, HasPrefixes, Executable, ClauseElement
):
    """Form the base for ``INSERT``, ``UPDATE``, and ``DELETE`` statements.

    """

    __visit_name__ = "update_base"

    _execution_options = Executable._execution_options.union(
        {"autocommit": True}
    )
    _hints = util.immutabledict()
    _parameter_ordering = None
    _prefixes = ()
    named_with_column = False

    def _process_colparams(self, parameters):
        def process_single(p):
            if isinstance(p, (list, tuple)):
                return dict((c.key, pval) for c, pval in zip(self.table.c, p))
            else:
                return p

        if self._preserve_parameter_order and parameters is not None:
            if not isinstance(parameters, list) or (
                parameters and not isinstance(parameters[0], tuple)
            ):
                raise ValueError(
                    "When preserve_parameter_order is True, "
                    "values() only accepts a list of 2-tuples"
                )
            self._parameter_ordering = [key for key, value in parameters]

            return dict(parameters), False

        if (
            isinstance(parameters, (list, tuple))
            and parameters
            and isinstance(parameters[0], (list, tuple, dict))
        ):

            if not self._supports_multi_parameters:
                raise exc.InvalidRequestError(
                    "This construct does not support "
                    "multiple parameter sets."
                )

            return [process_single(p) for p in parameters], True
        else:
            return process_single(parameters), False

    def params(self, *arg, **kw):
        """Set the parameters for the statement.

        This method raises ``NotImplementedError`` on the base class,
        and is overridden by :class:`.ValuesBase` to provide the
        SET/VALUES clause of UPDATE and INSERT.

        """
        raise NotImplementedError(
            "params() is not supported for INSERT/UPDATE/DELETE statements."
            " To set the values for an INSERT or UPDATE statement, use"
            " stmt.values(**parameters)."
        )

    def bind(self):
        """Return a 'bind' linked to this :class:`.UpdateBase`
        or a :class:`.Table` associated with it.

        """
        return self._bind or self.table.bind

    def _set_bind(self, bind):
        self._bind = bind

    bind = property(bind, _set_bind)

    @_generative
    def returning(self, *cols):
        r"""Add a :term:`RETURNING` or equivalent clause to this statement.

        e.g.::

            stmt = table.update().\
                      where(table.c.data == 'value').\
                      values(status='X').\
                      returning(table.c.server_flag,
                                table.c.updated_timestamp)

            for server_flag, updated_timestamp in connection.execute(stmt):
                print(server_flag, updated_timestamp)

        The given collection of column expressions should be derived from
        the table that is
        the target of the INSERT, UPDATE, or DELETE.  While :class:`.Column`
        objects are typical, the elements can also be expressions::

            stmt = table.insert().returning(
                (table.c.first_name + " " + table.c.last_name).
                label('fullname'))

        Upon compilation, a RETURNING clause, or database equivalent,
        will be rendered within the statement.   For INSERT and UPDATE,
        the values are the newly inserted/updated values.  For DELETE,
        the values are those of the rows which were deleted.

        Upon execution, the values of the columns to be returned are made
        available via the result set and can be iterated using
        :meth:`.ResultProxy.fetchone` and similar.   For DBAPIs which do not
        natively support returning values (i.e. cx_oracle), SQLAlchemy will
        approximate this behavior at the result level so that a reasonable
        amount of behavioral neutrality is provided.

        Note that not all databases/DBAPIs
        support RETURNING.   For those backends with no support,
        an exception is raised upon compilation and/or execution.
        For those who do support it, the functionality across backends
        varies greatly, including restrictions on executemany()
        and other statements which return multiple rows. Please
        read the documentation notes for the database in use in
        order to determine the availability of RETURNING.

        .. seealso::

          :meth:`.ValuesBase.return_defaults` - an alternative method tailored
          towards efficient fetching of server-side defaults and triggers
          for single-row INSERTs or UPDATEs.


        """
        self._returning = cols

    @_generative
    def with_hint(self, text, selectable=None, dialect_name="*"):
        """Add a table hint for a single table to this
        INSERT/UPDATE/DELETE statement.

        .. note::

         :meth:`.UpdateBase.with_hint` currently applies only to
         Microsoft SQL Server.  For MySQL INSERT/UPDATE/DELETE hints, use
         :meth:`.UpdateBase.prefix_with`.

        The text of the hint is rendered in the appropriate
        location for the database backend in use, relative
        to the :class:`.Table` that is the subject of this
        statement, or optionally to that of the given
        :class:`.Table` passed as the ``selectable`` argument.

        The ``dialect_name`` option will limit the rendering of a particular
        hint to a particular backend. Such as, to add a hint
        that only takes effect for SQL Server::

            mytable.insert().with_hint("WITH (PAGLOCK)", dialect_name="mssql")

        .. versionadded:: 0.7.6

        :param text: Text of the hint.
        :param selectable: optional :class:`.Table` that specifies
         an element of the FROM clause within an UPDATE or DELETE
         to be the subject of the hint - applies only to certain backends.
        :param dialect_name: defaults to ``*``, if specified as the name
         of a particular dialect, will apply these hints only when
         that dialect is in use.
         """
        if selectable is None:
            selectable = self.table

        self._hints = self._hints.union({(selectable, dialect_name): text})


class ValuesBase(UpdateBase):
    """Supplies support for :meth:`.ValuesBase.values` to
    INSERT and UPDATE constructs."""

    __visit_name__ = "values_base"

    _supports_multi_parameters = False
    _has_multi_parameters = False
    _preserve_parameter_order = False
    select = None
    _post_values_clause = None

    def __init__(self, table, values, prefixes):
        self.table = _interpret_as_from(table)
        self.parameters, self._has_multi_parameters = self._process_colparams(
            values
        )
        if prefixes:
            self._setup_prefixes(prefixes)

    @_generative
    def values(self, *args, **kwargs):
        r"""specify a fixed VALUES clause for an INSERT statement, or the SET
        clause for an UPDATE.

        Note that the :class:`.Insert` and :class:`.Update` constructs support
        per-execution time formatting of the VALUES and/or SET clauses,
        based on the arguments passed to :meth:`.Connection.execute`.
        However, the :meth:`.ValuesBase.values` method can be used to "fix" a
        particular set of parameters into the statement.

        Multiple calls to :meth:`.ValuesBase.values` will produce a new
        construct, each one with the parameter list modified to include
        the new parameters sent.  In the typical case of a single
        dictionary of parameters, the newly passed keys will replace
        the same keys in the previous construct.  In the case of a list-based
        "multiple values" construct, each new list of values is extended
        onto the existing list of values.

        :param \**kwargs: key value pairs representing the string key
          of a :class:`.Column` mapped to the value to be rendered into the
          VALUES or SET clause::

                users.insert().values(name="some name")

                users.update().where(users.c.id==5).values(name="some name")

        :param \*args: As an alternative to passing key/value parameters,
         a dictionary, tuple, or list of dictionaries or tuples can be passed
         as a single positional argument in order to form the VALUES or
         SET clause of the statement.  The forms that are accepted vary
         based on whether this is an :class:`.Insert` or an :class:`.Update`
         construct.

         For either an :class:`.Insert` or :class:`.Update` construct, a
         single dictionary can be passed, which works the same as that of
         the kwargs form::

            users.insert().values({"name": "some name"})

            users.update().values({"name": "some new name"})

         Also for either form but more typically for the :class:`.Insert`
         construct, a tuple that contains an entry for every column in the
         table is also accepted::

            users.insert().values((5, "some name"))

         The :class:`.Insert` construct also supports being passed a list
         of dictionaries or full-table-tuples, which on the server will
         render the less common SQL syntax of "multiple values" - this
         syntax is supported on backends such as SQLite, PostgreSQL, MySQL,
         but not necessarily others::

            users.insert().values([
                                {"name": "some name"},
                                {"name": "some other name"},
                                {"name": "yet another name"},
                            ])

         The above form would render a multiple VALUES statement similar to::

                INSERT INTO users (name) VALUES
                                (:name_1),
                                (:name_2),
                                (:name_3)

         It is essential to note that **passing multiple values is
         NOT the same as using traditional executemany() form**.  The above
         syntax is a **special** syntax not typically used.  To emit an
         INSERT statement against multiple rows, the normal method is
         to pass a multiple values list to the :meth:`.Connection.execute`
         method, which is supported by all database backends and is generally
         more efficient for a very large number of parameters.

           .. seealso::

               :ref:`execute_multiple` - an introduction to
               the traditional Core method of multiple parameter set
               invocation for INSERTs and other statements.

           .. versionchanged:: 1.0.0 an INSERT that uses a multiple-VALUES
              clause, even a list of length one,
              implies that the :paramref:`.Insert.inline` flag is set to
              True, indicating that the statement will not attempt to fetch
              the "last inserted primary key" or other defaults.  The
              statement deals with an arbitrary number of rows, so the
              :attr:`.ResultProxy.inserted_primary_key` accessor does not
              apply.

           .. versionchanged:: 1.0.0 A multiple-VALUES INSERT now supports
              columns with Python side default values and callables in the
              same way as that of an "executemany" style of invocation; the
              callable is invoked for each row.   See :ref:`bug_3288`
              for other details.

         The :class:`.Update` construct supports a special form which is a
         list of 2-tuples, which when provided must be passed in conjunction
         with the
         :paramref:`~sqlalchemy.sql.expression.update.preserve_parameter_order`
         parameter.
         This form causes the UPDATE statement to render the SET clauses
         using the order of parameters given to :meth:`.Update.values`, rather
         than the ordering of columns given in the :class:`.Table`.

           .. versionadded:: 1.0.10 - added support for parameter-ordered
              UPDATE statements via the
              :paramref:`~sqlalchemy.sql.expression.update.preserve_parameter_order`
              flag.

           .. seealso::

              :ref:`updates_order_parameters` - full example of the
              :paramref:`~sqlalchemy.sql.expression.update.preserve_parameter_order`
              flag

        .. seealso::

            :ref:`inserts_and_updates` - SQL Expression
            Language Tutorial

            :func:`~.expression.insert` - produce an ``INSERT`` statement

            :func:`~.expression.update` - produce an ``UPDATE`` statement

        """
        if self.select is not None:
            raise exc.InvalidRequestError(
                "This construct already inserts from a SELECT"
            )
        if self._has_multi_parameters and kwargs:
            raise exc.InvalidRequestError(
                "This construct already has multiple parameter sets."
            )

        if args:
            if len(args) > 1:
                raise exc.ArgumentError(
                    "Only a single dictionary/tuple or list of "
                    "dictionaries/tuples is accepted positionally."
                )
            v = args[0]
        else:
            v = {}

        if self.parameters is None:
            (
                self.parameters,
                self._has_multi_parameters,
            ) = self._process_colparams(v)
        else:
            if self._has_multi_parameters:
                self.parameters = list(self.parameters)
                p, self._has_multi_parameters = self._process_colparams(v)
                if not self._has_multi_parameters:
                    raise exc.ArgumentError(
                        "Can't mix single-values and multiple values "
                        "formats in one statement"
                    )

                self.parameters.extend(p)
            else:
                self.parameters = self.parameters.copy()
                p, self._has_multi_parameters = self._process_colparams(v)
                if self._has_multi_parameters:
                    raise exc.ArgumentError(
                        "Can't mix single-values and multiple values "
                        "formats in one statement"
                    )
                self.parameters.update(p)

        if kwargs:
            if self._has_multi_parameters:
                raise exc.ArgumentError(
                    "Can't pass kwargs and multiple parameter sets "
                    "simultaneously"
                )
            else:
                self.parameters.update(kwargs)

    @_generative
    def return_defaults(self, *cols):
        """Make use of a :term:`RETURNING` clause for the purpose
        of fetching server-side expressions and defaults.

        E.g.::

            stmt = table.insert().values(data='newdata').return_defaults()

            result = connection.execute(stmt)

            server_created_at = result.returned_defaults['created_at']

        When used against a backend that supports RETURNING, all column
        values generated by SQL expression or server-side-default will be
        added to any existing RETURNING clause, provided that
        :meth:`.UpdateBase.returning` is not used simultaneously.  The column
        values will then be available on the result using the
        :attr:`.ResultProxy.returned_defaults` accessor as a dictionary,
        referring to values keyed to the :class:`.Column` object as well as
        its ``.key``.

        This method differs from :meth:`.UpdateBase.returning` in these ways:

        1. :meth:`.ValuesBase.return_defaults` is only intended for use with
           an INSERT or an UPDATE statement that matches exactly one row.
           While the RETURNING construct in the general sense supports
           multiple rows for a multi-row UPDATE or DELETE statement, or for
           special cases of INSERT that return multiple rows (e.g. INSERT from
           SELECT, multi-valued VALUES clause),
           :meth:`.ValuesBase.return_defaults` is intended only for an
           "ORM-style" single-row INSERT/UPDATE statement.  The row returned
           by the statement is also consumed implicitly when
           :meth:`.ValuesBase.return_defaults` is used.  By contrast,
           :meth:`.UpdateBase.returning` leaves the RETURNING result-set
           intact with a collection of any number of rows.

        2. It is compatible with the existing logic to fetch auto-generated
           primary key values, also known as "implicit returning".  Backends
           that support RETURNING will automatically make use of RETURNING in
           order to fetch the value of newly generated primary keys; while the
           :meth:`.UpdateBase.returning` method circumvents this behavior,
           :meth:`.ValuesBase.return_defaults` leaves it intact.

        3. It can be called against any backend.  Backends that don't support
           RETURNING will skip the usage of the feature, rather than raising
           an exception.  The return value of
           :attr:`.ResultProxy.returned_defaults` will be ``None``

        :meth:`.ValuesBase.return_defaults` is used by the ORM to provide
        an efficient implementation for the ``eager_defaults`` feature of
        :func:`.mapper`.

        :param cols: optional list of column key names or :class:`.Column`
         objects.  If omitted, all column expressions evaluated on the server
         are added to the returning list.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`.UpdateBase.returning`

            :attr:`.ResultProxy.returned_defaults`

        """
        self._return_defaults = cols or True


class Insert(ValuesBase):
    """Represent an INSERT construct.

    The :class:`.Insert` object is created using the
    :func:`~.expression.insert()` function.

    .. seealso::

        :ref:`coretutorial_insert_expressions`

    """

    __visit_name__ = "insert"

    _supports_multi_parameters = True

    def __init__(
        self,
        table,
        values=None,
        inline=False,
        bind=None,
        prefixes=None,
        returning=None,
        return_defaults=False,
        **dialect_kw
    ):
        """Construct an :class:`.Insert` object.

        Similar functionality is available via the
        :meth:`~.TableClause.insert` method on
        :class:`~.schema.Table`.

        :param table: :class:`.TableClause` which is the subject of the
         insert.

        :param values: collection of values to be inserted; see
         :meth:`.Insert.values` for a description of allowed formats here.
         Can be omitted entirely; a :class:`.Insert` construct will also
         dynamically render the VALUES clause at execution time based on
         the parameters passed to :meth:`.Connection.execute`.

        :param inline: if True, no attempt will be made to retrieve the
         SQL-generated default values to be provided within the statement;
         in particular,
         this allows SQL expressions to be rendered 'inline' within the
         statement without the need to pre-execute them beforehand; for
         backends that support "returning", this turns off the "implicit
         returning" feature for the statement.

        If both `values` and compile-time bind parameters are present, the
        compile-time bind parameters override the information specified
        within `values` on a per-key basis.

        The keys within `values` can be either
        :class:`~sqlalchemy.schema.Column` objects or their string
        identifiers. Each key may reference one of:

        * a literal data value (i.e. string, number, etc.);
        * a Column object;
        * a SELECT statement.

        If a ``SELECT`` statement is specified which references this
        ``INSERT`` statement's table, the statement will be correlated
        against the ``INSERT`` statement.

        .. seealso::

            :ref:`coretutorial_insert_expressions` - SQL Expression Tutorial

            :ref:`inserts_and_updates` - SQL Expression Tutorial

        """
        ValuesBase.__init__(self, table, values, prefixes)
        self._bind = bind
        self.select = self.select_names = None
        self.include_insert_from_select_defaults = False
        self.inline = inline
        self._returning = returning
        self._validate_dialect_kwargs(dialect_kw)
        self._return_defaults = return_defaults

    def get_children(self, **kwargs):
        if self.select is not None:
            return (self.select,)
        else:
            return ()

    @_generative
    def from_select(self, names, select, include_defaults=True):
        """Return a new :class:`.Insert` construct which represents
        an ``INSERT...FROM SELECT`` statement.

        e.g.::

            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)

        :param names: a sequence of string column names or :class:`.Column`
         objects representing the target columns.
        :param select: a :func:`.select` construct, :class:`.FromClause`
         or other construct which resolves into a :class:`.FromClause`,
         such as an ORM :class:`.Query` object, etc.  The order of
         columns returned from this FROM clause should correspond to the
         order of columns sent as the ``names`` parameter;  while this
         is not checked before passing along to the database, the database
         would normally raise an exception if these column lists don't
         correspond.
        :param include_defaults: if True, non-server default values and
         SQL expressions as specified on :class:`.Column` objects
         (as documented in :ref:`metadata_defaults_toplevel`) not
         otherwise specified in the list of names will be rendered
         into the INSERT and SELECT statements, so that these values are also
         included in the data to be inserted.

         .. note:: A Python-side default that uses a Python callable function
            will only be invoked **once** for the whole statement, and **not
            per row**.

         .. versionadded:: 1.0.0 - :meth:`.Insert.from_select` now renders
            Python-side and SQL expression column defaults into the
            SELECT statement for columns otherwise not included in the
            list of column names.

        .. versionchanged:: 1.0.0 an INSERT that uses FROM SELECT
           implies that the :paramref:`.insert.inline` flag is set to
           True, indicating that the statement will not attempt to fetch
           the "last inserted primary key" or other defaults.  The statement
           deals with an arbitrary number of rows, so the
           :attr:`.ResultProxy.inserted_primary_key` accessor does not apply.

        .. versionadded:: 0.8.3

        """
        if self.parameters:
            raise exc.InvalidRequestError(
                "This construct already inserts value expressions"
            )

        self.parameters, self._has_multi_parameters = self._process_colparams(
            {_column_as_key(n): Null() for n in names}
        )

        self.select_names = names
        self.inline = True
        self.include_insert_from_select_defaults = include_defaults
        self.select = _interpret_as_select(select)

    def _copy_internals(self, clone=_clone, **kw):
        # TODO: coverage
        self.parameters = self.parameters.copy()
        if self.select is not None:
            self.select = _clone(self.select)


class Update(ValuesBase):
    """Represent an Update construct.

    The :class:`.Update` object is created using the :func:`update()`
    function.

    """

    __visit_name__ = "update"

    def __init__(
        self,
        table,
        whereclause=None,
        values=None,
        inline=False,
        bind=None,
        prefixes=None,
        returning=None,
        return_defaults=False,
        preserve_parameter_order=False,
        **dialect_kw
    ):
        r"""Construct an :class:`.Update` object.

        E.g.::

            from sqlalchemy import update

            stmt = update(users).where(users.c.id==5).\
                    values(name='user #5')

        Similar functionality is available via the
        :meth:`~.TableClause.update` method on
        :class:`.Table`::

            stmt = users.update().\
                        where(users.c.id==5).\
                        values(name='user #5')

        :param table: A :class:`.Table` object representing the database
         table to be updated.

        :param whereclause: Optional SQL expression describing the ``WHERE``
         condition of the ``UPDATE`` statement.   Modern applications
         may prefer to use the generative :meth:`~Update.where()`
         method to specify the ``WHERE`` clause.

         The WHERE clause can refer to multiple tables.
         For databases which support this, an ``UPDATE FROM`` clause will
         be generated, or on MySQL, a multi-table update.  The statement
         will fail on databases that don't have support for multi-table
         update statements.  A SQL-standard method of referring to
         additional tables in the WHERE clause is to use a correlated
         subquery::

            users.update().values(name='ed').where(
                    users.c.name==select([addresses.c.email_address]).\
                                where(addresses.c.user_id==users.c.id).\
                                as_scalar()
                    )

         .. versionchanged:: 0.7.4
             The WHERE clause of UPDATE can refer to multiple tables.

        :param values:
          Optional dictionary which specifies the ``SET`` conditions of the
          ``UPDATE``.  If left as ``None``, the ``SET``
          conditions are determined from those parameters passed to the
          statement during the execution and/or compilation of the
          statement.   When compiled standalone without any parameters,
          the ``SET`` clause generates for all columns.

          Modern applications may prefer to use the generative
          :meth:`.Update.values` method to set the values of the
          UPDATE statement.

        :param inline:
          if True, SQL defaults present on :class:`.Column` objects via
          the ``default`` keyword will be compiled 'inline' into the statement
          and not pre-executed.  This means that their values will not
          be available in the dictionary returned from
          :meth:`.ResultProxy.last_updated_params`.

        :param preserve_parameter_order: if True, the update statement is
          expected to receive parameters **only** via the
          :meth:`.Update.values` method, and they must be passed as a Python
          ``list`` of 2-tuples. The rendered UPDATE statement will emit the SET
          clause for each referenced column maintaining this order.

          .. versionadded:: 1.0.10

          .. seealso::

            :ref:`updates_order_parameters` - full example of the
            :paramref:`~.update.preserve_parameter_order` flag

        If both ``values`` and compile-time bind parameters are present, the
        compile-time bind parameters override the information specified
        within ``values`` on a per-key basis.

        The keys within ``values`` can be either :class:`.Column`
        objects or their string identifiers (specifically the "key" of the
        :class:`.Column`, normally but not necessarily equivalent to
        its "name").  Normally, the
        :class:`.Column` objects used here are expected to be
        part of the target :class:`.Table` that is the table
        to be updated.  However when using MySQL, a multiple-table
        UPDATE statement can refer to columns from any of
        the tables referred to in the WHERE clause.

        The values referred to in ``values`` are typically:

        * a literal data value (i.e. string, number, etc.)
        * a SQL expression, such as a related :class:`.Column`,
          a scalar-returning :func:`.select` construct,
          etc.

        When combining :func:`.select` constructs within the values
        clause of an :func:`.update` construct,
        the subquery represented by the :func:`.select` should be
        *correlated* to the parent table, that is, providing criterion
        which links the table inside the subquery to the outer table
        being updated::

            users.update().values(
                    name=select([addresses.c.email_address]).\
                            where(addresses.c.user_id==users.c.id).\
                            as_scalar()
                )

        .. seealso::

            :ref:`inserts_and_updates` - SQL Expression
            Language Tutorial


        """
        self._preserve_parameter_order = preserve_parameter_order
        ValuesBase.__init__(self, table, values, prefixes)
        self._bind = bind
        self._returning = returning
        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause)
        else:
            self._whereclause = None
        self.inline = inline
        self._validate_dialect_kwargs(dialect_kw)
        self._return_defaults = return_defaults

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return (self._whereclause,)
        else:
            return ()

    def _copy_internals(self, clone=_clone, **kw):
        # TODO: coverage
        self._whereclause = clone(self._whereclause, **kw)
        self.parameters = self.parameters.copy()

    @_generative
    def where(self, whereclause):
        """return a new update() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """
        if self._whereclause is not None:
            self._whereclause = and_(
                self._whereclause, _literal_as_text(whereclause)
            )
        else:
            self._whereclause = _literal_as_text(whereclause)

    @property
    def _extra_froms(self):
        froms = []
        seen = {self.table}

        if self._whereclause is not None:
            for item in _from_objects(self._whereclause):
                if not seen.intersection(item._cloned_set):
                    froms.append(item)
                seen.update(item._cloned_set)

        return froms


class Delete(UpdateBase):
    """Represent a DELETE construct.

    The :class:`.Delete` object is created using the :func:`delete()`
    function.

    """

    __visit_name__ = "delete"

    def __init__(
        self,
        table,
        whereclause=None,
        bind=None,
        returning=None,
        prefixes=None,
        **dialect_kw
    ):
        """Construct :class:`.Delete` object.

        Similar functionality is available via the
        :meth:`~.TableClause.delete` method on
        :class:`~.schema.Table`.

        :param table: The table to delete rows from.

        :param whereclause: A :class:`.ClauseElement` describing the ``WHERE``
          condition of the ``DELETE`` statement. Note that the
          :meth:`~Delete.where()` generative method may be used instead.

         The WHERE clause can refer to multiple tables.
         For databases which support this, a ``DELETE..USING`` or similar
         clause will be generated.  The statement
         will fail on databases that don't have support for multi-table
         delete statements.  A SQL-standard method of referring to
         additional tables in the WHERE clause is to use a correlated
         subquery::

            users.delete().where(
                    users.c.name==select([addresses.c.email_address]).\
                                where(addresses.c.user_id==users.c.id).\
                                as_scalar()
                    )

         .. versionchanged:: 1.2.0
             The WHERE clause of DELETE can refer to multiple tables.

        .. seealso::

            :ref:`deletes` - SQL Expression Tutorial

        """
        self._bind = bind
        self.table = _interpret_as_from(table)
        self._returning = returning

        if prefixes:
            self._setup_prefixes(prefixes)

        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause)
        else:
            self._whereclause = None

        self._validate_dialect_kwargs(dialect_kw)

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return (self._whereclause,)
        else:
            return ()

    @_generative
    def where(self, whereclause):
        """Add the given WHERE clause to a newly returned delete construct."""

        if self._whereclause is not None:
            self._whereclause = and_(
                self._whereclause, _literal_as_text(whereclause)
            )
        else:
            self._whereclause = _literal_as_text(whereclause)

    @property
    def _extra_froms(self):
        froms = []
        seen = {self.table}

        if self._whereclause is not None:
            for item in _from_objects(self._whereclause):
                if not seen.intersection(item._cloned_set):
                    froms.append(item)
                seen.update(item._cloned_set)

        return froms

    def _copy_internals(self, clone=_clone, **kw):
        # TODO: coverage
        self._whereclause = clone(self._whereclause, **kw)

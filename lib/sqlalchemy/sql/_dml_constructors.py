# sql/_dml_constructors.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from .dml import Delete
from .dml import Insert
from .dml import Update


def insert(table):
    """Construct an :class:`_expression.Insert` object.

    E.g.::

        from sqlalchemy import insert

        stmt = (
            insert(user_table).
            values(name='username', fullname='Full Username')
        )

    Similar functionality is available via the
    :meth:`_expression.TableClause.insert` method on
    :class:`_schema.Table`.

    .. seealso::

        :ref:`coretutorial_insert_expressions` - in the
        :ref:`1.x tutorial <sqlexpression_toplevel>`

        :ref:`tutorial_core_insert` - in the :ref:`unified_tutorial`


    :param table: :class:`_expression.TableClause`
     which is the subject of the
     insert.

    :param values: collection of values to be inserted; see
     :meth:`_expression.Insert.values`
     for a description of allowed formats here.
     Can be omitted entirely; a :class:`_expression.Insert` construct
     will also dynamically render the VALUES clause at execution time
     based on the parameters passed to :meth:`_engine.Connection.execute`.

    :param inline: if True, no attempt will be made to retrieve the
     SQL-generated default values to be provided within the statement;
     in particular,
     this allows SQL expressions to be rendered 'inline' within the
     statement without the need to pre-execute them beforehand; for
     backends that support "returning", this turns off the "implicit
     returning" feature for the statement.

    If both :paramref:`_expression.Insert.values` and compile-time bind
    parameters are present, the compile-time bind parameters override the
    information specified within :paramref:`_expression.Insert.values` on a
    per-key basis.

    The keys within :paramref:`_expression.Insert.values` can be either
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
    return Insert(table)


def update(table):
    r"""Construct an :class:`_expression.Update` object.

    E.g.::

        from sqlalchemy import update

        stmt = (
            update(user_table).
            where(user_table.c.id == 5).
            values(name='user #5')
        )

    Similar functionality is available via the
    :meth:`_expression.TableClause.update` method on
    :class:`_schema.Table`.

    .. seealso::

        :ref:`inserts_and_updates` - in the
        :ref:`1.x tutorial <sqlexpression_toplevel>`

        :ref:`tutorial_core_update_delete` - in the :ref:`unified_tutorial`



    :param table: A :class:`_schema.Table`
     object representing the database
     table to be updated.

    :param whereclause: Optional SQL expression describing the ``WHERE``
     condition of the ``UPDATE`` statement; is equivalent to using the
     more modern :meth:`~Update.where()` method to specify the ``WHERE``
     clause.

    :param values:
      Optional dictionary which specifies the ``SET`` conditions of the
      ``UPDATE``.  If left as ``None``, the ``SET``
      conditions are determined from those parameters passed to the
      statement during the execution and/or compilation of the
      statement.   When compiled standalone without any parameters,
      the ``SET`` clause generates for all columns.

      Modern applications may prefer to use the generative
      :meth:`_expression.Update.values` method to set the values of the
      UPDATE statement.

    :param inline:
      if True, SQL defaults present on :class:`_schema.Column` objects via
      the ``default`` keyword will be compiled 'inline' into the statement
      and not pre-executed.  This means that their values will not
      be available in the dictionary returned from
      :meth:`_engine.CursorResult.last_updated_params`.

    :param preserve_parameter_order: if True, the update statement is
      expected to receive parameters **only** via the
      :meth:`_expression.Update.values` method,
      and they must be passed as a Python
      ``list`` of 2-tuples. The rendered UPDATE statement will emit the SET
      clause for each referenced column maintaining this order.

      .. versionadded:: 1.0.10

      .. seealso::

        :ref:`updates_order_parameters` - illustrates the
        :meth:`_expression.Update.ordered_values` method.

    If both ``values`` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within ``values`` on a per-key basis.

    The keys within ``values`` can be either :class:`_schema.Column`
    objects or their string identifiers (specifically the "key" of the
    :class:`_schema.Column`, normally but not necessarily equivalent to
    its "name").  Normally, the
    :class:`_schema.Column` objects used here are expected to be
    part of the target :class:`_schema.Table` that is the table
    to be updated.  However when using MySQL, a multiple-table
    UPDATE statement can refer to columns from any of
    the tables referred to in the WHERE clause.

    The values referred to in ``values`` are typically:

    * a literal data value (i.e. string, number, etc.)
    * a SQL expression, such as a related :class:`_schema.Column`,
      a scalar-returning :func:`_expression.select` construct,
      etc.

    When combining :func:`_expression.select` constructs within the
    values clause of an :func:`_expression.update`
    construct, the subquery represented
    by the :func:`_expression.select` should be *correlated* to the
    parent table, that is, providing criterion which links the table inside
    the subquery to the outer table being updated::

        users.update().values(
                name=select(addresses.c.email_address).\
                        where(addresses.c.user_id==users.c.id).\
                        scalar_subquery()
            )

    .. seealso::

        :ref:`inserts_and_updates` - SQL Expression
        Language Tutorial


    """
    return Update(table)


def delete(table):
    r"""Construct :class:`_expression.Delete` object.

    E.g.::

        from sqlalchemy import delete

        stmt = (
            delete(user_table).
            where(user_table.c.id == 5)
        )

    Similar functionality is available via the
    :meth:`_expression.TableClause.delete` method on
    :class:`_schema.Table`.

    .. seealso::

        :ref:`inserts_and_updates` - in the
        :ref:`1.x tutorial <sqlexpression_toplevel>`

        :ref:`tutorial_core_update_delete` - in the :ref:`unified_tutorial`


    :param table: The table to delete rows from.

    :param whereclause: Optional SQL expression describing the ``WHERE``
     condition of the ``DELETE`` statement; is equivalent to using the
     more modern :meth:`~Delete.where()` method to specify the ``WHERE``
     clause.

    .. seealso::

        :ref:`deletes` - SQL Expression Tutorial

    """
    return Delete(table)

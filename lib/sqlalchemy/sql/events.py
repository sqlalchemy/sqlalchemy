# sqlalchemy/sql/events.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base import SchemaEventTarget
from .. import event


class DDLEvents(event.Events):
    """
    Define event listeners for schema objects,
    that is, :class:`.SchemaItem` and other :class:`.SchemaEventTarget`
    subclasses, including :class:`_schema.MetaData`, :class:`_schema.Table`,
    :class:`_schema.Column`.

    :class:`_schema.MetaData` and :class:`_schema.Table` support events
    specifically regarding when CREATE and DROP
    DDL is emitted to the database.

    Attachment events are also provided to customize
    behavior whenever a child schema element is associated
    with a parent, such as, when a :class:`_schema.Column` is associated
    with its :class:`_schema.Table`, when a
    :class:`_schema.ForeignKeyConstraint`
    is associated with a :class:`_schema.Table`, etc.

    Example using the ``after_create`` event::

        from sqlalchemy import event
        from sqlalchemy import Table, Column, Metadata, Integer

        m = MetaData()
        some_table = Table('some_table', m, Column('data', Integer))

        def after_create(target, connection, **kw):
            connection.execute(text(
                "ALTER TABLE %s SET name=foo_%s" % (target.name, target.name)
            ))

        event.listen(some_table, "after_create", after_create)

    DDL events integrate closely with the
    :class:`.DDL` class and the :class:`.DDLElement` hierarchy
    of DDL clause constructs, which are themselves appropriate
    as listener callables::

        from sqlalchemy import DDL
        event.listen(
            some_table,
            "after_create",
            DDL("ALTER TABLE %(table)s SET name=foo_%(table)s")
        )

    The methods here define the name of an event as well
    as the names of members that are passed to listener
    functions.

    For all :class:`.DDLEvent` events, the ``propagate=True`` keyword argument
    will ensure that a given event handler is propagated to copies of the
    object, which are made when using the :meth:`_schema.Table.to_metadata`
    method::

        from sqlalchemy import DDL
        event.listen(
            some_table,
            "after_create",
            DDL("ALTER TABLE %(table)s SET name=foo_%(table)s"),
            propagate=True
        )

        new_table = some_table.to_metadata(new_metadata)

    The above :class:`.DDL` object will also be associated with the
    :class:`_schema.Table` object represented by ``new_table``.

    .. seealso::

        :ref:`event_toplevel`

        :class:`.DDLElement`

        :class:`.DDL`

        :ref:`schema_ddl_sequences`

    """

    _target_class_doc = "SomeSchemaClassOrObject"
    _dispatch_target = SchemaEventTarget

    def before_create(self, target, connection, **kw):
        r"""Called before CREATE statements are emitted.

        :param target: the :class:`_schema.MetaData` or :class:`_schema.Table`
         object which is the target of the event.
        :param connection: the :class:`_engine.Connection` where the
         CREATE statement or statements will be emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other
         elements used by internal events.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def after_create(self, target, connection, **kw):
        r"""Called after CREATE statements are emitted.

        :param target: the :class:`_schema.MetaData` or :class:`_schema.Table`
         object which is the target of the event.
        :param connection: the :class:`_engine.Connection` where the
         CREATE statement or statements have been emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other
         elements used by internal events.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def before_drop(self, target, connection, **kw):
        r"""Called before DROP statements are emitted.

        :param target: the :class:`_schema.MetaData` or :class:`_schema.Table`
         object which is the target of the event.
        :param connection: the :class:`_engine.Connection` where the
         DROP statement or statements will be emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other
         elements used by internal events.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def after_drop(self, target, connection, **kw):
        r"""Called after DROP statements are emitted.

        :param target: the :class:`_schema.MetaData` or :class:`_schema.Table`
         object which is the target of the event.
        :param connection: the :class:`_engine.Connection` where the
         DROP statement or statements have been emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other
         elements used by internal events.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def before_parent_attach(self, target, parent):
        """Called before a :class:`.SchemaItem` is associated with
        a parent :class:`.SchemaItem`.

        :param target: the target object
        :param parent: the parent to which the target is being attached.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def after_parent_attach(self, target, parent):
        """Called after a :class:`.SchemaItem` is associated with
        a parent :class:`.SchemaItem`.

        :param target: the target object
        :param parent: the parent to which the target is being attached.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

    def column_reflect(self, inspector, table, column_info):
        """Called for each unit of 'column info' retrieved when
        a :class:`_schema.Table` is being reflected.

        The dictionary of column information as returned by the
        dialect is passed, and can be modified.  The dictionary
        is that returned in each element of the list returned
        by :meth:`.reflection.Inspector.get_columns`:

            * ``name`` - the column's name

            * ``type`` - the type of this column, which should be an instance
              of :class:`~sqlalchemy.types.TypeEngine`

            * ``nullable`` - boolean flag if the column is NULL or NOT NULL

            * ``default`` - the column's server default value.  This is
              normally specified as a plain string SQL expression, however the
              event can pass a :class:`.FetchedValue`, :class:`.DefaultClause`,
              or :func:`_expression.text` object as well.

              .. versionchanged:: 1.1.6

                    The :meth:`.DDLEvents.column_reflect` event allows a non
                    string :class:`.FetchedValue`,
                    :func:`_expression.text`, or derived object to be
                    specified as the value of ``default`` in the column
                    dictionary.

            * ``attrs``  - dict containing optional column attributes

        The event is called before any action is taken against
        this dictionary, and the contents can be modified.
        The :class:`_schema.Column` specific arguments ``info``, ``key``,
        and ``quote`` can also be added to the dictionary and
        will be passed to the constructor of :class:`_schema.Column`.

        Note that this event is only meaningful if either
        associated with the :class:`_schema.Table` class across the
        board, e.g.::

            from sqlalchemy.schema import Table
            from sqlalchemy import event

            def listen_for_reflect(inspector, table, column_info):
                "receive a column_reflect event"
                # ...

            event.listen(
                    Table,
                    'column_reflect',
                    listen_for_reflect)

        ...or with a specific :class:`_schema.Table` instance using
        the ``listeners`` argument::

            def listen_for_reflect(inspector, table, column_info):
                "receive a column_reflect event"
                # ...

            t = Table(
                'sometable',
                autoload=True,
                listeners=[
                    ('column_reflect', listen_for_reflect)
                ])

        This because the reflection process initiated by ``autoload=True``
        completes within the scope of the constructor for
        :class:`_schema.Table`.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """

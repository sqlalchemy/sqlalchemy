# sqlalchemy/events.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Core event interfaces."""

from . import event
from . import exc
from . import util
from .engine import Connectable
from .engine import Dialect
from .engine import Engine
from .pool import Pool
from .sql.base import SchemaEventTarget


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
            connection.execute("ALTER TABLE %s SET name=foo_%s" %
                                    (target.name, target.name))

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
    object, which are made when using the :meth:`_schema.Table.tometadata`
    method::

        from sqlalchemy import DDL
        event.listen(
            some_table,
            "after_create",
            DDL("ALTER TABLE %(table)s SET name=foo_%(table)s"),
            propagate=True
        )

        new_table = some_table.tometadata(new_metadata)

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
        :meth:`_schema.Table.tometadata` is used.

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
        :meth:`_schema.Table.tometadata` is used.

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
        :meth:`_schema.Table.tometadata` is used.

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
        :meth:`_schema.Table.tometadata` is used.

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
        :meth:`_schema.Table.tometadata` is used.

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
        :meth:`_schema.Table.tometadata` is used.

        """

    def column_reflect(self, inspector, table, column_info):
        """Called for each unit of 'column info' retrieved when
        a :class:`_schema.Table` is being reflected.

        Currently, this event may only be applied to the :class:`_schema.Table`
        class directly::

            from sqlalchemy import Table

            @event.listens_for(Table, 'column_reflect')
            def receive_column_reflect(inspector, table, column_info):
                # receives for all Table objects that are reflected

        Or applied using the
        :paramref:`_schema.Table.listeners` parameter::

            t1 = Table(
                "my_table",
                autoload_with=some_engine,
                listeners=[
                    ('column_reflect', receive_column_reflect)
                ]
            )

        A future release will allow it to be associated with a specific
        :class:`_schema.MetaData` object as well.

        The dictionary of column information as returned by the
        dialect is passed, and can be modified.  The dictionary
        is that returned in each element of the list returned
        by :meth:`.reflection.Inspector.get_columns`:

            * ``name`` - the column's name, is applied to the
              :paramref:`_schema.Column.name` parameter

            * ``type`` - the type of this column, which should be an instance
              of :class:`~sqlalchemy.types.TypeEngine`, is applied to the
              :paramref:`_schema.Column.type` parameter

            * ``nullable`` - boolean flag if the column is NULL or NOT NULL,
              is applied to the :paramref:`_schema.Column.nullable` parameter

            * ``default`` - the column's server default value.  This is
              normally specified as a plain string SQL expression, however the
              event can pass a :class:`.FetchedValue`, :class:`.DefaultClause`,
              or :func:`_expression.text` object as well.  Is applied to the
              :paramref:`_schema.Column.server_default` parameter

              .. versionchanged:: 1.1.6

                    The :meth:`.DDLEvents.column_reflect` event allows a non
                    string :class:`.FetchedValue`,
                    :func:`_expression.text`, or derived object to be
                    specified as the value of ``default`` in the column
                    dictionary.

        The event is called before any action is taken against
        this dictionary, and the contents can be modified; the following
        additional keys may be added to the dictionary to further modify
        how the :class:`_schema.Column` is constructed:


            * ``key`` - the string key that will be used to access this
              :class:`_schema.Column` in the ``.c`` collection; will be applied
              to the :paramref:`_schema.Column.key` parameter. Is also used
              for ORM mapping.  See the section
              :ref:`mapper_automated_reflection_schemes` for an example.

            * ``quote`` - force or un-force quoting on the column name;
              is applied to the :paramref:`_schema.Column.quote` parameter.

            * ``info`` - a dictionary of arbitrary data to follow along with
              the :class:`_schema.Column`, is applied to the
              :paramref:`_schema.Column.info` parameter.

        :func:`.event.listen` also accepts the ``propagate=True``
        modifier for this event; when True, the listener function will
        be established for any copies made of the target object,
        i.e. those copies that are generated when
        :meth:`_schema.Table.to_metadata` is used.

        """


class PoolEvents(event.Events):
    """Available events for :class:`_pool.Pool`.

    The methods here define the name of an event as well
    as the names of members that are passed to listener
    functions.

    e.g.::

        from sqlalchemy import event

        def my_on_checkout(dbapi_conn, connection_rec, connection_proxy):
            "handle an on checkout event"

        event.listen(Pool, 'checkout', my_on_checkout)

    In addition to accepting the :class:`_pool.Pool` class and
    :class:`_pool.Pool` instances, :class:`_events.PoolEvents` also accepts
    :class:`_engine.Engine` objects and the :class:`_engine.Engine` class as
    targets, which will be resolved to the ``.pool`` attribute of the
    given engine or the :class:`_pool.Pool` class::

        engine = create_engine("postgresql://scott:tiger@localhost/test")

        # will associate with engine.pool
        event.listen(engine, 'checkout', my_on_checkout)

    """

    _target_class_doc = "SomeEngineOrPool"
    _dispatch_target = Pool

    @classmethod
    def _accept_with(cls, target):
        if isinstance(target, type):
            if issubclass(target, Engine):
                return Pool
            elif issubclass(target, Pool):
                return target
        elif isinstance(target, Engine):
            return target.pool
        else:
            return target

    def connect(self, dbapi_connection, connection_record):
        """Called at the moment a particular DBAPI connection is first
        created for a given :class:`_pool.Pool`.

        This event allows one to capture the point directly after which
        the DBAPI module-level ``.connect()`` method has been used in order
        to produce a new DBAPI connection.

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        """

    def first_connect(self, dbapi_connection, connection_record):
        """Called exactly once for the first time a DBAPI connection is
        checked out from a particular :class:`_pool.Pool`.

        The rationale for :meth:`_events.PoolEvents.first_connect`
        is to determine
        information about a particular series of database connections based
        on the settings used for all connections.  Since a particular
        :class:`_pool.Pool`
        refers to a single "creator" function (which in terms
        of a :class:`_engine.Engine`
        refers to the URL and connection options used),
        it is typically valid to make observations about a single connection
        that can be safely assumed to be valid about all subsequent
        connections, such as the database version, the server and client
        encoding settings, collation settings, and many others.

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        """

    def checkout(self, dbapi_connection, connection_record, connection_proxy):
        """Called when a connection is retrieved from the Pool.

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        :param connection_proxy: the :class:`._ConnectionFairy` object which
          will proxy the public interface of the DBAPI connection for the
          lifespan of the checkout.

        If you raise a :class:`~sqlalchemy.exc.DisconnectionError`, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.

        .. seealso:: :meth:`_events.ConnectionEvents.engine_connect`
           - a similar event
           which occurs upon creation of a new :class:`_engine.Connection`.

        """

    def checkin(self, dbapi_connection, connection_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        """

    def reset(self, dbapi_connection, connection_record):
        """Called before the "reset" action occurs for a pooled connection.

        This event represents
        when the ``rollback()`` method is called on the DBAPI connection
        before it is returned to the pool.  The behavior of "reset" can
        be controlled, including disabled, using the ``reset_on_return``
        pool argument.


        The :meth:`_events.PoolEvents.reset` event is usually followed by the
        :meth:`_events.PoolEvents.checkin` event is called, except in those
        cases where the connection is discarded immediately after reset.

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        .. seealso::

            :meth:`_events.ConnectionEvents.rollback`

            :meth:`_events.ConnectionEvents.commit`

        """

    def invalidate(self, dbapi_connection, connection_record, exception):
        """Called when a DBAPI connection is to be "invalidated".

        This event is called any time the :meth:`._ConnectionRecord.invalidate`
        method is invoked, either from API usage or via "auto-invalidation",
        without the ``soft`` flag.

        The event occurs before a final attempt to call ``.close()`` on the
        connection occurs.

        :param dbapi_connection: a DBAPI connection.

        :param connection_record: the :class:`._ConnectionRecord` managing the
         DBAPI connection.

        :param exception: the exception object corresponding to the reason
         for this invalidation, if any.  May be ``None``.

        .. versionadded:: 0.9.2 Added support for connection invalidation
           listening.

        .. seealso::

            :ref:`pool_connection_invalidation`

        """

    def soft_invalidate(self, dbapi_connection, connection_record, exception):
        """Called when a DBAPI connection is to be "soft invalidated".

        This event is called any time the :meth:`._ConnectionRecord.invalidate`
        method is invoked with the ``soft`` flag.

        Soft invalidation refers to when the connection record that tracks
        this connection will force a reconnect after the current connection
        is checked in.   It does not actively close the dbapi_connection
        at the point at which it is called.

        .. versionadded:: 1.0.3

        """

    def close(self, dbapi_connection, connection_record):
        """Called when a DBAPI connection is closed.

        The event is emitted before the close occurs.

        The close of a connection can fail; typically this is because
        the connection is already closed.  If the close operation fails,
        the connection is discarded.

        The :meth:`.close` event corresponds to a connection that's still
        associated with the pool. To intercept close events for detached
        connections use :meth:`.close_detached`.

        .. versionadded:: 1.1

        """

    def detach(self, dbapi_connection, connection_record):
        """Called when a DBAPI connection is "detached" from a pool.

        This event is emitted after the detach occurs.  The connection
        is no longer associated with the given connection record.

        .. versionadded:: 1.1

        """

    def close_detached(self, dbapi_connection):
        """Called when a detached DBAPI connection is closed.

        The event is emitted before the close occurs.

        The close of a connection can fail; typically this is because
        the connection is already closed.  If the close operation fails,
        the connection is discarded.

        .. versionadded:: 1.1

        """


class ConnectionEvents(event.Events):
    """Available events for :class:`.Connectable`, which includes
    :class:`_engine.Connection` and :class:`_engine.Engine`.

    The methods here define the name of an event as well as the names of
    members that are passed to listener functions.

    An event listener can be associated with any :class:`.Connectable`
    class or instance, such as an :class:`_engine.Engine`, e.g.::

        from sqlalchemy import event, create_engine

        def before_cursor_execute(conn, cursor, statement, parameters, context,
                                                        executemany):
            log.info("Received statement: %s", statement)

        engine = create_engine('postgresql://scott:tiger@localhost/test')
        event.listen(engine, "before_cursor_execute", before_cursor_execute)

    or with a specific :class:`_engine.Connection`::

        with engine.begin() as conn:
            @event.listens_for(conn, 'before_cursor_execute')
            def before_cursor_execute(conn, cursor, statement, parameters,
                                            context, executemany):
                log.info("Received statement: %s", statement)

    When the methods are called with a `statement` parameter, such as in
    :meth:`.after_cursor_execute`, :meth:`.before_cursor_execute` and
    :meth:`.dbapi_error`, the statement is the exact SQL string that was
    prepared for transmission to the DBAPI ``cursor`` in the connection's
    :class:`.Dialect`.

    The :meth:`.before_execute` and :meth:`.before_cursor_execute`
    events can also be established with the ``retval=True`` flag, which
    allows modification of the statement and parameters to be sent
    to the database.  The :meth:`.before_cursor_execute` event is
    particularly useful here to add ad-hoc string transformations, such
    as comments, to all executions::

        from sqlalchemy.engine import Engine
        from sqlalchemy import event

        @event.listens_for(Engine, "before_cursor_execute", retval=True)
        def comment_sql_calls(conn, cursor, statement, parameters,
                                            context, executemany):
            statement = statement + " -- some comment"
            return statement, parameters

    .. note:: :class:`_events.ConnectionEvents` can be established on any
       combination of :class:`_engine.Engine`, :class:`_engine.Connection`,
       as well
       as instances of each of those classes.  Events across all
       four scopes will fire off for a given instance of
       :class:`_engine.Connection`.  However, for performance reasons, the
       :class:`_engine.Connection` object determines at instantiation time
       whether or not its parent :class:`_engine.Engine` has event listeners
       established.   Event listeners added to the :class:`_engine.Engine`
       class or to an instance of :class:`_engine.Engine`
       *after* the instantiation
       of a dependent :class:`_engine.Connection` instance will usually
       *not* be available on that :class:`_engine.Connection` instance.
       The newly
       added listeners will instead take effect for
       :class:`_engine.Connection`
       instances created subsequent to those event listeners being
       established on the parent :class:`_engine.Engine` class or instance.

    :param retval=False: Applies to the :meth:`.before_execute` and
      :meth:`.before_cursor_execute` events only.  When True, the
      user-defined event function must have a return value, which
      is a tuple of parameters that replace the given statement
      and parameters.  See those methods for a description of
      specific return arguments.

    """

    _target_class_doc = "SomeEngine"
    _dispatch_target = Connectable

    @classmethod
    def _listen(cls, event_key, retval=False):
        target, identifier, fn = (
            event_key.dispatch_target,
            event_key.identifier,
            event_key._listen_fn,
        )

        target._has_events = True

        if not retval:
            if identifier == "before_execute":
                orig_fn = fn

                def wrap_before_execute(
                    conn, clauseelement, multiparams, params
                ):
                    orig_fn(conn, clauseelement, multiparams, params)
                    return clauseelement, multiparams, params

                fn = wrap_before_execute
            elif identifier == "before_cursor_execute":
                orig_fn = fn

                def wrap_before_cursor_execute(
                    conn, cursor, statement, parameters, context, executemany
                ):
                    orig_fn(
                        conn,
                        cursor,
                        statement,
                        parameters,
                        context,
                        executemany,
                    )
                    return statement, parameters

                fn = wrap_before_cursor_execute
        elif retval and identifier not in (
            "before_execute",
            "before_cursor_execute",
            "handle_error",
        ):
            raise exc.ArgumentError(
                "Only the 'before_execute', "
                "'before_cursor_execute' and 'handle_error' engine "
                "event listeners accept the 'retval=True' "
                "argument."
            )
        event_key.with_wrapper(fn).base_listen()

    def before_execute(self, conn, clauseelement, multiparams, params):
        """Intercept high level execute() events, receiving uncompiled
        SQL constructs and other objects prior to rendering into SQL.

        This event is good for debugging SQL compilation issues as well
        as early manipulation of the parameters being sent to the database,
        as the parameter lists will be in a consistent format here.

        This event can be optionally established with the ``retval=True``
        flag.  The ``clauseelement``, ``multiparams``, and ``params``
        arguments should be returned as a three-tuple in this case::

            @event.listens_for(Engine, "before_execute", retval=True)
            def before_execute(conn, clauseelement, multiparams, params):
                # do something with clauseelement, multiparams, params
                return clauseelement, multiparams, params

        :param conn: :class:`_engine.Connection` object
        :param clauseelement: SQL expression construct, :class:`.Compiled`
         instance, or string statement passed to
         :meth:`_engine.Connection.execute`.
        :param multiparams: Multiple parameter sets, a list of dictionaries.
        :param params: Single parameter set, a single dictionary.

        .. seealso::

            :meth:`.before_cursor_execute`

        """

    def after_execute(self, conn, clauseelement, multiparams, params, result):
        """Intercept high level execute() events after execute.


        :param conn: :class:`_engine.Connection` object
        :param clauseelement: SQL expression construct, :class:`.Compiled`
         instance, or string statement passed to
         :meth:`_engine.Connection.execute`.
        :param multiparams: Multiple parameter sets, a list of dictionaries.
        :param params: Single parameter set, a single dictionary.
        :param result: :class:`_engine.ResultProxy` generated by the execution
                      .

        """

    def before_cursor_execute(
        self, conn, cursor, statement, parameters, context, executemany
    ):
        """Intercept low-level cursor execute() events before execution,
        receiving the string SQL statement and DBAPI-specific parameter list to
        be invoked against a cursor.

        This event is a good choice for logging as well as late modifications
        to the SQL string.  It's less ideal for parameter modifications except
        for those which are specific to a target backend.

        This event can be optionally established with the ``retval=True``
        flag.  The ``statement`` and ``parameters`` arguments should be
        returned as a two-tuple in this case::

            @event.listens_for(Engine, "before_cursor_execute", retval=True)
            def before_cursor_execute(conn, cursor, statement,
                            parameters, context, executemany):
                # do something with statement, parameters
                return statement, parameters

        See the example at :class:`_events.ConnectionEvents`.

        :param conn: :class:`_engine.Connection` object
        :param cursor: DBAPI cursor object
        :param statement: string SQL statement, as to be passed to the DBAPI
        :param parameters: Dictionary, tuple, or list of parameters being
         passed to the ``execute()`` or ``executemany()`` method of the
         DBAPI ``cursor``.  In some cases may be ``None``.
        :param context: :class:`.ExecutionContext` object in use.  May
         be ``None``.
        :param executemany: boolean, if ``True``, this is an ``executemany()``
         call, if ``False``, this is an ``execute()`` call.

        .. seealso::

            :meth:`.before_execute`

            :meth:`.after_cursor_execute`

        """

    def after_cursor_execute(
        self, conn, cursor, statement, parameters, context, executemany
    ):
        """Intercept low-level cursor execute() events after execution.

        :param conn: :class:`_engine.Connection` object
        :param cursor: DBAPI cursor object.  Will have results pending
         if the statement was a SELECT, but these should not be consumed
         as they will be needed by the :class:`_engine.ResultProxy`.
        :param statement: string SQL statement, as passed to the DBAPI
        :param parameters: Dictionary, tuple, or list of parameters being
         passed to the ``execute()`` or ``executemany()`` method of the
         DBAPI ``cursor``.  In some cases may be ``None``.
        :param context: :class:`.ExecutionContext` object in use.  May
         be ``None``.
        :param executemany: boolean, if ``True``, this is an ``executemany()``
         call, if ``False``, this is an ``execute()`` call.

        """

    @util.deprecated(
        "0.9",
        "The :meth:`_events.ConnectionEvents.dbapi_error` "
        "event is deprecated and will be removed in a future release. "
        "Please refer to the :meth:`_events.ConnectionEvents.handle_error` "
        "event.",
    )
    def dbapi_error(
        self, conn, cursor, statement, parameters, context, exception
    ):
        """Intercept a raw DBAPI error.

        This event is called with the DBAPI exception instance
        received from the DBAPI itself, *before* SQLAlchemy wraps the
        exception with it's own exception wrappers, and before any
        other operations are performed on the DBAPI cursor; the
        existing transaction remains in effect as well as any state
        on the cursor.

        The use case here is to inject low-level exception handling
        into an :class:`_engine.Engine`, typically for logging and
        debugging purposes.

        .. warning::

            Code should **not** modify
            any state or throw any exceptions here as this will
            interfere with SQLAlchemy's cleanup and error handling
            routines.  For exception modification, please refer to the
            new :meth:`_events.ConnectionEvents.handle_error` event.

        Subsequent to this hook, SQLAlchemy may attempt any
        number of operations on the connection/cursor, including
        closing the cursor, rolling back of the transaction in the
        case of connectionless execution, and disposing of the entire
        connection pool if a "disconnect" was detected.   The
        exception is then wrapped in a SQLAlchemy DBAPI exception
        wrapper and re-thrown.

        :param conn: :class:`_engine.Connection` object
        :param cursor: DBAPI cursor object
        :param statement: string SQL statement, as passed to the DBAPI
        :param parameters: Dictionary, tuple, or list of parameters being
         passed to the ``execute()`` or ``executemany()`` method of the
         DBAPI ``cursor``.  In some cases may be ``None``.
        :param context: :class:`.ExecutionContext` object in use.  May
         be ``None``.
        :param exception: The **unwrapped** exception emitted directly from the
         DBAPI.  The class here is specific to the DBAPI module in use.

        """

    def handle_error(self, exception_context):
        r"""Intercept all exceptions processed by the
        :class:`_engine.Connection`.

        This includes all exceptions emitted by the DBAPI as well as
        within SQLAlchemy's statement invocation process, including
        encoding errors and other statement validation errors.  Other areas
        in which the event is invoked include transaction begin and end,
        result row fetching, cursor creation.

        Note that :meth:`.handle_error` may support new kinds of exceptions
        and new calling scenarios at *any time*.  Code which uses this
        event must expect new calling patterns to be present in minor
        releases.

        To support the wide variety of members that correspond to an exception,
        as well as to allow extensibility of the event without backwards
        incompatibility, the sole argument received is an instance of
        :class:`.ExceptionContext`.   This object contains data members
        representing detail about the exception.

        Use cases supported by this hook include:

        * read-only, low-level exception handling for logging and
          debugging purposes
        * exception re-writing
        * Establishing or disabling whether a connection or the owning
          connection pool is invalidated or expired in response to a
          specific exception. [1]_.

        The hook is called while the cursor from the failed operation
        (if any) is still open and accessible.   Special cleanup operations
        can be called on this cursor; SQLAlchemy will attempt to close
        this cursor subsequent to this hook being invoked.  If the connection
        is in "autocommit" mode, the transaction also remains open within
        the scope of this hook; the rollback of the per-statement transaction
        also occurs after the hook is called.

        .. note::

            .. [1] The pool "pre_ping" handler enabled using the
                :paramref:`_sa.create_engine.pool_pre_ping` parameter does
                **not** consult this event before deciding if the "ping"
                returned false, as opposed to receiving an unhandled error.
                For this use case, the :ref:`legacy recipe based on
                engine_connect() may be used
                <pool_disconnects_pessimistic_custom>`.  A future API allow
                more comprehensive customization of the "disconnect"
                detection mechanism across all functions.

        A handler function has two options for replacing
        the SQLAlchemy-constructed exception into one that is user
        defined.   It can either raise this new exception directly, in
        which case all further event listeners are bypassed and the
        exception will be raised, after appropriate cleanup as taken
        place::

            @event.listens_for(Engine, "handle_error")
            def handle_exception(context):
                if isinstance(context.original_exception,
                    psycopg2.OperationalError) and \
                    "failed" in str(context.original_exception):
                    raise MySpecialException("failed operation")

        .. warning::  Because the
           :meth:`_events.ConnectionEvents.handle_error`
           event specifically provides for exceptions to be re-thrown as
           the ultimate exception raised by the failed statement,
           **stack traces will be misleading** if the user-defined event
           handler itself fails and throws an unexpected exception;
           the stack trace may not illustrate the actual code line that
           failed!  It is advised to code carefully here and use
           logging and/or inline debugging if unexpected exceptions are
           occurring.

        Alternatively, a "chained" style of event handling can be
        used, by configuring the handler with the ``retval=True``
        modifier and returning the new exception instance from the
        function.  In this case, event handling will continue onto the
        next handler.   The "chained" exception is available using
        :attr:`.ExceptionContext.chained_exception`::

            @event.listens_for(Engine, "handle_error", retval=True)
            def handle_exception(context):
                if context.chained_exception is not None and \
                    "special" in context.chained_exception.message:
                    return MySpecialException("failed",
                        cause=context.chained_exception)

        Handlers that return ``None`` may be used within the chain; when
        a handler returns ``None``, the previous exception instance,
        if any, is maintained as the current exception that is passed onto the
        next handler.

        When a custom exception is raised or returned, SQLAlchemy raises
        this new exception as-is, it is not wrapped by any SQLAlchemy
        object.  If the exception is not a subclass of
        :class:`sqlalchemy.exc.StatementError`,
        certain features may not be available; currently this includes
        the ORM's feature of adding a detail hint about "autoflush" to
        exceptions raised within the autoflush process.

        :param context: an :class:`.ExceptionContext` object.  See this
         class for details on all available members.

        .. versionadded:: 0.9.7 Added the
            :meth:`_events.ConnectionEvents.handle_error` hook.

        .. versionchanged:: 1.1 The :meth:`.handle_error` event will now
           receive all exceptions that inherit from ``BaseException``,
           including ``SystemExit`` and ``KeyboardInterrupt``.  The setting for
           :attr:`.ExceptionContext.is_disconnect` is ``True`` in this case and
           the default for
           :attr:`.ExceptionContext.invalidate_pool_on_disconnect` is
           ``False``.

        .. versionchanged:: 1.0.0 The :meth:`.handle_error` event is now
           invoked when an :class:`_engine.Engine` fails during the initial
           call to :meth:`_engine.Engine.connect`, as well as when a
           :class:`_engine.Connection` object encounters an error during a
           reconnect operation.

        .. versionchanged:: 1.0.0 The :meth:`.handle_error` event is
           not fired off when a dialect makes use of the
           ``skip_user_error_events`` execution option.   This is used
           by dialects which intend to catch SQLAlchemy-specific exceptions
           within specific operations, such as when the MySQL dialect detects
           a table not present within the ``has_table()`` dialect method.
           Prior to 1.0.0, code which implements :meth:`.handle_error` needs
           to ensure that exceptions thrown in these scenarios are re-raised
           without modification.

        """

    def engine_connect(self, conn, branch):
        """Intercept the creation of a new :class:`_engine.Connection`.

        This event is called typically as the direct result of calling
        the :meth:`_engine.Engine.connect` method.

        It differs from the :meth:`_events.PoolEvents.connect` method, which
        refers to the actual connection to a database at the DBAPI level;
        a DBAPI connection may be pooled and reused for many operations.
        In contrast, this event refers only to the production of a higher level
        :class:`_engine.Connection` wrapper around such a DBAPI connection.

        It also differs from the :meth:`_events.PoolEvents.checkout` event
        in that it is specific to the :class:`_engine.Connection` object,
        not the
        DBAPI connection that :meth:`_events.PoolEvents.checkout` deals with,
        although
        this DBAPI connection is available here via the
        :attr:`_engine.Connection.connection` attribute.
        But note there can in fact
        be multiple :meth:`_events.PoolEvents.checkout`
        events within the lifespan
        of a single :class:`_engine.Connection` object, if that
        :class:`_engine.Connection`
        is invalidated and re-established.  There can also be multiple
        :class:`_engine.Connection`
        objects generated for the same already-checked-out
        DBAPI connection, in the case that a "branch" of a
        :class:`_engine.Connection`
        is produced.

        :param conn: :class:`_engine.Connection` object.
        :param branch: if True, this is a "branch" of an existing
         :class:`_engine.Connection`.  A branch is generated within the course
         of a statement execution to invoke supplemental statements, most
         typically to pre-execute a SELECT of a default value for the purposes
         of an INSERT statement.

        .. versionadded:: 0.9.0

        .. seealso::

            :ref:`pool_disconnects_pessimistic` - illustrates how to use
            :meth:`_events.ConnectionEvents.engine_connect`
            to transparently ensure pooled connections are connected to the
            database.

            :meth:`_events.PoolEvents.checkout`
            the lower-level pool checkout event
            for an individual DBAPI connection

            :meth:`_events.ConnectionEvents.set_connection_execution_options`
            - a copy
            of a :class:`_engine.Connection` is also made when the
            :meth:`_engine.Connection.execution_options` method is called.

        """

    def set_connection_execution_options(self, conn, opts):
        """Intercept when the :meth:`_engine.Connection.execution_options`
        method is called.

        This method is called after the new :class:`_engine.Connection`
        has been
        produced, with the newly updated execution options collection, but
        before the :class:`.Dialect` has acted upon any of those new options.

        Note that this method is not called when a new
        :class:`_engine.Connection`
        is produced which is inheriting execution options from its parent
        :class:`_engine.Engine`; to intercept this condition, use the
        :meth:`_events.ConnectionEvents.engine_connect` event.

        :param conn: The newly copied :class:`_engine.Connection` object

        :param opts: dictionary of options that were passed to the
         :meth:`_engine.Connection.execution_options` method.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`_events.ConnectionEvents.set_engine_execution_options`
            - event
            which is called when :meth:`_engine.Engine.execution_options`
            is called.


        """

    def set_engine_execution_options(self, engine, opts):
        """Intercept when the :meth:`_engine.Engine.execution_options`
        method is called.

        The :meth:`_engine.Engine.execution_options` method produces a shallow
        copy of the :class:`_engine.Engine` which stores the new options.
        That new
        :class:`_engine.Engine` is passed here.
        A particular application of this
        method is to add a :meth:`_events.ConnectionEvents.engine_connect`
        event
        handler to the given :class:`_engine.Engine`
        which will perform some per-
        :class:`_engine.Connection` task specific to these execution options.

        :param conn: The newly copied :class:`_engine.Engine` object

        :param opts: dictionary of options that were passed to the
         :meth:`_engine.Connection.execution_options` method.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`_events.ConnectionEvents.set_connection_execution_options`
            - event
            which is called when :meth:`_engine.Connection.execution_options`
            is
            called.

        """

    def engine_disposed(self, engine):
        """Intercept when the :meth:`_engine.Engine.dispose` method is called.

        The :meth:`_engine.Engine.dispose` method instructs the engine to
        "dispose" of it's connection pool (e.g. :class:`_pool.Pool`), and
        replaces it with a new one.  Disposing of the old pool has the
        effect that existing checked-in connections are closed.  The new
        pool does not establish any new connections until it is first used.

        This event can be used to indicate that resources related to the
        :class:`_engine.Engine` should also be cleaned up,
        keeping in mind that the
        :class:`_engine.Engine`
        can still be used for new requests in which case
        it re-acquires connection resources.

        .. versionadded:: 1.0.5

        """

    def begin(self, conn):
        """Intercept begin() events.

        :param conn: :class:`_engine.Connection` object

        """

    def rollback(self, conn):
        """Intercept rollback() events, as initiated by a
        :class:`.Transaction`.

        Note that the :class:`_pool.Pool` also "auto-rolls back"
        a DBAPI connection upon checkin, if the ``reset_on_return``
        flag is set to its default value of ``'rollback'``.
        To intercept this
        rollback, use the :meth:`_events.PoolEvents.reset` hook.

        :param conn: :class:`_engine.Connection` object

        .. seealso::

            :meth:`_events.PoolEvents.reset`

        """

    def commit(self, conn):
        """Intercept commit() events, as initiated by a
        :class:`.Transaction`.

        Note that the :class:`_pool.Pool` may also "auto-commit"
        a DBAPI connection upon checkin, if the ``reset_on_return``
        flag is set to the value ``'commit'``.  To intercept this
        commit, use the :meth:`_events.PoolEvents.reset` hook.

        :param conn: :class:`_engine.Connection` object
        """

    def savepoint(self, conn, name):
        """Intercept savepoint() events.

        :param conn: :class:`_engine.Connection` object
        :param name: specified name used for the savepoint.

        """

    def rollback_savepoint(self, conn, name, context):
        """Intercept rollback_savepoint() events.

        :param conn: :class:`_engine.Connection` object
        :param name: specified name used for the savepoint.
        :param context: :class:`.ExecutionContext` in use.  May be ``None``.

        """

    def release_savepoint(self, conn, name, context):
        """Intercept release_savepoint() events.

        :param conn: :class:`_engine.Connection` object
        :param name: specified name used for the savepoint.
        :param context: :class:`.ExecutionContext` in use.  May be ``None``.

        """

    def begin_twophase(self, conn, xid):
        """Intercept begin_twophase() events.

        :param conn: :class:`_engine.Connection` object
        :param xid: two-phase XID identifier

        """

    def prepare_twophase(self, conn, xid):
        """Intercept prepare_twophase() events.

        :param conn: :class:`_engine.Connection` object
        :param xid: two-phase XID identifier
        """

    def rollback_twophase(self, conn, xid, is_prepared):
        """Intercept rollback_twophase() events.

        :param conn: :class:`_engine.Connection` object
        :param xid: two-phase XID identifier
        :param is_prepared: boolean, indicates if
         :meth:`.TwoPhaseTransaction.prepare` was called.

        """

    def commit_twophase(self, conn, xid, is_prepared):
        """Intercept commit_twophase() events.

        :param conn: :class:`_engine.Connection` object
        :param xid: two-phase XID identifier
        :param is_prepared: boolean, indicates if
         :meth:`.TwoPhaseTransaction.prepare` was called.

        """


class DialectEvents(event.Events):
    """event interface for execution-replacement functions.

    These events allow direct instrumentation and replacement
    of key dialect functions which interact with the DBAPI.

    .. note::

        :class:`.DialectEvents` hooks should be considered **semi-public**
        and experimental.
        These hooks are not for general use and are only for those situations
        where intricate re-statement of DBAPI mechanics must be injected onto
        an existing dialect.  For general-use statement-interception events,
        please use the :class:`_events.ConnectionEvents` interface.

    .. seealso::

        :meth:`_events.ConnectionEvents.before_cursor_execute`

        :meth:`_events.ConnectionEvents.before_execute`

        :meth:`_events.ConnectionEvents.after_cursor_execute`

        :meth:`_events.ConnectionEvents.after_execute`


    .. versionadded:: 0.9.4

    """

    _target_class_doc = "SomeEngine"
    _dispatch_target = Dialect

    @classmethod
    def _listen(cls, event_key, retval=False):
        target = event_key.dispatch_target

        target._has_events = True
        event_key.base_listen()

    @classmethod
    def _accept_with(cls, target):
        if isinstance(target, type):
            if issubclass(target, Engine):
                return Dialect
            elif issubclass(target, Dialect):
                return target
        elif isinstance(target, Engine):
            return target.dialect
        else:
            return target

    def do_connect(self, dialect, conn_rec, cargs, cparams):
        """Receive connection arguments before a connection is made.

        Return a DBAPI connection to halt further events from invoking;
        the returned connection will be used.

        Alternatively, the event can manipulate the cargs and/or cparams
        collections; cargs will always be a Python list that can be mutated
        in-place and cparams a Python dictionary.  Return None to
        allow control to pass to the next event handler and ultimately
        to allow the dialect to connect normally, given the updated
        arguments.

        .. versionadded:: 1.0.3

        .. seealso::

            :ref:`custom_dbapi_args`

        """

    def do_executemany(self, cursor, statement, parameters, context):
        """Receive a cursor to have executemany() called.

        Return the value True to halt further events from invoking,
        and to indicate that the cursor execution has already taken
        place within the event handler.

        """

    def do_execute_no_params(self, cursor, statement, context):
        """Receive a cursor to have execute() with no parameters called.

        Return the value True to halt further events from invoking,
        and to indicate that the cursor execution has already taken
        place within the event handler.

        """

    def do_execute(self, cursor, statement, parameters, context):
        """Receive a cursor to have execute() called.

        Return the value True to halt further events from invoking,
        and to indicate that the cursor execution has already taken
        place within the event handler.

        """

    def do_setinputsizes(
        self, inputsizes, cursor, statement, parameters, context
    ):
        """Receive the setinputsizes dictionary for possible modification.

        This event is emitted in the case where the dialect makes use of the
        DBAPI ``cursor.setinputsizes()`` method which passes information about
        parameter binding for a particular statement.   The given
        ``inputsizes`` dictionary will contain :class:`.BindParameter` objects
        as keys, linked to DBAPI-specific type objects as values; for
        parameters that are not bound, they are added to the dictionary with
        ``None`` as the value, which means the parameter will not be included
        in the ultimate setinputsizes call.   The event may be used to inspect
        and/or log the datatypes that are being bound, as well as to modify the
        dictionary in place.  Parameters can be added, modified, or removed
        from this dictionary.   Callers will typically want to inspect the
        :attr:`.BindParameter.type` attribute of the given bind objects in
        order to make decisions about the DBAPI object.

        After the event, the ``inputsizes`` dictionary is converted into
        an appropriate datastructure to be passed to ``cursor.setinputsizes``;
        either a list for a positional bound parameter execution style,
        or a dictionary of string parameter keys to DBAPI type objects for
        a named bound parameter execution style.

        Most dialects **do not use** this method at all; the only built-in
        dialect which uses this hook is the cx_Oracle dialect.   The hook here
        is made available so as to allow customization of how datatypes are set
        up with the  cx_Oracle DBAPI.

        .. versionadded:: 1.2.9

        .. seealso::

            :ref:`cx_oracle_setinputsizes`

        """
        pass

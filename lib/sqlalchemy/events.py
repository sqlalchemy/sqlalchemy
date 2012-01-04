# sqlalchemy/events.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Core event interfaces."""

from sqlalchemy import event, exc, util
engine = util.importlater('sqlalchemy', 'engine')
pool = util.importlater('sqlalchemy', 'pool')


class DDLEvents(event.Events):
    """
    Define event listeners for schema objects,
    that is, :class:`.SchemaItem` and :class:`.SchemaEvent`
    subclasses, including :class:`.MetaData`, :class:`.Table`,
    :class:`.Column`.
    
    :class:`.MetaData` and :class:`.Table` support events
    specifically regarding when CREATE and DROP
    DDL is emitted to the database.  
    
    Attachment events are also provided to customize
    behavior whenever a child schema element is associated
    with a parent, such as, when a :class:`.Column` is associated
    with its :class:`.Table`, when a :class:`.ForeignKeyConstraint`
    is associated with a :class:`.Table`, etc.

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

    See also:

        :ref:`event_toplevel`

        :class:`.DDLElement`

        :class:`.DDL`

        :ref:`schema_ddl_sequences`

    """

    def before_create(self, target, connection, **kw):
        """Called before CREATE statments are emitted.

        :param target: the :class:`.MetaData` or :class:`.Table`
         object which is the target of the event.
        :param connection: the :class:`.Connection` where the
         CREATE statement or statements will be emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other 
         elements used by internal events.

        """

    def after_create(self, target, connection, **kw):
        """Called after CREATE statments are emitted.

        :param target: the :class:`.MetaData` or :class:`.Table`
         object which is the target of the event.
        :param connection: the :class:`.Connection` where the
         CREATE statement or statements have been emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other 
         elements used by internal events.

        """

    def before_drop(self, target, connection, **kw):
        """Called before DROP statments are emitted.

        :param target: the :class:`.MetaData` or :class:`.Table`
         object which is the target of the event.
        :param connection: the :class:`.Connection` where the
         DROP statement or statements will be emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other 
         elements used by internal events.

        """

    def after_drop(self, target, connection, **kw):
        """Called after DROP statments are emitted.

        :param target: the :class:`.MetaData` or :class:`.Table`
         object which is the target of the event.
        :param connection: the :class:`.Connection` where the
         DROP statement or statements have been emitted.
        :param \**kw: additional keyword arguments relevant
         to the event.  The contents of this dictionary
         may vary across releases, and include the
         list of tables being generated for a metadata-level
         event, the checkfirst flag, and other 
         elements used by internal events.

        """

    def before_parent_attach(self, target, parent):
        """Called before a :class:`.SchemaItem` is associated with 
        a parent :class:`.SchemaItem`.
        
        :param target: the target object
        :param parent: the parent to which the target is being attached.
        
        :func:`.event.listen` also accepts a modifier for this event:
        
        :param propagate=False: When True, the listener function will
         be established for any copies made of the target object,
         i.e. those copies that are generated when
         :meth:`.Table.tometadata` is used.
        
        """

    def after_parent_attach(self, target, parent):
        """Called after a :class:`.SchemaItem` is associated with 
        a parent :class:`.SchemaItem`.

        :param target: the target object
        :param parent: the parent to which the target is being attached.
        
        :func:`.event.listen` also accepts a modifier for this event:
        
        :param propagate=False: When True, the listener function will
         be established for any copies made of the target object,
         i.e. those copies that are generated when
         :meth:`.Table.tometadata` is used.
        
        """

    def column_reflect(self, table, column_info):
        """Called for each unit of 'column info' retrieved when
        a :class:`.Table` is being reflected.   
        
        The dictionary of column information as returned by the
        dialect is passed, and can be modified.  The dictionary
        is that returned in each element of the list returned 
        by :meth:`.reflection.Inspector.get_columns`. 
        
        The event is called before any action is taken against
        this dictionary, and the contents can be modified.
        The :class:`.Column` specific arguments ``info``, ``key``,
        and ``quote`` can also be added to the dictionary and
        will be passed to the constructor of :class:`.Column`.

        Note that this event is only meaningful if either
        associated with the :class:`.Table` class across the 
        board, e.g.::
        
            from sqlalchemy.schema import Table
            from sqlalchemy import event

            def listen_for_reflect(table, column_info):
                "receive a column_reflect event"
                # ...
                
            event.listen(
                    Table, 
                    'column_reflect', 
                    listen_for_reflect)
                
        ...or with a specific :class:`.Table` instance using
        the ``listeners`` argument::
        
            def listen_for_reflect(table, column_info):
                "receive a column_reflect event"
                # ...
                
            t = Table(
                'sometable', 
                autoload=True,
                listeners=[
                    ('column_reflect', listen_for_reflect)
                ])
        
        This because the reflection process initiated by ``autoload=True``
        completes within the scope of the constructor for :class:`.Table`.
        
        """

class SchemaEventTarget(object):
    """Base class for elements that are the targets of :class:`.DDLEvents` events.
    
    This includes :class:`.SchemaItem` as well as :class:`.SchemaType`.
    
    """
    dispatch = event.dispatcher(DDLEvents)

    def _set_parent(self, parent):
        """Associate with this SchemaEvent's parent object."""

        raise NotImplementedError()

    def _set_parent_with_dispatch(self, parent):
        self.dispatch.before_parent_attach(self, parent) 
        self._set_parent(parent) 
        self.dispatch.after_parent_attach(self, parent) 

class PoolEvents(event.Events):
    """Available events for :class:`.Pool`.

    The methods here define the name of an event as well
    as the names of members that are passed to listener
    functions.

    e.g.::

        from sqlalchemy import event

        def my_on_checkout(dbapi_conn, connection_rec, connection_proxy):
            "handle an on checkout event"

        event.listen(Pool, 'checkout', my_on_checkout)

    In addition to accepting the :class:`.Pool` class and :class:`.Pool` instances,
    :class:`.PoolEvents` also accepts :class:`.Engine` objects and
    the :class:`.Engine` class as targets, which will be resolved
    to the ``.pool`` attribute of the given engine or the :class:`.Pool`
    class::

        engine = create_engine("postgresql://scott:tiger@localhost/test")

        # will associate with engine.pool
        event.listen(engine, 'checkout', my_on_checkout)

    """

    @classmethod
    def _accept_with(cls, target):
        if isinstance(target, type):
            if issubclass(target, engine.Engine):
                return pool.Pool
            elif issubclass(target, pool.Pool):
                return target
        elif isinstance(target, engine.Engine):
            return target.pool
        else:
            return target

    def connect(self, dbapi_connection, connection_record):
        """Called once for each new DB-API connection or Pool's ``creator()``.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def first_connect(self, dbapi_connection, connection_record):
        """Called exactly once for the first DB-API connection.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def checkout(self, dbapi_connection, connection_record, connection_proxy):
        """Called when a connection is retrieved from the Pool.

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        :param con_proxy:
          The ``_ConnectionFairy`` which manages the connection for the span of
          the current checkout.

        If you raise a :class:`~sqlalchemy.exc.DisconnectionError`, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.
        """

    def checkin(self, dbapi_connection, connection_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

class ConnectionEvents(event.Events):
    """Available events for :class:`.Connection`.

    The methods here define the name of an event as well as the names of members that are passed to listener functions.

    e.g.::

        from sqlalchemy import event, create_engine

        def before_execute(conn, clauseelement, multiparams, params):
            log.info("Received statement: %s" % clauseelement)

        engine = create_engine('postgresql://scott:tiger@localhost/test')
        event.listen(engine, "before_execute", before_execute)

    Some events allow modifiers to the listen() function.

    :param retval=False: Applies to the :meth:`.before_execute` and 
      :meth:`.before_cursor_execute` events only.  When True, the
      user-defined event function must have a return value, which
      is a tuple of parameters that replace the given statement 
      and parameters.  See those methods for a description of
      specific return arguments.

    """

    @classmethod
    def _listen(cls, target, identifier, fn, retval=False):
        target._has_events = True

        if not retval:
            if identifier == 'before_execute':
                orig_fn = fn
                def wrap(conn, clauseelement, multiparams, params):
                    orig_fn(conn, clauseelement, multiparams, params)
                    return clauseelement, multiparams, params
                fn = wrap
            elif identifier == 'before_cursor_execute':
                orig_fn = fn
                def wrap(conn, cursor, statement, 
                        parameters, context, executemany):
                    orig_fn(conn, cursor, statement, 
                        parameters, context, executemany)
                    return statement, parameters
                fn = wrap

        elif retval and identifier not in ('before_execute', 'before_cursor_execute'):
            raise exc.ArgumentError(
                    "Only the 'before_execute' and "
                    "'before_cursor_execute' engine "
                    "event listeners accept the 'retval=True' "
                    "argument.")
        event.Events._listen(target, identifier, fn)

    def before_execute(self, conn, clauseelement, multiparams, params):
        """Intercept high level execute() events."""

    def after_execute(self, conn, clauseelement, multiparams, params, result):
        """Intercept high level execute() events."""

    def before_cursor_execute(self, conn, cursor, statement, 
                        parameters, context, executemany):
        """Intercept low-level cursor execute() events."""

    def after_cursor_execute(self, conn, cursor, statement, 
                        parameters, context, executemany):
        """Intercept low-level cursor execute() events."""

    def begin(self, conn):
        """Intercept begin() events."""

    def rollback(self, conn):
        """Intercept rollback() events."""

    def commit(self, conn):
        """Intercept commit() events."""

    def savepoint(self, conn, name=None):
        """Intercept savepoint() events."""

    def rollback_savepoint(self, conn, name, context):
        """Intercept rollback_savepoint() events."""

    def release_savepoint(self, conn, name, context):
        """Intercept release_savepoint() events."""

    def begin_twophase(self, conn, xid):
        """Intercept begin_twophase() events."""

    def prepare_twophase(self, conn, xid):
        """Intercept prepare_twophase() events."""

    def rollback_twophase(self, conn, xid, is_prepared):
        """Intercept rollback_twophase() events."""

    def commit_twophase(self, conn, xid, is_prepared):
        """Intercept commit_twophase() events."""


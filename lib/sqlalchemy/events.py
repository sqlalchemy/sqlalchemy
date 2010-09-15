"""Core event interfaces."""

from sqlalchemy import event

class DDLEvents(event.Events):
    """
    Define create/drop event listers for schema objects.
    
    See also:

        :mod:`sqlalchemy.event`
    
    """
    
    def on_before_create(self, target, connection, **kw):
        pass

    def on_after_create(self, target, connection, **kw):
        pass

    def on_before_drop(self, target, connection, **kw):
        pass
    
    def on_after_drop(self, target, connection, **kw):
        pass
    

class PoolEvents(event.Events):
    """Available events for :class:`.Pool`.
    
    The methods here define the name of an event as well
    as the names of members that are passed to listener
    functions.
    
    e.g.::
    
        from sqlalchemy import events
        
        def my_on_checkout(dbapi_conn, connection_rec, connection_proxy):
            "handle an on checkout event"
            
        events.listen(my_on_checkout, 'on_checkout', Pool)

    """
    
    def on_connect(self, dbapi_connection, connection_record):
        """Called once for each new DB-API connection or Pool's ``creator()``.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def on_first_connect(self, dbapi_connection, connection_record):
        """Called exactly once for the first DB-API connection.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def on_checkout(self, dbapi_connection, connection_record, connection_proxy):
        """Called when a connection is retrieved from the Pool.

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        :param con_proxy:
          The ``_ConnectionFairy`` which manages the connection for the span of
          the current checkout.

        If you raise an ``exc.DisconnectionError``, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.
        """

    def on_checkin(self, dbapi_connection, connection_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

class EngineEvents(event.Events):
    """Available events for :class:`.Engine`."""
    
    @classmethod
    def listen(cls, fn, identifier, target):
        from sqlalchemy.engine.base import Connection, \
            _listener_connection_cls
        if target.Connection is Connection:
            target.Connection = _listener_connection_cls(
                                        Connection, 
                                        target.dispatch)
        event.Events.listen(fn, identifier, target)

    def on_before_execute(self, conn, clauseelement, multiparams, params):
        """Intercept high level execute() events."""

    def on_after_execute(self, conn, clauseelement, multiparams, params, result):
        """Intercept high level execute() events."""
        
    def on_before_cursor_execute(self, conn, cursor, statement, 
                        parameters, context, executemany):
        """Intercept low-level cursor execute() events."""

    def on_after_cursor_execute(self, conn, cursor, statement, 
                        parameters, context, executemany):
        """Intercept low-level cursor execute() events."""

    def on_begin(self, conn):
        """Intercept begin() events."""
        
    def on_rollback(self, conn):
        """Intercept rollback() events."""
        
    def on_commit(self, conn):
        """Intercept commit() events."""
        
    def on_savepoint(self, conn, name=None):
        """Intercept savepoint() events."""
        
    def on_rollback_savepoint(self, conn, name, context):
        """Intercept rollback_savepoint() events."""
        
    def on_release_savepoint(self, conn, name, context):
        """Intercept release_savepoint() events."""
        
    def on_begin_twophase(self, conn, xid):
        """Intercept begin_twophase() events."""
        
    def on_prepare_twophase(self, conn, xid):
        """Intercept prepare_twophase() events."""
        
    def on_rollback_twophase(self, conn, xid, is_prepared):
        """Intercept rollback_twophase() events."""
        
    def on_commit_twophase(self, conn, xid, is_prepared):
        """Intercept commit_twophase() events."""


# sqlalchemy/engine/events.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from .base import Engine
from .interfaces import Connectable
from .interfaces import Dialect
from .. import event
from .. import exc
from .. import util


class ConnectionEvents(event.Events):
    """Available events for :class:`.Connectable`, which includes
    :class:`.Connection` and :class:`.Engine`.

    The methods here define the name of an event as well as the names of
    members that are passed to listener functions.

    An event listener can be associated with any :class:`.Connectable`
    class or instance, such as an :class:`.Engine`, e.g.::

        from sqlalchemy import event, create_engine

        def before_cursor_execute(conn, cursor, statement, parameters, context,
                                                        executemany):
            log.info("Received statement: %s", statement)

        engine = create_engine('postgresql://scott:tiger@localhost/test')
        event.listen(engine, "before_cursor_execute", before_cursor_execute)

    or with a specific :class:`.Connection`::

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

    .. note:: :class:`.ConnectionEvents` can be established on any
       combination of :class:`.Engine`, :class:`.Connection`, as well
       as instances of each of those classes.  Events across all
       four scopes will fire off for a given instance of
       :class:`.Connection`.  However, for performance reasons, the
       :class:`.Connection` object determines at instantiation time
       whether or not its parent :class:`.Engine` has event listeners
       established.   Event listeners added to the :class:`.Engine`
       class or to an instance of :class:`.Engine` *after* the instantiation
       of a dependent :class:`.Connection` instance will usually
       *not* be available on that :class:`.Connection` instance.  The newly
       added listeners will instead take effect for :class:`.Connection`
       instances created subsequent to those event listeners being
       established on the parent :class:`.Engine` class or instance.

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

        :param conn: :class:`.Connection` object
        :param clauseelement: SQL expression construct, :class:`.Compiled`
         instance, or string statement passed to :meth:`.Connection.execute`.
        :param multiparams: Multiple parameter sets, a list of dictionaries.
        :param params: Single parameter set, a single dictionary.

        .. seealso::

            :meth:`.before_cursor_execute`

        """

    def after_execute(self, conn, clauseelement, multiparams, params, result):
        """Intercept high level execute() events after execute.


        :param conn: :class:`.Connection` object
        :param clauseelement: SQL expression construct, :class:`.Compiled`
         instance, or string statement passed to :meth:`.Connection.execute`.
        :param multiparams: Multiple parameter sets, a list of dictionaries.
        :param params: Single parameter set, a single dictionary.
        :param result: :class:`.ResultProxy` generated by the execution.

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

        See the example at :class:`.ConnectionEvents`.

        :param conn: :class:`.Connection` object
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

        :param conn: :class:`.Connection` object
        :param cursor: DBAPI cursor object.  Will have results pending
         if the statement was a SELECT, but these should not be consumed
         as they will be needed by the :class:`.ResultProxy`.
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
        "The :meth:`.ConnectionEvents.dbapi_error` "
        "event is deprecated and will be removed in a future release. "
        "Please refer to the :meth:`.ConnectionEvents.handle_error` "
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
        into an :class:`.Engine`, typically for logging and
        debugging purposes.

        .. warning::

            Code should **not** modify
            any state or throw any exceptions here as this will
            interfere with SQLAlchemy's cleanup and error handling
            routines.  For exception modification, please refer to the
            new :meth:`.ConnectionEvents.handle_error` event.

        Subsequent to this hook, SQLAlchemy may attempt any
        number of operations on the connection/cursor, including
        closing the cursor, rolling back of the transaction in the
        case of connectionless execution, and disposing of the entire
        connection pool if a "disconnect" was detected.   The
        exception is then wrapped in a SQLAlchemy DBAPI exception
        wrapper and re-thrown.

        :param conn: :class:`.Connection` object
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
        r"""Intercept all exceptions processed by the :class:`.Connection`.

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
          specific exception.

        The hook is called while the cursor from the failed operation
        (if any) is still open and accessible.   Special cleanup operations
        can be called on this cursor; SQLAlchemy will attempt to close
        this cursor subsequent to this hook being invoked.  If the connection
        is in "autocommit" mode, the transaction also remains open within
        the scope of this hook; the rollback of the per-statement transaction
        also occurs after the hook is called.

        For the common case of detecting a "disconnect" situation which
        is not currently handled by the SQLAlchemy dialect, the
        :attr:`.ExceptionContext.is_disconnect` flag can be set to True which
        will cause the exception to be considered as a disconnect situation,
        which typically results in the connection pool being invalidated::

            @event.listens_for(Engine, "handle_error")
            def handle_exception(context):
                if isinstance(context.original_exception, pyodbc.Error):
                    for code in (
                        '08S01', '01002', '08003',
                        '08007', '08S02', '08001', 'HYT00', 'HY010'):

                        if code in str(context.original_exception):
                            context.is_disconnect = True

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

        .. warning::  Because the :meth:`.ConnectionEvents.handle_error`
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
            :meth:`.ConnectionEvents.handle_error` hook.

        .. versionchanged:: 1.1 The :meth:`.handle_error` event will now
           receive all exceptions that inherit from ``BaseException``,
           including ``SystemExit`` and ``KeyboardInterrupt``.  The setting for
           :attr:`.ExceptionContext.is_disconnect` is ``True`` in this case and
           the default for
           :attr:`.ExceptionContext.invalidate_pool_on_disconnect` is
           ``False``.

        .. versionchanged:: 1.0.0 The :meth:`.handle_error` event is now
           invoked when an :class:`.Engine` fails during the initial
           call to :meth:`.Engine.connect`, as well as when a
           :class:`.Connection` object encounters an error during a
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
        """Intercept the creation of a new :class:`.Connection`.

        This event is called typically as the direct result of calling
        the :meth:`.Engine.connect` method.

        It differs from the :meth:`.PoolEvents.connect` method, which
        refers to the actual connection to a database at the DBAPI level;
        a DBAPI connection may be pooled and reused for many operations.
        In contrast, this event refers only to the production of a higher level
        :class:`.Connection` wrapper around such a DBAPI connection.

        It also differs from the :meth:`.PoolEvents.checkout` event
        in that it is specific to the :class:`.Connection` object, not the
        DBAPI connection that :meth:`.PoolEvents.checkout` deals with, although
        this DBAPI connection is available here via the
        :attr:`.Connection.connection` attribute.  But note there can in fact
        be multiple :meth:`.PoolEvents.checkout` events within the lifespan
        of a single :class:`.Connection` object, if that :class:`.Connection`
        is invalidated and re-established.  There can also be multiple
        :class:`.Connection` objects generated for the same already-checked-out
        DBAPI connection, in the case that a "branch" of a :class:`.Connection`
        is produced.

        :param conn: :class:`.Connection` object.
        :param branch: if True, this is a "branch" of an existing
         :class:`.Connection`.  A branch is generated within the course
         of a statement execution to invoke supplemental statements, most
         typically to pre-execute a SELECT of a default value for the purposes
         of an INSERT statement.

        .. versionadded:: 0.9.0

        .. seealso::

            :ref:`pool_disconnects_pessimistic` - illustrates how to use
            :meth:`.ConnectionEvents.engine_connect`
            to transparently ensure pooled connections are connected to the
            database.

            :meth:`.PoolEvents.checkout` the lower-level pool checkout event
            for an individual DBAPI connection

            :meth:`.ConnectionEvents.set_connection_execution_options` - a copy
            of a :class:`.Connection` is also made when the
            :meth:`.Connection.execution_options` method is called.

        """

    def set_connection_execution_options(self, conn, opts):
        """Intercept when the :meth:`.Connection.execution_options`
        method is called.

        This method is called after the new :class:`.Connection` has been
        produced, with the newly updated execution options collection, but
        before the :class:`.Dialect` has acted upon any of those new options.

        Note that this method is not called when a new :class:`.Connection`
        is produced which is inheriting execution options from its parent
        :class:`.Engine`; to intercept this condition, use the
        :meth:`.ConnectionEvents.engine_connect` event.

        :param conn: The newly copied :class:`.Connection` object

        :param opts: dictionary of options that were passed to the
         :meth:`.Connection.execution_options` method.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`.ConnectionEvents.set_engine_execution_options` - event
            which is called when :meth:`.Engine.execution_options` is called.


        """

    def set_engine_execution_options(self, engine, opts):
        """Intercept when the :meth:`.Engine.execution_options`
        method is called.

        The :meth:`.Engine.execution_options` method produces a shallow
        copy of the :class:`.Engine` which stores the new options.  That new
        :class:`.Engine` is passed here.   A particular application of this
        method is to add a :meth:`.ConnectionEvents.engine_connect` event
        handler to the given :class:`.Engine` which will perform some per-
        :class:`.Connection` task specific to these execution options.

        :param conn: The newly copied :class:`.Engine` object

        :param opts: dictionary of options that were passed to the
         :meth:`.Connection.execution_options` method.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`.ConnectionEvents.set_connection_execution_options` - event
            which is called when :meth:`.Connection.execution_options` is
            called.

        """

    def engine_disposed(self, engine):
        """Intercept when the :meth:`.Engine.dispose` method is called.

        The :meth:`.Engine.dispose` method instructs the engine to
        "dispose" of it's connection pool (e.g. :class:`.Pool`), and
        replaces it with a new one.  Disposing of the old pool has the
        effect that existing checked-in connections are closed.  The new
        pool does not establish any new connections until it is first used.

        This event can be used to indicate that resources related to the
        :class:`.Engine` should also be cleaned up, keeping in mind that the
        :class:`.Engine` can still be used for new requests in which case
        it re-acquires connection resources.

        .. versionadded:: 1.0.5

        """

    def begin(self, conn):
        """Intercept begin() events.

        :param conn: :class:`.Connection` object

        """

    def rollback(self, conn):
        """Intercept rollback() events, as initiated by a
        :class:`.Transaction`.

        Note that the :class:`.Pool` also "auto-rolls back"
        a DBAPI connection upon checkin, if the ``reset_on_return``
        flag is set to its default value of ``'rollback'``.
        To intercept this
        rollback, use the :meth:`.PoolEvents.reset` hook.

        :param conn: :class:`.Connection` object

        .. seealso::

            :meth:`.PoolEvents.reset`

        """

    def commit(self, conn):
        """Intercept commit() events, as initiated by a
        :class:`.Transaction`.

        Note that the :class:`.Pool` may also "auto-commit"
        a DBAPI connection upon checkin, if the ``reset_on_return``
        flag is set to the value ``'commit'``.  To intercept this
        commit, use the :meth:`.PoolEvents.reset` hook.

        :param conn: :class:`.Connection` object
        """

    def savepoint(self, conn, name):
        """Intercept savepoint() events.

        :param conn: :class:`.Connection` object
        :param name: specified name used for the savepoint.

        """

    def rollback_savepoint(self, conn, name, context):
        """Intercept rollback_savepoint() events.

        :param conn: :class:`.Connection` object
        :param name: specified name used for the savepoint.
        :param context: :class:`.ExecutionContext` in use.  May be ``None``.

        """

    def release_savepoint(self, conn, name, context):
        """Intercept release_savepoint() events.

        :param conn: :class:`.Connection` object
        :param name: specified name used for the savepoint.
        :param context: :class:`.ExecutionContext` in use.  May be ``None``.

        """

    def begin_twophase(self, conn, xid):
        """Intercept begin_twophase() events.

        :param conn: :class:`.Connection` object
        :param xid: two-phase XID identifier

        """

    def prepare_twophase(self, conn, xid):
        """Intercept prepare_twophase() events.

        :param conn: :class:`.Connection` object
        :param xid: two-phase XID identifier
        """

    def rollback_twophase(self, conn, xid, is_prepared):
        """Intercept rollback_twophase() events.

        :param conn: :class:`.Connection` object
        :param xid: two-phase XID identifier
        :param is_prepared: boolean, indicates if
         :meth:`.TwoPhaseTransaction.prepare` was called.

        """

    def commit_twophase(self, conn, xid, is_prepared):
        """Intercept commit_twophase() events.

        :param conn: :class:`.Connection` object
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
        please use the :class:`.ConnectionEvents` interface.

    .. seealso::

        :meth:`.ConnectionEvents.before_cursor_execute`

        :meth:`.ConnectionEvents.before_execute`

        :meth:`.ConnectionEvents.after_cursor_execute`

        :meth:`.ConnectionEvents.after_execute`


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

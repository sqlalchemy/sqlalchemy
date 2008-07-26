# session.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Provides the Session class and related utilities."""


import weakref
from sqlalchemy import util, exceptions, sql, engine
from sqlalchemy.orm import unitofwork, query, attributes, util as mapperutil
from sqlalchemy.orm.mapper import object_mapper as _object_mapper
from sqlalchemy.orm.mapper import class_mapper as _class_mapper
from sqlalchemy.orm.mapper import Mapper


__all__ = ['Session', 'SessionTransaction', 'SessionExtension']

def sessionmaker(bind=None, class_=None, autoflush=True, transactional=True, **kwargs):
    """Generate a custom-configured [sqlalchemy.orm.session#Session] class.

    The returned object is a subclass of ``Session``, which, when instantiated with no
    arguments, uses the keyword arguments configured here as its constructor arguments.

    It is intended that the `sessionmaker()` function be called within the global scope
    of an application, and the returned class be made available to the rest of the
    application as the single class used to instantiate sessions.

    e.g.::

        # global scope
        Session = sessionmaker(autoflush=False)

        # later, in a local scope, create and use a session:
        sess = Session()

    Any keyword arguments sent to the constructor itself will override the "configured"
    keywords::

        Session = sessionmaker()

        # bind an individual session to a connection
        sess = Session(bind=connection)

    The class also includes a special classmethod ``configure()``, which allows
    additional configurational options to take place after the custom ``Session``
    class has been generated.  This is useful particularly for defining the
    specific ``Engine`` (or engines) to which new instances of ``Session``
    should be bound::

        Session = sessionmaker()
        Session.configure(bind=create_engine('sqlite:///foo.db'))

        sess = Session()

    The function features a single keyword argument of its own, `class_`, which
    may be used to specify an alternate class other than ``sqlalchemy.orm.session.Session``
    which should be used by the returned class.  All other keyword arguments sent to
    `sessionmaker()` are passed through to the instantiated `Session()` object.
    """

    kwargs['bind'] = bind
    kwargs['autoflush'] = autoflush
    kwargs['transactional'] = transactional

    if class_ is None:
        class_ = Session

    class Sess(class_):
        def __init__(self, **local_kwargs):
            for k in kwargs:
                local_kwargs.setdefault(k, kwargs[k])
            super(Sess, self).__init__(**local_kwargs)

        def configure(self, **new_kwargs):
            """(re)configure the arguments for this sessionmaker.

            e.g.
                Session = sessionmaker()
                Session.configure(bind=create_engine('sqlite://'))
            """

            kwargs.update(new_kwargs)
        configure = classmethod(configure)

    return Sess

class SessionExtension(object):
    """An extension hook object for Sessions.  Subclasses may be installed into a Session
    (or sessionmaker) using the ``extension`` keyword argument.
    """

    def before_commit(self, session):
        """Execute right before commit is called.

        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def after_commit(self, session):
        """Execute after a commit has occured.

        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def after_rollback(self, session):
        """Execute after a rollback has occured.

        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def before_flush(self, session, flush_context, instances):
        """Execute before flush process has started.

        `instances` is an optional list of objects which were passed to the ``flush()``
        method.
        """

    def after_flush(self, session, flush_context):
        """Execute after flush has completed, but before commit has been called.

        Note that the session's state is still in pre-flush, i.e. 'new', 'dirty',
        and 'deleted' lists still show pre-flush state as well as the history
        settings on instance attributes."""

    def after_flush_postexec(self, session, flush_context):
        """Execute after flush has completed, and after the post-exec state occurs.

        This will be when the 'new', 'dirty', and 'deleted' lists are in their final
        state.  An actual commit() may or may not have occured, depending on whether or not
        the flush started its own transaction or participated in a larger transaction.
        """
    
    def after_begin(self, session, transaction, connection):
        """Execute after a transaction is begun on a connection
        
        `transaction` is the SessionTransaction. This method is called after an
        engine level transaction is begun on a connection.
        """

    def after_attach(self, session, instance):
        """Execute after an instance is attached to a session."""

class SessionTransaction(object):
    """Represents a Session-level Transaction.

    This corresponds to one or more [sqlalchemy.engine#Transaction]
    instances behind the scenes, with one ``Transaction`` per ``Engine`` in
    use.

    Direct usage of ``SessionTransaction`` is not necessary as of
    SQLAlchemy 0.4; use the ``begin()`` and ``commit()`` methods on
    ``Session`` itself.

    The ``SessionTransaction`` object is **not** threadsafe.
    """

    def __init__(self, session, parent=None, autoflush=True, nested=False):
        self.session = session
        self._connections = {}
        self._parent = parent
        self.autoflush = autoflush
        self.nested = nested
        self._active = True
        self._prepared = False

    is_active = property(lambda s: s.session is not None and s._active)
    
    def _assert_is_active(self):
        self._assert_is_open()
        if not self._active:
            raise exceptions.InvalidRequestError("The transaction is inactive due to a rollback in a subtransaction and should be closed")

    def _assert_is_open(self):
        if self.session is None:
            raise exceptions.InvalidRequestError("The transaction is closed")

    def connection(self, bindkey, **kwargs):
        self._assert_is_active()
        engine = self.session.get_bind(bindkey, **kwargs)
        return self.get_or_add(engine)

    def _begin(self, **kwargs):
        self._assert_is_active()
        return SessionTransaction(self.session, self, **kwargs)

    def _iterate_parents(self, upto=None):
        if self._parent is upto:
            return (self,)
        else:
            if self._parent is None:
                raise exceptions.InvalidRequestError("Transaction %s is not on the active transaction list" % upto)
            return (self,) + self._parent._iterate_parents(upto)

    def add(self, bind):
        self._assert_is_active()
        if self._parent is not None and not self.nested:
            return self._parent.add(bind)

        if bind.engine in self._connections:
            raise exceptions.InvalidRequestError("Session already has a Connection associated for the given %sEngine" % (isinstance(bind, engine.Connection) and "Connection's " or ""))
        return self.get_or_add(bind)

    def get_or_add(self, bind):
        self._assert_is_active()
        
        if bind in self._connections:
            return self._connections[bind][0]
        
        if self._parent is not None:
            conn = self._parent.get_or_add(bind)
            if not self.nested:
                return conn
        else:
            if isinstance(bind, engine.Connection):
                conn = bind
                if conn.engine in self._connections:
                    raise exceptions.InvalidRequestError("Session already has a Connection associated for the given Connection's Engine")
            else:
                conn = bind.contextual_connect()

        if self.session.twophase and self._parent is None:
            transaction = conn.begin_twophase()
        elif self.nested:
            transaction = conn.begin_nested()
        else:
            transaction = conn.begin()
        
        self._connections[conn] = self._connections[conn.engine] = (conn, transaction, conn is not bind)
        if self.session.extension is not None:
            self.session.extension.after_begin(self.session, self, conn)
        return conn

    def prepare(self):
        if self._parent is not None or not self.session.twophase:
            raise exceptions.InvalidRequestError("Only root two phase transactions of can be prepared")
        self._prepare_impl()
        
    def _prepare_impl(self):
        self._assert_is_active()
        if self.session.extension is not None and (self._parent is None or self.nested):
            self.session.extension.before_commit(self.session)
        
        if self.session.transaction is not self:
            for subtransaction in self.session.transaction._iterate_parents(upto=self):
                subtransaction.commit()
            
        if self.autoflush:
            self.session.flush()
        
        if self._parent is None and self.session.twophase:
            try:
                for t in util.Set(self._connections.values()):
                    t[1].prepare()
            except:
                self.rollback()
                raise
        
        self._deactivate()
        self._prepared = True
    
    def commit(self):
        self._assert_is_open()
        if not self._prepared:
            self._prepare_impl()
        
        if self._parent is None or self.nested:
            for t in util.Set(self._connections.values()):
                t[1].commit()

            if self.session.extension is not None:
                self.session.extension.after_commit(self.session)

        self.close()
        return self._parent

    def rollback(self):
        self._assert_is_open()
        
        if self.session.transaction is not self:
            for subtransaction in self.session.transaction._iterate_parents(upto=self):
                subtransaction.close()
        
        if self.is_active or self._prepared:
            for transaction in self._iterate_parents():
                if transaction._parent is None or transaction.nested:
                    transaction._rollback_impl()
                    transaction._deactivate()
                    break
                else:
                    transaction._deactivate()

        self.close()
        return self._parent
    
    def _rollback_impl(self):
        for t in util.Set(self._connections.values()):
            t[1].rollback()

        if self.session.extension is not None:
            self.session.extension.after_rollback(self.session)

    def _deactivate(self):
        self._active = False

    def close(self):
        self.session.transaction = self._parent
        if self._parent is None:
            for connection, transaction, autoclose in util.Set(self._connections.values()):
                if autoclose:
                    connection.close()
                else:
                    transaction.close()
        self._deactivate()
        self.session = None
        self._connections = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.session.transaction is None:
            return
        if type is None:
            try:
                self.commit()
            except:
                self.rollback()
                raise
        else:
            self.rollback()

class Session(object):
    """Encapsulates a set of objects being operated upon within an object-relational operation.

    The Session is the front end to SQLAlchemy's **Unit of Work** implementation. The concept
    behind Unit of Work is to track modifications to a field of objects, and then be able to
    flush those changes to the database in a single operation.

    SQLAlchemy's unit of work includes these functions:

    * The ability to track in-memory changes on scalar- and collection-based object
      attributes, such that database persistence operations can be assembled based on those
      changes.

    * The ability to organize individual SQL queries and population of newly generated
      primary and foreign key-holding attributes during a persist operation such that
      referential integrity is maintained at all times.

    * The ability to maintain insert ordering against the order in which new instances were
      added to the session.

    * an Identity Map, which is a dictionary keying instances to their unique primary key
      identity. This ensures that only one copy of a particular entity is ever present
      within the session, even if repeated load operations for the same entity occur. This
      allows many parts of an application to get a handle to a particular object without
      any chance of modifications going to two different places.

    When dealing with instances of mapped classes, an instance may be *attached* to a
    particular Session, else it is *unattached* . An instance also may or may not correspond
    to an actual row in the database. These conditions break up into four distinct states:

    * *Transient* - an instance that's not in a session, and is not saved to the database;
      i.e. it has no database identity. The only relationship such an object has to the ORM
      is that its class has a `mapper()` associated with it.

    * *Pending* - when you `save()` a transient instance, it becomes pending. It still
      wasn't actually flushed to the database yet, but it will be when the next flush
      occurs.

    * *Persistent* - An instance which is present in the session and has a record in the
      database. You get persistent instances by either flushing so that the pending
      instances become persistent, or by querying the database for existing instances (or
      moving persistent instances from other sessions into your local session).

    * *Detached* - an instance which has a record in the database, but is not in any
      session. Theres nothing wrong with this, and you can use objects normally when
      they're detached, **except** they will not be able to issue any SQL in order to load
      collections or attributes which are not yet loaded, or were marked as "expired".

    The session methods which control instance state include ``save()``, ``update()``,
    ``save_or_update()``, ``delete()``, ``merge()``, and ``expunge()``.

    The Session object is **not** threadsafe, particularly during flush operations.  A session
    which is only read from (i.e. is never flushed) can be used by concurrent threads if it's
    acceptable that some object instances may be loaded twice.

    The typical pattern to managing Sessions in a multi-threaded environment is either to use
    mutexes to limit concurrent access to one thread at a time, or more commonly to establish
    a unique session for every thread, using a threadlocal variable.  SQLAlchemy provides
    a thread-managed Session adapter, provided by the [sqlalchemy.orm#scoped_session()] function.
    """

    def __init__(self, bind=None, autoflush=True, transactional=False, twophase=False, echo_uow=False, weak_identity_map=True, binds=None, extension=None):
        """Construct a new Session.
        
        A session is usually constructed using the [sqlalchemy.orm#create_session()] function, 
        or its more "automated" variant [sqlalchemy.orm#sessionmaker()].

        autoflush
            When ``True``, all query operations will issue a ``flush()`` call to this
            ``Session`` before proceeding. This is a convenience feature so that
            ``flush()`` need not be called repeatedly in order for database queries to
            retrieve results. It's typical that ``autoflush`` is used in conjunction with
            ``transactional=True``, so that ``flush()`` is never called; you just call
            ``commit()`` when changes are complete to finalize all changes to the
            database.

        bind
            An optional ``Engine`` or ``Connection`` to which this ``Session`` should be
            bound. When specified, all SQL operations performed by this session will
            execute via this connectable.

        binds
            An optional dictionary, which contains more granular "bind" information than
            the ``bind`` parameter provides. This dictionary can map individual ``Table``
            instances as well as ``Mapper`` instances to individual ``Engine`` or
            ``Connection`` objects. Operations which proceed relative to a particular
            ``Mapper`` will consult this dictionary for the direct ``Mapper`` instance as
            well as the mapper's ``mapped_table`` attribute in order to locate an
            connectable to use. The full resolution is described in the ``get_bind()``
            method of ``Session``. Usage looks like::

                sess = Session(binds={
                    SomeMappedClass : create_engine('postgres://engine1'),
                    somemapper : create_engine('postgres://engine2'),
                    some_table : create_engine('postgres://engine3'),
                })

            Also see the ``bind_mapper()`` and ``bind_table()`` methods.

        echo_uow
            When ``True``, configure Python logging to dump all unit-of-work
            transactions. This is the equivalent of
            ``logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)``.

        extension
            An optional [sqlalchemy.orm.session#SessionExtension] instance, which will receive
            pre- and post- commit and flush events, as well as a post-rollback event.  User-
            defined code may be placed within these hooks using a user-defined subclass
            of ``SessionExtension``.

        transactional
            Set up this ``Session`` to automatically begin transactions. Setting this
            flag to ``True`` is the rough equivalent of calling ``begin()`` after each
            ``commit()`` operation, after each ``rollback()``, and after each
            ``close()``. Basically, this has the effect that all session operations are
            performed within the context of a transaction. Note that the ``begin()``
            operation does not immediately utilize any connection resources; only when
            connection resources are first required do they get allocated into a
            transactional context.

        twophase
            When ``True``, all transactions will be started using
            [sqlalchemy.engine_TwoPhaseTransaction]. During a ``commit()``, after
            ``flush()`` has been issued for all attached databases, the ``prepare()``
            method on each database's ``TwoPhaseTransaction`` will be called. This allows
            each database to roll back the entire transaction, before each transaction is
            committed.

        weak_identity_map
            When set to the default value of ``False``, a weak-referencing map is used;
            instances which are not externally referenced will be garbage collected
            immediately. For dereferenced instances which have pending changes present,
            the attribute management system will create a temporary strong-reference to
            the object which lasts until the changes are flushed to the database, at which
            point it's again dereferenced. Alternatively, when using the value ``True``,
            the identity map uses a regular Python dictionary to store instances. The
            session will maintain all instances present until they are removed using
            expunge(), clear(), or purge().
        """
        self.echo_uow = echo_uow
        self.weak_identity_map = weak_identity_map
        self.uow = unitofwork.UnitOfWork(self)
        self.identity_map = self.uow.identity_map

        self.bind = bind
        self.__binds = {}
        self.transaction = None
        self.hash_key = id(self)
        self.autoflush = autoflush
        self.transactional = transactional
        self.twophase = twophase
        self.extension = extension
        self._query_cls = query.Query
        self._mapper_flush_opts = {}

        if binds is not None:
            for mapperortable, value in binds.iteritems():
                if isinstance(mapperortable, type):
                    mapperortable = _class_mapper(mapperortable).base_mapper
                self.__binds[mapperortable] = value
                if isinstance(mapperortable, Mapper):
                    for t in mapperortable._all_tables:
                        self.__binds[t] = value

        if self.transactional:
            self.begin()
        _sessions[self.hash_key] = self

    def begin(self, **kwargs):
        """Begin a transaction on this Session."""

        if self.transaction is not None:
            self.transaction = self.transaction._begin(**kwargs)
        else:
            self.transaction = SessionTransaction(self, **kwargs)
        return self.transaction

    create_transaction = begin

    def begin_nested(self):
        """Begin a `nested` transaction on this Session.

        This utilizes a ``SAVEPOINT`` transaction for databases
        which support this feature.
        """

        return self.begin(nested=True)

    def rollback(self):
        """Rollback the current transaction in progress.

        If no transaction is in progress, this method is a
        pass-thru.
        """

        if self.transaction is None:
            pass
        else:
            self.transaction.rollback()
        # TODO: we can rollback attribute values.  however
        # we would want to expand attributes.py to be able to save *two* rollback points, one to the
        # last flush() and the other to when the object first entered the transaction.
        # [ticket:705]
        #attributes.rollback(*self.identity_map.values())
        if self.transaction is None and self.transactional:
            self.begin()

    def commit(self):
        """Commit the current transaction in progress.

        If no transaction is in progress, this method raises
        an InvalidRequestError.

        If the ``begin()`` method was called on this ``Session``
        additional times subsequent to its first call,
        ``commit()`` will not actually commit, and instead
        pops an internal SessionTransaction off its internal stack
        of transactions.  Only when the "root" SessionTransaction
        is reached does an actual database-level commit occur.
        """

        if self.transaction is None:
            if self.transactional:
                self.begin()
            else:
                raise exceptions.InvalidRequestError("No transaction is begun.")

        self.transaction.commit()
        if self.transaction is None and self.transactional:
            self.begin()
    
    def prepare(self):
        """Prepare the current transaction in progress for two phase commit.

        If no transaction is in progress, this method raises
        an InvalidRequestError.

        Only root transactions of two phase sessions can be prepared. If the current transaction is
        not such, an InvalidRequestError is raised.
        """
        if self.transaction is None:
            if self.transactional:
                self.begin()
            else:
                raise exceptions.InvalidRequestError("No transaction is begun.")

        self.transaction.prepare()

    def connection(self, mapper=None, clause=None, instance=None):
        """Return a ``Connection`` corresponding to this session's
        transactional context, if any.

        If this ``Session`` is transactional, the connection will be in
        the context of this session's transaction.  Otherwise, the
        connection is returned by the ``contextual_connect()`` method
        on the engine.

        The `mapper` argument is a class or mapper to which a bound engine
        will be located; use this when the Session itself is either bound
        to multiple engines or connections, or is not bound to any connectable.

        \**kwargs are additional arguments which will be passed to get_bind().
        See the get_bind() method for details.  Note that the ``ShardedSession``
        subclass takes a different get_bind() argument signature.
        """

        return self.__connection(self.get_bind(mapper, clause, instance))

    def __connection(self, engine, **kwargs):
        if self.transaction is not None:
            return self.transaction.get_or_add(engine)
        else:
            return engine.contextual_connect(**kwargs)

    def execute(self, clause, params=None, mapper=None, instance=None):
        """Execute the given clause, using the current transaction (if any).

        Returns a ``ResultProxy`` corresponding to the execution's results.
        
        clause
            a ClauseElement (i.e. select(), text(), etc.) or 
            string SQL statement to be executed
            
        params 
            a dictionary of bind parameters.
        
        mapper
            a mapped class or Mapper instance which may be needed
            in order to locate the proper bind.  This is typically
            if the Session is not directly bound to a single engine.
            
        instance
            used by some Query operations to further identify
            the proper bind, in the case of ShardedSession.
            
        """
        engine = self.get_bind(mapper, clause=clause, instance=instance)

        return self.__connection(engine, close_with_result=True).execute(clause, params or {})

    def scalar(self, clause, params=None, mapper=None, instance=None):
        """Like execute() but return a scalar result."""

        engine = self.get_bind(mapper, clause=clause, instance=instance)

        return self.__connection(engine, close_with_result=True).scalar(clause, params or {})

    def close(self):
        """Close this Session.

        This clears all items and ends any transaction in progress.

        If this session were created with ``transactional=True``, a
        new transaction is immediately begun.  Note that this new
        transaction does not use any connection resources until they
        are first needed.
        """

        self.clear()
        if self.transaction is not None:
            for transaction in self.transaction._iterate_parents():
                transaction.close()
        if self.transactional:
            # note this doesnt use any connection resources
            self.begin()

    def close_all(cls):
        """Close *all* sessions in memory."""

        for sess in _sessions.values():
            sess.close()
    close_all = classmethod(close_all)

    def clear(self):
        """Remove all object instances from this ``Session``.

        This is equivalent to calling ``expunge()`` for all objects in
        this ``Session``.
        """
        
        for instance in self:
            self._unattach(instance)
        self.uow = unitofwork.UnitOfWork(self)
        self.identity_map = self.uow.identity_map

    # TODO: need much more test coverage for bind_mapper() and similar !

    def bind_mapper(self, mapper, bind, entity_name=None):
        """Bind the given `mapper` or `class` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Mapper`` will use the
        given `bind`.
        """

        if isinstance(mapper, type):
            mapper = _class_mapper(mapper, entity_name=entity_name)

        self.__binds[mapper.base_mapper] = bind
        for t in mapper._all_tables:
            self.__binds[t] = bind

    def bind_table(self, table, bind):
        """Bind the given `table` to the given ``Engine`` or ``Connection``.

        All subsequent operations involving this ``Table`` will use the
        given `bind`.
        """

        self.__binds[table] = bind

    def get_bind(self, mapper, clause=None, instance=None):
        """Return an engine corresponding to the given arguments.

        mapper
            mapper relative to the desired operation.

        clause
            a ClauseElement which is to be executed.  if
            mapper is not present, this may be used to locate
            Table objects, which are then associated with mappers
            which have associated binds.
        
        instance
            an ORM mapped instance which may be used to further
            locate the correct bind.  This is currently used by 
            the ShardedSession subclass.
            
        """
        if mapper is None and clause is None:
            if self.bind is not None:
                return self.bind
            else:
                raise exceptions.UnboundExecutionError("This session is unbound to any Engine or Connection; specify a mapper to get_bind()")

        elif len(self.__binds):
            if mapper is not None:
                if isinstance(mapper, type):
                    mapper = _class_mapper(mapper)
                if mapper.base_mapper in self.__binds:
                    return self.__binds[mapper.base_mapper]
                elif mapper.compile().mapped_table in self.__binds:
                    return self.__binds[mapper.mapped_table]
            if clause is not None:
                for t in clause._table_iterator():
                    if t in self.__binds:
                        return self.__binds[t]

        if self.bind is not None:
            return self.bind
        elif isinstance(clause, sql.expression.ClauseElement) and clause.bind is not None:
            return clause.bind
        elif mapper is None:
            raise exceptions.UnboundExecutionError("Could not locate any mapper associated with SQL expression")
        else:
            if isinstance(mapper, type):
                mapper = _class_mapper(mapper)
            else:
                mapper = mapper.compile()
            e = mapper.mapped_table.bind
            if e is None:
                raise exceptions.UnboundExecutionError("Could not locate any Engine or Connection bound to mapper '%s'" % str(mapper))
            return e

    def query(self, mapper_or_class, *addtl_entities, **kwargs):
        """Return a new ``Query`` object corresponding to this ``Session`` and
        the mapper, or the classes' primary mapper.

        """
        entity_name = kwargs.pop('entity_name', None)

        if isinstance(mapper_or_class, type):
            q = self._query_cls(_class_mapper(mapper_or_class, entity_name=entity_name), self, **kwargs)
        else:
            q = self._query_cls(mapper_or_class, self, **kwargs)

        for ent in addtl_entities:
            q = q.add_entity(ent)
        return q

    def _autoflush(self):
        if self.autoflush and (self.transaction is None or self.transaction.autoflush):
            self.flush()

    def flush(self, objects=None):
        """Flush all the object modifications present in this session
        to the database.

        `objects` is a collection or iterator of objects specifically to be
        flushed; if ``None``, all new and modified objects are flushed.

        """
        if objects is not None:
            try:
                if not len(objects):
                    return
            except TypeError:
                objects = list(objects)
                if not objects:
                    return
        self.uow.flush(self, objects)

    def get(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier, or ``None`` if not found.
        
        DEPRECATED.  use session.query(class_).get(ident)

        """
        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).get(ident, **kwargs)

    def load(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier.

        DEPRECATED.  use session.query(class_).populate_existing().get(ident).

        """
        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).load(ident, **kwargs)

    def refresh(self, instance, attribute_names=None):
        """Refresh the attributes on the given instance.

        When called, a query will be issued
        to the database which will refresh all attributes with their
        current value.

        Lazy-loaded relational attributes will remain lazily loaded, so that
        the instance-wide refresh operation will be followed
        immediately by the lazy load of that attribute.

        Eagerly-loaded relational attributes will eagerly load within the
        single refresh operation.

        The ``attribute_names`` argument is an iterable collection
        of attribute names indicating a subset of attributes to be
        refreshed.
        """

        self._validate_persistent(instance)

        if self.query(_object_mapper(instance))._get(instance._instance_key, refresh_instance=instance._state, only_load_props=attribute_names) is None:
            raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % mapperutil.instance_str(instance))
    
    def expire_all(self):
        """Expires all persistent instances within this Session.  
        
        """
        for state in self.identity_map.all_states():
            _expire_state(state, None)
        
    def expire(self, instance, attribute_names=None):
        """Expire the attributes on the given instance.

        The instance's attributes are instrumented such that
        when an attribute is next accessed, a query will be issued
        to the database which will refresh all attributes with their
        current value.

        The ``attribute_names`` argument is an iterable collection
        of attribute names indicating a subset of attributes to be
        expired.
        """

        if attribute_names:
            self._validate_persistent(instance)
            _expire_state(instance._state, attribute_names=attribute_names)
        else:
            # pre-fetch the full cascade since the expire is going to
            # remove associations
            cascaded = list(_cascade_iterator('refresh-expire', instance))
            self._validate_persistent(instance)
            _expire_state(instance._state, None)
            for (c, m) in cascaded:
                self._validate_persistent(c)
                _expire_state(c._state, None)

    def prune(self):
        """Remove unreferenced instances cached in the identity map.

        Note that this method is only meaningful if "weak_identity_map"
        is set to False.

        Removes any object in this Session's identity map that is not
        referenced in user code, modified, new or scheduled for deletion.
        Returns the number of objects pruned.
        """

        return self.uow.prune_identity_map()

    def expunge(self, instance):
        """Remove the given `instance` from this ``Session``.

        This will free all internal references to the instance.
        Cascading will be applied according to the *expunge* cascade
        rule.
        """
        self._validate_persistent(instance)
        for c, m in [(instance, None)] + list(_cascade_iterator('expunge', instance)):
            if c in self:
                self.uow._remove_deleted(c._state)
                self._unattach(c)

    def save(self, instance, entity_name=None):
        """Add a transient (unsaved) instance to this ``Session``.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.

        The `entity_name` keyword argument will further qualify the
        specific ``Mapper`` used to handle this instance.
        """
        self._save_impl(instance, entity_name=entity_name)
        self._cascade_save_or_update(instance)

    def update(self, instance, entity_name=None):
        """Bring the given detached (saved) instance into this
        ``Session``.

        If there is a persistent instance with the same instance key, but
        different identity already associated with this ``Session``, an
        InvalidRequestError exception is thrown.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.
        """

        self._update_impl(instance, entity_name=entity_name)
        self._cascade_save_or_update(instance)

    def save_or_update(self, instance, entity_name=None):
        """Save or update the given instance into this ``Session``.

        The presence of an `_instance_key` attribute on the instance
        determines whether to ``save()`` or ``update()`` the instance.
        """

        self._save_or_update_impl(instance, entity_name=entity_name)
        self._cascade_save_or_update(instance)

    def add(self, instance, entity_name=None):
        """Add the given instance into this ``Session``.

        This provides forwards compatibility with 0.5.

        """
        self.save_or_update(instance, entity_name)

    def add_all(self, instances):
        """Add the given collection of instances to this ``Session``.
        
        This provides forwards compatibility with 0.5.
        """

        for instance in instances:
            self.add(instance)

    def _cascade_save_or_update(self, instance):
        for obj, mapper in _cascade_iterator('save-update', instance, halt_on=lambda c:c in self):
            self._save_or_update_impl(obj, mapper.entity_name)

    def delete(self, instance):
        """Mark the given instance as deleted.

        The delete operation occurs upon ``flush()``.
        """

        self._delete_impl(instance)
        for c, m in _cascade_iterator('delete', instance):
            self._delete_impl(c, ignore_transient=True)


    def merge(self, instance, entity_name=None, dont_load=False, _recursive=None):
        """Copy the state of the given `instance` onto the persistent
        instance with the same identifier.

        If there is no persistent instance currently associated with
        the session, it will be loaded.  Return the persistent
        instance. If the given instance is unsaved, save a copy of and
        return it as a newly persistent instance. The given instance
        does not become associated with the session.

        This operation cascades to associated instances if the
        association is mapped with ``cascade="merge"``.
        """

        if _recursive is None:
            _recursive = {}  # TODO: this should be an IdentityDict for instances, but will need a separate
                             # dict for PropertyLoader tuples
        if entity_name is not None:
            mapper = _class_mapper(instance.__class__, entity_name=entity_name)
        else:
            mapper = _object_mapper(instance)
        if instance in _recursive:
            return _recursive[instance]

        key = getattr(instance, '_instance_key', None)
        if key is None:
            if dont_load:
                raise exceptions.InvalidRequestError("merge() with dont_load=True option does not support objects transient (i.e. unpersisted) objects.  flush() all changes on mapped instances before merging with dont_load=True.")
            key = mapper.identity_key_from_instance(instance)

        merged = None
        if key:
            if key in self.identity_map:
                merged = self.identity_map[key]
            elif dont_load:
                if instance._state.modified:
                    raise exceptions.InvalidRequestError("merge() with dont_load=True option does not support objects marked as 'dirty'.  flush() all changes on mapped instances before merging with dont_load=True.")

                merged = attributes.new_instance(mapper.class_)
                merged._instance_key = key
                merged._entity_name = entity_name
                self._update_impl(merged, entity_name=mapper.entity_name)
            else:
                merged = self.get(mapper.class_, key[1])
        
        if merged is None:
            merged = attributes.new_instance(mapper.class_)
            self.save(merged, entity_name=mapper.entity_name)
            
        _recursive[instance] = merged
        
        for prop in mapper.iterate_properties:
            prop.merge(self, instance, merged, dont_load, _recursive)
            
        if dont_load:
            merged._state.commit_all()  # remove any history

        return merged

    def identity_key(cls, *args, **kwargs):
        """Get an identity key.

        Valid call signatures:

        * ``identity_key(class, ident, entity_name=None)``

          class
              mapped class (must be a positional argument)

          ident
              primary key, if the key is composite this is a tuple

          entity_name
              optional entity name

        * ``identity_key(instance=instance)``

          instance
              object instance (must be given as a keyword arg)

        * ``identity_key(class, row=row, entity_name=None)``

          class
              mapped class (must be a positional argument)

          row
              result proxy row (must be given as a keyword arg)

          entity_name
              optional entity name (must be given as a keyword arg)
        """

        if args:
            if len(args) == 1:
                class_ = args[0]
                try:
                    row = kwargs.pop("row")
                except KeyError:
                    ident = kwargs.pop("ident")
                entity_name = kwargs.pop("entity_name", None)
            elif len(args) == 2:
                class_, ident = args
                entity_name = kwargs.pop("entity_name", None)
            elif len(args) == 3:
                class_, ident, entity_name = args
            else:
                raise exceptions.ArgumentError("expected up to three "
                    "positional arguments, got %s" % len(args))
            if kwargs:
                raise exceptions.ArgumentError("unknown keyword arguments: %s"
                    % ", ".join(kwargs.keys()))
            mapper = _class_mapper(class_, entity_name=entity_name)
            if "ident" in locals():
                return mapper.identity_key_from_primary_key(ident)
            return mapper.identity_key_from_row(row)
        instance = kwargs.pop("instance")
        if kwargs:
            raise exceptions.ArgumentError("unknown keyword arguments: %s"
                % ", ".join(kwargs.keys()))
        mapper = _object_mapper(instance)
        return mapper.identity_key_from_instance(instance)
    identity_key = classmethod(identity_key)

    def object_session(cls, instance):
        """Return the ``Session`` to which the given object belongs."""

        return object_session(instance)
    object_session = classmethod(object_session)

    def _save_impl(self, instance, **kwargs):
        if hasattr(instance, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is already persistent" % mapperutil.instance_str(instance))
        else:
            # TODO: consolidate the steps here
            attributes.manage(instance)
            instance._entity_name = kwargs.get('entity_name', None)
            self._attach(instance)
            self.uow.register_new(instance)

    def _update_impl(self, instance, **kwargs):
        if instance in self and instance not in self.deleted:
            return
        if not hasattr(instance, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % mapperutil.instance_str(instance))
        elif self.identity_map.get(instance._instance_key, instance) is not instance:
            raise exceptions.InvalidRequestError("Could not update instance '%s', identity key %s; a different instance with the same identity key already exists in this session." % (mapperutil.instance_str(instance), instance._instance_key))
        self._attach(instance)

    def _save_or_update_impl(self, instance, entity_name=None):
        key = getattr(instance, '_instance_key', None)
        if key is None:
            self._save_impl(instance, entity_name=entity_name)
        else:
            self._update_impl(instance, entity_name=entity_name)

    def _delete_impl(self, instance, ignore_transient=False):
        if instance in self and instance in self.deleted:
            return
        if not hasattr(instance, '_instance_key'):
            if ignore_transient:
                return
            else:
                raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % mapperutil.instance_str(instance))
        if self.identity_map.get(instance._instance_key, instance) is not instance:
            raise exceptions.InvalidRequestError("Instance '%s' is with key %s already persisted with a different identity" % (mapperutil.instance_str(instance), instance._instance_key))
        self._attach(instance)
        self.uow.register_deleted(instance)

    def _attach(self, instance):
        old_id = getattr(instance, '_sa_session_id', None)
        if old_id != self.hash_key:
            if old_id is not None and old_id in _sessions and instance in _sessions[old_id]:
                raise exceptions.InvalidRequestError("Object '%s' is already attached "
                                                     "to session '%s' (this is '%s')" %
                                                     (mapperutil.instance_str(instance), old_id, id(self)))

            key = getattr(instance, '_instance_key', None)
            if key is not None:
                self.identity_map[key] = instance
            instance._sa_session_id = self.hash_key

            if self.extension is not None:
                self.extension.after_attach(self, instance)

    def _unattach(self, instance):
        if instance._sa_session_id == self.hash_key:
            del instance._sa_session_id

    def _validate_persistent(self, instance):
        """Validate that the given instance is persistent within this
        ``Session``.
        """

        if instance not in self:
            raise exceptions.InvalidRequestError("Instance '%s' is not persistent within this Session" % mapperutil.instance_str(instance))

    def __contains__(self, instance):
        """Return True if the given instance is associated with this session.

        The instance may be pending or persistent within the Session for a
        result of True.
        """

        return instance._state in self.uow.new or (hasattr(instance, '_instance_key') and self.identity_map.get(instance._instance_key) is instance)

    def __iter__(self):
        """Return an iterator of all instances which are pending or persistent within this Session."""

        return iter(list(self.uow.new.values()) + self.uow.identity_map.values())

    def is_modified(self, instance, include_collections=True, passive=False):
        """Return True if the given instance has modified attributes.

        This method retrieves a history instance for each instrumented attribute
        on the instance and performs a comparison of the current value to its
        previously committed value.  Note that instances present in the 'dirty'
        collection may result in a value of ``False`` when tested with this method.

        `include_collections` indicates if multivalued collections should be included
        in the operation.  Setting this to False is a way to detect only local-column
        based properties (i.e. scalar columns or many-to-one foreign keys) that would
        result in an UPDATE for this instance upon flush.

        The `passive` flag indicates if unloaded attributes and collections should
        not be loaded in the course of performing this test.
        """

        for attr in attributes._managed_attributes(instance.__class__):
            if not include_collections and hasattr(attr.impl, 'get_collection'):
                continue
            (added, unchanged, deleted) = attr.get_history(instance)
            if added or deleted:
                return True
        return False
    
    def is_active(self):
        """return True if this Session has an active transaction."""
        
        return self.transaction and self.transaction.is_active
    is_active = property(is_active)
    
    def dirty(self):
        """Return a ``Set`` of all instances marked as 'dirty' within this ``Session``.

        Note that the 'dirty' state here is 'optimistic'; most attribute-setting or collection
        modification operations will mark an instance as 'dirty' and place it in this set,
        even if there is no net change to the attribute's value.  At flush time, the value
        of each attribute is compared to its previously saved value,
        and if there's no net change, no SQL operation will occur (this is a more expensive
        operation so it's only done at flush time).

        To check if an instance has actionable net changes to its attributes, use the
        is_modified() method.
        """

        return self.uow.locate_dirty()
    dirty = property(dirty)

    def deleted(self):
        "Return a ``Set`` of all instances marked as 'deleted' within this ``Session``"
        
        return util.IdentitySet(self.uow.deleted.values())
    deleted = property(deleted)

    def new(self):
        "Return a ``Set`` of all instances marked as 'new' within this ``Session``."
        
        return util.IdentitySet(self.uow.new.values())
    new = property(new)

def _expire_state(state, attribute_names):
    """Standalone expire instance function.

    Installs a callable with the given instance's _state
    which will fire off when any of the named attributes are accessed;
    their existing value is removed.

    If the list is None or blank, the entire instance is expired.
    """

    state.expire_attributes(attribute_names)

register_attribute = unitofwork.register_attribute

_sessions = weakref.WeakValueDictionary()

def _cascade_iterator(cascade, instance, **kwargs):
    mapper = _object_mapper(instance)
    for (o, m) in mapper.cascade_iterator(cascade, instance._state, **kwargs):
        yield o, m

def object_session(instance):
    """Return the ``Session`` to which the given instance is bound, or ``None`` if none."""

    hashkey = getattr(instance, '_sa_session_id', None)
    if hashkey is not None:
        sess = _sessions.get(hashkey)
        if sess is not None and instance in sess:
            return sess
    return None

# Lazy initialization to avoid circular imports
unitofwork.object_session = object_session
from sqlalchemy.orm import mapper
mapper._expire_state = _expire_state

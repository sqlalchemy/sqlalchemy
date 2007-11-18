# objectstore.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

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
    arguments, uses the
    keyword arguments configured here as its constructor arguments.  It is intended
    that the `sessionmaker()` function be called within the global scope of an application,
    and the returned class be made available to the rest of the application as the 
    single class used to instantiate sessions.
    
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
    """an extension hook object for Sessions.  Subclasses may be installed into a Session
    (or sessionmaker) using the ``extension`` keyword argument.
    """
    
    def before_commit(self, session):
        """execute right before commit is called.
        
        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def after_commit(self, session):
        """execute after a commit has occured.
        
        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def after_rollback(self, session):
        """execute after a rollback has occured.

        Note that this may not be per-flush if a longer running transaction is ongoing."""

    def before_flush(self, session, flush_context, objects):
        """execute before flush process has started.
        
        'objects' is an optional list of objects which were passed to the ``flush()``
        method.
        """

    def after_flush(self, session, flush_context):
        """execute after flush has completed, but before commit has been called.
        
        Note that the session's state is still in pre-flush, i.e. 'new', 'dirty',
        and 'deleted' lists still show pre-flush state as well as the history
        settings on instance attributes."""
        
    def after_flush_postexec(self, session, flush_context):
        """execute after flush has completed, and after the post-exec state occurs.
        
        this will be when the 'new', 'dirty', and 'deleted' lists are in their final 
        state.  An actual commit() may or may not have occured, depending on whether or not
        the flush started its own transaction or participated in a larger transaction.
        """
        
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
        self.__connections = {}
        self.__parent = parent
        self.autoflush = autoflush
        self.nested = nested

    def connection(self, bindkey, **kwargs):
        engine = self.session.get_bind(bindkey, **kwargs)
        return self.get_or_add(engine)

    def _begin(self, **kwargs):
        return SessionTransaction(self.session, self, **kwargs)

    def add(self, bind):
        if self.__parent is not None:
            return self.__parent.add(bind)
            
        if bind.engine in self.__connections:
            raise exceptions.InvalidRequestError("Session already has a Connection associated for the given %sEngine" % (isinstance(bind, engine.Connection) and "Connection's " or ""))
        return self.get_or_add(bind)

    def _connection_dict(self):
        if self.__parent is not None and not self.nested:
            return self.__parent._connection_dict()
        else:
            return self.__connections
            
    def get_or_add(self, bind):
        if self.__parent is not None:
            if not self.nested:
                return self.__parent.get_or_add(bind)
            
            if bind in self.__connections:
                return self.__connections[bind][0]

            if bind in self.__parent._connection_dict():
                (conn, trans, autoclose) = self.__parent.__connections[bind]
                self.__connections[conn] = self.__connections[bind.engine] = (conn, conn.begin_nested(), autoclose)
                return conn
        elif bind in self.__connections:
            return self.__connections[bind][0]
            
        if not isinstance(bind, engine.Connection):
            e = bind
            c = bind.contextual_connect()
        else:
            e = bind.engine
            c = bind
            if e in self.__connections:
                raise exceptions.InvalidRequestError("Session already has a Connection associated for the given Connection's Engine")
        if self.nested:
            trans = c.begin_nested()
        elif self.session.twophase:
            trans = c.begin_twophase()
        else:
            trans = c.begin()
        self.__connections[c] = self.__connections[e] = (c, trans, c is not bind)
        return self.__connections[c][0]

    def commit(self):
        if self.__parent is not None and not self.nested:
            return self.__parent

        if self.session.extension is not None:
            self.session.extension.before_commit(self.session)
            
        if self.autoflush:
            self.session.flush()

        if self.session.twophase:
            for t in util.Set(self.__connections.values()):
                t[1].prepare()

        for t in util.Set(self.__connections.values()):
            t[1].commit()

        if self.session.extension is not None:
            self.session.extension.after_commit(self.session)

        self.close()
        return self.__parent

    def rollback(self):
        if self.__parent is not None and not self.nested:
            return self.__parent.rollback()
        
        for t in util.Set(self.__connections.values()):
            t[1].rollback()

        if self.session.extension is not None:
            self.session.extension.after_rollback(self.session)

        self.close()
        return self.__parent
        
    def close(self):
        if self.__parent is not None:
            return
        for t in util.Set(self.__connections.values()):
            if t[2]:
                t[0].close()
            else:
                t[1].close()
        self.session.transaction = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.session.transaction is None:
            return
        if type is None:
            self.commit()
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

            autoflush
                when ``True``, all query operations will issue a ``flush()`` call to this
                ``Session`` before proceeding. This is a convenience feature so that
                ``flush()`` need not be called repeatedly in order for database queries to
                retrieve results. It's typical that ``autoflush`` is used in conjunction with
                ``transactional=True``, so that ``flush()`` is never called; you just call
                ``commit()`` when changes are complete to finalize all changes to the
                database.
        
            bind
                an optional ``Engine`` or ``Connection`` to which this ``Session`` should be
                bound. When specified, all SQL operations performed by this session will
                execute via this connectable.
                
            binds
                an optional dictionary, which contains more granular "bind" information than
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
                an optional [sqlalchemy.orm.session#SessionExtension] instance, which will receive
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
                when ``True``, all transactions will be started using
                [sqlalchemy.engine_TwoPhaseTransaction]. During a ``commit()``, after
                ``flush()`` has been issued for all attached databaes, the ``prepare()``
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
        """begin a 'nested' transaction on this Session.
        
        this utilizes a SAVEPOINT transaction for databases 
        which support this feature.
        """
        return self.begin(nested=True)
    
    def rollback(self):
        """rollback the current transaction in progress.
        
        If no transaction is in progress, this method is a 
        pass-thru.
        """
        
        if self.transaction is None:
            pass
        else:
            self.transaction = self.transaction.rollback()
        # TODO: we can rollback attribute values.  however
        # we would want to expand attributes.py to be able to save *two* rollback points, one to the 
        # last flush() and the other to when the object first entered the transaction.
        # [ticket:705]
        #attribute_manager.rollback(*self.identity_map.values())
        if self.transaction is None and self.transactional:
            self.begin()
            
    def commit(self):
        """commit the current transaction in progress.
        
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
                self.transaction = self.transaction.commit()
            else:
                raise exceptions.InvalidRequestError("No transaction is begun.")
        else:
            self.transaction = self.transaction.commit()
        if self.transaction is None and self.transactional:
            self.begin()
    
    def connection(self, mapper=None, **kwargs):
        """Return a ``Connection`` corresponding to this session's
        transactional context, if any.

        If this ``Session`` is transactional, the connection will be in
        the context of this session's transaction.  Otherwise, the
        connection is returned by the ``contextual_connect()`` method
        on the engine.
        
        the "mapper" argument is a class or mapper to which a bound engine
        will be located; use this when the Session itself is either bound
        to multiple engines or connections, or is not bound to any connectable.
        
        \**kwargs are additional arguments which will be passed to get_bind().
        See the get_bind() method for details.  Note that the "ShardedSession"
        subclass takes a different get_bind() argument signature.
        """

        return self.__connection(self.get_bind(mapper, **kwargs))

    def __connection(self, engine, **kwargs):
        if self.transaction is not None:
            return self.transaction.get_or_add(engine)
        else:
            return engine.contextual_connect(**kwargs)
        
    def execute(self, clause, params=None, mapper=None, **kwargs):
        """Using the given mapper to identify the appropriate ``Engine``
        or ``Connection`` to be used for statement execution, execute the
        given ``ClauseElement`` using the provided parameter dictionary.

        Return a ``ResultProxy`` corresponding to the execution's results.

        If this method allocates a new ``Connection`` for the operation,
        then the ``ResultProxy`` 's ``close()`` method will release the
        resources of the underlying ``Connection``.
        """

        engine = self.get_bind(mapper, clause=clause, **kwargs)
        
        return self.__connection(engine, close_with_result=True).execute(clause, params or {})

    def scalar(self, clause, params=None, mapper=None, **kwargs):
        """Like execute() but return a scalar result."""

        engine = self.get_bind(mapper, clause=clause)
        
        return self.__connection(engine, close_with_result=True).scalar(clause, params or {}, **kwargs)

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
            self.transaction.close()
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

    def get_bind(self, mapper, clause=None, **kwargs):
        """return an engine corresponding to the given arguments.
        
            mapper
                mapper relative to the desired operation
            
            clause
                a ClauseElement which is to be executed.  if
                mapper is not present, this may be used to locate
                Table objects, which are then associated with mappers
                which have associated binds.
                
            \**kwargs
                Subclasses (i.e. ShardedSession) may add additional arguments 
                to get_bind() which are passed through here.
        """
        
        if mapper is None and clause is None:
            if self.bind is not None:
                return self.bind
            else:
                raise exceptions.InvalidRequestError("This session is unbound to any Engine or Connection; specify a mapper to get_bind()")
                
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
        elif mapper is None:
            raise exceptions.InvalidRequestError("Could not locate any mapper associated with SQL expression")
        else:
            if isinstance(mapper, type):
                mapper = _class_mapper(mapper)
            else:
                mapper = mapper.compile()
            e = mapper.mapped_table.bind
            if e is None:
                raise exceptions.InvalidRequestError("Could not locate any Engine or Connection bound to mapper '%s'" % str(mapper))
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

    def _sql(self):
        class SQLProxy(object):
            def __getattr__(self, key):
                def call(*args, **kwargs):
                    kwargs[engine] = self.engine
                    return getattr(sql, key)(*args, **kwargs)

    sql = property(_sql)

    def _autoflush(self):
        if self.autoflush and (self.transaction is None or self.transaction.autoflush):
            self.flush()

    def flush(self, objects=None):
        """Flush all the object modifications present in this session
        to the database.

        `objects` is a list or tuple of objects specifically to be
        flushed; if ``None``, all new and modified objects are flushed.
        """

        self.uow.flush(self, objects)

    def get(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier, or ``None`` if not found.

        The `ident` argument is a scalar or tuple of primary key
        column values in the order of the table def's primary key
        columns.

        The `entity_name` keyword argument may also be specified which
        further qualifies the underlying Mapper used to perform the
        query.
        """

        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).get(ident, **kwargs)

    def load(self, class_, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier.

        If not found, raises an exception.  The method will **remove
        all pending changes** to the object already existing in the
        ``Session``.  The `ident` argument is a scalar or tuple of primary
        key columns in the order of the table def's primary key
        columns.

        The `entity_name` keyword argument may also be specified which
        further qualifies the underlying ``Mapper`` used to perform the
        query.
        """

        entity_name = kwargs.pop('entity_name', None)
        return self.query(class_, entity_name=entity_name).load(ident, **kwargs)

    def refresh(self, obj, attribute_names=None):
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

        self._validate_persistent(obj)
            
        if self.query(obj.__class__)._get(obj._instance_key, refresh_instance=obj, only_load_props=attribute_names) is None:
            raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % mapperutil.instance_str(obj))

    def expire(self, obj, attribute_names=None):
        """Expire the attributes on the given instance.
        
        The instance's attributes are instrumented such that
        when an attribute is next accessed, a query will be issued
        to the database which will refresh all attributes with their
        current value.  
        
        Lazy-loaded relational attributes will remain lazily loaded, so that 
        triggering one will incur the instance-wide refresh operation, followed
        immediately by the lazy load of that attribute.
        
        Eagerly-loaded relational attributes will eagerly load within the
        single refresh operation.
        
        The ``attribute_names`` argument is an iterable collection
        of attribute names indicating a subset of attributes to be 
        expired.
        """
        
        if attribute_names:
            self._validate_persistent(obj)
            expire_instance(obj, attribute_names=attribute_names)
        else:
            for c in [obj] + list(_object_mapper(obj).cascade_iterator('refresh-expire', obj)):
                self._validate_persistent(obj)
                expire_instance(c, None)

    def prune(self):
        """Removes unreferenced instances cached in the identity map.

        Note that this method is only meaningful if "weak_identity_map"
        is set to False.
        
        Removes any object in this Session's identity map that is not
        referenced in user code, modified, new or scheduled for deletion.
        Returns the number of objects pruned.
        """

        return self.uow.prune_identity_map()

    def expunge(self, object):
        """Remove the given `object` from this ``Session``.

        This will free all internal references to the object.
        Cascading will be applied according to the *expunge* cascade
        rule.
        """
        self._validate_persistent(object)
        for c in [object] + list(_object_mapper(object).cascade_iterator('expunge', object)):
            if c in self:
                self.uow._remove_deleted(c)
                self._unattach(c)

    def save(self, object, entity_name=None):
        """Add a transient (unsaved) instance to this ``Session``.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.

        The `entity_name` keyword argument will further qualify the
        specific ``Mapper`` used to handle this instance.
        """

        self._save_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def update(self, object, entity_name=None):
        """Bring the given detached (saved) instance into this
        ``Session``.

        If there is a persistent instance with the same instance key, but 
        different identity already associated with this ``Session``, an
        InvalidRequestError exception is thrown.

        This operation cascades the `save_or_update` method to
        associated instances if the relation is mapped with
        ``cascade="save-update"``.
        """

        self._update_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def save_or_update(self, object, entity_name=None):
        """Save or update the given object into this ``Session``.

        The presence of an `_instance_key` attribute on the instance
        determines whether to ``save()`` or ``update()`` the instance.
        """

        self._save_or_update_impl(object, entity_name=entity_name)
        _object_mapper(object).cascade_callable('save-update', object,
                                                lambda c, e:self._save_or_update_impl(c, e),
                                                halt_on=lambda c:c in self)

    def delete(self, object):
        """Mark the given instance as deleted.

        The delete operation occurs upon ``flush()``.
        """

        self._delete_impl(object)
        for c in list(_object_mapper(object).cascade_iterator('delete', object)):
            self._delete_impl(c, ignore_transient=True)


    def merge(self, object, entity_name=None, dont_load=False, _recursive=None):
        """Copy the state of the given `object` onto the persistent
        object with the same identifier.

        If there is no persistent instance currently associated with
        the session, it will be loaded.  Return the persistent
        instance. If the given instance is unsaved, save a copy of and
        return it as a newly persistent instance. The given instance
        does not become associated with the session.

        This operation cascades to associated instances if the
        association is mapped with ``cascade="merge"``.
        """

        if _recursive is None:
            _recursive = util.Set()
        if entity_name is not None:
            mapper = _class_mapper(object.__class__, entity_name=entity_name)
        else:
            mapper = _object_mapper(object)
        if mapper in _recursive or object in _recursive:
            return None
        _recursive.add(mapper)
        _recursive.add(object)
        try:
            key = getattr(object, '_instance_key', None)
            if key is None:
                merged = attribute_manager.new_instance(mapper.class_)
            else:
                if key in self.identity_map:
                    merged = self.identity_map[key]
                elif dont_load:
                    merged = attribute_manager.new_instance(mapper.class_)
                    merged._instance_key = key
                    self.update(merged, entity_name=mapper.entity_name)
                    merged._state.committed_state = object._state.committed_state.copy()
                else:
                    merged = self.get(mapper.class_, key[1])
                    if merged is None:
                        raise exceptions.AssertionError("Instance %s has an instance key but is not persisted" % mapperutil.instance_str(object))
            for prop in mapper.iterate_properties:
                prop.merge(self, object, merged, dont_load, _recursive)
            if dont_load:
                merged._state.modified = object._state.modified
            if key is None:
                self.save(merged, entity_name=mapper.entity_name)
            return merged
        finally:
            _recursive.remove(mapper)
            
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
    
    def object_session(cls, obj):
        """return the ``Session`` to which the given object belongs."""
        
        return object_session(obj)
    object_session = classmethod(object_session)
    
    def _save_impl(self, obj, **kwargs):
        if hasattr(obj, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is already persistent" % mapperutil.instance_str(obj))
        else:
            # TODO: consolidate the steps here
            attribute_manager.manage(obj)
            obj._entity_name = kwargs.get('entity_name', None)
            self._attach(obj)
            self.uow.register_new(obj)

    def _update_impl(self, obj, **kwargs):
        if obj in self and obj not in self.deleted:
            return
        if not hasattr(obj, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % mapperutil.instance_str(obj))
        elif self.identity_map.get(obj._instance_key, obj) is not obj:
            raise exceptions.InvalidRequestError("Instance '%s' is with key %s already persisted with a different identity" % (mapperutil.instance_str(obj), obj._instance_key))
        self._attach(obj)

    def _save_or_update_impl(self, object, entity_name=None):
        key = getattr(object, '_instance_key', None)
        if key is None:
            self._save_impl(object, entity_name=entity_name)
        else:
            self._update_impl(object, entity_name=entity_name)

    def _delete_impl(self, obj, ignore_transient=False):
        if obj in self and obj in self.deleted:
            return
        if not hasattr(obj, '_instance_key'):
            if ignore_transient:
                return
            else:
                raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % mapperutil.instance_str(obj))
        if self.identity_map.get(obj._instance_key, obj) is not obj:
            raise exceptions.InvalidRequestError("Instance '%s' is with key %s already persisted with a different identity" % (mapperutil.instance_str(obj), obj._instance_key))
        self._attach(obj)
        self.uow.register_deleted(obj)

    def _register_persistent(self, obj):
        obj._sa_session_id = self.hash_key
        self.identity_map[obj._instance_key] = obj
        obj._state.commit_all()

    def _attach(self, obj):
        old_id = getattr(obj, '_sa_session_id', None)
        if old_id != self.hash_key:
            if old_id is not None and old_id in _sessions and obj in _sessions[old_id]:
                raise exceptions.InvalidRequestError("Object '%s' is already attached "
                                                     "to session '%s' (this is '%s')" %
                                                     (mapperutil.instance_str(obj), old_id, id(self)))

            key = getattr(obj, '_instance_key', None)
            if key is not None:
                self.identity_map[key] = obj
            obj._sa_session_id = self.hash_key
        
    def _unattach(self, obj):
        if obj._sa_session_id == self.hash_key:
            del obj._sa_session_id

    def _validate_persistent(self, obj):
        """Validate that the given object is persistent within this
        ``Session``.
        """
        
        return obj in self

    def __contains__(self, obj):
        """return True if the given object is associated with this session.
        
        The instance may be pending or persistent within the Session for a
        result of True.
        """
        
        return obj in self.uow.new or (hasattr(obj, '_instance_key') and self.identity_map.get(obj._instance_key) is obj)

    def __iter__(self):
        """return an iterator of all objects which are pending or persistent within this Session."""
        
        return iter(list(self.uow.new) + self.uow.identity_map.values())

    def is_modified(self, obj, include_collections=True, passive=False):
        """return True if the given object has modified attributes.
        
        This method retrieves a history instance for each instrumented attribute
        on the instance and performs a comparison of the current value to its
        previously committed value.  Note that instances present in the 'dirty'
        collection may result in a value of ``False`` when tested with this method.
        
        'include_collections' indicates if multivalued collections should be included
        in the operation.  Setting this to False is a way to detect only local-column
        based properties (i.e. scalar columns or many-to-one foreign keys) that would
        result in an UPDATE for this instance upon flush.
        
        the 'passive' flag indicates if unloaded attributes and collections should
        not be loaded in the course of performing this test.
        """

        for attr in attribute_manager.managed_attributes(obj.__class__):
            if not include_collections and hasattr(attr.impl, 'get_collection'):
                continue
            if attr.get_history(obj).is_modified():
                return True
        return False
        
    dirty = property(lambda s:s.uow.locate_dirty(),
                     doc="""A ``Set`` of all objects marked as 'dirty' within this ``Session``.  
                     
                     Note that the 'dirty' state here is 'optimistic'; most attribute-setting or collection
                     modification operations will mark an instance as 'dirty' and place it in this set,
                     even if there is no net change to the attribute's value.  At flush time, the value 
                     of each attribute is compared to its previously saved value,
                     and if there's no net change, no SQL operation will occur (this is a more expensive
                     operation so it's only done at flush time).
                     
                     To check if an instance has actionable net changes to its attributes, use the
                     is_modified() method.
                     """)

    deleted = property(lambda s:s.uow.deleted,
                       doc="A ``Set`` of all objects marked as 'deleted' within this ``Session``")

    new = property(lambda s:s.uow.new,
                   doc="A ``Set`` of all objects marked as 'new' within this ``Session``.")

def expire_instance(obj, attribute_names):
    """standalone expire instance function. 
    
    installs a callable with the given instance's _state
    which will fire off when any of the named attributes are accessed;
    their existing value is removed.
    
    If the list is None or blank, the entire instance is expired.
    """
    
    if obj._state.trigger is None:
        def load_attributes(instance, attribute_names):
            if object_session(instance).query(instance.__class__)._get(instance._instance_key, refresh_instance=instance, only_load_props=attribute_names) is None:
                raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % mapperutil.instance_str(instance))
        obj._state.trigger = load_attributes
        
    obj._state.expire_attributes(attribute_names)
    


# this is the AttributeManager instance used to provide attribute behavior on objects.
# to all the "global variable police" out there:  its a stateless object.
attribute_manager = unitofwork.attribute_manager

# this dictionary maps the hash key of a Session to the Session itself, and
# acts as a Registry with which to locate Sessions.  this is to enable
# object instances to be associated with Sessions without having to attach the
# actual Session object directly to the object instance.
_sessions = weakref.WeakValueDictionary()

def object_session(obj):
    """Return the ``Session`` to which the given object is bound, or ``None`` if none."""

    hashkey = getattr(obj, '_sa_session_id', None)
    if hashkey is not None:
        sess = _sessions.get(hashkey)
        if obj in sess:
            return sess
    return None

# Lazy initialization to avoid circular imports
unitofwork.object_session = object_session
from sqlalchemy.orm import mapper
mapper.attribute_manager = attribute_manager
mapper.expire_instance = expire_instance
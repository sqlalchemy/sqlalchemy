# objectstore.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util, exceptions, sql
import unitofwork, query
import weakref
import sqlalchemy

class SessionTransaction(object):
    def __init__(self, session, parent=None, autoflush=True):
        self.session = session
        self.connections = {}
        self.parent = parent
        self.autoflush = autoflush
    def connection(self, mapper_or_class, entity_name=None):
        if isinstance(mapper_or_class, type):
            mapper_or_class = class_mapper(mapper_or_class, entity_name=entity_name)
        if self.parent is not None:
            return self.parent.connection(mapper_or_class)
        engine = self.session.get_bind(mapper_or_class)
        return self.get_or_add(engine)
    def _begin(self):
        return SessionTransaction(self.session, self)
    def add(self, connectable):
        if self.connections.has_key(connectable.engine):
            raise exceptions.InvalidRequestError("Session already has a Connection associated for the given Connection's Engine")
        return self.get_or_add(connectable)
    def get_or_add(self, connectable):
        # we reference the 'engine' attribute on the given object, which in the case of 
        # Connection, ProxyEngine, Engine, ComposedSQLEngine, whatever, should return the original
        # "Engine" object that is handling the connection.
        if self.connections.has_key(connectable.engine):
            return self.connections[connectable.engine][0]
        e = connectable.engine
        c = connectable.contextual_connect()
        if not self.connections.has_key(e):
            self.connections[e] = (c, c.begin())
        return self.connections[e][0]
    def commit(self):
        if self.parent is not None:
            return
        if self.autoflush:
            self.session.flush()
        for t in self.connections.values():
            t[1].commit()
        self.close()
    def rollback(self):
        if self.parent is not None:
            self.parent.rollback()
            return
        for k, t in self.connections.iteritems():
            t[1].rollback()
        self.close()
    def close(self):
        if self.parent is not None:
            return
        for t in self.connections.values():
            t[0].close()
        self.session.transaction = None

class Session(object):
    """encapsulates a set of objects being operated upon within an object-relational operation."""
    def __init__(self, bind_to=None, hash_key=None, import_session=None, echo_uow=False):
        if import_session is not None:
            self.uow = unitofwork.UnitOfWork(identity_map=import_session.uow.identity_map)
        else:
            self.uow = unitofwork.UnitOfWork()
        
        self.bind_to = bind_to
        self.binds = {}
        self.echo_uow = echo_uow
        self.transaction = None
        if hash_key is None:
            self.hash_key = id(self)
        else:
            self.hash_key = hash_key
        _sessions[self.hash_key] = self

    def create_transaction(self, **kwargs):
        """returns a new SessionTransaction corresponding to an existing or new transaction.
        if the transaction is new, the returned SessionTransaction will have commit control
        over the underlying transaction, else will have rollback control only."""
        if self.transaction is not None:
            return self.transaction._begin()
        else:
            self.transaction = SessionTransaction(self, **kwargs)
            return self.transaction
    def connect(self, mapper=None, **kwargs):
        """returns a unique connection corresponding to the given mapper.  this connection
        will not be part of any pre-existing transactional context."""
        return self.get_bind(mapper).connect(**kwargs)
    def connection(self, mapper, **kwargs):
        """returns a Connection corresponding to the given mapper.  used by the execute()
        method which performs select operations for Mapper and Query.
        if this Session is transactional, 
        the connection will be in the context of this session's transaction.  otherwise, the connection
        is returned by the contextual_connect method, which some Engines override to return a thread-local
        connection, and will have close_with_result set to True.
        
        the given **kwargs will be sent to the engine's contextual_connect() method, if no transaction is in progress."""
        if self.transaction is not None:
            return self.transaction.connection(mapper)
        else:
            return self.get_bind(mapper).contextual_connect(**kwargs)
    def execute(self, mapper, clause, params, **kwargs):
        """using the given mapper to identify the appropriate Engine or Connection to be used for statement execution, 
        executes the given ClauseElement using the provided parameter dictionary.  Returns a ResultProxy corresponding
        to the execution's results.  If this method allocates a new Connection for the operation, then the ResultProxy's close() 
        method will release the resources of the underlying Connection, otherwise its a no-op.
        """
        return self.connection(mapper, close_with_result=True).execute(clause, params, **kwargs)
    def scalar(self, mapper, clause, params, **kwargs):
        """works like execute() but returns a scalar result."""
        return self.connection(mapper, close_with_result=True).scalar(clause, params, **kwargs)
        
    def close(self):
        """closes this Session.  
        """
        self.clear()
        if self.transaction is not None:
            self.transaction.close()

    def clear(self):
        """removes all object instances from this Session.  this is equivalent to calling expunge() for all
        objects in this Session."""
        for instance in self:
            self._unattach(instance)
        self.uow = unitofwork.UnitOfWork()
            
    def mapper(self, class_, entity_name=None):
        """given an Class, returns the primary Mapper responsible for persisting it"""
        return class_mapper(class_, entity_name = entity_name)
    def bind_mapper(self, mapper, bindto):
        """binds the given Mapper to the given Engine or Connection.  All subsequent operations involving this
        Mapper will use the given bindto."""
        self.binds[mapper] = bindto
    def bind_table(self, table, bindto):
        """binds the given Table to the given Engine or Connection.  All subsequent operations involving this
        Table will use the given bindto."""
        self.binds[table] = bindto
    def get_bind(self, mapper):
        """given a Mapper, returns the Engine or Connection which is used to execute statements on behalf of this 
        Mapper.  Calling connect() on the return result will always result in a Connection object.  This method 
        disregards any SessionTransaction that may be in progress.
        
        The order of searching is as follows:
        
        if an Engine or Connection was bound to this Mapper specifically within this Session, returns that 
        Engine or Connection.
        
        if an Engine or Connection was bound to this Mapper's underlying Table within this Session
        (i.e. not to the Table directly), returns that Engine or Conneciton.
        
        if an Engine or Connection was bound to this Session, returns that Engine or Connection.
        
        finally, returns the Engine which was bound directly to the Table's MetaData object.  
        
        If no Engine is bound to the Table, an exception is raised.
        """
        if mapper is None:
            return self.bind_to
        elif self.binds.has_key(mapper):
            return self.binds[mapper]
        elif self.binds.has_key(mapper.mapped_table):
            return self.binds[mapper.mapped_table]
        elif self.bind_to is not None:
            return self.bind_to
        else:
            e = mapper.mapped_table.engine
            if e is None:
                raise exceptions.InvalidRequestError("Could not locate any Engine bound to mapper '%s'" % str(mapper))
            return e
    def query(self, mapper_or_class, entity_name=None):
        """given a mapper or Class, returns a new Query object corresponding to this Session and the mapper, or the classes' primary mapper."""
        if isinstance(mapper_or_class, type):
            return query.Query(class_mapper(mapper_or_class, entity_name=entity_name), self)
        else:
            return query.Query(mapper_or_class, self)
    def _sql(self):
        class SQLProxy(object):
            def __getattr__(self, key):
                def call(*args, **kwargs):
                    kwargs[engine] = self.engine
                    return getattr(sql, key)(*args, **kwargs)
                    
    sql = property(_sql)
    
        
    def get_id_key(ident, class_, entity_name=None):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a tuple of the object's primary key values.

        ident - a tuple of primary key values corresponding to the object to be stored.  these
        values should be in the same order as the primary keys of the table 

        class_ - a reference to the object's class

        entity_name - optional string name to further qualify the class
        """
        return (class_, tuple(ident), entity_name)
    get_id_key = staticmethod(get_id_key)

    def get_row_key(row, class_, primary_key, entity_name=None):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a result set row.

        row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set
        column names to their values within a row.

        class_ - a reference to the object's class

        primary_key - a list of column objects that will target the primary key values
        in the given row.
        
        entity_name - optional string name to further qualify the class
        """
        return (class_, tuple([row[column] for column in primary_key]), entity_name)
    get_row_key = staticmethod(get_row_key)
    
    def begin(self, *obj):
        """deprecated"""
        raise exceptions.InvalidRequestError("Session.begin() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    
    def commit(self, *obj):
        """deprecated"""
        raise exceptions.InvalidRequestError("Session.commit() is deprecated.  use install_mod('legacy_session') to enable the old behavior")    

    def flush(self, objects=None):
        """flushes all the object modifications present in this session to the database.  'objects'
        is a list or tuple of objects specifically to be flushed."""
        self.uow.flush(self, objects, echo=self.echo_uow)

    def get(self, class_, ident, **kwargs):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The ident argument is a scalar or tuple of primary key column values in the order of the 
        table def's primary key columns.
        
        the entity_name keyword argument may also be specified which further qualifies the underlying
        Mapper used to perform the query."""
        entity_name = kwargs.get('entity_name', None)
        return self.query(class_, entity_name=entity_name).get(ident)
        
    def load(self, class_, ident, **kwargs):
        """returns an instance of the object based on the given identifier. If not found,
        raises an exception.  The method will *remove all pending changes* to the object
        already existing in the Session.  The ident argument is a scalar or tuple of
        primary key columns in the order of the table def's primary key columns.
        
        the entity_name keyword argument may also be specified which further qualifies the underlying
        Mapper used to perform the query."""
        entity_name = kwargs.get('entity_name', None)
        return self.query(class_, entity_name=entity_name).load(ident)
                
    def refresh(self, object):
        """reloads the attributes for the given object from the database, clears
        any changes made."""
        self.uow.refresh(self, object)

    def expire(self, object):
        """invalidates the data in the given object and sets them to refresh themselves
        the next time they are requested."""
        self.uow.expire(self, object)

    def expunge(self, object):
        """removes the given object from this Session.  this will free all internal references to the object."""
        self.uow.expunge(object)
        
    def save(self, object, entity_name=None):
        """
        Adds a transient (unsaved) instance to this Session.  This operation cascades the "save_or_update" 
        method to associated instances if the relation is mapped with cascade="save-update".        
        
        The 'entity_name' keyword argument will further qualify the specific Mapper used to handle this
        instance.
        """
        self._save_impl(object, entity_name=entity_name)
        object_mapper(object).cascade_callable('save-update', object, lambda c, e:self._save_or_update_impl(c, e))

    def update(self, object, entity_name=None):
        """Brings the given detached (saved) instance into this Session.
        If there is a persistent instance with the same identifier (i.e. a saved instance already associated with this
        Session), an exception is thrown. 
        This operation cascades the "save_or_update" method to associated instances if the relation is mapped 
        with cascade="save-update"."""
        self._update_impl(object, entity_name=entity_name)
        object_mapper(object).cascade_callable('save-update', object, lambda c, e:self._save_or_update_impl(c, e))

    def save_or_update(self, object, entity_name=None):
        self._save_or_update_impl(object, entity_name=entity_name)
        object_mapper(object).cascade_callable('save-update', object, lambda c, e:self._save_or_update_impl(c, e))
    
    def _save_or_update_impl(self, object, entity_name=None):
        key = getattr(object, '_instance_key', None)
        if key is None:
            self._save_impl(object, entity_name=entity_name)
        else:
            self._update_impl(object, entity_name=entity_name)
        
    def delete(self, object, entity_name=None):
        #self.uow.register_deleted(object)
        for c in [object] + list(object_mapper(object).cascade_iterator('delete', object)):
            self.uow.register_deleted(c)

    def merge(self, object, entity_name=None):
        instance = None
        for obj in [object] + list(object_mapper(object).cascade_iterator('merge', object)):
            key = getattr(obj, '_instance_key', None)
            if key is None:
                mapper = object_mapper(object)
                ident = mapper.identity(object)
                for k in ident:
                    if k is None:
                        raise exceptions.InvalidRequestError("Instance '%s' does not have a full set of identity values, and does not represent a saved entity in the database.  Use the add() method to add unsaved instances to this Session." % repr(obj))
                key = mapper.identity_key(ident)
            u = self.uow
            if u.identity_map.has_key(key):
                # TODO: copy the state of the given object into this one.  tricky !
                inst = u.identity_map[key]
            else:
                inst = self.get(object.__class__, *key[1])
            if obj is object:
                instance = inst
                
        return instance
                    
    def _save_impl(self, object, **kwargs):
        if hasattr(object, '_instance_key'):
            if not self.uow.has_key(object._instance_key):
                raise exceptions.InvalidRequestError("Instance '%s' is a detached instance or is already persistent in a different Session" % repr(object))
        else:
            m = class_mapper(object.__class__, entity_name=kwargs.get('entity_name', None))
            m._assign_entity_name(object)
            self._register_new(object)

    def _update_impl(self, object, **kwargs):
        if self._is_attached(object) and object not in self.deleted:
            return
        if not hasattr(object, '_instance_key'):
            raise exceptions.InvalidRequestError("Instance '%s' is not persisted" % repr(object))
        if attribute_manager.is_modified(object):
            self._register_dirty(object)
        else:
            self._register_clean(object)
    
    def _register_changed(self, obj):
        if hasattr(obj, '_instance_key'):
            self._register_dirty(obj)
        else:
            self._register_new(obj)
    def _register_new(self, obj):
        self._attach(obj)
        self.uow.register_new(obj)
    def _register_dirty(self, obj):
        self._attach(obj)
        self.uow.register_dirty(obj)
    def _register_clean(self, obj):
        self._attach(obj)
        self.uow.register_clean(obj)
    def _register_deleted(self, obj):
        self._attach(obj)
        self.uow.register_deleted(obj)
        
    def _attach(self, obj):
        """Attach the given object to this Session."""
        if getattr(obj, '_sa_session_id', None) != self.hash_key:
            old = getattr(obj, '_sa_session_id', None)
            if old is not None and _sessions.has_key(old):
                raise exceptions.InvalidRequestError("Object '%s' is already attached to session '%s' (this is '%s')" % (repr(obj), old, id(self)))
                
                # auto-removal from the old session is disabled.  but if we decide to 
                # turn it back on, do it as below: gingerly since _sessions is a WeakValueDict
                # and it might be affected by other threads
                #try:
                #    sess = _sessions[old]
                #except KeyError:
                #    sess = None
                #if sess is not None:
                #    sess.expunge(old)
            key = getattr(obj, '_instance_key', None)
            if key is not None:
                self.identity_map[key] = obj
            obj._sa_session_id = self.hash_key
            
    def _unattach(self, obj):
        if not self._is_attached(obj): #getattr(obj, '_sa_session_id', None) != self.hash_key:
            raise exceptions.InvalidRequestError("Object '%s' is not attached to this Session" % repr(obj))
        del obj._sa_session_id
        
    def _is_attached(self, obj):
        return getattr(obj, '_sa_session_id', None) == self.hash_key
    def __contains__(self, obj):
        return self._is_attached(obj) and (obj in self.uow.new or self.uow.has_key(obj._instance_key))
    def __iter__(self):
        return iter(list(self.uow.new) + self.uow.identity_map.values())
    def _get(self, key):
        return self.uow._get(key)
    def has_key(self, key):
        return self.uow.has_key(key)
    def is_expired(self, instance, **kwargs):
        return self.uow.is_expired(instance, **kwargs)
        
    dirty = property(lambda s:s.uow.dirty, doc="a Set of all objects marked as 'dirty' within this Session")
    deleted = property(lambda s:s.uow.deleted, doc="a Set of all objects marked as 'deleted' within this Session")
    new = property(lambda s:s.uow.new, doc="a Set of all objects marked as 'new' within this Session.")
    identity_map = property(lambda s:s.uow.identity_map, doc="a WeakValueDictionary consisting of all objects within this Session keyed to their _instance_key value.")
    
            
    def import_instance(self, *args, **kwargs):
        """deprecated; a synynom for merge()"""
        return self.merge(*args, **kwargs)

def get_id_key(ident, class_, entity_name=None):
    return Session.get_id_key(ident, class_, entity_name)

def get_row_key(row, class_, primary_key, entity_name=None):
    return Session.get_row_key(row, class_, primary_key, entity_name)

def object_mapper(obj):
    return sqlalchemy.orm.object_mapper(obj)

def class_mapper(class_, **kwargs):
    return sqlalchemy.orm.class_mapper(class_, **kwargs)

# this is the AttributeManager instance used to provide attribute behavior on objects.
# to all the "global variable police" out there:  its a stateless object.
attribute_manager = unitofwork.attribute_manager

# this dictionary maps the hash key of a Session to the Session itself, and 
# acts as a Registry with which to locate Sessions.  this is to enable
# object instances to be associated with Sessions without having to attach the
# actual Session object directly to the object instance.
_sessions = weakref.WeakValueDictionary()

def object_session(obj):
    hashkey = getattr(obj, '_sa_session_id', None)
    if hashkey is not None:
        return _sessions.get(hashkey)
    return None

unitofwork.object_session = object_session


def get_session(obj=None):
    """deprecated"""
    if obj is not None:
        return object_session(obj)
    raise exceptions.InvalidRequestError("get_session() is deprecated, and does not return the thread-local session anymore. Use the SessionContext.mapper_extension or import sqlalchemy.mod.threadlocal to establish a default thread-local context.")

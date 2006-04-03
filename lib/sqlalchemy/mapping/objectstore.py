# objectstore.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""provides the Session object and a function-oriented convenience interface.  This is the
"front-end" to the Unit of Work system in unitofwork.py.  Issues of "scope" are dealt with here,
primarily through an important function "get_session()", which is where mappers and units of work go to get a handle on the current threa-local context.  """

from sqlalchemy import util
from sqlalchemy.exceptions import *
import unitofwork
import weakref
import sqlalchemy
    
class Session(object):
    """Maintains a UnitOfWork instance, including transaction state."""
    
    def __init__(self, nest_on=None, hash_key=None):
        """Initialize the objectstore with a UnitOfWork registry.  If called
        with no arguments, creates a single UnitOfWork for all operations.
        
        nest_transactions - indicates begin/commit statements can be executed in a
        "nested", defaults to False which indicates "only commit on the outermost begin/commit"
        hash_key - the hash_key used to identify objects against this session, which 
        defaults to the id of the Session instance.
        """
        self.uow = unitofwork.UnitOfWork()
        self.parent_uow = None
        self.begin_count = 0
        self.nest_on = util.to_list(nest_on)
        self.__pushed_count = 0
        if hash_key is None:
            self.hash_key = id(self)
        else:
            self.hash_key = hash_key
        _sessions[self.hash_key] = self
    
    def was_pushed(self):
        if self.nest_on is None:
            return
        self.__pushed_count += 1
        if self.__pushed_count == 1:
            for n in self.nest_on:
                n.push_session()
    def was_popped(self):
        if self.nest_on is None or self.__pushed_count == 0:
            return
        self.__pushed_count -= 1
        if self.__pushed_count == 0:
            for n in self.nest_on:
                n.pop_session()
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

    class SessionTrans(object):
        """returned by Session.begin(), denotes a transactionalized UnitOfWork instance.
        call commit() on this to commit the transaction."""
        def __init__(self, parent, uow, isactive):
            self.__parent = parent
            self.__isactive = isactive
            self.__uow = uow
        isactive = property(lambda s:s.__isactive, doc="True if this SessionTrans is the 'active' transaction marker, else its a no-op.")
        parent = property(lambda s:s.__parent, doc="returns the parent Session of this SessionTrans object.")
        uow = property(lambda s:s.__uow, doc="returns the parent UnitOfWork corresponding to this transaction.")
        def begin(self):
            """calls begin() on the underlying Session object, returning a new no-op SessionTrans object."""
            if self.parent.uow is not self.uow:
                raise InvalidRequestError("This SessionTrans is no longer valid")
            return self.parent.begin()
        def commit(self):
            """commits the transaction noted by this SessionTrans object."""
            self.__parent._trans_commit(self)
            self.__isactive = False
        def rollback(self):
            """rolls back the current UnitOfWork transaction, in the case that begin()
            has been called.  The changes logged since the begin() call are discarded."""
            self.__parent._trans_rollback(self)
            self.__isactive = False

    def begin(self):
        """begins a new UnitOfWork transaction and returns a tranasaction-holding
        object.  commit() or rollback() should be called on the returned object.
        commit() on the Session will do nothing while a transaction is pending, and further
        calls to begin() will return no-op transactional objects."""
        if self.parent_uow is not None:
            return Session.SessionTrans(self, self.uow, False)
        self.parent_uow = self.uow
        self.uow = unitofwork.UnitOfWork(identity_map = self.uow.identity_map)
        return Session.SessionTrans(self, self.uow, True)
    
    def _trans_commit(self, trans):
        if trans.uow is self.uow and trans.isactive:
            try:
                self._commit_uow()
            finally:
                self.uow = self.parent_uow
                self.parent_uow = None
    def _trans_rollback(self, trans):
        if trans.uow is self.uow:
            self.uow = self.parent_uow
            self.parent_uow = None

    def _commit_uow(self, *obj):
        self.was_pushed()
        try:
            self.uow.commit(*obj)
        finally:
            self.was_popped()
                        
    def commit(self, *objects):
        """commits the current UnitOfWork transaction.  called with
        no arguments, this is only used
        for "implicit" transactions when there was no begin().
        if individual objects are submitted, then only those objects are committed, and the 
        begin/commit cycle is not affected."""
        # if an object list is given, commit just those but dont
        # change begin/commit status
        if len(objects):
            self._commit_uow(*objects)
            self.uow.commit(*objects)
            return
        if self.parent_uow is None:
            self._commit_uow()
            
    def refresh(self, *obj):
        """reloads the attributes for the given objects from the database, clears
        any changes made."""
        for o in obj:
            self.uow.refresh(o)

    def expire(self, *obj):
        """invalidates the data in the given objects and sets them to refresh themselves
        the next time they are requested."""
        for o in obj:
            self.uow.expire(o)

    def expunge(self, *obj):
        for o in obj:
            self.uow.expunge(obj)
            
    def register_clean(self, obj):
        self._bind_to(obj)
        self.uow.register_clean(obj)
        
    def register_new(self, obj):
        self._bind_to(obj)
        self.uow.register_new(obj)

    def _bind_to(self, obj):
        """given an object, binds it to this session.  changes on the object will affect
        the currently scoped UnitOfWork maintained by this session."""
        obj._sa_session_id = self.hash_key

    def __getattr__(self, key):
        """proxy other methods to our underlying UnitOfWork"""
        return getattr(self.uow, key)

    def clear(self):
        self.uow = unitofwork.UnitOfWork()

    def delete(self, *obj):
        """registers the given objects as to be deleted upon the next commit"""
        for o in obj:
            self.uow.register_deleted(o)
        
    def import_instance(self, instance):
        """places the given instance in the current thread's unit of work context,
        either in the current IdentityMap or marked as "new".  Returns either the object
        or the current corresponding version in the Identity Map.

        this method should be used for any object instance that is coming from a serialized
        storage, from another thread (assuming the regular threaded unit of work model), or any
        case where the instance was loaded/created corresponding to a different base unitofwork
        than the current one."""
        if instance is None:
            return None
        key = getattr(instance, '_instance_key', None)
        mapper = object_mapper(instance)
        u = self.uow
        if key is not None:
            if u.identity_map.has_key(key):
                return u.identity_map[key]
            else:
                instance._instance_key = key
                u.identity_map[key] = instance
                self._bind_to(instance)
        else:
            u.register_new(instance)
        return instance

def get_id_key(ident, class_, entity_name=None):
    return Session.get_id_key(ident, class_, entity_name)

def get_row_key(row, class_, primary_key, entity_name=None):
    return Session.get_row_key(row, class_, primary_key, entity_name)

def begin():
    """begins a new UnitOfWork transaction.  the next commit will affect only
    objects that are created, modified, or deleted following the begin statement."""
    return get_session().begin()

def commit(*obj):
    """commits the current UnitOfWork transaction.  if a transaction was begun 
    via begin(), commits only those objects that were created, modified, or deleted
    since that begin statement.  otherwise commits all objects that have been
    changed.
    
    if individual objects are submitted, then only those objects are committed, and the 
    begin/commit cycle is not affected."""
    get_session().commit(*obj)

def clear():
    """removes all current UnitOfWorks and IdentityMaps for this thread and 
    establishes a new one.  It is probably a good idea to discard all
    current mapped object instances, as they are no longer in the Identity Map."""
    get_session().clear()

def refresh(*obj):
    """reloads the state of this object from the database, and cancels any in-memory
    changes."""
    get_session().refresh(*obj)

def expire(*obj):
    """invalidates the data in the given objects and sets them to refresh themselves
    the next time they are requested."""
    get_session().expire(*obj)

def expunge(*obj):
    get_session().expunge(*obj)

def delete(*obj):
    """registers the given objects as to be deleted upon the next commit"""
    s = get_session().delete(*obj)

def has_key(key):
    """returns True if the current thread-local IdentityMap contains the given instance key"""
    return get_session().has_key(key)

def has_instance(instance):
    """returns True if the current thread-local IdentityMap contains the given instance"""
    return get_session().has_instance(instance)

def is_dirty(obj):
    """returns True if the given object is in the current UnitOfWork's new or dirty list,
    or if its a modified list attribute on an object."""
    return get_session().is_dirty(obj)

def instance_key(instance):
    """returns the IdentityMap key for the given instance"""
    return get_session().instance_key(instance)

def import_instance(instance):
    return get_session().import_instance(instance)

def mapper(*args, **params):
    return sqlalchemy.mapperlib.mapper(*args, **params)

def object_mapper(obj):
    return sqlalchemy.mapperlib.object_mapper(obj)

def class_mapper(class_):
    return sqlalchemy.mapperlib.class_mapper(class_)

global_attributes = unitofwork.global_attributes

session_registry = util.ScopedRegistry(Session) # Default session registry
_sessions = weakref.WeakValueDictionary() # all referenced sessions (including user-created)

def get_session(obj=None):
    # object-specific session ?
    if obj is not None:
        # does it have a hash key ?
        hashkey = getattr(obj, '_sa_session_id', None)
        if hashkey is not None:
            # ok, return that
            try:
                return _sessions[hashkey]
            except KeyError:
                raise InvalidRequestError("Session '%s' referenced by object '%s' no longer exists" % (hashkey, repr(obj)))

    return session_registry()
    
unitofwork.get_session = get_session
uow = get_session # deprecated

def push_session(sess):
    old = get_session()
    if getattr(sess, '_previous', None) is not None:
        raise InvalidRequestError("Given Session is already pushed onto some thread's stack")
    sess._previous = old
    session_registry.set(sess)
    sess.was_pushed()
    
def pop_session():
    sess = get_session()
    old = sess._previous
    sess._previous = None
    session_registry.set(old)
    sess.was_popped()
    return old
    
def using_session(sess, func):
    push_session(sess)
    try:
        return func()
    finally:
        pop_session()


.. _session_toplevel:

=================
Using the Session
=================

The `Mapper` is the entrypoint to the configurational API of the SQLAlchemy object relational mapper.  But the primary object one works with when using the ORM is the :class:`~sqlalchemy.orm.session.Session`.

What does the Session do ?
==========================

In the most general sense, the ``Session`` establishes all conversations with the database and represents a "holding zone" for all the mapped instances which you've loaded or created during its lifespan.  It implements the `Unit of Work <http://martinfowler.com/eaaCatalog/unitOfWork.html>`_ pattern, which means it keeps track of all changes which occur, and is capable of **flushing** those changes to the database as appropriate.   Another important facet of the ``Session`` is that it's also maintaining **unique** copies of each instance, where "unique" means "only one object with a particular primary key" - this pattern is called the `Identity Map <http://martinfowler.com/eaaCatalog/identityMap.html>`_.

Beyond that, the ``Session`` implements an interface which lets you move objects in or out of the session in a variety of ways, it provides the entryway to a ``Query`` object which is used to query the database for data, and it also provides a transactional context for SQL operations which rides on top of the transactional capabilities of ``Engine`` and ``Connection`` objects.

Getting a Session
=================

``Session`` is a regular Python class which can be directly instantiated.  However, to standardize how sessions are configured and acquired, the ``sessionmaker()`` function is normally used to create a top level ``Session`` configuration which can then be used throughout an application without the need to repeat the configurational arguments.

Using a sessionmaker() Configuration 
------------------------------------

The usage of ``sessionmaker()`` is illustrated below:

.. sourcecode:: python+sql

    from sqlalchemy.orm import sessionmaker
    
    # create a configured "Session" class
    Session = sessionmaker(bind=some_engine)

    # create a Session
    session = Session()
    
    # work with sess
    myobject = MyObject('foo', 'bar')
    session.add(myobject)
    session.commit()
    
    # close when finished
    session.close()

Above, the ``sessionmaker`` call creates a class for us, which we assign to the name ``Session``.  This class is a subclass of the actual ``sqlalchemy.orm.session.Session`` class, which will instantiate with a particular bound engine.

When you write your application, place the call to ``sessionmaker()`` somewhere global, and then make your new ``Session`` class available to the rest of your application.

Binding Session to an Engine 
----------------------------

In our previous example regarding ``sessionmaker()``, we specified a ``bind`` for a particular ``Engine``.  If we'd like to construct a ``sessionmaker()`` without an engine available and bind it later on, or to specify other options to an existing ``sessionmaker()``, we may use the ``configure()`` method::

    # configure Session class with desired options
    Session = sessionmaker()

    # later, we create the engine
    engine = create_engine('postgres://...')
    
    # associate it with our custom Session class
    Session.configure(bind=engine)

    # work with the session
    session = Session()

It's actually entirely optional to bind a Session to an engine.  If the underlying mapped ``Table`` objects use "bound" metadata, the ``Session`` will make use of the bound engine instead (or will even use multiple engines if multiple binds are present within the mapped tables).  "Bound" metadata is described at :ref:`metadata_binding`.

The ``Session`` also has the ability to be bound to multiple engines explicitly.   Descriptions of these scenarios are described in :ref:`session_partitioning`.

Binding Session to a Connection 
-------------------------------

The ``Session`` can also be explicitly bound to an individual database ``Connection``.  Reasons for doing this may include to join a ``Session`` with an ongoing transaction local to a specific ``Connection`` object, or to bypass connection pooling by just having connections persistently checked out and associated with distinct, long running sessions::

    # global application scope.  create Session class, engine
    Session = sessionmaker()

    engine = create_engine('postgres://...')
    
    ...
    
    # local scope, such as within a controller function
    
    # connect to the database
    connection = engine.connect()
    
    # bind an individual Session to the connection
    session = Session(bind=connection)

Using create_session() 
----------------------

As an alternative to ``sessionmaker()``, ``create_session()`` is a function which calls the normal ``Session`` constructor directly.  All arguments are passed through and the new ``Session`` object is returned::

    session = create_session(bind=myengine, autocommit=True, autoflush=False)

Note that ``create_session()`` disables all optional "automation" by default.  Called with no arguments, the session produced is not autoflushing, does not auto-expire, and does not maintain a transaction (i.e. it begins and commits a new transaction for each ``flush()``).  SQLAlchemy uses ``create_session()`` extensively within its own unit tests.

Configurational Arguments 
-------------------------

Configurational arguments accepted by ``sessionmaker()`` and ``create_session()`` are the same as that of the ``Session`` class itself, and are described at :func:`sqlalchemy.orm.sessionmaker`.

Note that the defaults of ``create_session()`` are the opposite of that of ``sessionmaker()``: autoflush and expire_on_commit are False, autocommit is True. It is recommended to use the ``sessionmaker()`` function instead of ``create_session()``. ``create_session()`` is used to get a session with no automation turned on and is useful for testing.

Using the Session 
==================

Quickie Intro to Object States 
------------------------------

It's helpful to know the states which an instance can have within a session:

* *Transient* - an instance that's not in a session, and is not saved to the database; i.e. it has no database identity.  The only relationship such an object has to the ORM is that its class has a ``mapper()`` associated with it.

* *Pending* - when you ``add()`` a transient instance, it becomes pending.  It still wasn't actually flushed to the database yet, but it will be when the next flush occurs.

* *Persistent* - An instance which is present in the session and has a record in the database.  You get persistent instances by either flushing so that the pending instances become persistent, or by querying the database for existing instances (or moving persistent instances from other sessions into your local session).

* *Detached* - an instance which has a record in the database, but is not in any session.  There's nothing wrong with this, and you can use objects normally when they're detached, **except** they will not be able to issue any SQL in order to load collections or attributes which are not yet loaded, or were marked as "expired".

Knowing these states is important, since the ``Session`` tries to be strict about ambiguous operations (such as trying to save the same object to two different sessions at the same time).

Frequently Asked Questions 
--------------------------

* When do I make a ``sessionmaker`` ?

    Just one time, somewhere in your application's global scope.  It should be looked upon as part of your application's configuration.  If your application has three .py files in a package, you could, for example, place the ``sessionmaker`` line in your ``__init__.py`` file; from that point on your other modules say "from mypackage import Session".   That way, everyone else just uses ``Session()``, and the configuration of that session is controlled by that central point.

    If your application starts up, does imports, but does not know what database it's going to be connecting to, you can bind the ``Session`` at the "class" level to the engine later on, using ``configure()``.

    In the examples in this section, we will frequently show the ``sessionmaker`` being created right above the line where we actually invoke ``Session()``.  But that's just for example's sake !  In reality, the ``sessionmaker`` would be somewhere at the module level, and your individual ``Session()`` calls would be sprinkled all throughout your app, such as in a web application within each controller method.

* When do I make a ``Session`` ? 

    You typically invoke ``Session()`` when you first need to talk to your database, and want to save some objects or load some existing ones.  Then, you work with it, save your changes, and then dispose of it....or at the very least ``close()`` it.  It's not a "global" kind of object, and should be handled more like a "local variable", as it's generally **not** safe to use with concurrent threads.  Sessions are very inexpensive to make, and don't use any resources whatsoever until they are first used...so create some !

    There is also a pattern whereby you're using a **contextual session**, this is described later in :ref:`unitofwork_contextual`.  In this pattern, a helper object is maintaining a ``Session`` for you, most commonly one that is local to the current thread (and sometimes also local to an application instance).  SQLAlchemy has worked this pattern out such that it still *looks* like you're creating a new session as you need one...so in that case, it's still a guaranteed win to just say ``Session()`` whenever you want a session.  

* Is the Session a cache ? 

    Yeee...no.  It's somewhat used as a cache, in that it implements the identity map pattern, and stores objects keyed to their primary key.  However, it doesn't do any kind of query caching.  This means, if you say ``session.query(Foo).filter_by(name='bar')``, even if ``Foo(name='bar')`` is right there, in the identity map, the session has no idea about that.  It has to issue SQL to the database, get the rows back, and then when it sees the primary key in the row, *then* it can look in the local identity map and see that the object is already there.  It's only when you say ``query.get({some primary key})`` that the ``Session`` doesn't have to issue a query.
    
    Additionally, the Session stores object instances using a weak reference by default.  This also defeats the purpose of using the Session as a cache, unless the ``weak_identity_map`` flag is set to ``False``.

    The ``Session`` is not designed to be a global object from which everyone consults as a "registry" of objects.  That is the job of a **second level cache**.  A good library for implementing second level caching is `Memcached <http://www.danga.com/memcached/>`_.  It *is* possible to "sort of" use the ``Session`` in this manner, if you set it to be non-transactional and it never flushes any SQL, but it's not a terrific solution,  since if concurrent threads load the same objects at the same time, you may have multiple copies of the same objects present in collections.

* How can I get the ``Session`` for a certain object ?

    Use the ``object_session()`` classmethod available on ``Session``::

        session = Session.object_session(someobject)

.. index::
   single: thread safety; sessions
   single: thread safety; Session

* Is the session thread-safe?

    Nope.  It has no thread synchronization of any kind built in, and particularly when you do a flush operation, it definitely is not open to concurrent threads accessing it, because it holds onto a single database connection at that point.  If you use a session which is non-transactional for read operations only, it's still not thread-"safe", but you also wont get any catastrophic failures either, since it opens and closes connections on an as-needed basis; it's just that different threads might load the same objects independently of each other, but only one will wind up in the identity map (however, the other one might still live in a collection somewhere).

    But the bigger point here is, you should not *want* to use the session with multiple concurrent threads.  That would be like having everyone at a restaurant all eat from the same plate.  The session is a local "workspace" that you use for a specific set of tasks; you don't want to, or need to, share that session with other threads who are doing some other task.  If, on the other hand, there are other threads  participating in the same task you are, such as in a desktop graphical application, then you would be sharing the session with those threads, but you also will have implemented a proper locking scheme (or your graphical framework does) so that those threads do not collide.
  
Querying
--------

The ``query()`` function takes one or more *entities* and returns a new ``Query`` object which will issue mapper queries within the context of this Session.  An entity is defined as a mapped class, a ``Mapper`` object, an orm-enabled *descriptor*, or an ``AliasedClass`` object::

    # query from a class
    session.query(User).filter_by(name='ed').all()

    # query with multiple classes, returns tuples
    session.query(User, Address).join('addresses').filter_by(name='ed').all()

    # query using orm-enabled descriptors
    session.query(User.name, User.fullname).all()
    
    # query from a mapper
    user_mapper = class_mapper(User)
    session.query(user_mapper)

When ``Query`` returns results, each object instantiated is stored within the identity map.   When a row matches an object which is already present, the same object is returned.  In the latter case, whether or not the row is populated onto an existing object depends upon whether the attributes of the instance have been *expired* or not.  As of 0.5, a default-configured ``Session`` automatically expires all instances along transaction boundaries, so that with a normally isolated transaction, there shouldn't be any issue of instances representing data which is stale with regards to the current transaction.

Adding New or Existing Items
----------------------------

``add()`` is used to place instances in the session.  For *transient* (i.e. brand new) instances, this will have the effect of an INSERT taking place for those instances upon the next flush.  For instances which are *persistent* (i.e. were loaded by this session), they are already present and do not need to be added.  Instances which are *detached* (i.e. have been removed from a session) may be re-associated with a session using this method::

    user1 = User(name='user1')
    user2 = User(name='user2')
    session.add(user1)
    session.add(user2)
    
    session.commit()     # write changes to the database

To add a list of items to the session at once, use ``add_all()``::

    session.add_all([item1, item2, item3])

The ``add()`` operation **cascades** along the ``save-update`` cascade.  For more details see the section :ref:`unitofwork_cascades`.

Merging
-------

``merge()`` reconciles the current state of an instance and its associated children with existing data in the database, and returns a copy of the instance associated with the session.  Usage is as follows::

    merged_object = session.merge(existing_object)

When given an instance, it follows these steps:

  * It examines the primary key of the instance.  If it's present, it attempts to load an instance with that primary key (or pulls from the local identity map).
  * If there's no primary key on the given instance, or the given primary key does not exist in the database, a new instance is created.
  * The state of the given instance is then copied onto the located/newly created instance.
  * The operation is cascaded to associated child items along the ``merge`` cascade.  Note that all changes present on the given instance, including changes to collections, are merged.
  * The new instance is returned.

With ``merge()``, the given instance is not placed within the session, and can be associated with a different session or detached.  ``merge()`` is very useful for taking the state of any kind of object structure without regard for its origins or current session associations and placing that state within a session.   Here's two examples:

  * An application which reads an object structure from a file and wishes to save it to the database might parse the file, build up the structure, and then use ``merge()`` to save it to the database, ensuring that the data within the file is used to formulate the primary key of each element of the structure.  Later, when the file has changed, the same process can be re-run, producing a slightly different object structure, which can then be ``merged()`` in again, and the ``Session`` will automatically update the database to reflect those changes.
  * A web application stores mapped entities within an HTTP session object.  When each request starts up, the serialized data can be merged into the session, so that the original entity may be safely shared among requests and threads.

``merge()`` is frequently used by applications which implement their own second level caches.  This refers to an application which uses an in memory dictionary, or an tool like Memcached to store objects over long running spans of time.  When such an object needs to exist within a ``Session``, ``merge()`` is a good choice since it leaves the original cached object untouched.  For this use case, merge provides a keyword option called ``dont_load=True``.  When this boolean flag is set to ``True``, ``merge()`` will not issue any SQL to reconcile the given object against the current state of the database, thereby reducing query overhead.   The limitation is that the given object and all of its children may not contain any pending changes, and it's also of course possible that newer information in the database will not be present on the merged object, since no load is issued.

Deleting
--------

The ``delete`` method places an instance into the Session's list of objects to be marked as deleted::

    # mark two objects to be deleted
    session.delete(obj1)
    session.delete(obj2)

    # commit (or flush)
    session.commit()

The big gotcha with ``delete()`` is that **nothing is removed from collections**.  Such as, if a ``User`` has a collection of three ``Addresses``, deleting an ``Address`` will not remove it from ``user.addresses``::

    >>> address = user.addresses[1]
    >>> session.delete(address)
    >>> session.flush()
    >>> address in user.addresses
    True

The solution is to use proper cascading::

    mapper(User, users_table, properties={
        'addresses':relation(Address, cascade="all, delete, delete-orphan")
    })
    del user.addresses[1]
    session.flush()

Flushing
--------

When the ``Session`` is used with its default configuration, the flush step is nearly always done transparently.  Specifically, the flush occurs before any individual ``Query`` is issued, as well as within the ``commit()`` call before the transaction is committed.  It also occurs before a SAVEPOINT is issued when ``begin_nested()`` is used.  

Regardless of the autoflush setting, a flush can always be forced by issuing ``flush()``::

    session.flush()
    
The "flush-on-Query" aspect of the behavior can be disabled by constructing ``sessionmaker()`` with the flag ``autoflush=False``::

    Session = sessionmaker(autoflush=False)
    
Additionally, autoflush can be temporarily disabled by setting the ``autoflush`` flag at any time::

    mysession = Session()
    mysession.autoflush = False

Some autoflush-disable recipes are available at `DisableAutoFlush <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DisableAutoflush>`_.

The flush process *always* occurs within a transaction, even if the ``Session`` has been configured with ``autocommit=True``, a setting that disables the session's persistent transactional state.  If no transaction is present, ``flush()`` creates its own transaction and commits it.  Any failures during flush will always result in a rollback of whatever transaction is present.  If the Session is not in ``autocommit=True`` mode, an explicit call to ``rollback()`` is required after a flush fails, even though the underlying transaction will have been rolled back already - this is so that the overall nesting pattern of so-called "subtransactions" is consistently maintained.

Committing
----------

``commit()`` is used to commit the current transaction.  It always issues ``flush()`` beforehand to flush any remaining state to the database; this is independent of the "autoflush" setting.   If no transaction is present, it raises an error.  Note that the default behavior of the ``Session`` is that a transaction is always present; this behavior can be disabled by setting ``autocommit=True``.  In autocommit mode, a transaction can be initiated by calling the ``begin()`` method.

Another behavior of ``commit()`` is that by default it expires the state of all instances present after the commit is complete.  This is so that when the instances are next accessed, either through attribute access or by them being present in a ``Query`` result set, they receive the most recent state.  To disable this behavior, configure ``sessionmaker()`` with ``expire_on_commit=False``.

Normally, instances loaded into the ``Session`` are never changed by subsequent queries; the assumption is that the current transaction is isolated so the state most recently loaded is correct as long as the transaction continues.  Setting ``autocommit=True`` works against this model to some degree since the ``Session`` behaves in exactly the same way with regard to attribute state, except no transaction is present.

Rolling Back
------------

``rollback()`` rolls back the current transaction.   With a default configured session, the post-rollback state of the session is as follows:

  * All connections are rolled back and returned to the connection pool, unless the Session was bound directly to a Connection, in which case the connection is still maintained (but still rolled back).
  * Objects which were initially in the *pending* state when they were added to the ``Session`` within the lifespan of the transaction are expunged, corresponding to their INSERT statement being rolled back.  The state of their attributes remains unchanged.
  * Objects which were marked as *deleted* within the lifespan of the transaction are promoted back to the *persistent* state, corresponding to their DELETE statement being rolled back.  Note that if those objects were first *pending* within the transaction, that operation takes precedence instead.
  * All objects not expunged are fully expired.  

With that state understood, the ``Session`` may safely continue usage after a rollback occurs (note that this is a new feature as of version 0.5).

When a ``flush()`` fails, typically for reasons like primary key, foreign key, or "not nullable" constraint violations, a ``rollback()`` is issued automatically (it's currently not possible for a flush to continue after a partial failure).  However, the flush process always uses its own transactional demarcator called a *subtransaction*, which is described more fully in the docstrings for ``Session``.  What it means here is that even though the database transaction has been rolled back, the end user must still issue ``rollback()`` to fully reset the state of the ``Session``.

Expunging
---------

Expunge removes an object from the Session, sending persistent instances to the detached state, and pending instances to the transient state:

.. sourcecode:: python+sql

    session.expunge(obj1)
    
To remove all items, call ``session.expunge_all()`` (this method was formerly known as ``clear()``).

Closing
-------

The ``close()`` method issues a ``expunge_all()``, and releases any transactional/connection resources.  When connections are returned to the connection pool, transactional state is rolled back as well.

Refreshing / Expiring
---------------------

To assist with the Session's "sticky" behavior of instances which are present, individual objects can have all of their attributes immediately re-loaded from the database, or marked as "expired" which will cause a re-load to occur upon the next access of any of the object's mapped attributes.  This includes all relationships, so lazy-loaders will be re-initialized, eager relationships will be repopulated.  Any changes marked on the object are discarded::

    # immediately re-load attributes on obj1, obj2
    session.refresh(obj1)
    session.refresh(obj2)
    
    # expire objects obj1, obj2, attributes will be reloaded
    # on the next access:
    session.expire(obj1)
    session.expire(obj2)

``refresh()`` and ``expire()`` also support being passed a list of individual attribute names in which to be refreshed.  These names can reference any attribute, column-based or relation based::

    # immediately re-load the attributes 'hello', 'world' on obj1, obj2
    session.refresh(obj1, ['hello', 'world'])
    session.refresh(obj2, ['hello', 'world'])
    
    # expire the attributes 'hello', 'world' objects obj1, obj2, attributes will be reloaded
    # on the next access:
    session.expire(obj1, ['hello', 'world'])
    session.expire(obj2, ['hello', 'world'])

The full contents of the session may be expired at once using ``expire_all()``::

    session.expire_all()

``refresh()`` and ``expire()`` are usually not needed when working with a default-configured ``Session``.  The usual need is when an UPDATE or DELETE has been issued manually within the transaction using ``Session.execute()``.

Session Attributes 
------------------

The ``Session`` itself acts somewhat like a set-like collection.  All items present may be accessed using the iterator interface::

    for obj in session:
        print obj

And presence may be tested for using regular "contains" semantics::

    if obj in session:
        print "Object is present"

The session is also keeping track of all newly created (i.e. pending) objects, all objects which have had changes since they were last loaded or saved (i.e. "dirty"), and everything that's been marked as deleted::

    # pending objects recently added to the Session
    session.new

    # persistent objects which currently have changes detected
    # (this collection is now created on the fly each time the property is called)
    session.dirty

    # persistent objects that have been marked as deleted via session.delete(obj)
    session.deleted

Note that objects within the session are by default *weakly referenced*.  This means that when they are dereferenced in the outside application, they fall out of scope from within the ``Session`` as well and are subject to garbage collection by the Python interpreter.  The exceptions to this include objects which are pending, objects which are marked as deleted, or persistent objects which have pending changes on them.  After a full flush, these collections are all empty, and all objects are again weakly referenced.  To disable the weak referencing behavior and force all objects within the session to remain until explicitly expunged, configure ``sessionmaker()`` with the ``weak_identity_map=False`` setting.

.. _unitofwork_cascades:

Cascades
========

Mappers support the concept of configurable *cascade* behavior on :func:`~sqlalchemy.orm.relation()` constructs.  This behavior controls how the Session should treat the instances that have a parent-child relationship with another instance that is operated upon by the Session.  Cascade is indicated as a comma-separated list of string keywords, with the possible values ``all``, ``delete``, ``save-update``, ``refresh-expire``, ``merge``, ``expunge``, and ``delete-orphan``.

Cascading is configured by setting the ``cascade`` keyword argument on a ``relation()``::

    mapper(Order, order_table, properties={
        'items' : relation(Item, items_table, cascade="all, delete-orphan"),
        'customer' : relation(User, users_table, user_orders_table, cascade="save-update"),
    })

The above mapper specifies two relations, ``items`` and ``customer``.  The ``items`` relationship specifies "all, delete-orphan" as its ``cascade`` value, indicating that all  ``add``, ``merge``, ``expunge``, ``refresh`` ``delete`` and ``expire`` operations performed on a parent ``Order`` instance should also be performed on the child ``Item`` instances attached to it.  The ``delete-orphan`` cascade value additionally indicates that if an ``Item`` instance is no longer associated with an ``Order``, it should also be deleted.  The "all, delete-orphan" cascade argument allows a so-called *lifecycle* relationship between an ``Order`` and an ``Item`` object.

The ``customer`` relationship specifies only the "save-update" cascade value, indicating most operations will not be cascaded from a parent ``Order`` instance to a child ``User`` instance except for the ``add()`` operation.  "save-update" cascade indicates that an ``add()`` on the parent will cascade to all child items, and also that items added to a parent which is already present in the session will also be added.

Note that the ``delete-orphan`` cascade only functions for relationships where the target object can have a single parent at a time, meaning it is only appropriate for one-to-one or one-to-many relationships.  For a :func:`~sqlalchemy.orm.relation` which establishes one-to-one via a local foreign key, i.e. a many-to-one that stores only a single parent, or one-to-one/one-to-many via a "secondary" (association) table, a warning will be issued if ``delete-orphan`` is configured.  To disable this warning, also specify the ``single_parent=True`` flag on the relationship, which constrains objects to allow attachment to only one parent at a time.

The default value for ``cascade`` on :func:`~sqlalchemy.orm.relation()` is ``save-update, merge``.

.. _unitofwork_transaction:

Managing Transactions
=====================

The ``Session`` manages transactions across all engines associated with it.  As the ``Session`` receives requests to execute SQL statements using a particular ``Engine`` or ``Connection``, it adds each individual ``Engine`` encountered to its transactional state and maintains an open connection for each one (note that a simple application normally has just one ``Engine``).  At commit time, all unflushed data is flushed, and each individual transaction is committed.  If the underlying databases support two-phase semantics, this may be used by the Session as well if two-phase transactions are enabled.

Normal operation ends the transactional state using the ``rollback()`` or ``commit()`` methods.  After either is called, the ``Session`` starts a new transaction::

    Session = sessionmaker()
    session = Session()
    try:
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'
    
        # commit- will immediately go into a new transaction afterwards
        session.commit()
    except:
        # rollback - will immediately go into a new transaction afterwards.
        session.rollback()

A session which is configured with ``autocommit=True`` may be placed into a transaction using ``begin()``.  With an ``autocommit=True`` session that's been placed into a transaction using ``begin()``, the session releases all connection resources after a ``commit()`` or ``rollback()`` and remains transaction-less (with the exception of flushes) until the next ``begin()`` call::

    Session = sessionmaker(autocommit=True)
    session = Session()
    session.begin()
    try:
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'
        session.commit()
    except:
        session.rollback()
        raise

The ``begin()`` method also returns a transactional token which is compatible with the Python 2.6 ``with`` statement::

    Session = sessionmaker(autocommit=True)
    session = Session()
    with session.begin():
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'

Using SAVEPOINT 
---------------

SAVEPOINT transactions, if supported by the underlying engine, may be delineated using the ``begin_nested()`` method::

    Session = sessionmaker()
    session = Session()
    session.add(u1)
    session.add(u2)

    session.begin_nested() # establish a savepoint
    session.add(u3)
    session.rollback()  # rolls back u3, keeps u1 and u2

    session.commit() # commits u1 and u2

``begin_nested()`` may be called any number of times, which will issue a new SAVEPOINT with a unique identifier for each call.  For each ``begin_nested()`` call, a corresponding ``rollback()`` or ``commit()`` must be issued.  

When ``begin_nested()`` is called, a ``flush()`` is unconditionally issued (regardless of the ``autoflush`` setting).  This is so that when a ``rollback()`` occurs, the full state of the session is expired, thus causing all subsequent attribute/instance access to reference the full state of the ``Session`` right before ``begin_nested()`` was called.

Enabling Two-Phase Commit 
-------------------------

Finally, for MySQL, PostgreSQL, and soon Oracle as well, the session can be instructed to use two-phase commit semantics. This will coordinate the committing of transactions across databases so that the transaction is either committed or rolled back in all databases. You can also ``prepare()`` the session for interacting with transactions not managed by SQLAlchemy. To use two phase transactions set the flag ``twophase=True`` on the session::

    engine1 = create_engine('postgres://db1')
    engine2 = create_engine('postgres://db2')
    
    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

    session = Session()
    
    # .... work with accounts and users
    
    # commit.  session will issue a flush to all DBs, and a prepare step to all DBs,
    # before committing both transactions
    session.commit()

Embedding SQL Insert/Update Expressions into a Flush 
=====================================================

This feature allows the value of a database column to be set to a SQL expression instead of a literal value.  It's especially useful for atomic updates, calling stored procedures, etc.  All you do is assign an expression to an attribute::

    class SomeClass(object):
        pass
    mapper(SomeClass, some_table)
    
    someobject = session.query(SomeClass).get(5)
    
    # set 'value' attribute to a SQL expression adding one
    someobject.value = some_table.c.value + 1
    
    # issues "UPDATE some_table SET value=value+1"
    session.commit()
    
This technique works both for INSERT and UPDATE statements.  After the flush/commit operation, the ``value`` attribute on ``someobject`` above is expired, so that when next accessed the newly generated value will be loaded from the database. 

Using SQL Expressions with Sessions 
====================================

SQL expressions and strings can be executed via the ``Session`` within its transactional context.  This is most easily accomplished using the ``execute()`` method, which returns a ``ResultProxy`` in the same manner as an ``Engine`` or ``Connection``::

    Session = sessionmaker(bind=engine)
    session = Session()
    
    # execute a string statement
    result = session.execute("select * from table where id=:id", {'id':7})
    
    # execute a SQL expression construct
    result = session.execute(select([mytable]).where(mytable.c.id==7))

The current ``Connection`` held by the ``Session`` is accessible using the ``connection()`` method::

    connection = session.connection()

The examples above deal with a ``Session`` that's bound to a single ``Engine`` or ``Connection``.  To execute statements using a ``Session`` which is bound either to multiple engines, or none at all (i.e. relies upon bound metadata), both ``execute()`` and ``connection()`` accept a ``mapper`` keyword argument, which is passed a mapped class or ``Mapper`` instance, which is used to locate the proper context for the desired engine::

    Session = sessionmaker()
    session = Session()
    
    # need to specify mapper or class when executing
    result = session.execute("select * from table where id=:id", {'id':7}, mapper=MyMappedClass)

    result = session.execute(select([mytable], mytable.c.id==7), mapper=MyMappedClass)

    connection = session.connection(MyMappedClass)

Joining a Session into an External Transaction 
===============================================

If a ``Connection`` is being used which is already in a transactional state (i.e. has a ``Transaction``), a ``Session`` can be made to participate within that transaction by just binding the ``Session`` to that ``Connection``::

    Session = sessionmaker()
    
    # non-ORM connection + transaction
    conn = engine.connect()
    trans = conn.begin()
    
    # create a Session, bind to the connection
    session = Session(bind=conn)
    
    # ... work with session
    
    session.commit() # commit the session
    session.close()  # close it out, prohibit further actions
    
    trans.commit() # commit the actual transaction

Note that above, we issue a ``commit()`` both on the ``Session`` as well as the ``Transaction``.  This is an example of where we take advantage of ``Connection``'s ability to maintain *subtransactions*, or nested begin/commit pairs.  The ``Session`` is used exactly as though it were managing the transaction on its own; its ``commit()`` method issues its ``flush()``, and commits the subtransaction.   The subsequent transaction the ``Session`` starts after commit will not begin until it's next used.  Above we issue a ``close()`` to prevent this from occurring.  Finally, the actual transaction is committed using ``Transaction.commit()``.

When using the ``threadlocal`` engine context, the process above is simplified; the ``Session`` uses the same connection/transaction as everyone else in the current thread, whether or not you explicitly bind it::

    engine = create_engine('postgres://mydb', strategy="threadlocal")
    engine.begin()
    
    session = Session()  # session takes place in the transaction like everyone else
    
    # ... go nuts
    
    engine.commit() # commit the transaction

.. _unitofwork_contextual:

Contextual/Thread-local Sessions 
=================================

A common need in applications, particularly those built around web frameworks, is the ability to "share" a ``Session`` object among disparate parts of an application, without needing to pass the object explicitly to all method and function calls.  What you're really looking for is some kind of "global" session object, or at least "global" to all the parts of an application which are tasked with servicing the current request.  For this pattern, SQLAlchemy provides the ability to enhance the ``Session`` class generated by ``sessionmaker()`` to provide auto-contextualizing support.  This means that whenever you create a ``Session`` instance with its constructor, you get an *existing* ``Session`` object which is bound to some "context".  By default, this context is the current thread.  This feature is what previously was accomplished using the ``sessioncontext`` SQLAlchemy extension.

Creating a Thread-local Context 
-------------------------------

The ``scoped_session()`` function wraps around the ``sessionmaker()`` function, and produces an object which behaves the same as the ``Session`` subclass returned by ``sessionmaker()``::

    from sqlalchemy.orm import scoped_session, sessionmaker
    Session = scoped_session(sessionmaker())
    
However, when you instantiate this ``Session`` "class", in reality the object is pulled from a threadlocal variable, or if it doesn't exist yet, it's created using the underlying class generated by ``sessionmaker()``::

    >>> # call Session() the first time.  the new Session instance is created.
    >>> session = Session()
    
    >>> # later, in the same application thread, someone else calls Session()
    >>> session2 = Session()
    
    >>> # the two Session objects are *the same* object
    >>> session is session2
    True

Since the ``Session()`` constructor now returns the same ``Session`` object every time within the current thread, the object returned by ``scoped_session()`` also implements most of the ``Session`` methods and properties at the "class" level, such that you don't even need to instantiate ``Session()``::

    # create some objects
    u1 = User()
    u2 = User()
    
    # save to the contextual session, without instantiating
    Session.add(u1)
    Session.add(u2)
    
    # view the "new" attribute
    assert u1 in Session.new
    
    # commit changes
    Session.commit()

The contextual session may be disposed of by calling ``Session.remove()``::

    # remove current contextual session
    Session.remove()

After ``remove()`` is called, the next operation with the contextual session will start a new ``Session`` for the current thread.

Lifespan of a Contextual Session 
--------------------------------

A (really, really) common question is when does the contextual session get created, when does it get disposed ?  We'll consider a typical lifespan as used in a web application::

    Web Server          Web Framework        User-defined Controller Call
    --------------      --------------       ------------------------------
    web request    -> 
                        call controller ->   # call Session().  this establishes a new,
                                             # contextual Session.
                                             session = Session()
                                             
                                             # load some objects, save some changes
                                             objects = session.query(MyClass).all()
                                             
                                             # some other code calls Session, it's the 
                                             # same contextual session as "sess"
                                             session2 = Session()
                                             session2.add(foo)
                                             session2.commit()
                                             
                                             # generate content to be returned
                                             return generate_content()
                        Session.remove() <-
    web response   <-  

The above example illustrates an explicit call to ``Session.remove()``.  This has the effect such that each web request starts fresh with a brand new session.   When integrating with a web framework, there's actually many options on how to proceed for this step, particularly as of version 0.5:

* Session.remove() - this is the most cut and dry approach; the ``Session`` is thrown away, all of its transactional/connection resources are closed out, everything within it is explicitly gone.  A new ``Session`` will be used on the next request.
* Session.close() - Similar to calling ``remove()``, in that all objects are explicitly expunged and all transactional/connection resources closed, except the actual ``Session`` object hangs around.  It doesn't make too much difference here unless the start of the web request would like to pass specific options to the initial construction of ``Session()``, such as a specific ``Engine`` to bind to.
* Session.commit() - In this case, the behavior is that any remaining changes pending are flushed, and the transaction is committed.  The full state of the session is expired, so that when the next web request is started, all data will be reloaded.  In reality, the contents of the ``Session`` are weakly referenced anyway so its likely that it will be empty on the next request in any case.
* Session.rollback() - Similar to calling commit, except we assume that the user would have called commit explicitly if that was desired; the ``rollback()`` ensures that no transactional state remains and expires all data, in the case that the request was aborted and did not roll back itself.
* do nothing - this is a valid option as well.  The controller code is responsible for doing one of the above steps at the end of the request.

Scoped Session API docs: :func:`sqlalchemy.orm.scoped_session`

.. _session_partitioning:

Partitioning Strategies
=======================

Vertical Partitioning
---------------------

Vertical partitioning places different kinds of objects, or different tables, across multiple databases::

    engine1 = create_engine('postgres://db1')
    engine2 = create_engine('postgres://db2')

    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

    session = Session()

Horizontal Partitioning
-----------------------

Horizontal partitioning partitions the rows of a single table (or a set of tables) across multiple databases.

See the "sharding" example in `attribute_shard.py <http://www.sqlalchemy.org/trac/browser/sqlalchemy/trunk/examples/sharding/attribute_shard.py>`_

Extending Session
=================

Extending the session can be achieved through subclassing as well as through a simple extension class, which resembles the style of :ref:`extending_mapper` called :class:`~sqlalchemy.orm.interfaces.SessionExtension`.  See the docstrings for more information on this class' methods.

Basic usage is similar to :class:`~sqlalchemy.orm.interfaces.MapperExtension`::

    class MySessionExtension(SessionExtension):
        def before_commit(self, session):
            print "before commit!"
            
    Session = sessionmaker(extension=MySessionExtension())
    
or with :func:`~sqlalchemy.orm.create_session()`::

    session = create_session(extension=MySessionExtension())
    
The same ``SessionExtension`` instance can be used with any number of sessions.

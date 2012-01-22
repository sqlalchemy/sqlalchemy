.. _session_toplevel:

=================
Using the Session
=================

.. module:: sqlalchemy.orm.session

The :func:`.orm.mapper` function and :mod:`~sqlalchemy.ext.declarative` extensions
are the primary configurational interface for the ORM. Once mappings are
configured, the primary usage interface for persistence operations is the
:class:`.Session`.

What does the Session do ?
==========================

In the most general sense, the :class:`~.Session` establishes all
conversations with the database and represents a "holding zone" for all the
objects which you've loaded or associated with it during its lifespan. It
provides the entrypoint to acquire a :class:`.Query` object, which sends
queries to the database using the :class:`~.Session` object's current database
connection, populating result rows into objects that are then stored in the
:class:`.Session`, inside a structure called the `Identity Map
<http://martinfowler.com/eaaCatalog/identityMap.html>`_ - a data structure
that maintains unique copies of each object, where "unique" means "only one
object with a particular primary key".

The :class:`.Session` begins in an essentially stateless form. Once queries
are issued or other objects are persisted with it, it requests a connection
resource from an :class:`.Engine` that is associated either with the
:class:`.Session` itself or with the mapped :class:`.Table` objects being
operated upon. This connection represents an ongoing transaction, which
remains in effect until the :class:`.Session` is instructed to commit or roll
back its pending state.

All changes to objects maintained by a :class:`.Session` are tracked - before
the database is queried again or before the current transaction is committed,
it **flushes** all pending changes to the database. This is known as the `Unit
of Work <http://martinfowler.com/eaaCatalog/unitOfWork.html>`_ pattern.

When using a :class:`.Session`, it's important to note that the objects
which are associated with it are **proxy objects** to the transaction being
held by the :class:`.Session` - there are a variety of events that will cause
objects to re-access the database in order to keep synchronized.   It is 
possible to "detach" objects from a :class:`.Session`, and to continue using 
them, though this practice has its caveats.  It's intended that
usually, you'd re-associate detached objects another :class:`.Session` when you 
want to work with them again, so that they can resume their normal task of 
representing database state.

Getting a Session
=================

:class:`.Session` is a regular Python class which can
be directly instantiated. However, to standardize how sessions are configured
and acquired, the :func:`.sessionmaker` function is normally
used to create a top level :class:`.Session`
configuration which can then be used throughout an application without the
need to repeat the configurational arguments.

The usage of :func:`.sessionmaker` is illustrated below:

.. sourcecode:: python+sql

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # an Engine, which the Session will use for connection
    # resources
    some_engine = create_engine('postgresql://scott:tiger@localhost/')

    # create a configured "Session" class
    Session = sessionmaker(bind=some_engine)

    # create a Session
    session = Session()

    # work with sess
    myobject = MyObject('foo', 'bar')
    session.add(myobject)
    session.commit()

Above, the :func:`.sessionmaker` call creates a class for us,
which we assign to the name ``Session``. This class is a subclass of the
actual :class:`.Session` class, which when instantiated, will
use the arguments we've given the function, in this case
to use a particular :class:`.Engine` for connection resources.

A typical setup will associate the :func:`.sessionmaker` with an :class:`.Engine`,
so that each :class:`.Session` generated will use this :class:`.Engine`
to acquire connection resources.   This association can
be set up as in the example above, using the ``bind`` argument. 

When you write your application, place the result of the
:func:`.sessionmaker` call at the global level.   The resulting
``Session`` class, configured for your application, should then
be used by the rest of the applcation as the source of new :class:`.Session`
instances.

An extremely common step taken by applications, including virtually
all web applications, is to further wrap the :func:`.sessionmaker`
construct in a so-called "contextual" session, provided by the
:func:`.scoped_session` construct.  This construct places the :func:`.sessionmaker`
into a **registry** that maintains a single :class:`.Session` per 
application thread.   Information on using contextual sessions
is at :ref:`unitofwork_contextual`.

Adding Additional Configuration to an Existing sessionmaker()
--------------------------------------------------------------

A common scenario is where the :func:`.sessionmaker` is invoked 
at module import time, however the generation of one or more :class:`.Engine`
instances to be associated with the :func:`.sessionmaker` has not yet proceeded.
For this use case, the :func:`.sessionmaker` construct offers the 
:meth:`.sessionmaker.configure` method, which will place additional configuration
directives into an existing :func:`.sessionmaker` that will take place
when the construct is invoked::


    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine

    # configure Session class with desired options
    Session = sessionmaker()

    # later, we create the engine
    engine = create_engine('postgresql://...')

    # associate it with our custom Session class
    Session.configure(bind=engine)

    # work with the session
    session = Session()

Creating Ad-Hoc Session Objects with Alternate Arguments
---------------------------------------------------------

For the use case where an application needs to create a new :class:`.Session` with
special arguments that deviate from what is normally used throughout the application, 
such as a :class:`.Session` that binds to an alternate 
source of connectivity, or a :class:`.Session` that should 
have other arguments such as ``expire_on_commit`` established differently from 
what most of the application wants, specific arguments can be passed to the 
:func:`.sessionmaker` construct's class itself.  These arguments will override whatever
configurations have already been placed, such as below, where a new :class:`.Session`
is constructed against a specific :class:`.Connection`::

    # at the module level, the global sessionmaker,
    # bound to a specific Engine
    Session = sessionmaker(bind=engine)

    # later, some unit of code wants to create a 
    # Session that is bound to a specific Connection
    conn = engine.connect()
    session = Session(bind=conn)

The typical rationale for the association of a :class:`.Session` with a specific
:class:`.Connection` is that of a test fixture that maintains an external 
transaction - see :ref:`session_external_transaction` for an example of this.

Using the Session
==================

.. _session_object_states:

Quickie Intro to Object States
------------------------------

It's helpful to know the states which an instance can have within a session:

* *Transient* - an instance that's not in a session, and is not saved to the
  database; i.e. it has no database identity. The only relationship such an
  object has to the ORM is that its class has a ``mapper()`` associated with
  it.

* *Pending* - when you :func:`~sqlalchemy.orm.session.Session.add` a transient
  instance, it becomes pending. It still wasn't actually flushed to the
  database yet, but it will be when the next flush occurs.

* *Persistent* - An instance which is present in the session and has a record
  in the database. You get persistent instances by either flushing so that the
  pending instances become persistent, or by querying the database for
  existing instances (or moving persistent instances from other sessions into
  your local session).

* *Detached* - an instance which has a record in the database, but is not in
  any session. There's nothing wrong with this, and you can use objects
  normally when they're detached, **except** they will not be able to issue
  any SQL in order to load collections or attributes which are not yet loaded,
  or were marked as "expired".

Knowing these states is important, since the
:class:`~sqlalchemy.orm.session.Session` tries to be strict about ambiguous
operations (such as trying to save the same object to two different sessions
at the same time).

.. _session_faq:

Session Frequently Asked Questions
-----------------------------------

* When do I make a :func:`.sessionmaker` ?

    Just one time, somewhere in your application's global scope. It should be
    looked upon as part of your application's configuration. If your
    application has three .py files in a package, you could, for example,
    place the :func:`.sessionmaker` line in your ``__init__.py`` file; from
    that point on your other modules say "from mypackage import Session". That
    way, everyone else just uses :class:`.Session()`,
    and the configuration of that session is controlled by that central point.

    If your application starts up, does imports, but does not know what
    database it's going to be connecting to, you can bind the
    :class:`.Session` at the "class" level to the
    engine later on, using ``configure()``.

    In the examples in this section, we will frequently show the
    :func:`.sessionmaker` being created right above the line where we actually
    invoke :class:`~sqlalchemy.orm.session.Session()`. But that's just for
    example's sake ! In reality, the :func:`.sessionmaker` would be somewhere
    at the module level, and your individual
    :class:`~sqlalchemy.orm.session.Session()` calls would be sprinkled all
    throughout your app, such as in a web application within each controller
    method.

* When do I make a :class:`.Session` ?

    You typically invoke :class:`.Session` when you first need to talk to your
    database, and want to save some objects or load some existing ones. It
    then remains in use for the lifespan of a particular database
    conversation, which includes not just the initial loading of objects but
    throughout the whole usage of those instances.

    Objects become detached if their owning session is discarded. They are
    still functional in the detached state if the user has ensured that their
    state has not been expired before detachment, but they will not be able to
    represent the current state of database data. Because of this, it's best
    to consider persisted objects as an extension of the state of a particular
    :class:`.Session`, and to keep that session around until all referenced
    objects have been discarded.

    An exception to this is when objects are placed in caches or otherwise
    shared among threads or processes, in which case their detached state can
    be stored, transmitted, or shared. However, the state of detached objects
    should still be transferred back into a new :class:`.Session` using
    :meth:`.Session.add` or :meth:`.Session.merge` before working with the
    object (or in the case of merge, its state) again.

    It is also very common that a :class:`.Session` as well as its associated
    objects are only referenced by a single thread.  Sharing objects between
    threads is most safely accomplished by sharing their state among multiple
    instances of those objects, each associated with a distinct
    :class:`.Session` per thread, :meth:`.Session.merge` to transfer state
    between threads.   This pattern is not a strict requirement by any means, 
    but it has the least chance of introducing concurrency issues.

    To help with the recommended :class:`.Session` -per-thread,
    :class:`.Session` -per-set-of-objects patterns, the
    :func:`.scoped_session` function is provided which produces a
    thread-managed registry of :class:`.Session` objects. It is commonly used
    in web applications so that a single global variable can be used to safely
    represent transactional sessions with sets of objects, localized to a
    single thread. More on this object is in :ref:`unitofwork_contextual`.

* Is the Session a cache ?

    Yeee...no. It's somewhat used as a cache, in that it implements the
    identity map pattern, and stores objects keyed to their primary key.
    However, it doesn't do any kind of query caching. This means, if you say
    ``session.query(Foo).filter_by(name='bar')``, even if ``Foo(name='bar')``
    is right there, in the identity map, the session has no idea about that.
    It has to issue SQL to the database, get the rows back, and then when it
    sees the primary key in the row, *then* it can look in the local identity
    map and see that the object is already there. It's only when you say
    ``query.get({some primary key})`` that the
    :class:`~sqlalchemy.orm.session.Session` doesn't have to issue a query.

    Additionally, the Session stores object instances using a weak reference
    by default. This also defeats the purpose of using the Session as a cache.

    The :class:`.Session` is not designed to be a
    global object from which everyone consults as a "registry" of objects.
    That's more the job of a **second level cache**.   SQLAlchemy provides
    a pattern for implementing second level caching using `Beaker <http://beaker.groovie.org/>`_, 
    via the :ref:`examples_caching` example.

* How can I get the :class:`~sqlalchemy.orm.session.Session` for a certain object ?

    Use the :func:`~sqlalchemy.orm.session.Session.object_session` classmethod
    available on :class:`~sqlalchemy.orm.session.Session`::

        session = Session.object_session(someobject)

.. index::
   single: thread safety; sessions
   single: thread safety; Session

* Is the session thread-safe?

    Nope. It has no thread synchronization of any kind built in, and
    particularly when you do a flush operation, it definitely is not open to
    concurrent threads accessing it, because it holds onto a single database
    connection at that point. If you use a session which is non-transactional
    (meaning, ``autocommit`` is set to ``True``, not the default setting)
    for read operations only, it's still not thread-"safe", but you also wont
    get any catastrophic failures either, since it checks out and returns
    connections to the connection pool on an as-needed basis; it's just that
    different threads might load the same objects independently of each other,
    but only one will wind up in the identity map (however, the other one
    might still live in a collection somewhere).

    But the bigger point here is, you should not *want* to use the session
    with multiple concurrent threads. That would be like having everyone at a
    restaurant all eat from the same plate. The session is a local "workspace"
    that you use for a specific set of tasks; you don't want to, or need to,
    share that session with other threads who are doing some other task. If,
    on the other hand, there are other threads participating in the same task
    you are, such as in a desktop graphical application, then you would be
    sharing the session with those threads, but you also will have implemented
    a proper locking scheme (or your graphical framework does) so that those
    threads do not collide.

    A multithreaded application is usually going to want to make usage of
    :func:`.scoped_session` to transparently manage sessions per thread.
    More on this at :ref:`unitofwork_contextual`.

Querying
--------

The :func:`~sqlalchemy.orm.session.Session.query` function takes one or more
*entities* and returns a new :class:`~sqlalchemy.orm.query.Query` object which
will issue mapper queries within the context of this Session. An entity is
defined as a mapped class, a :class:`~sqlalchemy.orm.mapper.Mapper` object, an
orm-enabled *descriptor*, or an ``AliasedClass`` object::

    # query from a class
    session.query(User).filter_by(name='ed').all()

    # query with multiple classes, returns tuples
    session.query(User, Address).join('addresses').filter_by(name='ed').all()

    # query using orm-enabled descriptors
    session.query(User.name, User.fullname).all()

    # query from a mapper
    user_mapper = class_mapper(User)
    session.query(user_mapper)

When :class:`~sqlalchemy.orm.query.Query` returns results, each object
instantiated is stored within the identity map. When a row matches an object
which is already present, the same object is returned. In the latter case,
whether or not the row is populated onto an existing object depends upon
whether the attributes of the instance have been *expired* or not. A
default-configured :class:`~sqlalchemy.orm.session.Session` automatically
expires all instances along transaction boundaries, so that with a normally
isolated transaction, there shouldn't be any issue of instances representing
data which is stale with regards to the current transaction.

The :class:`.Query` object is introduced in great detail in
:ref:`ormtutorial_toplevel`, and further documented in
:ref:`query_api_toplevel`.

Adding New or Existing Items
----------------------------

:func:`~sqlalchemy.orm.session.Session.add` is used to place instances in the
session. For *transient* (i.e. brand new) instances, this will have the effect
of an INSERT taking place for those instances upon the next flush. For
instances which are *persistent* (i.e. were loaded by this session), they are
already present and do not need to be added. Instances which are *detached*
(i.e. have been removed from a session) may be re-associated with a session
using this method::

    user1 = User(name='user1')
    user2 = User(name='user2')
    session.add(user1)
    session.add(user2)

    session.commit()     # write changes to the database

To add a list of items to the session at once, use
:func:`~sqlalchemy.orm.session.Session.add_all`::

    session.add_all([item1, item2, item3])

The :func:`~sqlalchemy.orm.session.Session.add` operation **cascades** along
the ``save-update`` cascade. For more details see the section
:ref:`unitofwork_cascades`.

.. _unitofwork_merging:

Merging
-------

:func:`~sqlalchemy.orm.session.Session.merge` reconciles the current state of
an instance and its associated children with existing data in the database,
and returns a copy of the instance associated with the session. Usage is as
follows::

    merged_object = session.merge(existing_object)

When given an instance, it follows these steps:

* It examines the primary key of the instance. If it's present, it attempts
  to load an instance with that primary key (or pulls from the local
  identity map).
* If there's no primary key on the given instance, or the given primary key
  does not exist in the database, a new instance is created.
* The state of the given instance is then copied onto the located/newly
  created instance.
* The operation is cascaded to associated child items along the ``merge``
  cascade. Note that all changes present on the given instance, including
  changes to collections, are merged.
* The new instance is returned.

With :func:`~sqlalchemy.orm.session.Session.merge`, the given instance is not
placed within the session, and can be associated with a different session or
detached. :func:`~sqlalchemy.orm.session.Session.merge` is very useful for
taking the state of any kind of object structure without regard for its
origins or current session associations and placing that state within a
session. Here's two examples:

* An application which reads an object structure from a file and wishes to
  save it to the database might parse the file, build up the
  structure, and then use
  :func:`~sqlalchemy.orm.session.Session.merge` to save it
  to the database, ensuring that the data within the file is
  used to formulate the primary key of each element of the
  structure. Later, when the file has changed, the same
  process can be re-run, producing a slightly different
  object structure, which can then be ``merged`` in again,
  and the :class:`~sqlalchemy.orm.session.Session` will
  automatically update the database to reflect those
  changes.
* A web application stores mapped entities within an HTTP session object.
  When each request starts up, the serialized data can be
  merged into the session, so that the original entity may
  be safely shared among requests and threads.

:func:`~sqlalchemy.orm.session.Session.merge` is frequently used by
applications which implement their own second level caches. This refers to an
application which uses an in memory dictionary, or an tool like Memcached to
store objects over long running spans of time. When such an object needs to
exist within a :class:`~sqlalchemy.orm.session.Session`,
:func:`~sqlalchemy.orm.session.Session.merge` is a good choice since it leaves
the original cached object untouched. For this use case, merge provides a
keyword option called ``load=False``. When this boolean flag is set to
``False``, :func:`~sqlalchemy.orm.session.Session.merge` will not issue any
SQL to reconcile the given object against the current state of the database,
thereby reducing query overhead. The limitation is that the given object and
all of its children may not contain any pending changes, and it's also of
course possible that newer information in the database will not be present on
the merged object, since no load is issued.

Merge Tips
~~~~~~~~~~

:meth:`~.Session.merge` is an extremely useful method for many purposes.  However,
it deals with the intricate border between objects that are transient/detached and
those that are persistent, as well as the automated transferrence of state.
The wide variety of scenarios that can present themselves here often require a
more careful approach to the state of objects.   Common problems with merge usually involve 
some unexpected state regarding the object being passed to :meth:`~.Session.merge`.

Lets use the canonical example of the User and Address objects::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String(50), nullable=False)
        addresses = relationship("Address", backref="user")

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        email_address = Column(String(50), nullable=False)
        user_id = Column(Integer, ForeignKey('user.id'), nullable=False)

Assume a ``User`` object with one ``Address``, already persistent::

    >>> u1 = User(name='ed', addresses=[Address(email_address='ed@ed.com')])
    >>> session.add(u1)
    >>> session.commit()

We now create ``a1``, an object outside the session, which we'd like
to merge on top of the existing ``Address``::

    >>> existing_a1 = u1.addresses[0]
    >>> a1 = Address(id=existing_a1.id)

A surprise would occur if we said this::

    >>> a1.user = u1
    >>> a1 = session.merge(a1)
    >>> session.commit()
    sqlalchemy.orm.exc.FlushError: New instance <Address at 0x1298f50> 
    with identity key (<class '__main__.Address'>, (1,)) conflicts with 
    persistent instance <Address at 0x12a25d0>

Why is that ?   We weren't careful with our cascades.   The assignment
of ``a1.user`` to a persistent object cascaded to the backref of ``User.addresses``
and made our ``a1`` object pending, as though we had added it.   Now we have
*two* ``Address`` objects in the session::

    >>> a1 = Address()
    >>> a1.user = u1
    >>> a1 in session
    True
    >>> existing_a1 in session
    True
    >>> a1 is existing_a1
    False

Above, our ``a1`` is already pending in the session. The
subsequent :meth:`~.Session.merge` operation essentially
does nothing. Cascade can be configured via the ``cascade``
option on :func:`.relationship`, although in this case it
would mean removing the ``save-update`` cascade from the
``User.addresses`` relationship - and usually, that behavior
is extremely convenient.  The solution here would usually be to not assign
``a1.user`` to an object already persistent in the target
session.

The ``cascade_backrefs=False`` option of :func:`.relationship`
will also prevent the ``Address`` from
being added to the session via the ``a1.user = u1`` assignment.

Further detail on cascade operation is at :ref:`unitofwork_cascades`.

Another example of unexpected state::

    >>> a1 = Address(id=existing_a1.id, user_id=u1.id)
    >>> assert a1.user is None
    >>> True
    >>> a1 = session.merge(a1)
    >>> session.commit()
    sqlalchemy.exc.IntegrityError: (IntegrityError) address.user_id 
    may not be NULL

Here, we accessed a1.user, which returned its default value
of ``None``, which as a result of this access, has been placed in the ``__dict__`` of 
our object ``a1``.  Normally, this operation creates no change event, 
so the ``user_id`` attribute takes precedence during a
flush.  But when we merge the ``Address`` object into the session, the operation
is equivalent to::

    >>> existing_a1.id = existing_a1.id
    >>> existing_a1.user_id = u1.id
    >>> existing_a1.user = None

Where above, both ``user_id`` and ``user`` are assigned to, and change events
are emitted for both.  The ``user`` association
takes precedence, and None is applied to ``user_id``, causing a failure.

Most :meth:`~.Session.merge` issues can be examined by first checking - 
is the object prematurely in the session ? 

.. sourcecode:: python+sql

    >>> a1 = Address(id=existing_a1, user_id=user.id)
    >>> assert a1 not in session
    >>> a1 = session.merge(a1)

Or is there state on the object that we don't want ?   Examining ``__dict__``
is a quick way to check::

    >>> a1 = Address(id=existing_a1, user_id=user.id)
    >>> a1.user
    >>> a1.__dict__
    {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at 0x1298d10>, 
        'user_id': 1, 
        'id': 1, 
        'user': None}
    >>> # we don't want user=None merged, remove it
    >>> del a1.user
    >>> a1 = session.merge(a1)
    >>> # success
    >>> session.commit()

Deleting
--------

The :meth:`~.Session.delete` method places an instance
into the Session's list of objects to be marked as deleted::

    # mark two objects to be deleted
    session.delete(obj1)
    session.delete(obj2)

    # commit (or flush)
    session.commit()

Deleting from Collections
~~~~~~~~~~~~~~~~~~~~~~~~~~

A common confusion that arises regarding :meth:`~.Session.delete` is when
objects which are members of a collection are being deleted.   While the
collection member is marked for deletion from the database, this does not
impact the collection itself in memory until the collection is expired.
Below, we illustrate that even after an ``Address`` object is marked
for deletion, it's still present in the collection associated with the
parent ``User``, even after a flush::

    >>> address = user.addresses[1]
    >>> session.delete(address)
    >>> session.flush()
    >>> address in user.addresses
    True

When the above session is committed, all attributes are expired.  The next
access of ``user.addresses`` will re-load the collection, revealing the
desired state::

    >>> session.commit()
    >>> address in user.addresses
    False

The usual practice of deleting items within collections is to forego the usage
of :meth:`~.Session.delete` directly, and instead use cascade behavior to
automatically invoke the deletion as a result of removing the object from
the parent collection.  The ``delete-orphan`` cascade accomplishes this,
as illustrated in the example below::

    mapper(User, users_table, properties={
        'addresses':relationship(Address, cascade="all, delete, delete-orphan")
    })
    del user.addresses[1]
    session.flush()

Where above, upon removing the ``Address`` object from the ``User.addresses`` 
collection, the ``delete-orphan`` cascade has the effect of marking the ``Address``
object for deletion in the same way as passing it to :meth:`~.Session.delete`.

See also :ref:`unitofwork_cascades` for detail on cascades.

Deleting based on Filter Criterion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The caveat with ``Session.delete()`` is that you need to have an object handy
already in order to delete. The Query includes a
:func:`~sqlalchemy.orm.query.Query.delete` method which deletes based on
filtering criteria::

    session.query(User).filter(User.id==7).delete()

The ``Query.delete()`` method includes functionality to "expire" objects
already in the session which match the criteria. However it does have some
caveats, including that "delete" and "delete-orphan" cascades won't be fully
expressed for collections which are already loaded. See the API docs for
:meth:`~sqlalchemy.orm.query.Query.delete` for more details.

Flushing
--------

When the :class:`~sqlalchemy.orm.session.Session` is used with its default
configuration, the flush step is nearly always done transparently.
Specifically, the flush occurs before any individual
:class:`~sqlalchemy.orm.query.Query` is issued, as well as within the
:func:`~sqlalchemy.orm.session.Session.commit` call before the transaction is
committed. It also occurs before a SAVEPOINT is issued when
:func:`~sqlalchemy.orm.session.Session.begin_nested` is used.

Regardless of the autoflush setting, a flush can always be forced by issuing
:func:`~sqlalchemy.orm.session.Session.flush`::

    session.flush()

The "flush-on-Query" aspect of the behavior can be disabled by constructing
:func:`.sessionmaker` with the flag ``autoflush=False``::

    Session = sessionmaker(autoflush=False)

Additionally, autoflush can be temporarily disabled by setting the
``autoflush`` flag at any time::

    mysession = Session()
    mysession.autoflush = False

Some autoflush-disable recipes are available at `DisableAutoFlush
<http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DisableAutoflush>`_.

The flush process *always* occurs within a transaction, even if the
:class:`~sqlalchemy.orm.session.Session` has been configured with
``autocommit=True``, a setting that disables the session's persistent
transactional state. If no transaction is present,
:func:`~sqlalchemy.orm.session.Session.flush` creates its own transaction and
commits it. Any failures during flush will always result in a rollback of
whatever transaction is present. If the Session is not in ``autocommit=True``
mode, an explicit call to :func:`~sqlalchemy.orm.session.Session.rollback` is
required after a flush fails, even though the underlying transaction will have
been rolled back already - this is so that the overall nesting pattern of
so-called "subtransactions" is consistently maintained.

Committing
----------

:func:`~sqlalchemy.orm.session.Session.commit` is used to commit the current
transaction. It always issues :func:`~sqlalchemy.orm.session.Session.flush`
beforehand to flush any remaining state to the database; this is independent
of the "autoflush" setting. If no transaction is present, it raises an error.
Note that the default behavior of the :class:`~sqlalchemy.orm.session.Session`
is that a "transaction" is always present; this behavior can be disabled by
setting ``autocommit=True``. In autocommit mode, a transaction can be
initiated by calling the :func:`~sqlalchemy.orm.session.Session.begin` method.

.. note:: 

   The term "transaction" here refers to a transactional
   construct within the :class:`.Session` itself which may be
   maintaining zero or more actual database (DBAPI) transactions.  An individual
   DBAPI connection begins participation in the "transaction" as it is first
   used to execute a SQL statement, then remains present until the session-level
   "transaction" is completed.  See :ref:`unitofwork_transaction` for 
   further detail.

Another behavior of :func:`~sqlalchemy.orm.session.Session.commit` is that by
default it expires the state of all instances present after the commit is
complete. This is so that when the instances are next accessed, either through
attribute access or by them being present in a
:class:`~sqlalchemy.orm.query.Query` result set, they receive the most recent
state. To disable this behavior, configure
:func:`.sessionmaker` with ``expire_on_commit=False``.

Normally, instances loaded into the :class:`~sqlalchemy.orm.session.Session`
are never changed by subsequent queries; the assumption is that the current
transaction is isolated so the state most recently loaded is correct as long
as the transaction continues. Setting ``autocommit=True`` works against this
model to some degree since the :class:`~sqlalchemy.orm.session.Session`
behaves in exactly the same way with regard to attribute state, except no
transaction is present.

Rolling Back
------------

:func:`~sqlalchemy.orm.session.Session.rollback` rolls back the current
transaction. With a default configured session, the post-rollback state of the
session is as follows:

  * All transactions are rolled back and all connections returned to the
    connection pool, unless the Session was bound directly to a Connection, in
    which case the connection is still maintained (but still rolled back).
  * Objects which were initially in the *pending* state when they were added
    to the :class:`~sqlalchemy.orm.session.Session` within the lifespan of the
    transaction are expunged, corresponding to their INSERT statement being
    rolled back. The state of their attributes remains unchanged.
  * Objects which were marked as *deleted* within the lifespan of the
    transaction are promoted back to the *persistent* state, corresponding to
    their DELETE statement being rolled back. Note that if those objects were
    first *pending* within the transaction, that operation takes precedence
    instead.
  * All objects not expunged are fully expired.

With that state understood, the :class:`~sqlalchemy.orm.session.Session` may
safely continue usage after a rollback occurs.

When a :func:`~sqlalchemy.orm.session.Session.flush` fails, typically for
reasons like primary key, foreign key, or "not nullable" constraint
violations, a :func:`~sqlalchemy.orm.session.Session.rollback` is issued
automatically (it's currently not possible for a flush to continue after a
partial failure). However, the flush process always uses its own transactional
demarcator called a *subtransaction*, which is described more fully in the
docstrings for :class:`~sqlalchemy.orm.session.Session`. What it means here is
that even though the database transaction has been rolled back, the end user
must still issue :func:`~sqlalchemy.orm.session.Session.rollback` to fully
reset the state of the :class:`~sqlalchemy.orm.session.Session`.

Expunging
---------

Expunge removes an object from the Session, sending persistent instances to
the detached state, and pending instances to the transient state:

.. sourcecode:: python+sql

    session.expunge(obj1)

To remove all items, call :func:`~sqlalchemy.orm.session.Session.expunge_all`
(this method was formerly known as ``clear()``).

Closing
-------

The :func:`~sqlalchemy.orm.session.Session.close` method issues a
:func:`~sqlalchemy.orm.session.Session.expunge_all`, and releases any
transactional/connection resources. When connections are returned to the
connection pool, transactional state is rolled back as well.

Refreshing / Expiring
---------------------

The Session normally works in the context of an ongoing transaction (with the
default setting of autoflush=False). Most databases offer "isolated"
transactions - this refers to a series of behaviors that allow the work within
a transaction to remain consistent as time passes, regardless of the
activities outside of that transaction. A key feature of a high degree of
transaction isolation is that emitting the same SELECT statement twice will
return the same results as when it was called the first time, even if the data
has been modified in another transaction.

For this reason, the :class:`.Session` gains very efficient behavior by 
loading the attributes of each instance only once.   Subsequent reads of the 
same row in the same transaction are assumed to have the same value.  The
user application also gains directly from this assumption, that the transaction
is regarded as a temporary shield against concurrent changes - a good application
will ensure that isolation levels are set appropriately such that this assumption
can be made, given the kind of data being worked with.

To clear out the currently loaded state on an instance, the instance or its individual
attributes can be marked as "expired", which results in a reload to
occur upon next access of any of the instance's attrbutes.  The instance
can also be immediately reloaded from the database.   The :meth:`~.Session.expire`
and :meth:`~.Session.refresh` methods achieve this::

    # immediately re-load attributes on obj1, obj2
    session.refresh(obj1)
    session.refresh(obj2)

    # expire objects obj1, obj2, attributes will be reloaded
    # on the next access:
    session.expire(obj1)
    session.expire(obj2)

When an expired object reloads, all non-deferred column-based attributes are
loaded in one query. Current behavior for expired relationship-based
attributes is that they load individually upon access - this behavior may be
enhanced in a future release. When a refresh is invoked on an object, the
ultimate operation is equivalent to a :meth:`.Query.get`, so any relationships
configured with eager loading should also load within the scope of the refresh
operation.

:meth:`~.Session.refresh` and
:meth:`~.Session.expire` also support being passed a
list of individual attribute names in which to be refreshed. These names can
refer to any attribute, column-based or relationship based::

    # immediately re-load the attributes 'hello', 'world' on obj1, obj2
    session.refresh(obj1, ['hello', 'world'])
    session.refresh(obj2, ['hello', 'world'])

    # expire the attributes 'hello', 'world' objects obj1, obj2, attributes will be reloaded
    # on the next access:
    session.expire(obj1, ['hello', 'world'])
    session.expire(obj2, ['hello', 'world'])

The full contents of the session may be expired at once using
:meth:`~.Session.expire_all`::

    session.expire_all()

Note that :meth:`~.Session.expire_all` is called **automatically** whenever
:meth:`~.Session.commit` or :meth:`~.Session.rollback` are called. If using the
session in its default mode of autocommit=False and with a well-isolated
transactional environment (which is provided by most backends with the notable
exception of MySQL MyISAM), there is virtually *no reason* to ever call
:meth:`~.Session.expire_all` directly - plenty of state will remain on the
current transaction until it is rolled back or committed or otherwise removed.

:meth:`~.Session.refresh` and :meth:`~.Session.expire` similarly are usually
only necessary when an UPDATE or DELETE has been issued manually within the
transaction using :meth:`.Session.execute()`.

Session Attributes
------------------

The :class:`~sqlalchemy.orm.session.Session` itself acts somewhat like a
set-like collection. All items present may be accessed using the iterator
interface::

    for obj in session:
        print obj

And presence may be tested for using regular "contains" semantics::

    if obj in session:
        print "Object is present"

The session is also keeping track of all newly created (i.e. pending) objects,
all objects which have had changes since they were last loaded or saved (i.e.
"dirty"), and everything that's been marked as deleted::

    # pending objects recently added to the Session
    session.new

    # persistent objects which currently have changes detected
    # (this collection is now created on the fly each time the property is called)
    session.dirty

    # persistent objects that have been marked as deleted via session.delete(obj)
    session.deleted

Note that objects within the session are by default *weakly referenced*. This
means that when they are dereferenced in the outside application, they fall
out of scope from within the :class:`~sqlalchemy.orm.session.Session` as well
and are subject to garbage collection by the Python interpreter. The
exceptions to this include objects which are pending, objects which are marked
as deleted, or persistent objects which have pending changes on them. After a
full flush, these collections are all empty, and all objects are again weakly
referenced. To disable the weak referencing behavior and force all objects
within the session to remain until explicitly expunged, configure
:func:`.sessionmaker` with the ``weak_identity_map=False``
setting.

.. _unitofwork_cascades:

Cascades
========

Mappers support the concept of configurable **cascade** behavior on
:func:`~sqlalchemy.orm.relationship` constructs.  This refers
to how operations performed on a parent object relative to a
particular :class:`.Session` should be propagated to items
referred to by that relationship.
The default cascade behavior is usually suitable for 
most situations, and the option is normally invoked explicitly
in order to enable ``delete`` and ``delete-orphan`` cascades,
which refer to how the relationship should be treated when
the parent is marked for deletion as well as when a child
is de-associated from its parent.

Cascade behavior is configured by setting the ``cascade`` keyword 
argument on 
:func:`~sqlalchemy.orm.relationship`::

    class Order(Base):
        __tablename__ = 'order'

        items = relationship("Item", cascade="all, delete-orphan")
        customer = relationship("User", secondary=user_orders_table, 
                                    cascade="save-update")

To set cascades on a backref, the same flag can be used with the
:func:`~.sqlalchemy.orm.backref` function, which ultimately feeds
its arguments back into :func:`~sqlalchemy.orm.relationship`::

    class Item(Base):
        __tablename__ = 'item'

        order = relationship("Order", 
                        backref=backref("items", cascade="all, delete-orphan")
                    )

The default value of ``cascade`` is ``save-update, merge``.  
The ``all`` symbol in the cascade options indicates that all
cascade flags should be enabled, with the exception of ``delete-orphan``.
Typically, cascade is usually left at its default, or configured
as ``all, delete-orphan``, indicating the child objects should be
treated as "owned" by the parent.

The list of available values which can be specified in ``cascade``
are as follows:

* ``save-update`` - Indicates that when an object is placed into a 
  :class:`.Session`
  via :meth:`.Session.add`, all the objects associated with it via this
  :func:`~sqlalchemy.orm.relationship` should also be added to that
  same :class:`.Session`.   Additionally, if this object is already present in
  a :class:`.Session`, child objects will be added to that session as they
  are associated with this parent, i.e. as they are appended to lists,
  added to sets, or otherwise associated with the parent.

  ``save-update`` cascade also cascades the *pending history* of the
  target attribute, meaning that objects which were 
  removed from a scalar or collection attribute whose changes have not 
  yet been flushed are  also placed into the target session.  This
  is because they may have foreign key attributes present which
  will need to be updated to no longer refer to the parent. 

  The ``save-update`` cascade is on by default, and it's common to not
  even be aware of it.  It's customary that only a single call to
  :meth:`.Session.add` against the lead object of a structure
  has the effect of placing the full structure of 
  objects into the :class:`.Session` at once.

  However, it can be turned off, which would 
  imply that objects associated with a parent would need to be 
  placed individually using :meth:`.Session.add` calls for
  each one.

  Another default behavior of ``save-update`` cascade is that it will
  take effect in the reverse direction, that is, associating a child
  with a parent when a backref is present means both relationships
  are affected; the parent will be added to the child's session.
  To disable this somewhat indirect session addition, use the 
  ``cascade_backrefs=False`` option described below in 
  :ref:`backref_cascade`.

* ``delete`` - This cascade indicates that when the parent object
  is marked for deletion, the related objects should also be marked 
  for deletion.   Without this cascade present, SQLAlchemy will
  set the foreign key on a one-to-many relationship to NULL 
  when the parent object is deleted.  When enabled, the row is instead
  deleted.

  ``delete`` cascade is often used in conjunction with ``delete-orphan``
  cascade, as is appropriate for an object whose foreign key is
  not intended to be nullable.  On some backends, it's also 
  a good idea to set ``ON DELETE`` on the foreign key itself;
  see the section :ref:`passive_deletes` for more details.

  Note that for many-to-many relationships which make usage of the 
  ``secondary`` argument to :func:`~.sqlalchemy.orm.relationship`, 
  SQLAlchemy always emits
  a DELETE for the association row in between "parent" and "child",
  when the parent is deleted or whenever the linkage between a particular
  parent and child is broken.

* ``delete-orphan`` - This cascade adds behavior to the ``delete`` cascade,
  such that a child object will be marked for deletion when it is
  de-associated from the parent, not just when the parent is marked
  for deletion.   This is a common feature when dealing with a related
  object that is "owned" by its parent, with a NOT NULL foreign key,
  so that removal of the item from the parent collection results
  in its deletion.   

  ``delete-orphan`` cascade implies that each child object can only 
  have one parent at a time, so is configured in the vast majority of cases
  on a one-to-many relationship.   Setting it on a many-to-one or 
  many-to-many relationship is more awkward; for this use case, 
  SQLAlchemy requires that the :func:`~sqlalchemy.orm.relationship`
  be configured with the ``single_parent=True`` function, which 
  establishes Python-side validation that ensures the object
  is associated with only one parent at a time.

* ``merge`` - This cascade indicates that the :meth:`.Session.merge`
  operation should be propagated from a parent that's the subject
  of the :meth:`.Session.merge` call down to referred objects. 
  This cascade is also on by default.

* ``refresh-expire`` - A less common option, indicates that the
  :meth:`.Session.expire` operation should be propagated from a parent
  down to referred objects.   When using :meth:`.Session.refresh`,
  the referred objects are expired only, but not actually refreshed.

* ``expunge`` - Indicate that when the parent object is removed
  from the :class:`.Session` using :meth:`.Session.expunge`, the 
  operation should be propagated down to referred objects.

.. _backref_cascade:

Controlling Cascade on Backrefs
-------------------------------

The ``save-update`` cascade takes place on backrefs by default.   This means
that, given a mapping such as this::

    mapper(Order, order_table, properties={
        'items' : relationship(Item, backref='order')
    })

If an ``Order`` is already in the session, and is assigned to the ``order``
attribute of an ``Item``, the backref appends the ``Order`` to the ``items``
collection of that ``Order``, resulting in the ``save-update`` cascade taking
place::

    >>> o1 = Order()
    >>> session.add(o1)
    >>> o1 in session
    True

    >>> i1 = Item()
    >>> i1.order = o1
    >>> i1 in o1.items
    True
    >>> i1 in session
    True

This behavior can be disabled using the ``cascade_backrefs`` flag::

    mapper(Order, order_table, properties={
        'items' : relationship(Item, backref='order', 
                                    cascade_backrefs=False)
    })

So above, the assignment of ``i1.order = o1`` will append ``i1`` to the ``items``
collection of ``o1``, but will not add ``i1`` to the session.   You can, of
course, :func:`~.Session.add` ``i1`` to the session at a later point.   This option
may be helpful for situations where an object needs to be kept out of a
session until it's construction is completed, but still needs to be given
associations to objects which are already persistent in the target session.


.. _unitofwork_transaction:

Managing Transactions
=====================

A newly constructed :class:`.Session` may be said to be in the "begin" state.
In this state, the :class:`.Session` has not established any connection or 
transactional state with any of the :class:`.Engine` objects that may be associated
with it.

The :class:`.Session` then receives requests to operate upon a database connection.
Typically, this means it is called upon to execute SQL statements using a particular
:class:`.Engine`, which may be via :meth:`.Session.query`, :meth:`.Session.execute`,
or within a flush operation of pending data, which occurs when such state exists
and :meth:`.Session.commit` or :meth:`.Session.flush` is called.

As these requests are received, each new :class:`.Engine` encountered is associated 
with an ongoing transactional state maintained by the :class:`.Session`.
When the first :class:`.Engine` is operated upon, the :class:`.Session` can be said
to have left the "begin" state and entered "transactional" state.   For each
:class:`.Engine` encountered, a :class:`.Connection` is associated with it,
which is acquired via the :meth:`.Engine.contextual_connect` method.  If a
:class:`.Connection` was directly associated with the :class:`.Session` (see :ref:`session_external_transaction`
for an example of this), it is 
added to the transactional state directly.

For each :class:`.Connection`, the :class:`.Session` also maintains a :class:`.Transaction` object, 
which is acquired by calling :meth:`.Connection.begin` on each :class:`.Connection`,
or if the :class:`.Session`
object has been established using the flag ``twophase=True``, a :class:`.TwoPhaseTransaction`
object acquired via :meth:`.Connection.begin_twophase`.  These transactions are all committed or 
rolled back corresponding to the invocation of the 
:meth:`.Session.commit` and :meth:`.Session.rollback` methods.   A commit operation will
also call the :meth:`.TwoPhaseTransaction.prepare` method on all transactions if applicable.

When the transactional state is completed after a rollback or commit, the :class:`.Session`
releases all :class:`.Transaction` and :class:`.Connection` resources (which has the effect
of returning DBAPI connections to the connection pool of each :class:`.Engine`), 
and goes back to the "begin" state, which
will again invoke new :class:`.Connection` and :class:`.Transaction` objects as new 
requests to emit SQL statements are received.

The example below illustrates this lifecycle::

    engine = create_engine("...")
    Session = sessionmaker(bind=engine)

    # new session.   no connections are in use.
    session = Session()
    try:
        # first query.  a Connection is acquired
        # from the Engine, and a Transaction 
        # started.
        item1 = session.query(Item).get(1)

        # second query.  the same Connection/Transaction
        # are used.
        item2 = session.query(Item).get(2)

        # pending changes are created.
        item1.foo = 'bar'
        item2.bar = 'foo'

        # commit.  The pending changes above
        # are flushed via flush(), the Transaction
        # is committed, the Connection object closed
        # and discarded, the underlying DBAPI connection
        # returned to the connection pool.
        session.commit()
    except:
        # on rollback, the same closure of state
        # as that of commit proceeds.
        session.rollback()
        raise

.. _session_begin_nested:

Using SAVEPOINT
---------------

SAVEPOINT transactions, if supported by the underlying engine, may be
delineated using the :func:`~sqlalchemy.orm.session.Session.begin_nested`
method::

    Session = sessionmaker()
    session = Session()
    session.add(u1)
    session.add(u2)

    session.begin_nested() # establish a savepoint
    session.add(u3)
    session.rollback()  # rolls back u3, keeps u1 and u2

    session.commit() # commits u1 and u2

:func:`~sqlalchemy.orm.session.Session.begin_nested` may be called any number
of times, which will issue a new SAVEPOINT with a unique identifier for each
call. For each :func:`~sqlalchemy.orm.session.Session.begin_nested` call, a
corresponding :func:`~sqlalchemy.orm.session.Session.rollback` or
:func:`~sqlalchemy.orm.session.Session.commit` must be issued.

When :func:`~sqlalchemy.orm.session.Session.begin_nested` is called, a
:func:`~sqlalchemy.orm.session.Session.flush` is unconditionally issued
(regardless of the ``autoflush`` setting). This is so that when a
:func:`~sqlalchemy.orm.session.Session.rollback` occurs, the full state of the
session is expired, thus causing all subsequent attribute/instance access to
reference the full state of the :class:`~sqlalchemy.orm.session.Session` right
before :func:`~sqlalchemy.orm.session.Session.begin_nested` was called.

Autocommit Mode
---------------

The example of :class:`.Session` transaction lifecycle illustrated at
the start of :ref:`unitofwork_transaction` applies to a :class:`.Session` configured in the
default mode of ``autocommit=False``.   Constructing a :class:`.Session`
with ``autocommit=True`` produces a :class:`.Session` placed into "autocommit" mode, where each SQL statement
invoked by a :meth:`.Session.query` or :meth:`.Session.execute` occurs
using a new connection from the connection pool, discarding it after
results have been iterated.   The :meth:`.Session.flush` operation
still occurs within the scope of a single transaction, though this transaction
is closed out after the :meth:`.Session.flush` operation completes.

"autocommit" mode should **not be considered for general use**.   While
very old versions of SQLAlchemy standardized on this mode, the modern
:class:`.Session` benefits highly from being given a clear point of transaction
demarcation via :meth:`.Session.rollback` and :meth:`.Session.commit`.
The autoflush action can safely emit SQL to the database as needed without
implicitly producing permanent effects, the contents of attributes 
are expired only when a logical series of steps has completed.   If the
:class:`.Session` were to be used in pure "autocommit" mode without 
an ongoing transaction, these features should be disabled, that is,
``autoflush=False, expire_on_commit=False``.

Modern usage of "autocommit" is for framework integrations that need to control
specifically when the "begin" state occurs.  A session which is configured with 
``autocommit=True`` may be placed into the "begin" state using the 
:meth:`.Session.begin` method.
After the cycle completes upon :meth:`.Session.commit` or :meth:`.Session.rollback`,
connection and transaction resources are released and the :class:`.Session`
goes back into "autocommit" mode, until :meth:`.Session.begin` is called again::

    Session = sessionmaker(bind=engine, autocommit=True)
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

The :func:`.Session.begin` method also returns a
transactional token which is compatible with the Python 2.6 ``with``
statement::

    Session = sessionmaker(bind=engine, autocommit=True)
    session = Session()
    with session.begin():
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'

.. _session_subtransactions:

Using Subtransactions with Autocommit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A subtransaction indicates usage of the :meth:`.Session.begin` method in conjunction with 
the ``subtransactions=True`` flag.  This produces a a non-transactional, delimiting construct that
allows nesting of calls to :meth:`~.Session.begin` and :meth:`~.Session.commit`.
It's purpose is to allow the construction of code that can function within a transaction
both independently of any external code that starts a transaction,
as well as within a block that has already demarcated a transaction. 

``subtransactions=True`` is generally only useful in conjunction with 
autocommit, and is equivalent to the pattern described at :ref:`connections_nested_transactions`, 
where any number of functions can call :meth:`.Connection.begin` and :meth:`.Transaction.commit`
as though they are the initiator of the transaction, but in fact may be participating
in an already ongoing transaction::

    # method_a starts a transaction and calls method_b
    def method_a(session):
        session.begin(subtransactions=True)
        try:
            method_b(session)
            session.commit()  # transaction is committed here
        except:
            session.rollback() # rolls back the transaction
            raise

    # method_b also starts a transaction, but when
    # called from method_a participates in the ongoing
    # transaction.
    def method_b(session):
        session.begin(subtransactions=True)
        try:
            session.add(SomeObject('bat', 'lala'))
            session.commit()  # transaction is not committed yet
        except:
            session.rollback() # rolls back the transaction, in this case
                               # the one that was initiated in method_a().
            raise

    # create a Session and call method_a
    session = Session(autocommit=True)
    method_a(session)
    session.close()

Subtransactions are used by the :meth:`.Session.flush` process to ensure that the
flush operation takes place within a transaction, regardless of autocommit.   When
autocommit is disabled, it is still useful in that it forces the :class:`.Session`
into a "pending rollback" state, as a failed flush cannot be resumed in mid-operation,
where the end user still maintains the "scope" of the transaction overall.

.. _session_twophase:

Enabling Two-Phase Commit
-------------------------

For backends which support two-phase operaration (currently MySQL and
PostgreSQL), the session can be instructed to use two-phase commit semantics.
This will coordinate the committing of transactions across databases so that
the transaction is either committed or rolled back in all databases. You can
also :func:`~sqlalchemy.orm.session.Session.prepare` the session for
interacting with transactions not managed by SQLAlchemy. To use two phase
transactions set the flag ``twophase=True`` on the session::

    engine1 = create_engine('postgresql://db1')
    engine2 = create_engine('postgresql://db2')

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

This feature allows the value of a database column to be set to a SQL
expression instead of a literal value. It's especially useful for atomic
updates, calling stored procedures, etc. All you do is assign an expression to
an attribute::

    class SomeClass(object):
        pass
    mapper(SomeClass, some_table)

    someobject = session.query(SomeClass).get(5)

    # set 'value' attribute to a SQL expression adding one
    someobject.value = some_table.c.value + 1

    # issues "UPDATE some_table SET value=value+1"
    session.commit()

This technique works both for INSERT and UPDATE statements. After the
flush/commit operation, the ``value`` attribute on ``someobject`` above is
expired, so that when next accessed the newly generated value will be loaded
from the database.

Using SQL Expressions with Sessions
====================================

SQL expressions and strings can be executed via the
:class:`~sqlalchemy.orm.session.Session` within its transactional context.
This is most easily accomplished using the
:func:`~sqlalchemy.orm.session.Session.execute` method, which returns a
:class:`~sqlalchemy.engine.base.ResultProxy` in the same manner as an
:class:`~sqlalchemy.engine.base.Engine` or
:class:`~sqlalchemy.engine.base.Connection`::

    Session = sessionmaker(bind=engine)
    session = Session()

    # execute a string statement
    result = session.execute("select * from table where id=:id", {'id':7})

    # execute a SQL expression construct
    result = session.execute(select([mytable]).where(mytable.c.id==7))

The current :class:`~sqlalchemy.engine.base.Connection` held by the
:class:`~sqlalchemy.orm.session.Session` is accessible using the
:func:`~sqlalchemy.orm.session.Session.connection` method::

    connection = session.connection()

The examples above deal with a :class:`~sqlalchemy.orm.session.Session` that's
bound to a single :class:`~sqlalchemy.engine.base.Engine` or
:class:`~sqlalchemy.engine.base.Connection`. To execute statements using a
:class:`~sqlalchemy.orm.session.Session` which is bound either to multiple
engines, or none at all (i.e. relies upon bound metadata), both
:func:`~sqlalchemy.orm.session.Session.execute` and
:func:`~sqlalchemy.orm.session.Session.connection` accept a ``mapper`` keyword
argument, which is passed a mapped class or
:class:`~sqlalchemy.orm.mapper.Mapper` instance, which is used to locate the
proper context for the desired engine::

    Session = sessionmaker()
    session = Session()

    # need to specify mapper or class when executing
    result = session.execute("select * from table where id=:id", {'id':7}, mapper=MyMappedClass)

    result = session.execute(select([mytable], mytable.c.id==7), mapper=MyMappedClass)

    connection = session.connection(MyMappedClass)

.. _session_external_transaction:

Joining a Session into an External Transaction
===============================================

If a :class:`.Connection` is being used which is already in a transactional
state (i.e. has a :class:`.Transaction` established), a :class:`.Session` can
be made to participate within that transaction by just binding the
:class:`.Session` to that :class:`.Connection`. The usual rationale for this
is a test suite that allows ORM code to work freely with a :class:`.Session`,
including the ability to call :meth:`.Session.commit`, where afterwards the
entire database interaction is rolled back::

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    from unittest import TestCase

    # global application scope.  create Session class, engine
    Session = sessionmaker()

    engine = create_engine('postgresql://...')

    class SomeTest(TestCase):
        def setUp(self):
            # connect to the database
            self.connection = engine.connect()

            # begin a non-ORM transaction
            self.trans = connection.begin()

            # bind an individual Session to the connection
            self.session = Session(bind=self.connection)

        def test_something(self):
            # use the session in tests.

            self.session.add(Foo())
            self.session.commit()

        def tearDown(self):
            # rollback - everything that happened with the
            # Session above (including calls to commit()) 
            # is rolled back.
            self.trans.rollback()
            self.session.close()

Above, we issue :meth:`.Session.commit` as well as
:meth:`.Transaction.rollback`. This is an example of where we take advantage
of the :class:`.Connection` object's ability to maintain *subtransactions*, or
nested begin/commit-or-rollback pairs where only the outermost begin/commit
pair actually commits the transaction, or if the outermost block rolls back,
everything is rolled back.

.. _unitofwork_contextual:

Contextual/Thread-local Sessions
=================================

A common need in applications, particularly those built around web frameworks,
is the ability to "share" a :class:`~sqlalchemy.orm.session.Session` object
among disparate parts of an application, without needing to pass the object
explicitly to all method and function calls. What you're really looking for is
some kind of "global" session object, or at least "global" to all the parts of
an application which are tasked with servicing the current request. For this
pattern, SQLAlchemy provides the ability to enhance the
:class:`~sqlalchemy.orm.session.Session` class generated by
:func:`.sessionmaker` to provide auto-contextualizing support.
This means that whenever you create a :class:`~sqlalchemy.orm.session.Session`
instance with its constructor, you get an *existing*
:class:`~sqlalchemy.orm.session.Session` object which is bound to some
"context". By default, this context is the current thread. This feature is
what previously was accomplished using the ``sessioncontext`` SQLAlchemy
extension.

Creating a Thread-local Context
-------------------------------

The :func:`~sqlalchemy.orm.scoped_session` function wraps around the
:func:`.sessionmaker` function, and produces an object which
behaves the same as the :class:`~sqlalchemy.orm.session.Session` subclass
returned by :func:`.sessionmaker`::

    from sqlalchemy.orm import scoped_session, sessionmaker
    Session = scoped_session(sessionmaker())

However, when you instantiate this :class:`~sqlalchemy.orm.session.Session`
"class", in reality the object is pulled from a threadlocal variable, or if it
doesn't exist yet, it's created using the underlying class generated by
:func:`.sessionmaker`::

    >>> # call Session() the first time.  the new Session instance is created.
    >>> session = Session()

    >>> # later, in the same application thread, someone else calls Session()
    >>> session2 = Session()

    >>> # the two Session objects are *the same* object
    >>> session is session2
    True

Since the :class:`~sqlalchemy.orm.session.Session()` constructor now returns
the same :class:`~sqlalchemy.orm.session.Session` object every time within the
current thread, the object returned by :func:`~sqlalchemy.orm.scoped_session`
also implements most of the :class:`~sqlalchemy.orm.session.Session` methods
and properties at the "class" level, such that you don't even need to
instantiate :class:`~sqlalchemy.orm.session.Session()`::

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

After ``remove()`` is called, the next operation with the contextual session
will start a new :class:`~sqlalchemy.orm.session.Session` for the current
thread.

.. _session_lifespan:

Lifespan of a Contextual Session
--------------------------------

A (really, really) common question is when does the contextual session get
created, when does it get disposed ? We'll consider a typical lifespan as used
in a web application::

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

The above example illustrates an explicit call to :meth:`.ScopedSession.remove`. This
has the effect such that each web request starts fresh with a brand new
session, and is the most definitive approach to closing out a request.

It's not strictly necessary to remove the session at the end of the request -
other options include calling :meth:`.Session.close`, :meth:`.Session.rollback`,
:meth:`.Session.commit` at the end so that the existing session returns 
its connections to the pool and removes any existing transactional context.
Doing nothing is an option too, if individual controller methods take responsibility
for ensuring that no transactions remain open after a request ends.

Contextual Session API
----------------------

.. autofunction:: sqlalchemy.orm.scoped_session

.. autoclass:: sqlalchemy.orm.scoping.ScopedSession
   :members:

.. autoclass:: sqlalchemy.util.ScopedRegistry
    :members:

.. autoclass:: sqlalchemy.util.ThreadLocalRegistry

.. _session_partitioning:

Partitioning Strategies
=======================

Simple Vertical Partitioning
----------------------------

Vertical partitioning places different kinds of objects, or different tables,
across multiple databases::

    engine1 = create_engine('postgresql://db1')
    engine2 = create_engine('postgresql://db2')

    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

    session = Session()

Above, operations against either class will make usage of the :class:`.Engine`
linked to that class.   Upon a flush operation, similar rules take place
to ensure each class is written to the right database.

The transactions among the multiple databases can optionally be coordinated
via two phase commit, if the underlying backend supports it.  See
:ref:`session_twophase` for an example.

Custom Vertical Partitioning
----------------------------

More comprehensive rule-based class-level partitioning can be built by
overriding the :meth:`.Session.get_bind` method.   Below we illustrate
a custom :class:`.Session` which delivers the following rules:

1. Flush operations are delivered to the engine named ``master``.

2. Operations on objects that subclass ``MyOtherClass`` all 
   occur on the ``other`` engine.

3. Read operations for all other classes occur on a random
   choice of the ``slave1`` or ``slave2`` database.

::

    engines = {
        'master':create_engine("sqlite:///master.db"),
        'other':create_engine("sqlite:///other.db"),
        'slave1':create_engine("sqlite:///slave1.db"),
        'slave2':create_engine("sqlite:///slave2.db"),
    }

    from sqlalchemy.orm import Session, sessionmaker
    import random

    class RoutingSession(Session):
        def get_bind(self, mapper=None, clause=None):
            if mapper and issubclass(mapper.class_, MyOtherClass):
                return engines['other']
            elif self._flushing:
                return engines['master']
            else:
                return engines[
                    random.choice(['slave1','slave2'])
                ]

The above :class:`.Session` class is plugged in using the ``class_``
argument to :func:`.sessionmaker`::

    Session = sessionmaker(class_=RoutingSession)

This approach can be combined with multiple :class:`.MetaData` objects,
using an approach such as that of using the declarative ``__abstract__`` 
keyword, described at :ref:`declarative_abstract`.

Horizontal Partitioning
-----------------------

Horizontal partitioning partitions the rows of a single table (or a set of
tables) across multiple databases.

See the "sharding" example: :ref:`examples_sharding`.

Sessions API
============

Session and sessionmaker()
---------------------------

.. autofunction:: sessionmaker

.. autoclass:: sqlalchemy.orm.session.Session
   :members:

.. autoclass:: sqlalchemy.orm.session.SessionTransaction
   :members:

Session Utilites
----------------

.. autofunction:: make_transient

.. autofunction:: object_session

Attribute and State Management Utilities
-----------------------------------------

These functions are provided by the SQLAlchemy attribute
instrumentation API to provide a detailed interface for dealing
with instances, attribute values, and history.  Some of them
are useful when constructing event listener functions, such as 
those described in :ref:`events_orm_toplevel`.

.. currentmodule:: sqlalchemy.orm.attributes

.. autofunction:: del_attribute

.. autofunction:: get_attribute

.. autofunction:: get_history

.. autofunction:: init_collection

.. autofunction:: flag_modified

.. function:: instance_state

    Return the :class:`.InstanceState` for a given object.

.. autofunction:: is_instrumented

.. function:: manager_of_class

    Return the :class:`.ClassManager` for a given class.

.. autofunction:: set_attribute

.. autofunction:: set_committed_value

.. autoclass:: History
    :members:

.. autodata:: PASSIVE_NO_INITIALIZE

.. autodata:: PASSIVE_NO_FETCH

.. autodata:: PASSIVE_NO_FETCH_RELATED

.. autodata:: PASSIVE_ONLY_PERSISTENT

.. autodata:: PASSIVE_OFF



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
usually, you'd re-associate detached objects with another :class:`.Session` when you
want to work with them again, so that they can resume their normal task of
representing database state.

.. _session_getting:

Getting a Session
=================

:class:`.Session` is a regular Python class which can
be directly instantiated. However, to standardize how sessions are configured
and acquired, the :class:`.sessionmaker` class is normally
used to create a top level :class:`.Session`
configuration which can then be used throughout an application without the
need to repeat the configurational arguments.

The usage of :class:`.sessionmaker` is illustrated below:

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

Above, the :class:`.sessionmaker` call creates a factory for us,
which we assign to the name ``Session``.  This factory, when
called, will create a new :class:`.Session` object using the configurational
arguments we've given the factory.  In this case, as is typical,
we've configured the factory to specify a particular :class:`.Engine` for
connection resources.

A typical setup will associate the :class:`.sessionmaker` with an :class:`.Engine`,
so that each :class:`.Session` generated will use this :class:`.Engine`
to acquire connection resources.   This association can
be set up as in the example above, using the ``bind`` argument.

When you write your application, place the
:class:`.sessionmaker` factory at the global level.   This
factory can then
be used by the rest of the applcation as the source of new :class:`.Session`
instances, keeping the configuration for how :class:`.Session` objects
are constructed in one place.

The :class:`.sessionmaker` factory can also be used in conjunction with
other helpers, which are passed a user-defined :class:`.sessionmaker` that
is then maintained by the helper.  Some of these helpers are discussed in the
section :ref:`session_faq_whentocreate`.

Adding Additional Configuration to an Existing sessionmaker()
--------------------------------------------------------------

A common scenario is where the :class:`.sessionmaker` is invoked
at module import time, however the generation of one or more :class:`.Engine`
instances to be associated with the :class:`.sessionmaker` has not yet proceeded.
For this use case, the :class:`.sessionmaker` construct offers the
:meth:`.sessionmaker.configure` method, which will place additional configuration
directives into an existing :class:`.sessionmaker` that will take place
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
:class:`.sessionmaker` factory's :meth:`.sessionmaker.__call__` method.
These arguments will override whatever
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

* **Transient** - an instance that's not in a session, and is not saved to the
  database; i.e. it has no database identity. The only relationship such an
  object has to the ORM is that its class has a ``mapper()`` associated with
  it.

* **Pending** - when you :meth:`~.Session.add` a transient
  instance, it becomes pending. It still wasn't actually flushed to the
  database yet, but it will be when the next flush occurs.

* **Persistent** - An instance which is present in the session and has a record
  in the database. You get persistent instances by either flushing so that the
  pending instances become persistent, or by querying the database for
  existing instances (or moving persistent instances from other sessions into
  your local session).

* **Detached** - an instance which has a record in the database, but is not in
  any session. There's nothing wrong with this, and you can use objects
  normally when they're detached, **except** they will not be able to issue
  any SQL in order to load collections or attributes which are not yet loaded,
  or were marked as "expired".

Knowing these states is important, since the
:class:`.Session` tries to be strict about ambiguous
operations (such as trying to save the same object to two different sessions
at the same time).

Getting the Current State of an Object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The actual state of any mapped object can be viewed at any time using
the :func:`.inspect` system::

  >>> from sqlalchemy import inspect
  >>> insp = inspect(my_object)
  >>> insp.persistent
  True

.. seealso::

    :attr:`.InstanceState.transient`

    :attr:`.InstanceState.pending`

    :attr:`.InstanceState.persistent`

    :attr:`.InstanceState.detached`


.. _session_faq:

Session Frequently Asked Questions
-----------------------------------


When do I make a :class:`.sessionmaker`?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Just one time, somewhere in your application's global scope. It should be
looked upon as part of your application's configuration. If your
application has three .py files in a package, you could, for example,
place the :class:`.sessionmaker` line in your ``__init__.py`` file; from
that point on your other modules say "from mypackage import Session". That
way, everyone else just uses :class:`.Session()`,
and the configuration of that session is controlled by that central point.

If your application starts up, does imports, but does not know what
database it's going to be connecting to, you can bind the
:class:`.Session` at the "class" level to the
engine later on, using :meth:`.sessionmaker.configure`.

In the examples in this section, we will frequently show the
:class:`.sessionmaker` being created right above the line where we actually
invoke :class:`.Session`. But that's just for
example's sake!  In reality, the :class:`.sessionmaker` would be somewhere
at the module level.   The calls to instantiate :class:`.Session`
would then be placed at the point in the application where database
conversations begin.

.. _session_faq_whentocreate:

When do I construct a :class:`.Session`, when do I commit it, and when do I close it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. topic:: tl;dr;

    As a general rule, keep the lifecycle of the session **separate and
    external** from functions and objects that access and/or manipulate
    database data.

A :class:`.Session` is typically constructed at the beginning of a logical
operation where database access is potentially anticipated.

The :class:`.Session`, whenever it is used to talk to the database,
begins a database transaction as soon as it starts communicating.
Assuming the ``autocommit`` flag is left at its recommended default
of ``False``, this transaction remains in progress until the :class:`.Session`
is rolled back, committed, or closed.   The :class:`.Session` will
begin a new transaction if it is used again, subsequent to the previous
transaction ending; from this it follows that the :class:`.Session`
is capable of having a lifespan across many transactions, though only
one at a time.   We refer to these two concepts as **transaction scope**
and **session scope**.

The implication here is that the SQLAlchemy ORM is encouraging the
developer to establish these two scopes in their application,
including not only when the scopes begin and end, but also the
expanse of those scopes, for example should a single
:class:`.Session` instance be local to the execution flow within a
function or method, should it be a global object used by the
entire application, or somewhere in between these two.

The burden placed on the developer to determine this scope is one
area where the SQLAlchemy ORM necessarily has a strong opinion
about how the database should be used.  The :term:`unit of work` pattern
is specifically one of accumulating changes over time and flushing
them periodically, keeping in-memory state in sync with what's
known to be present in a local transaction. This pattern is only
effective when meaningful transaction scopes are in place.

It's usually not very hard to determine the best points at which
to begin and end the scope of a :class:`.Session`, though the wide
variety of application architectures possible can introduce
challenging situations.

A common choice is to tear down the :class:`.Session` at the same
time the transaction ends, meaning the transaction and session scopes
are the same.  This is a great choice to start out with as it
removes the need to consider session scope as separate from transaction
scope.

While there's no one-size-fits-all recommendation for how transaction
scope should be determined, there are common patterns.   Especially
if one is writing a web application, the choice is pretty much established.

A web application is the easiest case because such an appication is already
constructed around a single, consistent scope - this is the **request**,
which represents an incoming request from a browser, the processing
of that request to formulate a response, and finally the delivery of that
response back to the client.    Integrating web applications with the
:class:`.Session` is then the straightforward task of linking the
scope of the :class:`.Session` to that of the request.  The :class:`.Session`
can be established as the request begins, or using a :term:`lazy initialization`
pattern which establishes one as soon as it is needed.  The request
then proceeds, with some system in place where application logic can access
the current :class:`.Session` in a manner associated with how the actual
request object is accessed.  As the request ends, the :class:`.Session`
is torn down as well, usually through the usage of event hooks provided
by the web framework.   The transaction used by the :class:`.Session`
may also be committed at this point, or alternatively the application may
opt for an explicit commit pattern, only committing for those requests
where one is warranted, but still always tearing down the :class:`.Session`
unconditionally at the end.

Some web frameworks include infrastructure to assist in the task
of aligning the lifespan of a :class:`.Session` with that of a web request.
This includes products such as `Flask-SQLAlchemy <http://packages.python.org/Flask-SQLAlchemy/>`_,
for usage in conjunction with the Flask web framework,
and `Zope-SQLAlchemy <http://pypi.python.org/pypi/zope.sqlalchemy>`_,
typically used with the Pyramid framework.
SQLAlchemy recommends that these products be used as available.

In those situations where the integration libraries are not
provided or are insufficient, SQLAlchemy includes its own "helper" class known as
:class:`.scoped_session`.   A tutorial on the usage of this object
is at :ref:`unitofwork_contextual`.   It provides both a quick way
to associate a :class:`.Session` with the current thread, as well as
patterns to associate :class:`.Session` objects with other kinds of
scopes.

As mentioned before, for non-web applications there is no one clear
pattern, as applications themselves don't have just one pattern
of architecture.   The best strategy is to attempt to demarcate
"operations", points at which a particular thread begins to perform
a series of operations for some period of time, which can be committed
at the end.   Some examples:

* A background daemon which spawns off child forks
  would want to create a :class:`.Session` local to each child
  process, work with that :class:`.Session` through the life of the "job"
  that the fork is handling, then tear it down when the job is completed.

* For a command-line script, the application would create a single, global
  :class:`.Session` that is established when the program begins to do its
  work, and commits it right as the program is completing its task.

* For a GUI interface-driven application, the scope of the :class:`.Session`
  may best be within the scope of a user-generated event, such as a button
  push.  Or, the scope may correspond to explicit user interaction, such as
  the user "opening" a series of records, then "saving" them.

As a general rule, the application should manage the lifecycle of the
session *externally* to functions that deal with specific data.  This is a
fundamental separation of concerns which keeps data-specific operations
agnostic of the context in which they access and manipulate that data.

E.g. **don't do this**::

    ### this is the **wrong way to do it** ###

    class ThingOne(object):
        def go(self):
            session = Session()
            try:
                session.query(FooBar).update({"x": 5})
                session.commit()
            except:
                session.rollback()
                raise

    class ThingTwo(object):
        def go(self):
            session = Session()
            try:
                session.query(Widget).update({"q": 18})
                session.commit()
            except:
                session.rollback()
                raise

    def run_my_program():
        ThingOne().go()
        ThingTwo().go()

Keep the lifecycle of the session (and usually the transaction)
**separate and external**::

    ### this is a **better** (but not the only) way to do it ###

    class ThingOne(object):
        def go(self, session):
            session.query(FooBar).update({"x": 5})

    class ThingTwo(object):
        def go(self, session):
            session.query(Widget).update({"q": 18})

    def run_my_program():
        session = Session()
        try:
            ThingOne().go(session)
            ThingTwo().go(session)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

The advanced developer will try to keep the details of session, transaction
and exception management as far as possible from the details of the program
doing its work.   For example, we can further separate concerns using a `context manager <http://docs.python.org/3/library/contextlib.html#contextlib.contextmanager>`_::

    ### another way (but again *not the only way*) to do it ###

    from contextlib import contextmanager

    @contextmanager
    def session_scope():
        """Provide a transactional scope around a series of operations."""
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    def run_my_program():
        with session_scope() as session:
            ThingOne().go(session)
            ThingTwo().go(session)


Is the Session a cache?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yeee...no. It's somewhat used as a cache, in that it implements the
:term:`identity map` pattern, and stores objects keyed to their primary key.
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
a pattern for implementing second level caching using `dogpile.cache <http://dogpilecache.readthedocs.org/>`_,
via the :ref:`examples_caching` example.

How can I get the :class:`~sqlalchemy.orm.session.Session` for a certain object?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :meth:`~.Session.object_session` classmethod
available on :class:`~sqlalchemy.orm.session.Session`::

    session = Session.object_session(someobject)

The newer :ref:`core_inspection_toplevel` system can also be used::

    from sqlalchemy import inspect
    session = inspect(someobject).session

.. _session_faq_threadsafe:

Is the session thread-safe?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.Session` is very much intended to be used in a
**non-concurrent** fashion, which usually means in only one thread at a
time.

The :class:`.Session` should be used in such a way that one
instance exists for a single series of operations within a single
transaction.   One expedient way to get this effect is by associating
a :class:`.Session` with the current thread (see :ref:`unitofwork_contextual`
for background).  Another is to use a pattern
where the :class:`.Session` is passed between functions and is otherwise
not shared with other threads.

The bigger point is that you should not *want* to use the session
with multiple concurrent threads. That would be like having everyone at a
restaurant all eat from the same plate. The session is a local "workspace"
that you use for a specific set of tasks; you don't want to, or need to,
share that session with other threads who are doing some other task.

Making sure the :class:`.Session` is only used in a single concurrent thread at a time
is called a "share nothing" approach to concurrency.  But actually, not
sharing the :class:`.Session` implies a more significant pattern; it
means not just the :class:`.Session` object itself, but
also **all objects that are associated with that Session**, must be kept within
the scope of a single concurrent thread.   The set of mapped
objects associated with a :class:`.Session` are essentially proxies for data
within database rows accessed over a database connection, and so just like
the :class:`.Session` itself, the whole
set of objects is really just a large-scale proxy for a database connection
(or connections).  Ultimately, it's mostly the DBAPI connection itself that
we're keeping away from concurrent access; but since the :class:`.Session`
and all the objects associated with it are all proxies for that DBAPI connection,
the entire graph is essentially not safe for concurrent access.

If there are in fact multiple threads participating
in the same task, then you may consider sharing the session and its objects between
those threads; however, in this extremely unusual scenario the application would
need to ensure that a proper locking scheme is implemented so that there isn't
*concurrent* access to the :class:`.Session` or its state.   A more common approach
to this situation is to maintain a single :class:`.Session` per concurrent thread,
but to instead *copy* objects from one :class:`.Session` to another, often
using the :meth:`.Session.merge` method to copy the state of an object into
a new object local to a different :class:`.Session`.

Querying
--------

The :meth:`~.Session.query` function takes one or more
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

:meth:`~.Session.add` is used to place instances in the
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
:meth:`~.Session.add_all`::

    session.add_all([item1, item2, item3])

The :meth:`~.Session.add` operation **cascades** along
the ``save-update`` cascade. For more details see the section
:ref:`unitofwork_cascades`.

.. _unitofwork_merging:

Merging
-------

:meth:`~.Session.merge` transfers state from an
outside object into a new or already existing instance within a session.   It
also reconciles the incoming data against the state of the
database, producing a history stream which will be applied towards the next
flush, or alternatively can be made to produce a simple "transfer" of
state without producing change history or accessing the database.  Usage is as follows::

    merged_object = session.merge(existing_object)

When given an instance, it follows these steps:

* It examines the primary key of the instance. If it's present, it attempts
  to locate that instance in the local identity map.   If the ``load=True``
  flag is left at its default, it also checks the database for this primary
  key if not located locally.
* If the given instance has no primary key, or if no instance can be found
  with the primary key given, a new instance is created.
* The state of the given instance is then copied onto the located/newly
  created instance.    For attributes which are present on the source
  instance, the value is transferred to the target instance.  For mapped
  attributes which aren't present on the source, the attribute is
  expired on the target instance, discarding its existing value.

  If the ``load=True`` flag is left at its default,
  this copy process emits events and will load the target object's
  unloaded collections for each attribute present on the source object,
  so that the incoming state can be reconciled against what's
  present in the database.  If ``load``
  is passed as ``False``, the incoming data is "stamped" directly without
  producing any history.
* The operation is cascaded to related objects and collections, as
  indicated by the ``merge`` cascade (see :ref:`unitofwork_cascades`).
* The new instance is returned.

With :meth:`~.Session.merge`, the given "source"
instance is not modified nor is it associated with the target :class:`.Session`,
and remains available to be merged with any number of other :class:`.Session`
objects.  :meth:`~.Session.merge` is useful for
taking the state of any kind of object structure without regard for its
origins or current session associations and copying its state into a
new session. Here's some examples:

* An application which reads an object structure from a file and wishes to
  save it to the database might parse the file, build up the
  structure, and then use
  :meth:`~.Session.merge` to save it
  to the database, ensuring that the data within the file is
  used to formulate the primary key of each element of the
  structure. Later, when the file has changed, the same
  process can be re-run, producing a slightly different
  object structure, which can then be ``merged`` in again,
  and the :class:`~sqlalchemy.orm.session.Session` will
  automatically update the database to reflect those
  changes, loading each object from the database by primary key and
  then updating its state with the new state given.

* An application is storing objects in an in-memory cache, shared by
  many :class:`.Session` objects simultaneously.   :meth:`~.Session.merge`
  is used each time an object is retrieved from the cache to create
  a local copy of it in each :class:`.Session` which requests it.
  The cached object remains detached; only its state is moved into
  copies of itself that are local to individual :class:`~.Session`
  objects.

  In the caching use case, it's common to use the ``load=False``
  flag to remove the overhead of reconciling the object's state
  with the database.   There's also a "bulk" version of
  :meth:`~.Session.merge` called :meth:`~.Query.merge_result`
  that was designed to work with cache-extended :class:`.Query`
  objects - see the section :ref:`examples_caching`.

* An application wants to transfer the state of a series of objects
  into a :class:`.Session` maintained by a worker thread or other
  concurrent system.  :meth:`~.Session.merge` makes a copy of each object
  to be placed into this new :class:`.Session`.  At the end of the operation,
  the parent thread/process maintains the objects it started with,
  and the thread/worker can proceed with local copies of those objects.

  In the "transfer between threads/processes" use case, the application
  may want to use the ``load=False`` flag as well to avoid overhead and
  redundant SQL queries as the data is transferred.

Merge Tips
~~~~~~~~~~

:meth:`~.Session.merge` is an extremely useful method for many purposes.  However,
it deals with the intricate border between objects that are transient/detached and
those that are persistent, as well as the automated transference of state.
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
does nothing. Cascade can be configured via the :paramref:`~.relationship.cascade`
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

.. _session_deleting_from_collections:

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

.. _session_flushing:

Flushing
--------

When the :class:`~sqlalchemy.orm.session.Session` is used with its default
configuration, the flush step is nearly always done transparently.
Specifically, the flush occurs before any individual
:class:`~sqlalchemy.orm.query.Query` is issued, as well as within the
:meth:`~.Session.commit` call before the transaction is
committed. It also occurs before a SAVEPOINT is issued when
:meth:`~.Session.begin_nested` is used.

Regardless of the autoflush setting, a flush can always be forced by issuing
:meth:`~.Session.flush`::

    session.flush()

The "flush-on-Query" aspect of the behavior can be disabled by constructing
:class:`.sessionmaker` with the flag ``autoflush=False``::

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
:meth:`~.Session.flush` creates its own transaction and
commits it. Any failures during flush will always result in a rollback of
whatever transaction is present. If the Session is not in ``autocommit=True``
mode, an explicit call to :meth:`~.Session.rollback` is
required after a flush fails, even though the underlying transaction will have
been rolled back already - this is so that the overall nesting pattern of
so-called "subtransactions" is consistently maintained.

.. _session_committing:

Committing
----------

:meth:`~.Session.commit` is used to commit the current
transaction. It always issues :meth:`~.Session.flush`
beforehand to flush any remaining state to the database; this is independent
of the "autoflush" setting. If no transaction is present, it raises an error.
Note that the default behavior of the :class:`~sqlalchemy.orm.session.Session`
is that a "transaction" is always present; this behavior can be disabled by
setting ``autocommit=True``. In autocommit mode, a transaction can be
initiated by calling the :meth:`~.Session.begin` method.

.. note::

   The term "transaction" here refers to a transactional
   construct within the :class:`.Session` itself which may be
   maintaining zero or more actual database (DBAPI) transactions.  An individual
   DBAPI connection begins participation in the "transaction" as it is first
   used to execute a SQL statement, then remains present until the session-level
   "transaction" is completed.  See :ref:`unitofwork_transaction` for
   further detail.

Another behavior of :meth:`~.Session.commit` is that by
default it expires the state of all instances present after the commit is
complete. This is so that when the instances are next accessed, either through
attribute access or by them being present in a
:class:`~sqlalchemy.orm.query.Query` result set, they receive the most recent
state. To disable this behavior, configure
:class:`.sessionmaker` with ``expire_on_commit=False``.

Normally, instances loaded into the :class:`~sqlalchemy.orm.session.Session`
are never changed by subsequent queries; the assumption is that the current
transaction is isolated so the state most recently loaded is correct as long
as the transaction continues. Setting ``autocommit=True`` works against this
model to some degree since the :class:`~sqlalchemy.orm.session.Session`
behaves in exactly the same way with regard to attribute state, except no
transaction is present.

.. _session_rollback:

Rolling Back
------------

:meth:`~.Session.rollback` rolls back the current
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

When a :meth:`~.Session.flush` fails, typically for
reasons like primary key, foreign key, or "not nullable" constraint
violations, a :meth:`~.Session.rollback` is issued
automatically (it's currently not possible for a flush to continue after a
partial failure). However, the flush process always uses its own transactional
demarcator called a *subtransaction*, which is described more fully in the
docstrings for :class:`~sqlalchemy.orm.session.Session`. What it means here is
that even though the database transaction has been rolled back, the end user
must still issue :meth:`~.Session.rollback` to fully
reset the state of the :class:`~sqlalchemy.orm.session.Session`.

Expunging
---------

Expunge removes an object from the Session, sending persistent instances to
the detached state, and pending instances to the transient state:

.. sourcecode:: python+sql

    session.expunge(obj1)

To remove all items, call :meth:`~.Session.expunge_all`
(this method was formerly known as ``clear()``).

Closing
-------

The :meth:`~.Session.close` method issues a
:meth:`~.Session.expunge_all`, and :term:`releases` any
transactional/connection resources. When connections are returned to the
connection pool, transactional state is rolled back as well.

.. _session_expire:

Refreshing / Expiring
---------------------

:term:`Expiring` means that the database-persisted data held inside a series
of object attributes is erased, in such a way that when those attributes
are next accessed, a SQL query is emitted which will refresh that data from
the database.

When we talk about expiration of data we are usually talking about an object
that is in the :term:`persistent` state.   For example, if we load an object
as follows::

    user = session.query(User).filter_by(name='user1').first()

The above ``User`` object is persistent, and has a series of attributes
present; if we were to look inside its ``__dict__``, we'd see that state
loaded::

    >>> user.__dict__
    {
      'id': 1, 'name': u'user1',
      '_sa_instance_state': <...>,
    }

where ``id`` and ``name`` refer to those columns in the database.
``_sa_instance_state`` is a non-database-persisted value used by SQLAlchemy
internally (it refers to the :class:`.InstanceState` for the instance.
While not directly relevant to this section, if we want to get at it,
we should use the :func:`.inspect` function to access it).

At this point, the state in our ``User`` object matches that of the loaded
database row.  But upon expiring the object using a method such as
:meth:`.Session.expire`, we see that the state is removed::

    >>> session.expire(user)
    >>> user.__dict__
    {'_sa_instance_state': <...>}

We see that while the internal "state" still hangs around, the values which
correspond to the ``id`` and ``name`` columns are gone.   If we were to access
one of these columns and are watching SQL, we'd see this:

.. sourcecode:: python+sql

    >>> print(user.name)
    {opensql}SELECT user.id AS user_id, user.name AS user_name
    FROM user
    WHERE user.id = ?
    (1,)
    {stop}user1

Above, upon accessing the expired attribute ``user.name``, the ORM initiated
a :term:`lazy load` to retrieve the most recent state from the database,
by emitting a SELECT for the user row to which this user refers.  Afterwards,
the ``__dict__`` is again populated::

    >>> user.__dict__
    {
      'id': 1, 'name': u'user1',
      '_sa_instance_state': <...>,
    }

.. note::  While we are peeking inside of ``__dict__`` in order to see a bit
   of what SQLAlchemy does with object attributes, we **should not modify**
   the contents of ``__dict__`` directly, at least as far as those attributes
   which the SQLAlchemy ORM is maintaining (other attributes outside of SQLA's
   realm are fine).  This is because SQLAlchemy uses :term:`descriptors` in
   order to track the changes we make to an object, and when we modify ``__dict__``
   directly, the ORM won't be able to track that we changed something.

Another key behavior of both :meth:`~.Session.expire` and :meth:`~.Session.refresh`
is that all un-flushed changes on an object are discarded.  That is,
if we were to modify an attribute on our ``User``::

    >>> user.name = 'user2'

but then we call :meth:`~.Session.expire` without first calling :meth:`~.Session.flush`,
our pending value of ``'user2'`` is discarded::

    >>> session.expire(user)
    >>> user.name
    'user1'

The :meth:`~.Session.expire` method can be used to mark as "expired" all ORM-mapped
attributes for an instance::

    # expire all ORM-mapped attributes on obj1
    session.expire(obj1)

it can also be passed a list of string attribute names, referring to specific
attributes to be marked as expired::

    # expire only attributes obj1.attr1, obj1.attr2
    session.expire(obj1, ['attr1', 'attr2'])

The :meth:`~.Session.refresh` method has a similar interface, but instead
of expiring, it emits an immediate SELECT for the object's row immediately::

    # reload all attributes on obj1
    session.refresh(obj1)

:meth:`~.Session.refresh` also accepts a list of string attribute names,
but unlike :meth:`~.Session.expire`, expects at least one name to
be that of a column-mapped attribute::

    # reload obj1.attr1, obj1.attr2
    session.refresh(obj1, ['attr1', 'attr2'])

The :meth:`.Session.expire_all` method allows us to essentially call
:meth:`.Session.expire` on all objects contained within the :class:`.Session`
at once::

    session.expire_all()

What Actually Loads
~~~~~~~~~~~~~~~~~~~

The SELECT statement that's emitted when an object marked with :meth:`~.Session.expire`
or loaded with :meth:`~.Session.refresh` varies based on several factors, including:

* The load of expired attributes is triggered from **column-mapped attributes only**.
  While any kind of attribute can be marked as expired, including a
  :func:`.relationship` - mapped attribute, accessing an expired :func:`.relationship`
  attribute will emit a load only for that attribute, using standard
  relationship-oriented lazy loading.   Column-oriented attributes, even if
  expired, will not load as part of this operation, and instead will load when
  any column-oriented attribute is accessed.

* :func:`.relationship`- mapped attributes will not load in response to
  expired column-based attributes being accessed.

* Regarding relationships, :meth:`~.Session.refresh` is more restrictive than
  :meth:`~.Session.expire` with regards to attributes that aren't column-mapped.
  Calling :meth:`.refresh` and passing a list of names that only includes
  relationship-mapped attributes will actually raise an error.
  In any case, non-eager-loading :func:`.relationship` attributes will not be
  included in any refresh operation.

* :func:`.relationship` attributes configured as "eager loading" via the
  :paramref:`~.relationship.lazy` parameter will load in the case of
  :meth:`~.Session.refresh`, if either no attribute names are specified, or
  if their names are inclued in the list of attributes to be
  refreshed.

* Attributes that are configured as :func:`.deferred` will not normally load,
  during either the expired-attribute load or during a refresh.
  An unloaded attribute that's :func:`.deferred` instead loads on its own when directly
  accessed, or if part of a "group" of deferred attributes where an unloaded
  attribute in that group is accessed.

* For expired attributes that are loaded on access, a joined-inheritance table
  mapping will emit a SELECT that typically only includes those tables for which
  unloaded attributes are present.   The action here is sophisticated enough
  to load only the parent or child table, for example, if the subset of columns
  that were originally expired encompass only one or the other of those tables.

* When :meth:`~.Session.refresh` is used on a joined-inheritance table mapping,
  the SELECT emitted will resemble that of when :meth:`.Session.query` is
  used on the target object's class.  This is typically all those tables that
  are set up as part of the mapping.


When to Expire or Refresh
~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.Session` uses the expiration feature automatically whenever
the transaction referred to by the session ends.  Meaning, whenever :meth:`.Session.commit`
or :meth:`.Session.rollback` is called, all objects within the :class:`.Session`
are expired, using a feature equivalent to that of the :meth:`.Session.expire_all`
method.   The rationale is that the end of a transaction is a
demarcating point at which there is no more context available in order to know
what the current state of the database is, as any number of other transactions
may be affecting it.  Only when a new transaction starts can we again have access
to the current state of the database, at which point any number of changes
may have occurred.

.. sidebar:: Transaction Isolation

    Of course, most databases are capable of handling
    multiple transactions at once, even involving the same rows of data.   When
    a relational database handles multiple transactions involving the same
    tables or rows, this is when the :term:`isolation` aspect of the database comes
    into play.  The isolation behavior of different databases varies considerably
    and even on a single database can be configured to behave in different ways
    (via the so-called :term:`isolation level` setting).  In that sense, the :class:`.Session`
    can't fully predict when the same SELECT statement, emitted a second time,
    will definitely return the data we already have, or will return new data.
    So as a best guess, it assumes that within the scope of a transaction, unless
    it is known that a SQL expression has been emitted to modify a particular row,
    there's no need to refresh a row unless explicitly told to do so.

The :meth:`.Session.expire` and :meth:`.Session.refresh` methods are used in
those cases when one wants to force an object to re-load its data from the
database, in those cases when it is known that the current state of data
is possibly stale.  Reasons for this might include:

* some SQL has been emitted within the transaction outside of the
  scope of the ORM's object handling, such as if a :meth:`.Table.update` construct
  were emitted using the :meth:`.Session.execute` method;

* if the application
  is attempting to acquire data that is known to have been modified in a
  concurrent transaction, and it is also known that the isolation rules in effect
  allow this data to be visible.

The second bullet has the important caveat that "it is also known that the isolation rules in effect
allow this data to be visible."  This means that it cannot be assumed that an
UPDATE that happened on another database connection will yet be visible here
locally; in many cases, it will not.  This is why if one wishes to use
:meth:`.expire` or :meth:`.refresh` in order to view data between ongoing
transactions, an understanding of the isolation behavior in effect is essential.

.. seealso::

    :meth:`.Session.expire`

    :meth:`.Session.expire_all`

    :meth:`.Session.refresh`

    :term:`isolation` - glossary explanation of isolation which includes links
    to Wikipedia.

    `The SQLAlchemy Session In-Depth <http://techspot.zzzeek.org/2012/11/14/pycon-canada-the-sqlalchemy-session-in-depth/>`_ - a video + slides with an in-depth discussion of the object
    lifecycle including the role of data expiration.


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

    # dictionary of all persistent objects, keyed on their
    # identity key
    session.identity_map

(Documentation: :attr:`.Session.new`, :attr:`.Session.dirty`,
:attr:`.Session.deleted`, :attr:`.Session.identity_map`).

Note that objects within the session are by default *weakly referenced*. This
means that when they are dereferenced in the outside application, they fall
out of scope from within the :class:`~sqlalchemy.orm.session.Session` as well
and are subject to garbage collection by the Python interpreter. The
exceptions to this include objects which are pending, objects which are marked
as deleted, or persistent objects which have pending changes on them. After a
full flush, these collections are all empty, and all objects are again weakly
referenced. To disable the weak referencing behavior and force all objects
within the session to remain until explicitly expunged, configure
:class:`.sessionmaker` with the ``weak_identity_map=False``
setting.

.. _unitofwork_cascades:

Cascades
========

Mappers support the concept of configurable :term:`cascade` behavior on
:func:`~sqlalchemy.orm.relationship` constructs.  This refers
to how operations performed on a "parent" object relative to a
particular :class:`.Session` should be propagated to items
referred to by that relationship (e.g. "child" objects), and is
affected by the :paramref:`.relationship.cascade` option.

The default behavior of cascade is limited to cascades of the
so-called :ref:`cascade_save_update` and :ref:`cascade_merge` settings.
The typical "alternative" setting for cascade is to add
the :ref:`cascade_delete` and :ref:`cascade_delete_orphan` options;
these settings are appropriate for related objects which only exist as
long as they are attached to their parent, and are otherwise deleted.

Cascade behavior is configured using the by changing the
:paramref:`~.relationship.cascade` option on
:func:`~sqlalchemy.orm.relationship`::

    class Order(Base):
        __tablename__ = 'order'

        items = relationship("Item", cascade="all, delete-orphan")
        customer = relationship("User", cascade="save-update")

To set cascades on a backref, the same flag can be used with the
:func:`~.sqlalchemy.orm.backref` function, which ultimately feeds
its arguments back into :func:`~sqlalchemy.orm.relationship`::

    class Item(Base):
        __tablename__ = 'item'

        order = relationship("Order",
                        backref=backref("items", cascade="all, delete-orphan")
                    )

.. sidebar:: The Origins of Cascade

    SQLAlchemy's notion of cascading behavior on relationships,
    as well as the options to configure them, are primarily derived
    from the similar feature in the Hibernate ORM; Hibernate refers
    to "cascade" in a few places such as in
    `Example: Parent/Child <https://docs.jboss.org/hibernate/orm/3.3/reference/en-US/html/example-parentchild.html>`_.
    If cascades are confusing, we'll refer to their conclusion,
    stating "The sections we have just covered can be a bit confusing.
    However, in practice, it all works out nicely."

The default value of :paramref:`~.relationship.cascade` is ``save-update, merge``.
The typical alternative setting for this parameter is either
``all`` or more commonly ``all, delete-orphan``.  The ``all`` symbol
is a synonym for ``save-update, merge, refresh-expire, expunge, delete``,
and using it in conjunction with ``delete-orphan`` indicates that the child
object should follow along with its parent in all cases, and be deleted once
it is no longer associated with that parent.

The list of available values which can be specified for
the :paramref:`~.relationship.cascade` parameter are described in the following subsections.

.. _cascade_save_update:

save-update
-----------

``save-update`` cascade indicates that when an object is placed into a
:class:`.Session` via :meth:`.Session.add`, all the objects associated
with it via this :func:`.relationship` should also be added to that
same :class:`.Session`.  Suppose we have an object ``user1`` with two
related objects ``address1``, ``address2``::

    >>> user1 = User()
    >>> address1, address2 = Address(), Address()
    >>> user1.addresses = [address1, address2]

If we add ``user1`` to a :class:`.Session`, it will also add
``address1``, ``address2`` implicitly::

    >>> sess = Session()
    >>> sess.add(user1)
    >>> address1 in sess
    True

``save-update`` cascade also affects attribute operations for objects
that are already present in a :class:`.Session`.  If we add a third
object, ``address3`` to the ``user1.addresses`` collection, it
becomes part of the state of that :class:`.Session`::

    >>> address3 = Address()
    >>> user1.append(address3)
    >>> address3 in sess
    >>> True

``save-update`` has the possibly surprising behavior which is that
persistent objects which were *removed* from a collection
or in some cases a scalar attribute
may also be pulled into the :class:`.Session` of a parent object; this is
so that the flush process may handle that related object appropriately.
This case can usually only arise if an object is removed from one :class:`.Session`
and added to another::

    >>> user1 = sess1.query(User).filter_by(id=1).first()
    >>> address1 = user1.addresses[0]
    >>> sess1.close()   # user1, address1 no longer associated with sess1
    >>> user1.addresses.remove(address1)  # address1 no longer associated with user1
    >>> sess2 = Session()
    >>> sess2.add(user1)   # ... but it still gets added to the new session,
    >>> address1 in sess2  # because it's still "pending" for flush
    True

The ``save-update`` cascade is on by default, and is typically taken
for granted; it simplifies code by allowing a single call to
:meth:`.Session.add` to register an entire structure of objects within
that :class:`.Session` at once.   While it can be disabled, there
is usually not a need to do so.

One case where ``save-update`` cascade does sometimes get in the way is in that
it takes place in both directions for bi-directional relationships, e.g.
backrefs, meaning that the association of a child object with a particular parent
can have the effect of the parent object being implicitly associated with that
child object's :class:`.Session`; this pattern, as well as how to modify its
behavior using the :paramref:`~.relationship.cascade_backrefs` flag,
is discussed in the section :ref:`backref_cascade`.

.. _cascade_delete:

delete
------

The ``delete`` cascade indicates that when a "parent" object
is marked for deletion, its related "child" objects should also be marked
for deletion.   If for example we we have a relationship ``User.addresses``
with ``delete`` cascade configured::

    class User(Base):
        # ...

        addresses = relationship("Address", cascade="save-update, merge, delete")

If using the above mapping, we have a ``User`` object and two
related ``Address`` objects::

    >>> user1 = sess.query(User).filter_by(id=1).first()
    >>> address1, address2 = user1.addresses

If we mark ``user1`` for deletion, after the flush operation proceeds,
``address1`` and ``address2`` will also be deleted:

.. sourcecode:: python+sql

    >>> sess.delete(user1)
    >>> sess.commit()
    {opensql}DELETE FROM address WHERE address.id = ?
    ((1,), (2,))
    DELETE FROM user WHERE user.id = ?
    (1,)
    COMMIT

Alternatively, if our ``User.addresses`` relationship does *not* have
``delete`` cascade, SQLAlchemy's default behavior is to instead de-associate
``address1`` and ``address2`` from ``user1`` by setting their foreign key
reference to ``NULL``.  Using a mapping as follows::

    class User(Base):
        # ...

        addresses = relationship("Address")

Upon deletion of a parent ``User`` object, the rows in ``address`` are not
deleted, but are instead de-associated:

.. sourcecode:: python+sql

    >>> sess.delete(user1)
    >>> sess.commit()
    {opensql}UPDATE address SET user_id=? WHERE address.id = ?
    (None, 1)
    UPDATE address SET user_id=? WHERE address.id = ?
    (None, 2)
    DELETE FROM user WHERE user.id = ?
    (1,)
    COMMIT

``delete`` cascade is more often than not used in conjunction with
:ref:`cascade_delete_orphan` cascade, which will emit a DELETE for the related
row if the "child" object is deassociated from the parent.  The combination
of ``delete`` and ``delete-orphan`` cascade covers both situations where
SQLAlchemy has to decide between setting a foreign key column to NULL versus
deleting the row entirely.

.. topic:: ORM-level "delete" cascade vs. FOREIGN KEY level "ON DELETE" cascade

    The behavior of SQLAlchemy's "delete" cascade has a lot of overlap with the
    ``ON DELETE CASCADE`` feature of a database foreign key, as well
    as with that of the ``ON DELETE SET NULL`` foreign key setting when "delete"
    cascade is not specified.   Database level "ON DELETE" cascades are specific to the
    "FOREIGN KEY" construct of the relational database; SQLAlchemy allows
    configuration of these schema-level constructs at the :term:`DDL` level
    using options on :class:`.ForeignKeyConstraint` which are described
    at :ref:`on_update_on_delete`.

    It is important to note the differences between the ORM and the relational
    database's notion of "cascade" as well as how they integrate:

    * A database level ``ON DELETE`` cascade is configured effectively
      on the **many-to-one** side of the relationship; that is, we configure
      it relative to the ``FOREIGN KEY`` constraint that is the "many" side
      of a relationship.  At the ORM level, **this direction is reversed**.
      SQLAlchemy handles the deletion of "child" objects relative to a
      "parent" from the "parent" side, which means that ``delete`` and
      ``delete-orphan`` cascade are configured on the **one-to-many**
      side.

    * Database level foreign keys with no ``ON DELETE`` setting
      are often used to **prevent** a parent
      row from being removed, as it would necessarily leave an unhandled
      related row present.  If this behavior is desired in a one-to-many
      relationship, SQLAlchemy's default behavior of setting a foreign key
      to ``NULL`` can be caught in one of two ways:

        * The easiest and most common is just to set the
          foreign-key-holding column to ``NOT NULL`` at the database schema
          level.  An attempt by SQLAlchemy to set the column to NULL will
          fail with a simple NOT NULL constraint exception.

        * The other, more special case way is to set the :paramref:`~.relationship.passive_deletes`
          flag to the string ``"all"``.  This has the effect of entirely
          disabling SQLAlchemy's behavior of setting the foreign key column
          to NULL, and a DELETE will be emitted for the parent row without
          any affect on the child row, even if the child row is present
          in memory. This may be desirable in the case when
          database-level foreign key triggers, either special ``ON DELETE`` settings
          or otherwise, need to be activated in all cases when a parent row is deleted.

    * Database level ``ON DELETE`` cascade is **vastly more efficient**
      than that of SQLAlchemy.  The database can chain a series of cascade
      operations across many relationships at once; e.g. if row A is deleted,
      all the related rows in table B can be deleted, and all the C rows related
      to each of those B rows, and on and on, all within the scope of a single
      DELETE statement.  SQLAlchemy on the other hand, in order to support
      the cascading delete operation fully, has to individually load each
      related collection in order to target all rows that then may have further
      related collections.  That is, SQLAlchemy isn't sophisticated enough
      to emit a DELETE for all those related rows at once within this context.

    * SQLAlchemy doesn't **need** to be this sophisticated, as we instead provide
      smooth integration with the database's own ``ON DELETE`` functionality,
      by using the :paramref:`~.relationship.passive_deletes` option in conjunction
      with properly configured foreign key constraints.   Under this behavior,
      SQLAlchemy only emits DELETE for those rows that are already locally
      present in the :class:`.Session`; for any collections that are unloaded,
      it leaves them to the database to handle, rather than emitting a SELECT
      for them.  The section :ref:`passive_deletes` provides an example of this use.

    * While database-level ``ON DELETE`` functionality works only on the "many"
      side of a relationship, SQLAlchemy's "delete" cascade
      has **limited** ability to operate in the *reverse* direction as well,
      meaning it can be configured on the "many" side to delete an object
      on the "one" side when the reference on the "many" side is deleted.  However
      this can easily result in constraint violations if there are other objects
      referring to this "one" side from the "many", so it typically is only
      useful when a relationship is in fact a "one to one".  The
      :paramref:`~.relationship.single_parent` flag should be used to establish
      an in-Python assertion for this case.


When using a :func:`.relationship` that also includes a many-to-many
table using the :paramref:`~.relationship.secondary` option, SQLAlchemy's
delete cascade handles the rows in this many-to-many table automatically.
Just like, as described in :ref:`relationships_many_to_many_deletion`,
the addition or removal of an object from a many-to-many collection
results in the INSERT or DELETE of a row in the many-to-many table,
the ``delete`` cascade, when activated as the result of a parent object
delete operation, will DELETE not just the row in the "child" table but also
in the many-to-many table.

.. _cascade_delete_orphan:

delete-orphan
-------------

``delete-orphan`` cascade adds behavior to the ``delete`` cascade,
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
be configured with the :paramref:`~.relationship.single_parent` argument,
establishes Python-side validation that ensures the object
is associated with only one parent at a time.

.. _cascade_merge:

merge
-----

``merge`` cascade indicates that the :meth:`.Session.merge`
operation should be propagated from a parent that's the subject
of the :meth:`.Session.merge` call down to referred objects.
This cascade is also on by default.

.. _cascade_refresh_expire:

refresh-expire
--------------

``refresh-expire`` is an uncommon option, indicating that the
:meth:`.Session.expire` operation should be propagated from a parent
down to referred objects.   When using :meth:`.Session.refresh`,
the referred objects are expired only, but not actually refreshed.

.. _cascade_expunge:

expunge
-------

``expunge`` cascade indicates that when the parent object is removed
from the :class:`.Session` using :meth:`.Session.expunge`, the
operation should be propagated down to referred objects.

.. _backref_cascade:

Controlling Cascade on Backrefs
-------------------------------

The :ref:`cascade_save_update` cascade by default takes place on attribute change events
emitted from backrefs.  This is probably a confusing statement more
easily described through demonstration; it means that, given a mapping such as this::

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

This behavior can be disabled using the :paramref:`~.relationship.cascade_backrefs` flag::

    mapper(Order, order_table, properties={
        'items' : relationship(Item, backref='order',
                                    cascade_backrefs=False)
    })

So above, the assignment of ``i1.order = o1`` will append ``i1`` to the ``items``
collection of ``o1``, but will not add ``i1`` to the session.   You can, of
course, :meth:`~.Session.add` ``i1`` to the session at a later point.   This
option may be helpful for situations where an object needs to be kept out of a
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
:term:`releases` all :class:`.Transaction` and :class:`.Connection` resources,
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
delineated using the :meth:`~.Session.begin_nested`
method::

    Session = sessionmaker()
    session = Session()
    session.add(u1)
    session.add(u2)

    session.begin_nested() # establish a savepoint
    session.add(u3)
    session.rollback()  # rolls back u3, keeps u1 and u2

    session.commit() # commits u1 and u2

:meth:`~.Session.begin_nested` may be called any number
of times, which will issue a new SAVEPOINT with a unique identifier for each
call. For each :meth:`~.Session.begin_nested` call, a
corresponding :meth:`~.Session.rollback` or
:meth:`~.Session.commit` must be issued. (But note that if the return value is
used as a context manager, i.e. in a with-statement, then this rollback/commit
is issued by the context manager upon exiting the context, and so should not be
added explicitly.)

When :meth:`~.Session.begin_nested` is called, a
:meth:`~.Session.flush` is unconditionally issued
(regardless of the ``autoflush`` setting). This is so that when a
:meth:`~.Session.rollback` occurs, the full state of the
session is expired, thus causing all subsequent attribute/instance access to
reference the full state of the :class:`~sqlalchemy.orm.session.Session` right
before :meth:`~.Session.begin_nested` was called.

:meth:`~.Session.begin_nested`, in the same manner as the less often
used :meth:`~.Session.begin` method, returns a transactional object
which also works as a context manager.
It can be succinctly used around individual record inserts in order to catch
things like unique constraint exceptions::

    for record in records:
        try:
            with session.begin_nested():
                session.merge(record)
        except:
            print "Skipped record %s" % record
    session.commit()

.. _session_autocommit:

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

.. warning::

    "autocommit" mode should **not be considered for general use**.
    If used, it should always be combined with the usage of
    :meth:`.Session.begin` and :meth:`.Session.commit`, to ensure
    a transaction demarcation.

    Executing queries outside of a demarcated transaction is a legacy mode
    of usage, and can in some cases lead to concurrent connection
    checkouts.

    In the absence of a demarcated transaction, the :class:`.Session`
    cannot make appropriate decisions as to when autoflush should
    occur nor when auto-expiration should occur, so these features
    should be disabled with ``autoflush=False, expire_on_commit=False``.

Modern usage of "autocommit" is for framework integrations that need to control
specifically when the "begin" state occurs.  A session which is configured with
``autocommit=True`` may be placed into the "begin" state using the
:meth:`.Session.begin` method.
After the cycle completes upon :meth:`.Session.commit` or :meth:`.Session.rollback`,
connection and transaction resources are :term:`released` and the :class:`.Session`
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

The :meth:`.Session.begin` method also returns a transactional token which is
compatible with the Python 2.6 ``with`` statement::

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
the ``subtransactions=True`` flag.  This produces a non-transactional, delimiting construct that
allows nesting of calls to :meth:`~.Session.begin` and :meth:`~.Session.commit`.
Its purpose is to allow the construction of code that can function within a transaction
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
also :meth:`~.Session.prepare` the session for
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

.. _session_sql_expressions:

Using SQL Expressions with Sessions
====================================

SQL expressions and strings can be executed via the
:class:`~sqlalchemy.orm.session.Session` within its transactional context.
This is most easily accomplished using the
:meth:`~.Session.execute` method, which returns a
:class:`~sqlalchemy.engine.ResultProxy` in the same manner as an
:class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection`::

    Session = sessionmaker(bind=engine)
    session = Session()

    # execute a string statement
    result = session.execute("select * from table where id=:id", {'id':7})

    # execute a SQL expression construct
    result = session.execute(select([mytable]).where(mytable.c.id==7))

The current :class:`~sqlalchemy.engine.Connection` held by the
:class:`~sqlalchemy.orm.session.Session` is accessible using the
:meth:`~.Session.connection` method::

    connection = session.connection()

The examples above deal with a :class:`~sqlalchemy.orm.session.Session` that's
bound to a single :class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection`. To execute statements using a
:class:`~sqlalchemy.orm.session.Session` which is bound either to multiple
engines, or none at all (i.e. relies upon bound metadata), both
:meth:`~.Session.execute` and
:meth:`~.Session.connection` accept a ``mapper`` keyword
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

Joining a Session into an External Transaction (such as for test suites)
========================================================================

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
            self.trans = self.connection.begin()

            # bind an individual Session to the connection
            self.session = Session(bind=self.connection)

        def test_something(self):
            # use the session in tests.

            self.session.add(Foo())
            self.session.commit()

        def tearDown(self):
            self.session.close()

            # rollback - everything that happened with the
            # Session above (including calls to commit())
            # is rolled back.
            self.trans.rollback()

            # return connection to the Engine
            self.connection.close()

Above, we issue :meth:`.Session.commit` as well as
:meth:`.Transaction.rollback`. This is an example of where we take advantage
of the :class:`.Connection` object's ability to maintain *subtransactions*, or
nested begin/commit-or-rollback pairs where only the outermost begin/commit
pair actually commits the transaction, or if the outermost block rolls back,
everything is rolled back.

.. topic:: Supporting Tests with Rollbacks

   The above recipe works well for any kind of database enabled test, except
   for a test that needs to actually invoke :meth:`.Session.rollback` within
   the scope of the test itself.   The above recipe can be expanded, such
   that the :class:`.Session` always runs all operations within the scope
   of a SAVEPOINT, which is established at the start of each transaction,
   so that tests can also rollback the "transaction" as well while still
   remaining in the scope of a larger "transaction" that's never committed,
   using two extra events::

      from sqlalchemy import event

      class SomeTest(TestCase):
          def setUp(self):
              # connect to the database
              self.connection = engine.connect()

              # begin a non-ORM transaction
              self.trans = connection.begin()

              # bind an individual Session to the connection
              self.session = Session(bind=self.connection)

              # start the session in a SAVEPOINT...
              self.session.begin_nested()

              # then each time that SAVEPOINT ends, reopen it
              @event.listens_for(self.session, "after_transaction_end")
              def restart_savepoint(session, transaction):
                  if transaction.nested and not transaction._parent.nested:
                      session.begin_nested()


          # ... the tearDown() method stays the same

.. _unitofwork_contextual:

Contextual/Thread-local Sessions
=================================

Recall from the section :ref:`session_faq_whentocreate`, the concept of
"session scopes" was introduced, with an emphasis on web applications
and the practice of linking the scope of a :class:`.Session` with that
of a web request.   Most modern web frameworks include integration tools
so that the scope of the :class:`.Session` can be managed automatically,
and these tools should be used as they are available.

SQLAlchemy includes its own helper object, which helps with the establishment
of user-defined :class:`.Session` scopes.  It is also used by third-party
integration systems to help construct their integration schemes.

The object is the :class:`.scoped_session` object, and it represents a
**registry** of :class:`.Session` objects.  If you're not familiar with the
registry pattern, a good introduction can be found in `Patterns of Enterprise
Architecture <http://martinfowler.com/eaaCatalog/registry.html>`_.

.. note::

   The :class:`.scoped_session` object is a very popular and useful object
   used by many SQLAlchemy applications.  However, it is important to note
   that it presents **only one approach** to the issue of :class:`.Session`
   management.  If you're new to SQLAlchemy, and especially if the
   term "thread-local variable" seems strange to you, we recommend that
   if possible you familiarize first with an off-the-shelf integration
   system such as `Flask-SQLAlchemy <http://packages.python.org/Flask-SQLAlchemy/>`_
   or `zope.sqlalchemy <http://pypi.python.org/pypi/zope.sqlalchemy>`_.

A :class:`.scoped_session` is constructed by calling it, passing it a
**factory** which can create new :class:`.Session` objects.   A factory
is just something that produces a new object when called, and in the
case of :class:`.Session`, the most common factory is the :class:`.sessionmaker`,
introduced earlier in this section.  Below we illustrate this usage::

    >>> from sqlalchemy.orm import scoped_session
    >>> from sqlalchemy.orm import sessionmaker

    >>> session_factory = sessionmaker(bind=some_engine)
    >>> Session = scoped_session(session_factory)

The :class:`.scoped_session` object we've created will now call upon the
:class:`.sessionmaker` when we "call" the registry::

    >>> some_session = Session()

Above, ``some_session`` is an instance of :class:`.Session`, which we
can now use to talk to the database.   This same :class:`.Session` is also
present within the :class:`.scoped_session` registry we've created.   If
we call upon the registry a second time, we get back the **same** :class:`.Session`::

    >>> some_other_session = Session()
    >>> some_session is some_other_session
    True

This pattern allows disparate sections of the application to call upon a global
:class:`.scoped_session`, so that all those areas may share the same session
without the need to pass it explicitly.   The :class:`.Session` we've established
in our registry will remain, until we explicitly tell our registry to dispose of it,
by calling :meth:`.scoped_session.remove`::

    >>> Session.remove()

The :meth:`.scoped_session.remove` method first calls :meth:`.Session.close` on
the current :class:`.Session`, which has the effect of releasing any connection/transactional
resources owned by the :class:`.Session` first, then discarding the :class:`.Session`
itself.  "Releasing" here means that connections are returned to their connection pool and any transactional state is rolled back, ultimately using the ``rollback()`` method of the underlying DBAPI connection.

At this point, the :class:`.scoped_session` object is "empty", and will create
a **new** :class:`.Session` when called again.  As illustrated below, this
is not the same :class:`.Session` we had before::

    >>> new_session = Session()
    >>> new_session is some_session
    False

The above series of steps illustrates the idea of the "registry" pattern in a
nutshell.  With that basic idea in hand, we can discuss some of the details
of how this pattern proceeds.

Implicit Method Access
----------------------

The job of the :class:`.scoped_session` is simple; hold onto a :class:`.Session`
for all who ask for it.  As a means of producing more transparent access to this
:class:`.Session`, the :class:`.scoped_session` also includes **proxy behavior**,
meaning that the registry itself can be treated just like a :class:`.Session`
directly; when methods are called on this object, they are **proxied** to the
underlying :class:`.Session` being maintained by the registry::

    Session = scoped_session(some_factory)

    # equivalent to:
    #
    # session = Session()
    # print session.query(MyClass).all()
    #
    print Session.query(MyClass).all()

The above code accomplishes the same task as that of acquiring the current
:class:`.Session` by calling upon the registry, then using that :class:`.Session`.

Thread-Local Scope
------------------

Users who are familiar with multithreaded programming will note that representing
anything as a global variable is usually a bad idea, as it implies that the
global object will be accessed by many threads concurrently.   The :class:`.Session`
object is entirely designed to be used in a **non-concurrent** fashion, which
in terms of multithreading means "only in one thread at a time".   So our
above example of :class:`.scoped_session` usage, where the same :class:`.Session`
object is maintained across multiple calls, suggests that some process needs
to be in place such that mutltiple calls across many threads don't actually get
a handle to the same session.   We call this notion **thread local storage**,
which means, a special object is used that will maintain a distinct object
per each application thread.   Python provides this via the
`threading.local() <http://docs.python.org/library/threading.html#threading.local>`_
construct.  The :class:`.scoped_session` object by default uses this object
as storage, so that a single :class:`.Session` is maintained for all who call
upon the :class:`.scoped_session` registry, but only within the scope of a single
thread.   Callers who call upon the registry in a different thread get a
:class:`.Session` instance that is local to that other thread.

Using this technique, the :class:`.scoped_session` provides a quick and relatively
simple (if one is familiar with thread-local storage) way of providing
a single, global object in an application that is safe to be called upon
from multiple threads.

The :meth:`.scoped_session.remove` method, as always, removes the current
:class:`.Session` associated with the thread, if any.  However, one advantage of the
``threading.local()`` object is that if the application thread itself ends, the
"storage" for that thread is also garbage collected.  So it is in fact "safe" to
use thread local scope with an application that spawns and tears down threads,
without the need to call :meth:`.scoped_session.remove`.  However, the scope
of transactions themselves, i.e. ending them via :meth:`.Session.commit` or
:meth:`.Session.rollback`, will usually still be something that must be explicitly
arranged for at the appropriate time, unless the application actually ties the
lifespan of a thread to the lifespan of a transaction.

.. _session_lifespan:

Using Thread-Local Scope with Web Applications
----------------------------------------------

As discussed in the section :ref:`session_faq_whentocreate`, a web application
is architected around the concept of a **web request**, and integrating
such an application with the :class:`.Session` usually implies that the :class:`.Session`
will be associated with that request.  As it turns out, most Python web frameworks,
with notable exceptions such as the asynchronous frameworks Twisted and
Tornado, use threads in a simple way, such that a particular web request is received,
processed, and completed within the scope of a single *worker thread*.  When
the request ends, the worker thread is released to a pool of workers where it
is available to handle another request.

This simple correspondence of web request and thread means that to associate a
:class:`.Session` with a thread implies it is also associated with the web request
running within that thread, and vice versa, provided that the :class:`.Session` is
created only after the web request begins and torn down just before the web request ends.
So it is a common practice to use :class:`.scoped_session` as a quick way
to integrate the :class:`.Session` with a web application.  The sequence
diagram below illustrates this flow::

    Web Server          Web Framework        SQLAlchemy ORM Code
    --------------      --------------       ------------------------------
    startup        ->   Web framework        # Session registry is established
                        initializes          Session = scoped_session(sessionmaker())

    incoming
    web request    ->   web request     ->   # The registry is *optionally*
                        starts               # called upon explicitly to create
                                             # a Session local to the thread and/or request
                                             Session()

                                             # the Session registry can otherwise
                                             # be used at any time, creating the
                                             # request-local Session() if not present,
                                             # or returning the existing one
                                             Session.query(MyClass) # ...

                                             Session.add(some_object) # ...

                                             # if data was modified, commit the
                                             # transaction
                                             Session.commit()

                        web request ends  -> # the registry is instructed to
                                             # remove the Session
                                             Session.remove()

                        sends output      <-
    outgoing web    <-
    response

Using the above flow, the process of integrating the :class:`.Session` with the
web application has exactly two requirements:

1. Create a single :class:`.scoped_session` registry when the web application
   first starts, ensuring that this object is accessible by the rest of the
   application.
2. Ensure that :meth:`.scoped_session.remove` is called when the web request ends,
   usually by integrating with the web framework's event system to establish
   an "on request end" event.

As noted earlier, the above pattern is **just one potential way** to integrate a :class:`.Session`
with a web framework, one which in particular makes the significant assumption
that the **web framework associates web requests with application threads**.  It is
however **strongly recommended that the integration tools provided with the web framework
itself be used, if available**, instead of :class:`.scoped_session`.

In particular, while using a thread local can be convenient, it is preferable that the :class:`.Session` be
associated **directly with the request**, rather than with
the current thread.   The next section on custom scopes details a more advanced configuration
which can combine the usage of :class:`.scoped_session` with direct request based scope, or
any kind of scope.

Using Custom Created Scopes
---------------------------

The :class:`.scoped_session` object's default behavior of "thread local" scope is only
one of many options on how to "scope" a :class:`.Session`.   A custom scope can be defined
based on any existing system of getting at "the current thing we are working with".

Suppose a web framework defines a library function ``get_current_request()``.  An application
built using this framework can call this function at any time, and the result will be
some kind of ``Request`` object that represents the current request being processed.
If the ``Request`` object is hashable, then this function can be easily integrated with
:class:`.scoped_session` to associate the :class:`.Session` with the request.  Below we illustrate
this in conjunction with a hypothetical event marker provided by the web framework
``on_request_end``, which allows code to be invoked whenever a request ends::

    from my_web_framework import get_current_request, on_request_end
    from sqlalchemy.orm import scoped_session, sessionmaker

    Session = scoped_session(sessionmaker(bind=some_engine), scopefunc=get_current_request)

    @on_request_end
    def remove_session(req):
        Session.remove()

Above, we instantiate :class:`.scoped_session` in the usual way, except that we pass
our request-returning function as the "scopefunc".  This instructs :class:`.scoped_session`
to use this function to generate a dictionary key whenever the registry is called upon
to return the current :class:`.Session`.   In this case it is particularly important
that we ensure a reliable "remove" system is implemented, as this dictionary is not
otherwise self-managed.


Contextual Session API
----------------------

.. autoclass:: sqlalchemy.orm.scoping.scoped_session
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
argument to :class:`.sessionmaker`::

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

.. autoclass:: sessionmaker
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.session.Session
   :members:
   :inherited-members:

.. autoclass:: sqlalchemy.orm.session.SessionTransaction
   :members:

Session Utilites
----------------

.. autofunction:: make_transient

.. autofunction:: make_transient_to_detached

.. autofunction:: object_session

.. autofunction:: sqlalchemy.orm.util.was_deleted

Attribute and State Management Utilities
-----------------------------------------

These functions are provided by the SQLAlchemy attribute
instrumentation API to provide a detailed interface for dealing
with instances, attribute values, and history.  Some of them
are useful when constructing event listener functions, such as
those described in :doc:`/orm/events`.

.. currentmodule:: sqlalchemy.orm.util

.. autofunction:: object_state

.. currentmodule:: sqlalchemy.orm.attributes

.. autofunction:: del_attribute

.. autofunction:: get_attribute

.. autofunction:: get_history

.. autofunction:: init_collection

.. autofunction:: flag_modified

.. function:: instance_state

    Return the :class:`.InstanceState` for a given
    mapped object.

    This function is the internal version
    of :func:`.object_state`.   The
    :func:`.object_state` and/or the
    :func:`.inspect` function is preferred here
    as they each emit an informative exception
    if the given object is not mapped.

.. autofunction:: sqlalchemy.orm.instrumentation.is_instrumented

.. autofunction:: set_attribute

.. autofunction:: set_committed_value

.. autoclass:: History
    :members:


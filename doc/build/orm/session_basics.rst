==============
Session Basics
==============

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
be used by the rest of the application as the source of new :class:`.Session`
instances, keeping the configuration for how :class:`.Session` objects
are constructed in one place.

The :class:`.sessionmaker` factory can also be used in conjunction with
other helpers, which are passed a user-defined :class:`.sessionmaker` that
is then maintained by the helper.  Some of these helpers are discussed in the
section :ref:`session_faq_whentocreate`.

Adding Additional Configuration to an Existing sessionmaker()
-------------------------------------------------------------

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
--------------------------------------------------------

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


.. _session_faq:

Session Frequently Asked Questions
==================================

By this point, many users already have questions about sessions.
This section presents a mini-FAQ (note that we have also a :doc:`real FAQ </faq/index>`)
of the most basic issues one is presented with when using a :class:`.Session`.

When do I make a :class:`.sessionmaker`?
----------------------------------------

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
-------------------------------------------------------------------------------------

.. topic:: tl;dr;

    1. As a general rule, keep the lifecycle of the session **separate and
       external** from functions and objects that access and/or manipulate
       database data.  This will greatly help with achieving a predictable
       and consistent transactional scope.

    2. Make sure you have a clear notion of where transactions
       begin and end, and keep transactions **short**, meaning, they end
       at the series of a sequence of operations, instead of being held
       open indefinitely.

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

A web application is the easiest case because such an application is already
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
-----------------------

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
a pattern for implementing second level caching using `dogpile.cache <https://dogpilecache.readthedocs.io/>`_,
via the :ref:`examples_caching` example.

How can I get the :class:`~sqlalchemy.orm.session.Session` for a certain object?
------------------------------------------------------------------------------------

Use the :meth:`~.Session.object_session` classmethod
available on :class:`~sqlalchemy.orm.session.Session`::

    session = Session.object_session(someobject)

The newer :ref:`core_inspection_toplevel` system can also be used::

    from sqlalchemy import inspect
    session = inspect(someobject).session

.. _session_faq_threadsafe:

Is the session thread-safe?
---------------------------

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

Basics of Using a Session
=========================

The most basic :class:`.Session` use patterns are presented here.

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
~~~~~~~~~~~~~~~~~~~~~~~~~

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


Closing
-------

The :meth:`~.Session.close` method issues a
:meth:`~.Session.expunge_all`, and :term:`releases` any
transactional/connection resources. When connections are returned to the
connection pool, transactional state is rolled back as well.



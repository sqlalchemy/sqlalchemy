==============
Session Basics
==============

What does the Session do ?
==========================

In the most general sense, the :class:`~.Session` establishes all conversations
with the database and represents a "holding zone" for all the objects which
you've loaded or associated with it during its lifespan. It provides the
interface where SELECT and other queries are made that will return and modify
ORM-mapped objects.  The ORM objects themselves are maintained inside the
:class:`.Session`, inside a structure called the :term:`identity map` - a data
structure that maintains unique copies of each object, where "unique" means
"only one object with a particular primary key".

The :class:`.Session` begins in a mostly stateless form. Once queries are
issued or other objects are persisted with it, it requests a connection
resource from an :class:`_engine.Engine` that is associated with the
:class:`.Session`, and then establishes a transaction on that connection. This
transaction remains in effect until the :class:`.Session` is instructed to
commit or roll back the transaction.

The ORM objects maintained by a :class:`_orm.Session` are :term:`instrumented`
such that whenever an attribute or a collection is modified in the Python
program, a change event is generated which is recorded by the
:class:`_orm.Session`.  Whenever the database is about to be queried, or when
the transaction is about to be committed, the :class:`_orm.Session` first
**flushes** all pending changes stored in memory to the database. This is
known as the :term:`unit of work` pattern.

When using a :class:`.Session`, it's useful to consider the ORM mapped objects
that it maintains as **proxy objects** to database rows, which are local to the
transaction being held by the :class:`.Session`.    In order to maintain the
state on the objects as matching what's actually in the database, there are a
variety of events that will cause objects to re-access the database in order to
keep synchronized.   It is possible to "detach" objects from a
:class:`.Session`, and to continue using them, though this practice has its
caveats.  It's intended that usually, you'd re-associate detached objects with
another :class:`.Session` when you want to work with them again, so that they
can resume their normal task of representing database state.


Basics of Using a Session
=========================

The most basic :class:`.Session` use patterns are presented here.

.. _session_getting:

Opening and Closing a Session
-----------------------------

The :class:`_orm.Session` may be constructed on its own or by using the
:class:`_orm.sessionmaker` class.    It typically is passed a single
:class:`_engine.Engine` as a source of connectivity up front.  A typical use
may look like::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    # an Engine, which the Session will use for connection
    # resources
    engine = create_engine('postgresql://scott:tiger@localhost/')

    # create session and add objects
    with Session(engine) as session:
        session.add(some_object)
        session.add(some_other_object)
        session.commit()

Above, the :class:`_orm.Session` is instantiated with an :class:`_engine.Engine`
associated with a particular database URL.   It is then used in a Python
context manager (i.e. ``with:`` statement) so that it is automatically
closed at the end of the block; this is equivalent
to calling the :meth:`_orm.Session.close` method.

The call to :meth:`_orm.Session.commit` is optional, and is only needed if the
work we've done with the :class:`_orm.Session` includes new data to be
persisted to the database.  If we were only issuing SELECT calls and did not
need to write any changes, then the call to :meth:`_orm.Session.commit` would
be unnecessary.

Framing out a begin / commit / rollback block
-----------------------------------------------

We may also enclose the :meth:`_orm.Session.commit` call and the overall
"framing" of the transaction within a context manager for those cases where
we will be committing data to the database.  By "framing" we mean that if all
operations succeed, the :meth:`_orm.Session.commit` method will be called,
but if any exceptions are raised, the :meth:`_orm.Session.rollback` method
will be called so that the transaction is rolled back immediately, before
propagating the exception outward.   In Python this is most fundamentally
expressed using a ``try: / except: / else:`` block such as::

    # verbose version of what a context manager will do
    with Session(engine) as session:
        try:
            session.add(some_object)
            session.add(some_other_object)
        except:
            session.rollback()
            raise
        else:
            session.commit()

The long-form sequence of operations illustrated above can be
achieved more succinctly by making use of the
:class:`_orm.SessionTransaction` object returned by the :meth:`_orm.Session.begin`
method, which provides a context manager interface for the same sequence of
operations::

    # create session and add objects
    with Session(engine) as session:
        with session.begin():
          session.add(some_object)
          session.add(some_other_object)
        # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()

More succinctly, the two contexts may be combined::

    # create session and add objects
    with Session(engine) as session, session.begin():
        session.add(some_object)
        session.add(some_other_object)
    # inner context calls session.commit(), if there were no exceptions
    # outer context calls session.close()

Using a sessionmaker
--------------------

The purpose of :class:`_orm.sessionmaker` is to provide a factory for
:class:`_orm.Session` objects with a fixed configuration.   As it is typical
that an application will have an :class:`_engine.Engine` object in module
scope, the :class:`_orm.sessionmaker` can provide a factory for
:class:`_orm.Session` objects that are against this engine::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # an Engine, which the Session will use for connection
    # resources, typically in module scope
    engine = create_engine('postgresql://scott:tiger@localhost/')

    # a sessionmaker(), also in the same scope as the engine
    Session = sessionmaker(engine)

    # we can now construct a Session() without needing to pass the
    # engine each time
    with Session() as session:
        session.add(some_object)
        session.add(some_other_object)
        session.commit()
    # closes the session

The :class:`_orm.sessionmaker` is analogous to the :class:`_engine.Engine`
as a module-level factory for function-level sessions / connections.   As such
it also has its own :meth:`_orm.sessionmaker.begin` method, analogous
to :meth:`_engine.Engine.begin`, which returns a :class:`_orm.Session` object
and also maintains a begin/commit/rollback block::


    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # an Engine, which the Session will use for connection
    # resources
    engine = create_engine('postgresql://scott:tiger@localhost/')

    # a sessionmaker(), also in the same scope as the engine
    Session = sessionmaker(engine)

    # we can now construct a Session() and include begin()/commit()/rollback()
    # at once
    with Session.begin() as session:
        session.add(some_object)
        session.add(some_other_object)
    # commits the transaction, closes the session

Where above, the :class:`_orm.Session` will both have its transaction committed
as well as that the :class:`_orm.Session` will be closed, when the above
``with:`` block ends.

When you write your application, the
:class:`.sessionmaker` factory should be scoped the same as the
:class:`_engine.Engine` object created by :func:`_sa.create_engine`, which
is typically at module-level or global scope.  As these objects are both
factories, they can be used by any number of functions and threads
simultaneously.

.. seealso::

    :class:`_orm.sessionmaker`

    :class:`_orm.Session`

.. _session_querying_1x:

Querying (1.x Style)
--------------------

The :meth:`~.Session.query` function takes one or more
**entities** and returns a new :class:`~sqlalchemy.orm.query.Query` object which
will issue mapper queries within the context of this Session.   By
"entity" we refer to a mapped class, an attribute of a mapped class, or
other ORM constructs such as an :func:`_orm.aliased` construct::

    # query from a class
    results = session.query(User).filter_by(name='ed').all()

    # query with multiple classes, returns tuples
    results = session.query(User, Address).join('addresses').filter_by(name='ed').all()

    # query using orm-columns, also returns tuples
    results = session.query(User.name, User.fullname).all()

When ORM objects are returned in results, they are also stored in the identity
map.  When an incoming database row has a primary key that matches an object
which is already present, the same object is returned, and those attributes
of the object which already have a value are not re-populated.

The :class:`_orm.Session` automatically expires all instances along transaction
boundaries (i.e. when the current transaction is committed or rolled back) so
that with a normally isolated transaction, data will refresh itself when a new
transaction begins.

The :class:`_query.Query` object is introduced in great detail in
:ref:`ormtutorial_toplevel`, and further documented in
:ref:`query_api_toplevel`.

.. seealso::

    :ref:`ormtutorial_toplevel`

    :meth:`_orm.Session.query`

    :ref:`query_api_toplevel`

.. _session_querying_20:

Querying (2.0 style)
--------------------

.. versionadded:: 1.4

SQLAlchemy 2.0 will standardize the production of SELECT statements across both
Core and ORM by making direct use of the :class:`_sql.Select` object within the
ORM, removing the need for there to be a separate :class:`_orm.Query`
object.    This mode of operation is available in SQLAlchemy 1.4 right now to
support applications that will be migrating to 2.0.   The :class:`_orm.Session`
must be instantiated with the
:paramref:`_orm.Session.future` flag set to ``True``; from that point on the
:meth:`_orm.Session.execute` method will return ORM results via the
standard :class:`_engine.Result` object when invoking :func:`_sql.select`
statements that use ORM entities::

    from sqlalchemy import select
    from sqlalchemy.orm import Session

    session = Session(engine, future=True)

    # query from a class
    statement = select(User).filter_by(name="ed")

    # list of first element of each row (i.e. User objects)
    result = session.execute(statement).scalars().all()

    # query with multiple classes
    statement = select(User, Address).join('addresses').filter_by(name='ed')

    # list of tuples
    result = session.execute(statement).all()

    # query with ORM columns
    statement = select(User.name, User.fullname)

    # list of tuples
    result = session.execute(statement).all()

It's important to note that while methods of :class:`_query.Query` such as
:meth:`_query.Query.all` and :meth:`_query.Query.one` will return instances
of ORM mapped objects directly in the case that only a single complete
entity were requested, the :class:`_engine.Result` object returned
by :meth:`_orm.Session.execute` will always deliver rows (named tuples)
by default; this is so that results against single or multiple ORM objects,
columns, tables, etc. may all be handled identically.

If only one ORM entity was queried, the rows returned will have exactly one
column, consisting of the ORM-mapped object instance for each row.  To convert
these rows into object instances without the tuples, the
:meth:`_engine.Result.scalars` method is used to first apply a "scalars" filter
to the result; then the :class:`_engine.Result` can be iterated or deliver rows
via standard methods such as :meth:`_engine.Result.all`,
:meth:`_engine.Result.first`, etc.

.. seealso::

    :ref:`migration_20_toplevel`



Adding New or Existing Items
----------------------------

:meth:`~.Session.add` is used to place instances in the
session. For :term:`transient` (i.e. brand new) instances, this will have the effect
of an INSERT taking place for those instances upon the next flush. For
instances which are :term:`persistent` (i.e. were loaded by this session), they are
already present and do not need to be added. Instances which are :term:`detached`
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

:meth:`_orm.Session.delete` marks an object for deletion, which will
result in a DELETE statement emitted for each primary key affected.
Before the pending deletes are flushed, objects marked by "delete" are present
in the :attr:`_orm.Session.deleted` collection.  After the DELETE, they
are expunged from the :class:`_orm.Session`, which becomes permanent after
the transaction is committed.

There are various important behaviors related to the
:meth:`_orm.Session.delete` operation, particularly in how relationships to
other objects and collections are handled.    There's more information on how
this works in the section :ref:`unitofwork_cascades`, but in general
the rules are:

* Rows that correspond to mapped objects that are related to a deleted
  object via the :func:`_orm.relationship` directive are **not
  deleted by default**.  If those objects have a foreign key constraint back
  to the row being deleted, those columns are set to NULL.   This will
  cause a constraint violation if the columns are non-nullable.

* To change the "SET NULL" into a DELETE of a related object's row, use the
  :ref:`cascade_delete` cascade on the :func:`_orm.relationship`.

* Rows that are in tables linked as "many-to-many" tables, via the
  :paramref:`_orm.relationship.secondary` parameter, **are** deleted in all
  cases when the object they refer to is deleted.

* When related objects include a foreign key constraint back to the object
  being deleted, and the related collections to which they belong are not
  currently loaded into memory, the unit of work will emit a SELECT to fetch
  all related rows, so that their primary key values can be used to emit either
  UPDATE or DELETE statements on those related rows.  In this way, the ORM
  without further instruction will perform the function of ON DELETE CASCADE,
  even if this is configured on Core :class:`_schema.ForeignKeyConstraint`
  objects.

* The :paramref:`_orm.relationship.passive_deletes` parameter can be used
  to tune this behavior and rely upon "ON DELETE CASCADE" more naturally;
  when set to True, this SELECT operation will no longer take place, however
  rows that are locally present will still be subject to explicit SET NULL
  or DELETE.   Setting :paramref:`_orm.relationship.passive_deletes` to
  the string ``"all"`` will disable **all** related object update/delete.

* When the DELETE occurs for an object marked for deletion, the object
  is not automatically removed from collections or object references that
  refer to it.   When the :class:`_orm.Session` is expired, these collections
  may be loaded again so that the object is no longer present.  However,
  it is preferable that instead of using :meth:`_orm.Session.delete` for
  these objects, the object should instead be removed from its collection
  and then :ref:`cascade_delete_orphan` should be used so that it is
  deleted as a secondary effect of that collection removal.   See the
  section :ref:`session_deleting_from_collections` for an example of this.

.. seealso::

    :ref:`cascade_delete` - describes "delete cascade", which marks related
    objects for deletion when a lead object is deleted.

    :ref:`cascade_delete_orphan` - describes "delete orphan cascade", which
    marks related objects for deletion when they are de-associated from their
    lead object.

    :ref:`session_deleting_from_collections` - important background on
    :meth:`_orm.Session.delete` as involves relationships being refreshed
    in memory.

.. _session_flushing:

Flushing
--------

When the :class:`~sqlalchemy.orm.session.Session` is used with its default
configuration, the flush step is nearly always done transparently.
Specifically, the flush occurs before any individual
SQL statement is issued as a result of a :class:`_query.Query` or
a :term:`2.0-style` :meth:`_orm.Session.execute` call, as well as within the
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

More conveniently, it can be turned off within a context managed block using :attr:`.Session.no_autoflush`::

    with mysession.no_autoflush:
        mysession.add(some_object)
        mysession.flush()

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


Expiring / Refreshing
---------------------

An important consideration that will often come up when using the
:class:`_orm.Session` is that of dealing with the state that is present on
objects that have been loaded from the database, in terms of keeping them
synchronized with the current state of the transaction.   The SQLAlchemy
ORM is based around the concept of an :term:`identity map` such that when
an object is "loaded" from a SQL query, there will be a unique Python
object instance maintained corresponding to a particular database identity.
This means if we emit two separate queries, each for the same row, and get
a mapped object back, the two queries will have returned the same Python
object::

  >>> u1 = session.query(User).filter(id=5).first()
  >>> u2 = session.query(User).filter(id=5).first()
  >>> u1 is u2
  True

Following from this, when the ORM gets rows back from a query, it will
**skip the population of attributes** for an object that's already loaded.
The design assumption here is to assume a transaction that's perfectly
isolated, and then to the degree that the transaction isn't isolated, the
application can take steps on an as-needed basis to refresh objects
from the database transaction.  The FAQ entry at :ref:`faq_session_identity`
discusses this concept in more detail.

When an ORM mapped object is loaded into memory, there are three general
ways to refresh its contents with new data from the current transaction:

* **the expire() method** - the :meth:`_orm.Session.expire` method will
  erase the contents of selected or all attributes of an object, such that they
  will be loaded from the database when they are next accessed, e.g. using
  a :term:`lazy loading` pattern::

    session.expire(u1)
    u1.some_attribute  # <-- lazy loads from the transaction

  ..

* **the refresh() method** - closely related is the :meth:`_orm.Session.refresh`
  method, which does everything the :meth:`_orm.Session.expire` method does
  but also emits one or more SQL queries immediately to actually refresh
  the contents of the object::

    session.refresh(u1)  # <-- emits a SQL query
    u1.some_attribute  # <-- is refreshed from the transaction

  ..

* **the populate_existing() method** - this method is actually on the
  :class:`_orm.Query` object as :meth:`_orm.Query.populate_existing`
  and indicates that it should return objects that are unconditionally
  re-populated from their contents in the database::

    u2 = session.query(User).populate_existing().filter(id=5).first()

  ..

Further discussion on the refresh / expire concept can be found at
:ref:`session_expire`.

.. seealso::

  :ref:`session_expire`

  :ref:`faq_session_identity`



.. _orm_expression_update_delete:

UPDATE and DELETE with arbitrary WHERE clause
---------------------------------------------

The sections above on :meth:`_orm.Session.flush` and :meth:`_orm.Session.delete`
detail how rows can be inserted, updated and deleted in the database,
based on primary key identities that are referred towards by mapped Python
objets in the application.   The :class:`_orm.Session` can also emit UPDATE
and DELETE statements with arbitrary WHERE clauses as well, and at the same
time refresh locally present objects which match those rows.

To emit an ORM-enabled UPDATE in :term:`1.x style`, the :meth:`_query.Query.update` method
may be used::

    session.query(User).filter(User.name == "squidward").\
        update({"name": "spongebob"}, synchronize_session="fetch")

Above, an UPDATE will be emitted against all rows that match the name
"squidward" and be updated to the name "spongebob".  The
:paramref:`_query.Query.update.synchronize_session` parameter referring to
"fetch" indicates the list of affected primary keys should be fetched either
via a separate SELECT statement or via RETURNING if the backend database supports it;
objects locally present in memory will be updated in memory based on these
primary key identities.

For ORM-enabled UPDATEs in :term:`2.0 style`, :meth:`_orm.Session.execute` is used with the
Core :class:`_sql.Update` construct::

    from sqlalchemy import update

    stmt = update(User).where(User.name == "squidward").values(name="spongebob").\
        execution_options(synchronize_session="fetch")

    session.execute(stmt)

Above, the :meth:`_dml.Update.execution_options` method may be used to
establish execution-time options such as "synchronize_session".

DELETEs work in the same way as UPDATE except there is no "values / set"
clause established.  When synchronize_session is used, matching objects
within the :class:`_orm.Session` will be marked as deleted and expunged.

ORM-enabled delete, :term:`1.x style`::

    session.query(User).filter(User.name == "squidward").\
        delete(synchronize_session="fetch")

ORM-enabled delete, :term:`2.0 style`::

    from sqlalchemy import delete

    stmt = delete(User).where(User.name == "squidward").execution_options(synchronize_session="fetch")

    session.execute(stmt)


With both the 1.x and 2.0 form of ORM-enabled updates and deletes, the following
values for ``synchronize_session`` are supported:

* ``False`` - don't synchronize the session. This option is the most
  efficient and is reliable once the session is expired, which
  typically occurs after a commit(), or explicitly using
  expire_all(). Before the expiration, objects that were updated or deleted
  in the database may still
  remain in the session with stale values, which
  can lead to confusing results.

* ``'fetch'`` - Retrieves the primary key identity of affected rows by either
  performing a SELECT before the UPDATE or DELETE, or by using RETURNING
  if the database supports it, so that in-memory objects which are affected
  by the operation can be refreshed with new values (updates) or expunged
  from the :class:`_orm.Session` (deletes)

* ``'evaluate'`` - Evaluate the WHERE criteria given in the UPDATE or DELETE
  statement in Python, to locate matching objects within the
  :class:`_orm.Session`.   This approach does not add any round trips and in
  the absense of RETURNING support is more efficient.  For UPDATE or DELETE
  statements with complex criteria, the ``'evaluate'`` strategy may not be
  able to evaluate the expression in Python and will raise an error.  If
  this occurs, use the ``'fetch'`` strategy for the operation instead.

  .. warning::

    The ``"evaluate"`` strategy should be avoided if an UPDATE operation is
    to run on a :class:`_orm.Session` that has many objects which have
    been expired, because it will necessarily need to refresh those objects
    as they are located which will emit a SELECT for each one.   The
    :class:`_orm.Session` may have expired objects if it is being used
    across multiple :meth:`_orm.Session.commit` calls and the
    :paramref:`_orm.Session.expire_on_commit` flag is at its default
    value of ``True``.


.. warning:: **Additional Caveats for ORM-enabled updates and deletes**

    The ORM-enabled UPDATE and DELETE features bypass ORM unit-of-work
    automation in favor being able to emit a single UPDATE or DELETE statement
    that matches multiple rows at once without complexity.

    * The operations do not offer in-Python cascading of
      relationships - it is assumed that ON UPDATE CASCADE and/or
      ON DELETE CASCADE is
      configured for any foreign key references which require
      it, otherwise the database may emit an integrity
      violation if foreign key references are being enforced.

    * After the UPDATE or DELETE, dependent objects in the
      :class:`.Session` which were impacted by an ON UPDATE CASCADE or ON
      DELETE CASCADE on related tables may not contain the current state;
      this issue is resolved once the :class:`.Session` is expired, which
      normally occurs upon :meth:`.Session.commit` or can be forced by
      using
      :meth:`.Session.expire_all`.

    * The ``'fetch'`` strategy, when run on a database that does not support
      RETURNING such as MySQL or SQLite, results in an additional SELECT
      statement emitted which may reduce performance.   Use SQL echoing when
      developing to evaluate the impact of SQL emitted.

    * ORM-enabled UPDATEs and DELETEs do not handle joined table inheritance
      automatically.   If the operation is against multiple tables, typically
      individual UPDATE / DELETE statements against the individual tables
      should be used.   Some databases support multiple table UPDATEs.
      Similar guidelines as those detailed at :ref:`multi_table_updates`
      may be applied.

    * The WHERE criteria needed in order to limit the polymorphic identity to
      specific subclasses for single-table-inheritance mappings **is included
      automatically** .   This only applies to a subclass mapper that has no
      table of its own.

      .. versionchanged:: 1.4  ORM updates/deletes now automatically
         accommodate for the WHERE criteria added for single-inheritance
         mappings.

    * The :func:`_orm.with_loader_criteria` option **is supported** by ORM
      update and delete operations; criteria here will be added to that of the
      UPDATE or DELETE statement being emitted, as well as taken into account
      during the "synchronize" process.

    * In order to intercept ORM-enabled UPDATE and DELETE operations with event
      handlers, use the :meth:`_orm.SessionEvents.do_orm_execute` event.



.. _session_committing:

Committing
----------

:meth:`~.Session.commit` is used to commit the current
transaction. It always issues :meth:`~.Session.flush`
beforehand to flush any remaining state to the database; this is independent
of the "autoflush" setting.

If the :class:`_orm.Session` does not currently have a transaction present,
the method will silently pass, unless the legacy "autocommit" mode is enabled
in which it will raise an error.

Another behavior of :meth:`~.Session.commit` is that by
default it expires the state of all instances present after the commit is
complete. This is so that when the instances are next accessed, either through
attribute access or by them being present in the result of a SELECT,
they receive the most recent  state.   This behavior may be controlled
by the :paramref:`_orm.Session.expire_on_commit` flag, which may be set
to ``False`` when this behavior is undesirable.

.. _session_rollback:

Rolling Back
------------

:meth:`~.Session.rollback` rolls back the current
transaction. With a default configured session, the post-rollback state of the
session is as follows:

  * All transactions are rolled back and all connections returned to the
    connection pool, unless the Session was bound directly to a Connection, in
    which case the connection is still maintained (but still rolled back).
  * Objects which were initially in the :term:`pending` state when they were added
    to the :class:`~sqlalchemy.orm.session.Session` within the lifespan of the
    transaction are expunged, corresponding to their INSERT statement being
    rolled back. The state of their attributes remains unchanged.
  * Objects which were marked as :term:`deleted` within the lifespan of the
    transaction are promoted back to the :term:`persistent` state, corresponding to
    their DELETE statement being rolled back. Note that if those objects were
    first :term:`pending` within the transaction, that operation takes precedence
    instead.
  * All objects not expunged are fully expired.

With that state understood, the :class:`~sqlalchemy.orm.session.Session` may
safely continue usage after a rollback occurs.

When a :meth:`~.Session.flush` fails, typically for
reasons like primary key, foreign key, or "not nullable" constraint
violations, a ROLLBACK is issued
automatically (it's currently not possible for a flush to continue after a
partial failure).   However, the :class:`_orm.Session` goes into a state
known as "inactive" at this point, and the calling application must
always call the :meth:`_orm.Session.rollback` method explicitly so that
the :class:`_orm.Session` can go back into a useable state (it can also
be simply closed and discarded).   See the FAQ entry at
:ref:`faq_session_rollback` for further discussion.



Closing
-------

The :meth:`~.Session.close` method issues a :meth:`~.Session.expunge_all` which
removes all ORM-mapped objects from the session, and :term:`releases` any
transactional/connection resources from the :class:`_engine.Engine` object(s)
to which it is bound.   When connections are returned to the connection pool,
transactional state is rolled back as well.

When the :class:`_orm.Session` is closed, it is essentially in the
original state as when it was first constructed, and **may be used again**.
In this sense, the :meth:`_orm.Session.close` method is more like a "reset"
back to the clean state and not as much like a "database close" method.

It's recommended that the scope of a :class:`_orm.Session` be limited by
a call to :meth:`_orm.Session.close` at the end, especially if the
:meth:`_orm.Session.commit` or :meth:`_orm.Session.rollback` methods are not
used.    The :class:`_orm.Session` may be used as a context manager to ensure
that :meth:`_orm.Session.close` is called::

    with Session(engine) as session:
        result = session.execute(select(User))

    # closes session automatically


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
This transaction remains in progress until the :class:`.Session`
is rolled back, committed, or closed.   The :class:`.Session` will
begin a new transaction if it is used again, subsequent to the previous
transaction ending; from this it follows that the :class:`.Session`
is capable of having a lifespan across many transactions, though only
one at a time.   We refer to these two concepts as **transaction scope**
and **session scope**.

It's usually not very hard to determine the best points at which
to begin and end the scope of a :class:`.Session`, though the wide
variety of application architectures possible can introduce
challenging situations.

Some sample scenarios include:

* Web applications.  In this case, it's best to make use of the SQLAlchemy
  integrations provided by the web framework in use.  Or otherwise, the
  basic pattern is create a :class:`_orm.Session` at the start of a web
  request, call the :meth:`_orm.Session.commit` method at the end of
  web requests that do POST, PUT, or DELETE, and then close the session
  at the end of web request.  It's also usually a good idea to set
  :paramref:`_orm.Session.expire_on_commit` to False so that subsequent
  access to objects that came from a :class:`_orm.Session` within the
  view layer do not need to emit new SQL queries to refresh the objects,
  if the transaction has been committed already.

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
**separate and external**.  The example below illustrates how this might look,
and additionally makes use of a Python context manager (i.e. the ``with:``
keyword) in order to manage the scope of the :class:`_orm.Session` and its
transaction automatically::

    ### this is a **better** (but not the only) way to do it ###

    class ThingOne(object):
        def go(self, session):
            session.query(FooBar).update({"x": 5})

    class ThingTwo(object):
        def go(self, session):
            session.query(Widget).update({"q": 18})

    def run_my_program():
        with Session() as session:
            with session.begin():
                ThingOne().go(session)
                ThingTwo().go(session)


.. versionchanged:: 1.4 The :class:`_orm.Session` may be used as a context
   manager without the use of external helper functions.

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

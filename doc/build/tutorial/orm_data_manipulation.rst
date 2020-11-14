.. |prev| replace:: :doc:`data`
.. |next| replace:: :doc:`orm_related_objects`

.. include:: tutorial_nav_include.rst

.. rst-class:: orm-header

.. _tutorial_orm_data_manipulation:

Data Manipulation with the ORM
==============================

The previous section :ref:`tutorial_working_with_data` remained focused on
the SQL Expression Language from a Core perspective, in order to provide
continuity across the major SQL statement constructs.  This section will
then build out the lifecycle of the :class:`_orm.Session` and how it interacts
with these constructs.

**Prerequisite Sections** - the ORM focused part of the tutorial builds upon
two previous ORM-centric sections in this document:

* :ref:`tutorial_executing_orm_session` - introduces how to make an ORM :class:`_orm.Session` object

* :ref:`tutorial_orm_table_metadata` - where we set up our ORM mappings of the ``User`` and ``Address`` entities

* :ref:`tutorial_selecting_orm_entities` - a few examples on how to run SELECT statements for entities like ``User``

.. _tutorial_inserting_orm:

Inserting Rows with the ORM
---------------------------

When using the ORM, the :class:`_orm.Session` object is responsible for
constructing :class:`_sql.Insert` constructs and emitting them for us in a
transaction. The way we instruct the :class:`_orm.Session` to do so is by
**adding** object entries to it; the :class:`_orm.Session` then makes sure
these new entries will be emitted to the database when they are needed, using
a process known as a **flush**.

Instances of Classes represent Rows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whereas in the previous example we emitted an INSERT using Python dictionaries
to indicate the data we wanted to add, with the ORM we make direct use of the
custom Python classes we defined, back at
:ref:`tutorial_orm_table_metadata`.    At the class level, the ``User`` and
``Address`` classes served as a place to define what the corresponding
database tables should look like.   These classes also serve as extensible
data objects that we use to create and manipulate rows within a transaction
as well.  Below we will create two ``User`` objects each representing a
potential database row to be INSERTed::

    >>> squidward = User(name="squidward", fullname="Squidward Tentacles")
    >>> krabs = User(name="ehkrabs", fullname="Eugene H. Krabs")

We are able to construct these objects using the names of the mapped columns as
keyword arguments in the constructor.  This is possible as the ``User`` class
includes an automatically generated ``__init__()`` constructor that was
provided by the ORM mapping so that we could create each object using column
names as keys in the constructor.

In a similar manner as in our Core examples of :class:`_sql.Insert`, we did not
include a primary key (i.e. an entry for the ``id`` column), since we would
like to make use of the auto-incrementing primary key feature of the database,
SQLite in this case, which the ORM also integrates with.
The value of the ``id`` attribute on the above
objects, if we were to view it, displays itself as ``None``::

    >>> squidward
    User(id=None, name='squidward', fullname='Squidward Tentacles')

The ``None`` value is provided by SQLAlchemy to indicate that the attribute
has no value as of yet.  SQLAlchemy-mapped attributes always return a value
in Python and don't raise ``AttributeError`` if they're missing, when
dealing with a new object that has not had a value assigned.

At the moment, our two objects above are said to be in a state called
:term:`transient` - they are not associated with any database state and are yet
to be associated with a :class:`_orm.Session` object that can generate
INSERT statements for them.

Adding objects to a Session
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To illustrate the addition process step by step, we will create a
:class:`_orm.Session` without using a context manager (and hence we must
make sure we close it later!)::

    >>> session = Session(engine)

The objects are then added to the :class:`_orm.Session` using the
:meth:`_orm.Session.add` method.   When this is called, the objects are in a
state known as :term:`pending` and have not been inserted yet::

    >>> session.add(squidward)
    >>> session.add(krabs)

When we have pending objects, we can see this state by looking at a
collection on the :class:`_orm.Session` called :attr:`_orm.Session.new`::

    >>> session.new
    IdentitySet([User(id=None, name='squidward', fullname='Squidward Tentacles'), User(id=None, name='ehkrabs', fullname='Eugene H. Krabs')])

The above view is using a collection called :class:`.IdentitySet` that is
essentially a Python set that hashes on object identity in all cases (i.e.,
using Python built-in ``id()`` function, rather than the Python ``hash()`` function).

Flushing
^^^^^^^^

The :class:`_orm.Session` makes use of a pattern known as :term:`unit of work`.
This generally means it accumulates changes one at a time, but does not actually
communicate them to the database until needed.   This allows it to make
better decisions about how SQL DML should be emitted in the transaction based
on a given set of pending changes.   When it does emit SQL to the database
to push out the current set of changes, the process is known as a **flush**.

We can illustrate the flush process manually by calling the :meth:`_orm.Session.flush`
method:

.. sourcecode:: pycon+sql

    >>> session.flush()
    {opensql}BEGIN (implicit)
    INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] ('squidward', 'Squidward Tentacles')
    INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] ('ehkrabs', 'Eugene H. Krabs')

Above we observe the :class:`_orm.Session` was first called upon to emit SQL,
so it created a new transaction and emitted the appropriate INSERT statements
for the two objects.   The transaction now **remains open** until we call any
of the :meth:`_orm.Session.commit`, :meth:`_orm.Session.rollback`, or
:meth:`_orm.Session.close` methods of :class:`_orm.Session`.

While :meth:`_orm.Session.flush` may be used to manually push out pending
changes to the current transaction, it is usually unnecessary as the
:class:`_orm.Session` features a behavior known as **autoflush**, which
we will illustrate later.   It also flushes out changes whenever
:meth:`_orm.Session.commit` is called.


Autogenerated primary key attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the rows are inserted, the two Python objects we've created are in a
state known as :term:`persistent`, where they are associated with the
:class:`_orm.Session` object in which they were added or loaded, and feature lots of
other behaviors that will be covered later.

Another effect of the INSERT that occurred was that the ORM has retrieved the
new primary key identifiers for each new object; internally it normally uses
the same :attr:`_engine.CursorResult.inserted_primary_key` accessor we
introduced previously.   The ``squidward`` and ``krabs`` objects now have these new
primary key identifiers associated with them and we can view them by acesssing
the ``id`` attribute::

    >>> squidward.id
    4
    >>> krabs.id
    5

.. tip::  Why did the ORM emit two separate INSERT statements when it could have
   used :ref:`executemany <tutorial_multiple_parameters>`?  As we'll see in the
   next section, the
   :class:`_orm.Session` when flushing objects always needs to know the
   primary key of newly inserted objects.  If a feature such as SQLite's autoincrement is used
   (other examples include PostgreSQL IDENTITY or SERIAL, using sequences,
   etc.), the :attr:`_engine.CursorResult.inserted_primary_key` feature
   usually requires that each INSERT is emitted one row at a time.  If we had provided values for the primary keys ahead of
   time, the ORM would have been able to optimize the operation better.  Some
   database backends such as :ref:`psycopg2 <postgresql_psycopg2>` can also
   INSERT many rows at once while still being able to retrieve the primary key
   values.

Identity Map
^^^^^^^^^^^^

The primary key identity of the objects are significant to the :class:`_orm.Session`,
as the objects are now linked to this identity in memory using a feature
known as the :term:`identity map`.   The identity map is an in-memory store
that links all objects currently loaded in memory to their primary key
identity.   We can observe this by retrieving one of the above objects
using the :meth:`_orm.Session.get` method, which will return an entry
from the identity map if locally present, otherwise emitting a SELECT::

    >>> some_squidward = session.get(User, 4)
    >>> some_squidward
    User(id=4, name='squidward', fullname='Squidward Tentacles')

The important thing to note about the identity map is that it maintains a
**unique instance** of a particular Python object per a particular database
identity, within the scope of a particular :class:`_orm.Session` object.  We
may observe that the ``some_squidward`` refers to the **same object** as that
of ``squidward`` previously::

    >>> some_squidward is squidward
    True

The identity map is a critical feature that allows complex sets of objects
to be manipulated within a transaction without things getting out of sync.


Committing
^^^^^^^^^^^

There's much more to say about how the :class:`_orm.Session` works which will
be discussed further.   For now we will commit the transaction so that
we can build up knowledge on how to SELECT rows before examining more ORM
behaviors and features:

.. sourcecode:: pycon+sql

    >>> session.commit()
    COMMIT

.. _tutorial_orm_updating:

Updating ORM Objects
--------------------

In the preceding section :ref:`tutorial_core_update_delete`, we introduced the
:class:`_sql.Update` construct that represents a SQL UPDATE statement. When
using the ORM, there are two ways in which this construct is used. The primary
way is that it is emitted automatically as part of the :term:`unit of work`
process used by the :class:`_orm.Session`, where an UPDATE statement is emitted
on a per-primary key basis corresponding to individual objects that have
changes on them.   A second form of UPDATE is called an "ORM enabled
UPDATE" and allows us to use the :class:`_sql.Update` construct with the
:class:`_orm.Session` explicitly; this is described in the next section.

Supposing we loaded the ``User`` object for the username ``sandy`` into
a transaction (also showing off the :meth:`_sql.Select.filter_by` method
as well as the :meth:`_engine.Result.scalar_one` method):

.. sourcecode:: pycon+sql

    {sql}>>> sandy = session.execute(select(User).filter_by(name="sandy")).scalar_one()
    BEGIN (implicit)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('sandy',)

The Python object ``sandy`` as mentioned before acts as a **proxy** for the
row in the database, more specifically the database row **in terms of the
current transaction**, that has the primary key identity of ``2``::

    >>> sandy
    User(id=2, name='sandy', fullname='Sandy Cheeks')

If we alter the attributes of this object, the :class:`_orm.Session` tracks
this change::

    >>> sandy.fullname = "Sandy Squirrel"

The object appears in a collection called :attr:`_orm.Session.dirty`, indicating
the object is "dirty"::

    >>> sandy in session.dirty
    True

When the :class:`_orm.Session` next emits a flush, an UPDATE will be emitted
that updates this value in the database.  As mentioned previously, a flush
occurs automatically before we emit any SELECT, using a behavior known as
**autoflush**.  We can query directly for the ``User.fullname`` column
from this row and we will get our updated value back:

.. sourcecode:: pycon+sql

    >>> sandy_fullname = session.execute(
    ...     select(User.fullname).where(User.id == 2)
    ... ).scalar_one()
    {opensql}UPDATE user_account SET fullname=? WHERE user_account.id = ?
    [...] ('Sandy Squirrel', 2)
    SELECT user_account.fullname
    FROM user_account
    WHERE user_account.id = ?
    [...] (2,){stop}
    >>> print(sandy_fullname)
    Sandy Squirrel

We can see above that we requested that the :class:`_orm.Session` execute
a single :func:`_sql.select` statement.  However the SQL emitted shows
that an UPDATE were emitted as well, which was the flush process pushing
out pending changes.  The ``sandy`` Python object is now no longer considered
dirty::

    >>> sandy in session.dirty
    False

However note we are **still in a transaction** and our changes have not
been pushed to the database's permanent storage.   Since Sandy's last name
is in fact "Cheeks" not "Squirrel", we will repair this mistake later when
we roll back the transction.  But first we'll make some more data changes.


.. seealso::

    :ref:`session_flushing`- details the flush process as well as information
    about the :paramref:`_orm.Session.autoflush` setting.

.. _tutorial_orm_enabled_update:

ORM-enabled UPDATE statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As previously mentioned, there's a second way to emit UPDATE statements in
terms of the ORM, which is known as an **ORM enabled UPDATE statement**.   This allows the use
of a generic SQL UPDATE statement that can affect many rows at once.   For example
to emit an UPDATE that will change the ``User.fullname`` column based on
a value in the ``User.name`` column:

.. sourcecode:: pycon+sql

    >>> session.execute(
    ...     update(User).
    ...     where(User.name == "sandy").
    ...     values(fullname="Sandy Squirrel Extraodinaire")
    ... )
    {opensql}UPDATE user_account SET fullname=? WHERE user_account.name = ?
    [...] ('Sandy Squirrel Extraodinaire', 'sandy'){stop}
    <sqlalchemy.engine.cursor.CursorResult object ...>

When invoking the ORM-enabled UPDATE statement, special logic is used to locate
objects in the current session that match the given criteria, so that they
are refreshed with the new data.  Above, the ``sandy`` object identity
was located in memory and refreshed::

    >>> sandy.fullname
    'Sandy Squirrel Extraodinaire'

The refresh logic is known as the ``synchronize_session`` option, and is described
in detail in the section :ref:`orm_expression_update_delete`.

.. seealso::

    :ref:`orm_expression_update_delete` - describes ORM use of :func:`_sql.update`
    and :func:`_sql.delete` as well as ORM synchronization options.


.. _tutorial_orm_deleting:


Deleting ORM Objects
---------------------

To round out the basic persistence operations, an individual ORM object
may be marked for deletion by using the :meth:`_orm.Session.delete` method.
Let's load up ``patrick`` from the database:

.. sourcecode:: pycon+sql

    {sql}>>> patrick = session.get(User, 3)
    SELECT user_account.id AS user_account_id, user_account.name AS user_account_name,
    user_account.fullname AS user_account_fullname
    FROM user_account
    WHERE user_account.id = ?
    [...] (3,)

If we mark ``patrick`` for deletion, as is the case with other operations,
nothing actually happens yet until a flush proceeds::

    >>> session.delete(patrick)

Current ORM behavior is that ``patrick`` stays in the :class:`_orm.Session`
until the flush proceeds, which as mentioned before occurs if we emit a query:

.. sourcecode:: pycon+sql

    >>> session.execute(select(User).where(User.name == "patrick")).first()
    {opensql}SELECT address.id AS address_id, address.email_address AS address_email_address,
    address.user_id AS address_user_id
    FROM address
    WHERE ? = address.user_id
    [...] (3,)
    DELETE FROM user_account WHERE user_account.id = ?
    [...] (3,)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('patrick',)

Above, the SELECT we asked to emit was preceded by a DELETE, which indicated
the pending deletion for ``patrick`` proceeded.  There was also a ``SELECT``
against the ``address`` table, which was prompted by the ORM looking for rows
in this table which may be related to the target row; this behavior is part of
a behavior known as :term:`cascade`, and can be tailored to work more
efficiently by allowing the database to handle related rows in ``address``
automatically; the section :ref:`cascade_delete` has all the detail on this.

.. seealso::

    :ref:`cascade_delete` - describes how to tune the behavior of
    :meth:`_orm.Session.delete` in terms of how related rows in other tables
    should be handled.

Beyond that, the ``patrick`` object instance now being deleted is no longer
considered to be persistent within the :class:`_orm.Session`, as is shown
by the containment check::

    >>> patrick in session
    False

However just like the UPDATEs we made to the ``sandy`` object, every change
we've made here is local to an ongoing transaction, which won't become
permanent if we don't commit it.  As rolling the transaction back is actually
more interesting at the moment, we will do that in the next section.

.. _tutorial_orm_enabled_delete:

ORM-enabled DELETE Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Like UPDATE operations, there is also an ORM-enabled version of DELETE which we can
illustrate by using the :func:`_sql.delete` construct with
:meth:`_orm.Session.execute`.  It also has a feature by which **non expired**
objects (see :term:`expired`) that match the given deletion criteria will be
automatically marked as ":term:`deleted`" in the :class:`_orm.Session`:

.. sourcecode:: pycon+sql

    >>> # refresh the target object for demonstration purposes
    >>> # only, not needed for the DELETE
    {sql}>>> squidward = session.get(User, 4)
    SELECT user_account.id AS user_account_id, user_account.name AS user_account_name,
    user_account.fullname AS user_account_fullname
    FROM user_account
    WHERE user_account.id = ?
    [...] (4,){stop}

    >>> session.execute(delete(User).where(User.name == "squidward"))
    {opensql}DELETE FROM user_account WHERE user_account.name = ?
    [...] ('squidward',){stop}
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>

The ``squidward`` identity, like that of ``patrick``, is now also in a
deleted state.   Note that we had to re-load ``squidward`` above in order
to demonstrate this; if the object were expired, the DELETE operation
would not take the time to refresh expired objects just to see that they
had been deleted::

    >>> squidward in session
    False



Rolling Back
-------------

The :class:`_orm.Session` has a :meth:`_orm.Session.rollback` method that as
expected emits a ROLLBACK on the SQL connection in progress.  However, it also
has an effect on the objects that are currently associated with the
:class:`_orm.Session`, in our previous example the Python object ``sandy``.
While we changed the ``.fullname`` of the ``sandy`` object to read ``"Sandy
Squirrel"``, we want to roll back this change.   Calling
:meth:`_orm.Session.rollback` will not only roll back the transaction but also
**expire** all objects currently associated with this :class:`_orm.Session`,
which will have the effect that they will refresh themselves when next accessed
using a process known as :term:`lazy loading`:

.. sourcecode:: pycon+sql

    >>> session.rollback()
    ROLLBACK

To view the "expiration" process more closely, we may observe that the
Python object ``sandy`` has no state left within its Python ``__dict__``,
with the exception of a special SQLAlchemy internal state object::

    >>> sandy.__dict__
    {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at 0x...>}

This is the ":term:`expired`" state; accessing the attribute again will autobegin
a new transaction and refresh ``sandy`` with the current database row:

.. sourcecode:: pycon+sql

    >>> sandy.fullname
    {opensql}BEGIN (implicit)
    SELECT user_account.id AS user_account_id, user_account.name AS user_account_name,
    user_account.fullname AS user_account_fullname
    FROM user_account
    WHERE user_account.id = ?
    [...] (2,){stop}
    'Sandy Cheeks'

We may now observe that the full database row was also populated into the
``__dict__`` of the ``sandy`` object::

    >>> sandy.__dict__  # doctest: +SKIP
    {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at 0x...>,
     'id': 2, 'name': 'sandy', 'fullname': 'Sandy Cheeks'}

For deleted objects, when we earlier noted that ``patrick`` was no longer
in the session, that object's identity is also restored::

    >>> patrick in session
    True

and of course the database data is present again as well:


.. sourcecode:: pycon+sql

    {sql}>>> session.execute(select(User).where(User.name == 'patrick')).scalar_one() is patrick
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('patrick',){stop}
    True


Closing a Session
------------------

Within the above sections we used a :class:`_orm.Session` object outside
of a Python context manager, that is, we didn't use the ``with`` statement.
That's fine, however if we are doing things this way, it's best that we explicitly
close out the :class:`_orm.Session` when we are done with it:

.. sourcecode:: pycon+sql

    >>> session.close()
    {opensql}ROLLBACK

Closing the :class:`_orm.Session`, which is what happens when we use it in
a context manager as well, accomplishes the following things:

* It :term:`releases` all connection resources to the connection pool, cancelling
  out (e.g. rolling back) any transactions that were in progress.

  This means that when we make use of a session to perform some read-only
  tasks and then close it, we don't need to explicitly call upon
  :meth:`_orm.Session.rollback` to make sure the transaction is rolled back;
  the connection pool handles this.

* It **expunges** all objects from the :class:`_orm.Session`.

  This means that all the Python objects we had loaded for this :class:`_orm.Session`,
  like ``sandy``, ``patrick`` and ``squidward``, are now in a state known
  as :term:`detached`.  In particular, we will note that objects that were still
  in an :term:`expired` state, for example due to the call to :meth:`_orm.Session.commit`,
  are now non-functional, as they don't contain the state of a current row and
  are no longer associated with any database transaction in which to be
  refreshed::

    >>> squidward.name
    Traceback (most recent call last):
      ...
    sqlalchemy.orm.exc.DetachedInstanceError: Instance <User at 0x...> is not bound to a Session; attribute refresh operation cannot proceed

  The detached objects can be re-associated with the same, or a new
  :class:`_orm.Session` using the :meth:`_orm.Session.add` method, which
  will re-establish their relationship with their particular database row:

  .. sourcecode:: pycon+sql

      >>> session.add(squidward)
      >>> squidward.name
      {opensql}BEGIN (implicit)
      SELECT user_account.id AS user_account_id, user_account.name AS user_account_name, user_account.fullname AS user_account_fullname
      FROM user_account
      WHERE user_account.id = ?
      [...] (4,){stop}
      'squidward'

  ..

  .. tip::

      Try to avoid using objects in their detached state, if possible. When the
      :class:`_orm.Session` is closed, clean up references to all the
      previously attached objects as well.   For cases where detached objects
      are necessary, typically the immediate display of just-committed objects
      for a web application where the :class:`_orm.Session` is closed before
      the view is rendered, set the :paramref:`_orm.Session.expire_on_commit`
      flag to ``False``.
  ..

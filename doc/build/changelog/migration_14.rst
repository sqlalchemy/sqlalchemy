=============================
What's New in SQLAlchemy 1.4?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.3
    and SQLAlchemy version 1.4.


Behavioral Changes - ORM
========================

.. _change_4519:

Accessing an uninitialized collection attribute on a transient object no longer mutates __dict__
-------------------------------------------------------------------------------------------------

It has always been SQLAlchemy's behavior that accessing mapped attributes on a
newly created object returns an implicitly generated value, rather than raising
``AttributeError``, such as ``None`` for scalar attributes or ``[]`` for a
list-holding relationship::

    >>> u1 = User()
    >>> u1.name
    None
    >>> u1.addresses
    []

The rationale for the above behavior was originally to make ORM objects easier
to work with.  Since an ORM object represents an empty row when first created
without any state, it is intuitive that its un-accessed attributes would
resolve to ``None`` (or SQL NULL) for scalars and to empty collections for
relationships.   In particular, it makes possible an extremely common pattern
of being able to mutate the new collection without manually creating and
assigning an empty collection first::

    >>> u1 = User()
    >>> u1.addresses.append(Address())  # no need to assign u1.addresses = []

Up until version 1.0 of SQLAlchemy, the behavior of this initialization  system
for both scalar attributes as well as collections would be that the ``None`` or
empty collection would be *populated* into the object's  state, e.g.
``__dict__``.  This meant that the following two operations were equivalent::

    >>> u1 = User()
    >>> u1.name = None  # explicit assignment

    >>> u2 = User()
    >>> u2.name  # implicit assignment just by accessing it
    None

Where above, both ``u1`` and ``u2`` would have the value ``None`` populated
in the value of the ``name`` attribute.  Since this is a SQL NULL, the ORM
would skip including these values within an INSERT so that SQL-level defaults
take place, if any, else the value defaults to NULL on the database side.

In version 1.0 as part of :ref:`migration_3061`, this behavior was refined so
that the ``None`` value was no longer populated into ``__dict__``, only
returned.   Besides removing the mutating side effect of a getter operation,
this change also made it possible to set columns that did have server defaults
to the value NULL by actually assigning ``None``, which was now distinguished
from just reading it.

The change however did not accommodate for collections, where returning an
empty collection that is not assigned meant that this mutable collection would
be different each time and also would not be able to correctly accommodate for
mutating operations (e.g. append, add, etc.) called upon it.    While the
behavior continued to generally not get in anyone's way, an edge case was
eventually identified in :ticket:`4519` where this empty collection could be
harmful, which is when the object is merged into a session::

    >>> u1 = User(id=1)  # create an empty User to merge with id=1 in the database
    >>> merged1 = session.merge(u1)  # value of merged1.addresses is unchanged from that of the DB

    >>> u2 = User(id=2) # create an empty User to merge with id=2 in the database
    >>> u2.addresses
    []
    >>> merged2 = session.merge(u2)  # value of merged2.addresses has been emptied in the DB

Above, the ``.addresses`` collection on ``merged1`` will contain all the
``Address()`` objects that were already in the database.   ``merged2`` will
not; because it has an empty list implicitly assigned, the ``.addresses``
collection will be erased.   This is an example of where this mutating side
effect can actually mutate the database itself.

While it was considered that perhaps the attribute system should begin using
strict "plain Python" behavior, raising ``AttributeError`` in all cases for
non-existent attributes on non-persistent objects and requiring that  all
collections be explicitly assigned, such a change would likely be too extreme
for the vast number of applications that have relied upon this  behavior for
many years, leading to a complex rollout / backwards compatibility problem as
well as the likelihood that workarounds to restore the old behavior would
become prevalent, thus rendering the whole change ineffective in any case.

The change then is to keep the default producing behavior, but to finally make
the non-mutating behavior of scalars a reality for collections as well, via the
addition of additional mechanics in the collection system.  When accessing the
empty attribute, the new collection is created and associated with the state,
however is not added to ``__dict__`` until it is actually mutated::

    >>> u1 = User()
    >>> l1 = u1.addresses  # new list is created, associated with the state
    >>> assert u1.addresses is l1  # you get the same list each time you access it
    >>> assert "addresses" not in u1.__dict__  # but it won't go into __dict__ until it's mutated
    >>> from sqlalchemy import inspect
    >>> inspect(u1).attrs.addresses.history
    History(added=None, unchanged=None, deleted=None)

When the list is changed, then it becomes part of the tracked changes to
be persisted to the database::

    >>> l1.append(Address())
    >>> assert "addresses" in u1.__dict__
    >>> inspect(u1).attrs.addresses.history
    History(added=[<__main__.Address object at 0x7f49b725eda0>], unchanged=[], deleted=[])

This change is expected to have *nearly* no impact on existing applications
in any way, except that it has been observed that some applications may be
relying upon the implicit assignment of this collection, such as to assert that
the object contains certain values based on its ``__dict__``::

    >>> u1 = User()
    >>> u1.addresses
    []
    # this will now fail, would pass before
    >>> assert {k: v for k, v in u1.__dict__.items() if not k.startswith("_")} == {"addresses": []}

or to ensure that the collection won't require a lazy load to proceed, the
(admittedly awkward) code below will now also fail::

    >>> u1 = User()
    >>> u1.addresses
    []
    >>> s.add(u1)
    >>> s.flush()
    >>> s.close()
    >>> u1.addresses  # <-- will fail, .addresses is not loaded and object is detached

Applications that rely upon the implicit mutating behavior of collections will
need to be changed so that they assign the desired collection explicitly::

    >>> u1.addresses = []

:ticket:`4519`

.. _change_4662:

The "New instance conflicts with existing identity" error is now a warning
---------------------------------------------------------------------------

SQLAlchemy has always had logic to detect when an object in the :class:`.Session`
to be inserted has the same primary key as an object that is already present::

    class Product(Base):
        __tablename__ = 'product'

        id = Column(Integer, primary_key=True)

    session = Session(engine)

    # add Product with primary key 1
    session.add(Product(id=1))
    session.flush()

    # add another Product with same primary key
    session.add(Product(id=1))
    s.commit()  # <-- will raise FlushError

The change is that the :class:`.FlushError` is altered to be only a warning::

    sqlalchemy/orm/persistence.py:408: SAWarning: New instance <Product at 0x7f1ff65e0ba8> with identity key (<class '__main__.Product'>, (1,), None) conflicts with persistent instance <Product at 0x7f1ff60a4550>


Subsequent to that, the condition will attempt to insert the row into the
database which will emit :class:`.IntegrityError`, which is the same error that
would be raised if the primary key identity was not already present in the
:class:`.Session`::

    sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) UNIQUE constraint failed: product.id

The rationale is to allow code that is using :class:`.IntegrityError` to catch
duplicates to function regardless of the existing state of the
:class:`.Session`, as is often done using savepoints::


    # add another Product with same primary key
    try:
        with session.begin_nested():
            session.add(Product(id=1))
    except exc.IntegrityError:
        print("row already exists")

The above logic was not fully feasible earlier, as in the case that the
``Product`` object with the existing identity were already in the
:class:`.Session`, the code would also have to catch :class:`.FlushError`,
which additionally is not filtered for the specific condition of integrity
issues.   With the change, the above block behaves consistently with the
exception of the warning also being emitted.

Since the logic in question deals with the primary key, all databases emit an
integrity error in the case of primary key conflicts on INSERT.    The case
where an error would not be raised, that would have earlier, is the extremely
unusual scenario of a mapping that defines a primary key on the mapped
selectable that is more restrictive than what is actually configured in the
database schema, such as when mapping to joins of tables or when defining
additional columns as part of a composite primary key that is not actually
constrained in the database schema. However, these situations also work  more
consistently in that the INSERT would theoretically proceed whether or not the
existing identity were still in the database.  The warning can also be
configured to raise an exception using the Python warnings filter.


:ticket:`4662`


Behavior Changes - Core
========================

.. _change_4712:

Connection-level transactions can now be inactive based on subtransaction
-------------------------------------------------------------------------

A :class:`.Connection` now includes the behavior where a :class:`.Transaction`
can be made inactive due to a rollback on an inner transaction, however the
:class:`.Transaction` will not clear until it is itself rolled back.

This is essentially a new error condition which will disallow statement
executions to proceed on a :class:`.Connection` if an inner "sub" transaction
has been rolled back.  The behavior works very similarly to that of the
ORM :class:`.Session`, where if an outer transaction has been begun, it needs
to be rolled back to clear the invalid transaction; this behavior is described
in :ref:`faq_session_rollback`

While the :class:`.Connection` has had a less strict behavioral pattern than
the :class:`.Session`, this change was made as it helps to identify when
a subtransaction has rolled back the DBAPI transaction, however the external
code isn't aware of this and attempts to continue proceeding, which in fact
runs operations on a new transaction.   The "test harness" pattern described
at :ref:`session_external_transaction` is the common place for this to occur.

The new behavior is described in the errors page at :ref:`error_8s2a`.

=============================
What's New in SQLAlchemy 1.4?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.3
    and SQLAlchemy version 1.4.

    Version 1.4 is taking on a different focus than other SQLAlchemy releases
    in that it is in many ways attempting to serve as a potential migration
    point for a more dramatic series of API changes currently planned for
    release  2.0 of SQLAlchemy.   The focus of SQLAlchemy 2.0 is a modernized
    and slimmed down API that removes lots of usage patterns that have long
    been discouraged, as well as mainstreams the best ideas in SQLAlchemy as
    first class API features, with the goal being that there is much less
    ambiguity in how the API is to be used, as well as that a series of
    implicit behaviors and rarely-used API flags that complicate the internals
    and hinder performance will be removed.

API Changes - Core
==================

.. _change_4617:

A SELECT statement is no longer implicitly considered to be a FROM clause
--------------------------------------------------------------------------

This change is one of the larger conceptual changes in SQLAlchemy in many years,
however it is hoped that the end user impact is relatively small, as the change
more closely matches what databases like MySQL and PostgreSQL require in any case.

The most immediate noticeable impact is that a :func:`.select` can no longer
be embedded inside of another :func:`.select` directly, without explicitly
turning the inner :func:`.select` into a subquery first.  This is historically
performed by using the :meth:`.SelectBase.alias` method, which remains, however
is more explicitly suited by using a new method :meth:`.SelectBase.subquery`;
both methods do the same thing.   The object returned is now :class:`.Subquery`,
which is very similar to the :class:`.Alias` object and shares a common
base :class:`.AliasedReturnsRows`.

That is, this will now raise::

    stmt1 = select([user.c.id, user.c.name])
    stmt2 = select([addresses, stmt1]).select_from(addresses.join(stmt1))

Raising::

    sqlalchemy.exc.ArgumentError: Column expression or FROM clause expected,
    got <...Select object ...>. To create a FROM clause from a <class
    'sqlalchemy.sql.selectable.Select'> object, use the .subquery() method.

The correct calling form is instead::

    sq1 = select([user.c.id, user.c.name]).subquery()
    stmt2 = select([addresses, sq1]).select_from(addresses.join(sq1))

Noting above that the :meth:`.SelectBase.subquery` method is essentially
equivalent to using the :meth:`.SelectBase.alias` method.

The above calling form is typically required in any case as the call to
:meth:`.SelectBase.subquery` or :meth:`.SelectBase.alias` is needed to
ensure the subquery has a name.  The MySQL and PostgreSQL databases do not
accept unnamed subqueries in the FROM clause and they are of limited use
on other platforms; this is described further below.

Along with the above change, the general capability of :func:`.select` and
related constructs to create unnamed subqueries, which means a FROM subquery
that renders without any name i.e. "AS somename", has been removed, and the
ability of the :func:`.select` construct to implicitly create subqueries
without explicit calling code to do so is mostly deprecated.   In the above
example, as has always been the case, using the :meth:`.SelectBase.alias`
method as well as the new :meth:`.SelectBase.subquery` method without passing a
name will generate a so-called "anonymous" name, which is the familiar
``anon_1`` name we see in SQLAlchemy queries::

    SELECT
        addresses.id, addresses.email, addresses.user_id,
        anon_1.id, anon_1.name
    FROM
    addresses JOIN
    (SELECT users.id AS id, users.name AS name FROM users) AS anon_1
    ON addresses.user_id = anon_1.id

Unnamed subqueries in the FROM clause (which note are different from
so-called "scalar subqueries" which take the place of a column expression
in the columns clause or WHERE clause) are of extremely limited use in SQL,
and their production in SQLAlchemy has mostly presented itself as an
undesirable behavior that needs to be worked around.    For example,
both the MySQL and PostgreSQL outright reject the usage of unnamed subqueries::

    # MySQL / MariaDB:

    MariaDB [(none)]> select * from (select 1);
    ERROR 1248 (42000): Every derived table must have its own alias


    # PostgreSQL:

    test=> select * from (select 1);
    ERROR:  subquery in FROM must have an alias
    LINE 1: select * from (select 1);
                          ^
    HINT:  For example, FROM (SELECT ...) [AS] foo.

A database like SQLite accepts them, however it is still often the case that
the names produced from such a subquery are too ambiguous to be useful::

    sqlite> CREATE TABLE a(id integer);
    sqlite> CREATE TABLE b(id integer);
    sqlite> SELECT * FROM a JOIN (SELECT * FROM b) ON a.id=id;
    Error: ambiguous column name: id
    sqlite> SELECT * FROM a JOIN (SELECT * FROM b) ON a.id=b.id;
    Error: no such column: b.id

    # use a name
    sqlite> SELECT * FROM a JOIN (SELECT * FROM b) AS anon_1 ON a.id=anon_1.id;

Due to the above limitations, there are very few places in SQLAlchemy where
such a query form was valid; the one exception was within the Oracle dialect
where they were used to create OFFSET / LIMIT subqueries as Oracle does not
support these keywords directly; this implementation has been replaced by
one which uses anonymous subqueries.   Throughout the ORM, exception cases
that detect where a SELECT statement would be SELECTed from either encourage
the user to, or implicitly create, an anonymously named subquery; it is hoped
by moving to an all-explicit subquery much of the complexity incurred by
these areas can be removed.

As :class:`.SelectBase` objects are no longer :class:`.FromClause` objects,
attributes like the ``.c`` attribute as well as methods like ``.select()``,
``.join()``, and ``.outerjoin()`` upon :class:`.SelectBase` are now
deprecated, as these methods all imply implicit production of a subquery.
Instead, as is already what the vast majority of applications have to do
in any case, invoking :meth:`.SelectBase.alias` or :meth:`.SelectBase.subquery`
will provide for a :class:`.Subquery` object that provides all these attributes,
as it is part of the :class:`.FromClause` hierarchy.   In the interim, these
methods are still available, however they now produce an anonymously named
subquery rather than an unnamed one, and this subquery is distinct from the
:class:`.SelectBase` construct itself.

In place of the ``.c`` attribute, a new attribute :attr:`.SelectBase.selected_columns`
is added.  This attribute resolves to a column collection that is what most
people hope that ``.c`` does (but does not), which is to reference the columns
that are in the columns clause of the SELECT statement.   A common beginner mistake
is code such as the following::

    stmt = select([users])
    stmt = stmt.where(stmt.c.name == 'foo')

The above code appears intuitive and that it would generate
"SELECT * FROM users WHERE name='foo'", however veteran SQLAlchemy users will
recognize that it in fact generates a useless subquery resembling
"SELECT * FROM (SELECT * FROM users) WHERE name='foo'".

The new :attr:`.SelectBase.selected_columns` attribute however **does** suit
the use case above, as in a case like the above it links directly to the columns
present in the ``users.c`` collection::

    stmt = select([users])
    stmt = stmt.where(stmt.selected_columns.name == 'foo')

There is of course the notion that perhaps ``.c`` on :class:`.SelectBase` could
simply act the way :attr:`.SelectBase.selected_columns` does above, however in
light of the fact that ``.c`` is strongly associated with the :class:`.FromClause`
hierarchy, meaning that it is a set of columns that can be directly in the
FROM clause of another SELECT, it's better that a column collection that
serves an entirely different purpose have a new name.

In the bigger picture, the reason this change is being made now is towards the
goal of unifying the ORM :class:`.Query` object into the :class:`.SelectBase`
hierarchy in SQLAlchemy 2.0, so that the ORM will have a "``select()``"
construct that extends directly from the existing :func:`.select` object,
having the same methods and behaviors except that it will have additional ORM
functionality.   All statement objects in Core will also be fully cacheable
using a new system that resembles "baked queries" except that it will work
transparently for all statements across Core and ORM.   In order to achieve
this, the Core class hierarchy needs to be refined to behave in such a way that
is more easily compatible with the ORM, and the ORM class hierarchy needs to be
refined so that it is more compatible with Core.


:ticket:`4617`


New Features - ORM
==================

.. _change_4826:

Raiseload for Columns
---------------------

The "raiseload" feature, which raises :class:`.InvalidRequestError` when an
unloaded attribute is accessed, is now available for column-oriented attributes
using the :paramref:`.orm.defer.raiseload` parameter of :func:`.defer`. This
works in the same manner as that of the :func:`.raiseload` option used by
relationship loading::

    book = session.query(Book).options(defer(Book.summary, raiseload=True)).first()

    # would raise an exception
    book.summary

To configure column-level raiseload on a mapping, the
:paramref:`.deferred.raiseload` parameter of :func:`.deferred` may be used.  The
:func:`.undefer` option may then be used at query time to eagerly load
the attribute::

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = deferred(Column(String(2000)), raiseload=True)
        excerpt = deferred(Column(Text), raiseload=True)

    book_w_excerpt = session.query(Book).options(undefer(Book.excerpt)).first()

It was originally considered that the existing :func:`.raiseload` option that
works for :func:`.relationship` attributes be expanded to also support column-oriented
attributes.    However, this would break the "wildcard" behavior of :func:`.raiseload`,
which is documented as allowing one to prevent all relationships from loading::

    session.query(Order).options(
        joinedload(Order.items), raiseload('*'))

Above, if we had expanded :func:`.raiseload` to accommodate for columns  as
well, the wildcard would also prevent columns from loading and thus be  a
backwards incompatible change; additionally, it's not clear if
:func:`.raiseload` covered both column expressions and relationships, how one
would achieve the  effect above of only blocking relationship loads, without
new API being added.   So to keep things simple, the option for columns
remains on :func:`.defer`:

    :func:`.raiseload` - query option to raise for relationship loads

    :paramref:`.orm.defer.raiseload` - query option to raise for column expression loads


As part of this change, the behavior of "deferred" in conjunction with
attribute expiration has changed.   Previously, when an object would be marked
as expired, and then unexpired via the access of one of the expired attributes,
attributes which were mapped as "deferred" at the mapper level would also load.
This has been changed such that an attribute that is deferred in the mapping
will never "unexpire", it only loads when accessed as part of the deferral
loader.

An attribute that is not mapped as "deferred", however was deferred at query
time via the :func:`.defer` option, will be reset when the object or attribute
is expired; that is, the deferred option is removed. This is the same behavior
as was present previously.


.. seealso::

    :ref:`deferred_raiseload`

:ticket:`4826`

Behavioral Changes - ORM
========================

.. _change_1763:

Eager loaders emit during unexpire operations
---------------------------------------------

A long sought behavior was that when an expired object is accessed, configured
eager loaders will run in order to eagerly load relationships on the expired
object when the object is refreshed or otherwise unexpired.   This behavior has
now been added, so that joinedloaders will add inline JOINs as usual, and
selectin/subquery loaders will run an "immediateload" operation for a given
relationship, when an expired object is unexpired or an object is refreshed::

    >>> a1 = session.query(A).options(joinedload(A.bs)).first()
    >>> a1.data = 'new data'
    >>> session.commit()

Above, the ``A`` object was loaded with a ``joinedload()`` option associated
with it in order to eagerly load the ``bs`` collection.    After the
``session.commit()``, the state of the object is expired.  Upon accessing
the ``.data`` column attribute, the object is refreshed and this will now
include the joinedload operation as well::

    >>> a1.data
    SELECT a.id AS a_id, a.data AS a_data, b_1.id AS b_1_id, b_1.a_id AS b_1_a_id
    FROM a LEFT OUTER JOIN b AS b_1 ON a.id = b_1.a_id
    WHERE a.id = ?

The behavior applies both to loader strategies applied to the
:func:`.relationship` directly, as well as with options used with
:meth:`.Query.options`, provided that the object was originally loaded by that
query.

For the "secondary" eager loaders "selectinload" and "subqueryload", the SQL
strategy for these loaders is not necessary in order to eagerly load attributes
on a single object; so they will instead invoke the "immediateload" strategy in
a refresh scenario, which resembles the query emitted by "lazyload", emitted as
an additional query::

    >>> a1 = session.query(A).options(selectinload(A.bs)).first()
    >>> a1.data = 'new data'
    >>> session.commit()
    >>> a1.data
    SELECT a.id AS a_id, a.data AS a_data
    FROM a
    WHERE a.id = ?
    (1,)
    SELECT b.id AS b_id, b.a_id AS b_a_id
    FROM b
    WHERE ? = b.a_id
    (1,)

Note that a loader option does not apply to an object that was introduced
into the :class:`.Session` in a different way.  That is, if the ``a1`` object
were just persisted in this :class:`.Session`, or was loaded with a different
query before the eager option had been applied, then the object doesn't have
an eager load option associated with it.  This is not a new concept, however
users who are looking for the eagerload on refresh behavior may find this
to be more noticeable.

:ticket:`1763`

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

.. _change_4994:

Persistence-related cascade operations disallowed with viewonly=True
---------------------------------------------------------------------

When a :func:`.relationship` is set as ``viewonly=True`` using the
:paramref:`.relationship.viewonly` flag, it indicates this relationship should
only be used to load data from the database, and should not be mutated
or involved in a persistence operation.   In order to ensure this contract
works successfully, the relationship can no longer specify
:paramref:`.relationship.cascade` settings that make no sense in terms of
"viewonly".

The primary targets here are the "delete, delete-orphan"  cascades, which
through 1.3 continued to impact persistence even if viewonly were True, which
is a bug; even if viewonly were True, an object would still cascade these
two operations onto the related object if the parent were deleted or the
object were detached.   Rather than modify the cascade operations to check
for viewonly, the configuration of both of these together is simply
disallowed::

    class User(Base):
        # ...

        # this is now an error
        addresses = relationship(
            "Address", viewonly=True, cascade="all, delete-orphan")

The above will raise::

    sqlalchemy.exc.ArgumentError: Cascade settings
    "delete, delete-orphan, merge, save-update" apply to persistence
    operations and should not be combined with a viewonly=True relationship.

Applications that have this issue should be emitting a warning as of
SQLAlchemy 1.3.12, and for the above error the solution is to remove
the cascade settings for a viewonly relationship.


:ticket:`4993`
:ticket:`4994`



Behavior Changes - Core
========================

.. _change_4753:

SELECT objects and derived FROM clauses allow for duplicate columns and column labels
-------------------------------------------------------------------------------------

This change allows that the :func:`.select` construct now allows for duplicate
column labels as well as duplicate column objects themselves, so that result
tuples are organized and ordered in the identical way in that the columns were
selected.  The ORM :class:`.Query` already works this way, so this change
allows for greater cross-compatibility between the two, which is a key goal of
the 2.0 transition::

    >>> from sqlalchemy import column, select
    >>> c1, c2, c3, c4 = column('c1'), column('c2'), column('c3'), column('c4')
    >>> stmt = select([c1, c2, c3.label('c2'), c2, c4])
    >>> print(stmt)
    SELECT c1, c2, c3 AS c2, c2, c4

To support this change, the :class:`.ColumnCollection` used by
:class:`.SelectBase` as well as for derived FROM clauses such as subqueries
also support duplicate columns; this includes the new
:attr:`.SelectBase.selected_columns` attribute, the deprecated ``SelectBase.c``
attribute, as well as the :attr:`.FromClause.c` attribute seen on constructs
such as :class:`.Subquery` and :class:`.Alias`::

    >>> list(stmt.selected_columns)
    [
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcca20; c1>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcc9e8; c2>,
        <sqlalchemy.sql.elements.Label object at 0x7fa540b3e2e8>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcc9e8; c2>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540897048; c4>
    ]

    >>> print(stmt.subquery().select())
    SELECT anon_1.c1, anon_1.c2, anon_1.c2, anon_1.c2, anon_1.c4
    FROM (SELECT c1, c2, c3 AS c2, c2, c4) AS anon_1

:class:`.ColumnCollection` also allows access by integer index to support
when the string "key" is ambiguous::

    >>> stmt.selected_columns[2]
    <sqlalchemy.sql.elements.Label object at 0x7fa540b3e2e8>

To suit the use of :class:`.ColumnCollection` in objects such as
:class:`.Table` and :class:`.PrimaryKeyConstraint`, the old "deduplicating"
behavior which is more critical for these objects is preserved in a new class
:class:`.DedupeColumnCollection`.

The change includes that the familiar warning ``"Column %r on table %r being
replaced by %r, which has the same key.  Consider use_labels for select()
statements."`` is **removed**; the :meth:`.Select.apply_labels` is still
available and is still used by the ORM for all SELECT operations, however it
does not imply deduplication of column objects, although it does imply
deduplication of implicitly generated labels::

    >>> from sqlalchemy import table
    >>> user = table('user', column('id'), column('name'))
    >>> stmt = select([user.c.id, user.c.name, user.c.id]).apply_labels()
    >>> print(stmt)
    SELECT "user".id AS user_id, "user".name AS user_name, "user".id AS id_1
    FROM "user"

Finally, the change makes it easier to create UNION and other
:class:`.CompoundSelect` objects, by ensuring that the number and position
of columns in a SELECT statement mirrors what was given, in a use case such
as::

    >>> s1 = select([user, user.c.id])
    >>> s2 = select([c1, c2, c3])
    >>> from sqlalchemy import union
    >>> u = union(s1, s2)
    >>> print(u)
    SELECT "user".id, "user".name, "user".id
    FROM "user" UNION SELECT c1, c2, c3



:ticket:`4753`

.. _change_4710_row:

The "RowProxy" is no longer a "proxy", now called ``Row``
---------------------------------------------------------

Since the beginning of SQLAlchemy, the Core result objects exposed to the
user are the :class:`.ResultProxy` and ``RowProxy`` objects.   The name
"proxy" refers to the `GOF Proxy Pattern <https://en.wikipedia.org/wiki/Proxy_pattern>`_,
emphasizing that these objects are presenting a facade around the DBAPI
``cursor`` object and the tuple-like objects returned by methods such
as ``cursor.fetchone()``; as methods on the result and row proxy objects
are invoked, the underlying methods or data members of the ``cursor`` and
the tuple-like objects returned are invoked.

In particular, SQLAlchemy's row-processing functions would be invoked
as a particular column in a row is accessed.  By row-processing functions,
we refer to functions such as that of the :class:`.Unicode` datatype, which under
Python 2 would often convert Python string objects to Python unicode
objects, as well as numeric functions that produce ``Decimal`` objects,
SQLite datetime functions that produce ``datetime`` objects from string
representations, as well as any-number of user-defined functions which can
be created using :class:`.TypeDecorator`.

The rationale for this pattern was performance, where the anticipated use
case of fetching a row from a legacy database that contained dozens of
columns would not need to run, for example, a unicode converter on every
element of each row, if only a few columns in the row were being fetched.
SQLAlchemy eventually gained C extensions which allowed for additional
performance gains within this process.

As part of SQLAlchemy 1.4's goal of migrating towards SQLAlchemy 2.0's updated
usage patterns, row objects will be made to behave more like tuples.  To
suit this, the "proxy" behavior of :class:`.Row` has been removed and instead
the row is populated with its final data values upon construction.  This
in particular allows an operation such as ``obj in row`` to work as that
of a tuple where it tests for containment of ``obj`` in the row itself,
rather than considering it to be a key in a mapping as is the case now.
For the moment, ``obj in row`` still does a key lookup,
that is, detects if the row has a particular column name as ``obj``, however
this behavior is deprecated and in 2.0 the :class:`.Row` will behave fully
as a tuple-like object; lookup of keys will be via the ``._mapping``
attribute.

The result of removing the proxy behavior from rows is that the C code has been
simplified and the performance of many operations is improved both with and
without the C extensions in use.   Modern Python DBAPIs handle unicode
conversion natively in most cases, and SQLAlchemy's unicode handlers are
very fast in any case, so the expense of unicode conversion
is a non-issue.

This change by itself has no behavioral impact on the row, but is part of
a larger series of changes in :ticket:`4710` which unifies the Core row/result
facade with that of the ORM.

:ticket:`4710`


.. _change_4449:

Improved column labeling for simple column expressions using CAST or similar
----------------------------------------------------------------------------

A user pointed out that the PostgreSQL database has a convenient behavior when
using functions like CAST against a named column, in that the result column name
is named the same as the inner expression::

    test=> SELECT CAST(data AS VARCHAR) FROM foo;

    data
    ------
     5
    (1 row)

This allows one to apply CAST to table columns while not losing the column
name (above using the name ``"data"``) in the result row.    Compare to
databases such as MySQL/MariaDB, as well as most others, where the column
name is taken from the full SQL expression and is not very portable::

    MariaDB [test]> SELECT CAST(data AS CHAR) FROM foo;
    +--------------------+
    | CAST(data AS CHAR) |
    +--------------------+
    | 5                  |
    +--------------------+
    1 row in set (0.003 sec)


In SQLAlchemy Core expressions, we never deal with a raw generated name like
the above, as SQLAlchemy applies auto-labeling to expressions like these, which
are up until now always a so-called "anonymous" expression::

    >>> print(select([cast(foo.c.data, String)]))
    SELECT CAST(foo.data AS VARCHAR) AS anon_1     # old behavior
    FROM foo

These anonymous expressions were necessary as SQLAlchemy's
:class:`.ResultProxy` made heavy use of result column names in order to match
up datatypes, such as the :class:`.String` datatype which used to have
result-row-processing behavior, to the correct column, so most importantly the
names had to be both easy to determine in a database-agnostic manner as well as
unique in all cases.    In SQLAlchemy 1.0 as part of :ticket:`918`, this
reliance on named columns in result rows (specifically the
``cursor.description`` element of the PEP-249 cursor) was scaled back to not be
necessary for most Core SELECT constructs; in release 1.4, the system overall
is becoming more comfortable with SELECT statements that have duplicate column
or label names such as in :ref:`change_4753`.  So we now emulate PostgreSQL's
reasonable behavior for simple modifications to a single column, most
prominently with CAST::

    >>> print(select([cast(foo.c.data, String)]))
    SELECT CAST(foo.data AS VARCHAR) AS data
    FROM foo

For CAST against expressions that don't have a name, the previous logic is used
to generate the usual "anonymous" labels::

    >>> print(select([cast('hi there,' + foo.c.data, String)]))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS anon_1
    FROM foo

A :func:`.cast` against a :class:`.Label`, despite having to omit the label
expression as these don't render inside of a CAST, will nonetheless make use of
the given name::

    >>> print(select([cast(('hi there,' + foo.c.data).label('hello_data'), String)]))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS hello_data
    FROM foo

And of course as was always the case, :class:`.Label` can be applied to the
expression on the outside to apply an "AS <name>" label directly::

    >>> print(select([cast(('hi there,' + foo.c.data), String).label('hello_data')]))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS hello_data
    FROM foo


:ticket:`4449`

.. _change_4808:

New "post compile" bound parameters used for LIMIT/OFFSET in Oracle, SQL Server
-------------------------------------------------------------------------------

A major goal of the 1.4 series is to establish that all Core SQL constructs
are completely cacheable, meaning that a particular :class:`.Compiled`
structure will produce an identical SQL string regardless of any SQL parameters
used with it, which notably includes those used to specify the LIMIT and
OFFSET values, typically used for pagination and "top N" style results.

While SQLAlchemy has used bound parameters for LIMIT/OFFSET schemes for many
years, a few outliers remained where such parameters were not allowed, including
a SQL Server "TOP N" statement, such as::

    SELECT TOP 5 mytable.id, mytable.data FROM mytable

as well as with Oracle, where the FIRST_ROWS() hint (which SQLAlchemy will
use if the ``optimize_limits=True`` parameter is passed to
:func:`.create_engine` with an Oracle URL) does not allow them,
but also that using bound parameters with ROWNUM comparisons has been reported
as producing slower query plans::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS(5) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id, mytable.data FROM mytable
        ) anon_2
        WHERE ROWNUM <= :param_1
    ) anon_1 WHERE ora_rn > :param_2

In order to allow for all statements to be unconditionally cacheable at the
compilation level, a new form of bound parameter called a "post compile"
parameter has been added, which makes use of the same mechanism as that
of "expanding IN parameters".  This is a :func:`.bindparam` that behaves
identically to any other bound parameter except that parameter value will
be rendered literally into the SQL string before sending it to the DBAPI
``cursor.execute()`` method.   The new parameter is used internally by the
SQL Server and Oracle dialects, so that the drivers receive the literal
rendered value but the rest of SQLAlchemy can still consider this as a
bound parameter.   The above two statements when stringified using
``str(statement.compile(dialect=<dialect>))`` now look like::

    SELECT TOP [POSTCOMPILE_param_1] mytable.id, mytable.data FROM mytable

and::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS([POSTCOMPILE__ora_frow_1]) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id, mytable.data FROM mytable
        ) anon_2
        WHERE ROWNUM <= [POSTCOMPILE_param_1]
    ) anon_1 WHERE ora_rn > [POSTCOMPILE_param_2]

The ``[POSTCOMPILE_<param>]`` format is also what is seen when an
"expanding IN" is used.

When viewing the SQL logging output, the final form of the statement will
be seen::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS(5) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id AS id, mytable.data AS data FROM mytable
        ) anon_2
        WHERE ROWNUM <= 8
    ) anon_1 WHERE ora_rn > 3


The "post compile parameter" feature is exposed as public API through the
:paramref:`.bindparam.literal_execute` parameter, however is currently not
intended for general use.   The literal values are rendered using the
:meth:`.TypeEngine.literal_processor` of the underlying datatype, which in
SQLAlchemy has **extremely limited** scope, supporting only integers and simple
string values.

:ticket:`4808`

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


Dialect Changes
===============

.. _change_4895:

Removed "join rewriting" logic from SQLite dialect; updated imports
-------------------------------------------------------------------

Dropped support for right-nested join rewriting to support old SQLite
versions prior to 3.7.16, released in 2013.   It is not expected that
any modern Python versions rely upon this limitation.

The behavior was first introduced in 0.9 and was part of the larger change of
allowing for right nested joins as described at :ref:`feature_joins_09`.
However the SQLite workaround produced many regressions in the 2013-2014
period due to its complexity. In 2016, the dialect was modified so that the
join rewriting logic would only occur for SQLite verisons prior to 3.7.16 after
bisection was used to  identify where SQLite fixed its support for this
construct, and no further issues were reported against the behavior (even
though some bugs were found internally).    It is now anticipated that there
are little to no Python builds for Python 2.7 or 3.4 and above (the supported
Python versions) which would include a SQLite version prior to 3.7.17, and
the behavior is only necessary only in more complex ORM joining scenarios.
A warning is now emitted if the installed SQLite version is older than
3.7.16.

In related changes, the module imports for SQLite no longer attempt to
import the "pysqlite2" driver on Python 3 as this driver does not exist
on Python 3; a very old warning for old pysqlite2 versions is also dropped.

:ticket:`4895`


.. _change_4976:

Added Sequence support for MariaDB 10.3
----------------------------------------

The MariaDB database as of 10.3 supports sequences.   SQLAlchemy's MySQL
dialect now implements support for the :class:`.Sequence` object against this
database, meaning "CREATE SEQUENCE" DDL will be emitted for a
:class:`.Sequence` that is present in a :class:`.Table` or :class:`.MetaData`
collection in the same way as it works for backends such as PostgreSQL, Oracle,
when the dialect's server version check has confirmed the database is MariaDB
10.3 or greater.    Additionally, the :class:`.Sequence` will act as a
column default and primary key generation object when used in these ways.

Since this change will impact the assumptions both for DDL as well as the
behavior of INSERT statements for an application that is currently deployed
against MariaDB 10.3 which also happens to make explicit use the
:class:`.Sequence` construct within its table definitions, it is important to
note that :class:`.Sequence` supports a flag :paramref:`.Sequence.optional`
which is used to limit the scenarios in which the :class:`.Sequence` to take
effect. When "optional" is used on a :class:`.Sequence` that is present in the
integer primary key column of a table::

    Table(
        "some_table", metadata,
        Column("id", Integer, Sequence("some_seq", optional=True), primary_key=True)
    )

The above :class:`.Sequence` is only used for DDL and INSERT statements if the
target database does not support any other means of generating integer primary
key values for the column.  That is, the Oracle database above would use the
sequence, however the PostgreSQL and MariaDB 10.3 databases would not. This may
be important for an existing application that is upgrading to SQLAlchemy 1.4
which may not have emitted DDL for this :class:`.Sequence` against its backing
database, as an INSERT statement will fail if it seeks to use a sequence that
was not created.


.. seealso::

    :ref:`defaults_sequences`

:ticket:`4976`
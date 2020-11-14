.. highlight:: pycon+sql

.. |prev| replace:: :doc:`orm_data_manipulation`
.. |next| replace:: :doc:`further_reading`

.. include:: tutorial_nav_include.rst

.. _tutorial_orm_related_objects:

Working with Related Objects
============================

In this section, we will cover one more essential ORM concept, which is
how the ORM interacts with mapped classes that refer to other objects. In the
section :ref:`tutorial_declaring_mapped_classes`, the mapped class examples
made use of a construct called :func:`_orm.relationship`.  This construct
defines a linkage between two different mapped classes, or from a mapped class
to itself, the latter of which is called a **self-referential** relationship.

To describe the basic idea of :func:`_orm.relationship`, first we'll review
the mapping in short form, omitting the :class:`_schema.Column` mappings
and other directives:

.. sourcecode:: python

  from sqlalchemy.orm import relationship
  class User(Base):
      __tablename__ = 'user_account'

      # ... Column mappings

      addresses = relationship("Address", back_populates="user")


  class Address(Base):
      __tablename__ = 'address'

      # ... Column mappings

      user = relationship("User", back_populates="addresses")


Above, the ``User`` class now has an attribute ``User.addresses`` and the
``Address`` class has an attribute ``Address.user``.   The
:func:`_orm.relationship` construct will be used to inspect the table
relationships between the :class:`_schema.Table` objects that are mapped to the
``User`` and ``Address`` classes. As the :class:`_schema.Table` object
representing the
``address`` table has a :class:`_schema.ForeignKeyConstraint` which refers to
the ``user_account`` table, the :func:`_orm.relationship` can determine
unambiguously that there is as :term:`one to many` relationship from
``User.addresses`` to ``User``; one particular row in the ``user_account``
table may be referred towards by many rows in the ``address`` table.

All one-to-many relationships naturally correspond to a :term:`many to one`
relationship in the other direction, in this case the one noted by
``Address.user``. The :paramref:`_orm.relationship.back_populates` parameter,
seen above configured on both :func:`_orm.relationship` objects referring to
the other name, establishes that each of these two :func:`_orm.relationship`
constructs should be considered to be complimentary to each other; we will see
how this plays out in the next section.


Persisting and Loading Relationships
-------------------------------------

We can start by illustrating what :func:`_orm.relationship` does to instances
of objects.   If we make a new ``User`` object, we can note that there is a
Python list when we access the ``.addresses`` element::

    >>> u1 = User(name='pkrabs', fullname='Pearl Krabs')
    >>> u1.addresses
    []

This object is a SQLAlchemy-specific version of Python ``list`` which
has the ability to track and respond to changes made to it.  The collection
also appeared automatically when we accessed the attribute, even though we never assigned it to the object.
This is similar to the behavior noted at :ref:`tutorial_inserting_orm` where
it was observed that column-based attributes to which we don't explicitly
assign a value also display as ``None`` automatically, rather than raising
an ``AttributeError`` as would be Python's usual behavior.

As the ``u1`` object is still :term:`transient` and the ``list`` that we got
from ``u1.addresses`` has not been mutated (i.e. appended or extended), it's
not actually associated with the object yet, but as we make changes to it,
it will become part of the state of the ``User`` object.

The collection is specific to the ``Address`` class which is the only type
of Python object that may be persisted within it.  Using the ``list.append()``
method we may add an ``Address`` object::

  >>> a1 = Address(email_address="pearl.krabs@gmail.com")
  >>> u1.addresses.append(a1)

At this point, the ``u1.addresses`` collection as expected contains the
new ``Address`` object::

  >>> u1.addresses
  [Address(id=None, email_address='pearl.krabs@gmail.com')]

As we associated the ``Address`` object with the ``User.addresses`` collection
of the ``u1`` instance, another behavior also occurred, which is that the
``User.addresses`` relationship synchronized itself with the ``Address.user``
relationship, such that we can navigate not only from the ``User`` object
to the ``Address`` object, we can also navigate from the ``Address`` object
back to the "parent" ``User`` object::

  >>> a1.user
  User(id=None, name='pkrabs', fullname='Pearl Krabs')

This synchronization occurred as a result of our use of the
:paramref:`_orm.relationship.back_populates` parameter between the two
:func:`_orm.relationship` objects.  This parameter names another
:func:`_orm.relationship` for which complementary attribute assignment / list
mutation should occur.   It will work equally well in the other
direction, which is that if we create another ``Address`` object and assign
to its ``Address.user`` attribute, that ``Address`` becomes part of the
``User.addresses`` collection on that ``User`` object::

  >>> a2 = Address(email_address="pearl@aol.com", user=u1)
  >>> u1.addresses
  [Address(id=None, email_address='pearl.krabs@gmail.com'), Address(id=None, email_address='pearl@aol.com')]

We actually made use of the ``user`` parameter as a keyword argument in the
``Address`` consructor, which is accepted just like any other mapped attribute
that was declared on the ``Address`` class.  It is equivalent to assignment
of the ``Address.user`` attribute after the fact::

  # equivalent effect as a2 = Address(user=u1)
  >>> a2.user = u1

Cascading Objects into the Session
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We now have a ``User`` and two ``Address`` objects that are associated in a
bidirectional structure
in memory, but as noted previously in :ref:`tutorial_inserting_orm` ,
these objects are said to be in the :term:`transient` state until they
are associated with a :class:`_orm.Session` object.

We make use of the :class:`_orm.Session` that's still ongoing, and note that
when we apply the :meth:`_orm.Session.add` method to the lead ``User`` object,
the related ``Address`` object also gets added to that same :class:`_orm.Session`::

  >>> session.add(u1)
  >>> u1 in session
  True
  >>> a1 in session
  True
  >>> a2 in session
  True

The above behavior, where the :class:`_orm.Session` received a ``User`` object,
and followed along the ``User.addresses`` relationship to locate a related
``Address`` object, is known as the **save-update cascade** and is discussed
in detail in the ORM reference documentation at :ref:`unitofwork_cascades`.

The three objects are now in the :term:`pending` state; this means they are
ready to be the subject of an INSERT operation but this has not yet proceeded;
all three objects have no primary key assigned yet, and in addition, the ``a1``
and ``a2`` objects have an attribute called ``user_id`` which refers to the
:class:`_schema.Column` that has a :class:`_schema.ForeignKeyConsraint`
referring to the ``user_account.id`` column; these are also ``None`` as the
objects are not yet associated with a real database row::

    >>> print(u1.id)
    None
    >>> print(a1.user_id)
    None

It's at this stage that we can see the very great utility that the unit of
work process provides; recall in the section :ref:`tutorial_core_insert_values_clause`,
rows were inserted into the ``user_account`` and
``address`` tables using some elaborate syntaxes in order to automatically
associate the ``address.user_id`` columns with those of the ``user_account``
rows.  Additionally, it was necessary that we emit INSERT for ``user_account``
rows first, before those of ``address``, since rows in ``address`` are
**dependent** on their parent row in ``user_account`` for a value in their
``user_id`` column.

When using the :class:`_orm.Session`, all this tedium is handled for us and
even the most die-hard SQL purist can benefit from automation of INSERT,
UPDATE and DELETE statements.   When we :meth:`_orm.Session.commit` the
transaction all steps invoke in the correct order, and furthermore the
newly generated primary key of the ``user_account`` row is applied to the
``address.user_id`` column appropriately:

.. sourcecode:: pycon+sql

  >>> session.commit()
  {opensql}INSERT INTO user_account (name, fullname) VALUES (?, ?)
  [...] ('pkrabs', 'Pearl Krabs')
  INSERT INTO address (email_address, user_id) VALUES (?, ?)
  [...] ('pearl.krabs@gmail.com', 6)
  INSERT INTO address (email_address, user_id) VALUES (?, ?)
  [...] ('pearl@aol.com', 6)
  COMMIT

.. _tutorial_loading_relationships:

Loading Relationships
---------------------

In the last step, we called :meth:`_orm.Session.commit` which emitted a COMMIT
for the transaction, and then per
:paramref:`_orm.Session.commit.expire_on_commit` expired all objects so that
they refresh for the next transaction.

When we next access an attribute on these objects, we'll see the SELECT
emitted for the primary attributes of the row, such as when we view the
newly generated primary key for the ``u1`` object:

.. sourcecode:: pycon+sql

  >>> u1.id
  {opensql}BEGIN (implicit)
  SELECT user_account.id AS user_account_id, user_account.name AS user_account_name,
  user_account.fullname AS user_account_fullname
  FROM user_account
  WHERE user_account.id = ?
  [...] (6,){stop}
  6

The ``u1`` ``User`` object now has a persistent collection ``User.addresses``
that we may also access.   As this collection consists of an additional set
of rows from the ``address`` table, when we access this collection as well
we again see a :term:`lazy load` emitted in order to retrieve the objects:

.. sourcecode:: pycon+sql

  >>> u1.addresses
  {opensql}SELECT address.id AS address_id, address.email_address AS address_email_address,
  address.user_id AS address_user_id
  FROM address
  WHERE ? = address.user_id
  [...] (6,){stop}
  [Address(id=4, email_address='pearl.krabs@gmail.com'), Address(id=5, email_address='pearl@aol.com')]

Collections and related attributes in the SQLAlchemy ORM are persistent in
memory; once the collection or attribute is populated, SQL is no longer emitted
until that collection or attribute is :term:`expired`.    We may access
``u1.addresses`` again as well as add or remove items and this will not
incur any new SQL calls::

  >>> u1.addresses
  [Address(id=4, email_address='pearl.krabs@gmail.com'), Address(id=5, email_address='pearl@aol.com')]

While the loading emitted by lazy loading can quickly become expensive if
we don't take explicit steps to optimize it, the network of lazy loading
at least is fairly well optimized to not perform redundant work; as the
``u1.addresses`` collection was refreshed, per the :term:`identity map`
these are in fact the same
``Address`` instances as the ``a1`` and ``a2`` objects we've been dealing with
already, so we're done loading all attributes in this particular object
graph::

  >>> a1
  Address(id=4, email_address='pearl.krabs@gmail.com')
  >>> a2
  Address(id=5, email_address='pearl@aol.com')

The issue of how relationships load, or not, is an entire subject onto
itself.  Some additional introduction to these concepts is later in this
section at :ref:`tutorial_orm_loader_strategies`.

.. _tutorial_select_relationships:

Using Relationships in Queries
------------------------------

The previous section introduced the behavior of the :func:`_orm.relationship`
construct when working with **instances of a mapped class**, above, the
``u1``, ``a1`` and ``a2`` instances of the ``User`` and ``Address`` classes.
In this section, we introduce the behavior of :func:`_orm.relationship` as it
applies to **class level behavior of a mapped class**, where it serves in
several ways to help automate the construction of SQL queries.

.. _tutorial_joining_relationships:

Using Relationships to Join
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The sections :ref:`tutorial_select_join` and
:ref:`tutorial_select_join_onclause` introduced the usage of the
:meth:`_sql.Select.join` and :meth:`_sql.Select.join_from` methods to compose
SQL JOIN clauses.   In order to describe how to join between tables, these
methods either **infer** the ON clause based on the presence of a single
unambiguous :class:`_schema.ForeignKeyConstraint` object within the table
metadata structure that links the two tables, or otherwise we may provide an
explicit SQL Expression construct that indicates a specific ON clause.

When using ORM entities, an additional mechanism is available to help us set up
the ON clause of a join, which is to make use of the :func:`_orm.relationship`
objects that we set up in our user mapping, as was demonstrated at
:ref:`tutorial_declaring_mapped_classes`. The class-bound attribute
corresponding to the :func:`_orm.relationship` may be passed as the **single
argument** to :meth:`_sql.Select.join`, where it serves to indicate both the
right side of the join as well as the ON clause at once::

    >>> print(
    ...     select(Address.email_address).
    ...     select_from(User).
    ...     join(User.addresses)
    ... )
    {opensql}SELECT address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id

The presence of an ORM :func:`_orm.relationship` on a mapping is not used
by :meth:`_sql.Select.join` or :meth:`_sql.Select.join_from` if we don't
specify it; it is **not used for ON clause
inference**.  This means, if we join from ``User`` to ``Address`` without an
ON clause, it works because of the :class:`_schema.ForeignKeyConstraint`
between the two mapped :class:`_schema.Table` objects, not because of the
:func:`_orm.relationship` objects on the ``User`` and ``Address`` classes::

    >>> print(
    ...    select(Address.email_address).
    ...    join_from(User, Address)
    ... )
    {opensql}SELECT address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id

.. _tutorial_joining_relationships_aliased:

Joining between Aliased targets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the section :ref:`tutorial_orm_entity_aliases` we introduced the
:func:`_orm.aliased` construct, which is used to apply a SQL alias to an
ORM entity.   When using a :func:`_orm.relationship` to help construct SQL JOIN, the
use case where the target of the join is to be an :func:`_orm.aliased` is suited
by making use of the :meth:`_orm.PropComparator.of_type` modifier.    To
demonstrate we will construct the same join illustrated at :ref:`tutorial_orm_entity_aliases`
using the :func:`_orm.relationship` attributes to join instead::

    >>> print(
    ...        select(User).
    ...        join(User.addresses.of_type(address_alias_1)).
    ...        where(address_alias_1.email_address == 'patrick@aol.com').
    ...        join(User.addresses.of_type(address_alias_2)).
    ...        where(address_alias_2.email_address == 'patrick@gmail.com')
    ...    )
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2

To make use of a :func:`_orm.relationship` to construct a join **from** an
aliased entity, the attribute is available from the :func:`_orm.aliased`
construct directly::

    >>> user_alias_1 = aliased(User)
    >>> print(
    ...     select(user_alias_1.name).
    ...     join(user_alias_1.addresses)
    ... )
    {opensql}SELECT user_account_1.name
    FROM user_account AS user_account_1
    JOIN address ON user_account_1.id = address.user_id

.. _tutorial_joining_relationships_augmented:

Augmenting the ON Criteria
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ON clause generated by the :func:`_orm.relationship` construct may
also be augmented with additional criteria.  This is useful both for
quick ways to limit the scope of a particular join over a relationship path,
and also for use cases like configuring loader strategies, introduced below
at :ref:`tutorial_orm_loader_strategies`.  The :meth:`_orm.PropComparator.and_`
method accepts a series of SQL expressions positionally that will be joined
to the ON clause of the JOIN via AND.  For example if we wanted to
JOIN from ``User`` to ``Address`` but also limit the ON criteria to only certain
email addresses:

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...   select(User.fullname).
    ...   join(User.addresses.and_(Address.email_address == 'pearl.krabs@gmail.com'))
    ... )
    >>> session.execute(stmt).all()
    {opensql}SELECT user_account.fullname
    FROM user_account
    JOIN address ON user_account.id = address.user_id AND address.email_address = ?
    [...] ('pearl.krabs@gmail.com',){stop}
    [('Pearl Krabs',)]


.. _tutorial_relationship_exists:

EXISTS forms: has() / any()
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the section :ref:`tutorial_exists`, we introduced the :class:`_sql.Exists`
object that provides for the SQL EXISTS keyword in conjunction with a
scalar subquery.   The :func:`_orm.relationship` construct provides for some
helper methods that may be used to generate some common EXISTS styles
of queries in terms of the relationship.

For a one-to-many relationship such as ``User.addresses``, an EXISTS against
the ``address`` table that correlates back to the ``user_account`` table
can be produced using :meth:`_orm.PropComparator.any`.  This method accepts
an optional WHERE criteria to limit the rows matched by the subquery:

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...   select(User.fullname).
    ...   where(User.addresses.any(Address.email_address == 'pearl.krabs@gmail.com'))
    ... )
    >>> session.execute(stmt).all()
    {opensql}SELECT user_account.fullname
    FROM user_account
    WHERE EXISTS (SELECT 1
    FROM address
    WHERE user_account.id = address.user_id AND address.email_address = ?)
    [...] ('pearl.krabs@gmail.com',){stop}
    [('Pearl Krabs',)]

As EXISTS tends to be more efficient for negative lookups, a common query
is to locate entities where there are no related entities present.  This
is succinct using a phrase such as ``~User.addresses.any()``, to select
for ``User`` entities that have no related ``Address`` rows:

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...   select(User.fullname).
    ...   where(~User.addresses.any())
    ... )
    >>> session.execute(stmt).all()
    {opensql}SELECT user_account.fullname
    FROM user_account
    WHERE NOT (EXISTS (SELECT 1
    FROM address
    WHERE user_account.id = address.user_id))
    [...] (){stop}
    [('Patrick McStar',), ('Squidward Tentacles',), ('Eugene H. Krabs',)]

The :meth:`_orm.PropComparator.has` method works in mostly the same way as
:meth:`_orm.PropComparator.any`, except that it's used for many-to-one
relationships, such as if we wanted to locate all ``Address`` objects
which belonged to "pearl":

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...   select(Address.email_address).
    ...   where(Address.user.has(User.name=="pkrabs"))
    ... )
    >>> session.execute(stmt).all()
    {opensql}SELECT address.email_address
    FROM address
    WHERE EXISTS (SELECT 1
    FROM user_account
    WHERE user_account.id = address.user_id AND user_account.name = ?)
    [...] ('pkrabs',){stop}
    [('pearl.krabs@gmail.com',), ('pearl@aol.com',)]

.. _tutorial_relationship_operators:

Common Relationship Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are some additional varieties of SQL generation helpers that come with
:func:`_orm.relationship`, including:

* **many to one equals comparison** - a specific object instance can be
  compared to many-to-one relationship, to select rows where the
  foreign key of the target entity matches the primary key value of the
  object given::

      >>> print(select(Address).where(Address.user == u1))
      {opensql}SELECT address.id, address.email_address, address.user_id
      FROM address
      WHERE :param_1 = address.user_id

  ..

* **many to one not equals comparison** - the not equals operator may also
  be used::

      >>> print(select(Address).where(Address.user != u1))
      {opensql}SELECT address.id, address.email_address, address.user_id
      FROM address
      WHERE address.user_id != :user_id_1 OR address.user_id IS NULL

  ..

* **object is contained in a one-to-many collection** - this is essentially
  the one-to-many version of the "equals" comparison, select rows where the
  primary key equals the value of the foreign key in a related object::

      >>> print(select(User).where(User.addresses.contains(a1)))
      {opensql}SELECT user_account.id, user_account.name, user_account.fullname
      FROM user_account
      WHERE user_account.id = :param_1

  ..

* **An object has a particular parent from a one-to-many perspective** - the
  :func:`_orm.with_parent` function produces a comparison that returns rows
  which are referred towards by a given parent, this is essentially the
  same as using the ``==`` operator with the many-to-one side::

      >>> from sqlalchemy.orm import with_parent
      >>> print(select(Address).where(with_parent(u1, User.addresses)))
      {opensql}SELECT address.id, address.email_address, address.user_id
      FROM address
      WHERE :param_1 = address.user_id

  ..

.. _tutorial_orm_loader_strategies:

Loader Strategies
-----------------

In the section :ref:`tutorial_loading_relationships` we introduced the concept
that when we work with instances of mapped objects, accessing the attributes
that are mapped using :func:`_orm.relationship` in the default case will emit
a :term:`lazy load` when the collection is not populated in order to load
the objects that should be present in this collection.

Lazy loading is one of the most famous ORM patterns, and is also the one that
is most controversial.   When several dozen ORM objects in memory each refer to
a handful of unloaded attributes, routine manipulation of these objects can
spin off many additional queries that can add up (otherwise known as the
:term:`N plus one problem`), and to make matters worse they are emitted
implicitly.    These implicit queries may not be noticed, may cause errors
when they are attempted after there's no longer a database tranasction
available, or when using alternative concurrency patterns such as :ref:`asyncio
<asyncio_toplevel>`, they actually won't work at all.

At the same time, lazy loading is a vastly popular and useful pattern when it
is compatible with the concurrency approach in use and isn't otherwise causing
problems.   For these reasons, SQLAlchemy's ORM places a lot of emphasis on
being able to control and optimize this loading behavior.

Above all, the first step in using ORM lazy loading effectively is to **test
the application, turn on SQL echoing, and watch the SQL that is emitted**. If
there seem to be lots of redundant SELECT statements that look very much like
they could be rolled into one much more efficiently, if there are loads
occurring inappropriately for objects that have been :term:`detached` from
their :class:`_orm.Session`, that's when to look into using **loader
strategies**.

Loader strategies are represented as objects that may be associated with a
SELECT statement using the :meth:`_sql.Select.options` method, e.g.:

.. sourcecode:: python

      for user_obj in session.execute(
          select(User).options(selectinload(User.addresses))
      ).scalars():
          user_obj.addresses  # access addresses collection already loaded

They may be also configured as defaults for a :func:`_orm.relationship` using
the :paramref:`_orm.relationship.lazy` option, e.g.:

.. sourcecode:: python

    from sqlalchemy.orm import relationship
    class User(Base):
        __tablename__ = 'user_account'

        addresses = relationship("Address", back_populates="user", lazy="selectin")

Each loader strategy object adds some kind of information to the statement that
will be used later by the :class:`_orm.Session` when it is deciding how various
attributes should be loaded and/or behave when they are accessed.

The sections below will introduce a few of the most prominently used
loader strategies.

.. seealso::

    Two sections in :ref:`loading_toplevel`:

    * :ref:`relationship_lazy_option` - details on configuring the strategy
      on :func:`_orm.relationship`

    * :ref:`relationship_loader_options` - details on using query-time
      loader strategies

Selectin Load
^^^^^^^^^^^^^

The most useful loader in modern SQLAlchemy is the
:func:`_orm.selectinload` loader option.  This option solves the most common
form of the "N plus one" problem which is that of a set of objects that refer
to related collections.   :func:`_orm.selectinload` will ensure that a particular
collection for a full series of objects are loaded up front using a single
query.   It does this using a SELECT form that in most cases can be emitted
against the related table alone, without the introduction of JOINs or
subqueries, and only queries for those parent objects for which the
collection isn't already loaded.   Below we illustrate :func:`_orm.selectinload`
by loading all of the ``User`` objects and all of their related ``Address``
objects; while we invoke :meth:`_orm.Session.execute` only once, given a
:func:`_sql.select` construct, when the database is accessed, there are
in fact two SELECT statements emitted, the second one being to fetch the
related ``Address`` objects:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.orm import selectinload
    >>> stmt = (
    ...   select(User).options(selectinload(User.addresses)).order_by(User.id)
    ... )
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User.name}  ({', '.join(a.email_address for a in row.User.addresses)})")
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.id
    [...] ()
    SELECT address.user_id AS address_user_id, address.id AS address_id,
    address.email_address AS address_email_address
    FROM address
    WHERE address.user_id IN (?, ?, ?, ?, ?, ?)
    [...] (1, 2, 3, 4, 5, 6){stop}
    spongebob  (spongebob@sqlalchemy.org)
    sandy  (sandy@sqlalchemy.org, sandy@squirrelpower.org)
    patrick  ()
    squidward  ()
    ehkrabs  ()
    pkrabs  (pearl.krabs@gmail.com, pearl@aol.com)

.. seealso::

    :ref:`selectin_eager_loading` - in :ref:`loading_toplevel`

Joined Load
^^^^^^^^^^^

The :func:`_orm.joinedload` eager load strategy is the oldest eager loader in
SQLAlchemy, which augments the SELECT statement that's being passed to the
database with a JOIN (which may an outer or an inner join depending on options),
which can then load in related objects.

The :func:`_orm.joinedload` strategy is best suited towards loading
related many-to-one objects, as this only requires that additional columns
are added to a primary entity row that would be fetched in any case.
For greater effiency, it also accepts an option :paramref:`_orm.joinedload.innerjoin`
so that an inner join instead of an outer join may be used for a case such
as below where we know that all ``Address`` objects have an associated
``User``:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.orm import joinedload
    >>> stmt = (
    ...   select(Address).options(joinedload(Address.user, innerjoin=True)).order_by(Address.id)
    ... )
    >>> for row in session.execute(stmt):
    ...     print(f"{row.Address.email_address} {row.Address.user.name}")
    {opensql}SELECT address.id, address.email_address, address.user_id, user_account_1.id AS id_1,
    user_account_1.name, user_account_1.fullname
    FROM address
    JOIN user_account AS user_account_1 ON user_account_1.id = address.user_id
    ORDER BY address.id
    [...] (){stop}
    spongebob@sqlalchemy.org spongebob
    sandy@sqlalchemy.org sandy
    sandy@squirrelpower.org sandy
    pearl.krabs@gmail.com pkrabs
    pearl@aol.com pkrabs

:func:`_orm.joinedload` also works for collections, meaning one-to-many relationships,
however it has the effect
of multiplying out primary rows per related item in a recursive way
that grows the amount of data sent for a result set by orders of magnitude for
nested collections and/or larger collections, so its use vs. another option
such as :func:`_orm.selectinload` should be evaluated on a per-case basis.

It's important to note that the WHERE and ORDER BY criteria of the enclosing
:class:`_sql.Select` statement **do not target the table rendered by
joinedload()**.   Above, it can be seen in the SQL that an **anonymous alias**
is applied to the ``user_account`` table such that is not directly addressible
in the query.   This concept is discussed in more detail in the section
:ref:`zen_of_eager_loading`.

The ON clause rendered by :func:`_orm.joinedload` may be affected directly by
using the :meth:`_orm.PropComparator.and_` method described previously at
:ref:`tutorial_joining_relationships_augmented`; examples of this technique
with loader strategies are further below at :ref:`tutorial_loader_strategy_augmented`.
However, more generally, "joined eager loading" may be applied to a
:class:`_sql.Select` that uses :meth:`_sql.Select.join` using the approach
described in the next section,
:ref:`tutorial_orm_loader_strategies_contains_eager`.


.. tip::

  It's important to note that many-to-one eager loads are often not necessary,
  as the "N plus one" problem is much less prevalent in the common case. When
  many objects all refer to the same related object, such as many ``Address``
  objects that each refer ot the same ``User``, SQL will be emitted only once
  for that ``User`` object using normal lazy loading.  The lazy load routine
  will look up the related object by primary key in the current
  :class:`_orm.Session` without emitting any SQL when possible.


.. seealso::

  :ref:`joined_eager_loading` - in :ref:`loading_toplevel`

.. _tutorial_orm_loader_strategies_contains_eager:

Explicit Join + Eager load
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If we were to load ``Address`` rows while joining to the ``user_account`` table
using a method such as :meth:`_sql.Select.join` to render the JOIN, we could
also leverage that JOIN in order to eagerly load the contents of the
``Address.user`` attribute on each ``Address`` object returned.  This is
essentially that we are using "joined eager loading" but rendering the JOIN
ourselves.   This common use case is acheived by using the
:func:`_orm.contains_eager` option. this option is very similar to
:func:`_orm.joinedload`, except that it assumes we have set up the JOIN
ourselves, and it instead only indicates that additional columns in the COLUMNS
clause should be loaded into related attributes on each returned object, for
example:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.orm import contains_eager
    >>> stmt = (
    ...   select(Address).
    ...   join(Address.user).
    ...   where(User.name == 'pkrabs').
    ...   options(contains_eager(Address.user)).order_by(Address.id)
    ... )
    >>> for row in session.execute(stmt):
    ...     print(f"{row.Address.email_address} {row.Address.user.name}")
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname,
    address.id AS id_1, address.email_address, address.user_id
    FROM address JOIN user_account ON user_account.id = address.user_id
    WHERE user_account.name = ? ORDER BY address.id
    [...] ('pkrabs',){stop}
    pearl.krabs@gmail.com pkrabs
    pearl@aol.com pkrabs

Above, we both filtered the rows on ``user_account.name`` and also loaded
rows from ``user_account`` into the ``Address.user`` attribute of the returned
rows.   If we had applied :func:`_orm.joinedload` separately, we would get a
SQL query that unnecessarily joins twice::

    >>> stmt = (
    ...   select(Address).
    ...   join(Address.user).
    ...   where(User.name == 'pkrabs').
    ...   options(joinedload(Address.user)).order_by(Address.id)
    ... )
    >>> print(stmt)  # SELECT has a JOIN and LEFT OUTER JOIN unnecessarily
    {opensql}SELECT address.id, address.email_address, address.user_id,
    user_account_1.id AS id_1, user_account_1.name, user_account_1.fullname
    FROM address JOIN user_account ON user_account.id = address.user_id
    LEFT OUTER JOIN user_account AS user_account_1 ON user_account_1.id = address.user_id
    WHERE user_account.name = :name_1 ORDER BY address.id

.. seealso::

    Two sections in :ref:`loading_toplevel`:

    * :ref:`zen_of_eager_loading` - describes the above problem in detail

    * :ref:`contains_eager` - using :func:`.contains_eager`

.. _tutorial_loader_strategy_augmented:

Augmenting Loader Strategy Paths
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In :ref:`tutorial_joining_relationships_augmented` we illustrated how to add
arbitrary criteria to a JOIN rendered with :func:`_orm.relationship` to also
include additional criteria in the ON clause.   The :meth:`_orm.PropComparator.and_`
method is in fact generally available for most loader options.   For example,
if we wanted to re-load the names of users and their email addresses, but omitting
the email addresses with the ``sqlalchemy.org`` domain, we can apply
:meth:`_orm.PropComparator.and_` to the argument passed to
:func:`_orm.selectinload` to limit this criteria:


.. sourcecode:: pycon+sql

    >>> from sqlalchemy.orm import selectinload
    >>> stmt = (
    ...   select(User).
    ...   options(
    ...       selectinload(
    ...           User.addresses.and_(
    ...             ~Address.email_address.endswith("sqlalchemy.org")
    ...           )
    ...       )
    ...   ).
    ...   order_by(User.id).
    ...   execution_options(populate_existing=True)
    ... )
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User.name}  ({', '.join(a.email_address for a in row.User.addresses)})")
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.id
    [...] ()
    SELECT address.user_id AS address_user_id, address.id AS address_id,
    address.email_address AS address_email_address
    FROM address
    WHERE address.user_id IN (?, ?, ?, ?, ?, ?)
    AND (address.email_address NOT LIKE '%' || ?)
    [...] (1, 2, 3, 4, 5, 6, 'sqlalchemy.org'){stop}
    spongebob  ()
    sandy  (sandy@squirrelpower.org)
    patrick  ()
    squidward  ()
    ehkrabs  ()
    pkrabs  (pearl.krabs@gmail.com, pearl@aol.com)


A very important thing to note above is that a special option is added with
``.execution_options(populate_existing=True)``.    This option which takes
effect when rows are being fetched indicates that the loader option we are
using should **replace** the existing contents of collections on the objects,
if they are already loaded.  As we are working with a single
:class:`_orm.Session` repeatedly, the objects we see being loaded above are the
same Python instances as those that were first persisted at the start of the
ORM section of this tutorial.


.. seealso::

    :ref:`loader_option_criteria` - in :ref:`loading_toplevel`

    :ref:`orm_queryguide_populate_existing` - in :ref:`queryguide_toplevel`


Raiseload
^^^^^^^^^

One additional loader strategy worth mentioning is :func:`_orm.raiseload`.
This option is used to completely block an application from having the
:term:`N plus one` problem at all by causing what would normally be a lazy
load to raise an error instead.   It has two variants that are controlled via
the :paramref:`_orm.raiseload.sql_only` option to block either lazy loads
that require SQL, versus all "load" operations including those which
only need to consult the current :class:`_orm.Session`.

One way to use :func:`_orm.raiseload` is to configure it on
:func:`_orm.relationship` itself, by setting :paramref:`_orm.relationship.lazy`
to the value ``"raise_on_sql"``, so that for a particular mapping, a certain
relationship will never try to emit SQL:

.. sourcecode:: python

    class User(Base):
        __tablename__ = 'user_account'

        # ... Column mappings

        addresses = relationship("Address", back_populates="user", lazy="raise_on_sql")


    class Address(Base):
        __tablename__ = 'address'

        # ... Column mappings

        user = relationship("User", back_populates="addresses", lazy="raise_on_sql")


Using such a mapping, the application is blocked from lazy loading,
indicating that a particular query would need to specify a loader strategy:

.. sourcecode:: python

    u1 = s.execute(select(User)).scalars().first()
    u1.addresses
    sqlalchemy.exc.InvalidRequestError: 'User.addresses' is not available due to lazy='raise_on_sql'


The exception would indicate that this collection should be loaded up front
instead:

.. sourcecode:: python

    u1 = s.execute(select(User).options(selectinload(User.addresses))).scalars().first()

The ``lazy="raise_on_sql"`` option tries to be smart about many-to-one
relationships as well; above, if the ``Address.user`` attribute of an
``Address`` object were not loaded, but that ``User`` object were locally
present in the same :class:`_orm.Session`, the "raiseload" strategy would not
raise an error.

.. seealso::

    :ref:`prevent_lazy_with_raiseload` - in :ref:`loading_toplevel`


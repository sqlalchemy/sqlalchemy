.. |prev| replace:: :doc:`columns`
.. |next| replace:: :doc:`api`

.. include:: queryguide_nav_include.rst

.. _orm_queryguide_relationship_loaders:

.. _loading_toplevel:

.. currentmodule:: sqlalchemy.orm

Relationship Loading Techniques
===============================

.. admonition:: About this Document

    This section presents an in-depth view of how to load related
    objects.   Readers should be familiar with
    :ref:`relationship_config_toplevel` and basic use.

    Most examples here assume the "User/Address" mapping setup similar
    to the one illustrated at :doc:`setup for selects <_plain_setup>`.

A big part of SQLAlchemy is providing a wide range of control over how related
objects get loaded when querying.   By "related objects" we refer to collections
or scalar associations configured on a mapper using :func:`_orm.relationship`.
This behavior can be configured at mapper construction time using the
:paramref:`_orm.relationship.lazy` parameter to the :func:`_orm.relationship`
function, as well as by using **ORM loader options** with
the :class:`_sql.Select` construct.

The loading of relationships falls into three categories; **lazy** loading,
**eager** loading, and **no** loading. Lazy loading refers to objects that are returned
from a query without the related
objects loaded at first.  When the given collection or reference is
first accessed on a particular object, an additional SELECT statement
is emitted such that the requested collection is loaded.

Eager loading refers to objects returned from a query with the related
collection or scalar reference already loaded up front.  The ORM
achieves this either by augmenting the SELECT statement it would normally
emit with a JOIN to load in related rows simultaneously, or by emitting
additional SELECT statements after the primary one to load collections
or scalar references at once.

"No" loading refers to the disabling of loading on a given relationship, either
that the attribute is empty and is just never loaded, or that it raises
an error when it is accessed, in order to guard against unwanted lazy loads.

Summary of Relationship Loading Styles
--------------------------------------

The primary forms of relationship loading are:

* **lazy loading** - available via ``lazy='select'`` or the :func:`.lazyload`
  option, this is the form of loading that emits a SELECT statement at
  attribute access time to lazily load a related reference on a single
  object at a time.  Lazy loading is the **default loading style** for all
  :func:`_orm.relationship` constructs that don't otherwise indicate the
  :paramref:`_orm.relationship.lazy` option.  Lazy loading is detailed at
  :ref:`lazy_loading`.

* **select IN loading** - available via ``lazy='selectin'`` or the :func:`.selectinload`
  option, this form of loading emits a second (or more) SELECT statement which
  assembles the primary key identifiers of the parent objects into an IN clause,
  so that all members of related collections / scalar references are loaded at once
  by primary key.  Select IN loading is detailed at :ref:`selectin_eager_loading`.

* **joined loading** - available via ``lazy='joined'`` or the :func:`_orm.joinedload`
  option, this form of loading applies a JOIN to the given SELECT statement
  so that related rows are loaded in the same result set.   Joined eager loading
  is detailed at :ref:`joined_eager_loading`.

* **raise loading** - available via ``lazy='raise'``, ``lazy='raise_on_sql'``,
  or the :func:`.raiseload` option, this form of loading is triggered at the
  same time a lazy load would normally occur, except it raises an ORM exception
  in order to guard against the application making unwanted lazy loads.
  An introduction to raise loading is at :ref:`prevent_lazy_with_raiseload`.

* **subquery loading** - available via ``lazy='subquery'`` or the :func:`.subqueryload`
  option, this form of loading emits a second SELECT statement which re-states the
  original query embedded inside of a subquery, then JOINs that subquery to the
  related table to be loaded to load all members of related collections / scalar
  references at once.  Subquery eager loading is detailed at :ref:`subquery_eager_loading`.

* **write only loading** - available via ``lazy='write_only'``, or by
  annotating the left side of the :class:`_orm.Relationship` object using the
  :class:`_orm.WriteOnlyMapped` annotation.   This collection-only
  loader style produces an alternative attribute instrumentation that never
  implicitly loads records from the database, instead only allowing
  :meth:`.WriteOnlyCollection.add`,
  :meth:`.WriteOnlyCollection.add_all` and :meth:`.WriteOnlyCollection.remove`
  methods.  Querying the collection is performed by invoking a SELECT statement
  which is constructed using the :meth:`.WriteOnlyCollection.select`
  method.    Write only loading is discussed at :ref:`write_only_relationship`.

* **dynamic loading** - available via ``lazy='dynamic'``, or by
  annotating the left side of the :class:`_orm.Relationship` object using the
  :class:`_orm.DynamicMapped` annotation. This is a legacy collection-only
  loader style which produces a :class:`_orm.Query` object when the collection
  is accessed, allowing custom SQL to be emitted against the collection's
  contents. However, dynamic loaders will implicitly iterate the underlying
  collection in various circumstances which makes them less useful for managing
  truly large collections. Dynamic loaders are superseded by
  :ref:`"write only" <write_only_relationship>` collections, which will prevent
  the underlying collection from being implicitly loaded under any
  circumstances. Dynamic loaders are discussed at :ref:`dynamic_relationship`.


.. _relationship_lazy_option:

Configuring Loader Strategies at Mapping Time
---------------------------------------------

The loader strategy for a particular relationship can be configured
at mapping time to take place in all cases where an object of the mapped
type is loaded, in the absence of any query-level options that modify it.
This is configured using the :paramref:`_orm.relationship.lazy` parameter to
:func:`_orm.relationship`; common values for this parameter
include ``select``, ``selectin`` and ``joined``.

The example below illustrates the relationship example at
:ref:`relationship_patterns_o2m`, configuring the ``Parent.children``
relationship to use :ref:`selectin_eager_loading` when a SELECT
statement for ``Parent`` objects is emitted::

    from typing import List

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Parent(Base):
        __tablename__ = "parent"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(lazy="selectin")


    class Child(Base):
        __tablename__ = "child"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))

Above, whenever a collection of ``Parent`` objects are loaded, each
``Parent`` will also have its ``children`` collection populated, using
the ``"selectin"`` loader strategy that emits a second query.

The default value of the :paramref:`_orm.relationship.lazy` argument is
``"select"``, which indicates :ref:`lazy_loading`.

.. _relationship_loader_options:

Relationship Loading with Loader Options
----------------------------------------

The other, and possibly more common way to configure loading strategies
is to set them up on a per-query basis against specific attributes using the
:meth:`_sql.Select.options` method.  Very detailed
control over relationship loading is available using loader options;
the most common are
:func:`_orm.joinedload`, :func:`_orm.selectinload`
and :func:`_orm.lazyload`.   The option accepts a class-bound attribute
referring to the specific class/attribute that should be targeted::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    # set children to load lazily
    stmt = select(Parent).options(lazyload(Parent.children))

    from sqlalchemy.orm import joinedload

    # set children to load eagerly with a join
    stmt = select(Parent).options(joinedload(Parent.children))

The loader options can also be "chained" using **method chaining**
to specify how loading should occur further levels deep::

    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    stmt = select(Parent).options(
        joinedload(Parent.children).subqueryload(Child.subelements)
    )

Chained loader options can be applied against a "lazy" loaded collection.
This means that when a collection or association is lazily loaded upon
access, the specified option will then take effect::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    stmt = select(Parent).options(lazyload(Parent.children).subqueryload(Child.subelements))

Above, the query will return ``Parent`` objects without the ``children``
collections loaded.  When the ``children`` collection on a particular
``Parent`` object is first accessed, it will lazy load the related
objects, but additionally apply eager loading to the ``subelements``
collection on each member of ``children``.


.. _loader_option_criteria:

Adding Criteria to loader options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The relationship attributes used to indicate loader options include the
ability to add additional filtering criteria to the ON clause of the join
that's created, or to the WHERE criteria involved, depending on the loader
strategy.  This can be achieved using the :meth:`.PropComparator.and_`
method which will pass through an option such that loaded results are limited
to the given filter criteria::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    stmt = select(A).options(lazyload(A.bs.and_(B.id > 5)))

When using limiting criteria, if a particular collection is already loaded
it won't be refreshed; to ensure the new criteria takes place, apply
the :ref:`orm_queryguide_populate_existing` execution option::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    stmt = (
        select(A)
        .options(lazyload(A.bs.and_(B.id > 5)))
        .execution_options(populate_existing=True)
    )

In order to add filtering criteria to all occurrences of an entity throughout
a query, regardless of loader strategy or where it occurs in the loading
process, see the :func:`_orm.with_loader_criteria` function.

.. versionadded:: 1.4

.. _orm_queryguide_relationship_sub_options:

Specifying Sub-Options with Load.options()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using method chaining, the loader style of each link in the path is explicitly
stated.  To navigate along a path without changing the existing loader style
of a particular attribute, the :func:`.defaultload` method/function may be used::

    from sqlalchemy import select
    from sqlalchemy.orm import defaultload

    stmt = select(A).options(defaultload(A.atob).joinedload(B.btoc))

A similar approach can be used to specify multiple sub-options at once, using
the :meth:`_orm.Load.options` method::

    from sqlalchemy import select
    from sqlalchemy.orm import defaultload
    from sqlalchemy.orm import joinedload

    stmt = select(A).options(
        defaultload(A.atob).options(joinedload(B.btoc), joinedload(B.btod))
    )

.. seealso::

    :ref:`orm_queryguide_load_only_related` - illustrates examples of combining
    relationship and column-oriented loader options.


.. note::  The loader options applied to an object's lazy-loaded collections
   are **"sticky"** to specific object instances, meaning they will persist
   upon collections loaded by that specific object for as long as it exists in
   memory.  For example, given the previous example::

      stmt = select(Parent).options(lazyload(Parent.children).subqueryload(Child.subelements))

   if the ``children`` collection on a particular ``Parent`` object loaded by
   the above query is expired (such as when a :class:`.Session` object's
   transaction is committed or rolled back, or :meth:`.Session.expire_all` is
   used), when the ``Parent.children`` collection is next accessed in order to
   re-load it, the ``Child.subelements`` collection will again be loaded using
   subquery eager loading. This stays the case even if the above ``Parent``
   object is accessed from a subsequent query that specifies a different set of
   options. To change the options on an existing object without expunging it
   and re-loading, they must be set explicitly in conjunction using the
   :ref:`orm_queryguide_populate_existing` execution option::

      # change the options on Parent objects that were already loaded
      stmt = (
          select(Parent)
          .execution_options(populate_existing=True)
          .options(lazyload(Parent.children).lazyload(Child.subelements))
          .all()
      )

   If the objects loaded above are fully cleared from the :class:`.Session`,
   such as due to garbage collection or that :meth:`.Session.expunge_all`
   were used, the "sticky" options will also be gone and the newly created
   objects will make use of new options if loaded again.

   A future SQLAlchemy release may add more alternatives to manipulating
   the loader options on already-loaded objects.


.. _lazy_loading:

Lazy Loading
------------

By default, all inter-object relationships are **lazy loading**. The scalar or
collection attribute associated with a :func:`_orm.relationship`
contains a trigger which fires the first time the attribute is accessed.  This
trigger typically issues a SQL call at the point of access
in order to load the related object or objects:

.. sourcecode:: pycon+sql

    >>> spongebob.addresses
    {execsql}SELECT
        addresses.id AS addresses_id,
        addresses.email_address AS addresses_email_address,
        addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id
    [5]
    {stop}[<Address(u'spongebob@google.com')>, <Address(u'j25@yahoo.com')>]

The one case where SQL is not emitted is for a simple many-to-one relationship, when
the related object can be identified by its primary key alone and that object is already
present in the current :class:`.Session`.  For this reason, while lazy loading
can be expensive for related collections, in the case that one is loading
lots of objects with simple many-to-ones against a relatively small set of
possible target objects, lazy loading may be able to refer to these objects locally
without emitting as many SELECT statements as there are parent objects.

This default behavior of "load upon attribute access" is known as "lazy" or
"select" loading - the name "select" because a "SELECT" statement is typically emitted
when the attribute is first accessed.

Lazy loading can be enabled for a given attribute that is normally
configured in some other way using the :func:`.lazyload` loader option::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    # force lazy loading for an attribute that is set to
    # load some other way normally
    stmt = select(User).options(lazyload(User.addresses))

.. _prevent_lazy_with_raiseload:

Preventing unwanted lazy loads using raiseload
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`.lazyload` strategy produces an effect that is one of the most
common issues referred to in object relational mapping; the
:term:`N plus one problem`, which states that for any N objects loaded,
accessing their lazy-loaded attributes means there will be N+1 SELECT
statements emitted.  In SQLAlchemy, the usual mitigation for the N+1 problem
is to make use of its very capable eager load system.  However, eager loading
requires that the attributes which are to be loaded be specified with the
:class:`_sql.Select` up front.  The problem of code that may access other attributes
that were not eagerly loaded, where lazy loading is not desired, may be
addressed using the :func:`.raiseload` strategy; this loader strategy
replaces the behavior of lazy loading with an informative error being
raised::

    from sqlalchemy import select
    from sqlalchemy.orm import raiseload

    stmt = select(User).options(raiseload(User.addresses))

Above, a ``User`` object loaded from the above query will not have
the ``.addresses`` collection loaded; if some code later on attempts to
access this attribute, an ORM exception is raised.

:func:`.raiseload` may be used with a so-called "wildcard" specifier to
indicate that all relationships should use this strategy.  For example,
to set up only one attribute as eager loading, and all the rest as raise::

    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from sqlalchemy.orm import raiseload

    stmt = select(Order).options(joinedload(Order.items), raiseload("*"))

The above wildcard will apply to **all** relationships not just on ``Order``
besides ``items``, but all those on the ``Item`` objects as well.  To set up
:func:`.raiseload` for only the ``Order`` objects, specify a full
path with :class:`_orm.Load`::

    from sqlalchemy import select
    from sqlalchemy.orm import joinedload
    from sqlalchemy.orm import Load

    stmt = select(Order).options(joinedload(Order.items), Load(Order).raiseload("*"))

Conversely, to set up the raise for just the ``Item`` objects::

    stmt = select(Order).options(joinedload(Order.items).raiseload("*"))

The :func:`.raiseload` option applies only to relationship attributes.  For
column-oriented attributes, the :func:`.defer` option supports the
:paramref:`.orm.defer.raiseload` option which works in the same way.

.. tip:: The "raiseload" strategies **do not apply**
   within the :term:`unit of work` flush process.   That means if the
   :meth:`_orm.Session.flush` process needs to load a collection in order
   to finish its work, it will do so while bypassing any :func:`_orm.raiseload`
   directives.

.. seealso::

    :ref:`wildcard_loader_strategies`

    :ref:`orm_queryguide_deferred_raiseload`

.. _joined_eager_loading:

Joined Eager Loading
--------------------

Joined eager loading is the oldest style of eager loading included with
the SQLAlchemy ORM.  It works by connecting a JOIN (by default
a LEFT OUTER join) to the SELECT statement emitted,
and populates the target scalar/collection from the
same result set as that of the parent.

At the mapping level, this looks like::

    class Address(Base):
        # ...

        user: Mapped[User] = relationship(lazy="joined")

Joined eager loading is usually applied as an option to a query, rather than
as a default loading option on the mapping, in particular when used for
collections rather than many-to-one-references.   This is achieved
using the :func:`_orm.joinedload` loader option:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy.orm import joinedload
    >>> stmt = select(User).options(joinedload(User.addresses)).filter_by(name="spongebob")
    >>> spongebob = session.scalars(stmt).unique().all()
    {execsql}SELECT
        addresses_1.id AS addresses_1_id,
        addresses_1.email_address AS addresses_1_email_address,
        addresses_1.user_id AS addresses_1_user_id,
        users.id AS users_id, users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    LEFT OUTER JOIN addresses AS addresses_1
        ON users.id = addresses_1.user_id
    WHERE users.name = ?
    ['spongebob']


.. tip::

    When including :func:`_orm.joinedload` in reference to a one-to-many or
    many-to-many collection, the :meth:`_result.Result.unique` method must be
    applied to the returned result, which will uniquify the incoming rows by
    primary key that otherwise are multiplied out by the join. The ORM will
    raise an error if this is not present.

    This is not automatic in modern SQLAlchemy, as it changes the behavior
    of the result set to return fewer ORM objects than the statement would
    normally return in terms of number of rows.  Therefore SQLAlchemy keeps
    the use of :meth:`_result.Result.unique` explicit, so there's no ambiguity
    that the returned objects are being uniqified on primary key.

The JOIN emitted by default is a LEFT OUTER JOIN, to allow for a lead object
that does not refer to a related row.  For an attribute that is guaranteed
to have an element, such as a many-to-one
reference to a related object where the referencing foreign key is NOT NULL,
the query can be made more efficient by using an inner join; this is available
at the mapping level via the :paramref:`_orm.relationship.innerjoin` flag::

    class Address(Base):
        # ...

        user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
        user: Mapped[User] = relationship(lazy="joined", innerjoin=True)

At the query option level, via the :paramref:`_orm.joinedload.innerjoin` flag::

    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    stmt = select(Address).options(joinedload(Address.user, innerjoin=True))

The JOIN will right-nest itself when applied in a chain that includes
an OUTER JOIN:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy.orm import joinedload
    >>> stmt = select(User).options(
    ...     joinedload(User.addresses).joinedload(Address.widgets, innerjoin=True)
    ... )
    >>> results = session.scalars(stmt).unique().all()
    {execsql}SELECT
        widgets_1.id AS widgets_1_id,
        widgets_1.name AS widgets_1_name,
        addresses_1.id AS addresses_1_id,
        addresses_1.email_address AS addresses_1_email_address,
        addresses_1.user_id AS addresses_1_user_id,
        users.id AS users_id, users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    LEFT OUTER JOIN (
        addresses AS addresses_1 JOIN widgets AS widgets_1 ON
        addresses_1.widget_id = widgets_1.id
    ) ON users.id = addresses_1.user_id


.. tip:: If using database row locking techniques when emitting the SELECT,
   meaning the :meth:`_sql.Select.with_for_update` method is being used
   to emit SELECT..FOR UPDATE, the joined table may be locked as well,
   depending on the behavior of the backend in use.   It's not recommended
   to use joined eager loading at the same time as SELECT..FOR UPDATE
   for this reason.



.. NOTE:  wow, this section. super long. it's not really reference material
   either it's conceptual

.. _zen_of_eager_loading:

The Zen of Joined Eager Loading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since joined eager loading seems to have many resemblances to the use of
:meth:`_sql.Select.join`, it often produces confusion as to when and how it should
be used.   It is critical to understand the distinction that while
:meth:`_sql.Select.join` is used to alter the results of a query, :func:`_orm.joinedload`
goes through great lengths to **not** alter the results of the query, and
instead hide the effects of the rendered join to only allow for related objects
to be present.

The philosophy behind loader strategies is that any set of loading schemes can
be applied to a particular query, and *the results don't change* - only the
number of SQL statements required to fully load related objects and collections
changes. A particular query might start out using all lazy loads.   After using
it in context, it might be revealed that particular attributes or collections
are always accessed, and that it would be more efficient to change the loader
strategy for these.   The strategy can be changed with no other modifications
to the query, the results will remain identical, but fewer SQL statements would
be emitted. In theory (and pretty much in practice), nothing you can do to the
:class:`_sql.Select` would make it load a different set of primary or related
objects based on a change in loader strategy.

How :func:`joinedload` in particular achieves this result of not impacting
entity rows returned in any way is that it creates an anonymous alias of the
joins it adds to your query, so that they can't be referenced by other parts of
the query.   For example, the query below uses :func:`_orm.joinedload` to create a
LEFT OUTER JOIN from ``users`` to ``addresses``, however the ``ORDER BY`` added
against ``Address.email_address`` is not valid - the ``Address`` entity is not
named in the query:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy.orm import joinedload
    >>> stmt = (
    ...     select(User)
    ...     .options(joinedload(User.addresses))
    ...     .filter(User.name == "spongebob")
    ...     .order_by(Address.email_address)
    ... )
    >>> result = session.scalars(stmt).unique().all()
    {execsql}SELECT
        addresses_1.id AS addresses_1_id,
        addresses_1.email_address AS addresses_1_email_address,
        addresses_1.user_id AS addresses_1_user_id,
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    LEFT OUTER JOIN addresses AS addresses_1
        ON users.id = addresses_1.user_id
    WHERE users.name = ?
    ORDER BY addresses.email_address   <-- this part is wrong !
    ['spongebob']

Above, ``ORDER BY addresses.email_address`` is not valid since ``addresses`` is not in the
FROM list.   The correct way to load the ``User`` records and order by email
address is to use :meth:`_sql.Select.join`:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> stmt = (
    ...     select(User)
    ...     .join(User.addresses)
    ...     .filter(User.name == "spongebob")
    ...     .order_by(Address.email_address)
    ... )
    >>> result = session.scalars(stmt).unique().all()
    {execsql}
    SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    JOIN addresses ON users.id = addresses.user_id
    WHERE users.name = ?
    ORDER BY addresses.email_address
    ['spongebob']

The statement above is of course not the same as the previous one, in that the
columns from ``addresses`` are not included in the result at all.   We can add
:func:`_orm.joinedload` back in, so that there are two joins - one is that which we
are ordering on, the other is used anonymously to load the contents of the
``User.addresses`` collection:

.. sourcecode:: pycon+sql


    >>> stmt = (
    ...     select(User)
    ...     .join(User.addresses)
    ...     .options(joinedload(User.addresses))
    ...     .filter(User.name == "spongebob")
    ...     .order_by(Address.email_address)
    ... )
    >>> result = session.scalars(stmt).unique().all()
    {execsql}SELECT
        addresses_1.id AS addresses_1_id,
        addresses_1.email_address AS addresses_1_email_address,
        addresses_1.user_id AS addresses_1_user_id,
        users.id AS users_id, users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users JOIN addresses
        ON users.id = addresses.user_id
    LEFT OUTER JOIN addresses AS addresses_1
        ON users.id = addresses_1.user_id
    WHERE users.name = ?
    ORDER BY addresses.email_address
    ['spongebob']

What we see above is that our usage of :meth:`_sql.Select.join` is to supply JOIN
clauses we'd like to use in subsequent query criterion, whereas our usage of
:func:`_orm.joinedload` only concerns itself with the loading of the
``User.addresses`` collection, for each ``User`` in the result. In this case,
the two joins most probably appear redundant - which they are.  If we wanted to
use just one JOIN for collection loading as well as ordering, we use the
:func:`.contains_eager` option, described in :ref:`contains_eager` below.   But
to see why :func:`joinedload` does what it does, consider if we were
**filtering** on a particular ``Address``:

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...     select(User)
    ...     .join(User.addresses)
    ...     .options(joinedload(User.addresses))
    ...     .filter(User.name == "spongebob")
    ...     .filter(Address.email_address == "someaddress@foo.com")
    ... )
    >>> result = session.scalars(stmt).unique().all()
    {execsql}SELECT
        addresses_1.id AS addresses_1_id,
        addresses_1.email_address AS addresses_1_email_address,
        addresses_1.user_id AS addresses_1_user_id,
        users.id AS users_id, users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users JOIN addresses
        ON users.id = addresses.user_id
    LEFT OUTER JOIN addresses AS addresses_1
        ON users.id = addresses_1.user_id
    WHERE users.name = ? AND addresses.email_address = ?
    ['spongebob', 'someaddress@foo.com']

Above, we can see that the two JOINs have very different roles.  One will match
exactly one row, that of the join of ``User`` and ``Address`` where
``Address.email_address=='someaddress@foo.com'``. The other LEFT OUTER JOIN
will match *all* ``Address`` rows related to ``User``, and is only used to
populate the ``User.addresses`` collection, for those ``User`` objects that are
returned.

By changing the usage of :func:`_orm.joinedload` to another style of loading, we
can change how the collection is loaded completely independently of SQL used to
retrieve the actual ``User`` rows we want.  Below we change :func:`_orm.joinedload`
into :func:`.selectinload`:

.. sourcecode:: pycon+sql

    >>> stmt = (
    ...     select(User)
    ...     .join(User.addresses)
    ...     .options(selectinload(User.addresses))
    ...     .filter(User.name == "spongebob")
    ...     .filter(Address.email_address == "someaddress@foo.com")
    ... )
    >>> result = session.scalars(stmt).all()
    {execsql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    JOIN addresses ON users.id = addresses.user_id
    WHERE
        users.name = ?
        AND addresses.email_address = ?
    ['spongebob', 'someaddress@foo.com']
    # ... selectinload() emits a SELECT in order
    # to load all address records ...


When using joined eager loading, if the query contains a modifier that impacts
the rows returned externally to the joins, such as when using DISTINCT, LIMIT,
OFFSET or equivalent, the completed statement is first wrapped inside a
subquery, and the joins used specifically for joined eager loading are applied
to the subquery.   SQLAlchemy's joined eager loading goes the extra mile, and
then ten miles further, to absolutely ensure that it does not affect the end
result of the query, only the way collections and related objects are loaded,
no matter what the format of the query is.

.. seealso::

    :ref:`contains_eager` - using :func:`.contains_eager`

.. _selectin_eager_loading:

Select IN loading
-----------------

In most cases, selectin loading is the most simple and
efficient way to eagerly load collections of objects.  The only scenario in
which selectin eager loading is not feasible is when the model is using
composite primary keys, and the backend database does not support tuples with
IN, which currently includes SQL Server.

"Select IN" eager loading is provided using the ``"selectin"`` argument to
:paramref:`_orm.relationship.lazy` or by using the :func:`.selectinload` loader
option.   This style of loading emits a SELECT that refers to the primary key
values of the parent object, or in the case of a many-to-one
relationship to the those of the child objects, inside of an IN clause, in
order to load related associations:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy import selectinload
    >>> stmt = (
    ...     select(User)
    ...     .options(selectinload(User.addresses))
    ...     .filter(or_(User.name == "spongebob", User.name == "ed"))
    ... )
    >>> result = session.scalars(stmt).all()
    {execsql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    WHERE users.name = ? OR users.name = ?
    ('spongebob', 'ed')
    SELECT
        addresses.id AS addresses_id,
        addresses.email_address AS addresses_email_address,
        addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE addresses.user_id IN (?, ?)
    (5, 7)

Above, the second SELECT refers to ``addresses.user_id IN (5, 7)``, where the
"5" and "7" are the primary key values for the previous two ``User``
objects loaded; after a batch of objects are completely loaded, their primary
key values are injected into the ``IN`` clause for the second SELECT.
Because the relationship between ``User`` and ``Address`` has a simple
primary join condition and provides that the
primary key values for ``User`` can be derived from ``Address.user_id``, the
statement has no joins or subqueries at all.

For simple many-to-one loads, a JOIN is also not needed as the foreign key
value from the parent object is used:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy import selectinload
    >>> stmt = select(Address).options(selectinload(Address.user))
    >>> result = session.scalars(stmt).all()
    {execsql}SELECT
        addresses.id AS addresses_id,
        addresses.email_address AS addresses_email_address,
        addresses.user_id AS addresses_user_id
        FROM addresses
    SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    WHERE users.id IN (?, ?)
    (1, 2)

.. tip::

   by "simple" we mean that the :paramref:`_orm.relationship.primaryjoin`
   condition expresses an equality comparison between the primary key of the
   "one" side and a straight foreign key of the "many" side, without any
   additional criteria.

Select IN loading also supports many-to-many relationships, where it currently
will JOIN across all three tables to match rows from one side to the other.

Things to know about this kind of loading include:

* The strategy emits a SELECT for up to 500 parent primary key values at a
  time, as the primary keys are rendered into a large IN expression in the
  SQL statement.   Some databases like Oracle have a hard limit on how large
  an IN expression can be, and overall the size of the SQL string shouldn't
  be arbitrarily large.

* As "selectin" loading relies upon IN, for a mapping with composite primary
  keys, it must use the "tuple" form of IN, which looks like ``WHERE
  (table.column_a, table.column_b) IN ((?, ?), (?, ?), (?, ?))``. This syntax
  is not currently supported on SQL Server and for SQLite requires at least
  version 3.15.  There is no special logic in SQLAlchemy to check
  ahead of time which platforms support this syntax or not; if run against a
  non-supporting platform, the database will return an error immediately.   An
  advantage to SQLAlchemy just running the SQL out for it to fail is that if a
  particular database does start supporting this syntax, it will work without
  any changes to SQLAlchemy (as was the case with SQLite).


.. _subquery_eager_loading:

Subquery Eager Loading
----------------------

.. legacy:: The :func:`_orm.subqueryload` eager loader is mostly legacy
   at this point, superseded by the :func:`_orm.selectinload` strategy
   which is of much simpler design, more flexible with features such as
   :ref:`Yield Per <orm_queryguide_yield_per>`, and emits more efficient SQL
   statements in most cases.   As :func:`_orm.subqueryload` relies upon
   re-interpreting the original SELECT statement, it may fail to work
   efficiently when given very complex source queries.

   :func:`_orm.subqueryload` may continue to be useful for the specific
   case of an eager loaded collection for objects that use composite primary
   keys, on the Microsoft SQL Server backend that continues to not have
   support for the "tuple IN" syntax.

Subquery loading is similar in operation to selectin eager loading, however
the SELECT statement which is emitted is derived from the original statement,
and has a more complex query structure as that of selectin eager loading.

Subquery eager loading is provided using the ``"subquery"`` argument to
:paramref:`_orm.relationship.lazy` or by using the :func:`.subqueryload` loader
option.

The operation of subquery eager loading is to emit a second SELECT statement
for each relationship to be loaded, across all result objects at once.
This SELECT statement refers to the original SELECT statement, wrapped
inside of a subquery, so that we retrieve the same list of primary keys
for the primary object being returned, then link that to the sum of all
the collection members to load them at once:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> from sqlalchemy.orm import subqueryload
    >>> stmt = select(User).options(subqueryload(User.addresses)).filter_by(name="spongebob")
    >>> results = session.scalars(stmt).all()
    {execsql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    WHERE users.name = ?
    ('spongebob',)
    SELECT
        addresses.id AS addresses_id,
        addresses.email_address AS addresses_email_address,
        addresses.user_id AS addresses_user_id,
        anon_1.users_id AS anon_1_users_id
    FROM (
        SELECT users.id AS users_id
        FROM users
        WHERE users.name = ?) AS anon_1
    JOIN addresses ON anon_1.users_id = addresses.user_id
    ORDER BY anon_1.users_id, addresses.id
    ('spongebob',)


Things to know about this kind of loading include:

* The SELECT statement emitted by the "subquery" loader strategy, unlike
  that of "selectin", requires a subquery, and will inherit whatever performance
  limitations are present in the original query.  The subquery itself may
  also incur performance penalties based on the specifics of the database in
  use.

* "subquery" loading imposes some special ordering requirements in order to work
  correctly.  A query which makes use of :func:`.subqueryload` in conjunction with a
  limiting modifier such as :meth:`_sql.Select.limit`,
  or :meth:`_sql.Select.offset` should **always** include :meth:`_sql.Select.order_by`
  against unique column(s) such as the primary key, so that the additional queries
  emitted by :func:`.subqueryload` include
  the same ordering as used by the parent query.  Without it, there is a chance
  that the inner query could return the wrong rows::

    # incorrect, no ORDER BY
    stmt = select(User).options(subqueryload(User.addresses).limit(1))

    # incorrect if User.name is not unique
    stmt = select(User).options(subqueryload(User.addresses)).order_by(User.name).limit(1)

    # correct
    stmt = (
        select(User)
        .options(subqueryload(User.addresses))
        .order_by(User.name, User.id)
        .limit(1)
    )

  .. seealso::

       :ref:`faq_subqueryload_limit_sort` - detailed example


* "subquery" loading also incurs additional performance / complexity issues
  when used on a many-levels-deep eager load, as subqueries will be nested
  repeatedly.

* "subquery" loading is not compatible with the
  "batched" loading supplied by :ref:`Yield Per <orm_queryguide_yield_per>`, both for collection
  and scalar relationships.

For the above reasons, the "selectin" strategy should be preferred over
"subquery".

.. seealso::

    :ref:`selectin_eager_loading`




.. _what_kind_of_loading:

What Kind of Loading to Use ?
-----------------------------

Which type of loading to use typically comes down to optimizing the tradeoff
between number of SQL executions, complexity of SQL emitted, and amount of
data fetched.


**One to Many / Many to Many Collection** - The :func:`_orm.selectinload` is
generally the best loading strategy to use.  It emits an additional SELECT
that uses as few tables as possible, leaving the original statement unaffected,
and is most flexible for any kind of
originating query.   Its only major limitation is when using a table with
composite primary keys on a backend that does not support "tuple IN", which
currently includes SQL Server and very old SQLite versions; all other included
backends support it.

**Many to One** - The :func:`_orm.joinedload` strategy is the most general
purpose strategy. In special cases, the :func:`_orm.immediateload` strategy may
also be useful, if there are a very small number of potential related values,
as this strategy will fetch the object from the local :class:`_orm.Session`
without emitting any SQL if the related object is already present.



Polymorphic Eager Loading
-------------------------

Specification of polymorphic options on a per-eager-load basis is supported.
See the section :ref:`eagerloading_polymorphic_subtypes` for examples
of the :meth:`.PropComparator.of_type` method in conjunction with the
:func:`_orm.with_polymorphic` function.

.. _wildcard_loader_strategies:

Wildcard Loading Strategies
---------------------------

Each of :func:`_orm.joinedload`, :func:`.subqueryload`, :func:`.lazyload`,
:func:`.selectinload`,
:func:`.noload`, and :func:`.raiseload` can be used to set the default
style of :func:`_orm.relationship` loading
for a particular query, affecting all :func:`_orm.relationship` -mapped
attributes not otherwise
specified in the statement.   This feature is available by passing
the string ``'*'`` as the argument to any of these options::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload

    stmt = select(MyClass).options(lazyload("*"))

Above, the ``lazyload('*')`` option will supersede the ``lazy`` setting
of all :func:`_orm.relationship` constructs in use for that query,
with the exception of those that use ``lazy='write_only'``
or ``lazy='dynamic'``.

If some relationships specify
``lazy='joined'`` or ``lazy='selectin'``, for example,
using ``lazyload('*')`` will unilaterally
cause all those relationships to use ``'select'`` loading, e.g. emit a
SELECT statement when each attribute is accessed.

The option does not supersede loader options stated in the
query, such as :func:`.joinedload`,
:func:`.selectinload`, etc.  The query below will still use joined loading
for the ``widget`` relationship::

    from sqlalchemy import select
    from sqlalchemy.orm import lazyload
    from sqlalchemy.orm import joinedload

    stmt = select(MyClass).options(lazyload("*"), joinedload(MyClass.widget))

While the instruction for :func:`.joinedload` above will take place regardless
of whether it appears before or after the :func:`.lazyload` option,
if multiple options that each included ``"*"`` were passed, the last one
will take effect.

.. _orm_queryguide_relationship_per_entity_wildcard:

Per-Entity Wildcard Loading Strategies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A variant of the wildcard loader strategy is the ability to set the strategy
on a per-entity basis.  For example, if querying for ``User`` and ``Address``,
we can instruct all relationships on ``Address`` to use lazy loading,
while leaving the loader strategies for ``User`` unaffected,
by first applying the :class:`_orm.Load` object, then specifying the ``*`` as a
chained option::

    from sqlalchemy import select
    from sqlalchemy.orm import Load

    stmt = select(User, Address).options(Load(Address).lazyload("*"))

Above, all relationships on ``Address`` will be set to a lazy load.

.. _joinedload_and_join:

.. _contains_eager:

Routing Explicit Joins/Statements into Eagerly Loaded Collections
-----------------------------------------------------------------

The behavior of :func:`_orm.joinedload()` is such that joins are
created automatically, using anonymous aliases as targets, the results of which
are routed into collections and
scalar references on loaded objects. It is often the case that a query already
includes the necessary joins which represent a particular collection or scalar
reference, and the joins added by the joinedload feature are redundant - yet
you'd still like the collections/references to be populated.

For this SQLAlchemy supplies the :func:`_orm.contains_eager`
option. This option is used in the same manner as the
:func:`_orm.joinedload()` option except it is assumed that the
:class:`_sql.Select` object will explicitly include the appropriate joins,
typically using methods like :meth:`_sql.Select.join`.
Below, we specify a join between ``User`` and ``Address``
and additionally establish this as the basis for eager loading of ``User.addresses``::

    from sqlalchemy.orm import contains_eager

    stmt = select(User).join(User.addresses).options(contains_eager(User.addresses))

If the "eager" portion of the statement is "aliased", the path
should be specified using :meth:`.PropComparator.of_type`, which allows
the specific :func:`_orm.aliased` construct to be passed:

.. sourcecode:: python+sql

    # use an alias of the Address entity
    adalias = aliased(Address)

    # construct a statement which expects the "addresses" results

    stmt = (
        select(User)
        .outerjoin(User.addresses.of_type(adalias))
        .options(contains_eager(User.addresses.of_type(adalias)))
    )

    # get results normally
    r = session.scalars(stmt).unique().all()
    {execsql}SELECT
        users.user_id AS users_user_id,
        users.user_name AS users_user_name,
        adalias.address_id AS adalias_address_id,
        adalias.user_id AS adalias_user_id,
        adalias.email_address AS adalias_email_address,
        (...other columns...)
    FROM users
    LEFT OUTER JOIN email_addresses AS email_addresses_1
    ON users.user_id = email_addresses_1.user_id

The path given as the argument to :func:`.contains_eager` needs
to be a full path from the starting entity. For example if we were loading
``Users->orders->Order->items->Item``, the option would be used as::

    stmt = select(User).options(contains_eager(User.orders).contains_eager(Order.items))

Using contains_eager() to load a custom-filtered collection result
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When we use :func:`.contains_eager`, *we* are constructing ourselves the
SQL that will be used to populate collections.  From this, it naturally follows
that we can opt to **modify** what values the collection is intended to store,
by writing our SQL to load a subset of elements for collections or
scalar attributes.

As an example, we can load a ``User`` object and eagerly load only particular
addresses into its ``.addresses`` collection by filtering the joined data,
routing it using :func:`_orm.contains_eager`, also using
:ref:`orm_queryguide_populate_existing` to ensure any already-loaded collections
are overwritten::

    stmt = (
        select(User)
        .join(User.addresses)
        .filter(Address.email_address.like("%@aol.com"))
        .options(contains_eager(User.addresses))
        .execution_options(populate_existing=True)
    )

The above query will load only ``User`` objects which contain at
least ``Address`` object that contains the substring ``'aol.com'`` in its
``email`` field; the ``User.addresses`` collection will contain **only**
these ``Address`` entries, and *not* any other ``Address`` entries that are
in fact associated with the collection.

.. tip::  In all cases, the SQLAlchemy ORM does **not overwrite already loaded
   attributes and collections** unless told to do so.   As there is an
   :term:`identity map` in use, it is often the case that an ORM query is
   returning objects that were in fact already present and loaded in memory.
   Therefore, when using :func:`_orm.contains_eager` to populate a collection
   in an alternate way, it is usually a good idea to use
   :ref:`orm_queryguide_populate_existing` as illustrated above so that an
   already-loaded collection is refreshed with the new data.
   The ``populate_existing`` option will reset **all** attributes that were
   already present, including pending changes, so make sure all data is flushed
   before using it.   Using the :class:`_orm.Session` with its default behavior
   of :ref:`autoflush <session_flushing>` is sufficient.

.. note::   The customized collection we load using :func:`_orm.contains_eager`
   is not "sticky"; that is, the next time this collection is loaded, it will
   be loaded with its usual default contents.   The collection is subject
   to being reloaded if the object is expired, which occurs whenever the
   :meth:`.Session.commit`, :meth:`.Session.rollback` methods are used
   assuming default session settings, or the :meth:`.Session.expire_all`
   or :meth:`.Session.expire` methods are used.


Relationship Loader API
-----------------------

.. autofunction:: contains_eager

.. autofunction:: defaultload

.. autofunction:: immediateload

.. autofunction:: joinedload

.. autofunction:: lazyload

.. autoclass:: sqlalchemy.orm.Load
    :members:
    :inherited-members: Generative

.. autofunction:: noload

.. autofunction:: raiseload

.. autofunction:: selectinload

.. autofunction:: subqueryload

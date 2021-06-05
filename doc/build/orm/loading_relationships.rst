.. _loading_toplevel:

.. currentmodule:: sqlalchemy.orm

Relationship Loading Techniques
===============================

A big part of SQLAlchemy is providing a wide range of control over how related
objects get loaded when querying.   By "related objects" we refer to collections
or scalar associations configured on a mapper using :func:`_orm.relationship`.
This behavior can be configured at mapper construction time using the
:paramref:`_orm.relationship.lazy` parameter to the :func:`_orm.relationship`
function, as well as by using options with the :class:`_query.Query` object.

The loading of relationships falls into three categories; **lazy** loading,
**eager** loading, and **no** loading. Lazy loading refers to objects are returned
from a query without the related
objects loaded at first.  When the given collection or reference is
first accessed on a particular object, an additional SELECT statement
is emitted such that the requested collection is loaded.

Eager loading refers to objects returned from a query with the related
collection or scalar reference already loaded up front.  The :class:`_query.Query`
achieves this either by augmenting the SELECT statement it would normally
emit with a JOIN to load in related rows simultaneously, or by emitting
additional SELECT statements after the primary one to load collections
or scalar references at once.

"No" loading refers to the disabling of loading on a given relationship, either
that the attribute is empty and is just never loaded, or that it raises
an error when it is accessed, in order to guard against unwanted lazy loads.

The primary forms of relationship loading are:

* **lazy loading** - available via ``lazy='select'`` or the :func:`.lazyload`
  option, this is the form of loading that emits a SELECT statement at
  attribute access time to lazily load a related reference on a single
  object at a time.  Lazy loading is detailed at :ref:`lazy_loading`.

* **joined loading** - available via ``lazy='joined'`` or the :func:`_orm.joinedload`
  option, this form of loading applies a JOIN to the given SELECT statement
  so that related rows are loaded in the same result set.   Joined eager loading
  is detailed at :ref:`joined_eager_loading`.

* **subquery loading** - available via ``lazy='subquery'`` or the :func:`.subqueryload`
  option, this form of loading emits a second SELECT statement which re-states the
  original query embedded inside of a subquery, then JOINs that subquery to the
  related table to be loaded to load all members of related collections / scalar
  references at once.  Subquery eager loading is detailed at :ref:`subquery_eager_loading`.

* **select IN loading** - available via ``lazy='selectin'`` or the :func:`.selectinload`
  option, this form of loading emits a second (or more) SELECT statement which
  assembles the primary key identifiers of the parent objects into an IN clause,
  so that all members of related collections / scalar references are loaded at once
  by primary key.  Select IN loading is detailed at :ref:`selectin_eager_loading`.

* **raise loading** - available via ``lazy='raise'``, ``lazy='raise_on_sql'``,
  or the :func:`.raiseload` option, this form of loading is triggered at the
  same time a lazy load would normally occur, except it raises an ORM exception
  in order to guard against the application making unwanted lazy loads.
  An introduction to raise loading is at :ref:`prevent_lazy_with_raiseload`.

* **no loading** - available via ``lazy='noload'``, or the :func:`.noload`
  option; this loading style turns the attribute into an empty attribute
  (``None`` or ``[]``) that will never load or have any loading effect. This
  seldom-used strategy behaves somewhat like an eager loader when objects are
  loaded in that an empty attribute or collection is placed, but for expired
  objects relies upon the default value of the attribute being returned on
  access; the net effect is the same except for whether or not the attribute
  name appears in the :attr:`.InstanceState.unloaded` collection.   ``noload``
  may be useful for implementing a "write-only" attribute but this usage is not
  currently tested or formally supported.


.. _relationship_lazy_option:

Configuring Loader Strategies at Mapping Time
---------------------------------------------

The loader strategy for a particular relationship can be configured
at mapping time to take place in all cases where an object of the mapped
type is loaded, in the absence of any query-level options that modify it.
This is configured using the :paramref:`_orm.relationship.lazy` parameter to
:func:`_orm.relationship`; common values for this parameter
include ``select``, ``joined``, ``subquery`` and ``selectin``.

For example, to configure a relationship to use joined eager loading when
the parent object is queried::

    class Parent(Base):
        __tablename__ = 'parent'

        id = Column(Integer, primary_key=True)
        children = relationship("Child", lazy='joined')

Above, whenever a collection of ``Parent`` objects are loaded, each
``Parent`` will also have its ``children`` collection populated, using
rows fetched by adding a JOIN to the query for ``Parent`` objects.
See :ref:`joined_eager_loading` for background on this style of loading.

The default value of the :paramref:`_orm.relationship.lazy` argument is
``"select"``, which indicates lazy loading.  See :ref:`lazy_loading` for
further background.

.. _relationship_loader_options:

Relationship Loading with Loader Options
----------------------------------------

The other, and possibly more common way to configure loading strategies
is to set them up on a per-query basis against specific attributes using the
:meth:`_query.Query.options` method.  Very detailed
control over relationship loading is available using loader options;
the most common are
:func:`~sqlalchemy.orm.joinedload`,
:func:`~sqlalchemy.orm.subqueryload`, :func:`~sqlalchemy.orm.selectinload`
and :func:`~sqlalchemy.orm.lazyload`.   The option accepts either
the string name of an attribute against a parent, or for greater specificity
can accommodate a class-bound attribute directly::

    # set children to load lazily
    session.query(Parent).options(lazyload(Parent.children)).all()

    # set children to load eagerly with a join
    session.query(Parent).options(joinedload(Parent.children)).all()

The loader options can also be "chained" using **method chaining**
to specify how loading should occur further levels deep::

    session.query(Parent).options(
        joinedload(Parent.children).
        subqueryload(Child.subelements)).all()

Chained loader options can be applied against a "lazy" loaded collection.
This means that when a collection or association is lazily loaded upon
access, the specified option will then take effect::

    session.query(Parent).options(
        lazyload(Parent.children).
        subqueryload(Child.subelements)).all()

Above, the query will return ``Parent`` objects without the ``children``
collections loaded.  When the ``children`` collection on a particular
``Parent`` object is first accessed, it will lazy load the related
objects, but additionally apply eager loading to the ``subelements``
collection on each member of ``children``.

The above examples, using :class:`_orm.Query`, are now referred to as
:term:`1.x style` queries.   The options system is available as well for
:term:`2.0 style` queries using the :meth:`_sql.Select.options` method::

  stmt = select(Parent).options(
        lazyload(Parent.children).
        subqueryload(Child.subelements))

  result = session.execute(stmt)

Under the hood, :class:`_orm.Query` is ultimately using the above
:class:`_sql.select` based mechanism.


.. _loader_option_criteria:

Adding Criteria to loader options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The relationship attributes used to indicate loader options include the
ability to add additional filtering criteria to the ON clause of the join
that's created, or to the WHERE criteria involved, depending on the loader
strategy.  This can be achieved using the :meth:`.PropComparator.and_`
method which will pass through an option such that loaded results are limited
to the given filter criteria::

    session.query(A).options(lazyload(A.bs.and_(B.id > 5)))

When using limiting criteria, if a particular collection is already loaded
it won't be refreshed; to ensure the new criteria takes place, apply
the :meth:`_query.Query.populate_existing` option::

    session.query(A).options(lazyload(A.bs.and_(B.id > 5))).populate_existing()

In order to add filtering criteria to all occurrences of an entity throughout
a query, regardless of loader strategy or where it occurs in the loading
process, see the :func:`_orm.with_loader_criteria` function.

.. versionadded:: 1.4

Specifying Sub-Options with Load.options()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using method chaining, the loader style of each link in the path is explicitly
stated.  To navigate along a path without changing the existing loader style
of a particular attribute, the :func:`.defaultload` method/function may be used::

    session.query(A).options(
        defaultload(A.atob).
        joinedload(B.btoc)).all()

A similar approach can be used to specify multiple sub-options at once, using
the :meth:`_orm.Load.options` method::

    session.query(A).options(
        defaultload(A.atob).options(
          joinedload(B.btoc),
          joinedload(B.btod)
        )).all()

.. versionadded:: 1.3.6 added :meth:`_orm.Load.options`


.. seealso::

    :ref:`deferred_loading_w_multiple` - illustrates examples of combining
    relationship and column-oriented loader options.


.. note::  The loader options applied to an object's lazy-loaded collections
   are **"sticky"** to specific object instances, meaning they will persist
   upon collections loaded by that specific object for as long as it exists in
   memory.  For example, given the previous example::

      session.query(Parent).options(
          lazyload(Parent.children).
          subqueryload(Child.subelements)).all()

   if the ``children`` collection on a particular ``Parent`` object loaded by
   the above query is expired (such as when a :class:`.Session` object's
   transaction is committed or rolled back, or :meth:`.Session.expire_all` is
   used), when the ``Parent.children`` collection is next accessed in order to
   re-load it, the ``Child.subelements`` collection will again be loaded using
   subquery eager loading.This stays the case even if the above ``Parent``
   object is accessed from a subsequent query that specifies a different set of
   options.To change the options on an existing object without expunging it and
   re-loading, they must be set explicitly in conjunction with the
   :meth:`_query.Query.populate_existing` method::

      # change the options on Parent objects that were already loaded
      session.query(Parent).populate_existing().options(
          lazyload(Parent.children).
          lazyload(Child.subelements)).all()

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
collection attribute associated with a :func:`~sqlalchemy.orm.relationship`
contains a trigger which fires the first time the attribute is accessed.  This
trigger typically issues a SQL call at the point of access
in order to load the related object or objects:

.. sourcecode:: python+sql

    >>> jack.addresses
    {opensql}SELECT
        addresses.id AS addresses_id,
        addresses.email_address AS addresses_email_address,
        addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id
    [5]
    {stop}[<Address(u'jack@google.com')>, <Address(u'j25@yahoo.com')>]

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

    from sqlalchemy.orm import lazyload

    # force lazy loading for an attribute that is set to
    # load some other way normally
    session.query(User).options(lazyload(User.addresses))

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
:class:`_query.Query` up front.  The problem of code that may access other attributes
that were not eagerly loaded, where lazy loading is not desired, may be
addressed using the :func:`.raiseload` strategy; this loader strategy
replaces the behavior of lazy loading with an informative error being
raised::

    from sqlalchemy.orm import raiseload
    session.query(User).options(raiseload(User.addresses))

Above, a ``User`` object loaded from the above query will not have
the ``.addresses`` collection loaded; if some code later on attempts to
access this attribute, an ORM exception is raised.

:func:`.raiseload` may be used with a so-called "wildcard" specifier to
indicate that all relationships should use this strategy.  For example,
to set up only one attribute as eager loading, and all the rest as raise::

    session.query(Order).options(
        joinedload(Order.items), raiseload('*'))

The above wildcard will apply to **all** relationships not just on ``Order``
besides ``items``, but all those on the ``Item`` objects as well.  To set up
:func:`.raiseload` for only the ``Order`` objects, specify a full
path with :class:`_orm.Load`::

    from sqlalchemy.orm import Load

    session.query(Order).options(
        joinedload(Order.items), Load(Order).raiseload('*'))

Conversely, to set up the raise for just the ``Item`` objects::

    session.query(Order).options(
        joinedload(Order.items).raiseload('*'))


The :func:`.raiseload` option applies only to relationship attributes.  For
column-oriented attributes, the :func:`.defer` option supports the
:paramref:`.orm.defer.raiseload` option which works in the same way.

.. versionchanged:: 1.4.0 The "raiseload" strategies **do not take place**
  within the unit of work flush process, as of SQLAlchemy 1.4.0. This means
  that if the unit of work needs to load a particular attribute in order to
  complete its work, it will perform the load. It's not always easy to prevent
  a particular relationship load from occurring within the UOW process
  particularly with less common kinds of relationships. The lazy="raise" case
  is more intended for explicit attribute access within the application space.

.. seealso::

    :ref:`wildcard_loader_strategies`

    :ref:`deferred_raiseload`

.. _joined_eager_loading:

Joined Eager Loading
--------------------

Joined eager loading is the most fundamental style of eager loading in the
ORM.  It works by connecting a JOIN (by default
a LEFT OUTER join) to the SELECT statement emitted by a :class:`_query.Query`
and populates the target scalar/collection from the
same result set as that of the parent.

At the mapping level, this looks like::

    class Address(Base):
        # ...

        user = relationship(User, lazy="joined")

Joined eager loading is usually applied as an option to a query, rather than
as a default loading option on the mapping, in particular when used for
collections rather than many-to-one-references.   This is achieved
using the :func:`_orm.joinedload` loader option:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... options(joinedload(User.addresses)).\
    ... filter_by(name='jack').all()
    {opensql}SELECT
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
    ['jack']


The JOIN emitted by default is a LEFT OUTER JOIN, to allow for a lead object
that does not refer to a related row.  For an attribute that is guaranteed
to have an element, such as a many-to-one
reference to a related object where the referencing foreign key is NOT NULL,
the query can be made more efficient by using an inner join; this is available
at the mapping level via the :paramref:`_orm.relationship.innerjoin` flag::

    class Address(Base):
        # ...

        user_id = Column(ForeignKey('users.id'), nullable=False)
        user = relationship(User, lazy="joined", innerjoin=True)

At the query option level, via the :paramref:`_orm.joinedload.innerjoin` flag::

    session.query(Address).options(
        joinedload(Address.user, innerjoin=True))

The JOIN will right-nest itself when applied in a chain that includes
an OUTER JOIN:

.. sourcecode:: python+sql

    >>> session.query(User).options(
    ...     joinedload(User.addresses).
    ...     joinedload(Address.widgets, innerjoin=True)).all()
    {opensql}SELECT
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

On older versions of SQLite, the above nested right JOIN may be re-rendered
as a nested subquery.  Older versions of SQLAlchemy would convert right-nested
joins into subqueries in all cases.

Joined eager loading and result set batching
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A central concept of joined eager loading when applied to collections is that
the :class:`_query.Query` object must de-duplicate rows against the leading
entity being queried.  Such as above,
if the ``User`` object we loaded referred to three ``Address`` objects, the
result of the SQL statement would have had three rows; yet the :class:`_query.Query`
returns only one ``User`` object.  As additional rows are received for a
``User`` object just loaded in a previous row, the additional columns that
refer to new ``Address`` objects are directed into additional results within
the ``User.addresses`` collection of that particular object.

This process is very transparent, however does imply that joined eager
loading is incompatible with "batched" query results, provided by the
:meth:`_query.Query.yield_per` method, when used for collection loading.  Joined
eager loading used for scalar references is however compatible with
:meth:`_query.Query.yield_per`.  The :meth:`_query.Query.yield_per` method will result
in an exception thrown if a collection based joined eager loader is
in play.

To "batch" queries with arbitrarily large sets of result data while maintaining
compatibility with collection-based joined eager loading, emit multiple
SELECT statements, each referring to a subset of rows using the WHERE
clause, e.g. windowing.   Alternatively, consider using "select IN" eager loading
which is **potentially** compatible with :meth:`_query.Query.yield_per`, provided
that the database driver in use supports multiple, simultaneous cursors
(SQLite, PostgreSQL drivers, not MySQL drivers or SQL Server ODBC drivers).


.. _zen_of_eager_loading:

The Zen of Joined Eager Loading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since joined eager loading seems to have many resemblances to the use of
:meth:`_query.Query.join`, it often produces confusion as to when and how it should
be used.   It is critical to understand the distinction that while
:meth:`_query.Query.join` is used to alter the results of a query, :func:`_orm.joinedload`
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
:class:`_query.Query` would make it load a different set of primary or related
objects based on a change in loader strategy.

How :func:`joinedload` in particular achieves this result of not impacting
entity rows returned in any way is that it creates an anonymous alias of the
joins it adds to your query, so that they can't be referenced by other parts of
the query.   For example, the query below uses :func:`_orm.joinedload` to create a
LEFT OUTER JOIN from ``users`` to ``addresses``, however the ``ORDER BY`` added
against ``Address.email_address`` is not valid - the ``Address`` entity is not
named in the query:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}SELECT
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
    ['jack']

Above, ``ORDER BY addresses.email_address`` is not valid since ``addresses`` is not in the
FROM list.   The correct way to load the ``User`` records and order by email
address is to use :meth:`_query.Query.join`:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}
    SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    JOIN addresses ON users.id = addresses.user_id
    WHERE users.name = ?
    ORDER BY addresses.email_address
    ['jack']

The statement above is of course not the same as the previous one, in that the
columns from ``addresses`` are not included in the result at all.   We can add
:func:`_orm.joinedload` back in, so that there are two joins - one is that which we
are ordering on, the other is used anonymously to load the contents of the
``User.addresses`` collection:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}SELECT
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
    ['jack']

What we see above is that our usage of :meth:`_query.Query.join` is to supply JOIN
clauses we'd like to use in subsequent query criterion, whereas our usage of
:func:`_orm.joinedload` only concerns itself with the loading of the
``User.addresses`` collection, for each ``User`` in the result. In this case,
the two joins most probably appear redundant - which they are.  If we wanted to
use just one JOIN for collection loading as well as ordering, we use the
:func:`.contains_eager` option, described in :ref:`contains_eager` below.   But
to see why :func:`joinedload` does what it does, consider if we were
**filtering** on a particular ``Address``:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... filter(Address.email_address=='someaddress@foo.com').\
    ... all()
    {opensql}SELECT
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
    ['jack', 'someaddress@foo.com']

Above, we can see that the two JOINs have very different roles.  One will match
exactly one row, that of the join of ``User`` and ``Address`` where
``Address.email_address=='someaddress@foo.com'``. The other LEFT OUTER JOIN
will match *all* ``Address`` rows related to ``User``, and is only used to
populate the ``User.addresses`` collection, for those ``User`` objects that are
returned.

By changing the usage of :func:`_orm.joinedload` to another style of loading, we
can change how the collection is loaded completely independently of SQL used to
retrieve the actual ``User`` rows we want.  Below we change :func:`_orm.joinedload`
into :func:`.subqueryload`:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(subqueryload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... filter(Address.email_address=='someaddress@foo.com').\
    ... all()
    {opensql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    JOIN addresses ON users.id = addresses.user_id
    WHERE
        users.name = ?
        AND addresses.email_address = ?
    ['jack', 'someaddress@foo.com']

    # ... subqueryload() emits a SELECT in order
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

.. _subquery_eager_loading:

Subquery Eager Loading
----------------------

Subqueryload eager loading is configured in the same manner as that of
joined eager loading;  for the :paramref:`_orm.relationship.lazy` parameter,
we would specify ``"subquery"`` rather than ``"joined"``, and for
the option we use the :func:`.subqueryload` option rather than the
:func:`_orm.joinedload` option.

The operation of subquery eager loading is to emit a second SELECT statement
for each relationship to be loaded, across all result objects at once.
This SELECT statement refers to the original SELECT statement, wrapped
inside of a subquery, so that we retrieve the same list of primary keys
for the primary object being returned, then link that to the sum of all
the collection members to load them at once:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... options(subqueryload(User.addresses)).\
    ... filter_by(name='jack').all()
    {opensql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    WHERE users.name = ?
    ('jack',)
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
    ('jack',)

The subqueryload strategy has many advantages over joined eager loading
in the area of loading collections.   First, it allows the original query
to proceed without changing it at all, not introducing in particular a
LEFT OUTER JOIN that may make it less efficient.  Secondly, it allows
for many collections to be eagerly loaded without producing a single query
that has many JOINs in it, which can be even less efficient; each relationship
is loaded in a fully separate query.  Finally, because the additional query
only needs to load the collection items and not the lead object, it can
use an inner JOIN in all cases for greater query efficiency.

Disadvantages of subqueryload include that the complexity of the original
query is transferred to the relationship queries, which when combined with the
use of a subquery, can on some backends in some cases (notably MySQL) produce
significantly slow queries.   Additionally, the subqueryload strategy can only
load the full contents of all collections at once, is therefore incompatible
with "batched" loading supplied by :meth:`_query.Query.yield_per`, both for collection
and scalar relationships.

The newer style of loading provided by :func:`.selectinload` solves these
limitations of :func:`.subqueryload`.

.. seealso::

    :ref:`selectin_eager_loading`


.. _subqueryload_ordering:

The Importance of Ordering
^^^^^^^^^^^^^^^^^^^^^^^^^^

A query which makes use of :func:`.subqueryload` in conjunction with a
limiting modifier such as :meth:`_query.Query.first`, :meth:`_query.Query.limit`,
or :meth:`_query.Query.offset` should **always** include :meth:`_query.Query.order_by`
against unique column(s) such as the primary key, so that the additional queries
emitted by :func:`.subqueryload` include
the same ordering as used by the parent query.  Without it, there is a chance
that the inner query could return the wrong rows::

    # incorrect, no ORDER BY
    session.query(User).options(
        subqueryload(User.addresses)).first()

    # incorrect if User.name is not unique
    session.query(User).options(
        subqueryload(User.addresses)
    ).order_by(User.name).first()

    # correct
    session.query(User).options(
        subqueryload(User.addresses)
    ).order_by(User.name, User.id).first()

.. seealso::

    :ref:`faq_subqueryload_limit_sort` - detailed example

.. _selectin_eager_loading:

Select IN loading
-----------------

Select IN loading is similar in operation to subquery eager loading, however
the SELECT statement which is emitted has a much simpler structure than that of
subquery eager loading.  In most cases, selectin loading is the most simple and
efficient way to eagerly load collections of objects.  The only scenario in
which selectin eager loading is not feasible is when the model is using
composite primary keys, and the backend database does not support tuples with
IN, which currently includes SQL Server.

.. versionadded:: 1.2

"Select IN" eager loading is provided using the ``"selectin"`` argument to
:paramref:`_orm.relationship.lazy` or by using the :func:`.selectinload` loader
option.   This style of loading emits a SELECT that refers to the primary key
values of the parent object, or in the case of a many-to-one
relationship to the those of the child objects, inside of an IN clause, in
order to load related associations:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... options(selectinload(User.addresses)).\
    ... filter(or_(User.name == 'jack', User.name == 'ed')).all()
    {opensql}SELECT
        users.id AS users_id,
        users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM users
    WHERE users.name = ? OR users.name = ?
    ('jack', 'ed')
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
Because the relationship between ``User`` and ``Address`` has a simple [1]_
primary join condition and provides that the
primary key values for ``User`` can be derived from ``Address.user_id``, the
statement has no joins or subqueries at all.

.. versionchanged:: 1.3 selectin loading can omit the JOIN for a simple
   one-to-many collection.

For simple [1]_ many-to-one loads, a JOIN is also not needed as the foreign key
value from the parent object is used:

.. sourcecode:: python+sql

    >>> session.query(Address).\
    ... options(selectinload(Address.user)).all()
    {opensql}SELECT
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

.. versionchanged:: 1.3.6 selectin loading can also omit the JOIN for a simple
   many-to-one relationship.

.. [1] by "simple" we mean that the :paramref:`_orm.relationship.primaryjoin`
   condition expresses an equality comparison between the primary key of the
   "one" side and a straight foreign key of the "many" side, without any
   additional criteria.

Select IN loading also supports many-to-many relationships, where it currently
will JOIN across all three tables to match rows from one side to the other.

Things to know about this kind of loading include:

* The SELECT statement emitted by the "selectin" loader strategy, unlike
  that of "subquery", does not
  require a subquery nor does it inherit any of the performance limitations
  of the original query; the lookup is a simple primary key lookup and should
  have high performance.

* The special ordering requirements of subqueryload described at
  :ref:`subqueryload_ordering` also don't apply to selectin loading; selectin
  is always linking directly to a parent primary key and can't really
  return the wrong result.

* "selectin" loading, unlike joined or subquery eager loading, always emits its
  SELECT in terms of the immediate parent objects just loaded, and not the
  original type of object at the top of the chain.  So if eager loading many
  levels deep, "selectin" loading still will not require any JOINs for simple
  one-to-many or many-to-one relationships.   In comparison, joined and
  subquery eager loading always refer to multiple JOINs up to the original
  parent.

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

In general, "selectin" loading is probably superior to "subquery" eager loading
in most ways, save for the syntax requirement with composite primary keys
and possibly that it may emit many SELECT statements for larger result sets.
As always, developers should spend time looking at the
statements and results generated by their applications in development to
check that things are working efficiently.

.. _what_kind_of_loading:

What Kind of Loading to Use ?
-----------------------------

Which type of loading to use typically comes down to optimizing the tradeoff
between number of SQL executions, complexity of SQL emitted, and amount of
data fetched. Lets take two examples, a :func:`~sqlalchemy.orm.relationship`
which references a collection, and a :func:`~sqlalchemy.orm.relationship` that
references a scalar many-to-one reference.

* One to Many Collection

 * When using the default lazy loading, if you load 100 objects, and then access a collection on each of
   them, a total of 101 SQL statements will be emitted, although each statement will typically be a
   simple SELECT without any joins.

 * When using joined loading, the load of 100 objects and their collections will emit only one SQL
   statement.  However, the
   total number of rows fetched will be equal to the sum of the size of all the collections, plus one
   extra row for each parent object that has an empty collection.  Each row will also contain the full
   set of columns represented by the parents, repeated for each collection item - SQLAlchemy does not
   re-fetch these columns other than those of the primary key, however most DBAPIs (with some
   exceptions) will transmit the full data of each parent over the wire to the client connection in
   any case.  Therefore joined eager loading only makes sense when the size of the collections are
   relatively small.  The LEFT OUTER JOIN can also be performance intensive compared to an INNER join.

 * When using subquery loading, the load of 100 objects will
   emit two SQL statements.  The second statement will fetch a total number of
   rows equal to the sum of the size of all collections.  An INNER JOIN is
   used, and a minimum of parent columns are requested, only the primary keys.
   So a subquery load makes sense when the collections are larger.

 * When multiple levels of depth are used with joined or subquery loading, loading collections-within-
   collections will multiply the total number of rows fetched in a cartesian fashion.  Both
   joined and subquery eager loading always join from the original parent class; if loading a collection
   four levels deep, there will be four JOINs out to the parent.  selectin loading
   on the other hand will always have exactly one JOIN to the immediate
   parent table.

 * Using selectin loading, the load of 100 objects will also emit two SQL
   statements, the second of which refers to the 100 primary keys of the
   objects loaded.   selectin loading will however render at most 500 primary
   key values into a single SELECT statement; so for a lead collection larger
   than 500, there will be a SELECT statement emitted for each batch of
   500 objects selected.

 * Using multiple levels of depth with selectin loading does not incur the
   "cartesian" issue that joined and subquery eager loading have; the queries
   for selectin loading have the best performance characteristics and the
   fewest number of rows.  The only caveat is that there might be more than
   one SELECT emitted depending on the size of the lead result.

 * selectin loading, unlike joined (when using collections) and subquery eager
   loading (all kinds of relationships), is potentially compatible with result
   set batching provided by :meth:`_query.Query.yield_per` assuming an appropriate
   database driver, so may be able to allow batching for large result sets.

* Many to One Reference

 * When using the default lazy loading, a load of 100 objects will like in the case of the collection
   emit as many as 101 SQL statements.  However - there is a significant exception to this, in that
   if the many-to-one reference is a simple foreign key reference to the target's primary key, each
   reference will be checked first in the current identity map using :meth:`_query.Query.get`.  So here,
   if the collection of objects references a relatively small set of target objects, or the full set
   of possible target objects have already been loaded into the session and are strongly referenced,
   using the default of `lazy='select'` is by far the most efficient way to go.

 * When using joined loading, the load of 100 objects will emit only one SQL statement.   The join
   will be a LEFT OUTER JOIN, and the total number of rows will be equal to 100 in all cases.
   If you know that each parent definitely has a child (i.e. the foreign
   key reference is NOT NULL), the joined load can be configured with
   :paramref:`_orm.relationship.innerjoin` set to ``True``, which is
   usually specified within the :func:`~sqlalchemy.orm.relationship`.   For a load of objects where
   there are many possible target references which may have not been loaded already, joined loading
   with an INNER JOIN is extremely efficient.

 * Subquery loading will issue a second load for all the child objects, so for a load of 100 objects
   there would be two SQL statements emitted.  There's probably not much advantage here over
   joined loading, however, except perhaps that subquery loading can use an INNER JOIN in all cases
   whereas joined loading requires that the foreign key is NOT NULL.

 * Selectin loading will also issue a second load for all the child objects (and as
   stated before, for larger results it will emit a SELECT per 500 rows), so for a load of 100 objects
   there would be two SQL statements emitted.  The query itself still has to
   JOIN to the parent table, so again there's not too much advantage to
   selectin loading for many-to-one vs. joined eager loading save for the
   use of INNER JOIN in all cases.

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
specified in the :class:`_query.Query`.   This feature is available by passing
the string ``'*'`` as the argument to any of these options::

    session.query(MyClass).options(lazyload('*'))

Above, the ``lazyload('*')`` option will supersede the ``lazy`` setting
of all :func:`_orm.relationship` constructs in use for that query,
except for those which use the ``'dynamic'`` style of loading.
If some relationships specify
``lazy='joined'`` or ``lazy='subquery'``, for example,
using ``lazyload('*')`` will unilaterally
cause all those relationships to use ``'select'`` loading, e.g. emit a
SELECT statement when each attribute is accessed.

The option does not supersede loader options stated in the
query, such as :func:`.eagerload`,
:func:`.subqueryload`, etc.  The query below will still use joined loading
for the ``widget`` relationship::

    session.query(MyClass).options(
        lazyload('*'),
        joinedload(MyClass.widget)
    )

If multiple ``'*'`` options are passed, the last one overrides
those previously passed.

Per-Entity Wildcard Loading Strategies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A variant of the wildcard loader strategy is the ability to set the strategy
on a per-entity basis.  For example, if querying for ``User`` and ``Address``,
we can instruct all relationships on ``Address`` only to use lazy loading
by first applying the :class:`_orm.Load` object, then specifying the ``*`` as a
chained option::

    session.query(User, Address).options(
        Load(Address).lazyload('*'))

Above, all relationships on ``Address`` will be set to a lazy load.

.. _joinedload_and_join:

.. _contains_eager:

Routing Explicit Joins/Statements into Eagerly Loaded Collections
-----------------------------------------------------------------

The behavior of :func:`~sqlalchemy.orm.joinedload()` is such that joins are
created automatically, using anonymous aliases as targets, the results of which
are routed into collections and
scalar references on loaded objects. It is often the case that a query already
includes the necessary joins which represent a particular collection or scalar
reference, and the joins added by the joinedload feature are redundant - yet
you'd still like the collections/references to be populated.

For this SQLAlchemy supplies the :func:`~sqlalchemy.orm.contains_eager()`
option. This option is used in the same manner as the
:func:`~sqlalchemy.orm.joinedload()` option except it is assumed that the
:class:`~sqlalchemy.orm.query.Query` will specify the appropriate joins
explicitly. Below, we specify a join between ``User`` and ``Address``
and additionally establish this as the basis for eager loading of ``User.addresses``::

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        addresses = relationship("Address")

    class Address(Base):
        __tablename__ = 'address'

        # ...

    q = session.query(User).join(User.addresses).\
                options(contains_eager(User.addresses))


If the "eager" portion of the statement is "aliased", the path
should be specified using :meth:`.PropComparator.of_type`, which allows
the specific :func:`_orm.aliased` construct to be passed:

.. sourcecode:: python+sql

    # use an alias of the Address entity
    adalias = aliased(Address)

    # construct a Query object which expects the "addresses" results
    query = session.query(User).\
        outerjoin(User.addresses.of_type(adalias)).\
        options(contains_eager(User.addresses.of_type(adalias)))

    # get results normally
    r = query.all()
    {opensql}SELECT
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

    query(User).options(
        contains_eager(User.orders).
        contains_eager(Order.items))

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
:meth:`_query.Query.populate_existing` to ensure any already-loaded collections
are overwritten::

    q = session.query(User).\
            join(User.addresses).\
            filter(Address.email_address.like('%@aol.com')).\
            options(contains_eager(User.addresses)).\
            populate_existing()

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
   :meth:`_query.Query.populate_existing` as illustrated above so that an
   already-loaded collection is refreshed with the new data.
   :meth:`_query.Query.populate_existing` will reset **all** attributes that were
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

Creating Custom Load Rules
--------------------------

.. deepalchemy::  This is an advanced technique!   Great care and testing
   should be applied.

The ORM has various edge cases where the value of an attribute is locally
available, however the ORM itself doesn't have awareness of this.   There
are also cases when a user-defined system of loading attributes is desirable.
To support the use case of user-defined loading systems, a key function
:func:`.attributes.set_committed_value` is provided.   This function is
basically equivalent to Python's own ``setattr()`` function, except that
when applied to a target object, SQLAlchemy's "attribute history" system
which is used to determine flush-time changes is bypassed; the attribute
is assigned in the same way as if the ORM loaded it that way from the database.

The use of :func:`.attributes.set_committed_value` can be combined with another
key event known as :meth:`.InstanceEvents.load` to produce attribute-population
behaviors when an object is loaded.   One such example is the bi-directional
"one-to-one" case, where loading the "many-to-one" side of a one-to-one
should also imply the value of the "one-to-many" side.  The SQLAlchemy ORM
does not consider backrefs when loading related objects, and it views a
"one-to-one" as just another "one-to-many", that just happens to be one
row.

Given the following mapping::

    from sqlalchemy import Integer, ForeignKey, Column
    from sqlalchemy.orm import relationship, backref
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()


    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        b_id = Column(ForeignKey('b.id'))
        b = relationship(
            "B",
            backref=backref("a", uselist=False),
            lazy='joined')


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)


If we query for an ``A`` row, and then ask it for ``a.b.a``, we will get
an extra SELECT::

    >>> a1.b.a
    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE ? = a.b_id

This SELECT is redundant because ``b.a`` is the same value as ``a1``.  We
can create an on-load rule to populate this for us::

    from sqlalchemy import event
    from sqlalchemy.orm import attributes

    @event.listens_for(A, "load")
    def load_b(target, context):
        if 'b' in target.__dict__:
            attributes.set_committed_value(target.b, 'a', target)

Now when we query for ``A``, we will get ``A.b`` from the joined eager load,
and ``A.b.a`` from our event:

.. sourcecode:: pycon+sql

    a1 = s.query(A).first()
    {opensql}SELECT
        a.id AS a_id,
        a.b_id AS a_b_id,
        b_1.id AS b_1_id
    FROM a
    LEFT OUTER JOIN b AS b_1 ON b_1.id = a.b_id
     LIMIT ? OFFSET ?
    (1, 0)
    {stop}assert a1.b.a is a1


Relationship Loader API
-----------------------

.. autofunction:: contains_eager

.. autofunction:: defaultload

.. autofunction:: eagerload

.. autofunction:: immediateload

.. autofunction:: joinedload

.. autofunction:: lazyload

.. autoclass:: Load
    :members:

.. autofunction:: noload

.. autofunction:: raiseload

.. autofunction:: selectinload

.. autofunction:: subqueryload

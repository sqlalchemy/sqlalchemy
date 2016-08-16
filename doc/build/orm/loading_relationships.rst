.. _loading_toplevel:

.. currentmodule:: sqlalchemy.orm

Relationship Loading Techniques
===============================

A big part of SQLAlchemy is providing a wide range of control over how related objects get loaded when querying.   This behavior
can be configured at mapper construction time using the ``lazy`` parameter to the :func:`.relationship` function,
as well as by using options with the :class:`.Query` object.

Using Loader Strategies: Lazy Loading, Eager Loading
----------------------------------------------------

By default, all inter-object relationships are **lazy loading**. The scalar or
collection attribute associated with a :func:`~sqlalchemy.orm.relationship`
contains a trigger which fires the first time the attribute is accessed.  This
trigger, in all but one case, issues a SQL call at the point of access
in order to load the related object or objects:

.. sourcecode:: python+sql

    {sql}>>> jack.addresses
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address,
    addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id
    [5]
    {stop}[<Address(u'jack@google.com')>, <Address(u'j25@yahoo.com')>]

The one case where SQL is not emitted is for a simple many-to-one relationship, when
the related object can be identified by its primary key alone and that object is already
present in the current :class:`.Session`.

This default behavior of "load upon attribute access" is known as "lazy" or
"select" loading - the name "select" because a "SELECT" statement is typically emitted
when the attribute is first accessed.

In the :ref:`ormtutorial_toplevel`, we introduced the concept of **Eager
Loading**. We used an ``option`` in conjunction with the
:class:`~sqlalchemy.orm.query.Query` object in order to indicate that a
relationship should be loaded at the same time as the parent, within a single
SQL query.   This option, known as :func:`.joinedload`, connects a JOIN (by default
a LEFT OUTER join) to the statement and populates the scalar/collection from the
same result set as that of the parent:

.. sourcecode:: python+sql

    {sql}>>> jack = session.query(User).\
    ... options(joinedload('addresses')).\
    ... filter_by(name='jack').all() #doctest: +NORMALIZE_WHITESPACE
    SELECT addresses_1.id AS addresses_1_id, addresses_1.email_address AS addresses_1_email_address,
    addresses_1.user_id AS addresses_1_user_id, users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ?
    ['jack']


In addition to "joined eager loading", a second option for eager loading
exists, called "subquery eager loading". This kind of eager loading emits an
additional SQL statement for each collection requested, aggregated across all
parent objects:

.. sourcecode:: python+sql

    {sql}>>> jack = session.query(User).\
    ... options(subqueryload('addresses')).\
    ... filter_by(name='jack').all()
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname,
    users.password AS users_password
    FROM users
    WHERE users.name = ?
    ('jack',)
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address,
    addresses.user_id AS addresses_user_id, anon_1.users_id AS anon_1_users_id
    FROM (SELECT users.id AS users_id
    FROM users
    WHERE users.name = ?) AS anon_1 JOIN addresses ON anon_1.users_id = addresses.user_id
    ORDER BY anon_1.users_id, addresses.id
    ('jack',)

The default **loader strategy** for any :func:`~sqlalchemy.orm.relationship`
is configured by the ``lazy`` keyword argument, which defaults to ``select`` - this indicates
a "select" statement .
Below we set it as ``joined`` so that the ``children`` relationship is eager
loaded using a JOIN::

    # load the 'children' collection using LEFT OUTER JOIN
    class Parent(Base):
        __tablename__ = 'parent'

        id = Column(Integer, primary_key=True)
        children = relationship("Child", lazy='joined')

We can also set it to eagerly load using a second query for all collections,
using ``subquery``::

    # load the 'children' collection using a second query which
    # JOINS to a subquery of the original
    class Parent(Base):
        __tablename__ = 'parent'

        id = Column(Integer, primary_key=True)
        children = relationship("Child", lazy='subquery')

When querying, all three choices of loader strategy are available on a
per-query basis, using the :func:`~sqlalchemy.orm.joinedload`,
:func:`~sqlalchemy.orm.subqueryload` and :func:`~sqlalchemy.orm.lazyload`
query options:

.. sourcecode:: python+sql

    # set children to load lazily
    session.query(Parent).options(lazyload('children')).all()

    # set children to load eagerly with a join
    session.query(Parent).options(joinedload('children')).all()

    # set children to load eagerly with a second statement
    session.query(Parent).options(subqueryload('children')).all()

.. _subqueryload_ordering:

The Importance of Ordering
--------------------------

A query which makes use of :func:`.subqueryload` in conjunction with a
limiting modifier such as :meth:`.Query.first`, :meth:`.Query.limit`,
or :meth:`.Query.offset` should **always** include :meth:`.Query.order_by`
against unique column(s) such as the primary key, so that the additional queries
emitted by :func:`.subqueryload` include
the same ordering as used by the parent query.  Without it, there is a chance
that the inner query could return the wrong rows::

    # incorrect, no ORDER BY
    session.query(User).options(subqueryload(User.addresses)).first()

    # incorrect if User.name is not unique
    session.query(User).options(subqueryload(User.addresses)).order_by(User.name).first()

    # correct
    session.query(User).options(subqueryload(User.addresses)).order_by(User.name, User.id).first()

.. seealso::

    :ref:`faq_subqueryload_limit_sort` - detailed example

Loading Along Paths
-------------------

To reference a relationship that is deeper than one level, method chaining
may be used.  The object returned by all loader options is an instance of
the :class:`.Load` class, which provides a so-called "generative" interface::

    session.query(Parent).options(
                                joinedload('foo').
                                    joinedload('bar').
                                    joinedload('bat')
                                ).all()

Using method chaining, the loader style of each link in the path is explicitly
stated.  To navigate along a path without changing the existing loader style
of a particular attribute, the :func:`.defaultload` method/function may be used::

    session.query(A).options(
                        defaultload("atob").joinedload("btoc")
                    ).all()

.. versionchanged:: 0.9.0
    The previous approach of specifying dot-separated paths within loader
    options has been superseded by the less ambiguous approach of the
    :class:`.Load` object and related methods.   With this system, the user
    specifies the style of loading for each link along the chain explicitly,
    rather than guessing between options like ``joinedload()`` vs. ``joinedload_all()``.
    The :func:`.orm.defaultload` is provided to allow path navigation without
    modification of existing loader options.   The dot-separated path system
    as well as the ``_all()`` functions will remain available for backwards-
    compatibility indefinitely.

Polymorphic Eager Loading
-------------------------

Specification of polymorpic options on a per-eager-load basis is supported.
See the section :ref:`eagerloading_polymorphic_subtypes` for examples
of the :meth:`.PropComparator.of_type` method in conjunction with the 
:func:`.orm.with_polymorphic` function.

Default Loading Strategies
--------------------------

.. versionadded:: 0.7.5
    Default loader strategies as a new feature.

Each of :func:`.joinedload`, :func:`.subqueryload`, :func:`.lazyload`,
:func:`.noload`, and :func:`.raiseload` can be used to set the default
style of :func:`.relationship` loading
for a particular query, affecting all :func:`.relationship` -mapped
attributes not otherwise
specified in the :class:`.Query`.   This feature is available by passing
the string ``'*'`` as the argument to any of these options::

    session.query(MyClass).options(lazyload('*'))

Above, the ``lazyload('*')`` option will supersede the ``lazy`` setting
of all :func:`.relationship` constructs in use for that query,
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

Per-Entity Default Loading Strategies
-------------------------------------

.. versionadded:: 0.9.0
    Per-entity default loader strategies.

A variant of the default loader strategy is the ability to set the strategy
on a per-entity basis.  For example, if querying for ``User`` and ``Address``,
we can instruct all relationships on ``Address`` only to use lazy loading
by first applying the :class:`.Load` object, then specifying the ``*`` as a
chained option::

    session.query(User, Address).options(Load(Address).lazyload('*'))

Above, all relationships on ``Address`` will be set to a lazy load.

.. _zen_of_eager_loading:

The Zen of Eager Loading
-------------------------

The philosophy behind loader strategies is that any set of loading schemes can be
applied to a particular query, and *the results don't change* - only the number
of SQL statements required to fully load related objects and collections changes. A particular
query might start out using all lazy loads.   After using it in context, it might be revealed
that particular attributes or collections are always accessed, and that it would be more
efficient to change the loader strategy for these.   The strategy can be changed with no other
modifications to the query, the results will remain identical, but fewer SQL statements would be emitted.
In theory (and pretty much in practice), nothing you can do to the :class:`.Query` would make it load
a different set of primary or related objects based on a change in loader strategy.

How :func:`joinedload` in particular achieves this result of not impacting
entity rows returned in any way is that it creates an anonymous alias of the joins it adds to your
query, so that they can't be referenced by other parts of the query.   For example,
the query below uses :func:`.joinedload` to create a LEFT OUTER JOIN from ``users``
to ``addresses``, however the ``ORDER BY`` added against ``Address.email_address``
is not valid - the ``Address`` entity is not named in the query:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}SELECT addresses_1.id AS addresses_1_id, addresses_1.email_address AS addresses_1_email_address,
    addresses_1.user_id AS addresses_1_user_id, users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ? ORDER BY addresses.email_address   <-- this part is wrong !
    ['jack']

Above, ``ORDER BY addresses.email_address`` is not valid since ``addresses`` is not in the
FROM list.   The correct way to load the ``User`` records and order by email
address is to use :meth:`.Query.join`:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}
    SELECT users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    WHERE users.name = ? ORDER BY addresses.email_address
    ['jack']

The statement above is of course not the same as the previous one, in that the columns from ``addresses``
are not included in the result at all.   We can add :func:`.joinedload` back in, so that
there are two joins - one is that which we are ordering on, the other is used anonymously to
load the contents of the ``User.addresses`` collection:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... order_by(Address.email_address).all()
    {opensql}SELECT addresses_1.id AS addresses_1_id, addresses_1.email_address AS addresses_1_email_address,
    addresses_1.user_id AS addresses_1_user_id, users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ? ORDER BY addresses.email_address
    ['jack']

What we see above is that our usage of :meth:`.Query.join` is to supply JOIN clauses we'd like
to use in subsequent query criterion, whereas our usage of :func:`.joinedload` only concerns
itself with the loading of the ``User.addresses`` collection, for each ``User`` in the result.
In this case, the two joins most probably appear redundant - which they are.  If we
wanted to use just one JOIN for collection loading as well as ordering, we use the
:func:`.contains_eager` option, described in :ref:`contains_eager` below.   But
to see why :func:`joinedload` does what it does, consider if we were **filtering** on a
particular ``Address``:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(joinedload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... filter(Address.email_address=='someaddress@foo.com').\
    ... all()
    {opensql}SELECT addresses_1.id AS addresses_1_id, addresses_1.email_address AS addresses_1_email_address,
    addresses_1.user_id AS addresses_1_user_id, users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ? AND addresses.email_address = ?
    ['jack', 'someaddress@foo.com']

Above, we can see that the two JOINs have very different roles.  One will match exactly
one row, that of the join of ``User`` and ``Address`` where ``Address.email_address=='someaddress@foo.com'``.
The other LEFT OUTER JOIN will match *all* ``Address`` rows related to ``User``,
and is only used to populate the ``User.addresses`` collection, for those ``User`` objects
that are returned.

By changing the usage of :func:`.joinedload` to another style of loading, we can change
how the collection is loaded completely independently of SQL used to retrieve
the actual ``User`` rows we want.  Below we change :func:`.joinedload` into
:func:`.subqueryload`:

.. sourcecode:: python+sql

    >>> jack = session.query(User).\
    ... join(User.addresses).\
    ... options(subqueryload(User.addresses)).\
    ... filter(User.name=='jack').\
    ... filter(Address.email_address=='someaddress@foo.com').\
    ... all()
    {opensql}SELECT users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    WHERE users.name = ? AND addresses.email_address = ?
    ['jack', 'someaddress@foo.com']

    # ... subqueryload() emits a SELECT in order
    # to load all address records ...

When using joined eager loading, if the
query contains a modifier that impacts the rows returned
externally to the joins, such as when using DISTINCT, LIMIT, OFFSET
or equivalent, the completed statement is first
wrapped inside a subquery, and the joins used specifically for joined eager
loading are applied to the subquery.   SQLAlchemy's
joined eager loading goes the extra mile, and then ten miles further, to
absolutely ensure that it does not affect the end result of the query, only
the way collections and related objects are loaded, no matter what the format of the query is.

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

 * When using subquery loading, the load of 100 objects will emit two SQL statements.  The second
   statement will fetch a total number of rows equal to the sum of the size of all collections.  An
   INNER JOIN is used, and a minimum of parent columns are requested, only the primary keys.  So a
   subquery load makes sense when the collections are larger.

 * When multiple levels of depth are used with joined or subquery loading, loading collections-within-
   collections will multiply the total number of rows fetched in a cartesian fashion.  Both forms
   of eager loading always join from the original parent class.

* Many to One Reference

 * When using the default lazy loading, a load of 100 objects will like in the case of the collection
   emit as many as 101 SQL statements.  However - there is a significant exception to this, in that
   if the many-to-one reference is a simple foreign key reference to the target's primary key, each
   reference will be checked first in the current identity map using :meth:`.Query.get`.  So here,
   if the collection of objects references a relatively small set of target objects, or the full set
   of possible target objects have already been loaded into the session and are strongly referenced,
   using the default of `lazy='select'` is by far the most efficient way to go.

 * When using joined loading, the load of 100 objects will emit only one SQL statement.   The join
   will be a LEFT OUTER JOIN, and the total number of rows will be equal to 100 in all cases.
   If you know that each parent definitely has a child (i.e. the foreign
   key reference is NOT NULL), the joined load can be configured with
   :paramref:`~.relationship.innerjoin` set to ``True``, which is
   usually specified within the :func:`~sqlalchemy.orm.relationship`.   For a load of objects where
   there are many possible target references which may have not been loaded already, joined loading
   with an INNER JOIN is extremely efficient.

 * Subquery loading will issue a second load for all the child objects, so for a load of 100 objects
   there would be two SQL statements emitted.  There's probably not much advantage here over
   joined loading, however, except perhaps that subquery loading can use an INNER JOIN in all cases
   whereas joined loading requires that the foreign key is NOT NULL.

.. _joinedload_and_join:

.. _contains_eager:

Routing Explicit Joins/Statements into Eagerly Loaded Collections
------------------------------------------------------------------

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


If the "eager" portion of the statement is "aliased", the ``alias`` keyword
argument to :func:`~sqlalchemy.orm.contains_eager` may be used to indicate it.
This is sent as a reference to an :func:`.aliased` or :class:`.Alias`
construct:

.. sourcecode:: python+sql

    # use an alias of the Address entity
    adalias = aliased(Address)

    # construct a Query object which expects the "addresses" results
    query = session.query(User).\
        outerjoin(adalias, User.addresses).\
        options(contains_eager(User.addresses, alias=adalias))

    # get results normally
    {sql}r = query.all()
    SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, adalias.address_id AS adalias_address_id,
    adalias.user_id AS adalias_user_id, adalias.email_address AS adalias_email_address, (...other columns...)
    FROM users LEFT OUTER JOIN email_addresses AS email_addresses_1 ON users.user_id = email_addresses_1.user_id

The path given as the argument to :func:`.contains_eager` needs
to be a full path from the starting entity. For example if we were loading
``Users->orders->Order->items->Item``, the string version would look like::

    query(User).options(contains_eager('orders').contains_eager('items'))

Or using the class-bound descriptor::

    query(User).options(contains_eager(User.orders).contains_eager(Order.items))

Using contains_eager() to load a custom-filtered collection result
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When we use :func:`.contains_eager`, *we* are constructing ourselves the
SQL that will be used to populate collections.  From this, it naturally follows
that we can opt to **modify** what values the collection is intended to store,
by writing our SQL to load a subset of elements for collections or
scalar attributes.

As an example, we can load a ``User`` object and eagerly load only particular
addresses into its ``.addresses`` collection just by filtering::

    q = session.query(User).join(User.addresses).\
                filter(Address.email.like('%ed%')).\
                options(contains_eager(User.addresses))

The above query will load only ``User`` objects which contain at
least ``Address`` object that contains the substring ``'ed'`` in its
``email`` field; the ``User.addresses`` collection will contain **only**
these ``Address`` entries, and *not* any other ``Address`` entries that are
in fact associated with the collection.

.. warning::

    Keep in mind that when we load only a subset of objects into a collection,
    that collection no longer represents what's actually in the database.  If
    we attempted to add entries to this collection, we might find ourselves
    conflicting with entries that are already in the database but not locally
    loaded.

    In addition, the **collection will fully reload normally** once the
    object or attribute is expired.  This expiration occurs whenever the
    :meth:`.Session.commit`, :meth:`.Session.rollback` methods are used
    assuming default session settings, or the :meth:`.Session.expire_all`
    or :meth:`.Session.expire` methods are used.

    For these reasons, prefer returning separate fields in a tuple rather
    than artificially altering a collection, when an object plus a custom
    set of related objects is desired::

        q = session.query(User, Address).join(User.addresses).\
                    filter(Address.email.like('%ed%'))


Advanced Usage with Arbitrary Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``alias`` argument can be more creatively used, in that it can be made
to represent any set of arbitrary names to match up into a statement.
Below it is linked to a :func:`.select` which links a set of column objects
to a string SQL statement::

    # label the columns of the addresses table
    eager_columns = select([
                        addresses.c.address_id.label('a1'),
                        addresses.c.email_address.label('a2'),
                        addresses.c.user_id.label('a3')])

    # select from a raw SQL statement which uses those label names for the
    # addresses table.  contains_eager() matches them up.
    query = session.query(User).\
        from_statement("select users.*, addresses.address_id as a1, "
                "addresses.email_address as a2, addresses.user_id as a3 "
                "from users left outer join addresses on users.user_id=addresses.user_id").\
        options(contains_eager(User.addresses, alias=eager_columns))

Creating Custom Load Rules
---------------------------

.. warning::  This is an advanced technique!   Great care and testing
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
        b = relationship("B", backref=backref("a", uselist=False), lazy='joined')


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

    {sql}a1 = s.query(A).first()
    SELECT a.id AS a_id, a.b_id AS a_b_id, b_1.id AS b_1_id
    FROM a LEFT OUTER JOIN b AS b_1 ON b_1.id = a.b_id
     LIMIT ? OFFSET ?
    (1, 0)
    {stop}assert a1.b.a is a1


Relationship Loader API
------------------------

.. autofunction:: contains_alias

.. autofunction:: contains_eager

.. autofunction:: defaultload

.. autofunction:: eagerload

.. autofunction:: eagerload_all

.. autofunction:: immediateload

.. autofunction:: joinedload

.. autofunction:: joinedload_all

.. autofunction:: lazyload

.. autofunction:: noload

.. autofunction:: raiseload

.. autofunction:: subqueryload

.. autofunction:: subqueryload_all

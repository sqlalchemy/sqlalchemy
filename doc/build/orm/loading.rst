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
loading, using a join:

.. sourcecode:: python+sql

    # load the 'children' collection using LEFT OUTER JOIN
    mapper(Parent, parent_table, properties={
        'children': relationship(Child, lazy='joined')
    })

We can also set it to eagerly load using a second query for all collections,
using ``subquery``:

.. sourcecode:: python+sql

    # load the 'children' attribute using a join to a subquery
    mapper(Parent, parent_table, properties={
        'children': relationship(Child, lazy='subquery')
    })

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

To reference a relationship that is deeper than one level, separate the names by periods:

.. sourcecode:: python+sql

    session.query(Parent).options(joinedload('foo.bar.bat')).all()

When using dot-separated names with :func:`~sqlalchemy.orm.joinedload` or
:func:`~sqlalchemy.orm.subqueryload`, the option applies **only** to the actual
attribute named, and **not** its ancestors. For example, suppose a mapping
from ``A`` to ``B`` to ``C``, where the relationships, named ``atob`` and
``btoc``, are both lazy-loading. A statement like the following:

.. sourcecode:: python+sql

    session.query(A).options(joinedload('atob.btoc')).all()

will load only ``A`` objects to start. When the ``atob`` attribute on each
``A`` is accessed, the returned ``B`` objects will *eagerly* load their ``C``
objects.

Therefore, to modify the eager load to load both ``atob`` as well as ``btoc``,
place joinedloads for both:

.. sourcecode:: python+sql

    session.query(A).options(joinedload('atob'), joinedload('atob.btoc')).all()

or more succinctly just use :func:`~sqlalchemy.orm.joinedload_all` or
:func:`~sqlalchemy.orm.subqueryload_all`:

.. sourcecode:: python+sql

    session.query(A).options(joinedload_all('atob.btoc')).all()

There are two other loader strategies available, **dynamic loading** and **no
loading**; these are described in :ref:`largecollections`.

Default Loading Strategies
--------------------------

.. note::

   Default loader strategies are a new feature as of version 0.7.5.

Each of :func:`.joinedload`, :func:`.subqueryload`, :func:`.lazyload`, 
and :func:`.noload` can be used to set the default style of
:func:`.relationship` loading 
for a particular query, affecting all :func:`.relationship` -mapped
attributes not otherwise
specified in the :class:`.Query`.   This feature is available by passing
the string ``'*'`` as the argument to any of these options::

    session.query(MyClass).options(lazyload('*'))

Above, the ``lazyload('*')`` option will supercede the ``lazy`` setting
of all :func:`.relationship` constructs in use for that query,
except for those which use the ``'dynamic'`` style of loading.   
If some relationships specify
``lazy='joined'`` or ``lazy='subquery'``, for example,
using ``default_strategy(lazy='select')`` will unilaterally
cause all those relationships to use ``'select'`` loading.

The option does not supercede loader options stated in the
query, such as :func:`.eagerload`, 
:func:`.subqueryload`, etc.  The query below will still use joined loading
for the ``widget`` relationship::

    session.query(MyClass).options(
                                lazyload('*'), 
                                joinedload(MyClass.widget)
                            )

If multiple ``'*'`` options are passed, the last one overrides
those previously passed.

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
   key reference is NOT NULL), the joined load can be configured with ``innerjoin=True``, which is
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
explicitly. Below it's used with a ``from_statement`` load::

    # mapping is the users->addresses mapping
    mapper(User, users_table, properties={
        'addresses': relationship(Address, addresses_table)
    })

    # define a query on USERS with an outer join to ADDRESSES
    statement = users_table.outerjoin(addresses_table).select().apply_labels()

    # construct a Query object which expects the "addresses" results
    query = session.query(User).options(contains_eager('addresses'))

    # get results normally
    r = query.from_statement(statement)

It works just as well with an inline :meth:`.Query.join` or
:meth:`.Query.outerjoin`::

    session.query(User).outerjoin(User.addresses).options(contains_eager(User.addresses)).all()

If the "eager" portion of the statement is "aliased", the ``alias`` keyword
argument to :func:`~sqlalchemy.orm.contains_eager` may be used to indicate it.
This is a string alias name or reference to an actual
:class:`~sqlalchemy.sql.expression.Alias` (or other selectable) object:

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

The ``alias`` argument is used only as a source of columns to match up to the
result set. You can use it to match up the result to arbitrary label
names in a string SQL statement, by passing a :func:`.select` which links those
labels to the mapped :class:`.Table`::

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

The path given as the argument to :func:`.contains_eager` needs
to be a full path from the starting entity. For example if we were loading
``Users->orders->Order->items->Item``, the string version would look like::

    query(User).options(contains_eager('orders', 'items'))

Or using the class-bound descriptor::

    query(User).options(contains_eager(User.orders, Order.items))


Relation Loader API
--------------------

.. autofunction:: contains_alias

.. autofunction:: contains_eager

.. autofunction:: eagerload

.. autofunction:: eagerload_all

.. autofunction:: immediateload

.. autofunction:: joinedload

.. autofunction:: joinedload_all

.. autofunction:: lazyload

.. autofunction:: noload

.. autofunction:: subqueryload

.. autofunction:: subqueryload_all

.. highlight:: pycon+sql

.. _queryguide_toplevel:

==================
ORM Querying Guide
==================

This section provides an overview of emitting queries with the
SQLAlchemy ORM using :term:`2.0 style` usage.

Readers of this section should be familiar with the SQLAlchemy overview
at :ref:`unified_tutorial`, and in particular most of the content here expands
upon the content at :ref:`tutorial_selecting_data`.


..  Setup code, not for display

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    >>> from sqlalchemy import MetaData, Table, Column, Integer, String
    >>> metadata = MetaData()
    >>> user_table = Table(
    ...     "user_account",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String(30)),
    ...     Column('fullname', String)
    ... )
    >>> from sqlalchemy import ForeignKey
    >>> address_table = Table(
    ...     "address",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('user_id', None, ForeignKey('user_account.id')),
    ...     Column('email_address', String, nullable=False)
    ... )
    >>> orders_table = Table(
    ...     "user_order",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('user_id', None, ForeignKey('user_account.id')),
    ...     Column('email_address', String, nullable=False)
    ... )
    >>> order_items_table = Table(
    ...     "order_items",
    ...     metadata,
    ...     Column("order_id", ForeignKey("user_order.id"), primary_key=True),
    ...     Column("item_id", ForeignKey("item.id"), primary_key=True)
    ... )
    >>> items_table = Table(
    ...     "item",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ...     Column('description', String)
    ... )
    >>> metadata.create_all(engine)
    BEGIN (implicit)
    ...
    >>> from sqlalchemy.orm import declarative_base
    >>> Base = declarative_base()
    >>> from sqlalchemy.orm import relationship
    >>> class User(Base):
    ...     __table__ = user_table
    ...
    ...     addresses = relationship("Address", back_populates="user")
    ...     orders = relationship("Order")
    ...
    ...     def __repr__(self):
    ...        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

    >>> class Address(Base):
    ...     __table__ = address_table
    ...
    ...     user = relationship("User", back_populates="addresses")
    ...
    ...     def __repr__(self):
    ...         return f"Address(id={self.id!r}, email_address={self.email_address!r})"

    >>> class Order(Base):
    ...     __table__ = orders_table
    ...     items = relationship("Item", secondary=order_items_table)

    >>> class Item(Base):
    ...     __table__ = items_table

    >>> conn = engine.connect()
    >>> from sqlalchemy.orm import Session
    >>> session = Session(conn)
    >>> session.add_all([
    ... User(name="spongebob", fullname="Spongebob Squarepants", addresses=[
    ...    Address(email_address="spongebob@sqlalchemy.org")
    ... ]),
    ... User(name="sandy", fullname="Sandy Cheeks", addresses=[
    ...    Address(email_address="sandy@sqlalchemy.org"),
    ...     Address(email_address="squirrel@squirrelpower.org")
    ...     ]),
    ...     User(name="patrick", fullname="Patrick Star", addresses=[
    ...         Address(email_address="pat999@aol.com")
    ...     ]),
    ...     User(name="squidward", fullname="Squidward Tentacles", addresses=[
    ...         Address(email_address="stentcl@sqlalchemy.org")
    ...     ]),
    ...     User(name="ehkrabs", fullname="Eugene H. Krabs"),
    ... ])
    >>> session.commit()
    BEGIN ...
    >>> conn.begin()
    BEGIN ...


SELECT statements
=================

SELECT statements are produced by the :func:`_sql.select` function which
returns a :class:`_sql.Select` object::

    >>> from sqlalchemy import select
    >>> stmt = select(User).where(User.name == 'spongebob')

To invoke a :class:`_sql.Select` with the ORM, it is passed to
:meth:`_orm.Session.execute`::

    {sql}>>> result = session.execute(stmt)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('spongebob',){stop}
    >>> for user_obj in result.scalars():
    ...     print(f"{user_obj.name} {user_obj.fullname}")
    spongebob Spongebob Squarepants


.. _orm_queryguide_select_columns:

Selecting ORM Entities and Attributes
--------------------------------------

The :func:`_sql.select` construct accepts ORM entities, including mapped
classes as well as class-level attributes representing mapped columns, which
are converted into ORM-annotated :class:`_sql.FromClause` and
:class:`_sql.ColumnElement` elements at construction time.

A :class:`_sql.Select` object that contains ORM-annotated entities is normally
executed using a :class:`_orm.Session` object, and not a :class:`_future.Connection`
object, so that ORM-related features may take effect, including that
instances of ORM-mapped objects may be returned.  When using the
:class:`_future.Connection` directly, result rows will only contain
column-level data.

Below we select from the ``User`` entity, producing a :class:`_sql.Select`
that selects from the mapped :class:`_schema.Table` to which ``User`` is mapped::

    {sql}>>> result = session.execute(select(User).order_by(User.id))
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.id
    [...] (){stop}

When selecting from ORM entities, the entity itself is returned in the result
as a single column value; for example above, the :class:`_engine.Result`
returns :class:`_engine.Row` objects that have just a single column, that column
holding onto a ``User`` object::

    >>> result.fetchone()
    (User(id=1, name='spongebob', fullname='Spongebob Squarepants'),)

When selecting a list of single-column ORM entities, it is typical to skip
the generation of :class:`_engine.Row` objects and instead receive
ORM entities directly, which is achieved using the :meth:`_engine.Result.scalars`
method::

    >>> result.scalars().all()
    [User(id=2, name='sandy', fullname='Sandy Cheeks'),
     User(id=3, name='patrick', fullname='Patrick Star'),
     User(id=4, name='squidward', fullname='Squidward Tentacles'),
     User(id=5, name='ehkrabs', fullname='Eugene H. Krabs')]

ORM Entities are named in the result row based on their class name,
such as below where we SELECT from both ``User`` and ``Address`` at the
same time::

    >>> stmt = select(User, Address).join(User.addresses).order_by(User.id, Address.id)

    {sql}>>> for row in session.execute(stmt):
    ...    print(f"{row.User.name} {row.Address.email_address}")
    SELECT user_account.id, user_account.name, user_account.fullname,
    address.id AS id_1, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    ORDER BY user_account.id, address.id
    [...] (){stop}
    spongebob spongebob@sqlalchemy.org
    sandy sandy@sqlalchemy.org
    sandy squirrel@squirrelpower.org
    patrick pat999@aol.com
    squidward stentcl@sqlalchemy.org


Selecting Individual Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The attributes on a mapped class, such as ``User.name`` and ``Address.email_address``,
have a similar behavior as that of the entity class itself such as ``User``
in that they are automatically converted into ORM-annotated Core objects
when passed to :func:`_sql.select`.   They may be used in the same way
as table columns are used::

    {sql}>>> result = session.execute(
    ...     select(User.name, Address.email_address).
    ...     join(User.addresses).
    ...     order_by(User.id, Address.id)
    ... )
    SELECT user_account.name, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    ORDER BY user_account.id, address.id
    [...] (){stop}

ORM attributes, themselves known as
:class:`_orm.InstrumentedAttribute`
objects, can be used in the same way as any :class:`_sql.ColumnElement`,
and are delivered in result rows just the same way, such as below
where we refer to their values by column name within each row::

    >>> for row in result:
    ...     print(f"{row.name}  {row.email_address}")
    spongebob  spongebob@sqlalchemy.org
    sandy  sandy@sqlalchemy.org
    sandy  squirrel@squirrelpower.org
    patrick  pat999@aol.com
    squidward  stentcl@sqlalchemy.org

Grouping Selected Attributes with Bundles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_orm.Bundle` construct is an extensible ORM-only construct that
allows sets of column expressions to be grouped in result rows::

    >>> from sqlalchemy.orm import Bundle
    >>> stmt = select(
    ...     Bundle("user", User.name, User.fullname),
    ...     Bundle("email", Address.email_address)
    ... ).join_from(User, Address)
    {sql}>>> for row in session.execute(stmt):
    ...     print(f"{row.user.name} {row.email.email_address}")
    SELECT user_account.name, user_account.fullname, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    [...] (){stop}
    spongebob spongebob@sqlalchemy.org
    sandy sandy@sqlalchemy.org
    sandy squirrel@squirrelpower.org
    patrick pat999@aol.com
    squidward stentcl@sqlalchemy.org


The :class:`_orm.Bundle` is potentially useful for creating lightweight
views as well as custom column groupings such as mappings.

.. seealso::

    :ref:`bundles` - in the ORM loading documentation.


Selecting ORM Aliases
^^^^^^^^^^^^^^^^^^^^^

As discussed in the tutorial at :ref:`tutorial_using_aliases`, to create a
SQL alias of an ORM entity is achieved using the :func:`_orm.aliased`
construct against a mapped class::

    >>> from sqlalchemy.orm import aliased
    >>> u1 = aliased(User)
    >>> print(select(u1).order_by(u1.id))
    {opensql}SELECT user_account_1.id, user_account_1.name, user_account_1.fullname
    FROM user_account AS user_account_1 ORDER BY user_account_1.id

As is the case when using :meth:`_schema.Table.alias`, the SQL alias
is anonymously named.   For the case of selecting the entity from a row
with an explicit name, the :paramref:`_orm.aliased.name` parameter may be
passed as well::

    >>> from sqlalchemy.orm import aliased
    >>> u1 = aliased(User, name="u1")
    >>> stmt = select(u1).order_by(u1.id)
    {sql}>>> row = session.execute(stmt).first()
    SELECT u1.id, u1.name, u1.fullname
    FROM user_account AS u1 ORDER BY u1.id
    [...] (){stop}
    >>> print(f"{row.u1.name}")
    spongebob

The :class:`_orm.aliased` construct is also central to making use of subqueries
with the ORM; the section :ref:`orm_queryguide_subqueries` discusses this further.

.. _orm_queryguide_selecting_text:

Getting ORM Results from Textual and Core Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ORM supports loading of entities from SELECT statements that come from other
sources.  The typical use case is that of a textual SELECT statement, which
in SQLAlchemy is represented using the :func:`_sql.text` construct.   The
:func:`_sql.text` construct, once constructed, can be augmented with
information
about the ORM-mapped columns that the statement would load; this can then be
associated with the ORM entity itself so that ORM objects can be loaded based
on this statement.

Given a textual SQL statement we'd like to load from::

    >>> from sqlalchemy import text
    >>> textual_sql = text("SELECT id, name, fullname FROM user_account ORDER BY id")

We can add column information to the statement by using the
:meth:`_sql.TextClause.columns` method; when this method is invoked, the
:class:`_sql.TextClause` object is converted into a :class:`_sql.TextualSelect`
object, which takes on a role that is comparable to the :class:`_sql.Select`
construct.  The :meth:`_sql.TextClause.columns` method
is typically passed :class:`_schema.Column` objects or equivalent, and in this
case we can make use of the ORM-mapped attributes on the ``User`` class
directly::

    >>> textual_sql = textual_sql.columns(User.id, User.name, User.fullname)

We now have an ORM-configured SQL construct that as given, can load the "id",
"name" and "fullname" columns separately.   To use this SELECT statement as a
source of complete ``User`` entities instead, we can link these columns to a
regular ORM-enabled
:class:`_sql.Select` construct using the :meth:`_sql.Select.from_statement`
method::

    >>> # using from_statement()
    >>> orm_sql = select(User).from_statement(textual_sql)
    >>> for user_obj in session.execute(orm_sql).scalars():
    ...     print(user_obj)
    {opensql}SELECT id, name, fullname FROM user_account ORDER BY id
    [...] (){stop}
    User(id=1, name='spongebob', fullname='Spongebob Squarepants')
    User(id=2, name='sandy', fullname='Sandy Cheeks')
    User(id=3, name='patrick', fullname='Patrick Star')
    User(id=4, name='squidward', fullname='Squidward Tentacles')
    User(id=5, name='ehkrabs', fullname='Eugene H. Krabs')

The same :class:`_sql.TextualSelect` object can also be converted into
a subquery using the :meth:`_sql.TextualSelect.subquery` method,
and linked to the ``User`` entity to it using the :func:`_orm.aliased`
construct, in a similar manner as discussed below in :ref:`orm_queryguide_subqueries`::

    >>> # using aliased() to select from a subquery
    >>> orm_subquery = aliased(User, textual_sql.subquery())
    >>> stmt = select(orm_subquery)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {opensql}SELECT anon_1.id, anon_1.name, anon_1.fullname
    FROM (SELECT id, name, fullname FROM user_account ORDER BY id) AS anon_1
    [...] (){stop}
    User(id=1, name='spongebob', fullname='Spongebob Squarepants')
    User(id=2, name='sandy', fullname='Sandy Cheeks')
    User(id=3, name='patrick', fullname='Patrick Star')
    User(id=4, name='squidward', fullname='Squidward Tentacles')
    User(id=5, name='ehkrabs', fullname='Eugene H. Krabs')

The difference between using the :class:`_sql.TextualSelect` directly with
:meth:`_sql.Select.from_statement` versus making use of :func:`_sql.aliased`
is that in the former case, no subquery is produced in the resulting SQL.
This can in some scenarios be advantageous from a performance or complexity
perspective.

.. _orm_queryguide_joins:

Joins
-----

The :meth:`_sql.Select.join` and :meth:`_sql.Select.join_from` methods
are used to construct SQL JOINs against a SELECT statement.

This section will detail ORM use cases for these methods.  For a general
overview of their use from a Core perspective, see :ref:`tutorial_select_join`
in the :ref:`unified_tutorial`.

The usage of :meth:`_sql.Select.join` in an ORM context for :term:`2.0 style`
queries is mostly equivalent, minus legacy use cases, to the usage of the
:meth:`_orm.Query.join` method in :term:`1.x style` queries.

Simple Relationship Joins
^^^^^^^^^^^^^^^^^^^^^^^^^^

Consider a mapping between two classes ``User`` and ``Address``,
with a relationship ``User.addresses`` representing a collection
of ``Address`` objects associated with each ``User``.   The most
common usage of :meth:`_sql.Select.join`
is to create a JOIN along this
relationship, using the ``User.addresses`` attribute as an indicator
for how this should occur::

    >>> stmt = select(User).join(User.addresses)

Where above, the call to :meth:`_sql.Select.join` along
``User.addresses`` will result in SQL approximately equivalent to::

    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

In the above example we refer to ``User.addresses`` as passed to
:meth:`_sql.Select.join` as the "on clause", that is, it indicates
how the "ON" portion of the JOIN should be constructed.

Chaining Multiple Joins
^^^^^^^^^^^^^^^^^^^^^^^^

To construct a chain of joins, multiple :meth:`_sql.Select.join` calls may be
used.  The relationship-bound attribute implies both the left and right side of
the join at once.   Consider additional entities ``Order`` and ``Item``, where
the ``User.orders`` relationship refers to the ``Order`` entity, and the
``Order.items`` relationship refers to the ``Item`` entity, via an association
table ``order_items``.   Two :meth:`_sql.Select.join` calls will result in
a JOIN first from ``User`` to ``Order``, and a second from ``Order`` to
``Item``.  However, since ``Order.items`` is a :ref:`many to many <relationships_many_to_many>`
relationship, it results in two separate JOIN elements, for a total of three
JOIN elements in the resulting SQL::

    >>> stmt = (
    ...     select(User).
    ...     join(User.orders).
    ...     join(Order.items)
    ... )
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN user_order ON user_account.id = user_order.user_id
    JOIN order_items AS order_items_1 ON user_order.id = order_items_1.order_id
    JOIN item ON item.id = order_items_1.item_id

The order in which each call to the :meth:`_sql.Select.join` method
is significant only to the degree that the "left" side of what we would like
to join from needs to be present in the list of FROMs before we indicate a
new target.   :meth:`_sql.Select.join` would not, for example, know how to
join correctly if we were to specify
``select(User).join(Order.items).join(User.orders)``, and would raise an
error.  In correct practice, the :meth:`_sql.Select.join` method is invoked
in such a way that lines up with how we would want the JOIN clauses in SQL
to be rendered, and each call should represent a clear link from what
precedes it.

All of the elements that we target in the FROM clause remain available
as potential points to continue joining FROM.    We can continue to add
other elements to join FROM the ``User`` entity above, for example adding
on the ``User.addresses`` relationship to our chain of joins::

    >>> stmt = (
    ...     select(User).
    ...     join(User.orders).
    ...     join(Order.items).
    ...     join(User.addresses)
    ... )
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN user_order ON user_account.id = user_order.user_id
    JOIN order_items AS order_items_1 ON user_order.id = order_items_1.order_id
    JOIN item ON item.id = order_items_1.item_id
    JOIN address ON user_account.id = address.user_id


Joins to a Target Entity or Selectable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A second form of :meth:`_sql.Select.join` allows any mapped entity or core
selectable construct as a target.   In this usage, :meth:`_sql.Select.join`
will attempt to **infer** the ON clause for the JOIN, using the natural foreign
key relationship between two entities::

    >>> stmt = select(User).join(Address)
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

In the above calling form, :meth:`_sql.Select.join` is called upon to infer
the "on clause" automatically.  This calling form will ultimately raise
an error if either there are no :class:`_schema.ForeignKeyConstraint` setup
between the two mapped :class:`_schema.Table` constructs, or if there are multiple
:class:`_schema.ForeignKeyConstraint` linakges between them such that the
appropriate constraint to use is ambiguous.

.. note:: When making use of :meth:`_sql.Select.join` or :meth:`_sql.Select.join_from`
    without indicating an ON clause, ORM
    configured :func:`_orm.relationship` constructs are **not taken into account**.
    Only the configured :class:`_schema.ForeignKeyConstraint` relationships between
    the entities at the level of the mapped :class:`_schema.Table` objects are consulted
    when an attempt is made to infer an ON clause for the JOIN.

Joins to a Target with an ON Clause
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The third calling form allows both the target entity as well
as the ON clause to be passed explicitly.    A example that includes
a SQL expression as the ON clause is as follows::

    >>> stmt = select(User).join(Address, User.id==Address.user_id)
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

The expression-based ON clause may also be the relationship-bound
attribute; this form in fact states the target of ``Address`` twice, however
this is accepted::

    >>> stmt = select(User).join(Address, User.addresses)
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

The above syntax has more functionality if we use it in terms of aliased
entities.  The default target for ``User.addresses`` is the ``Address``
class, however if we pass aliased forms using :func:`_orm.aliased`, the
:func:`_orm.aliased` form will be used as the target, as in the example
below::

    >>> a1 = aliased(Address)
    >>> a2 = aliased(Address)
    >>> stmt = (
    ...     select(User).
    ...     join(a1, User.addresses).
    ...     join(a2, User.addresses).
    ...     where(a1.email_address == 'ed@foo.com').
    ...     where(a2.email_address == 'ed@bar.com')
    ... )
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2

When using relationship-bound attributes, the target entity can also be
substituted with an aliased entity by using the
:meth:`_orm.PropComparator.of_type` method.   The same example using
this method would be::

    >>> stmt = (
    ...     select(User).
    ...     join(User.addresses.of_type(a1)).
    ...     join(User.addresses.of_type(a2)).
    ...     where(a1.email_address == 'ed@foo.com').
    ...     where(a2.email_address == 'ed@bar.com')
    ... )
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2

.. _orm_queryguide_join_on_augmented:

Augmenting Built-in ON Clauses
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a substitute for providing a full custom ON condition for an
existing relationship, the :meth:`_orm.PropComparator.and_` function
may be applied to a relationship attribute to augment additional
criteria into the ON clause; the additional criteria will be combined
with the default criteria using AND.  Below, the ON criteria between
``user_account`` and ``address`` contains two separate elements joined
by ``AND``, the first one being the natural join along the foreign key,
and the second being a custom limiting criteria::

    >>> stmt = (
    ...     select(User).
    ...     join(User.addresses.and_(Address.email_address != 'foo@bar.com'))
    ... )
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address ON user_account.id = address.user_id
    AND address.email_address != :email_address_1

.. seealso::

    The :meth:`_orm.PropComparator.and_` method also works with loader
    strategies. See the section :ref:`loader_option_criteria` for an example.

.. _orm_queryguide_subqueries:

Joining to Subqueries
^^^^^^^^^^^^^^^^^^^^^^^

The target of a join may be any "selectable" entity which usefully includes
subuqeries.   When using the ORM, it is typical
that these targets are stated in terms of an
:func:`_orm.aliased` construct, but this is not strictly required particularly
if the joined entity is not being returned in the results.  For example, to join from the
``User`` entity to the ``Address`` entity, where the ``Address`` entity
is represented as a row limited subquery, we first construct a :class:`_sql.Subquery`
object using :meth:`_sql.Select.subquery`, which may then be used as the
target of the :meth:`_sql.Select.join` method::

    >>> subq = (
    ...     select(Address).
    ...     where(Address.email_address == 'pat999@aol.com').
    ...     subquery()
    ... )
    >>> stmt = select(User).join(subq, User.id == subq.c.user_id)
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN (SELECT address.id AS id,
    address.user_id AS user_id, address.email_address AS email_address
    FROM address
    WHERE address.email_address = :email_address_1) AS anon_1
    ON user_account.id = anon_1.user_id{stop}

The above SELECT statement when invoked via :meth:`_orm.Session.execute`
will return rows that contain ``User`` entities, but not ``Address`` entities.
In order to add ``Address`` entities to the set of entities that would be
returned in result sets, we construct an :func:`_orm.aliased` object against
the ``Address`` entity and the custom subquery.  Note we also apply a name
``"address"`` to the :func:`_orm.aliased` construct so that we may
refer to it by name in the result row::

    >>> address_subq = aliased(Address, subq, name="address")
    >>> stmt = select(User, address_subq).join(address_subq)
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User} {row.address}")
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname,
    anon_1.id AS id_1, anon_1.user_id, anon_1.email_address
    FROM user_account
    JOIN (SELECT address.id AS id,
    address.user_id AS user_id, address.email_address AS email_address
    FROM address
    WHERE address.email_address = ?) AS anon_1 ON user_account.id = anon_1.user_id
    [...] ('pat999@aol.com',){stop}
    User(id=3, name='patrick', fullname='Patrick Star') Address(id=4, email_address='pat999@aol.com')

The same subquery may be referred towards by multiple entities as well,
for a subquery that represents more than one entity.  The subquery itself
will remain unique within the statement, while the entities that are linked
to it using :class:`_orm.aliased` refer to distinct sets of columns::

    >>> user_address_subq = (
    ...        select(User.id, User.name, Address.id, Address.email_address).
    ...        join_from(User, Address).
    ...        where(Address.email_address.in_(['pat999@aol.com', 'squirrel@squirrelpower.org'])).
    ...        subquery()
    ... )
    >>> user_alias = aliased(User, user_address_subq, name="user")
    >>> address_alias = aliased(Address, user_address_subq, name="address")
    >>> stmt = select(user_alias, address_alias).where(user_alias.name == 'sandy')
    >>> for row in session.execute(stmt):
    ...     print(f"{row.user} {row.address}")
    {opensql}SELECT anon_1.id, anon_1.name, anon_1.id_1, anon_1.email_address
    FROM (SELECT user_account.id AS id, user_account.name AS name, address.id AS id_1, address.email_address AS email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE address.email_address IN (?, ?)) AS anon_1
    WHERE anon_1.name = ?
    [...] ('pat999@aol.com', 'squirrel@squirrelpower.org', 'sandy'){stop}
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=3, email_address='squirrel@squirrelpower.org')


Controlling what to Join From
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In cases where the left side of the current state of
:class:`_sql.Select` is not in line with what we want to join from,
the :meth:`_sql.Select.join_from` method may be used::

    >>> stmt = select(Address).join_from(User, User.addresses).where(User.name == 'sandy')
    >>> print(stmt)
    SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1

The :meth:`_sql.Select.join_from` method accepts two or three arguments, either
in the form ``<join from>, <onclause>``, or ``<join from>, <join to>,
[<onclause>]``::

    >>> stmt = select(Address).join_from(User, Address).where(User.name == 'sandy')
    >>> print(stmt)
    SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1

To set up the initial FROM clause for a SELECT such that :meth:`_sql.Select.join`
can be used subsequent, the :meth:`_sql.Select.select_from` method may also
be used::


    >>> stmt = select(Address).select_from(User).join(User.addresses).where(User.name == 'sandy')
    >>> print(stmt)
    SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1


Special Relationship Operators
------------------------------

As detailed in the :ref:`unified_tutorial` at
:ref:`tutorial_select_relationships`, ORM attributes mapped by
:func:`_orm.relationship` may be used in a variety of ways as SQL construction
helpers.  In addition to the above documentation on
:ref:`orm_queryguide_joins`, relationships may produce criteria to be used in
the WHERE clause as well.  See the linked sections below.

.. seealso::

    Sections in the :ref:`tutorial_orm_related_objects` section of the
    :ref:`unified_tutorial`:

    * :ref:`tutorial_relationship_exists` - helpers to generate EXISTS clauses
      using :func:`_orm.relationship`


    * :ref:`tutorial_relationship_operators` - helpers to create comparisons in
      terms of a :func:`_orm.relationship` in reference to a specific object
      instance


ORM Loader Options
-------------------

Loader options are objects that are passed to the :meth:`_sql.Select.options`
method which affect the loading of both column and relationship-oriented
attributes.  The majority of loader options descend from the :class:`_orm.Load`
hierarchy.  For a complete overview of using loader options, see the linked
sections below.

.. seealso::

    * :ref:`loading_columns` - details mapper and loading options that affect
      how column and SQL-expression mapped attributes are loaded

    * :ref:`loading_toplevel` - details relationship and loading options that
      affect how :func:`_orm.relationship` mapped attributes are loaded

.. _orm_queryguide_execution_options:

ORM Execution Options
---------------------

Execution options are keyword arguments that are passed to an
"execution_options" method, which take place at the level of statement
execution.    The primary "execution option" method is in Core at
:meth:`_engine.Connection.execution_options`. In the ORM, execution options may
also be passed to :meth:`_orm.Session.execute` using the
:paramref:`_orm.Session.execute.execution_options` parameter. Perhaps more
succinctly, most execution options, including those specific to the ORM, can be
assigned to a statement directly, using the
:meth:`_sql.Executable.execution_options` method, so that the options may be
associated directly with the statement instead of being configured separately.
The examples below will use this form.

.. _orm_queryguide_populate_existing:

Populate Existing
^^^^^^^^^^^^^^^^^^

The ``populate_existing`` execution option ensures that for all rows
loaded, the corresponding instances in the :class:`_orm.Session` will
be fully refreshed, erasing any existing data within the objects
(including pending changes) and replacing with the data loaded from the
result.

Example use looks like::

    >>> stmt = select(User).execution_options(populate_existing=True)
    {sql}>>> result = session.execute(stmt)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    ...

Normally, ORM objects are only loaded once, and if they are matched up
to the primary key in a subsequent result row, the row is not applied to the
object.  This is both to preserve pending, unflushed changes on the object
as well as to avoid the overhead and complexity of refreshing data which
is already there.   The :class:`_orm.Session` assumes a default working
model of a highly isolated transaction, and to the degree that data is
expected to change within the transaction outside of the local changes being
made, those use cases would be handled using explicit steps such as this method.

Another use case for ``populate_existing`` is in support of various
attribute loading features that can change how an attribute is loaded on
a per-query basis.   Options for which this apply include:

* The :func:`_orm.with_expression` option

* The :meth:`_orm.PropComparator.and_` method that can modify what a loader
  strategy loads

* The :func:`_orm.contains_eager` option

* The :func:`_orm.with_loader_criteria` option

The ``populate_existing`` execution option is equvialent to the
:meth:`_orm.Query.populate_existing` method in :term:`1.x style` ORM queries.

.. seealso::

    :ref:`faq_session_identity` - in :doc:`/faq/index`

    :ref:`session_expire` - in the ORM :class:`_orm.Session`
    documentation

.. _orm_queryguide_autoflush:

Autoflush
^^^^^^^^^

This option when passed as ``False`` will cause the :class:`_orm.Session`
to not invoke the "autoflush" step.  It's equivalent to using the
:attr:`_orm.Session.no_autoflush` context manager to disable autoflush::

    >>> stmt = select(User).execution_options(autoflush=False)
    {sql}>>> session.execute(stmt)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    ...

This option will also work on ORM-enabled :class:`_sql.Update` and
:class:`_sql.Delete` queries.

The ``autoflush`` execution option is equvialent to the
:meth:`_orm.Query.autoflush` method in :term:`1.x style` ORM queries.

.. seealso::

    :ref:`session_flushing`

.. _orm_queryguide_yield_per:

Yield Per
^^^^^^^^^

The ``yield_per`` execution option is an integer value which will cause the
:class:`_engine.Result` to yield only a fixed count of rows at a time.  It is
often useful to use with a result partitioning method such as
:meth:`_engine.Result.partitions`, e.g.::

    >>> stmt = select(User).execution_options(yield_per=10)
    {sql}>>> for partition in session.execute(stmt).partitions(10):
    ...     for row in partition:
    ...         print(row)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    [...] (){stop}
    (User(id=1, name='spongebob', fullname='Spongebob Squarepants'),)
    ...

The purpose of this method is when fetching very large result sets
(> 10K rows), to batch results in sub-collections and yield them
out partially, so that the Python interpreter doesn't need to declare
very large areas of memory which is both time consuming and leads
to excessive memory use.   The performance from fetching hundreds of
thousands of rows can often double when a suitable yield-per setting
(e.g. approximately 1000) is used, even with DBAPIs that buffer
rows (which are most).

When ``yield_per`` is used, the
:paramref:`_engine.Connection.execution_options.stream_results` option is also
set for the Core execution, so that a streaming / server side cursor will be
used if the backend supports it [1]_


The ``yield_per`` execution option **is not compatible with subqueryload eager
loading or joinedload eager loading when using collections**.  It is
potentially compatible with selectinload eager loading, **provided the database
driver supports multiple, independent cursors** [2]_ .

The ``yield_per`` execution option is equvialent to the
:meth:`_orm.Query.yield_per` method in :term:`1.x style` ORM queries.

.. [1] currently known are
   :mod:`_postgresql.psycopg2`,
   :mod:`_mysql.mysqldb` and
   :mod:`_mysql.pymysql`.  Other backends will pre buffer
   all rows.  The memory use of raw database rows is much less than that of an
   ORM-mapped object, but should still be taken into consideration when
   benchmarking.

.. [2] the :mod:`_postgresql.psycopg2`
   and :mod:`_sqlite.pysqlite` drivers are
   known to work, drivers for MySQL and SQL Server ODBC drivers do not.

.. seealso::

    :ref:`engine_stream_results`


ORM Update / Delete with Arbitrary WHERE clause
================================================

The :meth:`_orm.Session.execute` method, in addition to handling ORM-enabled
:class:`_sql.Select` objects, can also accommodate ORM-enabled
:class:`_sql.Update` and :class:`_sql.Delete` objects, which UPDATE or DELETE
any number of database rows while also being able to synchronize the state of
matching objects locally present in the :class:`_orm.Session`. See the section
:ref:`orm_expression_update_delete` for background on this feature.


..  Setup code, not for display

    >>> conn.close()
    ROLLBACK
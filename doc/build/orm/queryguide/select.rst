.. highlight:: pycon+sql
.. |prev| replace:: :doc:`index`
.. |next| replace:: :doc:`inheritance`

.. include:: queryguide_nav_include.rst

Writing SELECT statements for ORM Mapped Classes
================================================

.. admonition:: About this Document

    This section makes use of ORM mappings first illustrated in the
    :ref:`unified_tutorial`, shown in the section
    :ref:`tutorial_declaring_mapped_classes`.

    :doc:`View the ORM setup for this page <_plain_setup>`.


SELECT statements are produced by the :func:`_sql.select` function which
returns a :class:`_sql.Select` object.  The entities and/or SQL expressions
to return (i.e. the "columns" clause) are passed positionally to the
function.  From there, additional methods are used to generate the complete
statement, such as the :meth:`_sql.Select.where` method illustrated below::

    >>> from sqlalchemy import select
    >>> stmt = select(User).where(User.name == "spongebob")

Given a completed :class:`_sql.Select` object, in order to execute it within
the ORM to get rows back, the object is passed to
:meth:`_orm.Session.execute`, where a :class:`.Result` object is then
returned::

    >>> result = session.execute(stmt)
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
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
are converted into :term:`ORM-annotated` :class:`_sql.FromClause` and
:class:`_sql.ColumnElement` elements at construction time.

A :class:`_sql.Select` object that contains ORM-annotated entities is normally
executed using a :class:`_orm.Session` object, and not a :class:`_engine.Connection`
object, so that ORM-related features may take effect, including that
instances of ORM-mapped objects may be returned.  When using the
:class:`_engine.Connection` directly, result rows will only contain
column-level data.

.. _orm_queryguide_select_orm_entities:

Selecting ORM Entities
^^^^^^^^^^^^^^^^^^^^^^

Below we select from the ``User`` entity, producing a :class:`_sql.Select`
that selects from the mapped :class:`_schema.Table` to which ``User`` is mapped::

    >>> result = session.execute(select(User).order_by(User.id))
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.id
    [...] ()

When selecting from ORM entities, the entity itself is returned in the result
as a row with a single element, as opposed to a series of individual columns;
for example above, the :class:`_engine.Result` returns :class:`_engine.Row`
objects that have just a single element per row, that element holding onto a
``User`` object::

    >>> result.all()
    [(User(id=1, name='spongebob', fullname='Spongebob Squarepants'),),
     (User(id=2, name='sandy', fullname='Sandy Cheeks'),),
     (User(id=3, name='patrick', fullname='Patrick Star'),),
     (User(id=4, name='squidward', fullname='Squidward Tentacles'),),
     (User(id=5, name='ehkrabs', fullname='Eugene H. Krabs'),)]


When selecting a list of single-element rows containing ORM entities, it is
typical to skip the generation of :class:`_engine.Row` objects and instead
receive ORM entities directly.   This is most easily achieved by using the
:meth:`_orm.Session.scalars` method to execute, rather than the
:meth:`_orm.Session.execute` method, so that a :class:`.ScalarResult` object
which yields single elements rather than rows is returned::

    >>> session.scalars(select(User).order_by(User.id)).all()
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.id
    [...] ()
    {stop}[User(id=1, name='spongebob', fullname='Spongebob Squarepants'),
     User(id=2, name='sandy', fullname='Sandy Cheeks'),
     User(id=3, name='patrick', fullname='Patrick Star'),
     User(id=4, name='squidward', fullname='Squidward Tentacles'),
     User(id=5, name='ehkrabs', fullname='Eugene H. Krabs')]

Calling the :meth:`_orm.Session.scalars` method is the equivalent to calling
upon :meth:`_orm.Session.execute` to receive a :class:`_engine.Result` object,
then calling upon :meth:`_engine.Result.scalars` to receive a
:class:`_engine.ScalarResult` object.


.. _orm_queryguide_select_multiple_entities:

Selecting Multiple ORM Entities Simultaneously
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.select` function accepts any number of ORM classes and/or
column expressions at once, including that multiple ORM classes may be
requested.   When SELECTing from multiple ORM classes, they are named
in each result row based on their class name.   In the example below,
the result rows for a SELECT against ``User`` and ``Address`` will
refer to them under the names ``User`` and ``Address``::

    >>> stmt = select(User, Address).join(User.addresses).order_by(User.id, Address.id)
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User.name} {row.Address.email_address}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname,
    address.id AS id_1, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    ORDER BY user_account.id, address.id
    [...] (){stop}
    spongebob spongebob@sqlalchemy.org
    sandy sandy@sqlalchemy.org
    sandy squirrel@squirrelpower.org
    patrick pat999@aol.com
    squidward stentcl@sqlalchemy.org

If we wanted to assign different names to these entities in the rows, we would
use the :func:`_orm.aliased` construct using the :paramref:`_orm.aliased.name`
parameter to alias them with an explicit name::

    >>> from sqlalchemy.orm import aliased
    >>> user_cls = aliased(User, name="user_cls")
    >>> email_cls = aliased(Address, name="email")
    >>> stmt = (
    ...     select(user_cls, email_cls)
    ...     .join(user_cls.addresses.of_type(email_cls))
    ...     .order_by(user_cls.id, email_cls.id)
    ... )
    >>> row = session.execute(stmt).first()
    {execsql}SELECT user_cls.id, user_cls.name, user_cls.fullname,
    email.id AS id_1, email.user_id, email.email_address
    FROM user_account AS user_cls JOIN address AS email
    ON user_cls.id = email.user_id ORDER BY user_cls.id, email.id
    [...] ()
    {stop}>>> print(f"{row.user_cls.name} {row.email.email_address}")
    spongebob spongebob@sqlalchemy.org

The aliased form above is discussed further at
:ref:`orm_queryguide_joining_relationships_aliased`.

An existing :class:`_sql.Select` construct may also have ORM classes and/or
column expressions added to its columns clause using the
:meth:`_sql.Select.add_columns` method. We can produce the same statement as
above using this form as well::

    >>> stmt = (
    ...     select(User).join(User.addresses).add_columns(Address).order_by(User.id, Address.id)
    ... )
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname,
    address.id AS id_1, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    ORDER BY user_account.id, address.id


Selecting Individual Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The attributes on a mapped class, such as ``User.name`` and
``Address.email_address``, can be used just like :class:`_schema.Column` or
other SQL expression objects when passed to :func:`_sql.select`. Creating a
:func:`_sql.select` that is against specific columns will return :class:`.Row`
objects, and **not** entities like ``User`` or ``Address`` objects.
Each :class:`.Row` will have each column represented individually::

    >>> result = session.execute(
    ...     select(User.name, Address.email_address)
    ...     .join(User.addresses)
    ...     .order_by(User.id, Address.id)
    ... )
    {execsql}SELECT user_account.name, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    ORDER BY user_account.id, address.id
    [...] (){stop}

The above statement returns :class:`.Row` objects with ``name`` and
``email_address`` columns, as illustrated in the runtime demonstration below::

    >>> for row in result:
    ...     print(f"{row.name}  {row.email_address}")
    spongebob  spongebob@sqlalchemy.org
    sandy  sandy@sqlalchemy.org
    sandy  squirrel@squirrelpower.org
    patrick  pat999@aol.com
    squidward  stentcl@sqlalchemy.org

.. _bundles:

Grouping Selected Attributes with Bundles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_orm.Bundle` construct is an extensible ORM-only construct that
allows sets of column expressions to be grouped in result rows::

    >>> from sqlalchemy.orm import Bundle
    >>> stmt = select(
    ...     Bundle("user", User.name, User.fullname),
    ...     Bundle("email", Address.email_address),
    ... ).join_from(User, Address)
    >>> for row in session.execute(stmt):
    ...     print(f"{row.user.name} {row.user.fullname} {row.email.email_address}")
    {execsql}SELECT user_account.name, user_account.fullname, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    [...] (){stop}
    spongebob Spongebob Squarepants spongebob@sqlalchemy.org
    sandy Sandy Cheeks sandy@sqlalchemy.org
    sandy Sandy Cheeks squirrel@squirrelpower.org
    patrick Patrick Star pat999@aol.com
    squidward Squidward Tentacles stentcl@sqlalchemy.org

The :class:`_orm.Bundle` is potentially useful for creating lightweight views
and custom column groupings. :class:`_orm.Bundle` may also be subclassed in
order to return alternate data structures; see
:meth:`_orm.Bundle.create_row_processor` for an example.

.. seealso::

    :class:`_orm.Bundle`

    :meth:`_orm.Bundle.create_row_processor`


.. _orm_queryguide_orm_aliases:

Selecting ORM Aliases
^^^^^^^^^^^^^^^^^^^^^

As discussed in the tutorial at :ref:`tutorial_using_aliases`, to create a
SQL alias of an ORM entity is achieved using the :func:`_orm.aliased`
construct against a mapped class::

    >>> from sqlalchemy.orm import aliased
    >>> u1 = aliased(User)
    >>> print(select(u1).order_by(u1.id))
    {printsql}SELECT user_account_1.id, user_account_1.name, user_account_1.fullname
    FROM user_account AS user_account_1 ORDER BY user_account_1.id

As is the case when using :meth:`_schema.Table.alias`, the SQL alias
is anonymously named.   For the case of selecting the entity from a row
with an explicit name, the :paramref:`_orm.aliased.name` parameter may be
passed as well::

    >>> from sqlalchemy.orm import aliased
    >>> u1 = aliased(User, name="u1")
    >>> stmt = select(u1).order_by(u1.id)
    >>> row = session.execute(stmt).first()
    {execsql}SELECT u1.id, u1.name, u1.fullname
    FROM user_account AS u1 ORDER BY u1.id
    [...] (){stop}
    >>> print(f"{row.u1.name}")
    spongebob

.. seealso::


    The :class:`_orm.aliased` construct is central for several use cases,
    including:

    * making use of subqueries with the ORM; the sections
      :ref:`orm_queryguide_subqueries` and
      :ref:`orm_queryguide_join_subqueries` discuss this further.
    * Controlling the name of an entity in a result set; see
      :ref:`orm_queryguide_select_multiple_entities` for an example
    * Joining to the same ORM entity multiple times; see
      :ref:`orm_queryguide_joining_relationships_aliased` for an example.

.. _orm_queryguide_selecting_text:

Getting ORM Results from Textual Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ORM supports loading of entities from SELECT statements that come from
other sources. The typical use case is that of a textual SELECT statement,
which in SQLAlchemy is represented using the :func:`_sql.text` construct. A
:func:`_sql.text` construct can be augmented with information about the
ORM-mapped columns that the statement would load; this can then be associated
with the ORM entity itself so that ORM objects can be loaded based on this
statement.

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

    >>> orm_sql = select(User).from_statement(textual_sql)
    >>> for user_obj in session.execute(orm_sql).scalars():
    ...     print(user_obj)
    {execsql}SELECT id, name, fullname FROM user_account ORDER BY id
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

    >>> orm_subquery = aliased(User, textual_sql.subquery())
    >>> stmt = select(orm_subquery)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {execsql}SELECT anon_1.id, anon_1.name, anon_1.fullname
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

.. _orm_queryguide_subqueries:

Selecting Entities from Subqueries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_orm.aliased` construct discussed in the previous section
can be used with any :class:`_sql.Subquery` construct that comes from a
method such as :meth:`_sql.Select.subquery` to link ORM entities to the
columns returned by that subquery; by default, there must be a **column correspondence**
relationship between the columns delivered by the subquery and the columns
to which the entity is mapped, meaning, the subquery needs to be ultimately
derived from those entities, such as in the example below::

    >>> inner_stmt = select(User).where(User.id < 7).order_by(User.id)
    >>> subq = inner_stmt.subquery()
    >>> aliased_user = aliased(User, subq)
    >>> stmt = select(aliased_user)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {execsql} SELECT anon_1.id, anon_1.name, anon_1.fullname
    FROM (SELECT user_account.id AS id, user_account.name AS name, user_account.fullname AS fullname
    FROM user_account
    WHERE user_account.id < ? ORDER BY user_account.id) AS anon_1
    [generated in ...] (7,)
    {stop}User(id=1, name='spongebob', fullname='Spongebob Squarepants')
    User(id=2, name='sandy', fullname='Sandy Cheeks')
    User(id=3, name='patrick', fullname='Patrick Star')
    User(id=4, name='squidward', fullname='Squidward Tentacles')
    User(id=5, name='ehkrabs', fullname='Eugene H. Krabs')

Alternatively, an aliased subquery can be matched to the entity based on name
by applying the :paramref:`_orm.aliased.adapt_on_names` parameter::

    >>> from sqlalchemy import literal
    >>> inner_stmt = select(
    ...     literal(14).label("id"),
    ...     literal("made up name").label("name"),
    ...     literal("made up fullname").label("fullname"),
    ... )
    >>> subq = inner_stmt.subquery()
    >>> aliased_user = aliased(User, subq, adapt_on_names=True)
    >>> stmt = select(aliased_user)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {execsql}SELECT anon_1.id, anon_1.name, anon_1.fullname
    FROM (SELECT ? AS id, ? AS name, ? AS fullname) AS anon_1
    [generated in ...] (14, 'made up name', 'made up fullname')
    {stop}User(id=14, name='made up name', fullname='made up fullname')

.. seealso::

    :ref:`tutorial_subqueries_orm_aliased` - in the :ref:`unified_tutorial`

    :ref:`orm_queryguide_join_subqueries`

.. _orm_queryguide_unions:

Selecting Entities from UNIONs and other set operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.union` and :func:`_sql.union_all` functions are the most
common set operations, which along with other set operations such as
:func:`_sql.except_`, :func:`_sql.intersect` and others deliver an object known as
a :class:`_sql.CompoundSelect`, which is composed of multiple
:class:`_sql.Select` constructs joined by a set-operation keyword.   ORM entities may
be selected from simple compound selects using the :meth:`_sql.Select.from_statement`
method illustrated previously at :ref:`orm_queryguide_selecting_text`.  In
this method, the UNION statement is the complete statement that will be
rendered, no additional criteria can be added after :meth:`_sql.Select.from_statement`
is used::

    >>> from sqlalchemy import union_all
    >>> u = union_all(
    ...     select(User).where(User.id < 2), select(User).where(User.id == 3)
    ... ).order_by(User.id)
    >>> stmt = select(User).from_statement(u)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.id < ? UNION ALL SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.id = ? ORDER BY id
    [generated in ...] (2, 3)
    {stop}User(id=1, name='spongebob', fullname='Spongebob Squarepants')
    User(id=3, name='patrick', fullname='Patrick Star')

A :class:`_sql.CompoundSelect` construct can be more flexibly used within
a query that can be further modified by organizing it into a subquery
and linking it to an ORM entity using :func:`_orm.aliased`,
as illustrated previously at :ref:`orm_queryguide_subqueries`.  In the
example below, we first use :meth:`_sql.CompoundSelect.subquery` to create
a subquery of the UNION ALL statement, we then package that into the
:func:`_orm.aliased` construct where it can be used like any other mapped
entity in a :func:`_sql.select` construct, including that we can add filtering
and order by criteria based on its exported columns::

    >>> subq = union_all(
    ...     select(User).where(User.id < 2), select(User).where(User.id == 3)
    ... ).subquery()
    >>> user_alias = aliased(User, subq)
    >>> stmt = select(user_alias).order_by(user_alias.id)
    >>> for user_obj in session.execute(stmt).scalars():
    ...     print(user_obj)
    {execsql}SELECT anon_1.id, anon_1.name, anon_1.fullname
    FROM (SELECT user_account.id AS id, user_account.name AS name, user_account.fullname AS fullname
    FROM user_account
    WHERE user_account.id < ? UNION ALL SELECT user_account.id AS id, user_account.name AS name, user_account.fullname AS fullname
    FROM user_account
    WHERE user_account.id = ?) AS anon_1 ORDER BY anon_1.id
    [generated in ...] (2, 3)
    {stop}User(id=1, name='spongebob', fullname='Spongebob Squarepants')
    User(id=3, name='patrick', fullname='Patrick Star')


.. seealso::

    :ref:`tutorial_orm_union` - in the :ref:`unified_tutorial`

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

.. _orm_queryguide_simple_relationship_join:

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
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

In the above example we refer to ``User.addresses`` as passed to
:meth:`_sql.Select.join` as the "on clause", that is, it indicates
how the "ON" portion of the JOIN should be constructed.

.. tip::

   Note that using :meth:`_sql.Select.join` to JOIN from one entity to another
   affects the FROM clause of the SELECT statement, but not the columns clause;
   the SELECT statement in this example will continue to return rows from only
   the ``User`` entity.  To SELECT
   columns / entities from both ``User`` and ``Address`` at the same time,
   the ``Address`` entity must also be named in the :func:`_sql.select` function,
   or added to the :class:`_sql.Select` construct afterwards using the
   :meth:`_sql.Select.add_columns` method.  See the section
   :ref:`orm_queryguide_select_multiple_entities` for examples of both
   of these forms.

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

    >>> stmt = select(User).join(User.orders).join(Order.items)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
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

    >>> stmt = select(User).join(User.orders).join(Order.items).join(User.addresses)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN user_order ON user_account.id = user_order.user_id
    JOIN order_items AS order_items_1 ON user_order.id = order_items_1.order_id
    JOIN item ON item.id = order_items_1.item_id
    JOIN address ON user_account.id = address.user_id


Joins to a Target Entity
^^^^^^^^^^^^^^^^^^^^^^^^

A second form of :meth:`_sql.Select.join` allows any mapped entity or core
selectable construct as a target.   In this usage, :meth:`_sql.Select.join`
will attempt to **infer** the ON clause for the JOIN, using the natural foreign
key relationship between two entities::

    >>> stmt = select(User).join(Address)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

In the above calling form, :meth:`_sql.Select.join` is called upon to infer
the "on clause" automatically.  This calling form will ultimately raise
an error if either there are no :class:`_schema.ForeignKeyConstraint` setup
between the two mapped :class:`_schema.Table` constructs, or if there are multiple
:class:`_schema.ForeignKeyConstraint` linkages between them such that the
appropriate constraint to use is ambiguous.

.. note:: When making use of :meth:`_sql.Select.join` or :meth:`_sql.Select.join_from`
    without indicating an ON clause, ORM
    configured :func:`_orm.relationship` constructs are **not taken into account**.
    Only the configured :class:`_schema.ForeignKeyConstraint` relationships between
    the entities at the level of the mapped :class:`_schema.Table` objects are consulted
    when an attempt is made to infer an ON clause for the JOIN.

.. _queryguide_join_onclause:

Joins to a Target with an ON Clause
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The third calling form allows both the target entity as well
as the ON clause to be passed explicitly.    A example that includes
a SQL expression as the ON clause is as follows::

    >>> stmt = select(User).join(Address, User.id == Address.user_id)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

The expression-based ON clause may also be a :func:`_orm.relationship`-bound
attribute, in the same way it's used in
:ref:`orm_queryguide_simple_relationship_join`::

    >>> stmt = select(User).join(Address, User.addresses)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account JOIN address ON user_account.id = address.user_id

The above example seems redundant in that it indicates the target of ``Address``
in two different ways; however, the utility of this form becomes apparent
when joining to aliased entities; see the section
:ref:`orm_queryguide_joining_relationships_aliased` for an example.

.. _orm_queryguide_join_relationship_onclause_and:

.. _orm_queryguide_join_on_augmented:

Combining Relationship with Custom ON Criteria
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ON clause generated by the :func:`_orm.relationship` construct may
be augmented with additional criteria.  This is useful both for
quick ways to limit the scope of a particular join over a relationship path,
as well as for cases like configuring loader strategies such as
:func:`_orm.joinedload` and :func:`_orm.selectinload`.
The :meth:`_orm.PropComparator.and_`
method accepts a series of SQL expressions positionally that will be joined
to the ON clause of the JOIN via AND.  For example if we wanted to
JOIN from ``User`` to ``Address`` but also limit the ON criteria to only certain
email addresses:

.. sourcecode:: pycon+sql

    >>> stmt = select(User.fullname).join(
    ...     User.addresses.and_(Address.email_address == "squirrel@squirrelpower.org")
    ... )
    >>> session.execute(stmt).all()
    {execsql}SELECT user_account.fullname
    FROM user_account
    JOIN address ON user_account.id = address.user_id AND address.email_address = ?
    [...] ('squirrel@squirrelpower.org',){stop}
    [('Sandy Cheeks',)]

.. seealso::

    The :meth:`_orm.PropComparator.and_` method also works with loader
    strategies such as :func:`_orm.joinedload` and :func:`_orm.selectinload`.
    See the section :ref:`loader_option_criteria`.

.. _tutorial_joining_relationships_aliased:

.. _orm_queryguide_joining_relationships_aliased:

Using Relationship to join between aliased targets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When constructing joins using :func:`_orm.relationship`-bound attributes to indicate
the ON clause, the two-argument syntax illustrated in
:ref:`queryguide_join_onclause` can be expanded to work with the
:func:`_orm.aliased` construct, to indicate a SQL alias as the target of a join
while still making use of the :func:`_orm.relationship`-bound attribute
to  indicate the ON clause, as in the example below, where the ``User``
entity is joined twice to two different :func:`_orm.aliased` constructs
against the ``Address`` entity::

    >>> address_alias_1 = aliased(Address)
    >>> address_alias_2 = aliased(Address)
    >>> stmt = (
    ...     select(User)
    ...     .join(address_alias_1, User.addresses)
    ...     .where(address_alias_1.email_address == "patrick@aol.com")
    ...     .join(address_alias_2, User.addresses)
    ...     .where(address_alias_2.email_address == "patrick@gmail.com")
    ... )
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2

The same pattern may be expressed more succinctly using the
modifier :meth:`_orm.PropComparator.of_type`, which may be applied to the
:func:`_orm.relationship`-bound attribute, passing along the target entity
in order to indicate the target
in one step.   The example below uses :meth:`_orm.PropComparator.of_type`
to produce the same SQL statement as the one just illustrated::

    >>> print(
    ...     select(User)
    ...     .join(User.addresses.of_type(address_alias_1))
    ...     .where(address_alias_1.email_address == "patrick@aol.com")
    ...     .join(User.addresses.of_type(address_alias_2))
    ...     .where(address_alias_2.email_address == "patrick@gmail.com")
    ... )
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2


To make use of a :func:`_orm.relationship` to construct a join **from** an
aliased entity, the attribute is available from the :func:`_orm.aliased`
construct directly::

    >>> user_alias_1 = aliased(User)
    >>> print(select(user_alias_1.name).join(user_alias_1.addresses))
    {printsql}SELECT user_account_1.name
    FROM user_account AS user_account_1
    JOIN address ON user_account_1.id = address.user_id



.. _orm_queryguide_join_subqueries:

Joining to Subqueries
^^^^^^^^^^^^^^^^^^^^^

The target of a join may be any "selectable" entity which includes
subqueries.   When using the ORM, it is typical
that these targets are stated in terms of an
:func:`_orm.aliased` construct, but this is not strictly required, particularly
if the joined entity is not being returned in the results.  For example, to join from the
``User`` entity to the ``Address`` entity, where the ``Address`` entity
is represented as a row limited subquery, we first construct a :class:`_sql.Subquery`
object using :meth:`_sql.Select.subquery`, which may then be used as the
target of the :meth:`_sql.Select.join` method::

    >>> subq = select(Address).where(Address.email_address == "pat999@aol.com").subquery()
    >>> stmt = select(User).join(subq, User.id == subq.c.user_id)
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN (SELECT address.id AS id,
    address.user_id AS user_id, address.email_address AS email_address
    FROM address
    WHERE address.email_address = :email_address_1) AS anon_1
    ON user_account.id = anon_1.user_id{stop}

The above SELECT statement when invoked via :meth:`_orm.Session.execute` will
return rows that contain ``User`` entities, but not ``Address`` entities. In
order to include ``Address`` entities to the set of entities that would be
returned in result sets, we construct an :func:`_orm.aliased` object against
the ``Address`` entity and :class:`.Subquery` object. We also may wish to apply
a name to the :func:`_orm.aliased` construct, such as ``"address"`` used below,
so that we can refer to it by name in the result row::

    >>> address_subq = aliased(Address, subq, name="address")
    >>> stmt = select(User, address_subq).join(address_subq)
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User} {row.address}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname,
    anon_1.id AS id_1, anon_1.user_id, anon_1.email_address
    FROM user_account
    JOIN (SELECT address.id AS id,
    address.user_id AS user_id, address.email_address AS email_address
    FROM address
    WHERE address.email_address = ?) AS anon_1 ON user_account.id = anon_1.user_id
    [...] ('pat999@aol.com',){stop}
    User(id=3, name='patrick', fullname='Patrick Star') Address(id=4, email_address='pat999@aol.com')

Joining to Subqueries along Relationship paths
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The subquery form illustrated in the previous section
may be expressed with more specificity using a
:func:`_orm.relationship`-bound attribute using one of the forms indicated at
:ref:`orm_queryguide_joining_relationships_aliased`. For example, to create the
same join while ensuring the join is along that of a particular
:func:`_orm.relationship`, we may use the
:meth:`_orm.PropComparator.of_type` method, passing the :func:`_orm.aliased`
construct containing the :class:`.Subquery` object that's the target
of the join::

    >>> address_subq = aliased(Address, subq, name="address")
    >>> stmt = select(User, address_subq).join(User.addresses.of_type(address_subq))
    >>> for row in session.execute(stmt):
    ...     print(f"{row.User} {row.address}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname,
    anon_1.id AS id_1, anon_1.user_id, anon_1.email_address
    FROM user_account
    JOIN (SELECT address.id AS id,
    address.user_id AS user_id, address.email_address AS email_address
    FROM address
    WHERE address.email_address = ?) AS anon_1 ON user_account.id = anon_1.user_id
    [...] ('pat999@aol.com',){stop}
    User(id=3, name='patrick', fullname='Patrick Star') Address(id=4, email_address='pat999@aol.com')

Subqueries that Refer to Multiple Entities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A subquery that contains columns spanning more than one ORM entity may be
applied to more than one :func:`_orm.aliased` construct at once, and
used in the same :class:`.Select` construct in terms of each entity separately.
The rendered SQL will continue to treat all such :func:`_orm.aliased`
constructs as the same subquery, however from the ORM / Python perspective
the different return values and object attributes can be referenced
by using the appropriate :func:`_orm.aliased` construct.

Given for example a subquery that refers to both ``User`` and ``Address``::

    >>> user_address_subq = (
    ...     select(User.id, User.name, User.fullname, Address.id, Address.email_address)
    ...     .join_from(User, Address)
    ...     .where(Address.email_address.in_(["pat999@aol.com", "squirrel@squirrelpower.org"]))
    ...     .subquery()
    ... )

We can create :func:`_orm.aliased` constructs against both ``User`` and
``Address`` that each refer to the same object::

    >>> user_alias = aliased(User, user_address_subq, name="user")
    >>> address_alias = aliased(Address, user_address_subq, name="address")

A :class:`.Select` construct selecting from both entities will render the
subquery once, but in a result-row context can return objects of both
``User`` and ``Address`` classes at the same time::

    >>> stmt = select(user_alias, address_alias).where(user_alias.name == "sandy")
    >>> for row in session.execute(stmt):
    ...     print(f"{row.user} {row.address}")
    {execsql}SELECT anon_1.id, anon_1.name, anon_1.fullname, anon_1.id_1, anon_1.email_address
    FROM (SELECT user_account.id AS id, user_account.name AS name,
    user_account.fullname AS fullname, address.id AS id_1,
    address.email_address AS email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE address.email_address IN (?, ?)) AS anon_1
    WHERE anon_1.name = ?
    [...] ('pat999@aol.com', 'squirrel@squirrelpower.org', 'sandy'){stop}
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=3, email_address='squirrel@squirrelpower.org')


.. _orm_queryguide_select_from:

Setting the leftmost FROM clause in a join
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In cases where the left side of the current state of
:class:`_sql.Select` is not in line with what we want to join from,
the :meth:`_sql.Select.join_from` method may be used::

    >>> stmt = select(Address).join_from(User, User.addresses).where(User.name == "sandy")
    >>> print(stmt)
    {printsql}SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1

The :meth:`_sql.Select.join_from` method accepts two or three arguments, either
in the form ``(<join from>, <onclause>)``, or ``(<join from>, <join to>,
[<onclause>])``::

    >>> stmt = select(Address).join_from(User, Address).where(User.name == "sandy")
    >>> print(stmt)
    {printsql}SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1

To set up the initial FROM clause for a SELECT such that :meth:`_sql.Select.join`
can be used subsequent, the :meth:`_sql.Select.select_from` method may also
be used::


    >>> stmt = select(Address).select_from(User).join(Address).where(User.name == "sandy")
    >>> print(stmt)
    {printsql}SELECT address.id, address.user_id, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE user_account.name = :name_1

.. tip::

    The :meth:`_sql.Select.select_from` method does not actually have the
    final say on the order of tables in the FROM clause.    If the statement
    also refers to a :class:`_sql.Join` construct that refers to existing
    tables in a different order, the :class:`_sql.Join` construct takes
    precedence.    When we use methods like :meth:`_sql.Select.join`
    and :meth:`_sql.Select.join_from`, these methods are ultimately creating
    such a :class:`_sql.Join` object.   Therefore we can see the contents
    of :meth:`_sql.Select.select_from` being overridden in a case like this::

        >>> stmt = select(Address).select_from(User).join(Address.user).where(User.name == "sandy")
        >>> print(stmt)
        {printsql}SELECT address.id, address.user_id, address.email_address
        FROM address JOIN user_account ON user_account.id = address.user_id
        WHERE user_account.name = :name_1

    Where above, we see that the FROM clause is ``address JOIN user_account``,
    even though we stated ``select_from(User)`` first. Because of the
    ``.join(Address.user)`` method call, the statement is ultimately equivalent
    to the following::

        >>> from sqlalchemy.sql import join
        >>>
        >>> user_table = User.__table__
        >>> address_table = Address.__table__
        >>>
        >>> j = address_table.join(user_table, user_table.c.id == address_table.c.user_id)
        >>> stmt = (
        ...     select(address_table)
        ...     .select_from(user_table)
        ...     .select_from(j)
        ...     .where(user_table.c.name == "sandy")
        ... )
        >>> print(stmt)
        {printsql}SELECT address.id, address.user_id, address.email_address
        FROM address JOIN user_account ON user_account.id = address.user_id
        WHERE user_account.name = :name_1

    The :class:`_sql.Join` construct above is added as another entry in the
    :meth:`_sql.Select.select_from` list which supersedes the previous entry.


.. _orm_queryguide_relationship_operators:


Relationship WHERE Operators
----------------------------


Besides the use of :func:`_orm.relationship` constructs within the
:meth:`.Select.join` and :meth:`.Select.join_from` methods,
:func:`_orm.relationship` also plays a role in helping to construct
SQL expressions that are typically for use in the WHERE clause, using
the :meth:`.Select.where` method.


.. _orm_queryguide_relationship_exists:

.. _tutorial_relationship_exists:

EXISTS forms: has() / any()
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_sql.Exists` construct was first introduced in the
:ref:`unified_tutorial` in the section :ref:`tutorial_exists`.  This object
is used to render the SQL EXISTS keyword in conjunction with a
scalar subquery.   The :func:`_orm.relationship` construct provides for some
helper methods that may be used to generate some common EXISTS styles
of queries in terms of the relationship.

For a one-to-many relationship such as ``User.addresses``, an EXISTS against
the ``address`` table that correlates back to the ``user_account`` table
can be produced using :meth:`_orm.PropComparator.any`.  This method accepts
an optional WHERE criteria to limit the rows matched by the subquery:

.. sourcecode:: pycon+sql

    >>> stmt = select(User.fullname).where(
    ...     User.addresses.any(Address.email_address == "squirrel@squirrelpower.org")
    ... )
    >>> session.execute(stmt).all()
    {execsql}SELECT user_account.fullname
    FROM user_account
    WHERE EXISTS (SELECT 1
    FROM address
    WHERE user_account.id = address.user_id AND address.email_address = ?)
    [...] ('squirrel@squirrelpower.org',){stop}
    [('Sandy Cheeks',)]

As EXISTS tends to be more efficient for negative lookups, a common query
is to locate entities where there are no related entities present.  This
is succinct using a phrase such as ``~User.addresses.any()``, to select
for ``User`` entities that have no related ``Address`` rows:

.. sourcecode:: pycon+sql

    >>> stmt = select(User.fullname).where(~User.addresses.any())
    >>> session.execute(stmt).all()
    {execsql}SELECT user_account.fullname
    FROM user_account
    WHERE NOT (EXISTS (SELECT 1
    FROM address
    WHERE user_account.id = address.user_id))
    [...] (){stop}
    [('Eugene H. Krabs',)]

The :meth:`_orm.PropComparator.has` method works in mostly the same way as
:meth:`_orm.PropComparator.any`, except that it's used for many-to-one
relationships, such as if we wanted to locate all ``Address`` objects
which belonged to "sandy":

.. sourcecode:: pycon+sql

    >>> stmt = select(Address.email_address).where(Address.user.has(User.name == "sandy"))
    >>> session.execute(stmt).all()
    {execsql}SELECT address.email_address
    FROM address
    WHERE EXISTS (SELECT 1
    FROM user_account
    WHERE user_account.id = address.user_id AND user_account.name = ?)
    [...] ('sandy',){stop}
    [('sandy@sqlalchemy.org',), ('squirrel@squirrelpower.org',)]

.. _orm_queryguide_relationship_common_operators:

Relationship Instance Comparison Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. comment

    >>> session.expunge_all()

The :func:`_orm.relationship`-bound attribute also offers a few SQL construction
implementations that are geared towards filtering a :func:`_orm.relationship`-bound
attribute in terms of a specific instance of a related object, which can unpack
the appropriate attribute values from a given :term:`persistent` (or less
commonly a :term:`detached`) object instance and construct WHERE criteria
in terms of the target :func:`_orm.relationship`.

* **many to one equals comparison** - a specific object instance can be
  compared to many-to-one relationship, to select rows where the
  foreign key of the target entity matches the primary key value of the
  object given::

      >>> user_obj = session.get(User, 1)
      {execsql}SELECT ...{stop}
      >>> print(select(Address).where(Address.user == user_obj))
      {printsql}SELECT address.id, address.user_id, address.email_address
      FROM address
      WHERE :param_1 = address.user_id

  ..

* **many to one not equals comparison** - the not equals operator may also
  be used::

      >>> print(select(Address).where(Address.user != user_obj))
      {printsql}SELECT address.id, address.user_id, address.email_address
      FROM address
      WHERE address.user_id != :user_id_1 OR address.user_id IS NULL

  ..

* **object is contained in a one-to-many collection** - this is essentially
  the one-to-many version of the "equals" comparison, select rows where the
  primary key equals the value of the foreign key in a related object::

      >>> address_obj = session.get(Address, 1)
      {execsql}SELECT ...{stop}
      >>> print(select(User).where(User.addresses.contains(address_obj)))
      {printsql}SELECT user_account.id, user_account.name, user_account.fullname
      FROM user_account
      WHERE user_account.id = :param_1

  ..

* **An object has a particular parent from a one-to-many perspective** - the
  :func:`_orm.with_parent` function produces a comparison that returns rows
  which are referenced by a given parent, this is essentially the
  same as using the ``==`` operator with the many-to-one side::

      >>> from sqlalchemy.orm import with_parent
      >>> print(select(Address).where(with_parent(user_obj, User.addresses)))
      {printsql}SELECT address.id, address.user_id, address.email_address
      FROM address
      WHERE :param_1 = address.user_id



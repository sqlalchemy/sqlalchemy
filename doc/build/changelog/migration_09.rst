==============================
What's New in SQLAlchemy 0.9?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.8,
    undergoing maintenance releases as of May, 2013,
    and SQLAlchemy version 0.9, which is expected for release
    in late 2013.

    Document date: May 29, 2013

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.9,
and also documents changes which affect users migrating
their applications from the 0.8 series of SQLAlchemy to 0.9.

Version 0.9 is a faster-than-usual push from version 0.8,
featuring a more versatile codebase with regards to modern
Python versions.   See :ref:`behavioral_changes_09` for
potentially backwards-incompatible changes.

Platform Support
================

Targeting Python 2.6 and Up Now, Python 3 without 2to3
-------------------------------------------------------

The first achievement of the 0.9 release is to remove the dependency
on the 2to3 tool for Python 3 compatibility.  To make this
more straightforward, the lowest Python release targeted now
is 2.6, which features a wide degree of cross-compatibility with
Python 3.   All SQLAlchemy modules and unit tests are now interpreted
equally well with any Python interpreter from 2.6 forward, including
the 3.1 and 3.2 interpreters.

At the moment, the C extensions are still not fully ported to
Python 3.



.. _behavioral_changes_09:

Behavioral Changes
==================

.. _migration_2736:

:meth:`.Query.select_from` no longer applies the clause to corresponding entities
---------------------------------------------------------------------------------

The :meth:`.Query.select_from` method has been popularized in recent versions
as a means of controlling the first thing that a :class:`.Query` object
"selects from", typically for the purposes of controlling how a JOIN will
render.

Consider the following example against the usual ``User`` mapping::

    select_stmt = select([User]).where(User.id == 7).alias()

    q = session.query(User).\
               join(select_stmt, User.id == select_stmt.c.id).\
               filter(User.name == 'ed')

The above statement predictably renders SQL like the following::

    SELECT "user".id AS user_id, "user".name AS user_name
    FROM "user" JOIN (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 ON "user".id = anon_1.id
    WHERE "user".name = :name_1

If we wanted to reverse the order of the left and right elements of the
JOIN, the documentation would lead us to believe we could use
:meth:`.Query.select_from` to do so::

    q = session.query(User).\
            select_from(select_stmt).\
            join(User, User.id == select_stmt.c.id).\
            filter(User.name == 'ed')

However, in version 0.8 and earlier, the above use of :meth:`.Query.select_from`
would apply the ``select_stmt`` to **replace** the ``User`` entity, as it
selects from the ``user`` table which is compatible with ``User``::

    -- SQLAlchemy 0.8 and earlier...
    SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name
    FROM (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON anon_1.id = anon_1.id
    WHERE anon_1.name = :name_1

The above statement is a mess, the ON clause refers ``anon_1.id = anon_1.id``,
our WHERE clause has been replaced with ``anon_1`` as well.

This behavior is quite intentional, but has a different use case from that
which has become popular for :meth:`.Query.select_from`.  The above behavior
is now available by a new method known as :meth:`.Query.select_entity_from`.
This is a lesser used behavior that in modern SQLAlchemy is roughly equivalent
to selecting from a customized :func:`.aliased` construct::

    select_stmt = select([User]).where(User.id == 7)
    user_from_stmt = aliased(User, select_stmt.alias())

    q = session.query(user_from_stmt).filter(user_from_stmt.name == 'ed')

So with SQLAlchemy 0.9, our query that selects from ``select_stmt`` produces
the SQL we expect::

    -- SQLAlchemy 0.9
    SELECT "user".id AS user_id, "user".name AS user_name
    FROM (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON "user".id = id
    WHERE "user".name = :name_1

The :meth:`.Query.select_entity_from` method will be available in SQLAlchemy
**0.8.2**, so applications which rely on the old behavior can transition
to this method first, ensure all tests continue to function, then upgrade
to 0.9 without issue.

:ticket:`2736`

.. _migration_2751:

Association Proxy SQL Expression Improvements and Fixes
-------------------------------------------------------

The ``==`` and ``!=`` operators as implemented by an association proxy
that refers to a scalar value on a scalar relationship now produces
a more complete SQL expression, intended to take into account
the "association" row being present or not when the comparison is against
``None``.

Consider this mapping::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

        b_id = Column(Integer, ForeignKey('b.id'), primary_key=True)
        b = relationship("B")
        b_value = association_proxy("b", "value")

    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        value = Column(String)

Up through 0.8, a query like the following::

    s.query(A).filter(A.b_value == None).all()

would produce::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL)

In 0.9, it now produces::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE (EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL)) OR a.b_id IS NULL

The difference being, it not only checks ``b.value``, it also checks
if ``a`` refers to no ``b`` row at all.  This will return different
results versus prior versions, for a system that uses this type of
comparison where some parent rows have no association row.

More critically, a correct expression is emitted for ``A.b_value != None``.
In 0.8, this would return ``True`` for ``A`` rows that had no ``b``::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE NOT (EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL))

Now in 0.9, the check has been reworked so that it ensures
the A.b_id row is present, in addition to ``B.value`` being
non-NULL::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NOT NULL)

In addition, the ``has()`` operator is enhanced such that you can
call it against a scalar column value with no criterion only,
and it will produce criteria that checks for the association row
being present or not::

    s.query(A).filter(A.b_value.has()).all()

output::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id)

This is equivalent to ``A.b.has()``, but allows one to query
against ``b_value`` directly.

:ticket:`2751`

New Features
============

.. _feature_722:

INSERT from SELECT
------------------

After literally years of pointless procrastination this relatively minor
syntactical feature has been added, and is also backported to 0.8.3,
so technically isn't "new" in 0.9.   A :func:`.select` construct or other
compatible construct can be passed to the new method :meth:`.Insert.from_select`
where it will be used to render an ``INSERT .. SELECT`` construct::

    >>> from sqlalchemy.sql import table, column
    >>> t1 = table('t1', column('a'), column('b'))
    >>> t2 = table('t2', column('x'), column('y'))
    >>> print(t1.insert().from_select(['a', 'b'], t2.select().where(t2.c.y == 5)))
    INSERT INTO t1 (a, b) SELECT t2.x, t2.y
    FROM t2
    WHERE t2.y = :y_1

The construct is smart enough to also accommodate ORM objects such as classes
and :class:`.Query` objects::

    s = Session()
    q = s.query(User.id, User.name).filter_by(name='ed')
    ins = insert(Address).from_select((Address.id, Address.email_address), q)

rendering::

    INSERT INTO addresses (id, email_address)
    SELECT users.id AS users_id, users.name AS users_name
    FROM users WHERE users.name = :name_1

:ticket:`722`

Behavioral Improvements
=======================

Improvements that should produce no compatibility issues, but are good
to be aware of in case there are unexpected issues.

.. _feature_joins_09:

Many JOIN and LEFT OUTER JOIN expressions will no longer be wrapped in (SELECT * FROM ..) AS ANON_1
---------------------------------------------------------------------------------------------------

For many years, the SQLAlchemy ORM has been held back from being able to nest
a JOIN inside the right side of an existing JOIN (typically a LEFT OUTER JOIN,
as INNER JOINs could always be flattened)::

    SELECT a.*, b.*, c.* FROM a LEFT OUTER JOIN (b JOIN c ON b.id = c.id) ON a.id

This was due to the fact that SQLite, even today, cannot parse a statement of the above format::

    SQLite version 3.7.15.2 2013-01-09 11:53:05
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"
    sqlite> create table a(id integer);
    sqlite> create table b(id integer);
    sqlite> create table c(id integer);
    sqlite> select a.id, b.id, c.id from a left outer join (b join c on b.id=c.id) on b.id=a.id;
    Error: no such column: b.id

Right-outer-joins are of course another way to work around right-side
parenthesization; this would be significantly complicated and visually unpleasant
to implement, but fortunately SQLite doesn't support RIGHT OUTER JOIN either :)::

    sqlite> select a.id, b.id, c.id from b join c on b.id=c.id
       ...> right outer join a on b.id=a.id;
    Error: RIGHT and FULL OUTER JOINs are not currently supported

Back in 2005, it wasn't clear if other databases had trouble with this form,
but today it seems clear every database tested except SQLite now supports it
(Oracle 8, a very old database, doesn't support the JOIN keyword at all,
but SQLAlchemy has always had a simple rewriting scheme in place for Oracle's syntax).
To make matters worse, SQLAlchemy's usual workaround of applying a
SELECT often degrades performance on platforms like Postgresql and MySQL::

    SELECT a.*, anon_1.* FROM a LEFT OUTER JOIN (
                    SELECT b.id AS b_id, c.id AS c_id
                    FROM b JOIN c ON b.id = c.id
                ) AS anon_1 ON a.id=anon_1.b_id

A JOIN like the above form is commonplace when working with joined-table inheritance structures;
any time :meth:`.Query.join` is used to join from some parent to a joined-table subclass, or
when :func:`.joinedload` is used similarly, SQLAlchemy's ORM would always make sure a nested
JOIN was never rendered, lest the query wouldn't be able to run on SQLite.  Even though
the Core has always supported a JOIN of the more compact form, the ORM had to avoid it.

An additional issue would arise when producing joins across many-to-many relationships
where special criteria is present in the ON clause. Consider an eager load join like the following::

    session.query(Order).outerjoin(Order.items)

Assuming a many-to-many from ``Order`` to ``Item`` which actually refers to a subclass
like ``Subitem``, the SQL for the above would look like::

    SELECT order.id, order.name
    FROM order LEFT OUTER JOIN order_item ON order.id = order_item.order_id
    LEFT OUTER JOIN item ON order_item.item_id = item.id AND item.type = 'subitem'

What's wrong with the above query?  Basically, that it will load many ``order`` /
``order_item`` rows where the criteria of ``item.type == 'subitem'`` is not true.

As of SQLAlchemy 0.9, an entirely new approach has been taken.  The ORM no longer
worries about nesting JOINs in the right side of an enclosing JOIN, and it now will
render these as often as possible while still returning the correct results.  When
the SQL statement is passed to be compiled, the **dialect compiler** will **rewrite the join**
to suit the target backend, if that backend is known to not support a right-nested
JOIN (which currently is only SQLite - if other backends have this issue please
let us know!).

So a regular ``query(Parent).join(Subclass)`` will now usually produce a simpler
expression::

    SELECT parent.id AS parent_id
    FROM parent JOIN (
            base_table JOIN subclass_table
            ON base_table.id = subclass_table.id) ON parent.id = base_table.parent_id

Joined eager loads like ``query(Parent).options(joinedload(Parent.subclasses))``
will alias the individual tables instead of wrapping in an ``ANON_1``::

    SELECT parent.*, base_table_1.*, subclass_table_1.* FROM parent
        LEFT OUTER JOIN (
            base_table AS base_table_1 JOIN subclass_table AS subclass_table_1
            ON base_table_1.id = subclass_table_1.id)
            ON parent.id = base_table_1.parent_id

Many-to-many joins and eagerloads will right nest the "secondary" and "right" tables::

    SELECT order.id, order.name
    FROM order LEFT OUTER JOIN
    (order_item JOIN item ON order_item.item_id = item.id AND item.type = 'subitem')
    ON order_item.order_id = order.id

All of these joins, when rendered with a :class:`.Select` statement that specifically
specifies ``use_labels=True``, which is true for all the queries the ORM emits,
are candidates for "join rewriting", which is the process of rewriting all those right-nested
joins into nested SELECT statements, while maintaining the identical labeling used by
the :class:`.Select`.  So SQLite, the one database that won't support this very
common SQL syntax even in 2013, shoulders the extra complexity itself,
with the above queries rewritten as::

    -- sqlite only!
    SELECT parent.id AS parent_id
        FROM parent JOIN (
            SELECT base_table.id AS base_table_id,
                    base_table.parent_id AS base_table_parent_id,
                    subclass_table.id AS subclass_table_id
            FROM base_table JOIN subclass_table ON base_table.id = subclass_table.id
        ) AS anon_1 ON parent.id = anon_1.base_table_parent_id

    -- sqlite only!
    SELECT parent.id AS parent_id, anon_1.subclass_table_1_id AS subclass_table_1_id,
            anon_1.base_table_1_id AS base_table_1_id,
            anon_1.base_table_1_parent_id AS base_table_1_parent_id
    FROM parent LEFT OUTER JOIN (
        SELECT base_table_1.id AS base_table_1_id,
            base_table_1.parent_id AS base_table_1_parent_id,
            subclass_table_1.id AS subclass_table_1_id
        FROM base_table AS base_table_1
        JOIN subclass_table AS subclass_table_1 ON base_table_1.id = subclass_table_1.id
    ) AS anon_1 ON parent.id = anon_1.base_table_1_parent_id

    -- sqlite only!
    SELECT "order".id AS order_id
    FROM "order" LEFT OUTER JOIN (
            SELECT order_item_1.order_id AS order_item_1_order_id,
                order_item_1.item_id AS order_item_1_item_id,
                item.id AS item_id, item.type AS item_type
    FROM order_item AS order_item_1
        JOIN item ON item.id = order_item_1.item_id AND item.type IN (?)
    ) AS anon_1 ON "order".id = anon_1.order_item_1_order_id

The :meth:`.Join.alias`, :func:`.aliased` and :func:`.with_polymorphic` functions now
support a new argument, ``flat=True``, which is used to construct aliases of joined-table
entities without embedding into a SELECT.   This flag is not on by default, to help with
backwards compatibility - but now a "polymorhpic" selectable can be joined as a target
without any subqueries generated::

    employee_alias = with_polymorphic(Person, [Engineer, Manager], flat=True)

    session.query(Company).join(
                        Company.employees.of_type(employee_alias)
                    ).filter(
                        or_(
                            Engineer.primary_language == 'python',
                            Manager.manager_name == 'dilbert'
                        )
                    )

Generates (everywhere except SQLite)::

    SELECT companies.company_id AS companies_company_id, companies.name AS companies_name
    FROM companies JOIN (
        people AS people_1
        LEFT OUTER JOIN engineers AS engineers_1 ON people_1.person_id = engineers_1.person_id
        LEFT OUTER JOIN managers AS managers_1 ON people_1.person_id = managers_1.person_id
    ) ON companies.company_id = people_1.company_id
    WHERE engineers.primary_language = %(primary_language_1)s
        OR managers.manager_name = %(manager_name_1)s

:ticket:`2369` :ticket:`2587`

.. _migration_1068:

Label constructs can now render as their name alone in an ORDER BY
------------------------------------------------------------------

For the case where a :class:`.Label` is used in both the columns clause
as well as the ORDER BY clause of a SELECT, the label will render as
just it's name in the ORDER BY clause, assuming the underlying dialect
reports support of this feature.

E.g. an example like::

    from sqlalchemy.sql import table, column, select, func

    t = table('t', column('c1'), column('c2'))
    expr = (func.foo(t.c.c1) + t.c.c2).label("expr")

    stmt = select([expr]).order_by(expr)

    print stmt

Prior to 0.9 would render as::

    SELECT foo(t.c1) + t.c2 AS expr
    FROM t ORDER BY foo(t.c1) + t.c2

And now renders as::

    SELECT foo(t.c1) + t.c2 AS expr
    FROM t ORDER BY expr

The ORDER BY only renders the label if the label isn't further embedded into an expression within the ORDER BY, other than a simple ``ASC`` or ``DESC``.

The above format works on all databases tested, but might have compatibility issues with older database versions (MySQL 4?  Oracle 8? etc.).   Based on user reports we can add rules
that will disable the feature based on database version detection.

:ticket:`1068`

.. _migration_1765:

Columns can reliably get their type from a column referred to via ForeignKey
----------------------------------------------------------------------------

There's a long standing behavior which says that a :class:`.Column` can be
declared without a type, as long as that :class:`.Column` is referred to
by a :class:`.ForeignKeyConstraint`, and the type from the referenced column
will be copied into this one.   The problem has been that this feature never
worked very well and wasn't maintained.   The core issue was that the
:class:`.ForeignKey` object doesn't know what target :class:`.Column` it
refers to until it is asked, typically the first time the foreign key is used
to construct a :class:`.Join`.   So until that time, the parent :class:`.Column`
would not have a type, or more specifically, it would have a default type
of :class:`.NullType`.

While it's taken a long time, the work to reorganize the initialization of
:class:`.ForeignKey` objects has been completed such that this feature can
finally work acceptably.  At the core of the change is that the :attr:`.ForeignKey.column`
attribute no longer lazily initializes the location of the target :class:`.Column`;
the issue with this system was that the owning :class:`.Column` would be stuck
with :class:`.NullType` as its type until the :class:`.ForeignKey` happened to
be used.

In the new version, the :class:`.ForeignKey` coordinates with the eventual
:class:`.Column` it will refer to using internal attachment events, so that the
moment the referencing :class:`.Column` is associated with the
:class:`.MetaData`, all :class:`.ForeignKey` objects that
refer to it will be sent a message that they need to initialize their parent
column.   This system is more complicated but works more solidly; as a bonus,
there are now tests in place for a wide variety of :class:`.Column` /
:class:`.ForeignKey` configuration scenarios and error messages have been
improved to be very specific to no less than seven different error conditions.

Scenarios which now work correctly include:

1. The type on a :class:`.Column` is immediately present as soon as the
   target :class:`.Column` becomes associated with the same :class:`.MetaData`;
   this works no matter which side is configured first::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKey
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata, Column('t1id', ForeignKey('t1.id')))
    >>> t2.c.t1id.type
    NullType()
    >>> t1 = Table('t1', metadata, Column('id', Integer, primary_key=True))
    >>> t2.c.t1id.type
    Integer()

2. The system now works with :class:`.ForeignKeyConstraint` as well::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKeyConstraint
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata,
    ...     Column('t1a'), Column('t1b'),
    ...     ForeignKeyConstraint(['t1a', 't1b'], ['t1.a', 't1.b']))
    >>> t2.c.t1a.type
    NullType()
    >>> t2.c.t1b.type
    NullType()
    >>> t1 = Table('t1', metadata,
    ...     Column('a', Integer, primary_key=True),
    ...     Column('b', Integer, primary_key=True))
    >>> t2.c.t1a.type
    Integer()
    >>> t2.c.t1b.type
    Integer()

3. It even works for "multiple hops" - that is, a :class:`.ForeignKey` that refers to a
   :class:`.Column` that refers to another :class:`.Column`::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKey
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata, Column('t1id', ForeignKey('t1.id')))
    >>> t3 = Table('t3', metadata, Column('t2t1id', ForeignKey('t2.t1id')))
    >>> t2.c.t1id.type
    NullType()
    >>> t3.c.t2t1id.type
    NullType()
    >>> t1 = Table('t1', metadata, Column('id', Integer, primary_key=True))
    >>> t2.c.t1id.type
    Integer()
    >>> t3.c.t2t1id.type
    Integer()

:ticket:`1765`

Dialect Changes
===============

Firebird ``fdb`` is now the default Firebird dialect.
-----------------------------------------------------

The ``fdb`` dialect is now used if an engine is created without a dialect
specifier, i.e. ``firebird://``.  ``fdb`` is a ``kinterbasdb`` compatible
DBAPI which per the Firebird project is now their official Python driver.

:ticket:`2504`

Firebird ``fdb`` and ``kinterbasdb`` set ``retaining=False`` by default
-----------------------------------------------------------------------

Both the ``fdb`` and ``kinterbasdb`` DBAPIs support a flag ``retaining=True``
which can be passed to the ``commit()`` and ``rollback()`` methods of its
connection.  The documented rationale for this flag is so that the DBAPI
can re-use internal transaction state for subsequent transactions, for the
purposes of improving performance.   However, newer documentation refers
to analyses of Firebird's "garbage collection" which expresses that this flag
can have a negative effect on the database's ability to process cleanup
tasks, and has been reported as *lowering* performance as a result.

It's not clear how this flag is actually usable given this information,
and as it appears to be only a performance enhancing feature, it now defaults
to ``False``.  The value can be controlled by passing the flag ``retaining=True``
to the :func:`.create_engine` call.  This is a new flag which is added as of
0.8.2, so applications on 0.8.2 can begin setting this to ``True`` or ``False``
as desired.

.. seealso::

    :mod:`sqlalchemy.dialects.firebird.fdb`

    :mod:`sqlalchemy.dialects.firebird.kinterbasdb`

    http://pythonhosted.org/fdb/usage-guide.html#retaining-transactions - information
    on the "retaining" flag.

:ticket:`2763`






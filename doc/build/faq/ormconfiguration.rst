ORM Configuration
=================

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_mapper_primary_key:

How do I map a table that has no primary key?
---------------------------------------------

The SQLAlchemy ORM, in order to map to a particular table, needs there to be
at least one column denoted as a primary key column; multiple-column,
i.e. composite, primary keys are of course entirely feasible as well.  These
columns do **not** need to be actually known to the database as primary key
columns, though it's a good idea that they are.  It's only necessary that the columns
*behave* as a primary key does, e.g. as a unique and not nullable identifier
for a row.

Most ORMs require that objects have some kind of primary key defined
because the object in memory must correspond to a uniquely identifiable
row in the database table; at the very least, this allows the
object can be targeted for UPDATE and DELETE statements which will affect only
that object's row and no other.   However, the importance of the primary key
goes far beyond that.  In SQLAlchemy, all ORM-mapped objects are at all times
linked uniquely within a :class:`.Session`
to their specific database row using a pattern called the :term:`identity map`,
a pattern that's central to the unit of work system employed by SQLAlchemy,
and is also key to the most common (and not-so-common) patterns of ORM usage.


.. note::

    It's important to note that we're only talking about the SQLAlchemy ORM; an
    application which builds on Core and deals only with :class:`_schema.Table` objects,
    :func:`_expression.select` constructs and the like, **does not** need any primary key
    to be present on or associated with a table in any way (though again, in SQL, all tables
    should really have some kind of primary key, lest you need to actually
    update or delete specific rows).

In almost all cases, a table does have a so-called :term:`candidate key`, which is a column or series
of columns that uniquely identify a row.  If a table truly doesn't have this, and has actual
fully duplicate rows, the table is not corresponding to `first normal form <https://en.wikipedia.org/wiki/First_normal_form>`_ and cannot be mapped.   Otherwise, whatever columns comprise the best candidate key can be
applied directly to the mapper::

    class SomeClass(Base):
        __table__ = some_table_with_no_pk
        __mapper_args__ = {
            "primary_key": [some_table_with_no_pk.c.uid, some_table_with_no_pk.c.bar]
        }

Better yet is when using fully declared table metadata, use the ``primary_key=True``
flag on those columns::

    class SomeClass(Base):
        __tablename__ = "some_table_with_no_pk"

        uid = Column(Integer, primary_key=True)
        bar = Column(String, primary_key=True)

All tables in a relational database should have primary keys.   Even a many-to-many
association table - the primary key would be the composite of the two association
columns:

.. sourcecode:: sql

    CREATE TABLE my_association (
      user_id INTEGER REFERENCES user(id),
      account_id INTEGER REFERENCES account(id),
      PRIMARY KEY (user_id, account_id)
    )


How do I configure a Column that is a Python reserved word or similar?
----------------------------------------------------------------------

Column-based attributes can be given any name desired in the mapping. See
:ref:`mapper_column_distinct_names`.

How do I get a list of all columns, relationships, mapped attributes, etc. given a mapped class?
-------------------------------------------------------------------------------------------------

This information is all available from the :class:`_orm.Mapper` object.

To get at the :class:`_orm.Mapper` for a particular mapped class, call the
:func:`_sa.inspect` function on it::

    from sqlalchemy import inspect

    mapper = inspect(MyClass)

From there, all information about the class can be accessed through properties
such as:

* :attr:`_orm.Mapper.attrs` - a namespace of all mapped attributes.  The attributes
  themselves are instances of :class:`.MapperProperty`, which contain additional
  attributes that can lead to the mapped SQL expression or column, if applicable.

* :attr:`_orm.Mapper.column_attrs` - the mapped attribute namespace
  limited to column and SQL expression attributes.   You might want to use
  :attr:`_orm.Mapper.columns` to get at the :class:`_schema.Column` objects directly.

* :attr:`_orm.Mapper.relationships` - namespace of all :class:`.RelationshipProperty` attributes.

* :attr:`_orm.Mapper.all_orm_descriptors` - namespace of all mapped attributes, plus user-defined
  attributes defined using systems such as :class:`.hybrid_property`, :class:`.AssociationProxy` and others.

* :attr:`_orm.Mapper.columns` - A namespace of :class:`_schema.Column` objects and other named
  SQL expressions associated with the mapping.

* :attr:`_orm.Mapper.persist_selectable` - The :class:`_schema.Table` or other selectable to which
  this mapper is mapped.

* :attr:`_orm.Mapper.local_table` - The :class:`_schema.Table` that is "local" to this mapper;
  this differs from :attr:`_orm.Mapper.persist_selectable` in the case of a mapper mapped
  using inheritance to a composed selectable.

.. _faq_combining_columns:

I'm getting a warning or error about "Implicitly combining column X under attribute Y"
--------------------------------------------------------------------------------------

This condition refers to when a mapping contains two columns that are being
mapped under the same attribute name due to their name, but there's no indication
that this is intentional.  A mapped class needs to have explicit names for
every attribute that is to store an independent value; when two columns have the
same name and aren't disambiguated, they fall under the same attribute and
the effect is that the value from one column is **copied** into the other, based
on which column was assigned to the attribute first.

This behavior is often desirable and is allowed without warning in the case
where the two columns are linked together via a foreign key relationship
within an inheritance mapping.   When the warning or exception occurs, the
issue can be resolved by either assigning the columns to differently-named
attributes, or if combining them together is desired, by using
:func:`.column_property` to make this explicit.

Given the example as follows::

    from sqlalchemy import Integer, Column, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()


    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)


    class B(A):
        __tablename__ = "b"

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey("a.id"))

As of SQLAlchemy version 0.9.5, the above condition is detected, and will
warn that the ``id`` column of ``A`` and ``B`` is being combined under
the same-named attribute ``id``, which above is a serious issue since it means
that a ``B`` object's primary key will always mirror that of its ``A``.

A mapping which resolves this is as follows::

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)


    class B(A):
        __tablename__ = "b"

        b_id = Column("id", Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey("a.id"))

Suppose we did want ``A.id`` and ``B.id`` to be mirrors of each other, despite
the fact that ``B.a_id`` is where ``A.id`` is related.  We could combine
them together using :func:`.column_property`::

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)


    class B(A):
        __tablename__ = "b"

        # probably not what you want, but this is a demonstration
        id = column_property(Column(Integer, primary_key=True), A.id)
        a_id = Column(Integer, ForeignKey("a.id"))

I'm using Declarative and setting primaryjoin/secondaryjoin using an ``and_()`` or ``or_()``, and I am getting an error message about foreign keys.
------------------------------------------------------------------------------------------------------------------------------------------------------------------

Are you doing this?::

    class MyClass(Base):
        # ....

        foo = relationship(
            "Dest", primaryjoin=and_("MyClass.id==Dest.foo_id", "MyClass.foo==Dest.bar")
        )

That's an ``and_()`` of two string expressions, which SQLAlchemy cannot apply any mapping towards.  Declarative allows :func:`_orm.relationship` arguments to be specified as strings, which are converted into expression objects using ``eval()``.   But this doesn't occur inside of an ``and_()`` expression - it's a special operation declarative applies only to the *entirety* of what's passed to primaryjoin or other arguments as a string::

    class MyClass(Base):
        # ....

        foo = relationship(
            "Dest", primaryjoin="and_(MyClass.id==Dest.foo_id, MyClass.foo==Dest.bar)"
        )

Or if the objects you need are already available, skip the strings::

    class MyClass(Base):
        # ....

        foo = relationship(
            Dest, primaryjoin=and_(MyClass.id == Dest.foo_id, MyClass.foo == Dest.bar)
        )

The same idea applies to all the other arguments, such as ``foreign_keys``::

    # wrong !
    foo = relationship(Dest, foreign_keys=["Dest.foo_id", "Dest.bar_id"])

    # correct !
    foo = relationship(Dest, foreign_keys="[Dest.foo_id, Dest.bar_id]")

    # also correct !
    foo = relationship(Dest, foreign_keys=[Dest.foo_id, Dest.bar_id])


    # if you're using columns from the class that you're inside of, just use the column objects !
    class MyClass(Base):
        foo_id = Column(...)
        bar_id = Column(...)
        # ...

        foo = relationship(Dest, foreign_keys=[foo_id, bar_id])

.. _faq_subqueryload_limit_sort:

Why is ``ORDER BY`` recommended with ``LIMIT`` (especially with ``subqueryload()``)?
------------------------------------------------------------------------------------

When ORDER BY is not used for a SELECT statement that returns rows, the
relational database is free to returned matched rows in any arbitrary
order.  While this ordering very often corresponds to the natural
order of rows within a table, this is not the case for all databases and all
queries. The consequence of this is that any query that limits rows using
``LIMIT`` or ``OFFSET``, or which merely selects the first row of the result,
discarding the rest, will not be deterministic in terms of what result row is
returned, assuming there's more than one row that matches the query's criteria.

While we may not notice this for simple queries on databases that usually
returns rows in their natural order, it becomes more of an issue if we
also use :func:`_orm.subqueryload` to load related collections, and we may not
be loading the collections as intended.

SQLAlchemy implements :func:`_orm.subqueryload` by issuing a separate query,
the results of which are matched up to the results from the first query.
We see two queries emitted like this:

.. sourcecode:: pycon+sql

    >>> session.scalars(select(User).options(subqueryload(User.addresses))).all()
    {execsql}-- the "main" query
    SELECT users.id AS users_id
    FROM users
    {stop}
    {execsql}-- the "load" query issued by subqueryload
    SELECT addresses.id AS addresses_id,
           addresses.user_id AS addresses_user_id,
           anon_1.users_id AS anon_1_users_id
    FROM (SELECT users.id AS users_id FROM users) AS anon_1
    JOIN addresses ON anon_1.users_id = addresses.user_id
    ORDER BY anon_1.users_id

The second query embeds the first query as a source of rows.
When the inner query uses ``OFFSET`` and/or ``LIMIT`` without ordering,
the two queries may not see the same results:

.. sourcecode:: pycon+sql

    >>> user = session.scalars(
    ...     select(User).options(subqueryload(User.addresses)).limit(1)
    ... ).first()
    {execsql}-- the "main" query
    SELECT users.id AS users_id
    FROM users
     LIMIT 1
    {stop}
    {execsql}-- the "load" query issued by subqueryload
    SELECT addresses.id AS addresses_id,
           addresses.user_id AS addresses_user_id,
           anon_1.users_id AS anon_1_users_id
    FROM (SELECT users.id AS users_id FROM users LIMIT 1) AS anon_1
    JOIN addresses ON anon_1.users_id = addresses.user_id
    ORDER BY anon_1.users_id

Depending on database specifics, there is
a chance we may get a result like the following for the two queries:

.. sourcecode:: text

    -- query #1
    +--------+
    |users_id|
    +--------+
    |       1|
    +--------+

    -- query #2
    +------------+-----------------+---------------+
    |addresses_id|addresses_user_id|anon_1_users_id|
    +------------+-----------------+---------------+
    |           3|                2|              2|
    +------------+-----------------+---------------+
    |           4|                2|              2|
    +------------+-----------------+---------------+

Above, we receive two ``addresses`` rows for ``user.id`` of 2, and none for
1.  We've wasted two rows and failed to actually load the collection.  This
is an insidious error because without looking at the SQL and the results, the
ORM will not show that there's any issue; if we access the ``addresses``
for the ``User`` we have, it will emit a lazy load for the collection and we
won't see that anything actually went wrong.

The solution to this problem is to always specify a deterministic sort order,
so that the main query always returns the same set of rows. This generally
means that you should :meth:`_sql.Select.order_by` on a unique column on the table.
The primary key is a good choice for this::

    session.scalars(
        select(User).options(subqueryload(User.addresses)).order_by(User.id).limit(1)
    ).first()

Note that the :func:`_orm.joinedload` eager loader strategy does not suffer from
the same problem because only one query is ever issued, so the load query
cannot be different from the main query.  Similarly, the :func:`.selectinload`
eager loader strategy also does not have this issue as it links its collection
loads directly to primary key values just loaded.

.. seealso::

    :ref:`subquery_eager_loading`

.. _defaults_default_factory_insert_default:

What are ``default``, ``default_factory`` and ``insert_default`` and what should I use?
---------------------------------------------------------------------------------------

There's a bit of a clash in SQLAlchemy's API here due to the addition of PEP-681
dataclass transforms, which is strict about its naming conventions. PEP-681 comes
into play if you are using :class:`_orm.MappedAsDataclass` as shown in :ref:`orm_declarative_native_dataclasses`.
If you are not using MappedAsDataclass, then it does not apply.

Part One - Classic SQLAlchemy that is not using dataclasses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When **not** using :class:`_orm.MappedAsDataclass`, as has been the case for many years
in SQLAlchemy, the :func:`_orm.mapped_column` (and :class:`_schema.Column`)
construct supports a parameter :paramref:`_orm.mapped_column.default`.
This indicates a Python-side default (as opposed to a server side default that
would be part of your database's schema definition) that will take place when
an ``INSERT`` statement is emitted. This default can be **any** of a static Python value
like a string, **or** a Python callable function, **or** a SQLAlchemy SQL construct.
Full documentation for :paramref:`_orm.mapped_column.default` is at
:ref:`defaults_client_invoked_sql`.

When using :paramref:`_orm.mapped_column.default` with an ORM mapping that is **not**
using :class:`_orm.MappedAsDataclass`, this default value /callable **does not show
up on your object when you first construct it**. It only takes place when SQLAlchemy
works up an ``INSERT`` statement for your object.

A very important thing to note is that when using :func:`_orm.mapped_column`
(and :class:`_schema.Column`), the classic :paramref:`_orm.mapped_column.default`
parameter is also available under a new name, called
:paramref:`_orm.mapped_column.insert_default`. If you build a
:func:`_orm.mapped_column` and you are **not** using :class:`_orm.MappedAsDataclass`, the
:paramref:`_orm.mapped_column.default` and :paramref:`_orm.mapped_column.insert_default`
parameters are **synonymous**.

Part Two - Using Dataclasses support with MappedAsDataclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionchanged:: 2.1 The behavior of column level defaults when using
   dataclasses has changed to use an approach that uses class-level descriptors
   to provide class behavior, in conjunction with Core-level column defaults
   to provide the correct INSERT behavior. See :ref:`change_12168` for
   background.

When you **are** using :class:`_orm.MappedAsDataclass`, that is, the specific form
of mapping used at :ref:`orm_declarative_native_dataclasses`, the meaning of the
:paramref:`_orm.mapped_column.default` keyword changes. We recognize that it's not
ideal that this name changes its behavior, however there was no alternative as
PEP-681 requires :paramref:`_orm.mapped_column.default` to take on this meaning.

When dataclasses are used, the :paramref:`_orm.mapped_column.default` parameter
must be used the way it's described at `Python Dataclasses
<https://docs.python.org/3/library/dataclasses.html>`_ - it refers to a
constant value like a string or a number, and **is available on your object
immediately when constructed**.  As of SQLAlchemy 2.1, the value is delivered
using a descriptor if not otherwise set, without the value actually being
placed in ``__dict__`` unless it were passed to the constructor explicitly.

The value used for :paramref:`_orm.mapped_column.default` is also applied to the
:paramref:`_schema.Column.default` parameter of :class:`_schema.Column`.
This is so that the value used as the dataclass default is also applied in
an ORM INSERT statement for a mapped object where the value was not
explicitly passed.  Using this parameter is **mutually exclusive** against the
:paramref:`_schema.Column.insert_default` parameter, meaning that both cannot
be used at the same time.

The :paramref:`_orm.mapped_column.default` and
:paramref:`_orm.mapped_column.insert_default` parameters may also be used
(one or the other, not both)
for a SQLAlchemy-mapped dataclass field, or for a dataclass overall,
that indicates ``init=False``.
In this usage, if :paramref:`_orm.mapped_column.default` is used, the default
value will be available on the constructed object immediately as well as
used within the INSERT statement.  If :paramref:`_orm.mapped_column.insert_default`
is used, the constructed object will return ``None`` for the attribute value,
but the default value will still be used for the INSERT statement.

To use a callable to generate defaults for the dataclass, which would be
applied to the object when constructed by populating it into ``__dict__``,
:paramref:`_orm.mapped_column.default_factory` may be used instead.

.. list-table:: Summary Chart
   :header-rows: 1

   * - Construct
     - Works with dataclasses?
     - Works without dataclasses?
     - Accepts scalar?
     - Accepts callable?
     - Available on object immediately?
   * - :paramref:`_orm.mapped_column.default`
     - ✔
     - ✔
     - ✔
     - Only if no dataclasses
     - Only if dataclasses
   * - :paramref:`_orm.mapped_column.insert_default`
     - ✔ (only if no ``default``)
     - ✔
     - ✔
     - ✔
     - ✖
   * - :paramref:`_orm.mapped_column.default_factory`
     - ✔
     - ✖
     - ✖
     - ✔
     - Only if dataclasses

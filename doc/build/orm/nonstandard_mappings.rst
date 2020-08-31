========================
Non-Traditional Mappings
========================

.. _orm_mapping_joins:

.. _maptojoin:

Mapping a Class against Multiple Tables
=======================================

Mappers can be constructed against arbitrary relational units (called
*selectables*) in addition to plain tables. For example, the :func:`_expression.join`
function creates a selectable unit comprised of
multiple tables, complete with its own composite primary key, which can be
mapped in the same way as a :class:`_schema.Table`::

    from sqlalchemy import Table, Column, Integer, \
            String, MetaData, join, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import column_property

    metadata = MetaData()

    # define two Table objects
    user_table = Table('user', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String),
            )

    address_table = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String)
                )

    # define a join between them.  This
    # takes place across the user.id and address.user_id
    # columns.
    user_address_join = join(user_table, address_table)

    Base = declarative_base()

    # map to it
    class AddressUser(Base):
        __table__ = user_address_join

        id = column_property(user_table.c.id, address_table.c.user_id)
        address_id = address_table.c.id

In the example above, the join expresses columns for both the
``user`` and the ``address`` table.  The ``user.id`` and ``address.user_id``
columns are equated by foreign key, so in the mapping they are defined
as one attribute, ``AddressUser.id``, using :func:`.column_property` to
indicate a specialized column mapping.   Based on this part of the
configuration, the mapping will copy
new primary key values from ``user.id`` into the ``address.user_id`` column
when a flush occurs.

Additionally, the ``address.id`` column is mapped explicitly to
an attribute named ``address_id``.   This is to **disambiguate** the
mapping of the ``address.id`` column from the same-named ``AddressUser.id``
attribute, which here has been assigned to refer to the ``user`` table
combined with the ``address.user_id`` foreign key.

The natural primary key of the above mapping is the composite of
``(user.id, address.id)``, as these are the primary key columns of the
``user`` and ``address`` table combined together.  The identity of an
``AddressUser`` object will be in terms of these two values, and
is represented from an ``AddressUser`` object as
``(AddressUser.id, AddressUser.address_id)``.

When referring to the ``AddressUser.id`` column, most SQL expressions will
make use of only the first column in the list of columns mapped, as the
two columns are synonymous.  However, for the special use case such as
a GROUP BY expression where both columns must be referenced at the same
time while making use of the proper context, that is, accommodating for
aliases and similar, the accessor :attr:`.ColumnProperty.Comparator.expressions`
may be used::

    q = session.query(AddressUser).group_by(*AddressUser.id.expressions)

.. versionadded:: 1.3.17 Added the
   :attr:`.ColumnProperty.Comparator.expressions` accessor.


.. note::

    A mapping against multiple tables as illustrated above supports
    persistence, that is, INSERT, UPDATE and DELETE of rows within the targeted
    tables. However, it does not support an operation that would UPDATE one
    table and perform INSERT or DELETE on others at the same time for one
    record. That is, if a record PtoQ is mapped to tables “p” and “q”, where it
    has a row based on a LEFT OUTER JOIN of “p” and “q”, if an UPDATE proceeds
    that is to alter data in the “q” table in an existing record, the row in
    “q” must exist; it won’t emit an INSERT if the primary key identity is
    already present.  If the row does not exist, for most DBAPI drivers which
    support reporting the number of rows affected by an UPDATE, the ORM will
    fail to detect an updated row and raise an error; otherwise, the data
    would be silently ignored.

    A recipe to allow for an on-the-fly “insert” of the related row might make
    use of the .MapperEvents.before_update event and look like::

        from sqlalchemy import event

        @event.listens_for(PtoQ, 'before_update')
        def receive_before_update(mapper, connection, target):
           if target.some_required_attr_on_q is None:
                connection.execute(q_table.insert(), {"id": target.id})

    where above, a row is INSERTed into the ``q_table`` table by creating an
    INSERT construct with :meth:`_schema.Table.insert`, then executing it  using the
    given :class:`_engine.Connection` which is the same one being used to emit other
    SQL for the flush process.   The user-supplied logic would have to detect
    that the LEFT OUTER JOIN from "p" to "q" does not have an entry for the "q"
    side.

.. _orm_mapping_arbitrary_subqueries:

Mapping a Class against Arbitrary Subqueries
============================================

Similar to mapping against a join, a plain :func:`_expression.select` object
can be used with a mapper as well.  The example fragment below illustrates
mapping a class called ``Customer`` to a :func:`_expression.select` which
includes a join to a subquery::

    from sqlalchemy import select, func

    subq = select(
        func.count(orders.c.id).label('order_count'),
        func.max(orders.c.price).label('highest_order'),
        orders.c.customer_id
    ).group_by(orders.c.customer_id).subquery()

    customer_select = select(customers, subq).join_from(
        customers, subq, customers.c.id == subq.c.customer_id
    ).subquery()

    class Customer(Base):
        __table__ = customer_select

Above, the full row represented by ``customer_select`` will be all the
columns of the ``customers`` table, in addition to those columns
exposed by the ``subq`` subquery, which are ``order_count``,
``highest_order``, and ``customer_id``.  Mapping the ``Customer``
class to this selectable then creates a class which will contain
those attributes.

When the ORM persists new instances of ``Customer``, only the
``customers`` table will actually receive an INSERT.  This is because the
primary key of the ``orders`` table is not represented in the mapping;  the ORM
will only emit an INSERT into a table for which it has mapped the primary
key.

.. note::

    The practice of mapping to arbitrary SELECT statements, especially
    complex ones as above, is
    almost never needed; it necessarily tends to produce complex queries
    which are often less efficient than that which would be produced
    by direct query construction.   The practice is to some degree
    based on the very early history of SQLAlchemy where the :func:`.mapper`
    construct was meant to represent the primary querying interface;
    in modern usage, the :class:`_query.Query` object can be used to construct
    virtually any SELECT statement, including complex composites, and should
    be favored over the "map-to-selectable" approach.

Multiple Mappers for One Class
==============================

In modern SQLAlchemy, a particular class is mapped by only one so-called
**primary** mapper at a time.   This mapper is involved in three main areas of
functionality: querying, persistence, and instrumentation of the mapped class.
The rationale of the primary mapper relates to the fact that the
:func:`.mapper` modifies the class itself, not only persisting it towards a
particular :class:`_schema.Table`, but also :term:`instrumenting` attributes upon the
class which are structured specifically according to the table metadata.   It's
not possible for more than one mapper to be associated with a class in equal
measure, since only one mapper can actually instrument the class.

The concept of a "non-primary" mapper had existed for many versions of
SQLAlchemy however as of version 1.3 this feature is deprecated.   The
one case where such a non-primary mapper is useful is when constructing
a relationship to a class against an alternative selectable.   This
use case is now suited using the :class:`.aliased` construct and is described
at :ref:`relationship_aliased_class`.

As far as the use case of a class that can actually be fully persisted
to different tables under different scenarios, very early versions of
SQLAlchemy offered a feature for this adapted from Hibernate, known
as the "entity name" feature.  However, this use case became infeasible
within SQLAlchemy once the mapped class itself became the source of SQL
expression construction; that is, the class' attributes themselves link
directly to mapped table columns.   The feature was removed and replaced
with a simple recipe-oriented approach to accomplishing this task
without any ambiguity of instrumentation - to create new subclasses, each
mapped individually.  This pattern is now available as a recipe at `Entity Name
<http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.


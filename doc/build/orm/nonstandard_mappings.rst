========================
Non-Traditional Mappings
========================

.. _maptojoin:

Mapping a Class against Multiple Tables
========================================

Mappers can be constructed against arbitrary relational units (called
*selectables*) in addition to plain tables. For example, the :func:`~.expression.join`
function creates a selectable unit comprised of
multiple tables, complete with its own composite primary key, which can be
mapped in the same way as a :class:`.Table`::

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


Mapping a Class against Arbitrary Selects
=========================================

Similar to mapping against a join, a plain :func:`~.expression.select` object can be used with a
mapper as well.  The example fragment below illustrates mapping a class
called ``Customer`` to a :func:`~.expression.select` which includes a join to a
subquery::

    from sqlalchemy import select, func

    subq = select([
                func.count(orders.c.id).label('order_count'),
                func.max(orders.c.price).label('highest_order'),
                orders.c.customer_id
                ]).group_by(orders.c.customer_id).alias()

    customer_select = select([customers, subq]).\
                select_from(
                    join(customers, subq,
                            customers.c.id == subq.c.customer_id)
                ).alias()

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
    in modern usage, the :class:`.Query` object can be used to construct
    virtually any SELECT statement, including complex composites, and should
    be favored over the "map-to-selectable" approach.

Multiple Mappers for One Class
==============================

In modern SQLAlchemy, a particular class is mapped by only one so-called
**primary** mapper at a time.   This mapper is involved in three main
areas of functionality: querying, persistence, and instrumentation of the
mapped class.   The rationale of the primary mapper relates to the fact
that the :func:`.mapper` modifies the class itself, not only
persisting it towards a particular :class:`.Table`, but also :term:`instrumenting`
attributes upon the class which are structured specifically according to the
table metadata.   It's not possible for more than one mapper
to be associated with a class in equal measure, since only one mapper can
actually instrument the class.

However, there is a class of mapper known as the **non primary** mapper
with allows additional mappers to be associated with a class, but with
a limited scope of use.   This scope typically applies to
being able to load rows from an alternate table or selectable unit, but
still producing classes which are ultimately persisted using the primary
mapping.    The non-primary mapper is created using the classical style
of mapping against a class that is already mapped with a primary mapper,
and involves the use of the :paramref:`~sqlalchemy.orm.mapper.non_primary`
flag.

The non primary mapper is of very limited use in modern SQLAlchemy, as the
task of being able to load classes from subqueries or other compound statements
can be now accomplished using the :class:`.Query` object directly.

There is really only one use case for the non-primary mapper, which is that
we wish to build a :func:`.relationship` to such a mapper; this is useful
in the rare and advanced case that our relationship is attempting to join two
classes together using many tables and/or joins in between.  An example of this
pattern is at :ref:`relationship_non_primary_mapper`.

As far as the use case of a class that can actually be fully persisted
to different tables under different scenarios, very early versions of
SQLAlchemy offered a feature for this adapted from Hibernate, known
as the "entity name" feature.  However, this use case became infeasable
within SQLAlchemy once the mapped class itself became the source of SQL
expression construction; that is, the class' attributes themselves link
directly to mapped table columns.   The feature was removed and replaced
with a simple recipe-oriented approach to accomplishing this task
without any ambiguity of instrumentation - to create new subclasses, each
mapped individually.  This pattern is now available as a recipe at `Entity Name
<http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.


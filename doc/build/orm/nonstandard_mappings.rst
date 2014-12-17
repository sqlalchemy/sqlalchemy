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

In modern SQLAlchemy, a particular class is only mapped by one :func:`.mapper`
at a time.  The rationale here is that the :func:`.mapper` modifies the class itself, not only
persisting it towards a particular :class:`.Table`, but also *instrumenting*
attributes upon the class which are structured specifically according to the
table metadata.

One potential use case for another mapper to exist at the same time is if we
wanted to load instances of our class not just from the immediate :class:`.Table`
to which it is mapped, but from another selectable that is a derivation of that
:class:`.Table`.   To create a second mapper that only handles querying
when used explicitly, we can use the :paramref:`.mapper.non_primary` argument.
In practice, this approach is usually not needed, as we
can do this sort of thing at query time using methods such as
:meth:`.Query.select_from`, however it is useful in the rare case that we
wish to build a :func:`.relationship` to such a mapper.  An example of this is
at :ref:`relationship_non_primary_mapper`.

Another potential use is if we genuinely want instances of our class to
be persisted into different tables at different times; certain kinds of
data sharding configurations may persist a particular class into tables
that are identical in structure except for their name.   For this kind of
pattern, Python offers a better approach than the complexity of mapping
the same class multiple times, which is to instead create new mapped classes
for each target table.    SQLAlchemy refers to this as the "entity name"
pattern, which is described as a recipe at `Entity Name
<http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.


.. _relationship_configure_joins:

Configuring how Relationship Joins
----------------------------------

:func:`_orm.relationship` will normally create a join between two tables
by examining the foreign key relationship between the two tables
to determine which columns should be compared.  There are a variety
of situations where this behavior needs to be customized.

.. _relationship_foreign_keys:

Handling Multiple Join Paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One of the most common situations to deal with is when
there are more than one foreign key path between two tables.

Consider a ``Customer`` class that contains two foreign keys to an ``Address``
class::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Customer(Base):
        __tablename__ = "customer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        billing_address_id = mapped_column(Integer, ForeignKey("address.id"))
        shipping_address_id = mapped_column(Integer, ForeignKey("address.id"))

        billing_address = relationship("Address")
        shipping_address = relationship("Address")


    class Address(Base):
        __tablename__ = "address"
        id = mapped_column(Integer, primary_key=True)
        street = mapped_column(String)
        city = mapped_column(String)
        state = mapped_column(String)
        zip = mapped_column(String)

The above mapping, when we attempt to use it, will produce the error:

.. sourcecode:: text

    sqlalchemy.exc.AmbiguousForeignKeysError: Could not determine join
    condition between parent/child tables on relationship
    Customer.billing_address - there are multiple foreign key
    paths linking the tables.  Specify the 'foreign_keys' argument,
    providing a list of those columns which should be
    counted as containing a foreign key reference to the parent table.

The above message is pretty long.  There are many potential messages
that :func:`_orm.relationship` can return, which have been carefully tailored
to detect a variety of common configurational issues; most will suggest
the additional configuration that's needed to resolve the ambiguity
or other missing information.

In this case, the message wants us to qualify each :func:`_orm.relationship`
by instructing for each one which foreign key column should be considered, and
the appropriate form is as follows::

    class Customer(Base):
        __tablename__ = "customer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        billing_address_id = mapped_column(Integer, ForeignKey("address.id"))
        shipping_address_id = mapped_column(Integer, ForeignKey("address.id"))

        billing_address = relationship("Address", foreign_keys=[billing_address_id])
        shipping_address = relationship("Address", foreign_keys=[shipping_address_id])

Above, we specify the ``foreign_keys`` argument, which is a :class:`_schema.Column` or list
of :class:`_schema.Column` objects which indicate those columns to be considered "foreign",
or in other words, the columns that contain a value referring to a parent table.
Loading the ``Customer.billing_address`` relationship from a ``Customer``
object will use the value present in ``billing_address_id`` in order to
identify the row in ``Address`` to be loaded; similarly, ``shipping_address_id``
is used for the ``shipping_address`` relationship.   The linkage of the two
columns also plays a role during persistence; the newly generated primary key
of a just-inserted ``Address`` object will be copied into the appropriate
foreign key column of an associated ``Customer`` object during a flush.

When specifying ``foreign_keys`` with Declarative, we can also use string
names to specify, however it is important that if using a list, the **list
is part of the string**::

        billing_address = relationship("Address", foreign_keys="[Customer.billing_address_id]")

In this specific example, the list is not necessary in any case as there's only
one :class:`_schema.Column` we need::

        billing_address = relationship("Address", foreign_keys="Customer.billing_address_id")

.. warning:: When passed as a Python-evaluable string, the
    :paramref:`_orm.relationship.foreign_keys` argument is interpreted using Python's
    ``eval()`` function. **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**. See
    :ref:`declarative_relationship_eval` for details on declarative
    evaluation of :func:`_orm.relationship` arguments.


.. _relationship_primaryjoin:

Specifying Alternate Join Conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default behavior of :func:`_orm.relationship` when constructing a join
is that it equates the value of primary key columns
on one side to that of foreign-key-referring columns on the other.
We can change this criterion to be anything we'd like using the
:paramref:`_orm.relationship.primaryjoin`
argument, as well as the :paramref:`_orm.relationship.secondaryjoin`
argument in the case when a "secondary" table is used.

In the example below, using the ``User`` class
as well as an ``Address`` class which stores a street address,  we
create a relationship ``boston_addresses`` which will only
load those ``Address`` objects which specify a city of "Boston"::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)
        boston_addresses = relationship(
            "Address",
            primaryjoin="and_(User.id==Address.user_id, Address.city=='Boston')",
        )


    class Address(Base):
        __tablename__ = "address"
        id = mapped_column(Integer, primary_key=True)
        user_id = mapped_column(Integer, ForeignKey("user.id"))

        street = mapped_column(String)
        city = mapped_column(String)
        state = mapped_column(String)
        zip = mapped_column(String)

Within this string SQL expression, we made use of the :func:`.and_` conjunction
construct to establish two distinct predicates for the join condition - joining
both the ``User.id`` and ``Address.user_id`` columns to each other, as well as
limiting rows in ``Address`` to just ``city='Boston'``.   When using
Declarative, rudimentary SQL functions like :func:`.and_` are automatically
available in the evaluated namespace of a string :func:`_orm.relationship`
argument.

.. warning:: When passed as a Python-evaluable string, the
    :paramref:`_orm.relationship.primaryjoin` argument is interpreted using
    Python's
    ``eval()`` function. **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**. See
    :ref:`declarative_relationship_eval` for details on declarative
    evaluation of :func:`_orm.relationship` arguments.


The custom criteria we use in a :paramref:`_orm.relationship.primaryjoin`
is generally only significant when SQLAlchemy is rendering SQL in
order to load or represent this relationship. That is, it's used in
the SQL statement that's emitted in order to perform a per-attribute
lazy load, or when a join is constructed at query time, such as via
:meth:`Select.join`, or via the eager "joined" or "subquery" styles of
loading.   When in-memory objects are being manipulated, we can place
any ``Address`` object we'd like into the ``boston_addresses``
collection, regardless of what the value of the ``.city`` attribute
is.   The objects will remain present in the collection until the
attribute is expired and re-loaded from the database where the
criterion is applied.   When a flush occurs, the objects inside of
``boston_addresses`` will be flushed unconditionally, assigning value
of the primary key ``user.id`` column onto the foreign-key-holding
``address.user_id`` column for each row.  The ``city`` criteria has no
effect here, as the flush process only cares about synchronizing
primary key values into referencing foreign key values.

.. _relationship_custom_foreign:

Creating Custom Foreign Conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another element of the primary join condition is how those columns
considered "foreign" are determined.  Usually, some subset
of :class:`_schema.Column` objects will specify :class:`_schema.ForeignKey`, or otherwise
be part of a :class:`_schema.ForeignKeyConstraint` that's relevant to the join condition.
:func:`_orm.relationship` looks to this foreign key status as it decides
how it should load and persist data for this relationship.   However, the
:paramref:`_orm.relationship.primaryjoin` argument can be used to create a join condition that
doesn't involve any "schema" level foreign keys.  We can combine :paramref:`_orm.relationship.primaryjoin`
along with :paramref:`_orm.relationship.foreign_keys` and :paramref:`_orm.relationship.remote_side` explicitly in order to
establish such a join.

Below, a class ``HostEntry`` joins to itself, equating the string ``content``
column to the ``ip_address`` column, which is a PostgreSQL type called ``INET``.
We need to use :func:`.cast` in order to cast one side of the join to the
type of the other::

    from sqlalchemy import cast, String, Column, Integer
    from sqlalchemy.orm import relationship
    from sqlalchemy.dialects.postgresql import INET

    from sqlalchemy.orm import DeclarativeBase


    class Base(DeclarativeBase):
        pass


    class HostEntry(Base):
        __tablename__ = "host_entry"

        id = mapped_column(Integer, primary_key=True)
        ip_address = mapped_column(INET)
        content = mapped_column(String(50))

        # relationship() using explicit foreign_keys, remote_side
        parent_host = relationship(
            "HostEntry",
            primaryjoin=ip_address == cast(content, INET),
            foreign_keys=content,
            remote_side=ip_address,
        )

The above relationship will produce a join like:

.. sourcecode:: sql

    SELECT host_entry.id, host_entry.ip_address, host_entry.content
    FROM host_entry JOIN host_entry AS host_entry_1
    ON host_entry_1.ip_address = CAST(host_entry.content AS INET)

An alternative syntax to the above is to use the :func:`.foreign` and
:func:`.remote` :term:`annotations`,
inline within the :paramref:`_orm.relationship.primaryjoin` expression.
This syntax represents the annotations that :func:`_orm.relationship` normally
applies by itself to the join condition given the :paramref:`_orm.relationship.foreign_keys` and
:paramref:`_orm.relationship.remote_side` arguments.  These functions may
be more succinct when an explicit join condition is present, and additionally
serve to mark exactly the column that is "foreign" or "remote" independent
of whether that column is stated multiple times or within complex
SQL expressions::

    from sqlalchemy.orm import foreign, remote


    class HostEntry(Base):
        __tablename__ = "host_entry"

        id = mapped_column(Integer, primary_key=True)
        ip_address = mapped_column(INET)
        content = mapped_column(String(50))

        # relationship() using explicit foreign() and remote() annotations
        # in lieu of separate arguments
        parent_host = relationship(
            "HostEntry",
            primaryjoin=remote(ip_address) == cast(foreign(content), INET),
        )

.. _relationship_custom_operator:

Using custom operators in join conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another use case for relationships is the use of custom operators, such
as PostgreSQL's "is contained within" ``<<`` operator when joining with
types such as :class:`_postgresql.INET` and :class:`_postgresql.CIDR`.
For custom boolean operators we use the :meth:`.Operators.bool_op` function::

    inet_column.bool_op("<<")(cidr_column)

A comparison like the above may be used directly with
:paramref:`_orm.relationship.primaryjoin` when constructing
a :func:`_orm.relationship`::

    class IPA(Base):
        __tablename__ = "ip_address"

        id = mapped_column(Integer, primary_key=True)
        v4address = mapped_column(INET)

        network = relationship(
            "Network",
            primaryjoin="IPA.v4address.bool_op('<<')(foreign(Network.v4representation))",
            viewonly=True,
        )


    class Network(Base):
        __tablename__ = "network"

        id = mapped_column(Integer, primary_key=True)
        v4representation = mapped_column(CIDR)

Above, a query such as::

    select(IPA).join(IPA.network)

Will render as:

.. sourcecode:: sql

    SELECT ip_address.id AS ip_address_id, ip_address.v4address AS ip_address_v4address
    FROM ip_address JOIN network ON ip_address.v4address << network.v4representation

.. _relationship_custom_operator_sql_function:

Custom operators based on SQL functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A variant to the use case for :paramref:`~.Operators.op.is_comparison` is
when we aren't using an operator, but a SQL function.   The typical example
of this use case is the PostgreSQL PostGIS functions however any SQL
function on any database that resolves to a binary condition may apply.
To suit this use case, the :meth:`.FunctionElement.as_comparison` method
can modify any SQL function, such as those invoked from the :data:`.func`
namespace, to indicate to the ORM that the function produces a comparison of
two expressions.  The below example illustrates this with the
`Geoalchemy2 <https://geoalchemy-2.readthedocs.io/>`_ library::

    from geoalchemy2 import Geometry
    from sqlalchemy import Column, Integer, func
    from sqlalchemy.orm import relationship, foreign


    class Polygon(Base):
        __tablename__ = "polygon"
        id = mapped_column(Integer, primary_key=True)
        geom = mapped_column(Geometry("POLYGON", srid=4326))
        points = relationship(
            "Point",
            primaryjoin="func.ST_Contains(foreign(Polygon.geom), Point.geom).as_comparison(1, 2)",
            viewonly=True,
        )


    class Point(Base):
        __tablename__ = "point"
        id = mapped_column(Integer, primary_key=True)
        geom = mapped_column(Geometry("POINT", srid=4326))

Above, the :meth:`.FunctionElement.as_comparison` indicates that the
``func.ST_Contains()`` SQL function is comparing the ``Polygon.geom`` and
``Point.geom`` expressions. The :func:`.foreign` annotation additionally notes
which column takes on the "foreign key" role in this particular relationship.

.. _relationship_overlapping_foreignkeys:

Overlapping Foreign Keys
~~~~~~~~~~~~~~~~~~~~~~~~

A rare scenario can arise when composite foreign keys are used, such that
a single column may be the subject of more than one column
referred to via foreign key constraint.

Consider an (admittedly complex) mapping such as the ``Magazine`` object,
referred to both by the ``Writer`` object and the ``Article`` object
using a composite primary key scheme that includes ``magazine_id``
for both; then to make ``Article`` refer to ``Writer`` as well,
``Article.magazine_id`` is involved in two separate relationships;
``Article.magazine`` and ``Article.writer``::

    class Magazine(Base):
        __tablename__ = "magazine"

        id = mapped_column(Integer, primary_key=True)


    class Article(Base):
        __tablename__ = "article"

        article_id = mapped_column(Integer)
        magazine_id = mapped_column(ForeignKey("magazine.id"))
        writer_id = mapped_column(Integer)

        magazine = relationship("Magazine")
        writer = relationship("Writer")

        __table_args__ = (
            PrimaryKeyConstraint("article_id", "magazine_id"),
            ForeignKeyConstraint(
                ["writer_id", "magazine_id"], ["writer.id", "writer.magazine_id"]
            ),
        )


    class Writer(Base):
        __tablename__ = "writer"

        id = mapped_column(Integer, primary_key=True)
        magazine_id = mapped_column(ForeignKey("magazine.id"), primary_key=True)
        magazine = relationship("Magazine")

When the above mapping is configured, we will see this warning emitted:

.. sourcecode:: text

    SAWarning: relationship 'Article.writer' will copy column
    writer.magazine_id to column article.magazine_id,
    which conflicts with relationship(s): 'Article.magazine'
    (copies magazine.id to article.magazine_id). Consider applying
    viewonly=True to read-only relationships, or provide a primaryjoin
    condition marking writable columns with the foreign() annotation.

What this refers to originates from the fact that ``Article.magazine_id`` is
the subject of two different foreign key constraints; it refers to
``Magazine.id`` directly as a source column, but also refers to
``Writer.magazine_id`` as a source column in the context of the
composite key to ``Writer``.

When objects are added to an ORM :class:`.Session` using :meth:`.Session.add`,
the ORM :term:`flush` process takes on the task of reconciling object
refereneces that correspond to :func:`_orm.relationship` configurations and
delivering this state to the databse using INSERT/UPDATE/DELETE statements.  In
this specific example, if we associate an ``Article`` with a particular
``Magazine``, but then associate the ``Article`` with a ``Writer`` that's
associated  with a *different* ``Magazine``, this flush process will overwrite
``Article.magazine_id`` non-deterministically, silently changing which magazine
to which we refer; it may also attempt to place NULL into this column if we
de-associate a ``Writer`` from an ``Article``.  The warning lets us know that
this scenario may occur during ORM flush sequences.

To solve this, we need to break out the behavior of ``Article`` to include
all three of the following features:

1. ``Article`` first and foremost writes to
   ``Article.magazine_id`` based on data persisted in the ``Article.magazine``
   relationship only, that is a value copied from ``Magazine.id``.

2. ``Article`` can write to ``Article.writer_id`` on behalf of data
   persisted in the  ``Article.writer`` relationship, but only the
   ``Writer.id`` column; the ``Writer.magazine_id`` column should not
   be written into ``Article.magazine_id`` as it ultimately is sourced
   from ``Magazine.id``.

3. ``Article`` takes ``Article.magazine_id`` into account when loading
   ``Article.writer``, even though it *doesn't* write to it on behalf
   of this relationship.

To get just #1 and #2, we could specify only ``Article.writer_id`` as the
"foreign keys" for ``Article.writer``::

    class Article(Base):
        # ...

        writer = relationship("Writer", foreign_keys="Article.writer_id")

However, this has the effect of ``Article.writer`` not taking
``Article.magazine_id`` into account when querying against ``Writer``:

.. sourcecode:: sql

    SELECT article.article_id AS article_article_id,
        article.magazine_id AS article_magazine_id,
        article.writer_id AS article_writer_id
    FROM article
    JOIN writer ON writer.id = article.writer_id

Therefore, to get at all of #1, #2, and #3, we express the join condition
as well as which columns to be written by combining
:paramref:`_orm.relationship.primaryjoin` fully, along with either the
:paramref:`_orm.relationship.foreign_keys` argument, or more succinctly by
annotating with :func:`_orm.foreign`::

    class Article(Base):
        # ...

        writer = relationship(
            "Writer",
            primaryjoin="and_(Writer.id == foreign(Article.writer_id), "
            "Writer.magazine_id == Article.magazine_id)",
        )

Non-relational Comparisons / Materialized Path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::  this section details an experimental feature.

Using custom expressions means we can produce unorthodox join conditions that
don't obey the usual primary/foreign key model.  One such example is the
materialized path pattern, where we compare strings for overlapping path tokens
in order to produce a tree structure.

Through careful use of :func:`.foreign` and :func:`.remote`, we can build
a relationship that effectively produces a rudimentary materialized path
system.   Essentially, when :func:`.foreign` and :func:`.remote` are
on the *same* side of the comparison expression, the relationship is considered
to be "one to many"; when they are on *different* sides, the relationship
is considered to be "many to one".   For the comparison we'll use here,
we'll be dealing with collections so we keep things configured as "one to many"::

    class Element(Base):
        __tablename__ = "element"

        path = mapped_column(String, primary_key=True)

        descendants = relationship(
            "Element",
            primaryjoin=remote(foreign(path)).like(path.concat("/%")),
            viewonly=True,
            order_by=path,
        )

Above, if given an ``Element`` object with a path attribute of ``"/foo/bar2"``,
we seek for a load of ``Element.descendants`` to look like:

.. sourcecode:: sql

    SELECT element.path AS element_path
    FROM element
    WHERE element.path LIKE ('/foo/bar2' || '/%') ORDER BY element.path

.. _self_referential_many_to_many:

Self-Referential Many-to-Many Relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. seealso::

    This section documents a two-table variant of the "adjacency list" pattern,
    which is documented at :ref:`self_referential`.  Be sure to review the
    self-referential querying patterns in subsections
    :ref:`self_referential_query` and :ref:`self_referential_eager_loading`
    which apply equally well to the mapping pattern discussed here.

Many to many relationships can be customized by one or both of :paramref:`_orm.relationship.primaryjoin`
and :paramref:`_orm.relationship.secondaryjoin` - the latter is significant for a relationship that
specifies a many-to-many reference using the :paramref:`_orm.relationship.secondary` argument.
A common situation which involves the usage of :paramref:`_orm.relationship.primaryjoin` and :paramref:`_orm.relationship.secondaryjoin`
is when establishing a many-to-many relationship from a class to itself, as shown below::

    from typing import List

    from sqlalchemy import Integer, ForeignKey, Column, Table
    from sqlalchemy.orm import DeclarativeBase, Mapped
    from sqlalchemy.orm import mapped_column, relationship


    class Base(DeclarativeBase):
        pass


    node_to_node = Table(
        "node_to_node",
        Base.metadata,
        Column("left_node_id", Integer, ForeignKey("node.id"), primary_key=True),
        Column("right_node_id", Integer, ForeignKey("node.id"), primary_key=True),
    )


    class Node(Base):
        __tablename__ = "node"
        id: Mapped[int] = mapped_column(primary_key=True)
        label: Mapped[str]
        right_nodes: Mapped[List["Node"]] = relationship(
            "Node",
            secondary=node_to_node,
            primaryjoin=id == node_to_node.c.left_node_id,
            secondaryjoin=id == node_to_node.c.right_node_id,
            back_populates="left_nodes",
        )
        left_nodes: Mapped[List["Node"]] = relationship(
            "Node",
            secondary=node_to_node,
            primaryjoin=id == node_to_node.c.right_node_id,
            secondaryjoin=id == node_to_node.c.left_node_id,
            back_populates="right_nodes",
        )

Where above, SQLAlchemy can't know automatically which columns should connect
to which for the ``right_nodes`` and ``left_nodes`` relationships.   The :paramref:`_orm.relationship.primaryjoin`
and :paramref:`_orm.relationship.secondaryjoin` arguments establish how we'd like to join to the association table.
In the Declarative form above, as we are declaring these conditions within the Python
block that corresponds to the ``Node`` class, the ``id`` variable is available directly
as the :class:`_schema.Column` object we wish to join with.

Alternatively, we can define the :paramref:`_orm.relationship.primaryjoin`
and :paramref:`_orm.relationship.secondaryjoin` arguments using strings, which is suitable
in the case that our configuration does not have either the ``Node.id`` column
object available yet or the ``node_to_node`` table perhaps isn't yet available.
When referring to a plain :class:`_schema.Table` object in a declarative string, we
use the string name of the table as it is present in the :class:`_schema.MetaData`::

    class Node(Base):
        __tablename__ = "node"
        id = mapped_column(Integer, primary_key=True)
        label = mapped_column(String)
        right_nodes = relationship(
            "Node",
            secondary="node_to_node",
            primaryjoin="Node.id==node_to_node.c.left_node_id",
            secondaryjoin="Node.id==node_to_node.c.right_node_id",
            backref="left_nodes",
        )

.. warning:: When passed as a Python-evaluable string, the
    :paramref:`_orm.relationship.primaryjoin` and
    :paramref:`_orm.relationship.secondaryjoin` arguments are interpreted using
    Python's ``eval()`` function. **DO NOT PASS UNTRUSTED INPUT TO THESE
    STRINGS**. See :ref:`declarative_relationship_eval` for details on
    declarative evaluation of :func:`_orm.relationship` arguments.


A classical mapping situation here is similar, where ``node_to_node`` can be joined
to ``node.c.id``::

    from sqlalchemy import Integer, ForeignKey, String, Column, Table, MetaData
    from sqlalchemy.orm import relationship, registry

    metadata_obj = MetaData()
    mapper_registry = registry()

    node_to_node = Table(
        "node_to_node",
        metadata_obj,
        Column("left_node_id", Integer, ForeignKey("node.id"), primary_key=True),
        Column("right_node_id", Integer, ForeignKey("node.id"), primary_key=True),
    )

    node = Table(
        "node",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("label", String),
    )


    class Node:
        pass


    mapper_registry.map_imperatively(
        Node,
        node,
        properties={
            "right_nodes": relationship(
                Node,
                secondary=node_to_node,
                primaryjoin=node.c.id == node_to_node.c.left_node_id,
                secondaryjoin=node.c.id == node_to_node.c.right_node_id,
                backref="left_nodes",
            )
        },
    )

Note that in both examples, the :paramref:`_orm.relationship.backref`
keyword specifies a ``left_nodes`` backref - when
:func:`_orm.relationship` creates the second relationship in the reverse
direction, it's smart enough to reverse the
:paramref:`_orm.relationship.primaryjoin` and
:paramref:`_orm.relationship.secondaryjoin` arguments.

.. seealso::

  * :ref:`self_referential` - single table version
  * :ref:`self_referential_query` - tips on querying with self-referential
    mappings
  * :ref:`self_referential_eager_loading` - tips on eager loading with self-
    referential mapping

.. _composite_secondary_join:

Composite "Secondary" Joins
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    This section features far edge cases that are somewhat supported
    by SQLAlchemy, however it is recommended to solve problems like these
    in simpler ways whenever possible, by using reasonable relational
    layouts and / or :ref:`in-Python attributes <mapper_hybrids>`.

Sometimes, when one seeks to build a :func:`_orm.relationship` between two tables
there is a need for more than just two or three tables to be involved in
order to join them.  This is an area of :func:`_orm.relationship` where one seeks
to push the boundaries of what's possible, and often the ultimate solution to
many of these exotic use cases needs to be hammered out on the SQLAlchemy mailing
list.

In more recent versions of SQLAlchemy, the :paramref:`_orm.relationship.secondary`
parameter can be used in some of these cases in order to provide a composite
target consisting of multiple tables.   Below is an example of such a
join condition (requires version 0.9.2 at least to function as is)::

    class A(Base):
        __tablename__ = "a"

        id = mapped_column(Integer, primary_key=True)
        b_id = mapped_column(ForeignKey("b.id"))

        d = relationship(
            "D",
            secondary="join(B, D, B.d_id == D.id).join(C, C.d_id == D.id)",
            primaryjoin="and_(A.b_id == B.id, A.id == C.a_id)",
            secondaryjoin="D.id == B.d_id",
            uselist=False,
            viewonly=True,
        )


    class B(Base):
        __tablename__ = "b"

        id = mapped_column(Integer, primary_key=True)
        d_id = mapped_column(ForeignKey("d.id"))


    class C(Base):
        __tablename__ = "c"

        id = mapped_column(Integer, primary_key=True)
        a_id = mapped_column(ForeignKey("a.id"))
        d_id = mapped_column(ForeignKey("d.id"))


    class D(Base):
        __tablename__ = "d"

        id = mapped_column(Integer, primary_key=True)

In the above example, we provide all three of :paramref:`_orm.relationship.secondary`,
:paramref:`_orm.relationship.primaryjoin`, and :paramref:`_orm.relationship.secondaryjoin`,
in the declarative style referring to the named tables ``a``, ``b``, ``c``, ``d``
directly.  A query from ``A`` to ``D`` looks like:

.. sourcecode:: python+sql

    sess.scalars(select(A).join(A.d)).all()

    {execsql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (
        b AS b_1 JOIN d AS d_1 ON b_1.d_id = d_1.id
            JOIN c AS c_1 ON c_1.d_id = d_1.id)
        ON a.b_id = b_1.id AND a.id = c_1.a_id JOIN d ON d.id = b_1.d_id

In the above example, we take advantage of being able to stuff multiple
tables into a "secondary" container, so that we can join across many
tables while still keeping things "simple" for :func:`_orm.relationship`, in that
there's just "one" table on both the "left" and the "right" side; the
complexity is kept within the middle.

.. warning:: A relationship like the above is typically marked as
   ``viewonly=True``, using :paramref:`_orm.relationship.viewonly`,
   and should be considered as read-only.  While there are
   sometimes ways to make relationships like the above writable, this is
   generally complicated and error prone.

.. seealso::

    :ref:`relationship_viewonly_notes`



.. _relationship_non_primary_mapper:

.. _relationship_aliased_class:

Relationship to Aliased Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the previous section, we illustrated a technique where we used
:paramref:`_orm.relationship.secondary` in order to place additional
tables within a join condition.   There is one complex join case where
even this technique is not sufficient; when we seek to join from ``A``
to ``B``, making use of any number of ``C``, ``D``, etc. in between,
however there are also join conditions between ``A`` and ``B``
*directly*.  In this case, the join from ``A`` to ``B`` may be
difficult to express with just a complex
:paramref:`_orm.relationship.primaryjoin` condition, as the intermediary
tables may need special handling, and it is also not expressible with
a :paramref:`_orm.relationship.secondary` object, since the
``A->secondary->B`` pattern does not support any references between
``A`` and ``B`` directly.  When this **extremely advanced** case
arises, we can resort to creating a second mapping as a target for the
relationship.  This is where we use :class:`.AliasedClass` in order to make a
mapping to a class that includes all the additional tables we need for
this join. In order to produce this mapper as an "alternative" mapping
for our class, we use the :func:`.aliased` function to produce the new
construct, then use :func:`_orm.relationship` against the object as though it
were a plain mapped class.

Below illustrates a :func:`_orm.relationship` with a simple join from ``A`` to
``B``, however the primaryjoin condition is augmented with two additional
entities ``C`` and ``D``, which also must have rows that line up with
the rows in both ``A`` and ``B`` simultaneously::

    class A(Base):
        __tablename__ = "a"

        id = mapped_column(Integer, primary_key=True)
        b_id = mapped_column(ForeignKey("b.id"))


    class B(Base):
        __tablename__ = "b"

        id = mapped_column(Integer, primary_key=True)


    class C(Base):
        __tablename__ = "c"

        id = mapped_column(Integer, primary_key=True)
        a_id = mapped_column(ForeignKey("a.id"))

        some_c_value = mapped_column(String)


    class D(Base):
        __tablename__ = "d"

        id = mapped_column(Integer, primary_key=True)
        c_id = mapped_column(ForeignKey("c.id"))
        b_id = mapped_column(ForeignKey("b.id"))

        some_d_value = mapped_column(String)


    # 1. set up the join() as a variable, so we can refer
    # to it in the mapping multiple times.
    j = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)

    # 2. Create an AliasedClass to B
    B_viacd = aliased(B, j, flat=True)

    A.b = relationship(B_viacd, primaryjoin=A.b_id == j.c.b_id)

With the above mapping, a simple join looks like:

.. sourcecode:: python+sql

    sess.scalars(select(A).join(A.b)).all()

    {execsql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (b JOIN d ON d.b_id = b.id JOIN c ON c.id = d.c_id) ON a.b_id = b.id

Integrating AliasedClass Mappings with Typing and Avoiding Early Mapper Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The creation of the :func:`_orm.aliased` construct against a mapped class
forces the :func:`_orm.configure_mappers` step to proceed, which will resolve
all current classes and their relationships.  This may be problematic if
unrelated mapped classes needed by the current mappings have not yet been
declared, or if the configuration of the relationship itself needs access
to as-yet undeclared classes.  Additionally, SQLAlchemy's Declarative pattern
works with Python typing most effectively when relationships are declared
up front.

To organize the construction of the relationship to work with these issues, a
configure level event hook like :meth:`.MapperEvents.before_mapper_configured`
may be used, which will invoke the configuration code only when all mappings
are ready for configuration::

    from sqlalchemy import event


    class A(Base):
        __tablename__ = "a"

        id = mapped_column(Integer, primary_key=True)
        b_id = mapped_column(ForeignKey("b.id"))


    @event.listens_for(A, "before_mapper_configured")
    def _configure_ab_relationship(mapper, cls):
        # do the above configuration in a configuration hook

        j = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)
        B_viacd = aliased(B, j, flat=True)
        A.b = relationship(B_viacd, primaryjoin=A.b_id == j.c.b_id)

Above, the function ``_configure_ab_relationship()`` will be invoked only
when a fully configured version of ``A`` is requested, at which point the
classes ``B``, ``D`` and ``C`` would be available.

For an approach that integrates with inline typing, a similar technique can be
used to effectively generate a "singleton" creation pattern for the aliased
class where it is late-initialized as a global variable, which can then be used
in the relationship inline::

    from typing import Any

    B_viacd: Any = None
    b_viacd_join: Any = None


    class A(Base):
        __tablename__ = "a"

        id: Mapped[int] = mapped_column(primary_key=True)
        b_id: Mapped[int] = mapped_column(ForeignKey("b.id"))

        # 1. the relationship can be declared using lambdas, allowing it to resolve
        #    to targets that are late-configured
        b: Mapped[B] = relationship(
            lambda: B_viacd, primaryjoin=lambda: A.b_id == b_viacd_join.c.b_id
        )


    # 2. configure the targets of the relationship using a before_mapper_configured
    #    hook.
    @event.listens_for(A, "before_mapper_configured")
    def _configure_ab_relationship(mapper, cls):
        # 3. set up the join() and AliasedClass as globals from within
        #    the configuration hook.

        global B_viacd, b_viacd_join

        b_viacd_join = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)
        B_viacd = aliased(B, b_viacd_join, flat=True)

Using the AliasedClass target in Queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the previous example, the ``A.b`` relationship refers to the ``B_viacd``
entity as the target, and **not** the ``B`` class directly. To add additional
criteria involving the ``A.b`` relationship, it's typically necessary to
reference the ``B_viacd`` directly rather than using ``B``, especially in a
case where the target entity of ``A.b`` is to be transformed into an alias or a
subquery. Below illustrates the same relationship using a subquery, rather than
a join::

    subq = select(B).join(D, D.b_id == B.id).join(C, C.id == D.c_id).subquery()

    B_viacd_subquery = aliased(B, subq)

    A.b = relationship(B_viacd_subquery, primaryjoin=A.b_id == subq.c.id)

A query using the above ``A.b`` relationship will render a subquery:

.. sourcecode:: python+sql

    sess.scalars(select(A).join(A.b)).all()

    {execsql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (SELECT b.id AS id, b.some_b_column AS some_b_column
    FROM b JOIN d ON d.b_id = b.id JOIN c ON c.id = d.c_id) AS anon_1 ON a.b_id = anon_1.id

If we want to add additional criteria based on the ``A.b`` join, we must do
so in terms of ``B_viacd_subquery`` rather than ``B`` directly:

.. sourcecode:: python+sql

    sess.scalars(
        select(A)
        .join(A.b)
        .where(B_viacd_subquery.some_b_column == "some b")
        .order_by(B_viacd_subquery.id)
    ).all()

    {execsql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (SELECT b.id AS id, b.some_b_column AS some_b_column
    FROM b JOIN d ON d.b_id = b.id JOIN c ON c.id = d.c_id) AS anon_1 ON a.b_id = anon_1.id
    WHERE anon_1.some_b_column = ? ORDER BY anon_1.id

.. _relationship_to_window_function:

Row-Limited Relationships with Window Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another interesting use case for relationships to :class:`.AliasedClass`
objects are situations where
the relationship needs to join to a specialized SELECT of any form.   One
scenario is when the use of a window function is desired, such as to limit
how many rows should be returned for a relationship.  The example below
illustrates a non-primary mapper relationship that will load the first
ten items for each collection::

    class A(Base):
        __tablename__ = "a"

        id = mapped_column(Integer, primary_key=True)


    class B(Base):
        __tablename__ = "b"
        id = mapped_column(Integer, primary_key=True)
        a_id = mapped_column(ForeignKey("a.id"))


    partition = select(
        B, func.row_number().over(order_by=B.id, partition_by=B.a_id).label("index")
    ).alias()

    partitioned_b = aliased(B, partition)

    A.partitioned_bs = relationship(
        partitioned_b, primaryjoin=and_(partitioned_b.a_id == A.id, partition.c.index < 10)
    )

We can use the above ``partitioned_bs`` relationship with most of the loader
strategies, such as :func:`.selectinload`::

    for a1 in session.scalars(select(A).options(selectinload(A.partitioned_bs))):
        print(a1.partitioned_bs)  # <-- will be no more than ten objects

Where above, the "selectinload" query looks like:

.. sourcecode:: sql

    SELECT
        a_1.id AS a_1_id, anon_1.id AS anon_1_id, anon_1.a_id AS anon_1_a_id,
        anon_1.data AS anon_1_data, anon_1.index AS anon_1_index
    FROM a AS a_1
    JOIN (
        SELECT b.id AS id, b.a_id AS a_id, b.data AS data,
        row_number() OVER (PARTITION BY b.a_id ORDER BY b.id) AS index
        FROM b) AS anon_1
    ON anon_1.a_id = a_1.id AND anon_1.index < %(index_1)s
    WHERE a_1.id IN ( ... primary key collection ...)
    ORDER BY a_1.id

Above, for each matching primary key in "a", we will get the first ten
"bs" as ordered by "b.id".   By partitioning on "a_id" we ensure that each
"row number" is local to the parent "a_id".

Such a mapping would ordinarily also include a "plain" relationship
from "A" to "B", for persistence operations as well as when the full
set of "B" objects per "A" is desired.

.. _query_enabled_properties:

Building Query-Enabled Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Very ambitious custom join conditions may fail to be directly persistable, and
in some cases may not even load correctly. To remove the persistence part of
the equation, use the flag :paramref:`_orm.relationship.viewonly` on the
:func:`~sqlalchemy.orm.relationship`, which establishes it as a read-only
attribute (data written to the collection will be ignored on flush()).
However, in extreme cases, consider using a regular Python property in
conjunction with :class:`_query.Query` as follows:

.. sourcecode:: python

    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)

        @property
        def addresses(self):
            return object_session(self).query(Address).with_parent(self).filter(...).all()

In other cases, the descriptor can be built to make use of existing in-Python
data.  See the section on :ref:`mapper_hybrids` for more general discussion
of special Python attributes.

.. seealso::

    :ref:`mapper_hybrids`

.. _relationship_viewonly_notes:

Notes on using the viewonly relationship parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :paramref:`_orm.relationship.viewonly` parameter when applied to a
:func:`_orm.relationship` construct indicates that this :func:`_orm.relationship`
will not take part in any ORM :term:`unit of work` operations, and additionally
that the attribute does not expect to participate within in-Python mutations
of its represented collection.  This means
that while the viewonly relationship may refer to a mutable Python collection
like a list or set, making changes to that list or set as present on a
mapped instance will have **no effect** on the ORM flush process.

To explore this scenario consider this mapping::

    from __future__ import annotations

    import datetime

    from sqlalchemy import and_
    from sqlalchemy import ForeignKey
    from sqlalchemy import func
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str | None]

        all_tasks: Mapped[list[Task]] = relationship()

        current_week_tasks: Mapped[list[Task]] = relationship(
            primaryjoin=lambda: and_(
                User.id == Task.user_account_id,
                # this expression works on PostgreSQL but may not be supported
                # by other database engines
                Task.task_date >= func.now() - datetime.timedelta(days=7),
            ),
            viewonly=True,
        )


    class Task(Base):
        __tablename__ = "task"

        id: Mapped[int] = mapped_column(primary_key=True)
        user_account_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
        description: Mapped[str | None]
        task_date: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

        user: Mapped[User] = relationship(back_populates="current_week_tasks")

The following sections will note different aspects of this configuration.

In-Python mutations including backrefs are not appropriate with viewonly=True
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above mapping targets the ``User.current_week_tasks`` viewonly relationship
as the :term:`backref` target of the ``Task.user`` attribute.  This is not
currently flagged by SQLAlchemy's ORM configuration process, however is a
configuration error.   Changing the ``.user`` attribute on a ``Task`` will not
affect the ``.current_week_tasks`` attribute::

    >>> u1 = User()
    >>> t1 = Task(task_date=datetime.datetime.now())
    >>> t1.user = u1
    >>> u1.current_week_tasks
    []

There is another parameter called :paramref:`_orm.relationship.sync_backrefs`
which can be turned on here to allow ``.current_week_tasks`` to be mutated in this
case, however this is not considered to be a best practice with a viewonly
relationship, which instead should not be relied upon for in-Python mutations.

In this mapping, backrefs can be configured between ``User.all_tasks`` and
``Task.user``, as these are both not viewonly and will synchronize normally.

Beyond the issue of backref mutations being disabled for viewonly relationships,
plain changes to the ``User.all_tasks`` collection in Python
are also not reflected in the ``User.current_week_tasks`` collection until
changes have been flushed to the database.

Overall, for a use case where a custom collection should respond immediately to
in-Python mutations, the viewonly relationship is generally not appropriate.  A
better approach is to use the :ref:`hybrids_toplevel` feature of SQLAlchemy, or
for instance-only cases to use a Python ``@property``, where a user-defined
collection that is generated in terms of the current Python instance can be
implemented.  To change our example to work this way, we repair the
:paramref:`_orm.relationship.back_populates` parameter on ``Task.user`` to
reference ``User.all_tasks``, and
then illustrate a simple ``@property`` that will deliver results in terms of
the immediate ``User.all_tasks`` collection::

    class User(Base):
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str | None]

        all_tasks: Mapped[list[Task]] = relationship(back_populates="user")

        @property
        def current_week_tasks(self) -> list[Task]:
            past_seven_days = datetime.datetime.now() - datetime.timedelta(days=7)
            return [t for t in self.all_tasks if t.task_date >= past_seven_days]


    class Task(Base):
        __tablename__ = "task"

        id: Mapped[int] = mapped_column(primary_key=True)
        user_account_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
        description: Mapped[str | None]
        task_date: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

        user: Mapped[User] = relationship(back_populates="all_tasks")

Using an in-Python collection calculated on the fly each time, we are guaranteed
to have the correct answer at all times, without the need to use a database
at all::

    >>> u1 = User()
    >>> t1 = Task(task_date=datetime.datetime.now())
    >>> t1.user = u1
    >>> u1.current_week_tasks
    [<__main__.Task object at 0x7f3d699523c0>]


viewonly=True collections / attributes do not get re-queried until expired
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Continuing with the original viewonly attribute, if we do in fact make changes
to the ``User.all_tasks`` collection on a :term:`persistent` object, the
viewonly collection can only show the net result of this change after **two**
things occur.  The first is that the change to ``User.all_tasks`` is
:term:`flushed`, so that the new data is available in the database, at least
within the scope of the local transaction.  The second is that the ``User.current_week_tasks``
attribute is :term:`expired` and reloaded via a new SQL query to the database.

To support this requirement, the simplest flow to use is one where the
**viewonly relationship is consumed only in operations that are primarily read
only to start with**.   Such as below, if we retrieve a ``User`` fresh from
the database, the collection will be current::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     print(u1.current_week_tasks)
    [<__main__.Task object at 0x7f8711b906b0>]


When we make modifications to ``u1.all_tasks``, if we want to see these changes
reflected in the ``u1.current_week_tasks`` viewonly relationship, these changes need to be flushed
and the ``u1.current_week_tasks`` attribute needs to be expired, so that
it will :term:`lazy load` on next access.  The simplest approach to this is
to use :meth:`_orm.Session.commit`, keeping the :paramref:`_orm.Session.expire_on_commit`
parameter set at its default of ``True``::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     u1.all_tasks.append(Task(task_date=datetime.datetime.now()))
    ...     sess.commit()
    ...     print(u1.current_week_tasks)
    [<__main__.Task object at 0x7f8711b90ec0>, <__main__.Task object at 0x7f8711b90a10>]

Above, the call to :meth:`_orm.Session.commit` flushed the changes to ``u1.all_tasks``
to the database, then expired all objects, so that when we accessed ``u1.current_week_tasks``,
a :term:` lazy load` occurred which fetched the contents for this attribute
freshly from the database.

To intercept operations without actually committing the transaction,
the attribute needs to be explicitly :term:`expired`
first.   A simplistic way to do this is to just call it directly.  In
the example below, :meth:`_orm.Session.flush` sends pending changes to the
database, then :meth:`_orm.Session.expire` is used to expire the ``u1.current_week_tasks``
collection so that it re-fetches on next access::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     u1.all_tasks.append(Task(task_date=datetime.datetime.now()))
    ...     sess.flush()
    ...     sess.expire(u1, ["current_week_tasks"])
    ...     print(u1.current_week_tasks)
    [<__main__.Task object at 0x7fd95a4c8c50>, <__main__.Task object at 0x7fd95a4c8c80>]

We can in fact skip the call to :meth:`_orm.Session.flush`, assuming a
:class:`_orm.Session` that keeps :paramref:`_orm.Session.autoflush` at its
default value of ``True``, as the expired ``current_week_tasks`` attribute will
trigger autoflush when accessed after expiration::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     u1.all_tasks.append(Task(task_date=datetime.datetime.now()))
    ...     sess.expire(u1, ["current_week_tasks"])
    ...     print(u1.current_week_tasks)  # triggers autoflush before querying
    [<__main__.Task object at 0x7fd95a4c8c50>, <__main__.Task object at 0x7fd95a4c8c80>]

Continuing with the above approach to something more elaborate, we can apply
the expiration programmatically when the related ``User.all_tasks`` collection
changes, using :ref:`event hooks <event_toplevel>`.   This an **advanced
technique**, where simpler architectures like ``@property`` or sticking to
read-only use cases should be examined first.  In our simple example, this
would be configured as::

    from sqlalchemy import event, inspect


    @event.listens_for(User.all_tasks, "append")
    @event.listens_for(User.all_tasks, "remove")
    @event.listens_for(User.all_tasks, "bulk_replace")
    def _expire_User_current_week_tasks(target, value, initiator):
        inspect(target).session.expire(target, ["current_week_tasks"])

With the above hooks, mutation operations are intercepted and result in
the ``User.current_week_tasks`` collection to be expired automatically::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     u1.all_tasks.append(Task(task_date=datetime.datetime.now()))
    ...     print(u1.current_week_tasks)
    [<__main__.Task object at 0x7f66d093ccb0>, <__main__.Task object at 0x7f66d093cce0>]

The :class:`_orm.AttributeEvents` event hooks used above are also triggered
by backref mutations, so with the above hooks a change to ``Task.user`` is
also intercepted::

    >>> with Session(e) as sess:
    ...     u1 = sess.scalar(select(User).where(User.id == 1))
    ...     t1 = Task(task_date=datetime.datetime.now())
    ...     t1.user = u1
    ...     sess.add(t1)
    ...     print(u1.current_week_tasks)
    [<__main__.Task object at 0x7f3b0c070d10>, <__main__.Task object at 0x7f3b0c057d10>]


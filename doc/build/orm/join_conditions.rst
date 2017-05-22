.. _relationship_configure_joins:

Configuring how Relationship Joins
----------------------------------

:func:`.relationship` will normally create a join between two tables
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
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class Customer(Base):
        __tablename__ = 'customer'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        billing_address_id = Column(Integer, ForeignKey("address.id"))
        shipping_address_id = Column(Integer, ForeignKey("address.id"))

        billing_address = relationship("Address")
        shipping_address = relationship("Address")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        street = Column(String)
        city = Column(String)
        state = Column(String)
        zip = Column(String)

The above mapping, when we attempt to use it, will produce the error::

    sqlalchemy.exc.AmbiguousForeignKeysError: Could not determine join
    condition between parent/child tables on relationship
    Customer.billing_address - there are multiple foreign key
    paths linking the tables.  Specify the 'foreign_keys' argument,
    providing a list of those columns which should be
    counted as containing a foreign key reference to the parent table.

The above message is pretty long.  There are many potential messages
that :func:`.relationship` can return, which have been carefully tailored
to detect a variety of common configurational issues; most will suggest
the additional configuration that's needed to resolve the ambiguity
or other missing information.

In this case, the message wants us to qualify each :func:`.relationship`
by instructing for each one which foreign key column should be considered, and
the appropriate form is as follows::

    class Customer(Base):
        __tablename__ = 'customer'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        billing_address_id = Column(Integer, ForeignKey("address.id"))
        shipping_address_id = Column(Integer, ForeignKey("address.id"))

        billing_address = relationship("Address", foreign_keys=[billing_address_id])
        shipping_address = relationship("Address", foreign_keys=[shipping_address_id])

Above, we specify the ``foreign_keys`` argument, which is a :class:`.Column` or list
of :class:`.Column` objects which indicate those columns to be considered "foreign",
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
one :class:`.Column` we need::

        billing_address = relationship("Address", foreign_keys="Customer.billing_address_id")

.. versionchanged:: 0.8
    :func:`.relationship` can resolve ambiguity between foreign key targets on the
    basis of the ``foreign_keys`` argument alone; the :paramref:`~.relationship.primaryjoin`
    argument is no longer needed in this situation.

.. _relationship_primaryjoin:

Specifying Alternate Join Conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default behavior of :func:`.relationship` when constructing a join
is that it equates the value of primary key columns
on one side to that of foreign-key-referring columns on the other.
We can change this criterion to be anything we'd like using the
:paramref:`~.relationship.primaryjoin`
argument, as well as the :paramref:`~.relationship.secondaryjoin`
argument in the case when a "secondary" table is used.

In the example below, using the ``User`` class
as well as an ``Address`` class which stores a street address,  we
create a relationship ``boston_addresses`` which will only
load those ``Address`` objects which specify a city of "Boston"::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        boston_addresses = relationship("Address",
                        primaryjoin="and_(User.id==Address.user_id, "
                            "Address.city=='Boston')")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'))

        street = Column(String)
        city = Column(String)
        state = Column(String)
        zip = Column(String)

Within this string SQL expression, we made use of the :func:`.and_` conjunction construct to establish
two distinct predicates for the join condition - joining both the ``User.id`` and
``Address.user_id`` columns to each other, as well as limiting rows in ``Address``
to just ``city='Boston'``.   When using Declarative, rudimentary SQL functions like
:func:`.and_` are automatically available in the evaluated namespace of a string
:func:`.relationship` argument.

The custom criteria we use in a :paramref:`~.relationship.primaryjoin`
is generally only significant when SQLAlchemy is rendering SQL in
order to load or represent this relationship. That is, it's used in
the SQL statement that's emitted in order to perform a per-attribute
lazy load, or when a join is constructed at query time, such as via
:meth:`.Query.join`, or via the eager "joined" or "subquery" styles of
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
of :class:`.Column` objects will specify :class:`.ForeignKey`, or otherwise
be part of a :class:`.ForeignKeyConstraint` that's relevant to the join condition.
:func:`.relationship` looks to this foreign key status as it decides
how it should load and persist data for this relationship.   However, the
:paramref:`~.relationship.primaryjoin` argument can be used to create a join condition that
doesn't involve any "schema" level foreign keys.  We can combine :paramref:`~.relationship.primaryjoin`
along with :paramref:`~.relationship.foreign_keys` and :paramref:`~.relationship.remote_side` explicitly in order to
establish such a join.

Below, a class ``HostEntry`` joins to itself, equating the string ``content``
column to the ``ip_address`` column, which is a PostgreSQL type called ``INET``.
We need to use :func:`.cast` in order to cast one side of the join to the
type of the other::

    from sqlalchemy import cast, String, Column, Integer
    from sqlalchemy.orm import relationship
    from sqlalchemy.dialects.postgresql import INET

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class HostEntry(Base):
        __tablename__ = 'host_entry'

        id = Column(Integer, primary_key=True)
        ip_address = Column(INET)
        content = Column(String(50))

        # relationship() using explicit foreign_keys, remote_side
        parent_host = relationship("HostEntry",
                            primaryjoin=ip_address == cast(content, INET),
                            foreign_keys=content,
                            remote_side=ip_address
                        )

The above relationship will produce a join like::

    SELECT host_entry.id, host_entry.ip_address, host_entry.content
    FROM host_entry JOIN host_entry AS host_entry_1
    ON host_entry_1.ip_address = CAST(host_entry.content AS INET)

An alternative syntax to the above is to use the :func:`.foreign` and
:func:`.remote` :term:`annotations`,
inline within the :paramref:`~.relationship.primaryjoin` expression.
This syntax represents the annotations that :func:`.relationship` normally
applies by itself to the join condition given the :paramref:`~.relationship.foreign_keys` and
:paramref:`~.relationship.remote_side` arguments.  These functions may
be more succinct when an explicit join condition is present, and additionally
serve to mark exactly the column that is "foreign" or "remote" independent
of whether that column is stated multiple times or within complex
SQL expressions::

    from sqlalchemy.orm import foreign, remote

    class HostEntry(Base):
        __tablename__ = 'host_entry'

        id = Column(Integer, primary_key=True)
        ip_address = Column(INET)
        content = Column(String(50))

        # relationship() using explicit foreign() and remote() annotations
        # in lieu of separate arguments
        parent_host = relationship("HostEntry",
                            primaryjoin=remote(ip_address) == \
                                    cast(foreign(content), INET),
                        )


.. _relationship_custom_operator:

Using custom operators in join conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another use case for relationships is the use of custom operators, such
as PostgreSQL's "is contained within" ``<<`` operator when joining with
types such as :class:`.postgresql.INET` and :class:`.postgresql.CIDR`.
For custom operators we use the :meth:`.Operators.op` function::

    inet_column.op("<<")(cidr_column)

However, if we construct a :paramref:`~.relationship.primaryjoin` using this
operator, :func:`.relationship` will still need more information.  This is because
when it examines our primaryjoin condition, it specifically looks for operators
used for **comparisons**, and this is typically a fixed list containing known
comparison operators such as ``==``, ``<``, etc.   So for our custom operator
to participate in this system, we need it to register as a comparison operator
using the :paramref:`~.Operators.op.is_comparison` parameter::

    inet_column.op("<<", is_comparison=True)(cidr_column)

A complete example::

    class IPA(Base):
        __tablename__ = 'ip_address'

        id = Column(Integer, primary_key=True)
        v4address = Column(INET)

        network = relationship("Network",
                            primaryjoin="IPA.v4address.op('<<', is_comparison=True)"
                                "(foreign(Network.v4representation))",
                            viewonly=True
                        )
    class Network(Base):
        __tablename__ = 'network'

        id = Column(Integer, primary_key=True)
        v4representation = Column(CIDR)

Above, a query such as::

    session.query(IPA).join(IPA.network)

Will render as::

    SELECT ip_address.id AS ip_address_id, ip_address.v4address AS ip_address_v4address
    FROM ip_address JOIN network ON ip_address.v4address << network.v4representation

.. versionadded:: 0.9.2 - Added the :paramref:`.Operators.op.is_comparison`
   flag to assist in the creation of :func:`.relationship` constructs using
   custom operators.

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
        __tablename__ = 'magazine'

        id = Column(Integer, primary_key=True)


    class Article(Base):
        __tablename__ = 'article'

        article_id = Column(Integer)
        magazine_id = Column(ForeignKey('magazine.id'))
        writer_id = Column()

        magazine = relationship("Magazine")
        writer = relationship("Writer")

        __table_args__ = (
            PrimaryKeyConstraint('article_id', 'magazine_id'),
            ForeignKeyConstraint(
                ['writer_id', 'magazine_id'],
                ['writer.id', 'writer.magazine_id']
            ),
        )


    class Writer(Base):
        __tablename__ = 'writer'

        id = Column(Integer, primary_key=True)
        magazine_id = Column(ForeignKey('magazine.id'), primary_key=True)
        magazine = relationship("Magazine")

When the above mapping is configured, we will see this warning emitted::

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
composite key to ``Writer``.   If we associate an ``Article`` with a
particular ``Magazine``, but then associate the ``Article`` with a
``Writer`` that's  associated  with a *different* ``Magazine``, the ORM
will overwrite ``Article.magazine_id`` non-deterministically, silently
changing which magazine we refer towards; it may
also attempt to place NULL into this columnn if we de-associate a
``Writer`` from an ``Article``.  The warning lets us know this is the case.

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

        writer = relationship("Writer", foreign_keys='Article.writer_id')

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
:paramref:`~.relationship.primaryjoin` fully, along with either the
:paramref:`~.relationship.foreign_keys` argument, or more succinctly by
annotating with :func:`~.orm.foreign`::

    class Article(Base):
        # ...

        writer = relationship(
            "Writer",
            primaryjoin="and_(Writer.id == foreign(Article.writer_id), "
                        "Writer.magazine_id == Article.magazine_id)")

.. versionchanged:: 1.0.0 the ORM will attempt to warn when a column is used
   as the synchronization target from more than one relationship
   simultaneously.


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
        __tablename__ = 'element'

        path = Column(String, primary_key=True)

        descendants = relationship('Element',
                               primaryjoin=
                                    remote(foreign(path)).like(
                                            path.concat('/%')),
                               viewonly=True,
                               order_by=path)

Above, if given an ``Element`` object with a path attribute of ``"/foo/bar2"``,
we seek for a load of ``Element.descendants`` to look like::

    SELECT element.path AS element_path
    FROM element
    WHERE element.path LIKE ('/foo/bar2' || '/%') ORDER BY element.path

.. versionadded:: 0.9.5 Support has been added to allow a single-column
   comparison to itself within a primaryjoin condition, as well as for
   primaryjoin conditions that use :meth:`.ColumnOperators.like` as the comparison
   operator.

.. _self_referential_many_to_many:

Self-Referential Many-to-Many Relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many to many relationships can be customized by one or both of :paramref:`~.relationship.primaryjoin`
and :paramref:`~.relationship.secondaryjoin` - the latter is significant for a relationship that
specifies a many-to-many reference using the :paramref:`~.relationship.secondary` argument.
A common situation which involves the usage of :paramref:`~.relationship.primaryjoin` and :paramref:`~.relationship.secondaryjoin`
is when establishing a many-to-many relationship from a class to itself, as shown below::

    from sqlalchemy import Integer, ForeignKey, String, Column, Table
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    node_to_node = Table("node_to_node", Base.metadata,
        Column("left_node_id", Integer, ForeignKey("node.id"), primary_key=True),
        Column("right_node_id", Integer, ForeignKey("node.id"), primary_key=True)
    )

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        label = Column(String)
        right_nodes = relationship("Node",
                            secondary=node_to_node,
                            primaryjoin=id==node_to_node.c.left_node_id,
                            secondaryjoin=id==node_to_node.c.right_node_id,
                            backref="left_nodes"
        )

Where above, SQLAlchemy can't know automatically which columns should connect
to which for the ``right_nodes`` and ``left_nodes`` relationships.   The :paramref:`~.relationship.primaryjoin`
and :paramref:`~.relationship.secondaryjoin` arguments establish how we'd like to join to the association table.
In the Declarative form above, as we are declaring these conditions within the Python
block that corresponds to the ``Node`` class, the ``id`` variable is available directly
as the :class:`.Column` object we wish to join with.

Alternatively, we can define the :paramref:`~.relationship.primaryjoin`
and :paramref:`~.relationship.secondaryjoin` arguments using strings, which is suitable
in the case that our configuration does not have either the ``Node.id`` column
object available yet or the ``node_to_node`` table perhaps isn't yet available.
When referring to a plain :class:`.Table` object in a declarative string, we
use the string name of the table as it is present in the :class:`.MetaData`::

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        label = Column(String)
        right_nodes = relationship("Node",
                            secondary="node_to_node",
                            primaryjoin="Node.id==node_to_node.c.left_node_id",
                            secondaryjoin="Node.id==node_to_node.c.right_node_id",
                            backref="left_nodes"
        )

A classical mapping situation here is similar, where ``node_to_node`` can be joined
to ``node.c.id``::

    from sqlalchemy import Integer, ForeignKey, String, Column, Table, MetaData
    from sqlalchemy.orm import relationship, mapper

    metadata = MetaData()

    node_to_node = Table("node_to_node", metadata,
        Column("left_node_id", Integer, ForeignKey("node.id"), primary_key=True),
        Column("right_node_id", Integer, ForeignKey("node.id"), primary_key=True)
    )

    node = Table("node", metadata,
        Column('id', Integer, primary_key=True),
        Column('label', String)
    )
    class Node(object):
        pass

    mapper(Node, node, properties={
        'right_nodes':relationship(Node,
                            secondary=node_to_node,
                            primaryjoin=node.c.id==node_to_node.c.left_node_id,
                            secondaryjoin=node.c.id==node_to_node.c.right_node_id,
                            backref="left_nodes"
                        )})


Note that in both examples, the :paramref:`~.relationship.backref`
keyword specifies a ``left_nodes`` backref - when
:func:`.relationship` creates the second relationship in the reverse
direction, it's smart enough to reverse the
:paramref:`~.relationship.primaryjoin` and
:paramref:`~.relationship.secondaryjoin` arguments.

.. _composite_secondary_join:

Composite "Secondary" Joins
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    This section features some new and experimental features of SQLAlchemy.

Sometimes, when one seeks to build a :func:`.relationship` between two tables
there is a need for more than just two or three tables to be involved in
order to join them.  This is an area of :func:`.relationship` where one seeks
to push the boundaries of what's possible, and often the ultimate solution to
many of these exotic use cases needs to be hammered out on the SQLAlchemy mailing
list.

In more recent versions of SQLAlchemy, the :paramref:`~.relationship.secondary`
parameter can be used in some of these cases in order to provide a composite
target consisting of multiple tables.   Below is an example of such a
join condition (requires version 0.9.2 at least to function as is)::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        b_id = Column(ForeignKey('b.id'))

        d = relationship("D",
                    secondary="join(B, D, B.d_id == D.id)."
                                "join(C, C.d_id == D.id)",
                    primaryjoin="and_(A.b_id == B.id, A.id == C.a_id)",
                    secondaryjoin="D.id == B.d_id",
                    uselist=False
                    )

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        d_id = Column(ForeignKey('d.id'))

    class C(Base):
        __tablename__ = 'c'

        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))
        d_id = Column(ForeignKey('d.id'))

    class D(Base):
        __tablename__ = 'd'

        id = Column(Integer, primary_key=True)

In the above example, we provide all three of :paramref:`~.relationship.secondary`,
:paramref:`~.relationship.primaryjoin`, and :paramref:`~.relationship.secondaryjoin`,
in the declarative style referring to the named tables ``a``, ``b``, ``c``, ``d``
directly.  A query from ``A`` to ``D`` looks like:

.. sourcecode:: python+sql

    sess.query(A).join(A.d).all()

    {opensql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (
        b AS b_1 JOIN d AS d_1 ON b_1.d_id = d_1.id
            JOIN c AS c_1 ON c_1.d_id = d_1.id)
        ON a.b_id = b_1.id AND a.id = c_1.a_id JOIN d ON d.id = b_1.d_id

In the above example, we take advantage of being able to stuff multiple
tables into a "secondary" container, so that we can join across many
tables while still keeping things "simple" for :func:`.relationship`, in that
there's just "one" table on both the "left" and the "right" side; the
complexity is kept within the middle.

.. versionadded:: 0.9.2  Support is improved for allowing a :func:`.join()`
   construct to be used directly as the target of the :paramref:`~.relationship.secondary`
   argument, including support for joins, eager joins and lazy loading,
   as well as support within declarative to specify complex conditions such
   as joins involving class names as targets.

.. _relationship_non_primary_mapper:

Relationship to Non Primary Mapper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the previous section, we illustrated a technique where we used
:paramref:`~.relationship.secondary` in order to place additional
tables within a join condition.   There is one complex join case where
even this technique is not sufficient; when we seek to join from ``A``
to ``B``, making use of any number of ``C``, ``D``, etc. in between,
however there are also join conditions between ``A`` and ``B``
*directly*.  In this case, the join from ``A`` to ``B`` may be
difficult to express with just a complex
:paramref:`~.relationship.primaryjoin` condition, as the intermediary
tables may need special handling, and it is also not expressable with
a :paramref:`~.relationship.secondary` object, since the
``A->secondary->B`` pattern does not support any references between
``A`` and ``B`` directly.  When this **extremely advanced** case
arises, we can resort to creating a second mapping as a target for the
relationship.  This is where we use :func:`.mapper` in order to make a
mapping to a class that includes all the additional tables we need for
this join. In order to produce this mapper as an "alternative" mapping
for our class, we use the :paramref:`~.mapper.non_primary` flag.

Below illustrates a :func:`.relationship` with a simple join from ``A`` to
``B``, however the primaryjoin condition is augmented with two additional
entities ``C`` and ``D``, which also must have rows that line up with
the rows in both ``A`` and ``B`` simultaneously::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        b_id = Column(ForeignKey('b.id'))

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)

    class C(Base):
        __tablename__ = 'c'

        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))

    class D(Base):
        __tablename__ = 'd'

        id = Column(Integer, primary_key=True)
        c_id = Column(ForeignKey('c.id'))
        b_id = Column(ForeignKey('b.id'))

    # 1. set up the join() as a variable, so we can refer
    # to it in the mapping multiple times.
    j = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)

    # 2. Create a new mapper() to B, with non_primary=True.
    # Columns in the join with the same name must be
    # disambiguated within the mapping, using named properties.
    B_viacd = mapper(B, j, non_primary=True, properties={
        "b_id": [j.c.b_id, j.c.d_b_id],
        "d_id": j.c.d_id
        })

    A.b = relationship(B_viacd, primaryjoin=A.b_id == B_viacd.c.b_id)

In the above case, our non-primary mapper for ``B`` will emit for
additional columns when we query; these can be ignored:

.. sourcecode:: python+sql

    sess.query(A).join(A.b).all()

    {opensql}SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a JOIN (b JOIN d ON d.b_id = b.id JOIN c ON c.id = d.c_id) ON a.b_id = b.id


Building Query-Enabled Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Very ambitious custom join conditions may fail to be directly persistable, and
in some cases may not even load correctly. To remove the persistence part of
the equation, use the flag :paramref:`~.relationship.viewonly` on the
:func:`~sqlalchemy.orm.relationship`, which establishes it as a read-only
attribute (data written to the collection will be ignored on flush()).
However, in extreme cases, consider using a regular Python property in
conjunction with :class:`.Query` as follows:

.. sourcecode:: python

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)

        def _get_addresses(self):
            return object_session(self).query(Address).with_parent(self).filter(...).all()
        addresses = property(_get_addresses)


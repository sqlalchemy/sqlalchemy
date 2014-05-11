.. module:: sqlalchemy.orm

.. _relationship_config_toplevel:

Relationship Configuration
==========================

This section describes the :func:`relationship` function and in depth discussion
of its usage.   The reference material here continues into the next section,
:ref:`collections_toplevel`, which has additional detail on configuration
of collections via :func:`relationship`.

.. _relationship_patterns:

Basic Relational Patterns
--------------------------

A quick walkthrough of the basic relational patterns.

The imports used for each of the following sections is as follows::

    from sqlalchemy import Table, Column, Integer, ForeignKey
    from sqlalchemy.orm import relationship, backref
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()


One To Many
~~~~~~~~~~~~

A one to many relationship places a foreign key on the child table referencing
the parent.  :func:`.relationship` is then specified on the parent, as referencing
a collection of items represented by the child::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

To establish a bidirectional relationship in one-to-many, where the "reverse"
side is a many to one, specify the :paramref:`~.relationship.backref` option::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child", backref="parent")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

``Child`` will get a ``parent`` attribute with many-to-one semantics.

Many To One
~~~~~~~~~~~~

Many to one places a foreign key in the parent table referencing the child.
:func:`.relationship` is declared on the parent, where a new scalar-holding
attribute will be created::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)

Bidirectional behavior is achieved by setting
:paramref:`~.relationship.backref` to the value ``"parents"``, which
will place a one-to-many collection on the ``Child`` class::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", backref="parents")

.. _relationships_one_to_one:

One To One
~~~~~~~~~~~

One To One is essentially a bidirectional relationship with a scalar
attribute on both sides. To achieve this, the :paramref:`~.relationship.uselist` flag indicates
the placement of a scalar attribute instead of a collection on the "many" side
of the relationship. To convert one-to-many into one-to-one::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child = relationship("Child", uselist=False, backref="parent")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

Or to turn a one-to-many backref into one-to-one, use the :func:`.backref` function
to provide arguments for the reverse side::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", backref=backref("parent", uselist=False))

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)

.. _relationships_many_to_many:

Many To Many
~~~~~~~~~~~~~

Many to Many adds an association table between two classes. The association
table is indicated by the :paramref:`~.relationship.secondary` argument to
:func:`.relationship`.  Usually, the :class:`.Table` uses the :class:`.MetaData`
object associated with the declarative base class, so that the :class:`.ForeignKey`
directives can locate the remote tables with which to link::

    association_table = Table('association', Base.metadata,
        Column('left_id', Integer, ForeignKey('left.id')),
        Column('right_id', Integer, ForeignKey('right.id'))
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=association_table)

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

For a bidirectional relationship, both sides of the relationship contain a
collection.  The :paramref:`~.relationship.backref` keyword will automatically use
the same :paramref:`~.relationship.secondary` argument for the reverse relationship::

    association_table = Table('association', Base.metadata,
        Column('left_id', Integer, ForeignKey('left.id')),
        Column('right_id', Integer, ForeignKey('right.id'))
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=association_table,
                        backref="parents")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

The :paramref:`~.relationship.secondary` argument of :func:`.relationship` also accepts a callable
that returns the ultimate argument, which is evaluated only when mappers are
first used.   Using this, we can define the ``association_table`` at a later
point, as long as it's available to the callable after all module initialization
is complete::

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=lambda: association_table,
                        backref="parents")

With the declarative extension in use, the traditional "string name of the table"
is accepted as well, matching the name of the table as stored in ``Base.metadata.tables``::

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary="association",
                        backref="parents")

.. _relationships_many_to_many_deletion:

Deleting Rows from the Many to Many Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A behavior which is unique to the :paramref:`~.relationship.secondary` argument to :func:`.relationship`
is that the :class:`.Table` which is specified here is automatically subject
to INSERT and DELETE statements, as objects are added or removed from the collection.
There is **no need to delete from this table manually**.   The act of removing a
record from the collection will have the effect of the row being deleted on flush::

    # row will be deleted from the "secondary" table
    # automatically
    myparent.children.remove(somechild)

A question which often arises is how the row in the "secondary" table can be deleted
when the child object is handed directly to :meth:`.Session.delete`::

    session.delete(somechild)

There are several possibilities here:

* If there is a :func:`.relationship` from ``Parent`` to ``Child``, but there is
  **not** a reverse-relationship that links a particular ``Child`` to each ``Parent``,
  SQLAlchemy will not have any awareness that when deleting this particular
  ``Child`` object, it needs to maintain the "secondary" table that links it to
  the ``Parent``.  No delete of the "secondary" table will occur.
* If there is a relationship that links a particular ``Child`` to each ``Parent``,
  suppose it's called ``Child.parents``, SQLAlchemy by default will load in
  the ``Child.parents`` collection to locate all ``Parent`` objects, and remove
  each row from the "secondary" table which establishes this link.  Note that
  this relationship does not need to be bidrectional; SQLAlchemy is strictly
  looking at every :func:`.relationship` associated with the ``Child`` object
  being deleted.
* A higher performing option here is to use ON DELETE CASCADE directives
  with the foreign keys used by the database.   Assuming the database supports
  this feature, the database itself can be made to automatically delete rows in the
  "secondary" table as referencing rows in "child" are deleted.   SQLAlchemy
  can be instructed to forego actively loading in the ``Child.parents``
  collection in this case using the :paramref:`~.relationship.passive_deletes`
  directive on :func:`.relationship`; see :ref:`passive_deletes` for more details
  on this.

Note again, these behaviors are *only* relevant to the :paramref:`~.relationship.secondary` option
used with :func:`.relationship`.   If dealing with association tables that
are mapped explicitly and are *not* present in the :paramref:`~.relationship.secondary` option
of a relevant :func:`.relationship`, cascade rules can be used instead
to automatically delete entities in reaction to a related entity being
deleted - see :ref:`unitofwork_cascades` for information on this feature.


.. _association_pattern:

Association Object
~~~~~~~~~~~~~~~~~~

The association object pattern is a variant on many-to-many: it's used
when your association table contains additional columns beyond those
which are foreign keys to the left and right tables. Instead of using
the :paramref:`~.relationship.secondary` argument, you map a new class
directly to the association table. The left side of the relationship
references the association object via one-to-many, and the association
class references the right side via many-to-one.  Below we illustrate
an association table mapped to the ``Association`` class which
includes a column called ``extra_data``, which is a string value that
is stored along with each association between ``Parent`` and
``Child``::

    class Association(Base):
        __tablename__ = 'association'
        left_id = Column(Integer, ForeignKey('left.id'), primary_key=True)
        right_id = Column(Integer, ForeignKey('right.id'), primary_key=True)
        extra_data = Column(String(50))
        child = relationship("Child")

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Association")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

The bidirectional version adds backrefs to both relationships::

    class Association(Base):
        __tablename__ = 'association'
        left_id = Column(Integer, ForeignKey('left.id'), primary_key=True)
        right_id = Column(Integer, ForeignKey('right.id'), primary_key=True)
        extra_data = Column(String(50))
        child = relationship("Child", backref="parent_assocs")

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Association", backref="parent")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

Working with the association pattern in its direct form requires that child
objects are associated with an association instance before being appended to
the parent; similarly, access from parent to child goes through the
association object::

    # create parent, append a child via association
    p = Parent()
    a = Association(extra_data="some data")
    a.child = Child()
    p.children.append(a)

    # iterate through child objects via association, including association
    # attributes
    for assoc in p.children:
        print assoc.extra_data
        print assoc.child

To enhance the association object pattern such that direct
access to the ``Association`` object is optional, SQLAlchemy
provides the :ref:`associationproxy_toplevel` extension. This
extension allows the configuration of attributes which will
access two "hops" with a single access, one "hop" to the
associated object, and a second to a target attribute.

.. note::

  When using the association object pattern, it is advisable that the
  association-mapped table not be used as the
  :paramref:`~.relationship.secondary` argument on a
  :func:`.relationship` elsewhere, unless that :func:`.relationship`
  contains the option :paramref:`~.relationship.viewonly` set to
  ``True``. SQLAlchemy otherwise may attempt to emit redundant INSERT
  and DELETE statements on the same table, if similar state is
  detected on the related attribute as well as the associated object.

.. _self_referential:

Adjacency List Relationships
-----------------------------

The **adjacency list** pattern is a common relational pattern whereby a table
contains a foreign key reference to itself. This is the most common
way to represent hierarchical data in flat tables.  Other methods
include **nested sets**, sometimes called "modified preorder",
as well as **materialized path**.  Despite the appeal that modified preorder
has when evaluated for its fluency within SQL queries, the adjacency list model is
probably the most appropriate pattern for the large majority of hierarchical
storage needs, for reasons of concurrency, reduced complexity, and that
modified preorder has little advantage over an application which can fully
load subtrees into the application space.

In this example, we'll work with a single mapped
class called ``Node``, representing a tree structure::

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('node.id'))
        data = Column(String(50))
        children = relationship("Node")

With this structure, a graph such as the following::

    root --+---> child1
           +---> child2 --+--> subchild1
           |              +--> subchild2
           +---> child3

Would be represented with data such as::

    id       parent_id     data
    ---      -------       ----
    1        NULL          root
    2        1             child1
    3        1             child2
    4        3             subchild1
    5        3             subchild2
    6        1             child3

The :func:`.relationship` configuration here works in the
same way as a "normal" one-to-many relationship, with the
exception that the "direction", i.e. whether the relationship
is one-to-many or many-to-one, is assumed by default to
be one-to-many.   To establish the relationship as many-to-one,
an extra directive is added known as :paramref:`~.relationship.remote_side`, which
is a :class:`.Column` or collection of :class:`.Column` objects
that indicate those which should be considered to be "remote"::

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('node.id'))
        data = Column(String(50))
        parent = relationship("Node", remote_side=[id])

Where above, the ``id`` column is applied as the :paramref:`~.relationship.remote_side`
of the ``parent`` :func:`.relationship`, thus establishing
``parent_id`` as the "local" side, and the relationship
then behaves as a many-to-one.

As always, both directions can be combined into a bidirectional
relationship using the :func:`.backref` function::

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('node.id'))
        data = Column(String(50))
        children = relationship("Node",
                    backref=backref('parent', remote_side=[id])
                )

There are several examples included with SQLAlchemy illustrating
self-referential strategies; these include :ref:`examples_adjacencylist` and
:ref:`examples_xmlpersistence`.

Composite Adjacency Lists
~~~~~~~~~~~~~~~~~~~~~~~~~

A sub-category of the adjacency list relationship is the rare
case where a particular column is present on both the "local" and
"remote" side of the join condition.  An example is the ``Folder``
class below; using a composite primary key, the ``account_id``
column refers to itself, to indicate sub folders which are within
the same account as that of the parent; while ``folder_id`` refers
to a specific folder within that account::

    class Folder(Base):
        __tablename__ = 'folder'
        __table_args__ = (
          ForeignKeyConstraint(
              ['account_id', 'parent_id'],
              ['folder.account_id', 'folder.folder_id']),
        )

        account_id = Column(Integer, primary_key=True)
        folder_id = Column(Integer, primary_key=True)
        parent_id = Column(Integer)
        name = Column(String)

        parent_folder = relationship("Folder",
                            backref="child_folders",
                            remote_side=[account_id, folder_id]
                      )

Above, we pass ``account_id`` into the :paramref:`~.relationship.remote_side` list.
:func:`.relationship` recognizes that the ``account_id`` column here
is on both sides, and aligns the "remote" column along with the
``folder_id`` column, which it recognizes as uniquely present on
the "remote" side.

.. versionadded:: 0.8
    Support for self-referential composite keys in :func:`.relationship`
    where a column points to itself.

Self-Referential Query Strategies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Querying of self-referential structures works like any other query::

    # get all nodes named 'child2'
    session.query(Node).filter(Node.data=='child2')

However extra care is needed when attempting to join along
the foreign key from one level of the tree to the next.  In SQL,
a join from a table to itself requires that at least one side of the
expression be "aliased" so that it can be unambiguously referred to.

Recall from :ref:`ormtutorial_aliases` in the ORM tutorial that the
:func:`.orm.aliased` construct is normally used to provide an "alias" of
an ORM entity.  Joining from ``Node`` to itself using this technique
looks like:

.. sourcecode:: python+sql

    from sqlalchemy.orm import aliased

    nodealias = aliased(Node)
    {sql}session.query(Node).filter(Node.data=='subchild1').\
                    join(nodealias, Node.parent).\
                    filter(nodealias.data=="child2").\
                    all()
    SELECT node.id AS node_id,
            node.parent_id AS node_parent_id,
            node.data AS node_data
    FROM node JOIN node AS node_1
        ON node.parent_id = node_1.id
    WHERE node.data = ?
        AND node_1.data = ?
    ['subchild1', 'child2']

:meth:`.Query.join` also includes a feature known as
:paramref:`.Query.join.aliased` that can shorten the verbosity self-
referential joins, at the expense of query flexibility.  This feature
performs a similar "aliasing" step to that above, without the need for
an explicit entity.   Calls to :meth:`.Query.filter` and similar
subsequent to the aliased join will **adapt** the ``Node`` entity to
be that of the alias:

.. sourcecode:: python+sql

    {sql}session.query(Node).filter(Node.data=='subchild1').\
            join(Node.parent, aliased=True).\
            filter(Node.data=='child2').\
            all()
    SELECT node.id AS node_id,
            node.parent_id AS node_parent_id,
            node.data AS node_data
    FROM node
        JOIN node AS node_1 ON node_1.id = node.parent_id
    WHERE node.data = ? AND node_1.data = ?
    ['subchild1', 'child2']

To add criterion to multiple points along a longer join, add
:paramref:`.Query.join.from_joinpoint` to the additional
:meth:`~.Query.join` calls:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a
    # parent named 'child2' and a grandparent 'root'
    {sql}session.query(Node).\
            filter(Node.data=='subchild1').\
            join(Node.parent, aliased=True).\
            filter(Node.data=='child2').\
            join(Node.parent, aliased=True, from_joinpoint=True).\
            filter(Node.data=='root').\
            all()
    SELECT node.id AS node_id,
            node.parent_id AS node_parent_id,
            node.data AS node_data
    FROM node
        JOIN node AS node_1 ON node_1.id = node.parent_id
        JOIN node AS node_2 ON node_2.id = node_1.parent_id
    WHERE node.data = ?
        AND node_1.data = ?
        AND node_2.data = ?
    ['subchild1', 'child2', 'root']

:meth:`.Query.reset_joinpoint` will also remove the "aliasing" from filtering
calls::

    session.query(Node).\
            join(Node.children, aliased=True).\
            filter(Node.data == 'foo').\
            reset_joinpoint().\
            filter(Node.data == 'bar')

For an example of using :paramref:`.Query.join.aliased` to
arbitrarily join along a chain of self-referential nodes, see
:ref:`examples_xmlpersistence`.

.. _self_referential_eager_loading:

Configuring Self-Referential Eager Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Eager loading of relationships occurs using joins or outerjoins from parent to
child table during a normal query operation, such that the parent and its
immediate child collection or reference can be populated from a single SQL
statement, or a second statement for all immediate child collections.
SQLAlchemy's joined and subquery eager loading use aliased tables in all cases
when joining to related items, so are compatible with self-referential
joining. However, to use eager loading with a self-referential relationship,
SQLAlchemy needs to be told how many levels deep it should join and/or query;
otherwise the eager load will not take place at all. This depth setting is
configured via :paramref:`~.relationships.join_depth`:

.. sourcecode:: python+sql

    class Node(Base):
        __tablename__ = 'node'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('node.id'))
        data = Column(String(50))
        children = relationship("Node",
                        lazy="joined",
                        join_depth=2)

    {sql}session.query(Node).all()
    SELECT node_1.id AS node_1_id,
            node_1.parent_id AS node_1_parent_id,
            node_1.data AS node_1_data,
            node_2.id AS node_2_id,
            node_2.parent_id AS node_2_parent_id,
            node_2.data AS node_2_data,
            node.id AS node_id,
            node.parent_id AS node_parent_id,
            node.data AS node_data
    FROM node
        LEFT OUTER JOIN node AS node_2
            ON node.id = node_2.parent_id
        LEFT OUTER JOIN node AS node_1
            ON node_2.id = node_1.parent_id
    []

.. _relationships_backref:

Linking Relationships with Backref
----------------------------------

The :paramref:`~.relationship.backref` keyword argument was first introduced in :ref:`ormtutorial_toplevel`, and has been
mentioned throughout many of the examples here.   What does it actually do ?   Let's start
with the canonical ``User`` and ``Address`` scenario::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", backref="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

The above configuration establishes a collection of ``Address`` objects on ``User`` called
``User.addresses``.   It also establishes a ``.user`` attribute on ``Address`` which will
refer to the parent ``User`` object.

In fact, the :paramref:`~.relationship.backref` keyword is only a common shortcut for placing a second
:func:`.relationship` onto the ``Address`` mapping, including the establishment
of an event listener on both sides which will mirror attribute operations
in both directions.   The above configuration is equivalent to::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", back_populates="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

        user = relationship("User", back_populates="addresses")

Above, we add a ``.user`` relationship to ``Address`` explicitly.  On
both relationships, the :paramref:`~.relationship.back_populates` directive tells each relationship
about the other one, indicating that they should establish "bidirectional"
behavior between each other.   The primary effect of this configuration
is that the relationship adds event handlers to both attributes
which have the behavior of "when an append or set event occurs here, set ourselves
onto the incoming attribute using this particular attribute name".
The behavior is illustrated as follows.   Start with a ``User`` and an ``Address``
instance.  The ``.addresses`` collection is empty, and the ``.user`` attribute
is ``None``::

    >>> u1 = User()
    >>> a1 = Address()
    >>> u1.addresses
    []
    >>> print a1.user
    None

However, once the ``Address`` is appended to the ``u1.addresses`` collection,
both the collection and the scalar attribute have been populated::

    >>> u1.addresses.append(a1)
    >>> u1.addresses
    [<__main__.Address object at 0x12a6ed0>]
    >>> a1.user
    <__main__.User object at 0x12a6590>

This behavior of course works in reverse for removal operations as well, as well
as for equivalent operations on both sides.   Such as
when ``.user`` is set again to ``None``, the ``Address`` object is removed
from the reverse collection::

    >>> a1.user = None
    >>> u1.addresses
    []

The manipulation of the ``.addresses`` collection and the ``.user`` attribute
occurs entirely in Python without any interaction with the SQL database.
Without this behavior, the proper state would be apparent on both sides once the
data has been flushed to the database, and later reloaded after a commit or
expiration operation occurs.  The :paramref:`~.relationship.backref`/:paramref:`~.relationship.back_populates` behavior has the advantage
that common bidirectional operations can reflect the correct state without requiring
a database round trip.

Remember, when the :paramref:`~.relationship.backref` keyword is used on a single relationship, it's
exactly the same as if the above two relationships were created individually
using :paramref:`~.relationship.back_populates` on each.

Backref Arguments
~~~~~~~~~~~~~~~~~~

We've established that the :paramref:`~.relationship.backref` keyword is merely a shortcut for building
two individual :func:`.relationship` constructs that refer to each other.  Part of
the behavior of this shortcut is that certain configurational arguments applied to
the :func:`.relationship`
will also be applied to the other direction - namely those arguments that describe
the relationship at a schema level, and are unlikely to be different in the reverse
direction.  The usual case
here is a many-to-many :func:`.relationship` that has a :paramref:`~.relationship.secondary` argument,
or a one-to-many or many-to-one which has a :paramref:`~.relationship.primaryjoin` argument (the
:paramref:`~.relationship.primaryjoin` argument is discussed in :ref:`relationship_primaryjoin`).  Such
as if we limited the list of ``Address`` objects to those which start with "tony"::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address",
                        primaryjoin="and_(User.id==Address.user_id, "
                            "Address.email.startswith('tony'))",
                        backref="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

We can observe, by inspecting the resulting property, that both sides
of the relationship have this join condition applied::

    >>> print User.addresses.property.primaryjoin
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>
    >>> print Address.user.property.primaryjoin
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>

This reuse of arguments should pretty much do the "right thing" - it
uses only arguments that are applicable, and in the case of a many-to-
many relationship, will reverse the usage of
:paramref:`~.relationship.primaryjoin` and
:paramref:`~.relationship.secondaryjoin` to correspond to the other
direction (see the example in :ref:`self_referential_many_to_many` for
this).

It's very often the case however that we'd like to specify arguments
that are specific to just the side where we happened to place the
"backref". This includes :func:`.relationship` arguments like
:paramref:`~.relationship.lazy`,
:paramref:`~.relationship.remote_side`,
:paramref:`~.relationship.cascade` and
:paramref:`~.relationship.cascade_backrefs`.   For this case we use
the :func:`.backref` function in place of a string::

    # <other imports>
    from sqlalchemy.orm import backref

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address",
                        backref=backref("user", lazy="joined"))

Where above, we placed a ``lazy="joined"`` directive only on the ``Address.user``
side, indicating that when a query against ``Address`` is made, a join to the ``User``
entity should be made automatically which will populate the ``.user`` attribute of each
returned ``Address``.   The :func:`.backref` function formatted the arguments we gave
it into a form that is interpreted by the receiving :func:`.relationship` as additional
arguments to be applied to the new relationship it creates.

One Way Backrefs
~~~~~~~~~~~~~~~~~

An unusual case is that of the "one way backref".   This is where the
"back-populating" behavior of the backref is only desirable in one
direction. An example of this is a collection which contains a
filtering :paramref:`~.relationship.primaryjoin` condition.   We'd
like to append items to this collection as needed, and have them
populate the "parent" object on the incoming object. However, we'd
also like to have items that are not part of the collection, but still
have the same "parent" association - these items should never be in
the collection.

Taking our previous example, where we established a
:paramref:`~.relationship.primaryjoin` that limited the collection
only to ``Address`` objects whose email address started with the word
``tony``, the usual backref behavior is that all items populate in
both directions.   We wouldn't want this behavior for a case like the
following::

    >>> u1 = User()
    >>> a1 = Address(email='mary')
    >>> a1.user = u1
    >>> u1.addresses
    [<__main__.Address object at 0x1411910>]

Above, the ``Address`` object that doesn't match the criterion of "starts with 'tony'"
is present in the ``addresses`` collection of ``u1``.   After these objects are flushed,
the transaction committed and their attributes expired for a re-load, the ``addresses``
collection will hit the database on next access and no longer have this ``Address`` object
present, due to the filtering condition.   But we can do away with this unwanted side
of the "backref" behavior on the Python side by using two separate :func:`.relationship` constructs,
placing :paramref:`~.relationship.back_populates` only on one side::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        addresses = relationship("Address",
                        primaryjoin="and_(User.id==Address.user_id, "
                            "Address.email.startswith('tony'))",
                        back_populates="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))
        user = relationship("User")

With the above scenario, appending an ``Address`` object to the ``.addresses``
collection of a ``User`` will always establish the ``.user`` attribute on that
``Address``::

    >>> u1 = User()
    >>> a1 = Address(email='tony')
    >>> u1.addresses.append(a1)
    >>> a1.user
    <__main__.User object at 0x1411850>

However, applying a ``User`` to the ``.user`` attribute of an ``Address``,
will not append the ``Address`` object to the collection::

    >>> a2 = Address(email='mary')
    >>> a2.user = u1
    >>> a2 in u1.addresses
    False

Of course, we've disabled some of the usefulness of
:paramref:`~.relationship.backref` here, in that when we do append an
``Address`` that corresponds to the criteria of
``email.startswith('tony')``, it won't show up in the
``User.addresses`` collection until the session is flushed, and the
attributes reloaded after a commit or expire operation.   While we
could consider an attribute event that checks this criterion in
Python, this starts to cross the line of duplicating too much SQL
behavior in Python.  The backref behavior itself is only a slight
transgression of this philosophy - SQLAlchemy tries to keep these to a
minimum overall.

.. _relationship_configure_joins:

Configuring how Relationship Joins
------------------------------------

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
column to the ``ip_address`` column, which is a Postgresql type called ``INET``.
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
:func:`.remote` :term:`annotations`, inline within the :paramref:`~.relationship.primaryjoin` expression.
This syntax represents the annotations that :func:`.relationship` normally
applies by itself to the join condition given the :paramref:`~.relationship.foreign_keys` and
:paramref:`~.relationship.remote_side` arguments; the functions are provided in the API in the
rare case that :func:`.relationship` can't determine the exact location
of these features on its own::

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another use case for relationships is the use of custom operators, such
as Postgresql's "is contained within" ``<<`` operator when joining with
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
   primaryjoin conditions that use :meth:`.Operators.like` as the comparison
   operator.

.. _self_referential_many_to_many:

Self-Referential Many-to-Many Relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Very ambitious custom join conditions may fail to be directly persistable, and
in some cases may not even load correctly. To remove the persistence part of
the equation, use the flag :paramref:`~.relationship.viewonly` on the
:func:`~sqlalchemy.orm.relationship`, which establishes it as a read-only
attribute (data written to the collection will be ignored on flush()).
However, in extreme cases, consider using a regular Python property in
conjunction with :class:`.Query` as follows:

.. sourcecode:: python+sql

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)

        def _get_addresses(self):
            return object_session(self).query(Address).with_parent(self).filter(...).all()
        addresses = property(_get_addresses)


.. _post_update:

Rows that point to themselves / Mutually Dependent Rows
-------------------------------------------------------

This is a very specific case where relationship() must perform an INSERT and a
second UPDATE in order to properly populate a row (and vice versa an UPDATE
and DELETE in order to delete without violating foreign key constraints). The
two use cases are:

* A table contains a foreign key to itself, and a single row will
  have a foreign key value pointing to its own primary key.
* Two tables each contain a foreign key referencing the other
  table, with a row in each table referencing the other.

For example::

              user
    ---------------------------------
    user_id    name   related_user_id
       1       'ed'          1

Or::

                 widget                                                  entry
    -------------------------------------------             ---------------------------------
    widget_id     name        favorite_entry_id             entry_id      name      widget_id
       1       'somewidget'          5                         5       'someentry'     1

In the first case, a row points to itself. Technically, a database that uses
sequences such as PostgreSQL or Oracle can INSERT the row at once using a
previously generated value, but databases which rely upon autoincrement-style
primary key identifiers cannot. The :func:`~sqlalchemy.orm.relationship`
always assumes a "parent/child" model of row population during flush, so
unless you are populating the primary key/foreign key columns directly,
:func:`~sqlalchemy.orm.relationship` needs to use two statements.

In the second case, the "widget" row must be inserted before any referring
"entry" rows, but then the "favorite_entry_id" column of that "widget" row
cannot be set until the "entry" rows have been generated. In this case, it's
typically impossible to insert the "widget" and "entry" rows using just two
INSERT statements; an UPDATE must be performed in order to keep foreign key
constraints fulfilled. The exception is if the foreign keys are configured as
"deferred until commit" (a feature some databases support) and if the
identifiers were populated manually (again essentially bypassing
:func:`~sqlalchemy.orm.relationship`).

To enable the usage of a supplementary UPDATE statement,
we use the :paramref:`~.relationship.post_update` option
of :func:`.relationship`.  This specifies that the linkage between the
two rows should be created using an UPDATE statement after both rows
have been INSERTED; it also causes the rows to be de-associated with
each other via UPDATE before a DELETE is emitted.  The flag should
be placed on just *one* of the relationships, preferably the
many-to-one side.  Below we illustrate
a complete example, including two :class:`.ForeignKey` constructs, one which
specifies :paramref:`~.ForeignKey.use_alter` to help with emitting CREATE TABLE statements::

    from sqlalchemy import Integer, ForeignKey, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class Entry(Base):
        __tablename__ = 'entry'
        entry_id = Column(Integer, primary_key=True)
        widget_id = Column(Integer, ForeignKey('widget.widget_id'))
        name = Column(String(50))

    class Widget(Base):
        __tablename__ = 'widget'

        widget_id = Column(Integer, primary_key=True)
        favorite_entry_id = Column(Integer,
                                ForeignKey('entry.entry_id',
                                use_alter=True,
                                name="fk_favorite_entry"))
        name = Column(String(50))

        entries = relationship(Entry, primaryjoin=
                                        widget_id==Entry.widget_id)
        favorite_entry = relationship(Entry,
                                    primaryjoin=
                                        favorite_entry_id==Entry.entry_id,
                                    post_update=True)

When a structure against the above configuration is flushed, the "widget" row will be
INSERTed minus the "favorite_entry_id" value, then all the "entry" rows will
be INSERTed referencing the parent "widget" row, and then an UPDATE statement
will populate the "favorite_entry_id" column of the "widget" table (it's one
row at a time for the time being):

.. sourcecode:: pycon+sql

    >>> w1 = Widget(name='somewidget')
    >>> e1 = Entry(name='someentry')
    >>> w1.favorite_entry = e1
    >>> w1.entries = [e1]
    >>> session.add_all([w1, e1])
    {sql}>>> session.commit()
    BEGIN (implicit)
    INSERT INTO widget (favorite_entry_id, name) VALUES (?, ?)
    (None, 'somewidget')
    INSERT INTO entry (widget_id, name) VALUES (?, ?)
    (1, 'someentry')
    UPDATE widget SET favorite_entry_id=? WHERE widget.widget_id = ?
    (1, 1)
    COMMIT

An additional configuration we can specify is to supply a more
comprehensive foreign key constraint on ``Widget``, such that
it's guaranteed that ``favorite_entry_id`` refers to an ``Entry``
that also refers to this ``Widget``.  We can use a composite foreign key,
as illustrated below::

    from sqlalchemy import Integer, ForeignKey, String, \
            Column, UniqueConstraint, ForeignKeyConstraint
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class Entry(Base):
        __tablename__ = 'entry'
        entry_id = Column(Integer, primary_key=True)
        widget_id = Column(Integer, ForeignKey('widget.widget_id'))
        name = Column(String(50))
        __table_args__ = (
            UniqueConstraint("entry_id", "widget_id"),
        )

    class Widget(Base):
        __tablename__ = 'widget'

        widget_id = Column(Integer, autoincrement='ignore_fk', primary_key=True)
        favorite_entry_id = Column(Integer)

        name = Column(String(50))

        __table_args__ = (
            ForeignKeyConstraint(
                ["widget_id", "favorite_entry_id"],
                ["entry.widget_id", "entry.entry_id"],
                name="fk_favorite_entry", use_alter=True
            ),
        )

        entries = relationship(Entry, primaryjoin=
                                        widget_id==Entry.widget_id,
                                        foreign_keys=Entry.widget_id)
        favorite_entry = relationship(Entry,
                                    primaryjoin=
                                        favorite_entry_id==Entry.entry_id,
                                    foreign_keys=favorite_entry_id,
                                    post_update=True)

The above mapping features a composite :class:`.ForeignKeyConstraint`
bridging the ``widget_id`` and ``favorite_entry_id`` columns.  To ensure
that ``Widget.widget_id`` remains an "autoincrementing" column we specify
:paramref:`~.Column.autoincrement` to the value ``"ignore_fk"``
on :class:`.Column`, and additionally on each
:func:`.relationship` we must limit those columns considered as part of
the foreign key for the purposes of joining and cross-population.

.. _passive_updates:

Mutable Primary Keys / Update Cascades
---------------------------------------

When the primary key of an entity changes, related items
which reference the primary key must also be updated as
well. For databases which enforce referential integrity,
it's required to use the database's ON UPDATE CASCADE
functionality in order to propagate primary key changes
to referenced foreign keys - the values cannot be out
of sync for any moment.

For databases that don't support this, such as SQLite and
MySQL without their referential integrity options turned
on, the :paramref:`~.relationship.passive_updates` flag can
be set to ``False``, most preferably on a one-to-many or
many-to-many :func:`.relationship`, which instructs
SQLAlchemy to issue UPDATE statements individually for
objects referenced in the collection, loading them into
memory if not already locally present. The
:paramref:`~.relationship.passive_updates` flag can also be ``False`` in
conjunction with ON UPDATE CASCADE functionality,
although in that case the unit of work will be issuing
extra SELECT and UPDATE statements unnecessarily.

A typical mutable primary key setup might look like::

    class User(Base):
        __tablename__ = 'user'

        username = Column(String(50), primary_key=True)
        fullname = Column(String(100))

        # passive_updates=False *only* needed if the database
        # does not implement ON UPDATE CASCADE
        addresses = relationship("Address", passive_updates=False)

    class Address(Base):
        __tablename__ = 'address'

        email = Column(String(50), primary_key=True)
        username = Column(String(50),
                    ForeignKey('user.username', onupdate="cascade")
                )

:paramref:`~.relationship.passive_updates` is set to ``True`` by default,
indicating that ON UPDATE CASCADE is expected to be in
place in the usual case for foreign keys that expect
to have a mutating parent key.

A :paramref:`~.relationship.passive_updates` setting of False may be configured on any
direction of relationship, i.e. one-to-many, many-to-one,
and many-to-many, although it is much more effective when
placed just on the one-to-many or many-to-many side.
Configuring the :paramref:`~.relationship.passive_updates`
to False only on the
many-to-one side will have only a partial effect, as the
unit of work searches only through the current identity
map for objects that may be referencing the one with a
mutating primary key, not throughout the database.

Relationships API
-----------------

.. autofunction:: relationship

.. autofunction:: backref

.. autofunction:: relation

.. autofunction:: dynamic_loader

.. autofunction:: foreign

.. autofunction:: remote




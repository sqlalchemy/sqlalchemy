.. _self_referential:

Adjacency List Relationships
----------------------------

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


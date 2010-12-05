.. module:: sqlalchemy.orm

.. _relationship_config_toplevel:

Relationship Configuration
==========================

This section describes the :func:`relationship` function and in depth discussion
of its usage.   The reference material here continues into the next section,
:ref:`collections_toplevel`, which has additional detail on configuration
of collections via :func:`relationship`.

Basic Relational Patterns
--------------------------

A quick walkthrough of the basic relational patterns. In this section we
illustrate the classical mapping using :func:`mapper` in conjunction with
:func:`relationship`. Then (by popular demand), we illustrate the declarative
form using the :mod:`~sqlalchemy.ext.declarative` module.

Note that :func:`.relationship` is historically known as
:func:`.relation` in older versions of SQLAlchemy.

One To Many
~~~~~~~~~~~~

A one to many relationship places a foreign key in the child table referencing
the parent. SQLAlchemy creates the relationship as a collection on the parent
object containing instances of the child object.

.. sourcecode:: python+sql

    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True))

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('parent.id'))
    )

    class Parent(object):
        pass

    class Child(object):
        pass

    mapper(Parent, parent_table, properties={
        'children': relationship(Child)
    })

    mapper(Child, child_table)

To establish a bi-directional relationship in one-to-many, where the "reverse" side is a many to one, specify the ``backref`` option:

.. sourcecode:: python+sql

    mapper(Parent, parent_table, properties={
        'children': relationship(Child, backref='parent')
    })

    mapper(Child, child_table)

``Child`` will get a ``parent`` attribute with many-to-one semantics.

Declarative::
    
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    
    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child", backref="parent")
        
    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))
        

Many To One
~~~~~~~~~~~~

Many to one places a foreign key in the parent table referencing the child.
The mapping setup is identical to one-to-many, however SQLAlchemy creates the
relationship as a scalar attribute on the parent object referencing a single
instance of the child object.

.. sourcecode:: python+sql

    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True),
        Column('child_id', Integer, ForeignKey('child.id')))

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True),
        )

    class Parent(object):
        pass

    class Child(object):
        pass

    mapper(Parent, parent_table, properties={
        'child': relationship(Child)
    })

    mapper(Child, child_table)

Backref behavior is available here as well, where ``backref="parents"`` will
place a one-to-many collection on the ``Child`` class::

    mapper(Parent, parent_table, properties={
        'child': relationship(Child, backref="parents")
    })

Declarative::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", backref="parents")
        
    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)

One To One
~~~~~~~~~~~

One To One is essentially a bi-directional relationship with a scalar
attribute on both sides. To achieve this, the ``uselist=False`` flag indicates
the placement of a scalar attribute instead of a collection on the "many" side
of the relationship. To convert one-to-many into one-to-one::

    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True)
    )

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('parent.id'))
    )

    mapper(Parent, parent_table, properties={
        'child': relationship(Child, uselist=False, backref='parent')
    })
    
    mapper(Child, child_table)

Or to turn a one-to-many backref into one-to-one, use the :func:`.backref` function
to provide arguments for the reverse side::
    
    from sqlalchemy.orm import backref
    
    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True),
        Column('child_id', Integer, ForeignKey('child.id'))
    )

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True)
    )

    mapper(Parent, parent_table, properties={
        'child': relationship(Child, backref=backref('parent', uselist=False))
    })

    mapper(Child, child_table)

The second example above as declarative::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", backref=backref("parent", uselist=False))
        
    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
    
Many To Many
~~~~~~~~~~~~~

Many to Many adds an association table between two classes. The association
table is indicated by the ``secondary`` argument to
:func:`.relationship`.

.. sourcecode:: python+sql

    left_table = Table('left', metadata,
        Column('id', Integer, primary_key=True)
    )

    right_table = Table('right', metadata,
        Column('id', Integer, primary_key=True)
    )

    association_table = Table('association', metadata,
        Column('left_id', Integer, ForeignKey('left.id')),
        Column('right_id', Integer, ForeignKey('right.id'))
    )

    mapper(Parent, left_table, properties={
        'children': relationship(Child, secondary=association_table)
    })

    mapper(Child, right_table)

For a bi-directional relationship, both sides of the relationship contain a
collection.  The ``backref`` keyword will automatically use
the same ``secondary`` argument for the reverse relationship:

.. sourcecode:: python+sql

    mapper(Parent, left_table, properties={
        'children': relationship(Child, secondary=association_table, 
                                        backref='parents')
    })

With declarative, we still use the :class:`.Table` for the ``secondary`` 
argument.  A class is not mapped to this table, so it remains in its 
plain schematic form::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

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
    
.. _association_pattern:

Association Object
~~~~~~~~~~~~~~~~~~

The association object pattern is a variant on many-to-many: it specifically
is used when your association table contains additional columns beyond those
which are foreign keys to the left and right tables. Instead of using the
``secondary`` argument, you map a new class directly to the association table.
The left side of the relationship references the association object via
one-to-many, and the association class references the right side via
many-to-one.

.. sourcecode:: python+sql

    left_table = Table('left', metadata,
        Column('id', Integer, primary_key=True)
    )

    right_table = Table('right', metadata,
        Column('id', Integer, primary_key=True)
    )

    association_table = Table('association', metadata,
        Column('left_id', Integer, ForeignKey('left.id'), primary_key=True),
        Column('right_id', Integer, ForeignKey('right.id'), primary_key=True),
        Column('data', String(50))
    )

    mapper(Parent, left_table, properties={
        'children':relationship(Association)
    })

    mapper(Association, association_table, properties={
        'child':relationship(Child)
    })

    mapper(Child, right_table)

The bi-directional version adds backrefs to both relationships:

.. sourcecode:: python+sql

    mapper(Parent, left_table, properties={
        'children':relationship(Association, backref="parent")
    })

    mapper(Association, association_table, properties={
        'child':relationship(Child, backref="parent_assocs")
    })

    mapper(Child, right_table)

Declarative::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

    class Association(Base):
        __tablename__ = 'association'
        left_id = Column(Integer, ForeignKey('left.id'), primary_key=True)
        right_id = Column(Integer, ForeignKey('right.id'), primary_key=True)
        child = relationship("Child", backref="parent_assocs")
        
    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship(Association, backref="parent")
        
    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)
        
Working with the association pattern in its direct form requires that child
objects are associated with an association instance before being appended to
the parent; similarly, access from parent to child goes through the
association object:

.. sourcecode:: python+sql

    # create parent, append a child via association
    p = Parent()
    a = Association()
    a.child = Child()
    p.children.append(a)

    # iterate through child objects via association, including association
    # attributes
    for assoc in p.children:
        print assoc.data
        print assoc.child

To enhance the association object pattern such that direct
access to the ``Association`` object is optional, SQLAlchemy
provides the :ref:`associationproxy` extension. This
extension allows the configuration of attributes which will
access two "hops" with a single access, one "hop" to the
associated object, and a second to a target attribute.

.. note:: When using the association object pattern, it is
  advisable that the association-mapped table not be used
  as the ``secondary`` argument on a :func:`.relationship`
  elsewhere, unless that :func:`.relationship` contains
  the option ``viewonly=True``.   SQLAlchemy otherwise 
  may attempt to emit redundant INSERT and DELETE 
  statements on the same table, if similar state is detected
  on the related attribute as well as the associated
  object.

Adjacency List Relationships
-----------------------------

The **adjacency list** pattern is a common relational pattern whereby a table
contains a foreign key reference to itself. This is the most common and simple
way to represent hierarchical data in flat tables. The other way is the
"nested sets" model, sometimes called "modified preorder". Despite what many
online articles say about modified preorder, the adjacency list model is
probably the most appropriate pattern for the large majority of hierarchical
storage needs, for reasons of concurrency, reduced complexity, and that
modified preorder has little advantage over an application which can fully
load subtrees into the application space.

SQLAlchemy commonly refers to an adjacency list relationship as a
**self-referential mapper**. In this example, we'll work with a single table
called ``nodes`` to represent a tree structure::

    nodes = Table('nodes', metadata,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('nodes.id')),
        Column('data', String(50)),
        )

A graph such as the following::

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

SQLAlchemy's :func:`.mapper` configuration for a self-referential one-to-many
relationship is exactly like a "normal" one-to-many relationship. When
SQLAlchemy encounters the foreign key relationship from ``nodes`` to
``nodes``, it assumes one-to-many unless told otherwise:

.. sourcecode:: python+sql

    # entity class
    class Node(object):
        pass

    mapper(Node, nodes, properties={
        'children': relationship(Node)
    })

To create a many-to-one relationship from child to parent, an extra indicator
of the "remote side" is added, which contains the
:class:`~sqlalchemy.schema.Column` object or objects indicating the remote
side of the relationship:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'parent': relationship(Node, remote_side=[nodes.c.id])
    })

And the bi-directional version combines both:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'children': relationship(Node, 
                            backref=backref('parent', remote_side=[nodes.c.id])
                        )
    })

For comparison, the declarative version typically uses the inline ``id`` 
:class:`.Column` attribute to declare remote_side (note the list form is optional
when the collection is only one column)::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    
    class Node(Base):
        __tablename__ = 'nodes'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('nodes.id'))
        data = Column(String(50))
        children = relationship("Node", 
                        backref=backref('parent', remote_side=id)
                    )
        
There are several examples included with SQLAlchemy illustrating
self-referential strategies; these include :ref:`examples_adjacencylist` and
:ref:`examples_xmlpersistence`.

Self-Referential Query Strategies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Querying self-referential structures is done in the same way as any other
query in SQLAlchemy, such as below, we query for any node whose ``data``
attribute stores the value ``child2``:

.. sourcecode:: python+sql

    # get all nodes named 'child2'
    session.query(Node).filter(Node.data=='child2')

On the subject of joins, i.e. those described in `datamapping_joins`,
self-referential structures require the usage of aliases so that the same
table can be referenced multiple times within the FROM clause of the query.
Aliasing can be done either manually using the ``nodes``
:class:`~sqlalchemy.schema.Table` object as a source of aliases:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2'
    nodealias = nodes.alias()
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        filter(and_(Node.parent_id==nodealias.c.id, nodealias.c.data=='child2')).all()
    SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data
    FROM nodes, nodes AS nodes_1
    WHERE nodes.data = ? AND nodes.parent_id = nodes_1.id AND nodes_1.data = ?
    ['subchild1', 'child2']

or automatically, using ``join()`` with ``aliased=True``:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2'
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        join('parent', aliased=True).filter(Node.data=='child2').all()
    SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data
    FROM nodes JOIN nodes AS nodes_1 ON nodes_1.id = nodes.parent_id
    WHERE nodes.data = ? AND nodes_1.data = ?
    ['subchild1', 'child2']

To add criterion to multiple points along a longer join, use ``from_joinpoint=True``:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2' and a grandparent 'root'
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        join('parent', aliased=True).filter(Node.data=='child2').\
        join('parent', aliased=True, from_joinpoint=True).filter(Node.data=='root').all()
    SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data
    FROM nodes JOIN nodes AS nodes_1 ON nodes_1.id = nodes.parent_id JOIN nodes AS nodes_2 ON nodes_2.id = nodes_1.parent_id
    WHERE nodes.data = ? AND nodes_1.data = ? AND nodes_2.data = ?
    ['subchild1', 'child2', 'root']

Configuring Eager Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~

Eager loading of relationships occurs using joins or outerjoins from parent to
child table during a normal query operation, such that the parent and its
child collection can be populated from a single SQL statement, or a second
statement for all collections at once. SQLAlchemy's joined and subquery eager
loading uses aliased tables in all cases when joining to related items, so it
is compatible with self-referential joining. However, to use eager loading
with a self-referential relationship, SQLAlchemy needs to be told how many
levels deep it should join; otherwise the eager load will not take place. This
depth setting is configured via ``join_depth``:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'children': relationship(Node, lazy='joined', join_depth=2)
    })

    {sql}session.query(Node).all()
    SELECT nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, nodes_1.data AS nodes_1_data, nodes_2.id AS nodes_2_id, nodes_2.parent_id AS nodes_2_parent_id, nodes_2.data AS nodes_2_data, nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data
    FROM nodes LEFT OUTER JOIN nodes AS nodes_2 ON nodes.id = nodes_2.parent_id LEFT OUTER JOIN nodes AS nodes_1 ON nodes_2.id = nodes_1.parent_id
    []

Specifying Alternate Join Conditions to relationship()
------------------------------------------------------

The :func:`~sqlalchemy.orm.relationship` function uses the foreign key
relationship between the parent and child tables to formulate the **primary
join condition** between parent and child; in the case of a many-to-many
relationship it also formulates the **secondary join condition**::

      one to many/many to one:
      ------------------------

      parent_table -->  parent_table.c.id == child_table.c.parent_id -->  child_table
                                     primaryjoin

      many to many:
      -------------

      parent_table -->  parent_table.c.id == secondary_table.c.parent_id -->
                                     primaryjoin

                        secondary_table.c.child_id == child_table.c.id --> child_table
                                    secondaryjoin

If you are working with a :class:`~sqlalchemy.schema.Table` which has no
:class:`~sqlalchemy.schema.ForeignKey` objects on it (which can be the case
when using reflected tables with MySQL), or if the join condition cannot be
expressed by a simple foreign key relationship, use the ``primaryjoin`` and
possibly ``secondaryjoin`` conditions to create the appropriate relationship.

In this example we create a relationship ``boston_addresses`` which will only
load the user addresses with a city of "Boston":

.. sourcecode:: python+sql

    class User(object):
        pass
    class Address(object):
        pass

    mapper(Address, addresses_table)
    mapper(User, users_table, properties={
        'boston_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='Boston'))
    })

Many to many relationships can be customized by one or both of ``primaryjoin``
and ``secondaryjoin``, shown below with just the default many-to-many
relationship explicitly set:

.. sourcecode:: python+sql

    class User(object):
        pass
    class Keyword(object):
        pass
    mapper(Keyword, keywords_table)
    mapper(User, users_table, properties={
        'keywords': relationship(Keyword, secondary=userkeywords_table,
            primaryjoin=users_table.c.user_id==userkeywords_table.c.user_id,
            secondaryjoin=userkeywords_table.c.keyword_id==keywords_table.c.keyword_id
            )
    })

Specifying Foreign Keys
~~~~~~~~~~~~~~~~~~~~~~~~

When using ``primaryjoin`` and ``secondaryjoin``, SQLAlchemy also needs to be
aware of which columns in the relationship reference the other. In most cases,
a :class:`~sqlalchemy.schema.Table` construct will have
:class:`~sqlalchemy.schema.ForeignKey` constructs which take care of this;
however, in the case of reflected tables on a database that does not report
FKs (like MySQL ISAM) or when using join conditions on columns that don't have
foreign keys, the :func:`~sqlalchemy.orm.relationship` needs to be told
specifically which columns are "foreign" using the ``foreign_keys``
collection:

.. sourcecode:: python+sql

    mapper(Address, addresses_table)
    mapper(User, users_table, properties={
        'addresses': relationship(Address, primaryjoin=
                    users_table.c.user_id==addresses_table.c.user_id,
                    foreign_keys=[addresses_table.c.user_id])
    })

Building Query-Enabled Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Very ambitious custom join conditions may fail to be directly persistable, and
in some cases may not even load correctly. To remove the persistence part of
the equation, use the flag ``viewonly=True`` on the
:func:`~sqlalchemy.orm.relationship`, which establishes it as a read-only
attribute (data written to the collection will be ignored on flush()).
However, in extreme cases, consider using a regular Python property in
conjunction with :class:`~sqlalchemy.orm.query.Query` as follows:

.. sourcecode:: python+sql

    class User(object):
        def _get_addresses(self):
            return object_session(self).query(Address).with_parent(self).filter(...).all()
        addresses = property(_get_addresses)

Multiple Relationships against the Same Parent/Child
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Theres no restriction on how many times you can relate from parent to child.
SQLAlchemy can usually figure out what you want, particularly if the join
conditions are straightforward. Below we add a ``newyork_addresses`` attribute
to complement the ``boston_addresses`` attribute:

.. sourcecode:: python+sql

    mapper(User, users_table, properties={
        'boston_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='Boston')),
        'newyork_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='New York')),
    })


Rows that point to themselves / Mutually Dependent Rows
-------------------------------------------------------

This is a very specific case where relationship() must perform an INSERT and a
second UPDATE in order to properly populate a row (and vice versa an UPDATE
and DELETE in order to delete without violating foreign key constraints). The
two use cases are:

 * A table contains a foreign key to itself, and a single row will have a foreign key value pointing to its own primary key.
 * Two tables each contain a foreign key referencing the other table, with a row in each table referencing the other.

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

To enable the UPDATE after INSERT / UPDATE before DELETE behavior on
:func:`~sqlalchemy.orm.relationship`, use the ``post_update`` flag on *one* of
the relationships, preferably the many-to-one side::

    mapper(Widget, widget, properties={
        'entries':relationship(Entry, primaryjoin=widget.c.widget_id==entry.c.widget_id),
        'favorite_entry':relationship(Entry, primaryjoin=widget.c.favorite_entry_id==entry.c.entry_id, post_update=True)
    })

When a structure using the above mapping is flushed, the "widget" row will be
INSERTed minus the "favorite_entry_id" value, then all the "entry" rows will
be INSERTed referencing the parent "widget" row, and then an UPDATE statement
will populate the "favorite_entry_id" column of the "widget" table (it's one
row at a time for the time being).


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
on, the ``passive_updates`` flag can
be set to ``False``, most preferably on a one-to-many or
many-to-many :func:`.relationship`, which instructs
SQLAlchemy to issue UPDATE statements individually for
objects referenced in the collection, loading them into
memory if not already locally present. The
``passive_updates`` flag can also be ``False`` in
conjunction with ON UPDATE CASCADE functionality,
although in that case the unit of work will be issuing
extra SELECT and UPDATE statements unnecessarily.

A typical mutable primary key setup might look like:

.. sourcecode:: python+sql

    users = Table('users', metadata,
        Column('username', String(50), primary_key=True),
        Column('fullname', String(100)))

    addresses = Table('addresses', metadata,
        Column('email', String(50), primary_key=True),
        Column('username', String(50), ForeignKey('users.username', onupdate="cascade")))

    class User(object):
        pass
    class Address(object):
        pass

    # passive_updates=False *only* needed if the database
    # does not implement ON UPDATE CASCADE
    
    mapper(User, users, properties={
        'addresses': relationship(Address, passive_updates=False)
    })
    mapper(Address, addresses)

``passive_updates`` is set to ``True`` by default,
indicating that ON UPDATE CASCADE is expected to be in
place in the usual case for foreign keys that expect
to have a mutating parent key.

``passive_updates=False`` may be configured on any
direction of relationship, i.e. one-to-many, many-to-one,
and many-to-many, although it is much more effective when
placed just on the one-to-many or many-to-many side.
Configuring the ``passive_updates=False`` only on the
many-to-one side will have only a partial effect, as the
unit of work searches only through the current identity
map for objects that may be referencing the one with a
mutating primary key, not throughout the database.

The :func:`relationship` API
----------------------------

.. autofunction:: relationship

.. autofunction:: backref

.. autofunction:: relation



=============================
What's new in SQLAlchemy 0.4?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.3,
    last released October 14, 2007, and SQLAlchemy version 0.4,
    last released October 12, 2008.

    Document date:  March 21, 2008

First Things First
==================

If you're using any ORM features, make sure you import from
``sqlalchemy.orm``:

::

    from sqlalchemy import *
    from sqlalchemy.orm import *

Secondly, anywhere you used to say ``engine=``,
``connectable=``, ``bind_to=``, ``something.engine``,
``metadata.connect()``, use ``bind``:

::

    myengine = create_engine('sqlite://')

    meta = MetaData(myengine)

    meta2 = MetaData()
    meta2.bind = myengine

    session = create_session(bind=myengine)

    statement = select([table], bind=myengine)

Got those ?  Good!  You're now (95%) 0.4 compatible.  If
you're using 0.3.10, you can make these changes immediately;
they'll work there too.

Module Imports
==============

In 0.3, "``from sqlalchemy import *``" would import all of
sqlalchemy's sub-modules into your namespace. Version 0.4 no
longer imports sub-modules into the namespace. This may mean
you need to add extra imports into your code.

In 0.3, this code worked:

::

    from sqlalchemy import *

    class UTCDateTime(types.TypeDecorator):
        pass

In 0.4, one must do:

::

    from sqlalchemy import *
    from sqlalchemy import types

    class UTCDateTime(types.TypeDecorator):
        pass

Object Relational Mapping
=========================

Querying
--------

New Query API
^^^^^^^^^^^^^

Query is standardized on the generative interface (old
interface is still there, just deprecated).   While most of
the generative interface is available in 0.3, the 0.4 Query
has the inner guts to match the generative outside, and has
a lot more tricks.  All result narrowing is via ``filter()``
and ``filter_by()``, limiting/offset is either through array
slices or ``limit()``/``offset()``, joining is via
``join()`` and ``outerjoin()`` (or more manually, through
``select_from()`` as well as manually-formed criteria).

To avoid deprecation warnings, you must make some changes to
your 03 code

User.query.get_by( \**kwargs )

::

    User.query.filter_by(**kwargs).first()

User.query.select_by( \**kwargs )

::

    User.query.filter_by(**kwargs).all()

User.query.select()

::

    User.query.filter(xxx).all()

New Property-Based Expression Constructs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By far the most palpable difference within the ORM is that
you can now construct your query criterion using class-based
attributes directly.  The ".c." prefix is no longer needed
when working with mapped classes:

::

    session.query(User).filter(and_(User.name == 'fred', User.id > 17))

While simple column-based comparisons are no big deal, the
class attributes have some new "higher level" constructs
available, including what was previously only available in
``filter_by()``:

::

    # comparison of scalar relations to an instance
    filter(Address.user == user)

    # return all users who contain a particular address
    filter(User.addresses.contains(address))

    # return all users who *dont* contain the address
    filter(~User.address.contains(address))

    # return all users who contain a particular address with
    # the email_address like '%foo%'
    filter(User.addresses.any(Address.email_address.like('%foo%')))

    # same, email address equals 'foo@bar.com'.  can fall back to keyword
    # args for simple comparisons
    filter(User.addresses.any(email_address = 'foo@bar.com'))

    # return all Addresses whose user attribute has the username 'ed'
    filter(Address.user.has(name='ed'))

    # return all Addresses whose user attribute has the username 'ed'
    # and an id > 5 (mixing clauses with kwargs)
    filter(Address.user.has(User.id > 5, name='ed'))

The ``Column`` collection remains available on mapped
classes in the ``.c`` attribute.  Note that property-based
expressions are only available with mapped properties of
mapped classes.  ``.c`` is still used to access columns in
regular tables and selectable objects produced from SQL
Expressions.

Automatic Join Aliasing
^^^^^^^^^^^^^^^^^^^^^^^

We've had join() and outerjoin() for a while now:

::

    session.query(Order).join('items')...

Now you can alias them:

::

    session.query(Order).join('items', aliased=True).
       filter(Item.name='item 1').join('items', aliased=True).filter(Item.name=='item 3')

The above will create two joins from orders->items using
aliases.  the ``filter()`` call subsequent to each will
adjust its table criterion to that of the alias.  To get at
the ``Item`` objects, use ``add_entity()`` and target each
join with an ``id``:

::

    session.query(Order).join('items', id='j1', aliased=True).
    filter(Item.name == 'item 1').join('items', aliased=True, id='j2').
    filter(Item.name == 'item 3').add_entity(Item, id='j1').add_entity(Item, id='j2')

Returns tuples in the form: ``(Order, Item, Item)``.

Self-referential Queries
^^^^^^^^^^^^^^^^^^^^^^^^

So query.join() can make aliases now.  What does that give
us ?  Self-referential queries !   Joins can be done without
any ``Alias`` objects:

::

    # standard self-referential TreeNode mapper with backref
    mapper(TreeNode, tree_nodes, properties={
        'children':relation(TreeNode, backref=backref('parent', remote_side=tree_nodes.id))
    })

    # query for node with child containing "bar" two levels deep
    session.query(TreeNode).join(["children", "children"], aliased=True).filter_by(name='bar')

To add criterion for each table along the way in an aliased
join, you can use ``from_joinpoint`` to keep joining against
the same line of aliases:

::

    # search for the treenode along the path "n1/n12/n122"

    # first find a Node with name="n122"
    q = sess.query(Node).filter_by(name='n122')

    # then join to parent with "n12"
    q = q.join('parent', aliased=True).filter_by(name='n12')

    # join again to the next parent with 'n1'.  use 'from_joinpoint'
    # so we join from the previous point, instead of joining off the
    # root table
    q = q.join('parent', aliased=True, from_joinpoint=True).filter_by(name='n1')

    node = q.first()

``query.populate_existing()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The eager version of ``query.load()`` (or
``session.refresh()``).  Every instance loaded from the
query, including all eagerly loaded items, get refreshed
immediately if already present in the session:

::

    session.query(Blah).populate_existing().all()

Relations
---------

SQL Clauses Embedded in Updates/Inserts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For inline execution of SQL clauses, embedded right in the
UPDATE or INSERT, during a ``flush()``:

::


    myobject.foo = mytable.c.value + 1

    user.pwhash = func.md5(password)

    order.hash = text("select hash from hashing_table")

The column-attribute is set up with a deferred loader after
the operation, so that it issues the SQL to load the new
value when you next access.

Self-referential and Cyclical Eager Loading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since our alias-fu has improved, ``relation()`` can join
along the same table \*any number of times*; you tell it how
deep you want to go.  Lets show the self-referential
``TreeNode`` more clearly:

::

    nodes = Table('nodes', metadata,
         Column('id', Integer, primary_key=True),
         Column('parent_id', Integer, ForeignKey('nodes.id')),
         Column('name', String(30)))

    class TreeNode(object):
        pass

    mapper(TreeNode, nodes, properties={
        'children':relation(TreeNode, lazy=False, join_depth=3)
    })

So what happens when we say:

::

    create_session().query(TreeNode).all()

?  A join along aliases, three levels deep off the parent:

::

    SELECT
    nodes_3.id AS nodes_3_id, nodes_3.parent_id AS nodes_3_parent_id, nodes_3.name AS nodes_3_name,
    nodes_2.id AS nodes_2_id, nodes_2.parent_id AS nodes_2_parent_id, nodes_2.name AS nodes_2_name,
    nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, nodes_1.name AS nodes_1_name,
    nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.name AS nodes_name
    FROM nodes LEFT OUTER JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id
    LEFT OUTER JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id
    LEFT OUTER JOIN nodes AS nodes_3 ON nodes_2.id = nodes_3.parent_id
    ORDER BY nodes.oid, nodes_1.oid, nodes_2.oid, nodes_3.oid

Notice the nice clean alias names too.  The joining doesn't
care if it's against the same immediate table or some other
object which then cycles back to the beginning.  Any kind
of chain of eager loads can cycle back onto itself when
``join_depth`` is specified.  When not present, eager
loading automatically stops when it hits a cycle.

Composite Types
^^^^^^^^^^^^^^^

This is one from the Hibernate camp.  Composite Types let
you define a custom datatype that is composed of more than
one column (or one column, if you wanted).   Lets define a
new type, ``Point``.  Stores an x/y coordinate:

::

    class Point(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y
        def __composite_values__(self):
            return self.x, self.y
        def __eq__(self, other):
            return other.x == self.x and other.y == self.y
        def __ne__(self, other):
            return not self.__eq__(other)

The way the ``Point`` object is defined is specific to a
custom type; constructor takes a list of arguments, and the
``__composite_values__()`` method produces a sequence of
those arguments.  The order will match up to our mapper, as
we'll see in a moment.

Let's create a table of vertices storing two points per row:

::

    vertices = Table('vertices', metadata,
        Column('id', Integer, primary_key=True),
        Column('x1', Integer),
        Column('y1', Integer),
        Column('x2', Integer),
        Column('y2', Integer),
        )

Then, map it !  We'll create a ``Vertex`` object which
stores two ``Point`` objects:

::

    class Vertex(object):
        def __init__(self, start, end):
            self.start = start
            self.end = end

    mapper(Vertex, vertices, properties={
        'start':composite(Point, vertices.c.x1, vertices.c.y1),
        'end':composite(Point, vertices.c.x2, vertices.c.y2)
    })

Once you've set up your composite type, it's usable just
like any other type:

::


    v = Vertex(Point(3, 4), Point(26,15))
    session.save(v)
    session.flush()

    # works in queries too
    q = session.query(Vertex).filter(Vertex.start == Point(3, 4))

If you'd like to define the way the mapped attributes
generate SQL clauses when used in expressions, create your
own ``sqlalchemy.orm.PropComparator`` subclass, defining any
of the common operators (like ``__eq__()``, ``__le__()``,
etc.), and send it in to ``composite()``.  Composite types
work as primary keys too, and are usable in ``query.get()``:

::

    # a Document class which uses a composite Version
    # object as primary key
    document = query.get(Version(1, 'a'))

``dynamic_loader()`` relations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A ``relation()`` that returns a live ``Query`` object for
all read operations.  Write operations are limited to just
``append()`` and ``remove()``, changes to the collection are
not visible until the session is flushed.  This feature is
particularly handy with an "autoflushing" session which will
flush before each query.

::

    mapper(Foo, foo_table, properties={
        'bars':dynamic_loader(Bar, backref='foo', <other relation() opts>)
    })

    session = create_session(autoflush=True)
    foo = session.query(Foo).first()

    foo.bars.append(Bar(name='lala'))

    for bar in foo.bars.filter(Bar.name=='lala'):
        print(bar)

    session.commit()

New Options: ``undefer_group()``, ``eagerload_all()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A couple of query options which are handy.
``undefer_group()`` marks a whole group of "deferred"
columns as undeferred:

::

    mapper(Class, table, properties={
        'foo' : deferred(table.c.foo, group='group1'),
        'bar' : deferred(table.c.bar, group='group1'),
        'bat' : deferred(table.c.bat, group='group1'),
    )

    session.query(Class).options(undefer_group('group1')).filter(...).all()

and ``eagerload_all()`` sets a chain of attributes to be
eager in one pass:

::

    mapper(Foo, foo_table, properties={
       'bar':relation(Bar)
    })
    mapper(Bar, bar_table, properties={
       'bat':relation(Bat)
    })
    mapper(Bat, bat_table)

    # eager load bar and bat
    session.query(Foo).options(eagerload_all('bar.bat')).filter(...).all()

New Collection API
^^^^^^^^^^^^^^^^^^

Collections are no longer proxied by an
{{{InstrumentedList}}} proxy, and access to members, methods
and attributes is direct.   Decorators now intercept objects
entering and leaving the collection, and it is now possible
to easily write a custom collection class that manages its
own membership.  Flexible decorators also replace the named
method interface of custom collections in 0.3, allowing any
class to be easily adapted to use as a collection container.

Dictionary-based collections are now much easier to use and
fully ``dict``-like.  Changing ``__iter__`` is no longer
needed for ``dict``s, and new built-in ``dict`` types cover
many needs:

::

    # use a dictionary relation keyed by a column
    relation(Item, collection_class=column_mapped_collection(items.c.keyword))
    # or named attribute
    relation(Item, collection_class=attribute_mapped_collection('keyword'))
    # or any function you like
    relation(Item, collection_class=mapped_collection(lambda entity: entity.a + entity.b))

Existing 0.3 ``dict``-like and freeform object derived
collection classes will need to be updated for the new API.
In most cases this is simply a matter of adding a couple
decorators to the class definition.

Mapped Relations from External Tables/Subqueries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This feature quietly appeared in 0.3 but has been improved
in 0.4 thanks to better ability to convert subqueries
against a table into subqueries against an alias of that
table; this is key for eager loading, aliased joins in
queries, etc.  It reduces the need to create mappers against
select statements when you just need to add some extra
columns or subqueries:

::

    mapper(User, users, properties={
           'fullname': column_property((users.c.firstname + users.c.lastname).label('fullname')),
           'numposts': column_property(
                select([func.count(1)], users.c.id==posts.c.user_id).correlate(users).label('posts')
           )
        })

a typical query looks like:

::

    SELECT (SELECT count(1) FROM posts WHERE users.id = posts.user_id) AS count,
    users.firstname || users.lastname AS fullname,
    users.id AS users_id, users.firstname AS users_firstname, users.lastname AS users_lastname
    FROM users ORDER BY users.oid

Horizontal Scaling (Sharding) API
---------------------------------

[browser:/sqlalchemy/trunk/examples/sharding/attribute_shard
.py]

Sessions
--------

New Session Create Paradigm; SessionContext, assignmapper Deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

That's right, the whole shebang is being replaced with two
configurational functions.  Using both will produce the most
0.1-ish feel we've had since 0.1 (i.e., the least amount of
typing).

Configure your own ``Session`` class right where you define
your ``engine`` (or anywhere):

::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('myengine://')
    Session = sessionmaker(bind=engine, autoflush=True, transactional=True)

    # use the new Session() freely
    sess = Session()
    sess.save(someobject)
    sess.flush()


If you need to post-configure your Session, say with an
engine, add it later with ``configure()``:

::

    Session.configure(bind=create_engine(...))

All the behaviors of ``SessionContext`` and the ``query``
and ``__init__`` methods of ``assignmapper`` are moved into
the new ``scoped_session()`` function, which is compatible
with both ``sessionmaker`` as well as ``create_session()``:

::

    from sqlalchemy.orm import scoped_session, sessionmaker

    Session = scoped_session(sessionmaker(autoflush=True, transactional=True))
    Session.configure(bind=engine)

    u = User(name='wendy')

    sess = Session()
    sess.save(u)
    sess.commit()

    # Session constructor is thread-locally scoped.  Everyone gets the same
    # Session in the thread when scope="thread".
    sess2 = Session()
    assert sess is sess2


When using a thread-local ``Session``, the returned class
has all of ``Session's`` interface implemented as
classmethods, and "assignmapper"'s functionality is
available using the ``mapper`` classmethod.  Just like the
old ``objectstore`` days....

::


    # "assignmapper"-like functionality available via ScopedSession.mapper
    Session.mapper(User, users_table)

    u = User(name='wendy')

    Session.commit()


Sessions are again Weak Referencing By Default
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The weak_identity_map flag is now set to ``True`` by default
on Session.  Instances which are externally deferenced and
fall out of scope are removed from the session
automatically.   However, items which have "dirty" changes
present will remain strongly referenced until those changes
are flushed at which case the object reverts to being weakly
referenced (this works for 'mutable' types, like picklable
attributes, as well).  Setting weak_identity_map to
``False`` restores the old strong-referencing behavior for
those of you using the session like a cache.

Auto-Transactional Sessions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

As you might have noticed above, we are calling ``commit()``
on ``Session``.  The flag ``transactional=True`` means the
``Session`` is always in a transaction, ``commit()``
persists permanently.

Auto-Flushing Sessions
^^^^^^^^^^^^^^^^^^^^^^

Also, ``autoflush=True`` means the ``Session`` will
``flush()`` before each ``query`` as well as when you call
``flush()`` or ``commit()``.  So now this will work:

::

    Session = sessionmaker(bind=engine, autoflush=True, transactional=True)

    u = User(name='wendy')

    sess = Session()
    sess.save(u)

    # wendy is flushed, comes right back from a query
    wendy = sess.query(User).filter_by(name='wendy').one()

Transactional methods moved onto sessions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``commit()`` and ``rollback()``, as well as ``begin()`` are
now directly on ``Session``.  No more need to use
``SessionTransaction`` for anything (it remains in the
background).

::

    Session = sessionmaker(autoflush=True, transactional=False)

    sess = Session()
    sess.begin()

    # use the session

    sess.commit() # commit transaction

Sharing a ``Session`` with an enclosing engine-level (i.e.
non-ORM) transaction is easy:

::

    Session = sessionmaker(autoflush=True, transactional=False)

    conn = engine.connect()
    trans = conn.begin()
    sess = Session(bind=conn)

    # ... session is transactional

    # commit the outermost transaction
    trans.commit()

Nested Session Transactions with SAVEPOINT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Available at the Engine and ORM level.  ORM docs so far:

http://www.sqlalchemy.org/docs/04/session.html#unitofwork_managing

Two-Phase Commit Sessions
^^^^^^^^^^^^^^^^^^^^^^^^^

Available at the Engine and ORM level.  ORM docs so far:

http://www.sqlalchemy.org/docs/04/session.html#unitofwork_managing

Inheritance
-----------

Polymorphic Inheritance with No Joins or Unions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

New docs for inheritance:  http://www.sqlalchemy.org/docs/04
/mappers.html#advdatamapping_mapper_inheritance_joined

Better Polymorphic Behavior with ``get()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All classes within a joined-table inheritance hierarchy get
an ``_instance_key`` using the base class, i.e.
``(BaseClass, (1, ), None)``.  That way when you call
``get()`` a ``Query`` against the base class, it can locate
subclass instances in the current identity map without
querying the database.

Types
-----

Custom Subclasses of ``sqlalchemy.types.TypeDecorator``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a `New API <http://www.sqlalchemy.org/docs/04/types
.html#types_custom>`_ for subclassing a TypeDecorator.
Using the 0.3 API causes compilation errors in some cases.

SQL Expressions
===============

All New, Deterministic Label/Alias Generation
---------------------------------------------

All the "anonymous" labels and aliases use a simple
<name>_<number> format now.  SQL is much easier to read and
is compatible with plan optimizer caches.  Just check out
some of the examples in the tutorials:
http://www.sqlalchemy.org/docs/04/ormtutorial.html
http://www.sqlalchemy.org/docs/04/sqlexpression.html

Generative select() Constructs
------------------------------

This is definitely the way to go with ``select()``.  See htt
p://www.sqlalchemy.org/docs/04/sqlexpression.html#sql_transf
orm .

New Operator System
-------------------

SQL operators and more or less every SQL keyword there is
are now abstracted into the compiler layer.  They now act
intelligently and are type/backend aware, see:
http://www.sqlalchemy.org/docs/04/sqlexpression.html#sql_operators

All ``type`` Keyword Arguments Renamed to ``type_``
---------------------------------------------------

Just like it says:

::

       b = bindparam('foo', type_=String)

in\_ Function Changed to Accept Sequence or Selectable
------------------------------------------------------

The in\_ function now takes a sequence of values or a
selectable as its sole argument. The previous API of passing
in values as positional arguments still works, but is now
deprecated. This means that

::

    my_table.select(my_table.c.id.in_(1,2,3)
    my_table.select(my_table.c.id.in_(*listOfIds)

should be changed to

::

    my_table.select(my_table.c.id.in_([1,2,3])
    my_table.select(my_table.c.id.in_(listOfIds)

Schema and Reflection
=====================

``MetaData``, ``BoundMetaData``, ``DynamicMetaData``...
-------------------------------------------------------

In the 0.3.x series, ``BoundMetaData`` and
``DynamicMetaData`` were deprecated in favor of ``MetaData``
and ``ThreadLocalMetaData``.  The older names have been
removed in 0.4.  Updating is simple:

::

    +-------------------------------------+-------------------------+
    |If You Had                           | Now Use                 |
    +=====================================+=========================+
    | ``MetaData``                        | ``MetaData``            |
    +-------------------------------------+-------------------------+
    | ``BoundMetaData``                   | ``MetaData``            |
    +-------------------------------------+-------------------------+
    | ``DynamicMetaData`` (with one       | ``MetaData``            |
    | engine or threadlocal=False)        |                         |
    +-------------------------------------+-------------------------+
    | ``DynamicMetaData``                 | ``ThreadLocalMetaData`` |
    | (with different engines per thread) |                         |
    +-------------------------------------+-------------------------+

The seldom-used ``name`` parameter to ``MetaData`` types has
been removed.  The ``ThreadLocalMetaData`` constructor now
takes no arguments.  Both types can now be bound to an
``Engine`` or a single ``Connection``.

One Step Multi-Table Reflection
-------------------------------

You can now load table definitions and automatically create
``Table`` objects from an entire database or schema in one
pass:

::

    >>> metadata = MetaData(myengine, reflect=True)
    >>> metadata.tables.keys()
    ['table_a', 'table_b', 'table_c', '...']

``MetaData`` also gains a ``.reflect()`` method enabling
finer control over the loading process, including
specification of a subset of available tables to load.

SQL Execution
=============

``engine``, ``connectable``, and ``bind_to`` are all now ``bind``
-----------------------------------------------------------------

``Transactions``, ``NestedTransactions`` and ``TwoPhaseTransactions``
---------------------------------------------------------------------

Connection Pool Events
----------------------

The connection pool now fires events when new DB-API
connections are created, checked out and checked back into
the pool.   You can use these to execute session-scoped SQL
setup statements on fresh connections, for example.

Oracle Engine Fixed
-------------------

In 0.3.11, there were bugs in the Oracle Engine on how
Primary Keys are handled.  These bugs could cause programs
that worked fine with other engines, such as sqlite, to fail
when using the Oracle Engine.  In 0.4, the Oracle Engine has
been reworked, fixing these Primary Key problems.

Out Parameters for Oracle
-------------------------

::

    result = engine.execute(text("begin foo(:x, :y, :z); end;", bindparams=[bindparam('x', Numeric), outparam('y', Numeric), outparam('z', Numeric)]), x=5)
    assert result.out_parameters == {'y':10, 'z':75}

Connection-bound ``MetaData``, ``Sessions``
-------------------------------------------

``MetaData`` and ``Session`` can be explicitly bound to a
connection:

::

    conn = engine.connect()
    sess = create_session(bind=conn)

Faster, More Foolproof ``ResultProxy`` Objects
----------------------------------------------


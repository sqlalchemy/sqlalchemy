.. _collections_toplevel:

.. currentmodule:: sqlalchemy.orm

Collection Configuration and Techniques
=======================================

The :func:`.relationship` function defines a linkage between two classes.   
When the linkage defines a one-to-many or many-to-many relationship, it's
represented as a Python collection when objects are loaded and manipulated.
This section presents additional information about collection configuration
and techniques.

.. _largecollections:
.. currentmodule:: sqlalchemy.orm

Working with Large Collections
-------------------------------

The default behavior of :func:`.relationship` is to fully load
the collection of items in, as according to the loading strategy of the
relationship. Additionally, the Session by default only knows how to delete
objects which are actually present within the session. When a parent instance
is marked for deletion and flushed, the Session loads its full list of child
items in so that they may either be deleted as well, or have their foreign key
value set to null; this is to avoid constraint violations. For large
collections of child items, there are several strategies to bypass full
loading of child items both at load time as well as deletion time.

Dynamic Relationship Loaders
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most useful by far is the :func:`~sqlalchemy.orm.dynamic_loader`
relationship. This is a variant of :func:`~sqlalchemy.orm.relationship` which
returns a :class:`~sqlalchemy.orm.query.Query` object in place of a collection
when accessed. :func:`~sqlalchemy.orm.query.Query.filter` criterion may be
applied as well as limits and offsets, either explicitly or via array slices:

.. sourcecode:: python+sql

    mapper(User, users_table, properties={
        'posts': dynamic_loader(Post)
    })

    jack = session.query(User).get(id)

    # filter Jack's blog posts
    posts = jack.posts.filter(Post.headline=='this is a post')

    # apply array slices
    posts = jack.posts[5:20]

The dynamic relationship supports limited write operations, via the
``append()`` and ``remove()`` methods::

    oldpost = jack.posts.filter(Post.headline=='old post').one()
    jack.posts.remove(oldpost)

    jack.posts.append(Post('new post'))

Since the read side of the dynamic relationship always queries the 
database, changes to the underlying collection will not be visible 
until the data has been flushed.  However, as long as "autoflush" is 
enabled on the :class:`.Session` in use, this will occur 
automatically each time the collection is about to emit a 
query.

To place a dynamic relationship on a backref, use ``lazy='dynamic'``:

.. sourcecode:: python+sql

    mapper(Post, posts_table, properties={
        'user': relationship(User, backref=backref('posts', lazy='dynamic'))
    })

Note that eager/lazy loading options cannot be used in conjunction dynamic relationships at this time.

.. autofunction:: dynamic_loader

Setting Noload
~~~~~~~~~~~~~~~

The opposite of the dynamic relationship is simply "noload", specified using ``lazy='noload'``:

.. sourcecode:: python+sql

    mapper(MyClass, table, properties={
        'children': relationship(MyOtherClass, lazy='noload')
    })

Above, the ``children`` collection is fully writeable, and changes to it will
be persisted to the database as well as locally available for reading at the
time they are added. However when instances of ``MyClass`` are freshly loaded
from the database, the ``children`` collection stays empty.

Using Passive Deletes
~~~~~~~~~~~~~~~~~~~~~~

Use ``passive_deletes=True`` to disable child object loading on a DELETE
operation, in conjunction with "ON DELETE (CASCADE|SET NULL)" on your database
to automatically cascade deletes to child objects. Note that "ON DELETE" is
not supported on SQLite, and requires ``InnoDB`` tables when using MySQL:

.. sourcecode:: python+sql

        mytable = Table('mytable', meta,
            Column('id', Integer, primary_key=True),
            )

        myothertable = Table('myothertable', meta,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer),
            ForeignKeyConstraint(['parent_id'], ['mytable.id'], ondelete="CASCADE"),
            )

        mapper(MyOtherClass, myothertable)

        mapper(MyClass, mytable, properties={
            'children': relationship(MyOtherClass, cascade="all, delete-orphan", passive_deletes=True)
        })

When ``passive_deletes`` is applied, the ``children`` relationship will not be
loaded into memory when an instance of ``MyClass`` is marked for deletion. The
``cascade="all, delete-orphan"`` *will* take effect for instances of
``MyOtherClass`` which are currently present in the session; however for
instances of ``MyOtherClass`` which are not loaded, SQLAlchemy assumes that
"ON DELETE CASCADE" rules will ensure that those rows are deleted by the
database and that no foreign key violation will occur.

.. currentmodule:: sqlalchemy.orm.collections
.. _custom_collections:

Customizing Collection Access
-----------------------------

Mapping a one-to-many or many-to-many relationship results in a collection of
values accessible through an attribute on the parent instance. By default,
this collection is a ``list``::

    mapper(Parent, properties={
        'children' : relationship(Child)
    })

    parent = Parent()
    parent.children.append(Child())
    print parent.children[0]

Collections are not limited to lists. Sets, mutable sequences and almost any
other Python object that can act as a container can be used in place of the
default list, by specifying the ``collection_class`` option on
:func:`~sqlalchemy.orm.relationship`.

.. sourcecode:: python+sql

    # use a set
    mapper(Parent, properties={
        'children' : relationship(Child, collection_class=set)
    })

    parent = Parent()
    child = Child()
    parent.children.add(child)
    assert child in parent.children


Custom Collection Implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use your own types for collections as well. For most cases, simply
inherit from ``list`` or ``set`` and add the custom behavior.

Collections in SQLAlchemy are transparently *instrumented*. Instrumentation
means that normal operations on the collection are tracked and result in
changes being written to the database at flush time. Additionally, collection
operations can fire *events* which indicate some secondary operation must take
place. Examples of a secondary operation include saving the child item in the
parent's :class:`~sqlalchemy.orm.session.Session` (i.e. the ``save-update``
cascade), as well as synchronizing the state of a bi-directional relationship
(i.e. a ``backref``).

The collections package understands the basic interface of lists, sets and
dicts and will automatically apply instrumentation to those built-in types and
their subclasses. Object-derived types that implement a basic collection
interface are detected and instrumented via duck-typing:

.. sourcecode:: python+sql

    class ListLike(object):
        def __init__(self):
            self.data = []
        def append(self, item):
            self.data.append(item)
        def remove(self, item):
            self.data.remove(item)
        def extend(self, items):
            self.data.extend(items)
        def __iter__(self):
            return iter(self.data)
        def foo(self):
            return 'foo'

``append``, ``remove``, and ``extend`` are known list-like methods, and will be instrumented automatically.  ``__iter__`` is not a mutator method and won't be instrumented, and ``foo`` won't be either.

Duck-typing (i.e. guesswork) isn't rock-solid, of course, so you can be
explicit about the interface you are implementing by providing an
``__emulates__`` class attribute::

    class SetLike(object):
        __emulates__ = set

        def __init__(self):
            self.data = set()
        def append(self, item):
            self.data.add(item)
        def remove(self, item):
            self.data.remove(item)
        def __iter__(self):
            return iter(self.data)

This class looks list-like because of ``append``, but ``__emulates__`` forces
it to set-like. ``remove`` is known to be part of the set interface and will
be instrumented.

But this class won't work quite yet: a little glue is needed to adapt it for
use by SQLAlchemy. The ORM needs to know which methods to use to append,
remove and iterate over members of the collection. When using a type like
``list`` or ``set``, the appropriate methods are well-known and used
automatically when present. This set-like class does not provide the expected
``add`` method, so we must supply an explicit mapping for the ORM via a
decorator.

Annotating Custom Collections via Decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Decorators can be used to tag the individual methods the ORM needs to manage
collections. Use them when your class doesn't quite meet the regular interface
for its container type, or you simply would like to use a different method to
get the job done.

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import collection

    class SetLike(object):
        __emulates__ = set

        def __init__(self):
            self.data = set()

        @collection.appender
        def append(self, item):
            self.data.add(item)

        def remove(self, item):
            self.data.remove(item)

        def __iter__(self):
            return iter(self.data)

And that's all that's needed to complete the example. SQLAlchemy will add
instances via the ``append`` method. ``remove`` and ``__iter__`` are the
default methods for sets and will be used for removing and iteration. Default
methods can be changed as well:

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import collection

    class MyList(list):
        @collection.remover
        def zark(self, item):
            # do something special...

        @collection.iterator
        def hey_use_this_instead_for_iteration(self):
            # ...

There is no requirement to be list-, or set-like at all. Collection classes
can be any shape, so long as they have the append, remove and iterate
interface marked for SQLAlchemy's use. Append and remove methods will be
called with a mapped entity as the single argument, and iterator methods are
called with no arguments and must return an iterator.

Dictionary-Based Collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``dict`` can be used as a collection, but a keying strategy is needed to map
entities loaded by the ORM to key, value pairs. The
:mod:`sqlalchemy.orm.collections` package provides several built-in types for
dictionary-based collections:

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import column_mapped_collection, attribute_mapped_collection, mapped_collection

    mapper(Item, items_table, properties={
        # key by column
        'notes': relationship(Note, collection_class=column_mapped_collection(notes_table.c.keyword)),
        # or named attribute
        'notes2': relationship(Note, collection_class=attribute_mapped_collection('keyword')),
        # or any callable
        'notes3': relationship(Note, collection_class=mapped_collection(lambda entity: entity.a + entity.b))
    })

    # ...
    item = Item()
    item.notes['color'] = Note('color', 'blue')
    print item.notes['color']

These functions each provide a ``dict`` subclass with decorated ``set`` and
``remove`` methods and the keying strategy of your choice.

The :class:`sqlalchemy.orm.collections.MappedCollection` class can be used as
a base class for your custom types or as a mix-in to quickly add ``dict``
collection support to other classes. It uses a keying function to delegate to
``__setitem__`` and ``__delitem__``:

.. sourcecode:: python+sql

    from sqlalchemy.util import OrderedDict
    from sqlalchemy.orm.collections import MappedCollection

    class NodeMap(OrderedDict, MappedCollection):
        """Holds 'Node' objects, keyed by the 'name' attribute with insert order maintained."""

        def __init__(self, *args, **kw):
            MappedCollection.__init__(self, keyfunc=lambda node: node.name)
            OrderedDict.__init__(self, *args, **kw)

When subclassing :class:`.MappedCollection`, user-defined versions 
of ``__setitem__()`` or ``__delitem__()`` should be decorated
with :meth:`.collection.internally_instrumented`, **if** they call down
to those same methods on :class:`.MappedCollection`.  This because the methods
on :class:`.MappedCollection` are already instrumented - calling them
from within an already instrumented call can cause events to be fired off
repeatedly, or inappropriately, leading to internal state corruption in
rare cases::
    
    from sqlalchemy.orm.collections import MappedCollection,\
                                        collection

    class MyMappedCollection(MappedCollection):
        """Use @internally_instrumented when your methods 
        call down to already-instrumented methods.
        
        """
        
        @collection.internally_instrumented
        def __setitem__(self, key, value, _sa_initiator=None):
            # do something with key, value
            super(MyMappedCollection, self).__setitem__(key, value, _sa_initiator)
        
        @collection.internally_instrumented
        def __delitem__(self, key, _sa_initiator=None):
            # do something with key
            super(MyMappedCollection, self).__delitem__(key, _sa_initiator)

The ORM understands the ``dict`` interface just like lists and sets, and will
automatically instrument all dict-like methods if you choose to subclass
``dict`` or provide dict-like collection behavior in a duck-typed class. You
must decorate appender and remover methods, however- there are no compatible
methods in the basic dictionary interface for SQLAlchemy to use by default.
Iteration will go through ``itervalues()`` unless otherwise decorated.

Instrumentation and Custom Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many custom types and existing library classes can be used as a entity
collection type as-is without further ado. However, it is important to note
that the instrumentation process _will_ modify the type, adding decorators
around methods automatically.

The decorations are lightweight and no-op outside of relationships, but they
do add unneeded overhead when triggered elsewhere. When using a library class
as a collection, it can be good practice to use the "trivial subclass" trick
to restrict the decorations to just your usage in relationships. For example:

.. sourcecode:: python+sql

    class MyAwesomeList(some.great.library.AwesomeList):
        pass

    # ... relationship(..., collection_class=MyAwesomeList)

The ORM uses this approach for built-ins, quietly substituting a trivial
subclass when a ``list``, ``set`` or ``dict`` is used directly.

The collections package provides additional decorators and support for
authoring custom types. See the :mod:`sqlalchemy.orm.collections` package for
more information and discussion of advanced usage and Python 2.3-compatible
decoration options.

Collections API
~~~~~~~~~~~~~~~

.. autofunction:: attribute_mapped_collection

.. autoclass:: collection
    :members:

.. autofunction:: collection_adapter

.. autofunction:: column_mapped_collection

.. autofunction:: mapped_collection

.. autoclass:: sqlalchemy.orm.collections.MappedCollection
   :members:



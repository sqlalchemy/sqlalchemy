.. _collections_toplevel:

.. currentmodule:: sqlalchemy.orm

=======================================
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
===============================

The default behavior of :func:`.relationship` is to fully load
the collection of items in, as according to the loading strategy of the
relationship. Additionally, the :class:`.Session` by default only knows how to delete
objects which are actually present within the session. When a parent instance
is marked for deletion and flushed, the :class:`.Session` loads its full list of child
items in so that they may either be deleted as well, or have their foreign key
value set to null; this is to avoid constraint violations. For large
collections of child items, there are several strategies to bypass full
loading of child items both at load time as well as deletion time.

.. _dynamic_relationship:

Dynamic Relationship Loaders
-----------------------------

A key feature to enable management of a large collection is the so-called "dynamic"
relationship.  This is an optional form of :func:`~sqlalchemy.orm.relationship` which
returns a :class:`~sqlalchemy.orm.query.Query` object in place of a collection
when accessed. :func:`~sqlalchemy.orm.query.Query.filter` criterion may be
applied as well as limits and offsets, either explicitly or via array slices::

    class User(Base):
        __tablename__ = 'user'

        posts = relationship(Post, lazy="dynamic")

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

To place a dynamic relationship on a backref, use the :func:`~.orm.backref`
function in conjunction with ``lazy='dynamic'``::

    class Post(Base):
        __table__ = posts_table

        user = relationship(User,
                    backref=backref('posts', lazy='dynamic')
                )

Note that eager/lazy loading options cannot be used in conjunction dynamic relationships at this time.

.. note::

   The :func:`~.orm.dynamic_loader` function is essentially the same
   as :func:`~.orm.relationship` with the ``lazy='dynamic'`` argument specified.

.. warning::

   The "dynamic" loader applies to **collections only**.   It is not valid
   to use "dynamic" loaders with many-to-one, one-to-one, or uselist=False
   relationships.   Newer versions of SQLAlchemy emit warnings or exceptions
   in these cases.

Setting Noload
---------------

A "noload" relationship never loads from the database, even when
accessed.   It is configured using ``lazy='noload'``::

    class MyClass(Base):
        __tablename__ = 'some_table'

        children = relationship(MyOtherClass, lazy='noload')

Above, the ``children`` collection is fully writeable, and changes to it will
be persisted to the database as well as locally available for reading at the
time they are added. However when instances of ``MyClass`` are freshly loaded
from the database, the ``children`` collection stays empty.

.. _passive_deletes:

Using Passive Deletes
----------------------

Use :paramref:`~.relationship.passive_deletes` to disable child object loading on a DELETE
operation, in conjunction with "ON DELETE (CASCADE|SET NULL)" on your database
to automatically cascade deletes to child objects::

    class MyClass(Base):
        __tablename__ = 'mytable'
        id = Column(Integer, primary_key=True)
        children = relationship("MyOtherClass",
                        cascade="all, delete-orphan",
                        passive_deletes=True)

    class MyOtherClass(Base):
        __tablename__ = 'myothertable'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer,
                    ForeignKey('mytable.id', ondelete='CASCADE')
                        )


.. note::

    To use "ON DELETE CASCADE", the underlying database engine must
    support foreign keys.

    * When using MySQL, an appropriate storage engine must be
      selected.  See :ref:`mysql_storage_engines` for details.

    * When using SQLite, foreign key support must be enabled explicitly.
      See :ref:`sqlite_foreign_keys` for details.

When :paramref:`~.relationship.passive_deletes` is applied, the ``children`` relationship will not be
loaded into memory when an instance of ``MyClass`` is marked for deletion. The
``cascade="all, delete-orphan"`` *will* take effect for instances of
``MyOtherClass`` which are currently present in the session; however for
instances of ``MyOtherClass`` which are not loaded, SQLAlchemy assumes that
"ON DELETE CASCADE" rules will ensure that those rows are deleted by the
database.

.. currentmodule:: sqlalchemy.orm.collections
.. _custom_collections:

Customizing Collection Access
=============================

Mapping a one-to-many or many-to-many relationship results in a collection of
values accessible through an attribute on the parent instance. By default,
this collection is a ``list``::

    class Parent(Base):
        __tablename__ = 'parent'
        parent_id = Column(Integer, primary_key=True)

        children = relationship(Child)

    parent = Parent()
    parent.children.append(Child())
    print(parent.children[0])

Collections are not limited to lists. Sets, mutable sequences and almost any
other Python object that can act as a container can be used in place of the
default list, by specifying the :paramref:`~.relationship.collection_class` option on
:func:`~sqlalchemy.orm.relationship`::

    class Parent(Base):
        __tablename__ = 'parent'
        parent_id = Column(Integer, primary_key=True)

        # use a set
        children = relationship(Child, collection_class=set)

    parent = Parent()
    child = Child()
    parent.children.add(child)
    assert child in parent.children

Dictionary Collections
-----------------------

A little extra detail is needed when using a dictionary as a collection.
This because objects are always loaded from the database as lists, and a key-generation
strategy must be available to populate the dictionary correctly.  The
:func:`.attribute_mapped_collection` function is by far the most common way
to achieve a simple dictionary collection.  It produces a dictionary class that will apply a particular attribute
of the mapped class as a key.   Below we map an ``Item`` class containing
a dictionary of ``Note`` items keyed to the ``Note.keyword`` attribute::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm.collections import attribute_mapped_collection
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Item(Base):
        __tablename__ = 'item'
        id = Column(Integer, primary_key=True)
        notes = relationship("Note",
                    collection_class=attribute_mapped_collection('keyword'),
                    cascade="all, delete-orphan")

    class Note(Base):
        __tablename__ = 'note'
        id = Column(Integer, primary_key=True)
        item_id = Column(Integer, ForeignKey('item.id'), nullable=False)
        keyword = Column(String)
        text = Column(String)

        def __init__(self, keyword, text):
            self.keyword = keyword
            self.text = text

``Item.notes`` is then a dictionary::

    >>> item = Item()
    >>> item.notes['a'] = Note('a', 'atext')
    >>> item.notes.items()
    {'a': <__main__.Note object at 0x2eaaf0>}

:func:`.attribute_mapped_collection` will ensure that
the ``.keyword`` attribute of each ``Note`` complies with the key in the
dictionary.   Such as, when assigning to ``Item.notes``, the dictionary
key we supply must match that of the actual ``Note`` object::

    item = Item()
    item.notes = {
                'a': Note('a', 'atext'),
                'b': Note('b', 'btext')
            }

The attribute which :func:`.attribute_mapped_collection` uses as a key
does not need to be mapped at all!  Using a regular Python ``@property`` allows virtually
any detail or combination of details about the object to be used as the key, as
below when we establish it as a tuple of ``Note.keyword`` and the first ten letters
of the ``Note.text`` field::

    class Item(Base):
        __tablename__ = 'item'
        id = Column(Integer, primary_key=True)
        notes = relationship("Note",
                    collection_class=attribute_mapped_collection('note_key'),
                    backref="item",
                    cascade="all, delete-orphan")

    class Note(Base):
        __tablename__ = 'note'
        id = Column(Integer, primary_key=True)
        item_id = Column(Integer, ForeignKey('item.id'), nullable=False)
        keyword = Column(String)
        text = Column(String)

        @property
        def note_key(self):
            return (self.keyword, self.text[0:10])

        def __init__(self, keyword, text):
            self.keyword = keyword
            self.text = text

Above we added a ``Note.item`` backref.  Assigning to this reverse relationship, the ``Note``
is added to the ``Item.notes`` dictionary and the key is generated for us automatically::

    >>> item = Item()
    >>> n1 = Note("a", "atext")
    >>> n1.item = item
    >>> item.notes
    {('a', 'atext'): <__main__.Note object at 0x2eaaf0>}

Other built-in dictionary types include :func:`.column_mapped_collection`,
which is almost like :func:`.attribute_mapped_collection` except given the :class:`.Column`
object directly::

    from sqlalchemy.orm.collections import column_mapped_collection

    class Item(Base):
        __tablename__ = 'item'
        id = Column(Integer, primary_key=True)
        notes = relationship("Note",
                    collection_class=column_mapped_collection(Note.__table__.c.keyword),
                    cascade="all, delete-orphan")

as well as :func:`.mapped_collection` which is passed any callable function.
Note that it's usually easier to use :func:`.attribute_mapped_collection` along
with a ``@property`` as mentioned earlier::

    from sqlalchemy.orm.collections import mapped_collection

    class Item(Base):
        __tablename__ = 'item'
        id = Column(Integer, primary_key=True)
        notes = relationship("Note",
                    collection_class=mapped_collection(lambda note: note.text[0:10]),
                    cascade="all, delete-orphan")

Dictionary mappings are often combined with the "Association Proxy" extension to produce
streamlined dictionary views.  See :ref:`proxying_dictionaries` and :ref:`composite_association_proxy`
for examples.

.. autofunction:: attribute_mapped_collection

.. autofunction:: column_mapped_collection

.. autofunction:: mapped_collection

Custom Collection Implementations
==================================

You can use your own types for collections as well.  In simple cases,
inherting from ``list`` or ``set``, adding custom behavior, is all that's needed.
In other cases, special decorators are needed to tell SQLAlchemy more detail
about how the collection operates.

.. topic:: Do I need a custom collection implementation?

   In most cases not at all!   The most common use cases for a "custom" collection
   is one that validates or marshals incoming values into a new form, such as
   a string that becomes a class instance, or one which goes a
   step beyond and represents the data internally in some fashion, presenting
   a "view" of that data on the outside of a different form.

   For the first use case, the :func:`.orm.validates` decorator is by far
   the simplest way to intercept incoming values in all cases for the purposes
   of validation and simple marshaling.  See :ref:`simple_validators`
   for an example of this.

   For the second use case, the :ref:`associationproxy_toplevel` extension is a
   well-tested, widely used system that provides a read/write "view" of a
   collection in terms of some attribute present on the target object. As the
   target attribute can be a ``@property`` that returns virtually anything, a
   wide array of "alternative" views of a collection can be constructed with
   just a few functions. This approach leaves the underlying mapped collection
   unaffected and avoids the need to carefully tailor collection behavior on a
   method-by-method basis.

   Customized collections are useful when the collection needs to
   have special behaviors upon access or mutation operations that can't
   otherwise be modeled externally to the collection.   They can of course
   be combined with the above two approaches.

Collections in SQLAlchemy are transparently *instrumented*. Instrumentation
means that normal operations on the collection are tracked and result in
changes being written to the database at flush time. Additionally, collection
operations can fire *events* which indicate some secondary operation must take
place. Examples of a secondary operation include saving the child item in the
parent's :class:`~sqlalchemy.orm.session.Session` (i.e. the ``save-update``
cascade), as well as synchronizing the state of a bi-directional relationship
(i.e. a :func:`.backref`).

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

``append``, ``remove``, and ``extend`` are known list-like methods, and will
be instrumented automatically. ``__iter__`` is not a mutator method and won't
be instrumented, and ``foo`` won't be either.

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
--------------------------------------------

Decorators can be used to tag the individual methods the ORM needs to manage
collections. Use them when your class doesn't quite meet the regular interface
for its container type, or when you otherwise would like to use a different method to
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

.. autoclass:: collection
    :members:

.. _dictionary_collections:

Custom Dictionary-Based Collections
-----------------------------------

The :class:`.MappedCollection` class can be used as
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

.. note::

   Due to a bug in MappedCollection prior to version 0.7.6, this
   workaround usually needs to be called before a custom subclass
   of :class:`.MappedCollection` which uses :meth:`.collection.internally_instrumented`
   can be used::

    from sqlalchemy.orm.collections import _instrument_class, MappedCollection
    _instrument_class(MappedCollection)

   This will ensure that the :class:`.MappedCollection` has been properly
   initialized with custom ``__setitem__()`` and ``__delitem__()``
   methods before used in a custom subclass.

.. autoclass:: sqlalchemy.orm.collections.MappedCollection
   :members:

Instrumentation and Custom Types
--------------------------------

Many custom types and existing library classes can be used as a entity
collection type as-is without further ado. However, it is important to note
that the instrumentation process will modify the type, adding decorators
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

Collection Internals
=====================

Various internal methods.

.. autofunction:: bulk_replace

.. autoclass:: collection

.. autodata:: collection_adapter

.. autoclass:: CollectionAdapter

.. autoclass:: InstrumentedDict

.. autoclass:: InstrumentedList

.. autoclass:: InstrumentedSet

.. autofunction:: prepare_instrumentation

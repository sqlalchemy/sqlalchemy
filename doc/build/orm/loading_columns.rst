.. module:: sqlalchemy.orm

===============
Loading Columns
===============

This section presents additional options regarding the loading of columns.

.. _deferred:

Deferred Column Loading
=======================

This feature allows particular columns of a table be loaded only
upon direct access, instead of when the entity is queried using
:class:`.Query`.  This feature is useful when one wants to avoid
loading a large text or binary field into memory when it's not needed.
Individual columns can be lazy loaded by themselves or placed into groups that
lazy-load together, using the :func:`.orm.deferred` function to
mark them as "deferred". In the example below, we define a mapping that will load each of
``.excerpt`` and ``.photo`` in separate, individual-row SELECT statements when each
attribute is first referenced on the individual object instance::

    from sqlalchemy.orm import deferred
    from sqlalchemy import Integer, String, Text, Binary, Column

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = Column(String(2000))
        excerpt = deferred(Column(Text))
        photo = deferred(Column(Binary))

Classical mappings as always place the usage of :func:`.orm.deferred` in the
``properties`` dictionary against the table-bound :class:`.Column`::

    mapper(Book, book_table, properties={
        'photo':deferred(book_table.c.photo)
    })

Deferred columns can be associated with a "group" name, so that they load
together when any of them are first accessed.  The example below defines a
mapping with a ``photos`` deferred group.  When one ``.photo`` is accessed, all three
photos will be loaded in one SELECT statement. The ``.excerpt`` will be loaded
separately when it is accessed::

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = Column(String(2000))
        excerpt = deferred(Column(Text))
        photo1 = deferred(Column(Binary), group='photos')
        photo2 = deferred(Column(Binary), group='photos')
        photo3 = deferred(Column(Binary), group='photos')

You can defer or undefer columns at the :class:`~sqlalchemy.orm.query.Query`
level using options, including :func:`.orm.defer` and :func:`.orm.undefer`::

    from sqlalchemy.orm import defer, undefer

    query = session.query(Book)
    query = query.options(defer('summary'))
    query = query.options(undefer('excerpt'))
    query.all()

:func:`.orm.deferred` attributes which are marked with a "group" can be undeferred
using :func:`.orm.undefer_group`, sending in the group name::

    from sqlalchemy.orm import undefer_group

    query = session.query(Book)
    query.options(undefer_group('photos')).all()

Load Only Cols
--------------

An arbitrary set of columns can be selected as "load only" columns, which will
be loaded while deferring all other columns on a given entity, using :func:`.orm.load_only`::

    from sqlalchemy.orm import load_only

    session.query(Book).options(load_only("summary", "excerpt"))

.. versionadded:: 0.9.0

.. _deferred_loading_w_multiple:

Deferred Loading with Multiple Entities
---------------------------------------

To specify column deferral options within a :class:`.Query` that loads multiple types
of entity, the :class:`.Load` object can specify which parent entity to start with::

    from sqlalchemy.orm import Load

    query = session.query(Book, Author).join(Book.author)
    query = query.options(
                Load(Book).load_only("summary", "excerpt"),
                Load(Author).defer("bio")
            )

To specify column deferral options along the path of various relationships,
the options support chaining, where the loading style of each relationship
is specified first, then is chained to the deferral options.  Such as, to load
``Book`` instances, then joined-eager-load the ``Author``, then apply deferral
options to the ``Author`` entity::

    from sqlalchemy.orm import joinedload

    query = session.query(Book)
    query = query.options(
                joinedload(Book.author).load_only("summary", "excerpt"),
            )

In the case where the loading style of parent relationships should be left
unchanged, use :func:`.orm.defaultload`::

    from sqlalchemy.orm import defaultload

    query = session.query(Book)
    query = query.options(
                defaultload(Book.author).load_only("summary", "excerpt"),
            )

.. versionadded:: 0.9.0 support for :class:`.Load` and other options which
   allow for better targeting of deferral options.

Column Deferral API
-------------------

.. autofunction:: deferred

.. autofunction:: defer

.. autofunction:: load_only

.. autofunction:: undefer

.. autofunction:: undefer_group

.. _bundles:

Column Bundles
==============

The :class:`.Bundle` may be used to query for groups of columns under one
namespace.

.. versionadded:: 0.9.0

The bundle allows columns to be grouped together::

    from sqlalchemy.orm import Bundle

    bn = Bundle('mybundle', MyClass.data1, MyClass.data2)
    for row in session.query(bn).filter(bn.c.data1 == 'd1'):
        print(row.mybundle.data1, row.mybundle.data2)

The bundle can be subclassed to provide custom behaviors when results
are fetched.  The method :meth:`.Bundle.create_row_processor` is given
the :class:`.Query` and a set of "row processor" functions at query execution
time; these processor functions when given a result row will return the
individual attribute value, which can then be adapted into any kind of
return data structure.  Below illustrates replacing the usual :class:`.KeyedTuple`
return structure with a straight Python dictionary::

    from sqlalchemy.orm import Bundle

    class DictBundle(Bundle):
        def create_row_processor(self, query, procs, labels):
            """Override create_row_processor to return values as dictionaries"""
            def proc(row):
                return dict(
                            zip(labels, (proc(row) for proc in procs))
                        )
            return proc

.. versionchanged:: 1.0

   The ``proc()`` callable passed to the ``create_row_processor()``
   method of custom :class:`.Bundle` classes now accepts only a single
   "row" argument.

A result from the above bundle will return dictionary values::

    bn = DictBundle('mybundle', MyClass.data1, MyClass.data2)
    for row in session.query(bn).filter(bn.c.data1 == 'd1'):
        print(row.mybundle['data1'], row.mybundle['data2'])

The :class:`.Bundle` construct is also integrated into the behavior
of :func:`.composite`, where it is used to return composite attributes as objects
when queried as individual attributes.


.. _loading_columns:

.. currentmodule:: sqlalchemy.orm

===============
Loading Columns
===============

This section presents additional options regarding the loading of columns.

.. _deferred:

Deferred Column Loading
=======================

Deferred column loading allows particular columns of a table be loaded only
upon direct access, instead of when the entity is queried using
:class:`_query.Query`.  This feature is useful when one wants to avoid
loading a large text or binary field into memory when it's not needed.
Individual columns can be lazy loaded by themselves or placed into groups that
lazy-load together, using the :func:`_orm.deferred` function to
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

Classical mappings as always place the usage of :func:`_orm.deferred` in the
``properties`` dictionary against the table-bound :class:`_schema.Column`::

    mapper_registry.map_imperatively(Book, book_table, properties={
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

.. _deferred_options:

Deferred Column Loader Query Options
------------------------------------

Columns can be marked as "deferred" or reset to "undeferred" at query time
using options which are passed to the :meth:`_query.Query.options` method; the most
basic query options are :func:`_orm.defer` and
:func:`_orm.undefer`::

    from sqlalchemy.orm import defer
    from sqlalchemy.orm import undefer

    query = session.query(Book)
    query = query.options(defer('summary'), undefer('excerpt'))
    query.all()

Above, the "summary" column will not load until accessed, and the "excerpt"
column will load immediately even if it was mapped as a "deferred" column.

:func:`_orm.deferred` attributes which are marked with a "group" can be undeferred
using :func:`_orm.undefer_group`, sending in the group name::

    from sqlalchemy.orm import undefer_group

    query = session.query(Book)
    query.options(undefer_group('photos')).all()

.. _deferred_loading_w_multiple:

Deferred Loading across Multiple Entities
-----------------------------------------

To specify column deferral for a :class:`_query.Query` that loads multiple types of
entities at once, the deferral options may be specified more explicitly using
class-bound attributes, rather than string names::

    from sqlalchemy.orm import defer

    query = session.query(Book, Author).join(Book.author)
    query = query.options(defer(Author.bio))

Column deferral options may also indicate that they take place along various
relationship paths, which are themselves often :ref:`eagerly loaded
<loading_toplevel>` with loader options.  All relationship-bound loader options
support chaining  onto additional loader options, which include loading for
further levels of relationships, as well as onto column-oriented attributes at
that path. Such as, to load ``Author`` instances, then joined-eager-load the
``Author.books`` collection for each author, then apply deferral options to
column-oriented attributes onto each ``Book`` entity from that relationship,
the :func:`_orm.joinedload` loader option can be combined with the :func:`.load_only`
option (described later in this section) to defer all ``Book`` columns except
those explicitly specified::

    from sqlalchemy.orm import joinedload

    query = session.query(Author)
    query = query.options(
                joinedload(Author.books).load_only(Book.summary, Book.excerpt),
            )

Option structures as above can also be organized in more complex ways, such
as hierarchically using the :meth:`_orm.Load.options`
method, which allows multiple sub-options to be chained to a common parent
option at once.  Any mixture of string names and class-bound attribute objects
may be used::

    from sqlalchemy.orm import defer
    from sqlalchemy.orm import joinedload
    from sqlalchemy.orm import load_only

    query = session.query(Author)
    query = query.options(
                joinedload(Author.book).options(
                    load_only("summary", "excerpt"),
                    joinedload(Book.citations).options(
                        joinedload(Citation.author),
                        defer(Citation.fulltext)
                    )
                )
            )

.. versionadded:: 1.3.6  Added :meth:`_orm.Load.options` to allow easier
   construction of hierarchies of loader options.

Another way to apply options to a path is to use the :func:`_orm.defaultload`
function.   This function is used to indicate a particular path within a loader
option structure without actually setting any options at that level, so that further
sub-options may be applied.  The :func:`_orm.defaultload` function can be used
to create the same structure as we did above using :meth:`_orm.Load.options` as::

    query = session.query(Author)
    query = query.options(
        joinedload(Author.book).load_only("summary", "excerpt"),
        defaultload(Author.book).joinedload(Book.citations).joinedload(Citation.author),
        defaultload(Author.book).defaultload(Book.citations).defer(Citation.fulltext)
    )

.. seealso::

    :ref:`relationship_loader_options` - targeted towards relationship loading

Load Only and Wildcard Options
------------------------------

The ORM loader option system supports the concept of "wildcard" loader options,
in which a loader option can be passed an asterisk ``"*"`` to indicate that
a particular option should apply to all applicable attributes of a mapped
class.   Such as, if we wanted to load the ``Book`` class but only
the "summary" and "excerpt" columns, we could say::

    from sqlalchemy.orm import defer
    from sqlalchemy.orm import undefer

    session.query(Book).options(
        defer('*'), undefer("summary"), undefer("excerpt"))

Above, the :func:`.defer` option is applied using a wildcard to all column
attributes on the ``Book`` class.   Then, the :func:`.undefer` option is used
against the "summary" and "excerpt" fields so that they are the  only columns
loaded up front. A query for the above entity will include only the "summary"
and "excerpt" fields in the SELECT, along with the primary key columns which
are always used by the ORM.

A similar function is available with less verbosity by using the
:func:`_orm.load_only` option.  This is a so-called **exclusionary** option
which will apply deferred behavior to all column attributes except those
that are named::

    from sqlalchemy.orm import load_only

    session.query(Book).options(load_only("summary", "excerpt"))

Wildcard and Exclusionary Options with Multiple-Entity Queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Wildcard options and exclusionary options such as :func:`.load_only` may
only be applied to a single entity at a time within a :class:`_query.Query`.  To
suit the less common case where a :class:`_query.Query` is returning multiple
primary entities at once, a special calling style may be required in order
to apply a wildcard or exclusionary option, which is to use the
:class:`_orm.Load` object to indicate the starting entity for a deferral option.
Such as, if we were loading ``Book`` and ``Author`` at once, the :class:`_query.Query`
will raise an informative error if we try to apply :func:`.load_only` to
both at once.  Using :class:`_orm.Load` looks like::

    from sqlalchemy.orm import Load

    query = session.query(Book, Author).join(Book.author)
    query = query.options(
                Load(Book).load_only("summary", "excerpt")
            )

Above, :class:`_orm.Load` is used in conjunction with the exclusionary option
:func:`.load_only` so that the deferral of all other columns only takes
place for the ``Book`` class and not the ``Author`` class.   Again,
the :class:`_query.Query` object should raise an informative error message when
the above calling style is actually required that describes those cases
where explicit use of :class:`_orm.Load` is needed.

.. _deferred_raiseload:

Raiseload for Deferred Columns
------------------------------

.. versionadded:: 1.4

The :func:`.deferred` loader option and the corresponding loader strategy also
support the concept of "raiseload", which is a loader strategy that will raise
:class:`.InvalidRequestError` if the attribute is accessed such that it would
need to emit a SQL query in order to be loaded.   This behavior is the
column-based equivalent of the :func:`.raiseload` feature for relationship
loading, discussed at :ref:`prevent_lazy_with_raiseload`.    Using the
:paramref:`.orm.defer.raiseload` parameter on the :func:`.defer` option,
an exception is raised if the attribute is accessed::

    book = session.query(Book).options(defer(Book.summary, raiseload=True)).first()

    # would raise an exception
    book.summary

Deferred "raiseload" can be configured at the mapper level via
:paramref:`.orm.deferred.raiseload` on :func:`.deferred`, so that an explicit
:func:`.undefer` is required in order for the attribute to be usable::


    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = deferred(Column(String(2000)), raiseload=True)
        excerpt = deferred(Column(Text), raiseload=True)

    book_w_excerpt = session.query(Book).options(undefer(Book.excerpt)).first()



Column Deferral API
-------------------

.. autofunction:: defer

.. autofunction:: deferred

.. autofunction:: query_expression

.. autofunction:: load_only

.. autofunction:: undefer

.. autofunction:: undefer_group

.. autofunction:: with_expression

.. _bundles:

Column Bundles
==============

The :class:`_orm.Bundle` may be used to query for groups of columns under one
namespace.

The bundle allows columns to be grouped together::

    from sqlalchemy.orm import Bundle

    bn = Bundle('mybundle', MyClass.data1, MyClass.data2)
    for row in session.query(bn).filter(bn.c.data1 == 'd1'):
        print(row.mybundle.data1, row.mybundle.data2)

The bundle can be subclassed to provide custom behaviors when results
are fetched.  The method :meth:`.Bundle.create_row_processor` is given
the statement object and a set of "row processor" functions at query execution
time; these processor functions when given a result row will return the
individual attribute value, which can then be adapted into any kind of
return data structure.  Below illustrates replacing the usual :class:`.Row`
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

.. note::

    The :class:`_orm.Bundle` construct only applies to column expressions.
    It does not apply to ORM attributes mapped using :func:`_orm.relationship`.

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


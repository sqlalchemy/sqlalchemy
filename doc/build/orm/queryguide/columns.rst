.. highlight:: pycon+sql

.. |prev| replace:: :doc:`dml`
.. |next| replace:: :doc:`relationships`

.. include:: queryguide_nav_include.rst


.. doctest-include _deferred_setup.rst

.. currentmodule:: sqlalchemy.orm

.. _loading_columns:

======================
Column Loading Options
======================

.. admonition:: About this Document

    This section presents additional options regarding the loading of
    columns.  The mappings used include columns that would store
    large string values for which we may want to limit when they
    are loaded.

    :doc:`View the ORM setup for this page <_deferred_setup>`.  Some
    of the examples below will redefine the ``Book`` mapper to modify
    some of the column definitions.

.. _orm_queryguide_column_deferral:

Limiting which Columns Load with Column Deferral
------------------------------------------------

**Column deferral** refers to ORM mapped columns that are omitted from a SELECT
statement when objects of that type are queried. The general rationale here is
performance, in cases where tables have seldom-used columns with potentially
large data values, as fully loading these columns on every query may be
time and/or memory intensive. SQLAlchemy ORM offers a variety of ways to
control the loading of columns when entities are loaded.

Most examples in this section are illustrating **ORM loader options**. These
are small constructs that are passed to the :meth:`_sql.Select.options` method
of the :class:`_sql.Select` object, which are then consumed by the ORM
when the object is compiled into a SQL string.

.. _orm_queryguide_load_only:

Using ``load_only()`` to reduce loaded columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_orm.load_only` loader option is the most expedient option to use
when loading objects where it is known that only a small handful of columns will
be accessed. This option accepts a variable number of class-bound attribute
objects indicating those column-mapped attributes that should be loaded, where
all other column-mapped attributes outside of the primary key will not be part
of the columns fetched . In the example below, the ``Book`` class contains
columns ``.title``, ``.summary`` and ``.cover_photo``. Using
:func:`_orm.load_only` we can instruct the ORM to only load the
``.title`` and ``.summary`` columns up front::

    >>> from sqlalchemy import select
    >>> from sqlalchemy.orm import load_only
    >>> stmt = select(Book).options(load_only(Book.title, Book.summary))
    >>> books = session.scalars(stmt).all()
    {execsql}SELECT book.id, book.title, book.summary
    FROM book
    [...] ()
    {stop}>>> for book in books:
    ...     print(f"{book.title}  {book.summary}")
    100 Years of Krabby Patties  some long summary
    Sea Catch 22  another long summary
    The Sea Grapes of Wrath  yet another summary
    A Nut Like No Other  some long summary
    Geodesic Domes: A Retrospective  another long summary
    Rocketry for Squirrels  yet another summary

Above, the SELECT statement has omitted the ``.cover_photo`` column and
included only ``.title`` and ``.summary``, as well as the primary key column
``.id``; the ORM will typically always fetch the primary key columns as these
are required to establish the identity for the row.

Once loaded, the object will normally have :term:`lazy loading` behavior
applied to the remaining unloaded attributes, meaning that when any are first
accessed, a SQL statement will be emitted within the current transaction in
order to load the value.  Below, accessing ``.cover_photo`` emits a SELECT
statement to load its value::

    >>> img_data = books[0].cover_photo
    {execsql}SELECT book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (1,)

Lazy loads are always emitted using the :class:`_orm.Session` to which the
object is in the :term:`persistent` state.  If the object is :term:`detached`
from any :class:`_orm.Session`, the operation fails, raising an exception.

As an alternative to lazy loading on access, deferred columns may also be
configured to raise an informative exception when accessed, regardless of their
attachment state.  When using the :func:`_orm.load_only` construct, this
may be indicated using the :paramref:`_orm.load_only.raiseload` parameter.
See the section :ref:`orm_queryguide_deferred_raiseload` for
background and examples.

.. tip::  as noted elsewhere, lazy loading is not available when using
   :ref:`asyncio_toplevel`.

Using ``load_only()`` with multiple entities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`_orm.load_only` limits itself to the single entity that is referred
towards in its list of attributes (passing a list of attributes that span more
than a single entity is currently disallowed). In the example below, the given
:func:`_orm.load_only` option applies only to the ``Book`` entity. The ``User``
entity that's also selected is not affected; within the resulting SELECT
statement, all columns for ``user_account`` are present, whereas only
``book.id`` and ``book.title`` are present for the ``book`` table::

    >>> stmt = select(User, Book).join_from(User, Book).options(load_only(Book.title))
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, user_account.fullname,
    book.id AS id_1, book.title
    FROM user_account JOIN book ON user_account.id = book.owner_id

If we wanted to apply :func:`_orm.load_only` options to both ``User`` and
``Book``, we would make use of two separate options::

    >>> stmt = (
    ...     select(User, Book)
    ...     .join_from(User, Book)
    ...     .options(load_only(User.name), load_only(Book.title))
    ... )
    >>> print(stmt)
    {printsql}SELECT user_account.id, user_account.name, book.id AS id_1, book.title
    FROM user_account JOIN book ON user_account.id = book.owner_id

.. _orm_queryguide_load_only_related:

Using ``load_only()`` on related objects and collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using :ref:`relationship loaders <loading_toplevel>` to control the
loading of related objects, the
:meth:`.Load.load_only` method of any relationship loader may be used
to apply :func:`_orm.load_only` rules to columns on the sub-entity.  In the example below,
:func:`_orm.selectinload` is used to load the related ``books`` collection
on each ``User`` object.   By applying :meth:`.Load.load_only` to the resulting
option object, when objects are loaded for the relationship, the
SELECT emitted will only refer to the ``title`` column
in addition to primary key column::

    >>> from sqlalchemy.orm import selectinload
    >>> stmt = select(User).options(selectinload(User.books).load_only(Book.title))
    >>> for user in session.scalars(stmt):
    ...     print(f"{user.fullname}   {[b.title for b in user.books]}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    [...] ()
    SELECT book.owner_id AS book_owner_id, book.id AS book_id, book.title AS book_title
    FROM book
    WHERE book.owner_id IN (?, ?)
    [...] (1, 2)
    {stop}Spongebob Squarepants   ['100 Years of Krabby Patties', 'Sea Catch 22', 'The Sea Grapes of Wrath']
    Sandy Cheeks   ['A Nut Like No Other', 'Geodesic Domes: A Retrospective', 'Rocketry for Squirrels']


.. comment

    >>> session.expunge_all()

:func:`_orm.load_only` may also be applied to sub-entities without needing
to state the style of loading to use for the relationship itself.  If we didn't
want to change the default loading style of ``User.books`` but still apply
load only rules to ``Book``, we would link using the :func:`_orm.defaultload`
option, which in this case will retain the default relationship loading
style of ``"lazy"``, and applying our custom :func:`_orm.load_only` rule to
the SELECT statement emitted for each ``User.books`` collection::

    >>> from sqlalchemy.orm import defaultload
    >>> stmt = select(User).options(defaultload(User.books).load_only(Book.title))
    >>> for user in session.scalars(stmt):
    ...     print(f"{user.fullname}   {[b.title for b in user.books]}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    [...] ()
    SELECT book.id, book.title
    FROM book
    WHERE ? = book.owner_id
    [...] (1,)
    {stop}Spongebob Squarepants   ['100 Years of Krabby Patties', 'Sea Catch 22', 'The Sea Grapes of Wrath']
    {execsql}SELECT book.id, book.title
    FROM book
    WHERE ? = book.owner_id
    [...] (2,)
    {stop}Sandy Cheeks   ['A Nut Like No Other', 'Geodesic Domes: A Retrospective', 'Rocketry for Squirrels']

.. _orm_queryguide_defer:

Using ``defer()`` to omit specific columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_orm.defer` loader option is a more fine grained alternative to
:func:`_orm.load_only`, which allows a single specific column to be marked as
"dont load".  In the example below, :func:`_orm.defer` is applied directly to the
``.cover_photo`` column, leaving the behavior of all other columns
unchanged::

    >>> from sqlalchemy.orm import defer
    >>> stmt = select(Book).where(Book.owner_id == 2).options(defer(Book.cover_photo))
    >>> books = session.scalars(stmt).all()
    {execsql}SELECT book.id, book.owner_id, book.title, book.summary
    FROM book
    WHERE book.owner_id = ?
    [...] (2,)
    {stop}>>> for book in books:
    ...     print(f"{book.title}: {book.summary}")
    A Nut Like No Other: some long summary
    Geodesic Domes: A Retrospective: another long summary
    Rocketry for Squirrels: yet another summary

As is the case with :func:`_orm.load_only`, unloaded columns by default
will load themselves when accessed using :term:`lazy loading`::

    >>> img_data = books[0].cover_photo
    {execsql}SELECT book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (4,)

Multiple :func:`_orm.defer` options may be used in one statement in order to
mark several columns as deferred.

As is the case with :func:`_orm.load_only`, the :func:`_orm.defer` option
also includes the ability to have a deferred attribute raise an exception on
access rather than lazy loading.  This is illustrated in the section
:ref:`orm_queryguide_deferred_raiseload`.

.. _orm_queryguide_deferred_raiseload:

Using raiseload to prevent deferred column loads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. comment

  >>> session.expunge_all()

When using the :func:`_orm.load_only` or :func:`_orm.defer` loader options,
attributes marked as deferred on an object have the default behavior that when
first accessed, a SELECT statement will be emitted within the current
transaction in order to load their value. It is often necessary to prevent this
load from occurring, and instead raise an exception when the attribute is
accessed, indicating that the need to query the database for this column was
not expected. A typical scenario is an operation where objects are loaded with
all the columns that are known to be required for the operation to proceed,
which are then passed onto a view layer. Any further SQL operations that emit
within the view layer should be caught, so that the up-front loading operation
can be adjusted to accommodate for that additional data up front, rather than
incurring additional lazy loading.

For this use case the :func:`_orm.defer` and :func:`_orm.load_only` options
include a boolean parameter :paramref:`_orm.defer.raiseload`, which when set to
``True`` will cause the affected attributes to raise on access.  In the
example below, the deferred column ``.cover_photo`` will disallow attribute
access::

  >>> book = session.scalar(
  ...     select(Book).options(defer(Book.cover_photo, raiseload=True)).where(Book.id == 4)
  ... )
  {execsql}SELECT book.id, book.owner_id, book.title, book.summary
  FROM book
  WHERE book.id = ?
  [...] (4,)
  {stop}>>> book.cover_photo
  Traceback (most recent call last):
  ...
  sqlalchemy.exc.InvalidRequestError: 'Book.cover_photo' is not available due to raiseload=True

When using :func:`_orm.load_only` to name a specific set of non-deferred
columns, ``raiseload`` behavior may be applied to the remaining columns
using the :paramref:`_orm.load_only.raiseload` parameter, which will be applied
to all deferred attributes::

  >>> session.expunge_all()
  >>> book = session.scalar(
  ...     select(Book).options(load_only(Book.title, raiseload=True)).where(Book.id == 5)
  ... )
  {execsql}SELECT book.id, book.title
  FROM book
  WHERE book.id = ?
  [...] (5,)
  {stop}>>> book.summary
  Traceback (most recent call last):
  ...
  sqlalchemy.exc.InvalidRequestError: 'Book.summary' is not available due to raiseload=True

.. note::

    It is not yet possible to mix :func:`_orm.load_only` and :func:`_orm.defer`
    options which refer to the same entity together in one statement in order
    to change the ``raiseload`` behavior of certain attributes; currently,
    doing so will produce undefined loading behavior of attributes.

.. seealso::

    The :paramref:`_orm.defer.raiseload` feature is the column-level version
    of the same "raiseload" feature that's available for relationships.
    For "raiseload" with relationships, see
    :ref:`prevent_lazy_with_raiseload` in the
    :ref:`loading_toplevel` section of this guide.



.. _orm_queryguide_deferred_declarative:

Configuring Column Deferral on Mappings
---------------------------------------

.. comment

    >>> class Base(DeclarativeBase):
    ...     pass

The functionality of :func:`_orm.defer` is available as a default behavior for
mapped columns, as may be appropriate for columns that should not be loaded
unconditionally on every query. To configure, use the
:paramref:`_orm.mapped_column.deferred` parameter of
:func:`_orm.mapped_column`. The example below illustrates a mapping for
``Book`` which applies default column deferral to the ``summary`` and
``cover_photo`` columns::

    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(Text, deferred=True)
    ...     cover_photo: Mapped[bytes] = mapped_column(LargeBinary, deferred=True)
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"

Using the above mapping, queries against ``Book`` will automatically not
include the ``summary`` and ``cover_photo`` columns::

    >>> book = session.scalar(select(Book).where(Book.id == 2))
    {execsql}SELECT book.id, book.owner_id, book.title
    FROM book
    WHERE book.id = ?
    [...] (2,)

As is the case with all deferral, the default behavior when deferred attributes
on the loaded object are first accessed is that they will :term:`lazy load`
their value::

    >>> img_data = book.cover_photo
    {execsql}SELECT book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (2,)

As is the case with the :func:`_orm.defer` and :func:`_orm.load_only`
loader options, mapper level deferral also includes an option for ``raiseload``
behavior to occur, rather than lazy loading, when no other options are
present in a statement.  This allows a mapping where certain columns
will not load by default and will also never load lazily without explicit
directives used in a statement.   See the section
:ref:`orm_queryguide_mapper_deferred_raiseload` for background on how to
configure and use this behavior.

.. _orm_queryguide_deferred_imperative:

Using ``deferred()`` for imperative mappers, mapped SQL expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_orm.deferred` function is the earlier, more general purpose
"deferred column" mapping directive that precedes the introduction of the
:func:`_orm.mapped_column` construct in SQLAlchemy.

:func:`_orm.deferred` is used when configuring ORM mappers, and accepts
arbitrary SQL expressions or
:class:`_schema.Column` objects. As such it's suitable to be used with
non-declarative :ref:`imperative mappings <orm_imperative_mapping>`, passing it
to the :paramref:`_orm.registry.map_imperatively.properties` dictionary:

.. sourcecode:: python

    from sqlalchemy import Blob
    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy import Text
    from sqlalchemy.orm import registry

    mapper_registry = registry()

    book_table = Table(
        "book",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("title", String(50)),
        Column("summary", Text),
        Column("cover_image", Blob),
    )


    class Book:
        pass


    mapper_registry.map_imperatively(
        Book,
        book_table,
        properties={
            "summary": deferred(book_table.c.summary),
            "cover_image": deferred(book_table.c.cover_image),
        },
    )

:func:`_orm.deferred` may also be used in place of :func:`_orm.column_property`
when mapped SQL expressions should be loaded on a deferred basis:

.. sourcecode:: python

    from sqlalchemy.orm import deferred


    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        firstname: Mapped[str] = mapped_column()
        lastname: Mapped[str] = mapped_column()
        fullname: Mapped[str] = deferred(firstname + " " + lastname)

.. seealso::

    :ref:`mapper_column_property_sql_expressions` - in the section
    :ref:`mapper_sql_expressions`

    :ref:`orm_imperative_table_column_options` - in the section
    :ref:`orm_declarative_table_config_toplevel`

Using ``undefer()`` to "eagerly" load deferred columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With columns configured on mappings to defer by default, the
:func:`_orm.undefer` option will cause any column that is normally deferred
to be undeferred, that is, to load up front with all the other columns
of the mapping.   For example we may apply :func:`_orm.undefer` to the
``Book.summary`` column, which is indicated in the previous mapping
as deferred::

    >>> from sqlalchemy.orm import undefer
    >>> book = session.scalar(select(Book).where(Book.id == 2).options(undefer(Book.summary)))
    {execsql}SELECT book.id, book.owner_id, book.title, book.summary
    FROM book
    WHERE book.id = ?
    [...] (2,)

The ``Book.summary`` column was now eagerly loaded, and may be accessed without
additional SQL being emitted::

    >>> print(book.summary)
    another long summary

.. _orm_queryguide_deferred_group:

Loading deferred columns in groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. comment

    >>> class Base(DeclarativeBase):
    ...     pass

Normally when a column is mapped with ``mapped_column(deferred=True)``, when
the deferred attribute is accessed on an object, SQL will be emitted to load
only that specific column and no others, even if the mapping has other columns
that are also marked as deferred. In the common case that the deferred
attribute is part of a group of attributes that should all load at once, rather
than emitting SQL for each attribute individually, the
:paramref:`_orm.mapped_column.deferred_group` parameter may be used, which
accepts an arbitrary string which will define a common group of columns to be
undeferred::

    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(
    ...         Text, deferred=True, deferred_group="book_attrs"
    ...     )
    ...     cover_photo: Mapped[bytes] = mapped_column(
    ...         LargeBinary, deferred=True, deferred_group="book_attrs"
    ...     )
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"

Using the above mapping, accessing either ``summary`` or ``cover_photo``
will load both columns at once using just one SELECT statement::

    >>> book = session.scalar(select(Book).where(Book.id == 2))
    {execsql}SELECT book.id, book.owner_id, book.title
    FROM book
    WHERE book.id = ?
    [...] (2,)
    {stop}>>> img_data, summary = book.cover_photo, book.summary
    {execsql}SELECT book.summary, book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (2,)


Undeferring by group with ``undefer_group()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If deferred columns are configured with :paramref:`_orm.mapped_column.deferred_group`
as introduced in the preceding section, the
entire group may be indicated to load eagerly using the :func:`_orm.undefer_group`
option, passing the string name of the group to be eagerly loaded::

    >>> from sqlalchemy.orm import undefer_group
    >>> book = session.scalar(
    ...     select(Book).where(Book.id == 2).options(undefer_group("book_attrs"))
    ... )
    {execsql}SELECT book.id, book.owner_id, book.title, book.summary, book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (2,)

Both ``summary`` and ``cover_photo`` are available without additional loads::

    >>> img_data, summary = book.cover_photo, book.summary

Undeferring on wildcards
^^^^^^^^^^^^^^^^^^^^^^^^

Most ORM loader options accept a wildcard expression, indicated by
``"*"``, which indicates that the option should be applied to all relevant
attributes.   If a mapping has a series of deferred columns, all such
columns can be undeferred at once, without using a group name, by indicating
a wildcard::

    >>> book = session.scalar(select(Book).where(Book.id == 3).options(undefer("*")))
    {execsql}SELECT book.id, book.owner_id, book.title, book.summary, book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (3,)

.. _orm_queryguide_mapper_deferred_raiseload:

Configuring mapper-level "raiseload" behavior
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. comment

    >>> class Base(DeclarativeBase):
    ...     pass

The "raiseload" behavior first introduced at :ref:`orm_queryguide_deferred_raiseload` may
also be applied as a default mapper-level behavior, using the
:paramref:`_orm.mapped_column.deferred_raiseload` parameter of
:func:`_orm.mapped_column`.  When using this parameter, the affected columns
will raise on access in all cases unless explicitly "undeferred" using
:func:`_orm.undefer` or :func:`_orm.load_only` at query time::

    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(Text, deferred=True, deferred_raiseload=True)
    ...     cover_photo: Mapped[bytes] = mapped_column(
    ...         LargeBinary, deferred=True, deferred_raiseload=True
    ...     )
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"

Using the above mapping, the ``.summary`` and ``.cover_photo`` columns are
by default not loadable::

    >>> book = session.scalar(select(Book).where(Book.id == 2))
    {execsql}SELECT book.id, book.owner_id, book.title
    FROM book
    WHERE book.id = ?
    [...] (2,)
    {stop}>>> book.summary
    Traceback (most recent call last):
    ...
    sqlalchemy.exc.InvalidRequestError: 'Book.summary' is not available due to raiseload=True

Only by overriding their behavior at query time, typically using
:func:`_orm.undefer` or :func:`_orm.undefer_group`, or less commonly
:func:`_orm.defer`, may the attributes be loaded.  The example below applies
``undefer('*')`` to undefer all attributes, also making use of
:ref:`orm_queryguide_populate_existing` to refresh the already-loaded object's loader options::

    >>> book = session.scalar(
    ...     select(Book)
    ...     .where(Book.id == 2)
    ...     .options(undefer("*"))
    ...     .execution_options(populate_existing=True)
    ... )
    {execsql}SELECT book.id, book.owner_id, book.title, book.summary, book.cover_photo
    FROM book
    WHERE book.id = ?
    [...] (2,)
    {stop}>>> book.summary
    'another long summary'



.. _orm_queryguide_with_expression:

Loading Arbitrary SQL Expressions onto Objects
-----------------------------------------------

.. comment

    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class User(Base):
    ...     __tablename__ = "user_account"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     fullname: Mapped[Optional[str]]
    ...     books: Mapped[List["Book"]] = relationship(back_populates="owner")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"
    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(Text)
    ...     cover_photo: Mapped[bytes] = mapped_column(LargeBinary)
    ...     owner: Mapped["User"] = relationship(back_populates="books")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"


As discussed :ref:`orm_queryguide_select_columns` and elsewhere,
the :func:`.select` construct may be used to load arbitrary SQL expressions
in a result set.  Such as if we wanted to issue a query that loads
``User`` objects, but also includes a count of how many books
each ``User`` owned, we could use ``func.count(Book.id)`` to add a "count"
column to a query which includes a JOIN to ``Book`` as well as a GROUP BY
owner id.  This will yield :class:`.Row` objects that each contain two
entries, one for ``User`` and one for ``func.count(Book.id)``::

    >>> from sqlalchemy import func
    >>> stmt = select(User, func.count(Book.id)).join_from(User, Book).group_by(Book.owner_id)
    >>> for user, book_count in session.execute(stmt):
    ...     print(f"Username: {user.name}  Number of books: {book_count}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname,
    count(book.id) AS count_1
    FROM user_account JOIN book ON user_account.id = book.owner_id
    GROUP BY book.owner_id
    [...] ()
    {stop}Username: spongebob  Number of books: 3
    Username: sandy  Number of books: 3

In the above example, the ``User`` entity and the "book count" SQL expression
are returned separately. However, a popular use case is to produce a query that
will yield ``User`` objects alone, which can be iterated for example using
:meth:`_orm.Session.scalars`, where the result of the ``func.count(Book.id)``
SQL expression is applied *dynamically* to each ``User`` entity. The end result
would be similar to the case where an arbitrary SQL expression were mapped to
the class using :func:`_orm.column_property`, except that the SQL expression
can be modified at query time. For this use case SQLAlchemy provides the
:func:`_orm.with_expression` loader option, which when combined with the mapper
level :func:`_orm.query_expression` directive may produce this result.

.. comment

    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(Text)
    ...     cover_photo: Mapped[bytes] = mapped_column(LargeBinary)
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"


To apply :func:`_orm.with_expression` to a query, the mapped class must have
pre-configured an ORM mapped attribute using the :func:`_orm.query_expression`
directive; this directive will produce an attribute on the mapped
class that is suitable for receiving query-time SQL expressions.  Below
we add a new attribute ``User.book_count`` to ``User``.  This ORM mapped attribute
is read-only and has no default value; accessing it on a loaded instance will
normally produce ``None``::

    >>> from sqlalchemy.orm import query_expression
    >>> class User(Base):
    ...     __tablename__ = "user_account"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     fullname: Mapped[Optional[str]]
    ...     book_count: Mapped[int] = query_expression()
    ...
    ...     def __repr__(self) -> str:
    ...         return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

With the ``User.book_count`` attribute configured in our mapping, we may populate
it with data from a SQL expression using the
:func:`_orm.with_expression` loader option to apply a custom SQL expression
to each ``User`` object as it's loaded::


    >>> from sqlalchemy.orm import with_expression
    >>> stmt = (
    ...     select(User)
    ...     .join_from(User, Book)
    ...     .group_by(Book.owner_id)
    ...     .options(with_expression(User.book_count, func.count(Book.id)))
    ... )
    >>> for user in session.scalars(stmt):
    ...     print(f"Username: {user.name}  Number of books: {user.book_count}")
    {execsql}SELECT count(book.id) AS count_1, user_account.id, user_account.name,
    user_account.fullname
    FROM user_account JOIN book ON user_account.id = book.owner_id
    GROUP BY book.owner_id
    [...] ()
    {stop}Username: spongebob  Number of books: 3
    Username: sandy  Number of books: 3

Above, we moved our ``func.count(Book.id)`` expression out of the columns
argument of the :func:`_sql.select` construct and into the :func:`_orm.with_expression`
loader option.  The ORM then considers this to be a special column load
option that's applied dynamically to the statement.

The :func:`.query_expression` mapping has these caveats:

* On an object where :func:`_orm.with_expression` were not used to populate
  the attribute, the attribute on an object instance will have the value
  ``None``, unless on the mapping the :paramref:`_orm.query_expression.default_expr`
  parameter is set to a default SQL expression.

* The :func:`_orm.with_expression` value **does not populate on an object that is
  already loaded**, unless :ref:`orm_queryguide_populate_existing` is used.
  The example below will **not work**, as the ``A`` object
  is already loaded:

  .. sourcecode:: python

    # load the first A
    obj = session.scalars(select(A).order_by(A.id)).first()

    # load the same A with an option; expression will **not** be applied
    # to the already-loaded object
    obj = session.scalars(select(A).options(with_expression(A.expr, some_expr))).first()

  To ensure the attribute is re-loaded on an existing object, use the
  :ref:`orm_queryguide_populate_existing` execution option to ensure
  all columns are re-populated:

  .. sourcecode:: python

    obj = session.scalars(
        select(A)
        .options(with_expression(A.expr, some_expr))
        .execution_options(populate_existing=True)
    ).first()

* The :func:`_orm.with_expression` SQL expression **is lost when the object is
  expired**.  Once the object is expired, either via :meth:`.Session.expire`
  or via the expire_on_commit behavior of :meth:`.Session.commit`, the SQL
  expression and its value is no longer associated with the attribute and will
  return ``None`` on subsequent access.

* :func:`_orm.with_expression`, as an object loading option, only takes effect
  on the **outermost part
  of a query** and only for a query against a full entity, and not for arbitrary
  column selects, within subqueries, or the elements of a compound
  statement such as a UNION.  See the next
  section :ref:`orm_queryguide_with_expression_unions` for an example.

* The mapped attribute **cannot** be applied to other parts of the
  query, such as the WHERE clause, the ORDER BY clause, and make use of the
  ad-hoc expression; that is, this won't work:

  .. sourcecode:: python

    # can't refer to A.expr elsewhere in the query
    stmt = (
        select(A)
        .options(with_expression(A.expr, A.x + A.y))
        .filter(A.expr > 5)
        .order_by(A.expr)
    )

  The ``A.expr`` expression will resolve to NULL in the above WHERE clause
  and ORDER BY clause. To use the expression throughout the query, assign to a
  variable and use that:

  .. sourcecode:: python

    # assign desired expression up front, then refer to that in
    # the query
    a_expr = A.x + A.y
    stmt = (
        select(A)
        .options(with_expression(A.expr, a_expr))
        .filter(a_expr > 5)
        .order_by(a_expr)
    )

.. seealso::

    The :func:`_orm.with_expression` option is a special option used to
    apply SQL expressions to mapped classes dynamically at query time.
    For ordinary fixed SQL expressions configured on mappers,
    see the section :ref:`mapper_sql_expressions`.

.. _orm_queryguide_with_expression_unions:

Using ``with_expression()`` with UNIONs, other subqueries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. comment

  >>> session.close()

The :func:`_orm.with_expression` construct is an ORM loader option, and as
such may only be applied to the outermost level of a SELECT statement which
is to load a particular ORM entity.   It does not have any effect if used
inside of a :func:`_sql.select` that will then be used as a subquery or
as an element within a compound statement such as a UNION.

In order to use arbitrary SQL expressions in subqueries, normal Core-style
means of adding expressions should be used. To assemble a subquery-derived
expression onto the ORM entity's :func:`_orm.query_expression` attributes,
:func:`_orm.with_expression` is used at the top layer of ORM object loading,
referencing the SQL expression within the subquery.

In the example below, two :func:`_sql.select` constructs are used against
the ORM entity ``A`` with an additional SQL expression labeled in
``expr``, and combined using :func:`_sql.union_all`.  Then, at the topmost
layer, the ``A`` entity is SELECTed from this UNION, using the
querying technique described at :ref:`orm_queryguide_unions`, adding an
option with :func:`_orm.with_expression` to extract this SQL expression
onto newly loaded instances of ``A``::

    >>> from sqlalchemy import union_all
    >>> s1 = (
    ...     select(User, func.count(Book.id).label("book_count"))
    ...     .join_from(User, Book)
    ...     .where(User.name == "spongebob")
    ... )
    >>> s2 = (
    ...     select(User, func.count(Book.id).label("book_count"))
    ...     .join_from(User, Book)
    ...     .where(User.name == "sandy")
    ... )
    >>> union_stmt = union_all(s1, s2)
    >>> orm_stmt = (
    ...     select(User)
    ...     .from_statement(union_stmt)
    ...     .options(with_expression(User.book_count, union_stmt.selected_columns.book_count))
    ... )
    >>> for user in session.scalars(orm_stmt):
    ...     print(f"Username: {user.name}  Number of books: {user.book_count}")
    {execsql}SELECT user_account.id, user_account.name, user_account.fullname, count(book.id) AS book_count
    FROM user_account JOIN book ON user_account.id = book.owner_id
    WHERE user_account.name = ?
    UNION ALL
    SELECT user_account.id, user_account.name, user_account.fullname, count(book.id) AS book_count
    FROM user_account JOIN book ON user_account.id = book.owner_id
    WHERE user_account.name = ?
    [...] ('spongebob', 'sandy'){stop}
    Username: spongebob  Number of books: 3
    Username: sandy  Number of books: 3



Column Loading API
-------------------

.. autofunction:: defer

.. autofunction:: deferred

.. autofunction:: query_expression

.. autofunction:: load_only

.. autofunction:: undefer

.. autofunction:: undefer_group

.. autofunction:: with_expression

.. comment

  >>> session.close()
  >>> conn.close()
  ROLLBACK...

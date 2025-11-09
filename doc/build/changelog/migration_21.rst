.. _whatsnew_21_toplevel:

=============================
What's New in SQLAlchemy 2.1?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 2.0 and
    version 2.1.


Introduction
============

This guide introduces what's new in SQLAlchemy version 2.1
and also documents changes which affect users migrating
their applications from the 2.0 series of SQLAlchemy to 2.1.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

General
=======

.. _change_10197:

Asyncio "greenlet" dependency no longer installs by default
------------------------------------------------------------

SQLAlchemy 1.4 and 2.0 used a complex expression to determine if the
``greenlet`` dependency, needed by the :ref:`asyncio <asyncio_toplevel>`
extension, could be installed from pypi using a pre-built wheel instead
of having to build from source.   This because the source build of ``greenlet``
is not always trivial on some platforms.

Disadvantages to this approach included that SQLAlchemy needed to track
exactly which versions of ``greenlet`` were published as wheels on pypi;
the setup expression led to problems with some package management tools
such as ``poetry``; it was not possible to install SQLAlchemy **without**
``greenlet`` being installed, even though this is completely feasible
if the asyncio extension is not used.

These problems are all solved by keeping ``greenlet`` entirely within the
``[asyncio]`` target.  The only downside is that users of the asyncio extension
need to be aware of this extra installation dependency.

:ticket:`10197`

New Features and Improvements - ORM
====================================



.. _change_9809:

Session autoflush behavior simplified to be unconditional
---------------------------------------------------------

Session autoflush behavior has been simplified to unconditionally flush the
session each time an execution takes place, regardless of whether an ORM
statement or Core statement is being executed. This change eliminates the
previous conditional logic that only flushed when ORM-related statements
were detected.

Previously, the session would only autoflush when executing ORM queries::

    # 2.0 behavior - autoflush only occurred for ORM statements
    session.add(User(name="new user"))

    # This would trigger autoflush
    users = session.execute(select(User)).scalars().all()

    # This would NOT trigger autoflush
    result = session.execute(text("SELECT * FROM users"))

In 2.1, autoflush occurs for all statement executions::

    # 2.1 behavior - autoflush occurs for all executions
    session.add(User(name="new user"))

    # Both of these now trigger autoflush
    users = session.execute(select(User)).scalars().all()
    result = session.execute(text("SELECT * FROM users"))

This change provides more consistent and predictable session behavior across
all types of SQL execution.

:ticket:`9809`


.. _change_10050:

ORM Relationship allows callable for back_populates
---------------------------------------------------

To help produce code that is more amenable to IDE-level linting and type
checking, the :paramref:`_orm.relationship.back_populates` parameter now
accepts both direct references to a class-bound attribute as well as
lambdas which do the same::

    class A(Base):
        __tablename__ = "a"

        id: Mapped[int] = mapped_column(primary_key=True)

        # use a lambda: to link to B.a directly when it exists
        bs: Mapped[list[B]] = relationship(back_populates=lambda: B.a)


    class B(Base):
        __tablename__ = "b"
        id: Mapped[int] = mapped_column(primary_key=True)
        a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        # A.bs already exists, so can link directly
        a: Mapped[A] = relationship(back_populates=A.bs)

:ticket:`10050`

.. _change_12168:

ORM Mapped Dataclasses no longer populate implicit ``default``, collection-based ``default_factory`` in ``__dict__``
--------------------------------------------------------------------------------------------------------------------

This behavioral change addresses a widely reported issue with SQLAlchemy's
:ref:`orm_declarative_native_dataclasses` feature that was introduced in 2.0.
SQLAlchemy ORM has always featured a behavior where a particular attribute on
an ORM mapped class will have different behaviors depending on if it has an
actively set value, including if that value is ``None``, versus if the
attribute is not set at all.  When Declarative Dataclass Mapping was introduced, the
:paramref:`_orm.mapped_column.default` parameter introduced a new capability
which is to set up a dataclass-level default to be present in the generated
``__init__`` method. This had the unfortunate side effect of breaking various
popular workflows, the most prominent of which is creating an ORM object with
the foreign key value in lieu of a many-to-one reference::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class Parent(Base):
        __tablename__ = "parent"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

        related_id: Mapped[int | None] = mapped_column(ForeignKey("child.id"), default=None)
        related: Mapped[Child | None] = relationship(default=None)


    class Child(Base):
        __tablename__ = "child"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

In the above mapping, the ``__init__`` method generated for ``Parent``
would in Python code look like this::


    def __init__(self, related_id=None, related=None): ...

This means that creating a new ``Parent`` with ``related_id`` only would populate
both ``related_id`` and ``related`` in ``__dict__``::

    # 2.0 behavior; will INSERT NULL for related_id due to the presence
    # of related=None
    >>> p1 = Parent(related_id=5)
    >>> p1.__dict__
    {'related_id': 5, 'related': None, '_sa_instance_state': ...}

The ``None`` value for ``'related'`` means that SQLAlchemy favors the non-present
related ``Child`` over the present value for ``'related_id'``, which would be
discarded, and ``NULL`` would be inserted for ``'related_id'`` instead.

In the new behavior, the ``__init__`` method instead looks like the example below,
using a special constant ``DONT_SET`` indicating a non-present value for ``'related'``
should be ignored.  This allows the class to behave more closely to how
SQLAlchemy ORM mapped classes traditionally operate::

    def __init__(self, related_id=DONT_SET, related=DONT_SET): ...

We then get a ``__dict__`` setup that will follow the expected behavior of
omitting ``related`` from ``__dict__`` and later running an INSERT with
``related_id=5``::

    # 2.1 behavior; will INSERT 5 for related_id
    >>> p1 = Parent(related_id=5)
    >>> p1.__dict__
    {'related_id': 5, '_sa_instance_state': ...}

Dataclass defaults are delivered via descriptor instead of __dict__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above behavior goes a step further, which is that in order to
honor default values that are something other than ``None``, the value of the
dataclass-level default (i.e. set using any of the
:paramref:`_orm.mapped_column.default`,
:paramref:`_orm.column_property.default`, or :paramref:`_orm.deferred.default`
parameters) is directed to be delivered at the
Python :term:`descriptor` level using mechanisms in SQLAlchemy's attribute
system that normally return ``None`` for un-popualted columns, so that even though the default is not
populated into ``__dict__``, it's still delivered when the attribute is
accessed.  This behavior is based on what Python dataclasses itself does
when a default is indicated for a field that also includes ``init=False``.

In the example below, an immutable default ``"default_status"``
is applied to a column called ``status``::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class SomeObject(Base):
        __tablename__ = "parent"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

        status: Mapped[str] = mapped_column(default="default_status")

In the above mapping, constructing ``SomeObject`` with no parameters will
deliver no values inside of ``__dict__``, but will deliver the default
value via descriptor::

    # object is constructed with no value for ``status``
    >>> s1 = SomeObject()

    # the default value is not placed in ``__dict__``
    >>> s1.__dict__
    {'_sa_instance_state': ...}

    # but the default value is delivered at the object level via descriptor
    >>> s1.status
    'default_status'

    # the value still remains unpopulated in ``__dict__``
    >>> s1.__dict__
    {'_sa_instance_state': ...}

The value passed
as :paramref:`_orm.mapped_column.default` is also assigned as was the
case before to the :paramref:`_schema.Column.default` parameter of the
underlying :class:`_schema.Column`, where it takes
place as a Python-level default for INSERT statements.  So while ``__dict__``
is never populated with the default value on the object, the INSERT
still includes the value in the parameter set.  This essentially modifies
the Declarative Dataclass Mapping system to work more like traditional
ORM mapped classes, where a "default" means just that, a column level
default.

Dataclass defaults are accessible on objects even without init
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As the new behavior makes use of descriptors in a similar way as Python
dataclasses do themselves when ``init=False``, the new feature implements
this behavior as well.   This is an all new behavior where an ORM mapped
class can deliver a default value for fields even if they are not part of
the ``__init__()`` method at all.  In the mapping below, the ``status``
field is configured with ``init=False``, meaning it's not part of the
constructor at all::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class SomeObject(Base):
        __tablename__ = "parent"
        id: Mapped[int] = mapped_column(primary_key=True, init=False)
        status: Mapped[str] = mapped_column(default="default_status", init=False)

When we construct ``SomeObject()`` with no arguments, the default is accessible
on the instance, delivered via descriptor::

    >>> so = SomeObject()
    >>> so.status
    default_status

default_factory for collection-based relationships internally uses DONT_SET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A late add to the behavioral change brings equivalent behavior to the
use of the :paramref:`_orm.relationship.default_factory` parameter with
collection-based relationships.   This attribute is `documented <orm_declarative_dc_relationships>`
as being limited to exactly the collection class that's stated on the left side
of the annotation, which is now enforced at mapper configuration time::

    class Parent(Base):
        __tablename__ = "parents"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)
        name: Mapped[str]

        children: Mapped[list["Child"]] = relationship(default_factory=list)

With the above mapping, the actual
:paramref:`_orm.relationship.default_factory` parameter is replaced internally
to instead use the same ``DONT_SET`` constant that's applied to
:paramref:`_orm.relationship.default` for many-to-one relationships.
SQLAlchemy's existing collection-on-attribute access behavior occurs as always
on access::

    >>> p1 = Parent(name="p1")
    >>> p1.children
    []

This change to :paramref:`_orm.relationship.default_factory` accommodates a
similar merge-based condition where an empty collection would be forced into
a new object that in fact wants a merged collection to arrive.


Related Changes
^^^^^^^^^^^^^^^

This change includes the following API changes:

* The :paramref:`_orm.relationship.default` parameter, when present, only
  accepts a value of ``None``, and is only accepted when the relationship is
  ultimately a many-to-one relationship or one that establishes
  :paramref:`_orm.relationship.uselist` as ``False``.
* The :paramref:`_orm.mapped_column.default` and :paramref:`_orm.mapped_column.insert_default`
  parameters are mutually exclusive, and only one may be passed at a time.
  The behavior of the two parameters is equivalent at the :class:`_schema.Column`
  level, however at the Declarative Dataclass Mapping level, only
  :paramref:`_orm.mapped_column.default` actually sets the dataclass-level
  default with descriptor access; using :paramref:`_orm.mapped_column.insert_default`
  will have the effect of the object attribute defaulting to ``None`` on the
  instance until the INSERT takes place, in the same way it works on traditional
  ORM mapped classes.

:ticket:`12168`

.. _change_12570:

New rules for None-return for ORM Composites
--------------------------------------------

ORM composite attributes configured using :func:`_orm.composite` can now
specify whether or not they should return ``None`` using a new parameter
:paramref:`_orm.composite.return_none_on`.   By default, a composite
attribute now returns a non-None object in all cases, whereas previously
under 2.0, a ``None`` value would be returned for a pending object with
``None`` values for all composite columns.

Given a composite mapping::

    import dataclasses


    @dataclasses.dataclass
    class Point:
        x: int | None
        y: int | None


    class Base(DeclarativeBase):
        pass


    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)

        start: Mapped[Point] = composite(mapped_column("x1"), mapped_column("y1"))
        end: Mapped[Point] = composite(mapped_column("x2"), mapped_column("y2"))

When constructing a pending ``Vertex`` object, the initial value of the
``x1``, ``y1``, ``x2``, ``y2`` columns is ``None``.   Under version 2.0,
accessing the composite at this stage would automatically return ``None``::

    >>> v1 = Vertex()
    >>> v1.start
    None

Under 2.1, the default behavior is to return the composite class with attributes
set to ``None``::

    >>> v1 = Vertex()
    >>> v1.start
    Point(x=None, y=None)

This behavior is now consistent with other forms of access, such as accessing
the attribute from a persistent object as well as querying for the attribute
directly.  It is also consistent with the mapped annotation ``Mapped[Point]``.

The behavior can be further controlled by applying the
:paramref:`_orm.composite.return_none_on` parameter, which accepts a callable
that returns True if the composite should be returned as None, given the
arguments that would normally be passed to the composite class.  The typical callable
here would return True (i.e. the value should be ``None``) for the case where all
columns are ``None``::

    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)

        start: Mapped[Point] = composite(
            mapped_column("x1"),
            mapped_column("y1"),
            return_none_on=lambda x, y: x is None and y is None,
        )
        end: Mapped[Point] = composite(
            mapped_column("x2"),
            mapped_column("y2"),
            return_none_on=lambda x, y: x is None and y is None,
        )

For the above class, any ``Vertex`` instance whether pending or persistent will
return ``None`` for ``start`` and ``end`` if both composite columns for the attribute
are ``None``::

    >>> v1 = Vertex()
    >>> v1.start
    None

The :paramref:`_orm.composite.return_none_on` parameter is also set
automatically, if not otherwise set explicitly, when using
:ref:`orm_declarative_mapped_column`; setting the left hand side to
``Optional`` or ``| None`` will assign the above ``None``-handling callable::


    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)

        # will apply return_none_on=lambda *args: all(arg is None for arg in args)
        start: Mapped[Point | None] = composite(mapped_column("x1"), mapped_column("y1"))
        end: Mapped[Point | None] = composite(mapped_column("x2"), mapped_column("y2"))

The above object will return ``None`` for ``start`` and ``end`` automatically
if the columns are also None::

    >>> session.scalars(
    ...     select(Vertex.start).where(Vertex.x1 == None, Vertex.y1 == None)
    ... ).first()
    None

If :paramref:`_orm.composite.return_none_on` is set explicitly, that value will
supersede the choice made by ORM Annotated Declarative.   This includes that
the parameter may be explicitly set to ``None`` which will disable the ORM
Annotated Declarative setting from taking place.

:ticket:`12570`

.. _change_9832:

New RegistryEvents System for ORM Mapping Customization
--------------------------------------------------------

SQLAlchemy 2.1 introduces :class:`.RegistryEvents`, providing for event
hooks that are specific to a :class:`_orm.registry`.  These events include
:meth:`_orm.RegistryEvents.before_configured` and :meth:`_orm.RegistryEvents.after_configured`
to complement the same-named events that can be established on a
:class:`_orm.Mapper`, as well as :meth:`_orm.RegistryEvents.resolve_type_annotation`
that allows programmatic access to the ORM Annotated Declarative type resolution
process.  Examples are provided illustrating how to define resolution schemes
for any kind of type hierarchy in an automated fashion, including :pep:`695`
type aliases.

E.g.::

    from typing import Any

    from sqlalchemy import event
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import registry as RegistryType
    from sqlalchemy.orm import TypeResolve
    from sqlalchemy.types import TypeEngine


    class Base(DeclarativeBase):
        pass


    @event.listens_for(Base, "resolve_type_annotation")
    def resolve_custom_type(resolve_type: TypeResolve) -> TypeEngine[Any] | None:
        if resolve_type.resolved_type is MyCustomType:
            return MyCustomSQLType()
        else:
            return None


    @event.listens_for(Base, "after_configured")
    def after_base_configured(registry: RegistryType) -> None:
        print(f"Registry {registry} fully configured")

.. seealso::

    :ref:`orm_declarative_resolve_type_event` - Complete documentation on using
    the :meth:`.RegistryEvents.resolve_type_annotation` event

    :class:`.RegistryEvents` - Complete API reference for all registry events

:ticket:`9832`

New Features and Improvements - Core
=====================================


.. _change_10635:

``Row`` now represents individual column types directly without ``Tuple``
--------------------------------------------------------------------------

SQLAlchemy 2.0 implemented a broad array of :pep:`484` typing throughout
all components, including a new ability for row-returning statements such
as :func:`_sql.select` to maintain track of individual column types, which
were then passed through the execution phase onto the :class:`_engine.Result`
object and then to the individual :class:`_engine.Row` objects.   Described
at :ref:`change_result_typing_20`, this approach solved several issues
with statement / row typing, but some remained unsolvable.  In 2.1, one
of those issues, that the individual column types needed to be packaged
into a ``typing.Tuple``, is now resolved using new :pep:`646` integration,
which allows for tuple-like types that are not actually typed as ``Tuple``.

In SQLAlchemy 2.0, a statement such as::

    stmt = select(column("x", Integer), column("y", String))

Would be typed as::

    Select[Tuple[int, str]]

In 2.1, it's now typed as::

    Select[int, str]

When executing ``stmt``, the :class:`_engine.Result` and :class:`_engine.Row`
objects will be typed as ``Result[int, str]`` and ``Row[int, str]``, respectively.
The prior workaround using :attr:`_engine.Row._t` to type as a real ``Tuple``
is no longer needed and projects can migrate off this pattern.

Mypy users will need to make use of **Mypy 1.7 or greater** for pep-646
integration to be available.

Limitations
^^^^^^^^^^^

Not yet solved by pep-646 or any other pep is the ability for an arbitrary
number of expressions within :class:`_sql.Select` and others to be mapped to
row objects, without stating each argument position explicitly within typing
annotations.   To work around this issue, SQLAlchemy makes use of automated
"stub generation" tools to generate hardcoded mappings of different numbers of
positional arguments to constructs like :func:`_sql.select` to resolve to
individual ``Unpack[]`` expressions (in SQLAlchemy 2.0, this generation
produced ``Tuple[]`` annotations instead).  This means that there are arbitrary
limits on how many specific column expressions will be typed within the
:class:`_engine.Row` object, without restoring to ``Any`` for remaining
expressions; for :func:`_sql.select`, it's currently ten expressions, and
for DML expressions like :func:`_dml.insert` that use :meth:`_dml.Insert.returning`,
it's eight.    If and when a new pep that provides a ``Map`` operator
to pep-646 is proposed, this limitation can be lifted. [1]_  Originally, it was
mistakenly assumed that this limitation prevented pep-646 from being usable at all,
however, the ``Unpack`` construct does in fact replace everything that
was done using ``Tuple`` in 2.0.

An additional limitation for which there is no proposed solution is that
there's no way for the name-based attributes on :class:`_engine.Row` to be
automatically typed, so these continue to be typed as ``Any`` (e.g. ``row.x``
and ``row.y`` for the above example).   With current language features,
this could only be fixed by having an explicit class-based construct that
allows one to compose an explicit :class:`_engine.Row` with explicit fields
up front, which would be verbose and not automatic.

.. [1] https://github.com/python/typing/discussions/1001#discussioncomment-1897813

:ticket:`10635`


.. _change_11234:

URL stringify and parse now supports URL escaping for the "database" portion
----------------------------------------------------------------------------

A URL that includes URL-escaped characters in the database portion will
now parse with conversion of those escaped characters::

    >>> from sqlalchemy import make_url
    >>> u = make_url("driver://user:pass@host/database%3Fname")
    >>> u.database
    'database?name'

Previously, such characters would not be unescaped::

    >>> # pre-2.1 behavior
    >>> from sqlalchemy import make_url
    >>> u = make_url("driver://user:pass@host/database%3Fname")
    >>> u.database
    'database%3Fname'

This change also applies to the stringify side; most special characters in
the database name will be URL escaped, omitting a few such as plus signs and
slashes::

    >>> from sqlalchemy import URL
    >>> u = URL.create("driver", database="a?b=c")
    >>> str(u)
    'driver:///a%3Fb%3Dc'

Where the above URL correctly round-trips to itself::

    >>> make_url(str(u))
    driver:///a%3Fb%3Dc
    >>> make_url(str(u)).database == u.database
    True


Whereas previously, special characters applied programmatically would not
be escaped in the result, leading to a URL that does not represent the
original database portion.  Below, `b=c` is part of the query string and
not the database portion::

    >>> from sqlalchemy import URL
    >>> u = URL.create("driver", database="a?b=c")
    >>> str(u)
    'driver:///a?b=c'

:ticket:`11234`

.. _change_4950:

CREATE TABLE AS SELECT Support
-------------------------------

SQLAlchemy 2.1 adds support for the SQL ``CREATE TABLE ... AS SELECT``
construct as well as the ``SELECT ... INTO`` variant for selected backends,
which creates a new table directly from the results of a SELECT
statement. This is available via the new :class:`_schema.CreateTableAs` DDL
construct and the :meth:`_sql.SelectBase.into` convenience method.

The :class:`_schema.CreateTableAs` construct can be used to create a new table
from any SELECT statement::

    >>> from sqlalchemy import select, CreateTableAs
    >>> select_stmt = select(users.c.id, users.c.name).where(users.c.status == "active")
    >>> create_table_as = CreateTableAs(select_stmt, "active_users")

The above construct renders as a ``CREATE TABLE AS`` statement::

    >>> print(create_table_as)
    CREATE TABLE active_users AS SELECT users.id, users.name
    FROM users
    WHERE users.status = 'active'

The construct can be executed to emit the above DDL, and the table may then
be accessed using the :attr:`.CreateTableAs.table` attribute which
supplies a :class:`.Table`::

    >>> print(select(create_table_as.table))
    SELECT users.id, users.name
    FROM active_users

See :ref:`tutorial_create_table_as` for a tutorial.

.. seealso::

    :ref:`tutorial_create_table_as` - in the :ref:`unified_tutorial`

    :class:`_schema.CreateTableAs` - DDL construct for CREATE TABLE AS

    :meth:`_sql.SelectBase.into` - convenience method on SELECT and UNION
    statements

:ticket:`4950`


.. _change_12496:

New Hybrid DML hook features
----------------------------

To complement the existing :meth:`.hybrid_property.update_expression` decorator,
a new decorator :meth:`.hybrid_property.bulk_dml` is added, which works
specifically with parameter dictionaries passed to :meth:`_orm.Session.execute`
when dealing with ORM-enabled :func:`_dml.insert` or :func:`_dml.update`::

    from typing import MutableMapping
    from dataclasses import dataclass


    @dataclass
    class Point:
        x: int
        y: int


    class Location(Base):
        __tablename__ = "location"

        id: Mapped[int] = mapped_column(primary_key=True)
        x: Mapped[int]
        y: Mapped[int]

        @hybrid_property
        def coordinates(self) -> Point:
            return Point(self.x, self.y)

        @coordinates.inplace.bulk_dml
        @classmethod
        def _coordinates_bulk_dml(
            cls, mapping: MutableMapping[str, Any], value: Point
        ) -> None:
            mapping["x"] = value.x
            mapping["y"] = value.y

Additionally, a new helper :func:`_sql.from_dml_column` is added, which may be
used with the :meth:`.hybrid_property.update_expression` hook to indicate
re-use of a column expression from elsewhere in the UPDATE statement's SET
clause::

    from sqlalchemy import from_dml_column


    class Product(Base):
        __tablename__ = "product"

        id: Mapped[int] = mapped_column(primary_key=True)
        price: Mapped[float]
        tax_rate: Mapped[float]

        @hybrid_property
        def total_price(self) -> float:
            return self.price * (1 + self.tax_rate)

        @total_price.inplace.update_expression
        @classmethod
        def _total_price_update_expression(cls, value: Any) -> List[Tuple[Any, Any]]:
            return [(cls.price, value / (1 + from_dml_column(cls.tax_rate)))]

In the above example, if the ``tax_rate`` column is also indicated in the
SET clause of the UPDATE, that expression will be used for the ``total_price``
expression rather than making use of the previous value of the ``tax_rate``
column:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import update
    >>> print(update(Product).values({Product.tax_rate: 0.08, Product.total_price: 125.00}))
    {printsql}UPDATE product SET tax_rate=:tax_rate, price=(:param_1 / (:tax_rate + :param_2))

When the target column is omitted, :func:`_sql.from_dml_column` falls back to
using the original column expression:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import update
    >>> print(update(Product).values({Product.total_price: 125.00}))
    {printsql}UPDATE product SET price=(:param_1 / (tax_rate + :param_2))


.. seealso::

    :ref:`hybrid_bulk_update`

:ticket:`12496`

.. _change_12736:

Operator classes added to validate operator usage with datatypes
----------------------------------------------------------------

SQLAlchemy 2.1 introduces a new "operator classes" system that provides
validation when SQL operators are used with specific datatypes. This feature
helps catch usage of operators that are not appropriate for a given datatype
during the initial construction of expression objects. A simple example is an
integer or numeric column used with a "string match" operator. When an
incompatible operation is used, a deprecation warning is emitted; in a future
major release this will raise :class:`.InvalidRequestError`.

The initial motivation for this new system is to revise the use of the
:meth:`.ColumnOperators.contains` method when used with :class:`_types.JSON` columns.
The :meth:`.ColumnOperators.contains` method in the case of the :class:`_types.JSON`
datatype makes use of the string-oriented version of the method, that
assumes string data and uses LIKE to match substrings.  This is not compatible
with the same-named method that is defined by the PostgreSQL
:class:`_postgresql.JSONB` type, which uses PostgreSQL's native JSONB containment
operators. Because :class:`_types.JSON` data is normally stored as a plain string,
:meth:`.ColumnOperators.contains` would "work", and even in trivial cases
behave similarly to that of :class:`_postgresql.JSONB`. However, since the two
operations are not actually compatible at all, this mis-use can easily lead to
unexpected inconsistencies.

Code that uses :meth:`.ColumnOperators.contains` with :class:`_types.JSON` columns will
now emit a deprecation warning::

    from sqlalchemy import JSON, select, Column
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


    class Base(DeclarativeBase):
        pass


    class MyTable(Base):
        __tablename__ = "my_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        json_column: Mapped[dict] = mapped_column(JSON)


    # This will now emit a deprecation warning
    select(MyTable).filter(MyTable.json_column.contains("some_value"))

Above, using :meth:`.ColumnOperators.contains` with :class:`_types.JSON` columns
is considered to be inappropriate, since :meth:`.ColumnOperators.contains`
works as a simple string search without any awareness of JSON structuring.
To explicitly indicate that the JSON data should be searched as a string
using LIKE, the
column should first be cast (using either :func:`_sql.cast` for a full CAST,
or :func:`_sql.type_coerce` for a Python-side cast) to :class:`.String`::

    from sqlalchemy import type_coerce, String

    # Explicit string-based matching
    select(MyTable).filter(type_coerce(MyTable.json_column, String).contains("some_value"))

This change forces code to distinguish between using string-based "contains"
with a :class:`_types.JSON` column and using PostgreSQL's JSONB containment
operator with :class:`_postgresql.JSONB` columns as separate, explicitly-stated operations.

The operator class system involves a mapping of SQLAlchemy operators listed
out in :mod:`sqlalchemy.sql.operators` to operator class combinations that come
from the :class:`.OperatorClass` enumeration, which are reconciled at
expression construction time with datatypes using the
:attr:`.TypeEngine.operator_classes` attribute.  A custom user defined type
may want to set this attribute to indicate the kinds of operators that make
sense::

    from sqlalchemy.types import UserDefinedType
    from sqlalchemy.sql.sqltypes import OperatorClass


    class ComplexNumber(UserDefinedType):
        operator_classes = OperatorClass.MATH

The above ``ComplexNumber`` datatype would then validate that operators
used are included in the "math" operator class.   By default, user defined
types made with :class:`.UserDefinedType` are left open to accept all
operators by default, whereas classes defined with :class:`.TypeDecorator`
will make use of the operator classes declared by the "impl" type.

.. seealso::

    :paramref:`.Operators.op.operator_class` - define an operator class when creating custom operators

    :class:`.OperatorClass`

:ticket:`12736`

.. _change_12596:

Non-integer RANGE window frame clauses now supported
-----------------------------------------------------

The :func:`_sql.over` clause now supports non-integer values in the
:paramref:`_sql.over.range_` parameter through the new :class:`_sql.FrameClause`
construct. Previously, only integer values were allowed in RANGE clauses, which
limited their use to integer-based ordering columns.

With this change, applications can now use RANGE with other data types such
as floating-point numbers, dates, and intervals. The new :class:`_sql.FrameClause`
construct provides explicit control over frame boundaries using the
:class:`_sql.FrameClauseType` enum::

    from datetime import timedelta
    from sqlalchemy import FrameClause, FrameClauseType

    # Example: date-based RANGE with a 7-day window
    func.sum(my_table.c.amount).over(
        order_by=my_table.c.date,
        range_=FrameClause(
            start=timedelta(days=7),
            end=None,
            start_frame_type=FrameClauseType.PRECEDING,
            end_frame_type=FrameClauseType.CURRENT,
        ),
    )

For backwards compatibility, the traditional tuple-based syntax continues to
work with integer values::

    # This continues to work unchanged
    func.row_number().over(order_by=table.c.col, range_=(None, 10))

However, attempting to use non-integer values in the tuple syntax will now
raise an error, directing users to use :class:`_sql.FrameClause` instead.


:ticket:`12596`


PostgreSQL
==========

.. _change_10594_postgresql:

Changes to Named Type Handling in PostgreSQL
---------------------------------------------

Named types such as :class:`_postgresql.ENUM`, :class:`_postgresql.DOMAIN` and
the dialect-agnostic :class:`._types.Enum` have undergone behavioral changes in
SQLAlchemy 2.1 to better align with how a distinct type object that may
be shared among tables works in practice.

Named Types are Now Associated with MetaData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Named types are now more strongly associated with the :class:`_schema.MetaData`
at the top of the table hierarchy and are de-associated with any particular
:class:`_schema.Table` they may be a part of. This better represents how
PostgreSQL named types exist independently of any particular table, and that
they may be used across many tables simultaneously.

:class:`_types.Enum` and :class:`_postgresql.DOMAIN` now have their
:attr:`~_types.SchemaType.metadata` attribute set as soon as they are
associated with a table, and no longer refer to the :class:`_schema.Table`
or tables they are within (a table of course still refers to the named types
that it uses).

Schema Inheritance from MetaData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Named types will now "inherit" the schema of the :class:`_schema.MetaData`
by default. For example, ``MetaData(schema="myschema")`` will cause all
:class:`_types.Enum` and :class:`_postgresql.DOMAIN` to use the schema
"myschema"::

    metadata = MetaData(schema="myschema")

    table = Table(
        "mytable",
        metadata,
        Column("status", Enum("active", "inactive", name="status_enum")),
    )

    # The enum will be created as "myschema.status_enum"

To have named types use the schema name of an immediate :class:`_schema.Table`
that they are associated with, set the :paramref:`~_types.SchemaType.schema`
parameter of the type to be that same schema name::

    table = Table(
        "mytable",
        metadata,
        Column(
            "status", Enum("active", "inactive", name="status_enum", schema="tableschema")
        ),
        schema="tableschema",
    )

The :paramref:`_types.SchemaType.inherit_schema` parameter remains available
for this release but is deprecated for eventual removal, and will emit a
deprecation warning when used.

Modified Create and Drop Behavior
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The rules by which named types are created and dropped are also modified to
flow more in terms of a :class:`_schema.MetaData`:

1. :meth:`_schema.MetaData.create_all` and :meth:`_schema.Table.create` will
   create any named types needed
2. :meth:`_schema.Table.drop` will not drop any named types
3. :meth:`_schema.MetaData.drop_all` will drop named types after all tables
   are dropped

Refined CheckFirst Behavior
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is also newly refined "checkfirst" behavior. A new enumeration
:class:`_schema.CheckFirst` is introduced which allows fine-grained control
within :meth:`_schema.MetaData.create_all`, :meth:`_schema.MetaData.drop_all`,
:meth:`_schema.Table.create`, and :meth:`_schema.Table.drop` as to what "check"
queries are emitted, allowing tests for types, sequences etc. to be included
or not::

    from sqlalchemy import CheckFirst

    # Only check for table existence, skip type checks
    metadata.create_all(engine, checkfirst=CheckFirst.TABLES)

    # Check for both tables and types
    metadata.create_all(engine, checkfirst=CheckFirst.TABLES | CheckFirst.TYPES)

inherit_schema is Deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because named types now inherit the schema of :class:`.MetaData` automatically
and remain agnostic of what :class:`.Table` objects refer to them, the
:paramref:`_types.Enum.inherit_schema` parameter is deprecated.  For 2.1
it still works the old way by associating the type with the parent
:class:`.Table`, however as this binds the type to a single :class:`.Table`
even though the type can be used against any number of tables, it's preferred
to set :paramref:`_types.Enum.schema` directly as desired when the schema
used by the :class:`.MetaData` is not what's desired.

:ticket:`10594`

.. _change_10556:

Addition of ``BitString`` subclass for handling postgresql ``BIT`` columns
--------------------------------------------------------------------------

Values of :class:`_postgresql.BIT` columns in the PostgreSQL dialect are
returned as instances of a new ``str`` subclass,
:class:`_postgresql.BitString`.  Previously, the value of :class:`_postgresql.BIT`
columns was driver dependent, with most drivers returning ``str`` instances
except ``asyncpg``, which used ``asyncpg.BitString``.

With this change, for the ``psycopg``, ``psycopg2``, and ``pg8000`` drivers,
the new :class:`_postgresql.BitString` type is mostly compatible with ``str``, but
adds methods for bit manipulation and supports bitwise operators.

As :class:`_postgresql.BitString` is a string subclass, hashability as well
as equality tests continue to work against plain strings.   This also leaves
ordering operators intact.

For implementations using the ``asyncpg`` driver, the new type is incompatible with
the existing ``asyncpg.BitString`` type.

:ticket:`10556`

.. _change_12948:

HSTORE subscripting now uses native PostgreSQL 14+ syntax
----------------------------------------------------------

When connected to PostgreSQL 14 or later, HSTORE column subscripting operations
now automatically use PostgreSQL's native subscript notation ``hstore_col['key']``
instead of the traditional arrow operator ``hstore_col -> 'key'``. This change
applies to both read and write operations and provides better compatibility with
PostgreSQL's native HSTORE subscripting feature introduced in version 14.

The change is similar to the equivalent change made for JSONB columns in
SQLAlchemy 2.0.42.   The change for HSTORE is kept in 2.1 to provide a longer
migration buffer for the issue of indexes which may refer to a subscript
function, which was unanticipated when the JSONB change was made.

For PostgreSQL versions prior to 14, SQLAlchemy continues to use the arrow
operator syntax automatically, ensuring backward compatibility.

Example of the new syntax when connected to PostgreSQL 14+::

    from sqlalchemy import table, column, update
    from sqlalchemy.dialects.postgresql import HSTORE

    data = table("data", column("h", HSTORE))

    # SELECT operations use subscript notation
    stmt = select(data.c.h["key"])
    # Renders as: SELECT data.h['key'] FROM data

    # UPDATE operations also use subscript notation
    stmt = update(data).values({data.c.h["status"]: "active"})
    # Renders as: UPDATE data SET h['status'] = 'active'

On PostgreSQL 13 and earlier, the same code automatically renders using the
arrow operator::

    # Same code on PostgreSQL 13 renders as:
    # SELECT data.h -> 'key' FROM data
    # UPDATE data SET h -> 'status' = 'active'

Impact on Existing Indexes
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If existing PostgreSQL 14+ databases have expression indexes on HSTORE
subscript operations, those indexes will need to be recreated to match
the new SQL syntax. The index definitions that used the arrow operator
syntax (``h -> 'key'``) will not match the new subscript syntax
(``h['key']``), which may cause index scans to not be used.

To update an existing index:

.. sourcecode:: sql

    -- Drop the old index
    DROP INDEX IF EXISTS idx_hstore_key;

    -- Create new index with subscript syntax
    CREATE INDEX idx_hstore_key ON my_table ((h['key']));

:ticket:`12948`


Microsoft SQL Server
====================

.. _change_11250:

Potential breaking change to odbc_connect= handling for mssql+pyodbc
--------------------------------------------------------------------

Fixed a mssql+pyodbc issue where valid plus signs in an already-unquoted
``odbc_connect=`` (raw DBAPI) connection string were replaced with spaces.

Previously, the pyodbc connector would always pass the odbc_connect value
to unquote_plus(), even if it was not required. So, if the (unquoted)
odbc_connect value contained ``PWD=pass+word`` that would get changed to
``PWD=pass word``, and the login would fail. One workaround was to quote
just the plus sign — ``PWD=pass%2Bword`` — which would then get unquoted
to ``PWD=pass+word``.

Implementations using the above workaround with :meth:`_engine.URL.create`
to specify a plus sign in the ``PWD=`` argument of an odbc_connect string
will have to remove the workaround and just pass the ``PWD=`` value as it
would appear in a valid ODBC connection string (i.e., the same as would be
required if using the connection string directly with ``pyodbc.connect()``).

:ticket:`11250`



.. currentmodule:: sqlalchemy.orm

.. _mapper_composite:

Composite Column Types
======================

Sets of columns can be associated with a single user-defined datatype,
which in modern use is normally a Python dataclass_. The ORM
provides a single attribute which represents the group of columns using the
class you provide.

A simple example represents pairs of :class:`_types.Integer` columns as a
``Point`` object, with attributes ``.x`` and ``.y``.   Using a
dataclass, these attributes are defined with the corresponding ``int``
Python type::

    import dataclasses


    @dataclasses.dataclass
    class Point:
        x: int
        y: int

Non-dataclass forms are also accepted, but require additional methods
to be implemented.  For an example using a non-dataclass class, see the section
:ref:`composite_legacy_no_dataclass`.

.. versionadded:: 2.0 The :func:`_orm.composite` construct fully supports
   Python dataclasses including the ability to derive mapped column datatypes
   from the composite class.

We will create a mapping to a table ``vertices``, which represents two points
as ``x1/y1`` and ``x2/y2``.   The ``Point`` class is associated with
the mapped columns using the :func:`_orm.composite` construct.

The example below illustrates the most modern form of :func:`_orm.composite` as
used with a fully
:ref:`Annotated Declarative Table <orm_declarative_mapped_column>`
configuration. :func:`_orm.mapped_column` constructs representing each column
are passed directly to :func:`_orm.composite`, indicating zero or more aspects
of the columns to be generated, in this case the names; the
:func:`_orm.composite` construct derives the column types (in this case
``int``, corresponding to :class:`_types.Integer`) from the dataclass directly::

    from sqlalchemy.orm import DeclarativeBase, Mapped
    from sqlalchemy.orm import composite, mapped_column


    class Base(DeclarativeBase):
        pass


    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)

        start: Mapped[Point] = composite(mapped_column("x1"), mapped_column("y1"))
        end: Mapped[Point] = composite(mapped_column("x2"), mapped_column("y2"))

        def __repr__(self):
            return f"Vertex(start={self.start}, end={self.end})"

The above mapping would correspond to a CREATE TABLE statement as:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.schema import CreateTable
    >>> print(CreateTable(Vertex.__table__))
    {printsql}CREATE TABLE vertices (
      id INTEGER NOT NULL,
      x1 INTEGER NOT NULL,
      y1 INTEGER NOT NULL,
      x2 INTEGER NOT NULL,
      y2 INTEGER NOT NULL,
      PRIMARY KEY (id)
    )


Working with Mapped Composite Column Types
-------------------------------------------

With a mapping as illustrated in the top section, we can work with the
``Vertex`` class, where the ``.start`` and ``.end`` attributes will
transparently refer to the columns referred towards by the ``Point`` class, as
well as with instances of the ``Vertex`` class, where the ``.start`` and
``.end`` attributes will refer to instances of the ``Point`` class. The ``x1``,
``y1``, ``x2``, and ``y2`` columns are handled transparently:

* **Persisting Point objects**

  We can create a ``Vertex`` object, assign ``Point`` objects as members,
  and they will be persisted as expected:

  .. sourcecode:: pycon+sql

    >>> v = Vertex(start=Point(3, 4), end=Point(5, 6))
    >>> session.add(v)
    >>> session.commit()
    {execsql}BEGIN (implicit)
    INSERT INTO vertices (x1, y1, x2, y2) VALUES (?, ?, ?, ?)
    [generated in ...] (3, 4, 5, 6)
    COMMIT

* **Selecting Point objects as columns**

  :func:`_orm.composite` will allow the ``Vertex.start`` and ``Vertex.end``
  attributes to behave like a single SQL expression to as much an extent
  as possible when using the ORM :class:`_orm.Session` (including the legacy
  :class:`_orm.Query` object) to select ``Point`` objects:

  .. sourcecode:: pycon+sql

    >>> stmt = select(Vertex.start, Vertex.end)
    >>> session.execute(stmt).all()
    {execsql}SELECT vertices.x1, vertices.y1, vertices.x2, vertices.y2
    FROM vertices
    [...] ()
    {stop}[(Point(x=3, y=4), Point(x=5, y=6))]

* **Comparing Point objects in SQL expressions**

  The ``Vertex.start`` and ``Vertex.end`` attributes may be used in
  WHERE criteria and similar, using ad-hoc ``Point`` objects for comparisons:

  .. sourcecode:: pycon+sql

    >>> stmt = select(Vertex).where(Vertex.start == Point(3, 4)).where(Vertex.end < Point(7, 8))
    >>> session.scalars(stmt).all()
    {execsql}SELECT vertices.id, vertices.x1, vertices.y1, vertices.x2, vertices.y2
    FROM vertices
    WHERE vertices.x1 = ? AND vertices.y1 = ? AND vertices.x2 < ? AND vertices.y2 < ?
    [...] (3, 4, 7, 8)
    {stop}[Vertex(Point(x=3, y=4), Point(x=5, y=6))]

  .. versionadded:: 2.0  :func:`_orm.composite` constructs now support
     "ordering" comparisons such as ``<``, ``>=``, and similar, in addition
     to the already-present support for ``==``, ``!=``.

  .. tip::  The "ordering" comparison above using the "less than" operator (``<``)
     as well as the "equality" comparison using ``==``, when used to generate
     SQL expressions, are implemented by the :class:`_orm.Composite.Comparator`
     class, and don't make use of the comparison methods on the composite class
     itself, e.g. the ``__lt__()`` or ``__eq__()`` methods. From this it
     follows that the ``Point`` dataclass above also need not implement the
     dataclasses ``order=True`` parameter for the above SQL operations to work.
     The section :ref:`composite_operations` contains background on how
     to customize the comparison operations.

* **Updating Point objects on Vertex Instances**

  By default, the ``Point`` object **must be replaced by a new object** for
  changes to be detected:

  .. sourcecode:: pycon+sql

    >>> v1 = session.scalars(select(Vertex)).one()
    {execsql}SELECT vertices.id, vertices.x1, vertices.y1, vertices.x2, vertices.y2
    FROM vertices
    [...] ()
    {stop}

    >>> v1.end = Point(x=10, y=14)
    >>> session.commit()
    {execsql}UPDATE vertices SET x2=?, y2=? WHERE vertices.id = ?
    [...] (10, 14, 1)
    COMMIT

  In order to allow in place changes on the composite object, the
  :ref:`mutable_toplevel` extension must be used.  See the section
  :ref:`mutable_composites` for examples.



.. _orm_composite_other_forms:

Other mapping forms for composites
----------------------------------

The :func:`_orm.composite` construct may be passed the relevant columns
using a :func:`_orm.mapped_column` construct, a :class:`_schema.Column`,
or the string name of an existing mapped column.   The following examples
illustrate an equvalent mapping as that of the main section above.

* Map columns directly, then pass to composite

  Here we pass the existing :func:`_orm.mapped_column` instances to the
  :func:`_orm.composite` construct, as in the non-annotated example below
  where we also pass the ``Point`` class as the first argument to
  :func:`_orm.composite`::

    from sqlalchemy import Integer
    from sqlalchemy.orm import mapped_column, composite


    class Vertex(Base):
        __tablename__ = "vertices"

        id = mapped_column(Integer, primary_key=True)
        x1 = mapped_column(Integer)
        y1 = mapped_column(Integer)
        x2 = mapped_column(Integer)
        y2 = mapped_column(Integer)

        start = composite(Point, x1, y1)
        end = composite(Point, x2, y2)

* Map columns directly, pass attribute names to composite

  We can write the same example above using more annotated forms where we have
  the option to pass attribute names to :func:`_orm.composite` instead of
  full column constructs::

    from sqlalchemy.orm import mapped_column, composite, Mapped


    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)
        x1: Mapped[int]
        y1: Mapped[int]
        x2: Mapped[int]
        y2: Mapped[int]

        start: Mapped[Point] = composite("x1", "y1")
        end: Mapped[Point] = composite("x2", "y2")

* Imperative mapping and imperative table

  When using :ref:`imperative table <orm_imperative_table_configuration>` or
  fully :ref:`imperative <orm_imperative_mapping>` mappings, we have access
  to :class:`_schema.Column` objects directly.  These may be passed to
  :func:`_orm.composite` as well, as in the imperative example below::

     mapper_registry.map_imperatively(
         Vertex,
         vertices_table,
         properties={
             "start": composite(Point, vertices_table.c.x1, vertices_table.c.y1),
             "end": composite(Point, vertices_table.c.x2, vertices_table.c.y2),
         },
     )

.. _composite_legacy_no_dataclass:

Using Legacy Non-Dataclasses
----------------------------


If not using a dataclass, the requirements for the custom datatype class are
that it have a constructor
which accepts positional arguments corresponding to its column format, and
also provides a method ``__composite_values__()`` which returns the state of
the object as a list or tuple, in order of its column-based attributes. It
also should supply adequate ``__eq__()`` and ``__ne__()`` methods which test
the equality of two instances.

To illustrate the equivalent ``Point`` class from the main section
not using a dataclass::

    class Point:
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def __composite_values__(self):
            return self.x, self.y

        def __repr__(self):
            return f"Point(x={self.x!r}, y={self.y!r})"

        def __eq__(self, other):
            return isinstance(other, Point) and other.x == self.x and other.y == self.y

        def __ne__(self, other):
            return not self.__eq__(other)

Usage with :func:`_orm.composite` then proceeds where the columns to be
associated with the ``Point`` class must also be declared with explicit
types, using one of the forms at :ref:`orm_composite_other_forms`.


Tracking In-Place Mutations on Composites
-----------------------------------------

In-place changes to an existing composite value are
not tracked automatically.  Instead, the composite class needs to provide
events to its parent object explicitly.   This task is largely automated
via the usage of the :class:`.MutableComposite` mixin, which uses events
to associate each user-defined composite object with all parent associations.
Please see the example in :ref:`mutable_composites`.

.. _composite_operations:

Redefining Comparison Operations for Composites
-----------------------------------------------

The "equals" comparison operation by default produces an AND of all
corresponding columns equated to one another. This can be changed using
the ``comparator_factory`` argument to :func:`.composite`, where we
specify a custom :class:`.CompositeProperty.Comparator` class
to define existing or new operations.
Below we illustrate the "greater than" operator, implementing
the same expression that the base "greater than" does::

    import dataclasses

    from sqlalchemy.orm import composite
    from sqlalchemy.orm import CompositeProperty
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.sql import and_


    @dataclasses.dataclass
    class Point:
        x: int
        y: int


    class PointComparator(CompositeProperty.Comparator):
        def __gt__(self, other):
            """redefine the 'greater than' operation"""

            return and_(
                *[
                    a > b
                    for a, b in zip(
                        self.__clause_element__().clauses,
                        dataclasses.astuple(other),
                    )
                ]
            )


    class Base(DeclarativeBase):
        pass


    class Vertex(Base):
        __tablename__ = "vertices"

        id: Mapped[int] = mapped_column(primary_key=True)

        start: Mapped[Point] = composite(
            mapped_column("x1"), mapped_column("y1"), comparator_factory=PointComparator
        )
        end: Mapped[Point] = composite(
            mapped_column("x2"), mapped_column("y2"), comparator_factory=PointComparator
        )

Since ``Point`` is a dataclass, we may make use of
``dataclasses.astuple()`` to get a tuple form of ``Point`` instances.

The custom comparator then returns the appropriate SQL expression:

.. sourcecode:: pycon+sql

  >>> print(Vertex.start > Point(5, 6))
  {printsql}vertices.x1 > :x1_1 AND vertices.y1 > :y1_1


Nesting Composites
-------------------

Composite objects can be defined to work in simple nested schemes, by
redefining behaviors within the composite class to work as desired, then
mapping the composite class to the full length of individual columns normally.
This requires that additional methods to move between the "nested" and
"flat" forms are defined.

Below we reorganize the ``Vertex`` class to itself be a composite object which
refers to ``Point`` objects. ``Vertex`` and ``Point`` can be dataclasses,
however we will add a custom construction method to ``Vertex`` that can be used
to create new ``Vertex`` objects given four column values, which will will
arbitrarily name ``_generate()`` and define as a classmethod so that we can
make new ``Vertex`` objects by passing values to the ``Vertex._generate()``
method.

We will also implement the ``__composite_values__()`` method, which is a fixed
name recognized by the :func:`_orm.composite` construct (introduced previously
at :ref:`composite_legacy_no_dataclass`) that indicates a standard way of
receiving the object as a flat tuple of column values, which in this case will
supersede the usual dataclass-oriented methodology.

With our custom ``_generate()`` constructor and
``__composite_values__()`` serializer method, we can now move between
a flat tuple of columns and ``Vertex`` objects that contain ``Point``
instances.   The ``Vertex._generate`` method is passed as the
first argument to the :func:`_orm.composite` construct as the source of new
``Vertex`` instances, and the ``__composite_values__()`` method will be
used implicitly by :func:`_orm.composite`.

For the purposes of the example, the ``Vertex`` composite is then mapped to a
class called ``HasVertex``, which is where the :class:`.Table` containing the
four source columns ultimately resides::

    from __future__ import annotations

    import dataclasses
    from typing import Any
    from typing import Tuple

    from sqlalchemy.orm import composite
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    @dataclasses.dataclass
    class Point:
        x: int
        y: int


    @dataclasses.dataclass
    class Vertex:
        start: Point
        end: Point

        @classmethod
        def _generate(cls, x1: int, y1: int, x2: int, y2: int) -> Vertex:
            """generate a Vertex from a row"""
            return Vertex(Point(x1, y1), Point(x2, y2))

        def __composite_values__(self) -> Tuple[Any, ...]:
            """generate a row from a Vertex"""
            return dataclasses.astuple(self.start) + dataclasses.astuple(self.end)


    class Base(DeclarativeBase):
        pass


    class HasVertex(Base):
        __tablename__ = "has_vertex"
        id: Mapped[int] = mapped_column(primary_key=True)
        x1: Mapped[int]
        y1: Mapped[int]
        x2: Mapped[int]
        y2: Mapped[int]

        vertex: Mapped[Vertex] = composite(Vertex._generate, "x1", "y1", "x2", "y2")

The above mapping can then be used in terms of ``HasVertex``, ``Vertex``, and
``Point``::

    hv = HasVertex(vertex=Vertex(Point(1, 2), Point(3, 4)))

    session.add(hv)
    session.commit()

    stmt = select(HasVertex).where(HasVertex.vertex == Vertex(Point(1, 2), Point(3, 4)))

    hv = session.scalars(stmt).first()
    print(hv.vertex.start)
    print(hv.vertex.end)

.. _dataclass: https://docs.python.org/3/library/dataclasses.html

Composite API
-------------

.. autofunction:: composite


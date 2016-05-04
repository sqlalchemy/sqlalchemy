.. module:: sqlalchemy.orm

.. _mapper_composite:

Composite Column Types
=======================

Sets of columns can be associated with a single user-defined datatype. The ORM
provides a single attribute which represents the group of columns using the
class you provide.

.. versionchanged:: 0.7
    Composites have been simplified such that
    they no longer "conceal" the underlying column based attributes.  Additionally,
    in-place mutation is no longer automatic; see the section below on
    enabling mutability to support tracking of in-place changes.

.. versionchanged:: 0.9
    Composites will return their object-form, rather than as individual columns,
    when used in a column-oriented :class:`.Query` construct.  See :ref:`migration_2824`.

A simple example represents pairs of columns as a ``Point`` object.
``Point`` represents such a pair as ``.x`` and ``.y``::

    class Point(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def __composite_values__(self):
            return self.x, self.y

        def __repr__(self):
            return "Point(x=%r, y=%r)" % (self.x, self.y)

        def __eq__(self, other):
            return isinstance(other, Point) and \
                other.x == self.x and \
                other.y == self.y

        def __ne__(self, other):
            return not self.__eq__(other)

The requirements for the custom datatype class are that it have a constructor
which accepts positional arguments corresponding to its column format, and
also provides a method ``__composite_values__()`` which returns the state of
the object as a list or tuple, in order of its column-based attributes. It
also should supply adequate ``__eq__()`` and ``__ne__()`` methods which test
the equality of two instances.

We will create a mapping to a table ``vertices``, which represents two points
as ``x1/y1`` and ``x2/y2``. These are created normally as :class:`.Column`
objects. Then, the :func:`.composite` function is used to assign new
attributes that will represent sets of columns via the ``Point`` class::

    from sqlalchemy import Column, Integer
    from sqlalchemy.orm import composite
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Vertex(Base):
        __tablename__ = 'vertices'

        id = Column(Integer, primary_key=True)
        x1 = Column(Integer)
        y1 = Column(Integer)
        x2 = Column(Integer)
        y2 = Column(Integer)

        start = composite(Point, x1, y1)
        end = composite(Point, x2, y2)

A classical mapping above would define each :func:`.composite`
against the existing table::

    mapper(Vertex, vertices_table, properties={
        'start':composite(Point, vertices_table.c.x1, vertices_table.c.y1),
        'end':composite(Point, vertices_table.c.x2, vertices_table.c.y2),
    })

We can now persist and use ``Vertex`` instances, as well as query for them,
using the ``.start`` and ``.end`` attributes against ad-hoc ``Point`` instances:

.. sourcecode:: python+sql

    >>> v = Vertex(start=Point(3, 4), end=Point(5, 6))
    >>> session.add(v)
    >>> q = session.query(Vertex).filter(Vertex.start == Point(3, 4))
    {sql}>>> print(q.first().start)
    BEGIN (implicit)
    INSERT INTO vertices (x1, y1, x2, y2) VALUES (?, ?, ?, ?)
    (3, 4, 5, 6)
    SELECT vertices.id AS vertices_id,
            vertices.x1 AS vertices_x1,
            vertices.y1 AS vertices_y1,
            vertices.x2 AS vertices_x2,
            vertices.y2 AS vertices_y2
    FROM vertices
    WHERE vertices.x1 = ? AND vertices.y1 = ?
     LIMIT ? OFFSET ?
    (3, 4, 1, 0)
    {stop}Point(x=3, y=4)

.. autofunction:: composite


Tracking In-Place Mutations on Composites
-----------------------------------------

In-place changes to an existing composite value are
not tracked automatically.  Instead, the composite class needs to provide
events to its parent object explicitly.   This task is largely automated
via the usage of the :class:`.MutableComposite` mixin, which uses events
to associate each user-defined composite object with all parent associations.
Please see the example in :ref:`mutable_composites`.

.. versionchanged:: 0.7
    In-place changes to an existing composite value are no longer
    tracked automatically; the functionality is superseded by the
    :class:`.MutableComposite` class.

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

    from sqlalchemy.orm.properties import CompositeProperty
    from sqlalchemy import sql

    class PointComparator(CompositeProperty.Comparator):
        def __gt__(self, other):
            """redefine the 'greater than' operation"""

            return sql.and_(*[a>b for a, b in
                              zip(self.__clause_element__().clauses,
                                  other.__composite_values__())])

    class Vertex(Base):
        ___tablename__ = 'vertices'

        id = Column(Integer, primary_key=True)
        x1 = Column(Integer)
        y1 = Column(Integer)
        x2 = Column(Integer)
        y2 = Column(Integer)

        start = composite(Point, x1, y1,
                            comparator_factory=PointComparator)
        end = composite(Point, x2, y2,
                            comparator_factory=PointComparator)


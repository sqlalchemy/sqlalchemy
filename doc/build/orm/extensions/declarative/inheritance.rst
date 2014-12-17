.. _declarative_inheritance:

Inheritance Configuration
=========================

Declarative supports all three forms of inheritance as intuitively
as possible.  The ``inherits`` mapper keyword argument is not needed
as declarative will determine this from the class itself.   The various
"polymorphic" keyword arguments are specified using ``__mapper_args__``.

Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Joined table inheritance is defined as a subclass that defines its own
table::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        id = Column(Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

Note that above, the ``Engineer.id`` attribute, since it shares the
same attribute name as the ``Person.id`` attribute, will in fact
represent the ``people.id`` and ``engineers.id`` columns together,
with the "Engineer.id" column taking precedence if queried directly.
To provide the ``Engineer`` class with an attribute that represents
only the ``engineers.id`` column, give it a different attribute name::

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        engineer_id = Column('id', Integer, ForeignKey('people.id'),
                                                    primary_key=True)
        primary_language = Column(String(50))


.. versionchanged:: 0.7 joined table inheritance favors the subclass
   column over that of the superclass, such as querying above
   for ``Engineer.id``.  Prior to 0.7 this was the reverse.

.. _declarative_single_table:

Single Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Single table inheritance is defined as a subclass that does not have
its own table; you just leave out the ``__table__`` and ``__tablename__``
attributes::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

When the above mappers are configured, the ``Person`` class is mapped
to the ``people`` table *before* the ``primary_language`` column is
defined, and this column will not be included in its own mapping.
When ``Engineer`` then defines the ``primary_language`` column, the
column is added to the ``people`` table so that it is included in the
mapping for ``Engineer`` and is also part of the table's full set of
columns.  Columns which are not mapped to ``Person`` are also excluded
from any other single or joined inheriting classes using the
``exclude_properties`` mapper argument.  Below, ``Manager`` will have
all the attributes of ``Person`` and ``Manager`` but *not* the
``primary_language`` attribute of ``Engineer``::

    class Manager(Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}
        golf_swing = Column(String(50))

The attribute exclusion logic is provided by the
``exclude_properties`` mapper argument, and declarative's default
behavior can be disabled by passing an explicit ``exclude_properties``
collection (empty or otherwise) to the ``__mapper_args__``.

Resolving Column Conflicts
^^^^^^^^^^^^^^^^^^^^^^^^^^

Note above that the ``primary_language`` and ``golf_swing`` columns
are "moved up" to be applied to ``Person.__table__``, as a result of their
declaration on a subclass that has no table of its own.   A tricky case
comes up when two subclasses want to specify *the same* column, as below::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        start_date = Column(DateTime)

    class Manager(Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}
        start_date = Column(DateTime)

Above, the ``start_date`` column declared on both ``Engineer`` and ``Manager``
will result in an error::

    sqlalchemy.exc.ArgumentError: Column 'start_date' on class
    <class '__main__.Manager'> conflicts with existing
    column 'people.start_date'

In a situation like this, Declarative can't be sure
of the intent, especially if the ``start_date`` columns had, for example,
different types.   A situation like this can be resolved by using
:class:`.declared_attr` to define the :class:`.Column` conditionally, taking
care to return the **existing column** via the parent ``__table__`` if it
already exists::

    from sqlalchemy.ext.declarative import declared_attr

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}

        @declared_attr
        def start_date(cls):
            "Start date column, if not present already."
            return Person.__table__.c.get('start_date', Column(DateTime))

    class Manager(Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}

        @declared_attr
        def start_date(cls):
            "Start date column, if not present already."
            return Person.__table__.c.get('start_date', Column(DateTime))

Above, when ``Manager`` is mapped, the ``start_date`` column is
already present on the ``Person`` class.  Declarative lets us return
that :class:`.Column` as a result in this case, where it knows to skip
re-assigning the same column. If the mapping is mis-configured such
that the ``start_date`` column is accidentally re-assigned to a
different table (such as, if we changed ``Manager`` to be joined
inheritance without fixing ``start_date``), an error is raised which
indicates an existing :class:`.Column` is trying to be re-assigned to
a different owning :class:`.Table`.

.. versionadded:: 0.8 :class:`.declared_attr` can be used on a non-mixin
   class, and the returned :class:`.Column` or other mapped attribute
   will be applied to the mapping as any other attribute.  Previously,
   the resulting attribute would be ignored, and also result in a warning
   being emitted when a subclass was created.

.. versionadded:: 0.8 :class:`.declared_attr`, when used either with a
   mixin or non-mixin declarative class, can return an existing
   :class:`.Column` already assigned to the parent :class:`.Table`,
   to indicate that the re-assignment of the :class:`.Column` should be
   skipped, however should still be mapped on the target class,
   in order to resolve duplicate column conflicts.

The same concept can be used with mixin classes (see
:ref:`declarative_mixins`)::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class HasStartDate(object):
        @declared_attr
        def start_date(cls):
            return cls.__table__.c.get('start_date', Column(DateTime))

    class Engineer(HasStartDate, Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}

    class Manager(HasStartDate, Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}

The above mixin checks the local ``__table__`` attribute for the column.
Because we're using single table inheritance, we're sure that in this case,
``cls.__table__`` refers to ``People.__table__``.  If we were mixing joined-
and single-table inheritance, we might want our mixin to check more carefully
if ``cls.__table__`` is really the :class:`.Table` we're looking for.

Concrete Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~

Concrete is defined as a subclass which has its own table and sets the
``concrete`` keyword argument to ``True``::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'concrete':True}
        id = Column(Integer, primary_key=True)
        primary_language = Column(String(50))
        name = Column(String(50))

Usage of an abstract base class is a little less straightforward as it
requires usage of :func:`~sqlalchemy.orm.util.polymorphic_union`,
which needs to be created with the :class:`.Table` objects
before the class is built::

    engineers = Table('engineers', Base.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', String(50)),
                    Column('primary_language', String(50))
                )
    managers = Table('managers', Base.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', String(50)),
                    Column('golf_swing', String(50))
                )

    punion = polymorphic_union({
        'engineer':engineers,
        'manager':managers
    }, 'type', 'punion')

    class Person(Base):
        __table__ = punion
        __mapper_args__ = {'polymorphic_on':punion.c.type}

    class Engineer(Person):
        __table__ = engineers
        __mapper_args__ = {'polymorphic_identity':'engineer', 'concrete':True}

    class Manager(Person):
        __table__ = managers
        __mapper_args__ = {'polymorphic_identity':'manager', 'concrete':True}

.. _declarative_concrete_helpers:

Using the Concrete Helpers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Helper classes provides a simpler pattern for concrete inheritance.
With these objects, the ``__declare_first__`` helper is used to configure the
"polymorphic" loader for the mapper after all subclasses have been declared.

.. versionadded:: 0.7.3

An abstract base can be declared using the
:class:`.AbstractConcreteBase` class::

    from sqlalchemy.ext.declarative import AbstractConcreteBase

    class Employee(AbstractConcreteBase, Base):
        pass

To have a concrete ``employee`` table, use :class:`.ConcreteBase` instead::

    from sqlalchemy.ext.declarative import ConcreteBase

    class Employee(ConcreteBase, Base):
        __tablename__ = 'employee'
        employee_id = Column(Integer, primary_key=True)
        name = Column(String(50))
        __mapper_args__ = {
                        'polymorphic_identity':'employee',
                        'concrete':True}


Either ``Employee`` base can be used in the normal fashion::

    class Manager(Employee):
        __tablename__ = 'manager'
        employee_id = Column(Integer, primary_key=True)
        name = Column(String(50))
        manager_data = Column(String(40))
        __mapper_args__ = {
                        'polymorphic_identity':'manager',
                        'concrete':True}

    class Engineer(Employee):
        __tablename__ = 'engineer'
        employee_id = Column(Integer, primary_key=True)
        name = Column(String(50))
        engineer_info = Column(String(40))
        __mapper_args__ = {'polymorphic_identity':'engineer',
                        'concrete':True}


The :class:`.AbstractConcreteBase` class is itself mapped, and can be
used as a target of relationships::

    class Company(Base):
        __tablename__ = 'company'

        id = Column(Integer, primary_key=True)
        employees = relationship("Employee",
                        primaryjoin="Company.id == Employee.company_id")


.. versionchanged:: 0.9.3 Support for use of :class:`.AbstractConcreteBase`
   as the target of a :func:`.relationship` has been improved.

It can also be queried directly::

    for employee in session.query(Employee).filter(Employee.name == 'qbert'):
        print(employee)


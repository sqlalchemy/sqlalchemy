.. _inheritance_toplevel:

Mapping Class Inheritance Hierarchies
=====================================

SQLAlchemy supports three forms of inheritance:

* **single table inheritance** – several types of classes are represented by a single table;

* **concrete table inheritance** – each type of class is represented by independent tables;

* **joined table inheritance** – the class hierarchy is broken up among dependent tables. Each class represented by its own table that only includes those attributes local to that class.

The most common forms of inheritance are single and joined table, while
concrete inheritance presents more configurational challenges.

When mappers are configured in an inheritance relationship, SQLAlchemy has the
ability to load elements :term:`polymorphically`, meaning that a single query can
return objects of multiple types.

.. seealso::

    :ref:`loading_joined_inheritance` - in the :ref:`queryguide_toplevel`

    :ref:`examples_inheritance` - complete examples of joined, single and
    concrete inheritance

.. _joined_inheritance:

Joined Table Inheritance
------------------------

In joined table inheritance, each class along a hierarchy of classes
is represented by a distinct table.  Querying for a particular subclass
in the hierarchy will render as a SQL JOIN along all tables in its
inheritance path. If the queried class is the base class, the base table
is queried instead, with options to include other tables at the same time
or to allow attributes specific to sub-tables to load later.

In all cases, the ultimate class to instantiate for a given row is determined
by a :term:`discriminator` column or SQL expression, defined on the base class,
which will yield a scalar value that is associated with a particular subclass.


The base class in a joined inheritance hierarchy is configured with
additional arguments that will indicate to the polymorphic discriminator
column, and optionally a polymorphic identifier for the base class itself::

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }

        def __repr__(self):
            return f"{self.__class__.__name__}({self.name!r})"

In the above example, the discriminator is the ``type`` column, whichever is
configured using the :paramref:`_orm.Mapper.polymorphic_on` parameter. This
parameter accepts a column-oriented expression, specified either as a string
name of the mapped attribute to use or as a column expression object such as
:class:`_schema.Column` or :func:`_orm.mapped_column` construct.

The discriminator column will store a value which indicates the type of object
represented within the row. The column may be of any datatype, though string
and integer are the most common.  The actual data value to be applied to this
column for a particular row in the database is specified using the
:paramref:`_orm.Mapper.polymorphic_identity` parameter, described below.

While a polymorphic discriminator expression is not strictly necessary, it is
required if polymorphic loading is desired.   Establishing a column on
the base table is the easiest way to achieve this, however very sophisticated
inheritance mappings may make use of SQL expressions, such as a CASE
expression, as the polymorphic discriminator.

.. note::

   Currently, **only one discriminator column or SQL expression may be
   configured for the entire inheritance hierarchy**, typically on the base-
   most class in the hierarchy. "Cascading" polymorphic discriminator
   expressions are not yet supported.

We next define ``Engineer`` and ``Manager`` subclasses of ``Employee``.
Each contains columns that represent the attributes unique to the subclass
they represent. Each table also must contain a primary key column (or
columns), as well as a foreign key reference to the parent table::

    class Engineer(Employee):
        __tablename__ = "engineer"
        id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
        engineer_name: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
        manager_name: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }

In the above example, each mapping specifies the
:paramref:`_orm.Mapper.polymorphic_identity` parameter within its mapper arguments.
This value populates the column designated by the
:paramref:`_orm.Mapper.polymorphic_on` parameter established on the base  mapper.
The :paramref:`_orm.Mapper.polymorphic_identity`  parameter should be unique to
each mapped class across the whole hierarchy, and there should only be one
"identity" per mapped class; as noted above,  "cascading" identities where some
subclasses introduce a second identity are not supported.

The ORM uses the value set up by :paramref:`_orm.Mapper.polymorphic_identity` in
order to determine which class a row belongs towards when loading rows
polymorphically.  In the example above, every row which represents an
``Employee`` will have the value ``'employee'`` in its ``type`` column; similarly,
every ``Engineer`` will get the value ``'engineer'``, and each ``Manager`` will
get the value ``'manager'``. Regardless of whether the inheritance mapping uses
distinct joined tables for subclasses as in joined table inheritance, or all
one table as in single table inheritance, this value is expected to be
persisted and available to the ORM when querying. The
:paramref:`_orm.Mapper.polymorphic_identity` parameter also applies to concrete
table inheritance, but is not actually persisted; see the later section at
:ref:`concrete_inheritance` for details.

In a polymorphic setup, it is most common that the foreign key constraint is
established on the same column or columns as the primary key itself, however
this is not required; a column distinct from the primary key may also be made
to refer to the parent via foreign key.  The way that a JOIN is constructed
from the base table to subclasses is also directly customizable, however this
is rarely necessary.

.. topic:: Joined inheritance primary keys

    One natural effect of the joined table inheritance configuration is that
    the identity of any mapped object can be determined entirely from rows  in
    the base table alone. This has obvious advantages, so SQLAlchemy always
    considers the primary key columns of a joined inheritance class to be those
    of the base table only. In other words, the ``id`` columns of both the
    ``engineer`` and ``manager`` tables are not used to locate ``Engineer`` or
    ``Manager`` objects - only the value in ``employee.id`` is considered.
    ``engineer.id`` and ``manager.id`` are still of course critical to the
    proper operation of the pattern overall as they are used to locate the
    joined row, once the parent row has been determined within a statement.

With the joined inheritance mapping complete, querying against ``Employee``
will return a combination of ``Employee``, ``Engineer`` and ``Manager``
objects. Newly saved ``Engineer``, ``Manager``, and ``Employee`` objects will
automatically populate the ``employee.type`` column with the correct
"discriminator" value in this case ``"engineer"``,
``"manager"``, or ``"employee"``, as appropriate.

Relationships with Joined Inheritance
+++++++++++++++++++++++++++++++++++++

Relationships are fully supported with joined table inheritance.   The
relationship involving a joined-inheritance class should target the class
in the hierarchy that also corresponds to the foreign key constraint;
below, as the ``employee`` table has a foreign key constraint back to
the ``company`` table, the relationships are set up between ``Company``
and ``Employee``::

    from __future__ import annotations

    from sqlalchemy.orm import relationship


    class Company(Base):
        __tablename__ = "company"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        employees: Mapped[List[Employee]] = relationship(back_populates="company")


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]
        company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
        company: Mapped[Company] = relationship(back_populates="employees")

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }


    class Manager(Employee): ...


    class Engineer(Employee): ...

If the foreign key constraint is on a table corresponding to a subclass,
the relationship should target that subclass instead.  In the example
below, there is a foreign
key constraint from ``manager`` to ``company``, so the relationships are
established between the ``Manager`` and ``Company`` classes::

    class Company(Base):
        __tablename__ = "company"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        managers: Mapped[List[Manager]] = relationship(back_populates="company")


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
        manager_name: Mapped[str]

        company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
        company: Mapped[Company] = relationship(back_populates="managers")

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }


    class Engineer(Employee): ...

Above, the ``Manager`` class will have a ``Manager.company`` attribute;
``Company`` will have a ``Company.managers`` attribute that always
loads against a join of the ``employee`` and ``manager`` tables together.

Loading Joined Inheritance Mappings
+++++++++++++++++++++++++++++++++++

See the section :ref:`inheritance_loading_toplevel` for background
on inheritance loading techniques, including configuration of tables
to be queried both at mapper configuration time as well as query time.

.. _single_inheritance:

Single Table Inheritance
------------------------

Single table inheritance represents all attributes of all subclasses
within a single table.  A particular subclass that has attributes unique
to that class will persist them within columns in the table that are otherwise
NULL if the row refers to a different kind of object.

Querying for a particular subclass
in the hierarchy will render as a SELECT against the base table, which
will include a WHERE clause that limits rows to those with a particular
value or values present in the discriminator column or expression.

Single table inheritance has the advantage of simplicity compared to
joined table inheritance; queries are much more efficient as only one table
needs to be involved in order to load objects of every represented class.

Single-table inheritance configuration looks much like joined-table
inheritance, except only the base class specifies ``__tablename__``. A
discriminator column is also required on the base table so that classes can be
differentiated from each other.

Even though subclasses share the base table for all of their attributes, when
using Declarative, :class:`_orm.mapped_column` objects may still be specified
on subclasses, indicating that the column is to be mapped only to that
subclass; the :class:`_orm.mapped_column` will be applied to the same base
:class:`_schema.Table` object::

    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_on": "type",
            "polymorphic_identity": "employee",
        }


    class Manager(Employee):
        manager_data: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }


    class Engineer(Employee):
        engineer_info: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }

Note that the mappers for the derived classes Manager and Engineer omit the
``__tablename__``, indicating they do not have a mapped table of
their own.  Additionally, a :func:`_orm.mapped_column` directive with
``nullable=True`` is included; as the Python types declared for these classes
do not include ``Optional[]``, the column would normally be mapped as
``NOT NULL``, which would not be appropriate as this column only expects to
be populated for those rows that correspond to that particular subclass.

.. _orm_inheritance_column_conflicts:

Resolving Column Conflicts with ``use_existing_column``
+++++++++++++++++++++++++++++++++++++++++++++++++++++++

Note in the previous section that the ``manager_name`` and ``engineer_info`` columns
are "moved up" to be applied to ``Employee.__table__``, as a result of their
declaration on a subclass that has no table of its own.   A tricky case
comes up when two subclasses want to specify *the same* column, as below::

    from datetime import datetime


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_on": "type",
            "polymorphic_identity": "employee",
        }


    class Engineer(Employee):
        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }
        start_date: Mapped[datetime] = mapped_column(nullable=True)


    class Manager(Employee):
        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }
        start_date: Mapped[datetime] = mapped_column(nullable=True)

Above, the ``start_date`` column declared on both ``Engineer`` and ``Manager``
will result in an error:

.. sourcecode:: text


    sqlalchemy.exc.ArgumentError: Column 'start_date' on class Manager conflicts
    with existing column 'employee.start_date'.  If using Declarative,
    consider using the use_existing_column parameter of mapped_column() to
    resolve conflicts.

The above scenario presents an ambiguity to the Declarative mapping system that
may be resolved by using the :paramref:`_orm.mapped_column.use_existing_column`
parameter on :func:`_orm.mapped_column`, which instructs :func:`_orm.mapped_column`
to look on the inheriting superclass present and use the column that's already
mapped, if already present, else to map a new column::


    from sqlalchemy import DateTime


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_on": "type",
            "polymorphic_identity": "employee",
        }


    class Engineer(Employee):
        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }

        start_date: Mapped[datetime] = mapped_column(
            nullable=True, use_existing_column=True
        )


    class Manager(Employee):
        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }

        start_date: Mapped[datetime] = mapped_column(
            nullable=True, use_existing_column=True
        )

Above, when ``Manager`` is mapped, the ``start_date`` column is
already present on the ``Employee`` class, having been provided by the
``Engineer`` mapping already.   The :paramref:`_orm.mapped_column.use_existing_column`
parameter indicates to :func:`_orm.mapped_column` that it should look for the
requested :class:`_schema.Column` on the mapped :class:`.Table` for
``Employee`` first, and if present, maintain that existing mapping.  If not
present, :func:`_orm.mapped_column` will map the column normally, adding it
as one of the columns in the :class:`.Table` referenced by the
``Employee`` superclass.


.. versionadded:: 2.0.0b4 - Added :paramref:`_orm.mapped_column.use_existing_column`,
   which provides a 2.0-compatible means of mapping a column on an inheriting
   subclass conditionally.  The previous approach which combines
   :class:`.declared_attr` with a lookup on the parent ``.__table__``
   continues to function as well, but lacks :pep:`484` typing support.


A similar concept can be used with mixin classes (see :ref:`orm_mixins_toplevel`)
to define a particular series of columns and/or other mapped attributes
from a reusable mixin class::

    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_on": type,
            "polymorphic_identity": "employee",
        }


    class HasStartDate:
        start_date: Mapped[datetime] = mapped_column(
            nullable=True, use_existing_column=True
        )


    class Engineer(HasStartDate, Employee):
        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }


    class Manager(HasStartDate, Employee):
        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }

Relationships with Single Table Inheritance
+++++++++++++++++++++++++++++++++++++++++++

Relationships are fully supported with single table inheritance.   Configuration
is done in the same manner as that of joined inheritance; a foreign key
attribute should be on the same class that's the "foreign" side of the
relationship::

    class Company(Base):
        __tablename__ = "company"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        employees: Mapped[List[Employee]] = relationship(back_populates="company")


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]
        company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
        company: Mapped[Company] = relationship(back_populates="employees")

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }


    class Manager(Employee):
        manager_data: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }


    class Engineer(Employee):
        engineer_info: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }

Also, like the case of joined inheritance, we can create relationships
that involve a specific subclass.   When queried, the SELECT statement will
include a WHERE clause that limits the class selection to that subclass
or subclasses::

    class Company(Base):
        __tablename__ = "company"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        managers: Mapped[List[Manager]] = relationship(back_populates="company")


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }


    class Manager(Employee):
        manager_name: Mapped[str] = mapped_column(nullable=True)

        company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
        company: Mapped[Company] = relationship(back_populates="managers")

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }


    class Engineer(Employee):
        engineer_info: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }

Above, the ``Manager`` class will have a ``Manager.company`` attribute;
``Company`` will have a ``Company.managers`` attribute that always
loads against the ``employee`` with an additional WHERE clause that
limits rows to those with ``type = 'manager'``.

.. _orm_inheritance_abstract_poly:

Building Deeper Hierarchies with ``polymorphic_abstract``
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. versionadded:: 2.0

When building any kind of inheritance hierarchy, a mapped class may include the
:paramref:`_orm.Mapper.polymorphic_abstract` parameter set to ``True``, which
indicates that the class should be mapped normally, however would not expect to
be instantiated directly and would not include a
:paramref:`_orm.Mapper.polymorphic_identity`. Subclasses may then be declared
as subclasses of this mapped class, which themselves can include a
:paramref:`_orm.Mapper.polymorphic_identity` and therefore be used normally.
This allows a series of subclasses to be referenced at once by a common base
class which is considered to be "abstract" within the hierarchy, both in
queries as well as in :func:`_orm.relationship` declarations. This use differs
from the use of the :ref:`declarative_abstract` attribute with Declarative,
which leaves the target class entirely unmapped and thus not usable as a mapped
class by itself. :paramref:`_orm.Mapper.polymorphic_abstract` may be applied to
any class or classes at any level in the hierarchy, including on multiple
levels at once.

As an example, suppose ``Manager`` and ``Principal`` were both to be classified
against a superclass ``Executive``, and ``Engineer`` and ``Sysadmin`` were
classified against a superclass ``Technologist``. Neither ``Executive`` or
``Technologist`` is ever instantiated, therefore have no
:paramref:`_orm.Mapper.polymorphic_identity`. These classes can be configured
using :paramref:`_orm.Mapper.polymorphic_abstract` as follows::

    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "polymorphic_on": "type",
        }


    class Executive(Employee):
        """An executive of the company"""

        executive_background: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {"polymorphic_abstract": True}


    class Technologist(Employee):
        """An employee who works with technology"""

        competencies: Mapped[str] = mapped_column(nullable=True)

        __mapper_args__ = {"polymorphic_abstract": True}


    class Manager(Executive):
        """a manager"""

        __mapper_args__ = {"polymorphic_identity": "manager"}


    class Principal(Executive):
        """a principal of the company"""

        __mapper_args__ = {"polymorphic_identity": "principal"}


    class Engineer(Technologist):
        """an engineer"""

        __mapper_args__ = {"polymorphic_identity": "engineer"}


    class SysAdmin(Technologist):
        """a systems administrator"""

        __mapper_args__ = {"polymorphic_identity": "sysadmin"}

In the above example, the new classes ``Technologist`` and ``Executive``
are ordinary mapped classes, and also indicate new columns to be added to the
superclass called ``executive_background`` and ``competencies``.   However,
they both lack a setting for :paramref:`_orm.Mapper.polymorphic_identity`;
this is because it's not expected that ``Technologist`` or ``Executive`` would
ever be instantiated directly; we'd always have one of ``Manager``, ``Principal``,
``Engineer`` or ``SysAdmin``.   We can however query for
``Principal`` and ``Technologist`` roles, as well as have them be targets
of :func:`_orm.relationship`.  The example below demonstrates a SELECT
statement for ``Technologist`` objects:


.. sourcecode:: python+sql

    session.scalars(select(Technologist)).all()
    {execsql}
    SELECT employee.id, employee.name, employee.type, employee.competencies
    FROM employee
    WHERE employee.type IN (?, ?)
    [...] ('engineer', 'sysadmin')

The ``Technologist`` and ``Executive`` abstract mapped classes may also be
made the targets of :func:`_orm.relationship` mappings, like any other
mapped class.  We can extend the above example to include ``Company``,
with separate collections ``Company.technologists`` and ``Company.principals``::

    class Company(Base):
        __tablename__ = "company"
        id = Column(Integer, primary_key=True)

        executives: Mapped[List[Executive]] = relationship()
        technologists: Mapped[List[Technologist]] = relationship()


    class Employee(Base):
        __tablename__ = "employee"
        id: Mapped[int] = mapped_column(primary_key=True)

        # foreign key to "company.id" is added
        company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))

        # rest of mapping is the same
        name: Mapped[str]
        type: Mapped[str]

        __mapper_args__ = {
            "polymorphic_on": "type",
        }


    # Executive, Technologist, Manager, Principal, Engineer, SysAdmin
    # classes from previous example would follow here unchanged

Using the above mapping we can use joins and relationship loading techniques
across ``Company.technologists`` and ``Company.executives`` individually:

.. sourcecode:: python+sql

    session.scalars(
        select(Company)
        .join(Company.technologists)
        .where(Technologist.competency.ilike("%java%"))
        .options(selectinload(Company.executives))
    ).all()
    {execsql}
    SELECT company.id
    FROM company JOIN employee ON company.id = employee.company_id AND employee.type IN (?, ?)
    WHERE lower(employee.competencies) LIKE lower(?)
    [...] ('engineer', 'sysadmin', '%java%')

    SELECT employee.company_id AS employee_company_id, employee.id AS employee_id,
    employee.name AS employee_name, employee.type AS employee_type,
    employee.executive_background AS employee_executive_background
    FROM employee
    WHERE employee.company_id IN (?) AND employee.type IN (?, ?)
    [...] (1, 'manager', 'principal')



.. seealso::

    :ref:`declarative_abstract` - Declarative parameter which allows a
    Declarative class to be completely un-mapped within a hierarchy, while
    still extending from a mapped superclass.


Loading Single Inheritance Mappings
+++++++++++++++++++++++++++++++++++

The loading techniques for single-table inheritance are mostly identical to
those used for joined-table inheritance, and a high degree of abstraction is
provided between these two mapping types such that it is easy to switch between
them as well as to intermix them in a single hierarchy (just omit
``__tablename__`` from whichever subclasses are to be single-inheriting). See
the sections :ref:`inheritance_loading_toplevel` and
:ref:`loading_single_inheritance` for documentation on inheritance loading
techniques, including configuration of classes to be queried both at mapper
configuration time as well as query time.

.. _concrete_inheritance:

Concrete Table Inheritance
--------------------------

Concrete inheritance maps each subclass to its own distinct table, each
of which contains all columns necessary to produce an instance of that class.
A concrete inheritance configuration by default queries non-polymorphically;
a query for a particular class will only query that class' table
and only return instances of that class.  Polymorphic loading of concrete
classes is enabled by configuring within the mapper
a special SELECT that typically is produced as a UNION of all the tables.

.. warning::

    Concrete table inheritance is **much more complicated** than joined
    or single table inheritance, and is **much more limited in functionality**
    especially pertaining to using it with relationships, eager loading,
    and polymorphic loading.  When used polymorphically it produces
    **very large queries** with UNIONS that won't perform as well as simple
    joins.  It is strongly advised that if flexibility in relationship loading
    and polymorphic loading is required, that joined or single table inheritance
    be used if at all possible.   If polymorphic loading isn't required, then
    plain non-inheriting mappings can be used if each class refers to its
    own table completely.

Whereas joined and single table inheritance are fluent in "polymorphic"
loading, it is a more awkward affair in concrete inheritance.  For this
reason, concrete inheritance is more appropriate when **polymorphic loading
is not required**.   Establishing relationships that involve concrete inheritance
classes is also more awkward.

To establish a class as using concrete inheritance, add the
:paramref:`_orm.Mapper.concrete` parameter within the ``__mapper_args__``.
This indicates to Declarative as well as the mapping that the superclass
table should not be considered as part of the mapping::

    class Employee(Base):
        __tablename__ = "employee"

        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))


    class Manager(Employee):
        __tablename__ = "manager"

        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(50))

        __mapper_args__ = {
            "concrete": True,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"

        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(50))

        __mapper_args__ = {
            "concrete": True,
        }

Two critical points should be noted:

* We must **define all columns explicitly** on each subclass, even those of
  the same name.  A column such as
  ``Employee.name`` here is **not** copied out to the tables mapped
  by ``Manager`` or ``Engineer`` for us.

* while the ``Engineer`` and ``Manager`` classes are
  mapped in an inheritance relationship with ``Employee``, they still **do not
  include polymorphic loading**.  Meaning, if we query for ``Employee``
  objects, the ``manager`` and ``engineer`` tables are not queried at all.

.. _concrete_polymorphic:

Concrete Polymorphic Loading Configuration
++++++++++++++++++++++++++++++++++++++++++

Polymorphic loading with concrete inheritance requires that a specialized
SELECT is configured against each base class that should have polymorphic
loading.  This SELECT needs to be capable of accessing all the
mapped tables individually, and is typically a UNION statement that is
constructed using a SQLAlchemy helper :func:`.polymorphic_union`.

As discussed in :ref:`inheritance_loading_toplevel`, mapper inheritance
configurations of any type can be configured to load from a special selectable
by default using the :paramref:`_orm.Mapper.with_polymorphic` argument.  Current
public API requires that this argument is set on a :class:`_orm.Mapper` when
it is first constructed.

However, in the case of Declarative, both the mapper and the :class:`_schema.Table`
that is mapped are created at once, the moment the mapped class is defined.
This means that the :paramref:`_orm.Mapper.with_polymorphic` argument cannot
be provided yet, since the :class:`_schema.Table` objects that correspond to the
subclasses haven't yet been defined.

There are a few strategies available to resolve this cycle, however
Declarative provides helper classes :class:`.ConcreteBase` and
:class:`.AbstractConcreteBase` which handle this issue behind the scenes.

Using :class:`.ConcreteBase`, we can set up our concrete mapping in
almost the same way as we do other forms of inheritance mappings::

    from sqlalchemy.ext.declarative import ConcreteBase
    from sqlalchemy.orm import DeclarativeBase


    class Base(DeclarativeBase):
        pass


    class Employee(ConcreteBase, Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "concrete": True,
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(40))

        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(40))

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }

Above, Declarative sets up the polymorphic selectable for the
``Employee`` class at mapper "initialization" time; this is the late-configuration
step for mappers that resolves other dependent mappers.  The :class:`.ConcreteBase`
helper uses the
:func:`.polymorphic_union` function to create a UNION of all concrete-mapped
tables after all the other classes are set up, and then configures this statement
with the already existing base-class mapper.

Upon select, the polymorphic union produces a query like this:

.. sourcecode:: python+sql

    session.scalars(select(Employee)).all()
    {execsql}
    SELECT
        pjoin.id,
        pjoin.name,
        pjoin.type,
        pjoin.manager_data,
        pjoin.engineer_info
    FROM (
        SELECT
            employee.id AS id,
            employee.name AS name,
            CAST(NULL AS VARCHAR(40)) AS manager_data,
            CAST(NULL AS VARCHAR(40)) AS engineer_info,
            'employee' AS type
        FROM employee
        UNION ALL
        SELECT
            manager.id AS id,
            manager.name AS name,
            manager.manager_data AS manager_data,
            CAST(NULL AS VARCHAR(40)) AS engineer_info,
            'manager' AS type
        FROM manager
        UNION ALL
        SELECT
            engineer.id AS id,
            engineer.name AS name,
            CAST(NULL AS VARCHAR(40)) AS manager_data,
            engineer.engineer_info AS engineer_info,
            'engineer' AS type
        FROM engineer
    ) AS pjoin

The above UNION query needs to manufacture "NULL" columns for each subtable
in order to accommodate for those columns that aren't members of that
particular subclass.

.. seealso::

    :class:`.ConcreteBase`

.. _abstract_concrete_base:

Abstract Concrete Classes
+++++++++++++++++++++++++

The concrete mappings illustrated thus far show both the subclasses as well
as the base class mapped to individual tables.   In the concrete inheritance
use case, it is common that the base class is not represented within the
database, only the subclasses.  In other words, the base class is
"abstract".

Normally, when one would like to map two different subclasses to individual
tables, and leave the base class unmapped, this can be achieved very easily.
When using Declarative, just declare the
base class with the ``__abstract__`` indicator::

    from sqlalchemy.orm import DeclarativeBase


    class Base(DeclarativeBase):
        pass


    class Employee(Base):
        __abstract__ = True


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(40))


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(40))

Above, we are not actually making use of SQLAlchemy's inheritance mapping
facilities; we can load and persist instances of ``Manager`` and ``Engineer``
normally.   The situation changes however when we need to **query polymorphically**,
that is, we'd like to emit ``select(Employee)`` and get back a collection
of ``Manager`` and ``Engineer`` instances.    This brings us back into the
domain of concrete inheritance, and we must build a special mapper against
``Employee`` in order to achieve this.

To modify our concrete inheritance example to illustrate an "abstract" base
that is capable of polymorphic loading,
we will have only an ``engineer`` and a ``manager`` table and no ``employee``
table, however the ``Employee`` mapper will be mapped directly to the
"polymorphic union", rather than specifying it locally to the
:paramref:`_orm.Mapper.with_polymorphic` parameter.

To help with this, Declarative offers a variant of the :class:`.ConcreteBase`
class called :class:`.AbstractConcreteBase` which achieves this automatically::

    from sqlalchemy.ext.declarative import AbstractConcreteBase
    from sqlalchemy.orm import DeclarativeBase


    class Base(DeclarativeBase):
        pass


    class Employee(AbstractConcreteBase, Base):
        strict_attrs = True

        name = mapped_column(String(50))


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(40))

        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(40))

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }


    Base.registry.configure()

Above, the :meth:`_orm.registry.configure` method is invoked, which will
trigger the ``Employee`` class to be actually mapped; before the configuration
step, the class has no mapping as the sub-tables which it will query from
have not yet been defined.   This process is more complex than that of
:class:`.ConcreteBase`, in that the entire mapping
of the base class must be delayed until all the subclasses have been declared.
With a mapping like the above, only instances of ``Manager`` and ``Engineer``
may be persisted; querying against the ``Employee`` class will always produce
``Manager`` and ``Engineer`` objects.

Using the above mapping, queries can be produced in terms of the ``Employee``
class and any attributes that are locally declared upon it, such as the
``Employee.name``:

.. sourcecode:: pycon+sql

    >>> stmt = select(Employee).where(Employee.name == "n1")
    >>> print(stmt)
    {printsql}SELECT pjoin.id, pjoin.name, pjoin.type, pjoin.manager_data, pjoin.engineer_info
    FROM (
      SELECT engineer.id AS id, engineer.name AS name, engineer.engineer_info AS engineer_info,
      CAST(NULL AS VARCHAR(40)) AS manager_data, 'engineer' AS type
      FROM engineer
      UNION ALL
      SELECT manager.id AS id, manager.name AS name, CAST(NULL AS VARCHAR(40)) AS engineer_info,
      manager.manager_data AS manager_data, 'manager' AS type
      FROM manager
    ) AS pjoin
    WHERE pjoin.name = :name_1

The :paramref:`.AbstractConcreteBase.strict_attrs` parameter indicates that the
``Employee`` class should directly map only those attributes which are local to
the ``Employee`` class, in this case the ``Employee.name`` attribute. Other
attributes such as ``Manager.manager_data`` and ``Engineer.engineer_info`` are
present only on their corresponding subclass.
When :paramref:`.AbstractConcreteBase.strict_attrs`
is not set, then all subclass attributes such as ``Manager.manager_data`` and
``Engineer.engineer_info`` get mapped onto the base ``Employee`` class.  This
is a legacy mode of use which may be more convenient for querying but has the
effect that all subclasses share the
full set of attributes for the whole hierarchy; in the above example, not
using :paramref:`.AbstractConcreteBase.strict_attrs` would have the effect
of generating non-useful ``Engineer.manager_name`` and ``Manager.engineer_info``
attributes.

.. versionadded:: 2.0  Added :paramref:`.AbstractConcreteBase.strict_attrs`
   parameter to :class:`.AbstractConcreteBase` which produces a cleaner
   mapping; the default is False to allow legacy mappings to continue working
   as they did in 1.x versions.



.. seealso::

    :class:`.AbstractConcreteBase`


Classical and Semi-Classical Concrete Polymorphic Configuration
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The Declarative configurations illustrated with :class:`.ConcreteBase`
and :class:`.AbstractConcreteBase` are equivalent to two other forms
of configuration that make use of :func:`.polymorphic_union` explicitly.
These configurational forms make use of the :class:`_schema.Table` object explicitly
so that the "polymorphic union" can be created first, then applied
to the mappings.   These are illustrated here to clarify the role
of the :func:`.polymorphic_union` function in terms of mapping.

A **semi-classical mapping** for example makes use of Declarative, but
establishes the :class:`_schema.Table` objects separately::

    metadata_obj = Base.metadata

    employees_table = Table(
        "employee",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )

    managers_table = Table(
        "manager",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("manager_data", String(50)),
    )

    engineers_table = Table(
        "engineer",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("engineer_info", String(50)),
    )

Next, the UNION is produced using :func:`.polymorphic_union`::

    from sqlalchemy.orm import polymorphic_union

    pjoin = polymorphic_union(
        {
            "employee": employees_table,
            "manager": managers_table,
            "engineer": engineers_table,
        },
        "type",
        "pjoin",
    )

With the above :class:`_schema.Table` objects, the mappings can be produced using "semi-classical" style,
where we use Declarative in conjunction with the ``__table__`` argument;
our polymorphic union above is passed via ``__mapper_args__`` to
the :paramref:`_orm.Mapper.with_polymorphic` parameter::

    class Employee(Base):
        __table__ = employee_table
        __mapper_args__ = {
            "polymorphic_on": pjoin.c.type,
            "with_polymorphic": ("*", pjoin),
            "polymorphic_identity": "employee",
        }


    class Engineer(Employee):
        __table__ = engineer_table
        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }


    class Manager(Employee):
        __table__ = manager_table
        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }

Alternatively, the same :class:`_schema.Table` objects can be used in
fully "classical" style, without using Declarative at all.
A constructor similar to that supplied by Declarative is illustrated::

    class Employee:
        def __init__(self, **kw):
            for k in kw:
                setattr(self, k, kw[k])


    class Manager(Employee):
        pass


    class Engineer(Employee):
        pass


    employee_mapper = mapper_registry.map_imperatively(
        Employee,
        pjoin,
        with_polymorphic=("*", pjoin),
        polymorphic_on=pjoin.c.type,
    )
    manager_mapper = mapper_registry.map_imperatively(
        Manager,
        managers_table,
        inherits=employee_mapper,
        concrete=True,
        polymorphic_identity="manager",
    )
    engineer_mapper = mapper_registry.map_imperatively(
        Engineer,
        engineers_table,
        inherits=employee_mapper,
        concrete=True,
        polymorphic_identity="engineer",
    )

The "abstract" example can also be mapped using "semi-classical" or "classical"
style.  The difference is that instead of applying the "polymorphic union"
to the :paramref:`_orm.Mapper.with_polymorphic` parameter, we apply it directly
as the mapped selectable on our basemost mapper.  The semi-classical
mapping is illustrated below::

    from sqlalchemy.orm import polymorphic_union

    pjoin = polymorphic_union(
        {
            "manager": managers_table,
            "engineer": engineers_table,
        },
        "type",
        "pjoin",
    )


    class Employee(Base):
        __table__ = pjoin
        __mapper_args__ = {
            "polymorphic_on": pjoin.c.type,
            "with_polymorphic": "*",
            "polymorphic_identity": "employee",
        }


    class Engineer(Employee):
        __table__ = engineer_table
        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }


    class Manager(Employee):
        __table__ = manager_table
        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }

Above, we use :func:`.polymorphic_union` in the same manner as before, except
that we omit the ``employee`` table.

.. seealso::

    :ref:`orm_imperative_mapping` - background information on imperative, or "classical" mappings



Relationships with Concrete Inheritance
+++++++++++++++++++++++++++++++++++++++

In a concrete inheritance scenario, mapping relationships is challenging
since the distinct classes do not share a table.    If the relationships
only involve specific classes, such as a relationship between ``Company`` in
our previous examples and ``Manager``, special steps aren't needed as these
are just two related tables.

However, if ``Company`` is to have a one-to-many relationship
to ``Employee``, indicating that the collection may include both
``Engineer`` and ``Manager`` objects, that implies that ``Employee`` must
have polymorphic loading capabilities and also that each table to be related
must have a foreign key back to the ``company`` table.  An example of
such a configuration is as follows::

    from sqlalchemy.ext.declarative import ConcreteBase


    class Company(Base):
        __tablename__ = "company"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        employees = relationship("Employee")


    class Employee(ConcreteBase, Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        company_id = mapped_column(ForeignKey("company.id"))

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "concrete": True,
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(40))
        company_id = mapped_column(ForeignKey("company.id"))

        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(40))
        company_id = mapped_column(ForeignKey("company.id"))

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }

The next complexity with concrete inheritance and relationships involves
when we'd like one or all of ``Employee``, ``Manager`` and ``Engineer`` to
themselves refer back to ``Company``.   For this case, SQLAlchemy has
special behavior in that a :func:`_orm.relationship` placed on ``Employee``
which links to ``Company`` **does not work**
against the ``Manager`` and ``Engineer`` classes, when exercised at the
instance level.  Instead, a distinct
:func:`_orm.relationship` must be applied to each class.   In order to achieve
bi-directional behavior in terms of three separate relationships which
serve as the opposite of ``Company.employees``, the
:paramref:`_orm.relationship.back_populates` parameter is used between
each of the relationships::

    from sqlalchemy.ext.declarative import ConcreteBase


    class Company(Base):
        __tablename__ = "company"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        employees = relationship("Employee", back_populates="company")


    class Employee(ConcreteBase, Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        company_id = mapped_column(ForeignKey("company.id"))
        company = relationship("Company", back_populates="employees")

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "concrete": True,
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        manager_data = mapped_column(String(40))
        company_id = mapped_column(ForeignKey("company.id"))
        company = relationship("Company", back_populates="employees")

        __mapper_args__ = {
            "polymorphic_identity": "manager",
            "concrete": True,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        engineer_info = mapped_column(String(40))
        company_id = mapped_column(ForeignKey("company.id"))
        company = relationship("Company", back_populates="employees")

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
            "concrete": True,
        }

The above limitation is related to the current implementation, including
that concrete inheriting classes do not share any of the attributes of
the superclass and therefore need distinct relationships to be set up.

Loading Concrete Inheritance Mappings
+++++++++++++++++++++++++++++++++++++

The options for loading with concrete inheritance are limited; generally,
if polymorphic loading is configured on the mapper using one of the
declarative concrete mixins, it can't be modified at query time
in current SQLAlchemy versions.   Normally, the :func:`_orm.with_polymorphic`
function would be able to override the style of loading used by concrete,
however due to current limitations this is not yet supported.


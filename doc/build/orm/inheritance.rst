.. _inheritance_toplevel:

Mapping Class Inheritance Hierarchies
======================================

SQLAlchemy supports three forms of inheritance: **single table inheritance**,
where several types of classes are represented by a single table, **concrete table
inheritance**, where each type of class is represented by independent tables,
and **joined
table inheritance**, where the class hierarchy is broken up
among dependent tables, each class represented by its own table that only
includes those attributes local to that class.

The most common forms of inheritance are single and joined table, while
concrete inheritance presents more configurational challenges.

When mappers are configured in an inheritance relationship, SQLAlchemy has the
ability to load elements :term:`polymorphically`, meaning that a single query can
return objects of multiple types.

Joined Table Inheritance
-------------------------

In joined table inheritance, each class along a particular classes' list of
parents is represented by a unique table. The total set of attributes for a
particular instance is represented as a join along all tables in its
inheritance path. Here, we first define the ``Employee`` class.
This table will contain a primary key column (or columns), and a column
for each attribute that's represented by ``Employee``. In this case it's just
``name``::

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        type = Column(String(50))

        __mapper_args__ = {
            'polymorphic_identity':'employee',
            'polymorphic_on':type
        }

The mapped table also has a column called ``type``.   The purpose of
this column is to act as the **discriminator**, and stores a value
which indicates the type of object represented within the row. The column may
be of any datatype, though string and integer are the most common.

.. warning::

   Currently, **only one discriminator column may be set**, typically
   on the base-most class in the hierarchy. "Cascading" polymorphic columns
   are not yet supported.

The discriminator column is only needed if polymorphic loading is
desired, as is usually the case.   It is not strictly necessary that
it be present directly on the base mapped table, and can instead be defined on a
derived select statement that's used when the class is queried;
however, this is a much more sophisticated configuration scenario.

The mapping receives additional arguments via the ``__mapper_args__``
dictionary.   Here the ``type`` column is explicitly stated as the
discriminator column, and the **polymorphic identity** of ``employee``
is also given; this is the value that will be
stored in the polymorphic discriminator column for instances of this
class.

We next define ``Engineer`` and ``Manager`` subclasses of ``Employee``.
Each contains columns that represent the attributes unique to the subclass
they represent. Each table also must contain a primary key column (or
columns), and in most cases a foreign key reference to the parent table::

    class Engineer(Employee):
        __tablename__ = 'engineer'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        engineer_name = Column(String(30))

        __mapper_args__ = {
            'polymorphic_identity':'engineer',
        }

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        manager_name = Column(String(30))

        __mapper_args__ = {
            'polymorphic_identity':'manager',
        }

It is standard practice that the same column is used for both the role
of primary key as well as foreign key to the parent table,
and that the column is also named the same as that of the parent table.
However, both of these practices are optional.  Separate columns may be used for
primary key and parent-relationship, the column may be named differently than
that of the parent, and even a custom join condition can be specified between
parent and child tables instead of using a foreign key.

.. topic:: Joined inheritance primary keys

    One natural effect of the joined table inheritance configuration is that the
    identity of any mapped object can be determined entirely from the base table.
    This has obvious advantages, so SQLAlchemy always considers the primary key
    columns of a joined inheritance class to be those of the base table only.
    In other words, the ``id``
    columns of both the ``engineer`` and ``manager`` tables are not used to locate
    ``Engineer`` or ``Manager`` objects - only the value in
    ``employee.id`` is considered. ``engineer.id`` and ``manager.id`` are
    still of course critical to the proper operation of the pattern overall as
    they are used to locate the joined row, once the parent row has been
    determined within a statement.

With the joined inheritance mapping complete, querying against ``Employee`` will return a combination of
``Employee``, ``Engineer`` and ``Manager`` objects. Newly saved ``Engineer``,
``Manager``, and ``Employee`` objects will automatically populate the
``employee.type`` column with ``engineer``, ``manager``, or ``employee``, as
appropriate.

.. _with_polymorphic:

Basic Control of Which Tables are Queried
++++++++++++++++++++++++++++++++++++++++++

The :func:`.orm.with_polymorphic` function and the
:func:`~sqlalchemy.orm.query.Query.with_polymorphic` method of
:class:`~sqlalchemy.orm.query.Query` affects the specific tables
which the :class:`.Query` selects from.  Normally, a query such as this::

    session.query(Employee).all()

...selects only from the ``employee`` table. When loading fresh from the
database, our joined-table setup will query from the parent table only, using
SQL such as this:

.. sourcecode:: python+sql

    {opensql}
    SELECT employee.id AS employee_id,
        employee.name AS employee_name, employee.type AS employee_type
    FROM employee
    []

As attributes are requested from those ``Employee`` objects which are
represented in either the ``engineer`` or ``manager`` child tables, a second
load is issued for the columns in that related row, if the data was not
already loaded. So above, after accessing the objects you'd see further SQL
issued along the lines of:

.. sourcecode:: python+sql

    {opensql}
    SELECT manager.id AS manager_id,
        manager.manager_data AS manager_manager_data
    FROM manager
    WHERE ? = manager.id
    [5]
    SELECT engineer.id AS engineer_id,
        engineer.engineer_info AS engineer_engineer_info
    FROM engineer
    WHERE ? = engineer.id
    [2]

This behavior works well when issuing searches for small numbers of items,
such as when using :meth:`.Query.get`, since the full range of joined tables are not
pulled in to the SQL statement unnecessarily. But when querying a larger span
of rows which are known to be of many types, you may want to actively join to
some or all of the joined tables. The ``with_polymorphic`` feature
provides this.

Telling our query to polymorphically load ``Engineer`` and ``Manager``
objects, we can use the :func:`.orm.with_polymorphic` function
to create a new aliased class which represents a select of the base
table combined with outer joins to each of the inheriting tables::

    from sqlalchemy.orm import with_polymorphic

    eng_plus_manager = with_polymorphic(Employee, [Engineer, Manager])

    query = session.query(eng_plus_manager)

The above produces a query which joins the ``employee`` table to both the
``engineer`` and ``manager`` tables like the following:

.. sourcecode:: python+sql

    query.all()
    {opensql}
    SELECT employee.id AS employee_id,
        engineer.id AS engineer_id,
        manager.id AS manager_id,
        employee.name AS employee_name,
        employee.type AS employee_type,
        engineer.engineer_info AS engineer_engineer_info,
        manager.manager_data AS manager_manager_data
    FROM employee
        LEFT OUTER JOIN engineer
        ON employee.id = engineer.id
        LEFT OUTER JOIN manager
        ON employee.id = manager.id
    []

The entity returned by :func:`.orm.with_polymorphic` is an :class:`.AliasedClass`
object, which can be used in a :class:`.Query` like any other alias, including
named attributes for those attributes on the ``Employee`` class.   In our
example, ``eng_plus_manager`` becomes the entity that we use to refer to the
three-way outer join above.  It also includes namespaces for each class named
in the list of classes, so that attributes specific to those subclasses can be
called upon as well.   The following example illustrates calling upon attributes
specific to ``Engineer`` as well as ``Manager`` in terms of ``eng_plus_manager``::

    eng_plus_manager = with_polymorphic(Employee, [Engineer, Manager])
    query = session.query(eng_plus_manager).filter(
                    or_(
                        eng_plus_manager.Engineer.engineer_info=='x',
                        eng_plus_manager.Manager.manager_data=='y'
                    )
                )

:func:`.orm.with_polymorphic` accepts a single class or
mapper, a list of classes/mappers, or the string ``'*'`` to indicate all
subclasses:

.. sourcecode:: python+sql

    # join to the engineer table
    entity = with_polymorphic(Employee, Engineer)

    # join to the engineer and manager tables
    entity = with_polymorphic(Employee, [Engineer, Manager])

    # join to all subclass tables
    entity = query.with_polymorphic(Employee, '*')

    # use with Query
    session.query(entity).all()

It also accepts a third argument ``selectable`` which replaces the automatic
join creation and instead selects directly from the selectable given. This
feature is normally used with "concrete" inheritance, described later, but can
be used with any kind of inheritance setup in the case that specialized SQL
should be used to load polymorphically::

    # custom selectable
    employee = Employee.__table__
    manager = Manager.__table__
    engineer = Engineer.__table__
    entity = with_polymorphic(
                Employee,
                [Engineer, Manager],
                employee.outerjoin(manager).outerjoin(engineer)
            )

    # use with Query
    session.query(entity).all()

Note that if you only need to load a single subtype, such as just the
``Engineer`` objects, :func:`.orm.with_polymorphic` is
not needed since you would query against the ``Engineer`` class directly.

:meth:`.Query.with_polymorphic` has the same purpose
as :func:`.orm.with_polymorphic`, except is not as
flexible in its usage patterns in that it only applies to the first full
mapping, which then impacts all occurrences of that class or the target
subclasses within the :class:`.Query`.  For simple cases it might be
considered to be more succinct::

    session.query(Employee).with_polymorphic([Engineer, Manager]).\
        filter(or_(Engineer.engineer_info=='w', Manager.manager_data=='q'))

.. versionadded:: 0.8
    :func:`.orm.with_polymorphic`, an improved version of
    :meth:`.Query.with_polymorphic` method.

The mapper also accepts ``with_polymorphic`` as a configurational argument so
that the joined-style load will be issued automatically. This argument may be
the string ``'*'``, a list of classes, or a tuple consisting of either,
followed by a selectable::

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        type = Column(String(20))

        __mapper_args__ = {
            'polymorphic_on':type,
            'polymorphic_identity':'employee',
            'with_polymorphic':'*'
        }

    class Engineer(Employee):
        __tablename__ = 'engineer'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        __mapper_args__ = {'polymorphic_identity':'engineer'}

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        __mapper_args__ = {'polymorphic_identity':'manager'}

The above mapping will produce a query similar to that of
``with_polymorphic('*')`` for every query of ``Employee`` objects.

Using :func:`.orm.with_polymorphic` or :meth:`.Query.with_polymorphic`
will override the mapper-level ``with_polymorphic`` setting.

.. autofunction:: sqlalchemy.orm.with_polymorphic

Advanced Control of Which Tables are Queried
+++++++++++++++++++++++++++++++++++++++++++++

The ``with_polymorphic`` functions work fine for
simplistic scenarios.   However, direct control of table rendering
is called for, such as the case when one wants to
render to only the subclass table and not the parent table.

This use case can be achieved by using the mapped :class:`.Table`
objects directly.   For example, to
query the name of employees with particular criterion::

    engineer = Engineer.__table__
    manager = Manager.__table__

    session.query(Employee.name).\
        outerjoin((engineer, engineer.c.employee_id==Employee.employee_id)).\
        outerjoin((manager, manager.c.employee_id==Employee.employee_id)).\
        filter(or_(Engineer.engineer_info=='w', Manager.manager_data=='q'))

The base table, in this case the "employees" table, isn't always necessary. A
SQL query is always more efficient with fewer joins. Here, if we wanted to
just load information specific to manager or engineer, we can instruct
:class:`.Query` to use only those tables. The ``FROM`` clause is determined by
what's specified in the :meth:`.Session.query`, :meth:`.Query.filter`, or
:meth:`.Query.select_from` methods::

    session.query(Manager.manager_data).select_from(manager)

    session.query(engineer.c.id).\
            filter(engineer.c.engineer_info==manager.c.manager_data)

.. _of_type:

Creating Joins to Specific Subtypes
+++++++++++++++++++++++++++++++++++

The :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` method is a
helper which allows the construction of joins along
:func:`~sqlalchemy.orm.relationship` paths while narrowing the criterion to
specific subclasses. Suppose the ``employees`` table represents a collection
of employees which are associated with a ``Company`` object. We'll add a
``company_id`` column to the ``employees`` table and a new table
``companies``:

.. sourcecode:: python+sql

    class Company(Base):
        __tablename__ = 'company'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        employees = relationship("Employee",
                        backref='company',
                        cascade='all, delete-orphan')

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        type = Column(String(20))
        company_id = Column(Integer, ForeignKey('company.id'))
        __mapper_args__ = {
            'polymorphic_on':type,
            'polymorphic_identity':'employee',
            'with_polymorphic':'*'
        }

    class Engineer(Employee):
        __tablename__ = 'engineer'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        engineer_info = Column(String(50))
        __mapper_args__ = {'polymorphic_identity':'engineer'}

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        manager_data = Column(String(50))
        __mapper_args__ = {'polymorphic_identity':'manager'}

When querying from ``Company`` onto the ``Employee`` relationship, the
``join()`` method as well as the ``any()`` and ``has()`` operators will create
a join from ``company`` to ``employee``, without including ``engineer`` or
``manager`` in the mix. If we wish to have criterion which is specifically
against the ``Engineer`` class, we can tell those methods to join or subquery
against the joined table representing the subclass using the
:meth:`~.orm.interfaces.PropComparator.of_type` operator::

    session.query(Company).\
        join(Company.employees.of_type(Engineer)).\
        filter(Engineer.engineer_info=='someinfo')

A longhand version of this would involve spelling out the full target
selectable within a 2-tuple::

    employee = Employee.__table__
    engineer = Engineer.__table__

    session.query(Company).\
        join((employee.join(engineer), Company.employees)).\
        filter(Engineer.engineer_info=='someinfo')

:func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` accepts a
single class argument.  More flexibility can be achieved either by
joining to an explicit join as above, or by using the :func:`.orm.with_polymorphic`
function to create a polymorphic selectable::

    manager_and_engineer = with_polymorphic(
                                Employee, [Manager, Engineer],
                                aliased=True)

    session.query(Company).\
        join(manager_and_engineer, Company.employees).\
        filter(
            or_(manager_and_engineer.Engineer.engineer_info=='someinfo',
                manager_and_engineer.Manager.manager_data=='somedata')
        )

Above, we use the ``aliased=True`` argument with :func:`.orm.with_polymorhpic`
so that the right hand side of the join between ``Company`` and ``manager_and_engineer``
is converted into an aliased subquery.  Some backends, such as SQLite and older
versions of MySQL can't handle a FROM clause of the following form::

    FROM x JOIN (y JOIN z ON <onclause>) ON <onclause>

Using ``aliased=True`` instead renders it more like::

    FROM x JOIN (SELECT * FROM y JOIN z ON <onclause>) AS anon_1 ON <onclause>

The above join can also be expressed more succinctly by combining ``of_type()``
with the polymorphic construct::

    manager_and_engineer = with_polymorphic(
                                Employee, [Manager, Engineer],
                                aliased=True)

    session.query(Company).\
        join(Company.employees.of_type(manager_and_engineer)).\
        filter(
            or_(manager_and_engineer.Engineer.engineer_info=='someinfo',
                manager_and_engineer.Manager.manager_data=='somedata')
        )

The ``any()`` and ``has()`` operators also can be used with
:func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` when the embedded
criterion is in terms of a subclass::

    session.query(Company).\
            filter(
                Company.employees.of_type(Engineer).
                    any(Engineer.engineer_info=='someinfo')
                ).all()

Note that the ``any()`` and ``has()`` are both shorthand for a correlated
EXISTS query. To build one by hand looks like::

    session.query(Company).filter(
        exists([1],
            and_(Engineer.engineer_info=='someinfo',
                employees.c.company_id==companies.c.company_id),
            from_obj=employees.join(engineers)
        )
    ).all()

The EXISTS subquery above selects from the join of ``employees`` to
``engineers``, and also specifies criterion which correlates the EXISTS
subselect back to the parent ``companies`` table.

.. versionadded:: 0.8
   :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` accepts
   :func:`.orm.aliased` and :func:`.orm.with_polymorphic` constructs in conjunction
   with :meth:`.Query.join`, ``any()`` and ``has()``.

Eager Loading of Specific or Polymorphic Subtypes
++++++++++++++++++++++++++++++++++++++++++++++++++

The :func:`.joinedload`, :func:`.subqueryload`, :func:`.contains_eager` and
other loading-related options also support
paths which make use of :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type`.
Below we load ``Company`` rows while eagerly loading related ``Engineer``
objects, querying the ``employee`` and ``engineer`` tables simultaneously::

    session.query(Company).\
        options(
            subqueryload(Company.employees.of_type(Engineer)).
            subqueryload("machines")
            )
        )

As is the case with :meth:`.Query.join`, :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type`
also can be used with eager loading and :func:`.orm.with_polymorphic`
at the same time, so that all sub-attributes of all referenced subtypes
can be loaded::

    manager_and_engineer = with_polymorphic(
                                Employee, [Manager, Engineer],
                                aliased=True)

    session.query(Company).\
        options(
            joinedload(Company.employees.of_type(manager_and_engineer))
            )
        )

.. versionadded:: 0.8
    :func:`.joinedload`, :func:`.subqueryload`, :func:`.contains_eager`
    and related loader options support
    paths that are qualified with
    :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type`, supporting
    single target types as well as :func:`.orm.with_polymorphic` targets.


Single Table Inheritance
------------------------

Single table inheritance is where the attributes of the base class as well as
all subclasses are represented within a single table. A column is present in
the table for every attribute mapped to the base class and all subclasses; the
columns which correspond to a single subclass are nullable. This configuration
looks much like joined-table inheritance except there's only one table. In
this case, a ``type`` column is required, as there would be no other way to
discriminate between classes. The table is specified in the base mapper only;
for the inheriting classes, leave their ``table`` parameter blank:

.. sourcecode:: python+sql

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        manager_data = Column(String(50))
        engineer_info = Column(String(50))
        type = Column(String(20))

        __mapper_args__ = {
            'polymorphic_on':type,
            'polymorphic_identity':'employee'
        }

    class Manager(Employee):
        __mapper_args__ = {
            'polymorphic_identity':'manager'
        }

    class Engineer(Employee):
        __mapper_args__ = {
            'polymorphic_identity':'engineer'
        }

Note that the mappers for the derived classes Manager and Engineer omit the
``__tablename__``, indicating they do not have a mapped table of
their own.

.. _concrete_inheritance:

Concrete Table Inheritance
--------------------------

.. note::

    this section is currently using classical mappings.  The
    Declarative system fully supports concrete inheritance
    however.   See the links below for more information on using
    declarative with concrete table inheritance.

This form of inheritance maps each class to a distinct table, as below:

.. sourcecode:: python+sql

    employees_table = Table('employees', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
    )

    managers_table = Table('managers', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('manager_data', String(50)),
    )

    engineers_table = Table('engineers', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('engineer_info', String(50)),
    )

Notice in this case there is no ``type`` column. If polymorphic loading is not
required, there's no advantage to using ``inherits`` here; you just define a
separate mapper for each class.

.. sourcecode:: python+sql

    mapper(Employee, employees_table)
    mapper(Manager, managers_table)
    mapper(Engineer, engineers_table)

To load polymorphically, the ``with_polymorphic`` argument is required, along
with a selectable indicating how rows should be loaded. In this case we must
construct a UNION of all three tables. SQLAlchemy includes a helper function
to create these called :func:`~sqlalchemy.orm.util.polymorphic_union`, which
will map all the different columns into a structure of selects with the same
numbers and names of columns, and also generate a virtual ``type`` column for
each subselect:

.. sourcecode:: python+sql

    pjoin = polymorphic_union({
        'employee': employees_table,
        'manager': managers_table,
        'engineer': engineers_table
    }, 'type', 'pjoin')

    employee_mapper = mapper(Employee, employees_table,
                                        with_polymorphic=('*', pjoin),
                                        polymorphic_on=pjoin.c.type,
                                        polymorphic_identity='employee')
    manager_mapper = mapper(Manager, managers_table,
                                        inherits=employee_mapper,
                                        concrete=True,
                                        polymorphic_identity='manager')
    engineer_mapper = mapper(Engineer, engineers_table,
                                        inherits=employee_mapper,
                                        concrete=True,
                                        polymorphic_identity='engineer')

Upon select, the polymorphic union produces a query like this:

.. sourcecode:: python+sql

    session.query(Employee).all()
    {opensql}
    SELECT pjoin.type AS pjoin_type,
            pjoin.manager_data AS pjoin_manager_data,
            pjoin.employee_id AS pjoin_employee_id,
    pjoin.name AS pjoin_name, pjoin.engineer_info AS pjoin_engineer_info
    FROM (
        SELECT employees.employee_id AS employee_id,
            CAST(NULL AS VARCHAR(50)) AS manager_data, employees.name AS name,
            CAST(NULL AS VARCHAR(50)) AS engineer_info, 'employee' AS type
        FROM employees
    UNION ALL
        SELECT managers.employee_id AS employee_id,
            managers.manager_data AS manager_data, managers.name AS name,
            CAST(NULL AS VARCHAR(50)) AS engineer_info, 'manager' AS type
        FROM managers
    UNION ALL
        SELECT engineers.employee_id AS employee_id,
            CAST(NULL AS VARCHAR(50)) AS manager_data, engineers.name AS name,
        engineers.engineer_info AS engineer_info, 'engineer' AS type
        FROM engineers
    ) AS pjoin
    []

Concrete Inheritance with Declarative
++++++++++++++++++++++++++++++++++++++

.. versionadded:: 0.7.3
    The :ref:`declarative_toplevel` module includes helpers for concrete
    inheritance. See :ref:`declarative_concrete_helpers` for more information.

Using Relationships with Inheritance
------------------------------------

Both joined-table and single table inheritance scenarios produce mappings
which are usable in :func:`~sqlalchemy.orm.relationship` functions; that is,
it's possible to map a parent object to a child object which is polymorphic.
Similarly, inheriting mappers can have :func:`~sqlalchemy.orm.relationship`
objects of their own at any level, which are inherited to each child class.
The only requirement for relationships is that there is a table relationship
between parent and child. An example is the following modification to the
joined table inheritance example, which sets a bi-directional relationship
between ``Employee`` and ``Company``:

.. sourcecode:: python+sql

    employees_table = Table('employees', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('company_id', Integer, ForeignKey('companies.company_id'))
    )

    companies = Table('companies', metadata,
       Column('company_id', Integer, primary_key=True),
       Column('name', String(50)))

    class Company(object):
        pass

    mapper(Company, companies, properties={
       'employees': relationship(Employee, backref='company')
    })

Relationships with Concrete Inheritance
+++++++++++++++++++++++++++++++++++++++

In a concrete inheritance scenario, mapping relationships is more challenging
since the distinct classes do not share a table. In this case, you *can*
establish a relationship from parent to child if a join condition can be
constructed from parent to child, if each child table contains a foreign key
to the parent:

.. sourcecode:: python+sql

    companies = Table('companies', metadata,
       Column('id', Integer, primary_key=True),
       Column('name', String(50)))

    employees_table = Table('employees', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('company_id', Integer, ForeignKey('companies.id'))
    )

    managers_table = Table('managers', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('manager_data', String(50)),
        Column('company_id', Integer, ForeignKey('companies.id'))
    )

    engineers_table = Table('engineers', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('engineer_info', String(50)),
        Column('company_id', Integer, ForeignKey('companies.id'))
    )

    mapper(Employee, employees_table,
                    with_polymorphic=('*', pjoin),
                    polymorphic_on=pjoin.c.type,
                    polymorphic_identity='employee')

    mapper(Manager, managers_table,
                    inherits=employee_mapper,
                    concrete=True,
                    polymorphic_identity='manager')

    mapper(Engineer, engineers_table,
                    inherits=employee_mapper,
                    concrete=True,
                    polymorphic_identity='engineer')

    mapper(Company, companies, properties={
        'employees': relationship(Employee)
    })

The big limitation with concrete table inheritance is that
:func:`~sqlalchemy.orm.relationship` objects placed on each concrete mapper do
**not** propagate to child mappers. If you want to have the same
:func:`~sqlalchemy.orm.relationship` objects set up on all concrete mappers,
they must be configured manually on each. To configure back references in such
a configuration the ``back_populates`` keyword may be used instead of
``backref``, such as below where both ``A(object)`` and ``B(A)``
bidirectionally reference ``C``::

    ajoin = polymorphic_union({
            'a':a_table,
            'b':b_table
        }, 'type', 'ajoin')

    mapper(A, a_table, with_polymorphic=('*', ajoin),
        polymorphic_on=ajoin.c.type, polymorphic_identity='a',
        properties={
            'some_c':relationship(C, back_populates='many_a')
    })
    mapper(B, b_table,inherits=A, concrete=True,
        polymorphic_identity='b',
        properties={
            'some_c':relationship(C, back_populates='many_a')
    })
    mapper(C, c_table, properties={
        'many_a':relationship(A, collection_class=set,
                                    back_populates='some_c'),
    })

Using Inheritance with Declarative
-----------------------------------

Declarative makes inheritance configuration more intuitive.   See the docs at :ref:`declarative_inheritance`.

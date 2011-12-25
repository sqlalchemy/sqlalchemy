.. _inheritance_toplevel:

Mapping Class Inheritance Hierarchies
======================================

SQLAlchemy supports three forms of inheritance: *single table inheritance*,
where several types of classes are stored in one table, *concrete table
inheritance*, where each type of class is stored in its own table, and *joined
table inheritance*, where the parent/child classes are stored in their own
tables that are joined together in a select. Whereas support for single and
joined table inheritance is strong, concrete table inheritance is a less
common scenario with some particular problems so is not quite as flexible.

When mappers are configured in an inheritance relationship, SQLAlchemy has the
ability to load elements "polymorphically", meaning that a single query can
return objects of multiple types.

.. note:: 

   This section currently uses classical mappings to illustrate inheritance
   configurations, and will soon be updated to standardize on Declarative.
   Until then, please refer to :ref:`declarative_inheritance` for information on
   how common inheritance mappings are constructed declaratively.

For the following sections, assume this class relationship:

.. sourcecode:: python+sql

    class Employee(object):
        def __init__(self, name):
            self.name = name
        def __repr__(self):
            return self.__class__.__name__ + " " + self.name

    class Manager(Employee):
        def __init__(self, name, manager_data):
            self.name = name
            self.manager_data = manager_data
        def __repr__(self):
            return (
                self.__class__.__name__ + " " + 
                self.name + " " +  self.manager_data
            )

    class Engineer(Employee):
        def __init__(self, name, engineer_info):
            self.name = name
            self.engineer_info = engineer_info
        def __repr__(self):
            return (
                self.__class__.__name__ + " " + 
                self.name + " " +  self.engineer_info
            )

Joined Table Inheritance
-------------------------

In joined table inheritance, each class along a particular classes' list of
parents is represented by a unique table. The total set of attributes for a
particular instance is represented as a join along all tables in its
inheritance path. Here, we first define a table to represent the ``Employee``
class. This table will contain a primary key column (or columns), and a column
for each attribute that's represented by ``Employee``. In this case it's just
``name``::

    employees = Table('employees', metadata,
       Column('employee_id', Integer, primary_key=True),
       Column('name', String(50)),
       Column('type', String(30), nullable=False)
    )

The table also has a column called ``type``. It is strongly advised in both
single- and joined- table inheritance scenarios that the root table contains a
column whose sole purpose is that of the **discriminator**; it stores a value
which indicates the type of object represented within the row. The column may
be of any desired datatype. While there are some "tricks" to work around the
requirement that there be a discriminator column, they are more complicated to
configure when one wishes to load polymorphically.

Next we define individual tables for each of ``Engineer`` and ``Manager``,
which contain columns that represent the attributes unique to the subclass
they represent. Each table also must contain a primary key column (or
columns), and in most cases a foreign key reference to the parent table. It is
standard practice that the same column is used for both of these roles, and
that the column is also named the same as that of the parent table. However
this is optional in SQLAlchemy; separate columns may be used for primary key
and parent-relationship, the column may be named differently than that of the
parent, and even a custom join condition can be specified between parent and
child tables instead of using a foreign key::

    engineers = Table('engineers', metadata,
       Column('employee_id', Integer, 
                        ForeignKey('employees.employee_id'), 
                        primary_key=True),
       Column('engineer_info', String(50)),
    )

    managers = Table('managers', metadata,
       Column('employee_id', Integer, 
                        ForeignKey('employees.employee_id'), 
                        primary_key=True),
       Column('manager_data', String(50)),
    )

One natural effect of the joined table inheritance configuration is that the
identity of any mapped object can be determined entirely from the base table.
This has obvious advantages, so SQLAlchemy always considers the primary key
columns of a joined inheritance class to be those of the base table only,
unless otherwise manually configured. In other words, the ``employee_id``
column of both the ``engineers`` and ``managers`` table is not used to locate
the ``Engineer`` or ``Manager`` object itself - only the value in
``employees.employee_id`` is considered, and the primary key in this case is
non-composite. ``engineers.employee_id`` and ``managers.employee_id`` are
still of course critical to the proper operation of the pattern overall as
they are used to locate the joined row, once the parent row has been
determined, either through a distinct SELECT statement or all at once within a
JOIN.

We then configure mappers as usual, except we use some additional arguments to
indicate the inheritance relationship, the polymorphic discriminator column,
and the **polymorphic identity** of each class; this is the value that will be
stored in the polymorphic discriminator column.

.. sourcecode:: python+sql

    mapper(Employee, employees, polymorphic_on=employees.c.type, 
                                polymorphic_identity='employee')
    mapper(Engineer, engineers, inherits=Employee, 
                                polymorphic_identity='engineer')
    mapper(Manager, managers, inherits=Employee, 
                                polymorphic_identity='manager')

And that's it. Querying against ``Employee`` will return a combination of
``Employee``, ``Engineer`` and ``Manager`` objects. Newly saved ``Engineer``,
``Manager``, and ``Employee`` objects will automatically populate the
``employees.type`` column with ``engineer``, ``manager``, or ``employee``, as
appropriate.

.. _with_polymorphic:

Basic Control of Which Tables are Queried
++++++++++++++++++++++++++++++++++++++++++

The :func:`~sqlalchemy.orm.query.Query.with_polymorphic` method of
:class:`~sqlalchemy.orm.query.Query` affects the specific subclass tables
which the Query selects from. Normally, a query such as this:

.. sourcecode:: python+sql

    session.query(Employee).all()

...selects only from the ``employees`` table. When loading fresh from the
database, our joined-table setup will query from the parent table only, using
SQL such as this:

.. sourcecode:: python+sql

    {opensql}
    SELECT employees.employee_id AS employees_employee_id, 
        employees.name AS employees_name, employees.type AS employees_type
    FROM employees
    []

As attributes are requested from those ``Employee`` objects which are
represented in either the ``engineers`` or ``managers`` child tables, a second
load is issued for the columns in that related row, if the data was not
already loaded. So above, after accessing the objects you'd see further SQL
issued along the lines of:

.. sourcecode:: python+sql

    {opensql}
    SELECT managers.employee_id AS managers_employee_id, 
        managers.manager_data AS managers_manager_data
    FROM managers
    WHERE ? = managers.employee_id
    [5]
    SELECT engineers.employee_id AS engineers_employee_id, 
        engineers.engineer_info AS engineers_engineer_info
    FROM engineers
    WHERE ? = engineers.employee_id
    [2]

This behavior works well when issuing searches for small numbers of items,
such as when using :meth:`.Query.get`, since the full range of joined tables are not
pulled in to the SQL statement unnecessarily. But when querying a larger span
of rows which are known to be of many types, you may want to actively join to
some or all of the joined tables. The ``with_polymorphic`` feature of
:class:`~sqlalchemy.orm.query.Query` and ``mapper`` provides this.

Telling our query to polymorphically load ``Engineer`` and ``Manager``
objects:

.. sourcecode:: python+sql

    query = session.query(Employee).with_polymorphic([Engineer, Manager])

produces a query which joins the ``employees`` table to both the ``engineers`` and ``managers`` tables like the following:

.. sourcecode:: python+sql

    query.all()
    {opensql}
    SELECT employees.employee_id AS employees_employee_id, 
        engineers.employee_id AS engineers_employee_id, 
        managers.employee_id AS managers_employee_id, 
        employees.name AS employees_name, 
        employees.type AS employees_type, 
        engineers.engineer_info AS engineers_engineer_info, 
        managers.manager_data AS managers_manager_data
    FROM employees 
        LEFT OUTER JOIN engineers 
        ON employees.employee_id = engineers.employee_id 
        LEFT OUTER JOIN managers 
        ON employees.employee_id = managers.employee_id
    []

:func:`~sqlalchemy.orm.query.Query.with_polymorphic` accepts a single class or
mapper, a list of classes/mappers, or the string ``'*'`` to indicate all
subclasses:

.. sourcecode:: python+sql

    # join to the engineers table
    query.with_polymorphic(Engineer)

    # join to the engineers and managers tables
    query.with_polymorphic([Engineer, Manager])

    # join to all subclass tables
    query.with_polymorphic('*')

It also accepts a second argument ``selectable`` which replaces the automatic
join creation and instead selects directly from the selectable given. This
feature is normally used with "concrete" inheritance, described later, but can
be used with any kind of inheritance setup in the case that specialized SQL
should be used to load polymorphically:

.. sourcecode:: python+sql

    # custom selectable
    query.with_polymorphic(
                [Engineer, Manager], 
                employees.outerjoin(managers).outerjoin(engineers)
            )

:func:`~sqlalchemy.orm.query.Query.with_polymorphic` is also needed
when you wish to add filter criteria that are specific to one or more
subclasses; it makes the subclasses' columns available to the WHERE clause:

.. sourcecode:: python+sql

    session.query(Employee).with_polymorphic([Engineer, Manager]).\
        filter(or_(Engineer.engineer_info=='w', Manager.manager_data=='q'))

Note that if you only need to load a single subtype, such as just the
``Engineer`` objects, :func:`~sqlalchemy.orm.query.Query.with_polymorphic` is
not needed since you would query against the ``Engineer`` class directly.

The mapper also accepts ``with_polymorphic`` as a configurational argument so
that the joined-style load will be issued automatically. This argument may be
the string ``'*'``, a list of classes, or a tuple consisting of either,
followed by a selectable.

.. sourcecode:: python+sql

    mapper(Employee, employees, polymorphic_on=employees.c.type, 
                                polymorphic_identity='employee', 
                                with_polymorphic='*')
    mapper(Engineer, engineers, inherits=Employee, 
                                polymorphic_identity='engineer')
    mapper(Manager, managers, inherits=Employee, 
                                polymorphic_identity='manager')

The above mapping will produce a query similar to that of
``with_polymorphic('*')`` for every query of ``Employee`` objects.

Using :func:`~sqlalchemy.orm.query.Query.with_polymorphic` with
:class:`~sqlalchemy.orm.query.Query` will override the mapper-level
``with_polymorphic`` setting.

Advanced Control of Which Tables are Queried
+++++++++++++++++++++++++++++++++++++++++++++

The :meth:`.Query.with_polymorphic` method and configuration works fine for
simplistic scenarios. However, it currently does not work with any
:class:`.Query` that selects against individual columns or against multiple
classes - it also has to be called at the outset of a query.

For total control of how :class:`.Query` joins along inheritance relationships,
use the :class:`.Table` objects directly and construct joins manually.  For example, to 
query the name of employees with particular criterion::

    session.query(Employee.name).\
        outerjoin((engineer, engineer.c.employee_id==Employee.employee_id)).\
        outerjoin((manager, manager.c.employee_id==Employee.employee_id)).\
        filter(or_(Engineer.engineer_info=='w', Manager.manager_data=='q'))

The base table, in this case the "employees" table, isn't always necessary. A
SQL query is always more efficient with fewer joins. Here, if we wanted to
just load information specific to managers or engineers, we can instruct
:class:`.Query` to use only those tables. The ``FROM`` clause is determined by
what's specified in the :meth:`.Session.query`, :meth:`.Query.filter`, or
:meth:`.Query.select_from` methods::

    session.query(Manager.manager_data).select_from(manager)

    session.query(engineer.c.id).\
            filter(engineer.c.engineer_info==manager.c.manager_data)

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

    companies = Table('companies', metadata,
       Column('company_id', Integer, primary_key=True),
       Column('name', String(50))
       )

    employees = Table('employees', metadata,
      Column('employee_id', Integer, primary_key=True),
      Column('name', String(50)),
      Column('type', String(30), nullable=False),
      Column('company_id', Integer, ForeignKey('companies.company_id'))
    )

    class Company(object):
        pass

    mapper(Company, companies, properties={
        'employees': relationship(Employee)
    })

When querying from ``Company`` onto the ``Employee`` relationship, the
``join()`` method as well as the ``any()`` and ``has()`` operators will create
a join from ``companies`` to ``employees``, without including ``engineers`` or
``managers`` in the mix. If we wish to have criterion which is specifically
against the ``Engineer`` class, we can tell those methods to join or subquery
against the joined table representing the subclass using the
:func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` operator::

    session.query(Company).\
        join(Company.employees.of_type(Engineer)).\
        filter(Engineer.engineer_info=='someinfo')

A longhand version of this would involve spelling out the full target
selectable within a 2-tuple::

    session.query(Company).\
        join((employees.join(engineers), Company.employees)).\
        filter(Engineer.engineer_info=='someinfo')

Currently, :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` accepts a
single class argument. It may be expanded later on to accept multiple classes.
For now, to join to any group of subclasses, the longhand notation allows this
flexibility:

.. sourcecode:: python+sql

    session.query(Company).\
        join(
            (employees.outerjoin(engineers).outerjoin(managers), 
            Company.employees)
        ).\
        filter(
            or_(Engineer.engineer_info=='someinfo', 
                Manager.manager_data=='somedata')
        )

The ``any()`` and ``has()`` operators also can be used with
:func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` when the embedded
criterion is in terms of a subclass:

.. sourcecode:: python+sql

    session.query(Company).\
            filter(
                Company.employees.of_type(Engineer).
                    any(Engineer.engineer_info=='someinfo')
                ).all()

Note that the ``any()`` and ``has()`` are both shorthand for a correlated
EXISTS query. To build one by hand looks like:

.. sourcecode:: python+sql

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

    employees_table = Table('employees', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('manager_data', String(50)),
        Column('engineer_info', String(50)),
        Column('type', String(20), nullable=False)
    )

    employee_mapper = mapper(Employee, employees_table, \
        polymorphic_on=employees_table.c.type, polymorphic_identity='employee')
    manager_mapper = mapper(Manager, inherits=employee_mapper, 
                                        polymorphic_identity='manager')
    engineer_mapper = mapper(Engineer, inherits=employee_mapper, 
                                        polymorphic_identity='engineer')

Note that the mappers for the derived classes Manager and Engineer omit the
specification of their associated table, as it is inherited from the
employee_mapper. Omitting the table specification for derived mappers in
single-table inheritance is required.

.. _concrete_inheritance:

Concrete Table Inheritance
--------------------------

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

As of 0.7.3, the :ref:`declarative_toplevel` module includes helpers for concrete inheritance.
See :ref:`declarative_concrete_helpers` for more information.

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

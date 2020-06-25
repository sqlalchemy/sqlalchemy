.. _inheritance_loading_toplevel:

.. currentmodule:: sqlalchemy.orm

Loading Inheritance Hierarchies
===============================

When classes are mapped in inheritance hierarchies using the "joined",
"single", or "concrete" table inheritance styles as described at
:ref:`inheritance_toplevel`, the usual behavior is that a query for a
particular base class will also yield objects corresponding to subclasses
as well.   When a single query is capable of returning a result with
a different class or subclasses per result row, we use the term
"polymorphic loading".

Within the realm of polymorphic loading, specifically with joined and single
table inheritance, there is an additional problem of which subclass attributes
are to be queried up front, and which are to be loaded later. When an attribute
of a particular subclass is queried up front, we can use it in our query as
something to filter on, and it also will be loaded when we get our objects
back.   If it's not queried up front, it gets loaded later when we first need
to access it.   Basic control of this behavior is provided using the
:func:`_orm.with_polymorphic` function, as well as two variants, the mapper
configuration :paramref:`.mapper.with_polymorphic` in conjunction with
the :paramref:`.mapper.polymorphic_load` option, and the :class:`_query.Query`
-level :meth:`_query.Query.with_polymorphic` method.    The "with_polymorphic" family
each provide a means of specifying which specific subclasses of a particular
base class should be included within a query, which implies what columns and
tables will be available in the SELECT.

.. _with_polymorphic:

Using with_polymorphic
----------------------

For the following sections, assume the ``Employee`` / ``Engineer`` / ``Manager``
examples introduced in :ref:`inheritance_toplevel`.

Normally, when a :class:`_query.Query` specifies the base class of an
inheritance hierarchy, only the columns that are local to that base
class are queried::

    session.query(Employee).all()

Above, for both single and joined table inheritance, only the columns
local to ``Employee`` will be present in the SELECT.   We may get back
instances of ``Engineer`` or ``Manager``, however they will not have the
additional attributes loaded until we first access them, at which point a
lazy load is emitted.

Similarly, if we wanted to refer to columns mapped
to ``Engineer`` or ``Manager`` in our query that's against ``Employee``,
these columns aren't available directly in either the single or joined table
inheritance case, since the ``Employee`` entity does not refer to these columns
(note that for single-table inheritance, this is common if Declarative is used,
but not for a classical mapping).

To solve both of these issues, the :func:`_orm.with_polymorphic` function
provides a special :class:`.AliasedClass` that represents a range of
columns across subclasses. This object can be used in a :class:`_query.Query`
like any other alias.  When queried, it represents all the columns present in
the classes given::

    from sqlalchemy.orm import with_polymorphic

    eng_plus_manager = with_polymorphic(Employee, [Engineer, Manager])

    query = session.query(eng_plus_manager)

If the above mapping were using joined table inheritance, the SELECT
statement for the above would be:

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

Where above, the additional tables / columns for "engineer" and "manager" are
included.  Similar behavior occurs in the case of single table inheritance.

:func:`_orm.with_polymorphic` accepts a single class or
mapper, a list of classes/mappers, or the string ``'*'`` to indicate all
subclasses:

.. sourcecode:: python+sql

    # include columns for Engineer
    entity = with_polymorphic(Employee, Engineer)

    # include columns for Engineer, Manager
    entity = with_polymorphic(Employee, [Engineer, Manager])

    # include columns for all mapped subclasses
    entity = with_polymorphic(Employee, '*')

Using aliasing with with_polymorphic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.with_polymorphic` function also provides "aliasing" of the
polymorphic selectable itself, meaning, two different :func:`_orm.with_polymorphic`
entities, referring to the same class hierarchy, can be used together.  This
is available using the :paramref:`.orm.with_polymorphic.aliased` flag.
For a polymorphic selectable that is across multiple tables, the default behavior
is to wrap the selectable into a subquery.  Below we emit a query that will
select for "employee or manager" paired with "employee or engineer" on employees
with the same name:

.. sourcecode:: python+sql

    engineer_employee = with_polymorphic(
        Employee, [Engineer], aliased=True)
    manager_employee = with_polymorphic(
        Employee, [Manager], aliased=True)

    q = s.query(engineer_employee, manager_employee).\
        join(
            manager_employee,
            and_(
                engineer_employee.id > manager_employee.id,
                engineer_employee.name == manager_employee.name
            )
    )
    q.all()
    {opensql}
    SELECT
        anon_1.employee_id AS anon_1_employee_id,
        anon_1.employee_name AS anon_1_employee_name,
        anon_1.employee_type AS anon_1_employee_type,
        anon_1.engineer_id AS anon_1_engineer_id,
        anon_1.engineer_engineer_name AS anon_1_engineer_engineer_name,
        anon_2.employee_id AS anon_2_employee_id,
        anon_2.employee_name AS anon_2_employee_name,
        anon_2.employee_type AS anon_2_employee_type,
        anon_2.manager_id AS anon_2_manager_id,
        anon_2.manager_manager_name AS anon_2_manager_manager_name
    FROM (
        SELECT
            employee.id AS employee_id,
            employee.name AS employee_name,
            employee.type AS employee_type,
            engineer.id AS engineer_id,
            engineer.engineer_name AS engineer_engineer_name
        FROM employee
        LEFT OUTER JOIN engineer ON employee.id = engineer.id
    ) AS anon_1
    JOIN (
        SELECT
            employee.id AS employee_id,
            employee.name AS employee_name,
            employee.type AS employee_type,
            manager.id AS manager_id,
             manager.manager_name AS manager_manager_name
        FROM employee
        LEFT OUTER JOIN manager ON employee.id = manager.id
    ) AS anon_2
    ON anon_1.employee_id > anon_2.employee_id
    AND anon_1.employee_name = anon_2.employee_name

The creation of subqueries above is very verbose.  While it creates the best
encapsulation of the two distinct queries, it may be inefficient.
:func:`_orm.with_polymorphic` includes an additional flag to help with this
situation, :paramref:`.orm.with_polymorphic.flat`, which will "flatten" the
subquery / join combination into straight joins, applying aliasing to the
individual tables instead.   Setting :paramref:`.orm.with_polymorphic.flat`
implies :paramref:`.orm.with_polymorphic.aliased`, so only one flag
is necessary:

.. sourcecode:: python+sql

    engineer_employee = with_polymorphic(
        Employee, [Engineer], flat=True)
    manager_employee = with_polymorphic(
        Employee, [Manager], flat=True)

    q = s.query(engineer_employee, manager_employee).\
        join(
            manager_employee,
            and_(
                engineer_employee.id > manager_employee.id,
                engineer_employee.name == manager_employee.name
            )
    )
    q.all()
    {opensql}
    SELECT
        employee_1.id AS employee_1_id,
        employee_1.name AS employee_1_name,
        employee_1.type AS employee_1_type,
        engineer_1.id AS engineer_1_id,
        engineer_1.engineer_name AS engineer_1_engineer_name,
        employee_2.id AS employee_2_id,
        employee_2.name AS employee_2_name,
        employee_2.type AS employee_2_type,
        manager_1.id AS manager_1_id,
        manager_1.manager_name AS manager_1_manager_name
    FROM employee AS employee_1
    LEFT OUTER JOIN engineer AS engineer_1
    ON employee_1.id = engineer_1.id
    JOIN (
        employee AS employee_2
        LEFT OUTER JOIN manager AS manager_1
        ON employee_2.id = manager_1.id
    )
    ON employee_1.id > employee_2.id
    AND employee_1.name = employee_2.name

Note above, when using :paramref:`.orm.with_polymorphic.flat`, it is often the
case when used in conjunction with joined table inheritance that we get a
right-nested JOIN in our statement.   Some older databases, in particular older
versions of SQLite, may have a problem with this syntax, although virtually all
modern database versions now support this syntax.

.. note::

    The :paramref:`.orm.with_polymorphic.flat` flag only applies to the use
    of :paramref:`.with_polymorphic` with **joined table inheritance** and when
    the :paramref:`.with_polymorphic.selectable` argument is **not** used.

Referring to Specific Subclass Attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The entity returned by :func:`_orm.with_polymorphic` is an :class:`.AliasedClass`
object, which can be used in a :class:`_query.Query` like any other alias, including
named attributes for those attributes on the ``Employee`` class.   In our
previous example, ``eng_plus_manager`` becomes the entity that we use to refer to the
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

.. _with_polymorphic_mapper_config:

Setting with_polymorphic at mapper configuration time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.with_polymorphic` function serves the purpose of allowing
"eager" loading of attributes from subclass tables, as well as the ability
to refer to the attributes from subclass tables at query time.   Historically,
the "eager loading" of columns has been the more important part of the
equation.   So just as eager loading for relationships can be specified
as a configurational option, the :paramref:`.mapper.with_polymorphic`
configuration parameter allows an entity to use a polymorphic load by
default.  We can add the parameter to our ``Employee`` mapping
first introduced at :ref:`joined_inheritance`::

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        type = Column(String(50))

        __mapper_args__ = {
            'polymorphic_identity':'employee',
            'polymorphic_on':type,
            'with_polymorphic': '*'
        }

Above is a common setting for :paramref:`.mapper.with_polymorphic`,
which is to indicate an asterisk to load all subclass columns.   In the
case of joined table inheritance, this option
should be used sparingly, as it implies that the mapping will always emit
a (often large) series of LEFT OUTER JOIN to many tables, which is not
efficient from a SQL perspective.   For single table inheritance, specifying the
asterisk is often a good idea as the load is still against a single table only,
but an additional lazy load of subclass-mapped columns will be prevented.

Using :func:`_orm.with_polymorphic` or :meth:`_query.Query.with_polymorphic`
will override the mapper-level :paramref:`.mapper.with_polymorphic` setting.

The :paramref:`.mapper.with_polymorphic` option also accepts a list of
classes just like :func:`_orm.with_polymorphic` to polymorphically load among
a subset of classes.  However, when using Declarative, providing classes
to this list is not directly possible as the subclasses we'd like to add
are not available yet.   Instead, we can specify on each subclass
that they should individually participate in polymorphic loading by
default using the :paramref:`.mapper.polymorphic_load` parameter::

    class Engineer(Employee):
        __tablename__ = 'engineer'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        engineer_info = Column(String(50))
        __mapper_args__ = {
            'polymorphic_identity':'engineer',
            'polymorphic_load': 'inline'
        }

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        manager_data = Column(String(50))
        __mapper_args__ = {
            'polymorphic_identity':'manager',
            'polymorphic_load': 'inline'
        }

Setting the :paramref:`.mapper.polymorphic_load` parameter to the value
``"inline"`` means that the ``Engineer`` and ``Manager`` classes above
are part of the "polymorphic load" of the base ``Employee`` class by default,
exactly as though they had been appended to the
:paramref:`.mapper.with_polymorphic` list of classes.

Setting with_polymorphic against a query
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.with_polymorphic` function evolved from a query-level
method :meth:`_query.Query.with_polymorphic`.  This method has the same purpose
as :func:`_orm.with_polymorphic`, except is not as
flexible in its usage patterns in that it only applies to the first entity
of the :class:`_query.Query`.   It then takes effect for all occurrences of
that entity, so that the entity (and its subclasses) can be referred to
directly, rather than using an alias object.  For simple cases it might be
considered to be more succinct::

    session.query(Employee).\
        with_polymorphic([Engineer, Manager]).\
        filter(
            or_(
                Engineer.engineer_info=='w',
                Manager.manager_data=='q'
            )
        )

The :meth:`_query.Query.with_polymorphic` method has a more complicated job
than the :func:`_orm.with_polymorphic` function, as it needs to correctly
transform entities like ``Engineer`` and ``Manager`` appropriately, but
not interfere with other entities.  If its flexibility is lacking, switch
to using :func:`_orm.with_polymorphic`.

.. _polymorphic_selectin:

Polymorphic Selectin Loading
----------------------------

An alternative to using the :func:`_orm.with_polymorphic` family of
functions to "eagerly" load the additional subclasses on an inheritance
mapping, primarily when using joined table inheritance, is to use polymorphic
"selectin" loading.   This is an eager loading
feature which works similarly to the :ref:`selectin_eager_loading` feature
of relationship loading.   Given our example mapping, we can instruct
a load of ``Employee`` to emit an extra SELECT per subclass by using
the :func:`_orm.selectin_polymorphic` loader option::

    from sqlalchemy.orm import selectin_polymorphic

    query = session.query(Employee).options(
        selectin_polymorphic(Employee, [Manager, Engineer])
    )

When the above query is run, two additional SELECT statements will
be emitted:

.. sourcecode:: python+sql

    {opensql}query.all()
    SELECT
        employee.id AS employee_id,
        employee.name AS employee_name,
        employee.type AS employee_type
    FROM employee
    ()

    SELECT
        engineer.id AS engineer_id,
        employee.id AS employee_id,
        employee.type AS employee_type,
        engineer.engineer_name AS engineer_engineer_name
    FROM employee JOIN engineer ON employee.id = engineer.id
    WHERE employee.id IN (?, ?) ORDER BY employee.id
    (1, 2)

    SELECT
        manager.id AS manager_id,
        employee.id AS employee_id,
        employee.type AS employee_type,
        manager.manager_name AS manager_manager_name
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?) ORDER BY employee.id
    (3,)

We can similarly establish the above style of loading to take place
by default by specifying the :paramref:`.mapper.polymorphic_load` parameter,
using the value ``"selectin"`` on a per-subclass basis::

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        type = Column(String(50))

        __mapper_args__ = {
            'polymorphic_identity': 'employee',
            'polymorphic_on': type
        }

    class Engineer(Employee):
        __tablename__ = 'engineer'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        engineer_name = Column(String(30))

        __mapper_args__ = {
            'polymorphic_load': 'selectin',
            'polymorphic_identity': 'engineer',
        }

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        manager_name = Column(String(30))

        __mapper_args__ = {
            'polymorphic_load': 'selectin',
            'polymorphic_identity': 'manager',
        }


Unlike when using :func:`_orm.with_polymorphic`, when using the
:func:`_orm.selectin_polymorphic` style of loading, we do **not** have the
ability to refer to the ``Engineer`` or ``Manager`` entities within our main
query as filter, order by, or other criteria, as these entities are not present
in the initial query that is used to locate results.   However, we can apply
loader options that apply towards ``Engineer`` or ``Manager``, which will take
effect when the secondary SELECT is emitted.  Below we assume ``Manager`` has
an additional relationship ``Manager.paperwork``, that we'd like to eagerly
load as well.  We can use any type of eager loading, such as joined eager
loading via the :func:`_orm.joinedload` function::

    from sqlalchemy.orm import joinedload
    from sqlalchemy.orm import selectin_polymorphic

    query = session.query(Employee).options(
        selectin_polymorphic(Employee, [Manager, Engineer]),
        joinedload(Manager.paperwork)
    )

Using the query above, we get three SELECT statements emitted, however
the one against ``Manager`` will be:

.. sourcecode:: sql

    SELECT
        manager.id AS manager_id,
        employee.id AS employee_id,
        employee.type AS employee_type,
        manager.manager_name AS manager_manager_name,
        paperwork_1.id AS paperwork_1_id,
        paperwork_1.manager_id AS paperwork_1_manager_id,
        paperwork_1.data AS paperwork_1_data
    FROM employee JOIN manager ON employee.id = manager.id
    LEFT OUTER JOIN paperwork AS paperwork_1
    ON manager.id = paperwork_1.manager_id
    WHERE employee.id IN (?) ORDER BY employee.id
    (3,)

Note that selectin polymorphic loading has similar caveats as that of
selectin relationship loading; for entities that make use of a composite
primary key, the database in use must support tuples with "IN", currently
known to work with MySQL and PostgreSQL.

.. versionadded:: 1.2

.. warning::  The selectin polymorphic loading feature should be considered
   as **experimental** within early releases of the 1.2 series.

.. _polymorphic_selectin_and_withpoly:

Combining selectin and with_polymorphic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: works as of 1.2.0b3

With careful planning, selectin loading can be applied against a hierarchy
that itself uses "with_polymorphic".  A particular use case is that of
using selectin loading to load a joined-inheritance subtable, which then
uses "with_polymorphic" to refer to further sub-classes, which may be
joined- or single-table inheritance.  If we added a class ``VicePresident`` that
extends ``Manager`` using single-table inheritance, we could ensure that
a load of ``Manager`` also fully loads ``VicePresident`` subtypes at the same time::

    # use "Employee" example from the enclosing section

    class Manager(Employee):
        __tablename__ = 'manager'
        id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
        manager_name = Column(String(30))

        __mapper_args__ = {
            'polymorphic_load': 'selectin',
            'polymorphic_identity': 'manager',
        }

    class VicePresident(Manager):
        vp_info = Column(String(30))

        __mapper_args__ = {
            "polymorphic_load": "inline",
            "polymorphic_identity": "vp"
        }


Above, we add a ``vp_info`` column to the ``manager`` table, local to the
``VicePresident`` subclass.   This subclass is linked to the polymorphic
identity ``"vp"`` which refers to rows which have this data.     By setting the
load style to "inline", it means that a load of ``Manager`` objects will also
ensure that the ``vp_info`` column is queried for in the same SELECT statement.
A query against ``Employee`` that encounters a ``Manager`` row would emit
similarly to the following:

.. sourcecode:: sql

    SELECT employee.id AS employee_id, employee.name AS employee_name,
           employee.type AS employee_type
    FROM employee
    )

    SELECT manager.id AS manager_id, employee.id AS employee_id,
           employee.type AS employee_type,
           manager.manager_name AS manager_manager_name,
           manager.vp_info AS manager_vp_info
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?) ORDER BY employee.id
    (1,)

Combining "selectin" polymorphic loading with query-time
:func:`_orm.with_polymorphic` usage is also possible (though this is very
outer-space stuff!); assuming the above mappings had no ``polymorphic_load``
set up, we could get the same result as follows::

    from sqlalchemy.orm import with_polymorphic, selectin_polymorphic

    manager_poly = with_polymorphic(Manager, [VicePresident])

    s.query(Employee).options(
        selectin_polymorphic(Employee, [manager_poly])).all()

.. _inheritance_of_type:

Referring to specific subtypes on relationships
-----------------------------------------------

Mapped attributes which correspond to a :func:`_orm.relationship` are used
in querying in order to refer to the linkage between two mappings.  Common
uses for this are to refer to a :func:`_orm.relationship` in :meth:`_query.Query.join`
as well as in loader options like :func:`_orm.joinedload`.   When using
:func:`_orm.relationship` where the target class is an inheritance hierarchy,
the API allows that the join, eager load, or other linkage should target a specific
subclass, alias, or :func:`_orm.with_polymorphic` alias, of that class hierarchy,
rather than the class directly targeted by the :func:`_orm.relationship`.

The :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` method allows the
construction of joins along :func:`~sqlalchemy.orm.relationship` paths while
narrowing the criterion to specific derived aliases or subclasses. Suppose the
``employees`` table represents a collection of employees which are associated
with a ``Company`` object. We'll add a ``company_id`` column to the
``employees`` table and a new table ``companies``:

.. sourcecode:: python

    class Company(Base):
        __tablename__ = 'company'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        employees = relationship("Employee",
                        backref='company')

    class Employee(Base):
        __tablename__ = 'employee'
        id = Column(Integer, primary_key=True)
        type = Column(String(20))
        company_id = Column(Integer, ForeignKey('company.id'))
        __mapper_args__ = {
            'polymorphic_on':type,
            'polymorphic_identity':'employee',
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
:meth:`_query.Query.join` method as well as operators like :meth:`.PropComparator.any`
and :meth:`.PropComparator.has` will create
a join from ``company`` to ``employee``, without including ``engineer`` or
``manager`` in the mix. If we wish to have criterion which is specifically
against the ``Engineer`` class, we can tell those methods to join or subquery
against the set of columns representing the subclass using the
:meth:`~.orm.interfaces.PropComparator.of_type` operator::

    session.query(Company).\
        join(Company.employees.of_type(Engineer)).\
        filter(Engineer.engineer_info=='someinfo')

Similarly, to join from ``Company`` to the polymorphic entity that includes both
``Engineer`` and ``Manager`` columns::

    manager_and_engineer = with_polymorphic(
                                Employee, [Manager, Engineer])

    session.query(Company).\
        join(Company.employees.of_type(manager_and_engineer)).\
        filter(
            or_(
                manager_and_engineer.Engineer.engineer_info == 'someinfo',
                manager_and_engineer.Manager.manager_data == 'somedata'
            )
        )

The :meth:`.PropComparator.any` and :meth:`.PropComparator.has` operators also
can be used with :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type`,
such as when the embedded criterion is in terms of a subclass::

    session.query(Company).\
            filter(
                Company.employees.of_type(Engineer).
                    any(Engineer.engineer_info=='someinfo')
                ).all()

.. _eagerloading_polymorphic_subtypes:

Eager Loading of Specific or Polymorphic Subtypes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.joinedload`, :func:`.subqueryload`, :func:`.contains_eager` and
other eagerloader options support
paths which make use of :func:`~.PropComparator.of_type`.
Below, we load ``Company`` rows while eagerly loading related ``Engineer``
objects, querying the ``employee`` and ``engineer`` tables simultaneously::

    session.query(Company).\
        options(
            subqueryload(Company.employees.of_type(Engineer)).
            subqueryload(Engineer.machines)
            )
        )

As is the case with :meth:`_query.Query.join`, :meth:`~.PropComparator.of_type`
can be used to combine eager loading and :func:`_orm.with_polymorphic`,
so that all sub-attributes of all referenced subtypes
can be loaded::

    manager_and_engineer = with_polymorphic(
                                Employee, [Manager, Engineer],
                                flat=True)

    session.query(Company).\
        options(
            joinedload(
                Company.employees.of_type(manager_and_engineer)
            )
        )

.. note::

    When using :func:`.with_polymorphic` in conjunction with
    :func:`_orm.joinedload`, the :func:`.with_polymorphic` object must be against
    an "aliased" object, that is an instance of :class:`_expression.Alias`, so that the
    polymorphic selectable is aliased (an informative error message is raised
    otherwise).

    The typical way to do this is to include the
    :paramref:`.with_polymorphic.aliased` or :paramref:`.flat` flag, which will
    apply this aliasing automatically.  However, if the
    :paramref:`.with_polymorphic.selectable` argument is being used to pass an
    object that is already an :class:`_expression.Alias` object then this flag should
    **not** be set.  The "flat" option implies the "aliased" option and is an
    alternate form of aliasing against join objects that produces fewer
    subqueries.

Once :meth:`~.PropComparator.of_type` is the target of the eager load,
that's the entity we would use for subsequent chaining, not the original class
or derived class.  If we wanted to further eager load a collection on the
eager-loaded ``Engineer`` class, we access this class from the namespace of the
:func:`_orm.with_polymorphic` object::

    session.query(Company).\
        options(
            joinedload(Company.employees.of_type(manager_and_engineer)).\
            subqueryload(manager_and_engineer.Engineer.computers)
            )
        )

.. _loading_joined_inheritance:

Loading objects with joined table inheritance
---------------------------------------------

When using joined table inheritance, if we query for a specific subclass
that represents a JOIN of two tables such as our ``Engineer`` example
from the inheritance section, the SQL emitted is a join::

    session.query(Engineer).all()

The above query will emit SQL like:

.. sourcecode:: python+sql

    {opensql}
    SELECT employee.id AS employee_id,
        employee.name AS employee_name, employee.type AS employee_type,
        engineer.name AS engineer_name
    FROM employee JOIN engineer
    ON employee.id = engineer.id

We will then get a collection of ``Engineer`` objects back, which will
contain all columns from ``employee`` and ``engineer`` loaded.

However, when emitting a :class:`_query.Query` against a base class, the behavior
is to load only from the base table::

    session.query(Employee).all()

Above, the default behavior would be to SELECT only from the ``employee``
table and not from any "sub" tables (``engineer`` and ``manager``, in our
previous examples):

.. sourcecode:: python+sql

    {opensql}
    SELECT employee.id AS employee_id,
        employee.name AS employee_name, employee.type AS employee_type
    FROM employee
    []

After a collection of ``Employee`` objects has been returned from the
query, and as attributes are requested from those ``Employee`` objects which are
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

The :func:`_orm.with_polymorphic`
function and related configuration options allow us to instead emit a JOIN up
front which will conditionally load against ``employee``, ``engineer``, or
``manager``, very much like joined eager loading works for relationships,
removing the necessity for a second per-entity load::

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

The section :ref:`with_polymorphic` discusses the :func:`_orm.with_polymorphic`
function and its configurational variants.

.. seealso::

    :ref:`with_polymorphic`

.. _loading_single_inheritance:

Loading objects with single table inheritance
---------------------------------------------

In modern Declarative, single inheritance mappings produce :class:`_schema.Column`
objects that are mapped only to a subclass, and not available from the
superclass, even though they are present on the same table.
In our example from :ref:`single_inheritance`, the ``Manager`` mapping for example had a
:class:`_schema.Column` specified::

    class Manager(Employee):
        manager_data = Column(String(50))

        __mapper_args__ = {
            'polymorphic_identity':'manager'
        }

Above, there would be no ``Employee.manager_data``
attribute, even though the ``employee`` table has a ``manager_data`` column.
A query against ``Manager`` will include this column in the query, as well
as an IN clause to limit rows only to ``Manager`` objects:

.. sourcecode:: python+sql

    session.query(Manager).all()
    {opensql}
    SELECT
        employee.id AS employee_id,
        employee.name AS employee_name,
        employee.type AS employee_type,
        employee.manager_data AS employee_manager_data
    FROM employee
    WHERE employee.type IN (?)

    ('manager',)

However, in a similar way to that of joined table inheritance, a query
against ``Employee`` will only query for columns mapped to ``Employee``:

.. sourcecode:: python+sql

    session.query(Employee).all()
    {opensql}
    SELECT employee.id AS employee_id,
        employee.name AS employee_name,
        employee.type AS employee_type
    FROM employee

If we get back an instance of ``Manager`` from our result, accessing
additional columns only mapped to ``Manager`` emits a lazy load
for those columns, in a similar way to joined inheritance::

    SELECT employee.manager_data AS employee_manager_data
    FROM employee
    WHERE employee.id = ? AND employee.type IN (?)

The :func:`_orm.with_polymorphic` function serves a similar role as  joined
inheritance in the case of single inheritance; it allows both for eager loading
of subclass attributes as well as specification of subclasses in a query,
just without the overhead of using OUTER JOIN::

    employee_poly = with_polymorphic(Employee, '*')

    q = session.query(employee_poly).filter(
        or_(
            employee_poly.name == 'a',
            employee_poly.Manager.manager_data == 'b'
        )
    )

Above, our query remains against a single table however we can refer to the
columns present in ``Manager`` or ``Engineer`` using the "polymorphic" namespace.
Since we specified ``"*"`` for the entities, both ``Engineer`` and
``Manager`` will be loaded at once.  SQL emitted would be:

.. sourcecode:: python+sql

    q.all()
    {opensql}
    SELECT
        employee.id AS employee_id, employee.name AS employee_name,
        employee.type AS employee_type,
        employee.manager_data AS employee_manager_data,
        employee.engineer_info AS employee_engineer_info
    FROM employee
    WHERE employee.name = :name_1
    OR employee.manager_data = :manager_data_1


Inheritance Loading API
-----------------------

.. autofunction:: sqlalchemy.orm.with_polymorphic

.. autofunction:: sqlalchemy.orm.selectin_polymorphic

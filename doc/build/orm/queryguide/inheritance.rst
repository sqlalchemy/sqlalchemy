.. highlight:: pycon+sql
.. |prev| replace:: :doc:`select`
.. |next| replace:: :doc:`dml`

.. include:: queryguide_nav_include.rst

.. doctest-include _inheritance_setup.rst

.. _inheritance_loading_toplevel:


.. currentmodule:: sqlalchemy.orm

.. _loading_joined_inheritance:

Writing SELECT statements for Inheritance Mappings
==================================================

.. admonition:: About this Document

    This section makes use of ORM mappings configured using
    the :ref:`ORM Inheritance <inheritance_toplevel>` feature,
    described at :ref:`inheritance_toplevel`.  The emphasis will be on
    :ref:`joined_inheritance` as this is the most intricate ORM querying
    case.

    :doc:`View the ORM setup for this page <_inheritance_setup>`.

SELECTing from the base class vs. specific sub-classes
------------------------------------------------------

A SELECT statement constructed against a class in a joined inheritance
hierarchy will query against the table to which the class is mapped, as well as
any super-tables present, using JOIN to link them together. The query would
then return objects that are of that requested type as well as any sub-types of
the requested type,  using the :term:`discriminator` value in each row
to determine the correct type. The query below is established against the ``Manager``
subclass of ``Employee``, which then returns a result that will contain only
objects of type ``Manager``::

    >>> from sqlalchemy import select
    >>> stmt = select(Manager).order_by(Manager.id)
    >>> managers = session.scalars(stmt).all()
    {execsql}BEGIN (implicit)
    SELECT manager.id, employee.id AS id_1, employee.name, employee.type, employee.company_id, manager.manager_name
    FROM employee JOIN manager ON employee.id = manager.id ORDER BY manager.id
    [...] ()
    {stop}>>> print(managers)
    [Manager('Mr. Krabs')]

..  Setup code, not for display


    >>> session.close()
    ROLLBACK

When the SELECT statement is against the base class in the hierarchy, the
default behavior is that only that class' table will be included in the
rendered SQL and JOIN will not be used. As in all cases, the
:term:`discriminator` column is used to distinguish between different requested
sub-types, which then results in objects of any possible sub-type being
returned. The objects returned will have attributes corresponding to the base
table populated, and attributes corresponding to sub-tables will start in an
un-loaded state, loading automatically when accessed. The loading of
sub-attributes is configurable to be more "eager" in a variety of ways,
discussed later in this section.

The example below creates a query against the ``Employee`` superclass.
This indicates that objects of any type, including ``Manager``, ``Engineer``,
and ``Employee``, may be within the result set::

    >>> from sqlalchemy import select
    >>> stmt = select(Employee).order_by(Employee.id)
    >>> objects = session.scalars(stmt).all()
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type, employee.company_id
    FROM employee ORDER BY employee.id
    [...] ()
    {stop}>>> print(objects)
    [Manager('Mr. Krabs'), Engineer('SpongeBob'), Engineer('Squidward')]

Above, the additional tables for ``Manager`` and ``Engineer`` were not included
in the SELECT, which means that the returned objects will not yet contain
data represented from those tables, in this example the ``.manager_name``
attribute of the ``Manager`` class as well as the ``.engineer_info`` attribute
of the ``Engineer`` class.  These attributes start out in the
:term:`expired` state, and will automatically populate themselves when first
accessed using :term:`lazy loading`::

    >>> mr_krabs = objects[0]
    >>> print(mr_krabs.manager_name)
    {execsql}SELECT manager.manager_name AS manager_manager_name
    FROM manager
    WHERE ? = manager.id
    [...] (1,)
    {stop}Eugene H. Krabs

This lazy load behavior is not desirable if a large number of objects have been
loaded, in the case that the consuming application will need to be accessing
subclass-specific attributes, as this would be an example of the
:term:`N plus one` problem that emits additional SQL per row.  This additional SQL can
impact performance and also be incompatible with approaches such as
using :ref:`asyncio <asyncio_toplevel>`.  Additionally, in our query for
``Employee`` objects, since the query is against the base table only, we did
not have a way to add SQL criteria involving subclass-specific attributes in
terms of ``Manager`` or ``Engineer``. The next two sections detail two
constructs that provide solutions to these two issues in different ways, the
:func:`_orm.selectin_polymorphic` loader option and the
:func:`_orm.with_polymorphic` entity construct.


.. _polymorphic_selectin:

Using selectin_polymorphic()
----------------------------

..  Setup code, not for display


    >>> session.close()
    ROLLBACK

To address the issue of performance when accessing attributes on subclasses,
the :func:`_orm.selectin_polymorphic` loader strategy may be used to
:term:`eagerly load` these additional attributes up front across many
objects at once.  This loader option works in a similar fashion as the
:func:`_orm.selectinload` relationship loader strategy to emit an additional
SELECT statement against each sub-table for objects loaded in the hierarchy,
using ``IN`` to query for additional rows based on primary key.

:func:`_orm.selectin_polymorphic` accepts as its arguments the base entity that is
being queried, followed by a sequence of subclasses of that entity for which
their specific attributes should be loaded for incoming rows::

    >>> from sqlalchemy.orm import selectin_polymorphic
    >>> loader_opt = selectin_polymorphic(Employee, [Manager, Engineer])

The :func:`_orm.selectin_polymorphic` construct is then used as a loader
option, passing it to the :meth:`.Select.options` method of :class:`.Select`.
The example illustrates the use of :func:`_orm.selectin_polymorphic` to eagerly
load columns local to both the ``Manager`` and ``Engineer`` subclasses::

    >>> from sqlalchemy.orm import selectin_polymorphic
    >>> loader_opt = selectin_polymorphic(Employee, [Manager, Engineer])
    >>> stmt = select(Employee).order_by(Employee.id).options(loader_opt)
    >>> objects = session.scalars(stmt).all()
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type, employee.company_id
    FROM employee ORDER BY employee.id
    [...] ()
    SELECT manager.id AS manager_id, employee.id AS employee_id,
    employee.type AS employee_type, manager.manager_name AS manager_manager_name
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?)
    [...] (1,)
    SELECT engineer.id AS engineer_id, employee.id AS employee_id,
    employee.type AS employee_type, engineer.engineer_info AS engineer_engineer_info
    FROM employee JOIN engineer ON employee.id = engineer.id
    WHERE employee.id IN (?, ?)
    [...] (2, 3)
    {stop}>>> print(objects)
    [Manager('Mr. Krabs'), Engineer('SpongeBob'), Engineer('Squidward')]

The above example illustrates two additional SELECT statements being emitted
in order to eagerly fetch additional attributes such as ``Engineer.engineer_info``
as well as ``Manager.manager_name``.   We can now access these sub-attributes on the
objects that were loaded without any additional SQL statements being emitted::

    >>> print(objects[0].manager_name)
    Eugene H. Krabs

.. tip:: The :func:`_orm.selectin_polymorphic` loader option does not yet
   optimize for the fact that the base ``employee`` table does not need to be
   included in the second two "eager load" queries; hence in the example above
   we see a JOIN from ``employee`` to ``manager`` and ``engineer``, even though
   columns from ``employee`` are already loaded.  This is in contrast to
   the :func:`_orm.selectinload` relationship strategy which is more
   sophisticated in this regard and can factor out the JOIN when not needed.

.. _polymorphic_selectin_as_loader_option_target:

Applying selectin_polymorphic() to an existing eager load
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display


    >>> session.close()
    ROLLBACK

In addition to :func:`_orm.selectin_polymorphic` being specified as an option
for a top-level entity loaded by a statement, we may also indicate
:func:`_orm.selectin_polymorphic` on the target of an existing load.
As our :doc:`setup <_inheritance_setup>` mapping includes a parent
``Company`` entity with a ``Company.employees`` :func:`_orm.relationship`
referring to ``Employee`` entities, we may illustrate a SELECT against
the ``Company`` entity that eagerly loads all ``Employee`` objects as well as
all attributes on their subtypes as follows, by applying :meth:`.Load.selectin_polymorphic`
as a chained loader option; in this form, the first argument is implicit from
the previous loader option (in this case :func:`_orm.selectinload`), so
we only indicate the additional target subclasses we wish to load::

    >>> from sqlalchemy.orm import selectinload
    >>> stmt = select(Company).options(
    ...     selectinload(Company.employees).selectin_polymorphic([Manager, Engineer])
    ... )
    >>> for company in session.scalars(stmt):
    ...     print(f"company: {company.name}")
    ...     print(f"employees: {company.employees}")
    {execsql}BEGIN (implicit)
    SELECT company.id, company.name
    FROM company
    [...] ()
    SELECT employee.company_id, employee.id, employee.name, employee.type
    FROM employee
    WHERE employee.company_id IN (?)
    [...] (1,)
    SELECT manager.id AS manager_id, employee.id AS employee_id,
    employee.type AS employee_type,
    manager.manager_name AS manager_manager_name
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?)
    [...] (1,)
    SELECT engineer.id AS engineer_id, employee.id AS employee_id,
    employee.type AS employee_type,
    engineer.engineer_info AS engineer_engineer_info
    FROM employee JOIN engineer ON employee.id = engineer.id
    WHERE employee.id IN (?, ?)
    [...] (2, 3)
    {stop}company: Krusty Krab
    employees: [Manager('Mr. Krabs'), Engineer('SpongeBob'), Engineer('Squidward')]

.. seealso::

    :ref:`eagerloading_polymorphic_subtypes` - illustrates the equivalent example
    as above using :func:`_orm.with_polymorphic` instead


.. _polymorphic_selectin_w_loader_options:

Applying loader options to the subclasses loaded by selectin_polymorphic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The SELECT statements emitted by :func:`_orm.selectin_polymorphic` are themselves
ORM statements, so we may also add other loader options (such as those
documented at :ref:`orm_queryguide_relationship_loaders`) that refer to specific
subclasses.   These options should be applied as **siblings** to a
:func:`_orm.selectin_polymorphic` option, that is, comma separated within
:meth:`_sql.select.options`.

For example, if we considered that the ``Manager`` mapper had
a :ref:`one to many <relationship_patterns_o2m>` relationship to an entity
called ``Paperwork``, we could combine the use of
:func:`_orm.selectin_polymorphic` and :func:`_orm.selectinload` to eagerly load
this collection on all ``Manager`` objects, where the sub-attributes of
``Manager`` objects were also themselves eagerly loaded::

    >>> from sqlalchemy.orm import selectin_polymorphic
    >>> stmt = (
    ...     select(Employee)
    ...     .order_by(Employee.id)
    ...     .options(
    ...         selectin_polymorphic(Employee, [Manager, Engineer]),
    ...         selectinload(Manager.paperwork),
    ...     )
    ... )
    >>> objects = session.scalars(stmt).all()
    {execsql}SELECT employee.id, employee.name, employee.type, employee.company_id
    FROM employee ORDER BY employee.id
    [...] ()
    SELECT manager.id AS manager_id, employee.id AS employee_id, employee.type AS employee_type, manager.manager_name AS manager_manager_name
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?)
    [...] (1,)
    SELECT paperwork.manager_id, paperwork.id, paperwork.document_name
    FROM paperwork
    WHERE paperwork.manager_id IN (?)
    [...] (1,)
    SELECT engineer.id AS engineer_id, employee.id AS employee_id, employee.type AS employee_type, engineer.engineer_info AS engineer_engineer_info
    FROM employee JOIN engineer ON employee.id = engineer.id
    WHERE employee.id IN (?, ?)
    [...] (2, 3)
    {stop}>>> print(objects[0])
    Manager('Mr. Krabs')
    >>> print(objects[0].paperwork)
    [Paperwork('Secret Recipes'), Paperwork('Krabby Patty Orders')]

.. _polymorphic_selectin_as_loader_option_target_plus_opts:

Applying loader options when selectin_polymorphic is itself a sub-option
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  Setup code, not for display


    >>> session.close()
    ROLLBACK

.. versionadded:: 2.0.21

The previous section illustrated :func:`_orm.selectin_polymorphic` and
:func:`_orm.selectinload` used as sibling options, both used within a single
call to :meth:`_sql.select.options`.   If the target entity is one that is
already being loaded from a parent relationship, as in the example at
:ref:`polymorphic_selectin_as_loader_option_target`, we can apply this
"sibling" pattern using the :meth:`_orm.Load.options` method that applies
sub-options to a parent, as illustrated at
:ref:`orm_queryguide_relationship_sub_options`.  Below we combine the two
examples to load ``Company.employees``, also loading the attributes for the
``Manager`` and ``Engineer`` classes, as well as eagerly loading the
```Manager.paperwork``` attribute::

    >>> from sqlalchemy.orm import selectinload
    >>> stmt = select(Company).options(
    ...     selectinload(Company.employees).options(
    ...         selectin_polymorphic(Employee, [Manager, Engineer]),
    ...         selectinload(Manager.paperwork),
    ...     )
    ... )
    >>> for company in session.scalars(stmt):
    ...     print(f"company: {company.name}")
    ...     for employee in company.employees:
    ...         if isinstance(employee, Manager):
    ...             print(f"manager: {employee.name} paperwork: {employee.paperwork}")
    {execsql}BEGIN (implicit)
    SELECT company.id, company.name
    FROM company
    [...] ()
    SELECT employee.company_id, employee.id, employee.name, employee.type
    FROM employee
    WHERE employee.company_id IN (?)
    [...] (1,)
    SELECT manager.id AS manager_id, employee.id AS employee_id, employee.type AS employee_type, manager.manager_name AS manager_manager_name
    FROM employee JOIN manager ON employee.id = manager.id
    WHERE employee.id IN (?)
    [...] (1,)
    SELECT paperwork.manager_id, paperwork.id, paperwork.document_name
    FROM paperwork
    WHERE paperwork.manager_id IN (?)
    [...] (1,)
    SELECT engineer.id AS engineer_id, employee.id AS employee_id, employee.type AS employee_type, engineer.engineer_info AS engineer_engineer_info
    FROM employee JOIN engineer ON employee.id = engineer.id
    WHERE employee.id IN (?, ?)
    [...] (2, 3)
    {stop}company: Krusty Krab
    manager: Mr. Krabs paperwork: [Paperwork('Secret Recipes'), Paperwork('Krabby Patty Orders')]


Configuring selectin_polymorphic() on mappers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The behavior of :func:`_orm.selectin_polymorphic` may be configured on specific
mappers so that it takes place by default, by using the
:paramref:`_orm.Mapper.polymorphic_load` parameter, using the value ``"selectin"``
on a per-subclass basis.  The example below illustrates the use of this
parameter within ``Engineer`` and ``Manager`` subclasses:

.. sourcecode:: python

    class Employee(Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        type = mapped_column(String(50))

        __mapper_args__ = {"polymorphic_identity": "employee", "polymorphic_on": type}


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        engineer_info = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_load": "selectin",
            "polymorphic_identity": "engineer",
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        manager_name = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_load": "selectin",
            "polymorphic_identity": "manager",
        }

With the above mapping, SELECT statements against the ``Employee`` class will
automatically assume the use of
``selectin_polymorphic(Employee, [Engineer, Manager])`` as a loader option when the statement is
emitted.

.. _with_polymorphic:

Using with_polymorphic()
------------------------

..  Setup code, not for display


    >>> session.close()
    ROLLBACK

In contrast to :func:`_orm.selectin_polymorphic` which affects only the loading
of objects, the :func:`_orm.with_polymorphic` construct affects how the SQL
query for a polymorphic structure is rendered, most commonly as a series of
LEFT OUTER JOINs to each of the included sub-tables. This join structure is
known as the **polymorphic selectable**. By providing for a view of
several sub-tables at once, :func:`_orm.with_polymorphic` offers a means of
writing a SELECT statement across several inherited classes at once with the
ability to add filtering criteria based on individual sub-tables.

:func:`_orm.with_polymorphic` is essentially a special form of the
:func:`_orm.aliased` construct. It accepts as its arguments a similar form to
that of :func:`_orm.selectin_polymorphic`, which is the base entity that is
being queried, followed by a sequence of subclasses of that entity for which
their specific attributes should be loaded for incoming rows::

    >>> from sqlalchemy.orm import with_polymorphic
    >>> employee_poly = with_polymorphic(Employee, [Engineer, Manager])

In order to indicate that all subclasses should be part of the entity,
:func:`_orm.with_polymorphic` will also accept the string ``"*"``, which may be
passed in place of the sequence of classes to indicate all classes (note this
is not yet supported by :func:`_orm.selectin_polymorphic`)::

    >>> employee_poly = with_polymorphic(Employee, "*")

The example below illustrates the same operation as illustrated in the previous
section, to load all columns for ``Manager`` and ``Engineer`` at once::

    >>> stmt = select(employee_poly).order_by(employee_poly.id)
    >>> objects = session.scalars(stmt).all()
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type, employee.company_id,
    manager.id AS id_1, manager.manager_name, engineer.id AS id_2, engineer.engineer_info
    FROM employee
    LEFT OUTER JOIN manager ON employee.id = manager.id
    LEFT OUTER JOIN engineer ON employee.id = engineer.id ORDER BY employee.id
    [...] ()
    {stop}>>> print(objects)
    [Manager('Mr. Krabs'), Engineer('SpongeBob'), Engineer('Squidward')]

As is the case with :func:`_orm.selectin_polymorphic`, attributes on subclasses
are already loaded::

    >>> print(objects[0].manager_name)
    Eugene H. Krabs

As the default selectable produced by :func:`_orm.with_polymorphic`
uses LEFT OUTER JOIN, from a database point of view the query is not as well
optimized as the approach that :func:`_orm.selectin_polymorphic` takes,
with simple SELECT statements using only JOINs emitted on a per-table basis.


.. _with_polymorphic_subclass_attributes:

Filtering Subclass Attributes with with_polymorphic()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.with_polymorphic` construct makes available the attributes
on the included subclass mappers, by including namespaces that allow
references to subclasses.   The ``employee_poly`` construct created in the
previous section includes attributes named ``.Engineer`` and ``.Manager``
which provide the namespace for ``Engineer`` and ``Manager`` in terms of
the polymorphic SELECT.   In the example below, we can use the :func:`_sql.or_`
construct to create criteria against both classes at once::

    >>> from sqlalchemy import or_
    >>> employee_poly = with_polymorphic(Employee, [Engineer, Manager])
    >>> stmt = (
    ...     select(employee_poly)
    ...     .where(
    ...         or_(
    ...             employee_poly.Manager.manager_name == "Eugene H. Krabs",
    ...             employee_poly.Engineer.engineer_info
    ...             == "Senior Customer Engagement Engineer",
    ...         )
    ...     )
    ...     .order_by(employee_poly.id)
    ... )
    >>> objects = session.scalars(stmt).all()
    {execsql}SELECT employee.id, employee.name, employee.type, employee.company_id, manager.id AS id_1,
    manager.manager_name, engineer.id AS id_2, engineer.engineer_info
    FROM employee
    LEFT OUTER JOIN manager ON employee.id = manager.id
    LEFT OUTER JOIN engineer ON employee.id = engineer.id
    WHERE manager.manager_name = ? OR engineer.engineer_info = ?
    ORDER BY employee.id
    [...] ('Eugene H. Krabs', 'Senior Customer Engagement Engineer')
    {stop}>>> print(objects)
    [Manager('Mr. Krabs'), Engineer('Squidward')]

.. _with_polymorphic_aliasing:

Using aliasing with with_polymorphic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.with_polymorphic` construct, as a special case of
:func:`_orm.aliased`, also provides the basic feature that :func:`_orm.aliased`
does, which is that of "aliasing" of the polymorphic selectable itself.
Specifically this means two or more :func:`_orm.with_polymorphic` entities,
referring to the same class hierarchy, can be used at once in a single
statement.

To use this feature with a joined inheritance mapping, we typically want to
pass two parameters, :paramref:`_orm.with_polymorphic.aliased` as well as
:paramref:`_orm.with_polymorphic.flat`.  The :paramref:`_orm.with_polymorphic.aliased`
parameter indicates that the polymorphic selectable should be referenced
by an alias name that is unique to this construct.   The
:paramref:`_orm.with_polymorphic.flat` parameter is specific to the default
LEFT OUTER JOIN polymorphic selectable and indicates that a more optimized
form of aliasing should be used in the statement.

To illustrate this feature, the example below emits a SELECT for two
separate polymorphic entities, ``Employee`` joined with ``Engineer``,
and ``Employee`` joined with ``Manager``.  Since these two polymorphic entities
will both be including the base ``employee`` table in their polymorphic selectable, aliasing must
be applied in order to differentiate this table in its two different contexts.
The two polymorphic entities are treated like two individual tables,
and as such typically need to be joined with each other in some way,
as illustrated below where the entities are joined on the ``company_id``
column along with some additional limiting criteria against the
``Employee`` / ``Manager`` entity::

    >>> manager_employee = with_polymorphic(Employee, [Manager], aliased=True, flat=True)
    >>> engineer_employee = with_polymorphic(Employee, [Engineer], aliased=True, flat=True)
    >>> stmt = (
    ...     select(manager_employee, engineer_employee)
    ...     .join(
    ...         engineer_employee,
    ...         engineer_employee.company_id == manager_employee.company_id,
    ...     )
    ...     .where(
    ...         or_(
    ...             manager_employee.name == "Mr. Krabs",
    ...             manager_employee.Manager.manager_name == "Eugene H. Krabs",
    ...         )
    ...     )
    ...     .order_by(engineer_employee.name, manager_employee.name)
    ... )
    >>> for manager, engineer in session.execute(stmt):
    ...     print(f"{manager} {engineer}")
    {execsql}SELECT
    employee_1.id, employee_1.name, employee_1.type, employee_1.company_id,
    manager_1.id AS id_1, manager_1.manager_name,
    employee_2.id AS id_2, employee_2.name AS name_1, employee_2.type AS type_1,
    employee_2.company_id AS company_id_1, engineer_1.id AS id_3, engineer_1.engineer_info
    FROM employee AS employee_1
    LEFT OUTER JOIN manager AS manager_1 ON employee_1.id = manager_1.id
    JOIN
       (employee AS employee_2 LEFT OUTER JOIN engineer AS engineer_1 ON employee_2.id = engineer_1.id)
    ON employee_2.company_id = employee_1.company_id
    WHERE employee_1.name = ? OR manager_1.manager_name = ?
    ORDER BY employee_2.name, employee_1.name
    [...] ('Mr. Krabs', 'Eugene H. Krabs')
    {stop}Manager('Mr. Krabs') Manager('Mr. Krabs')
    Manager('Mr. Krabs') Engineer('SpongeBob')
    Manager('Mr. Krabs') Engineer('Squidward')

In the above example, the behavior of :paramref:`_orm.with_polymorphic.flat`
is that the polymorphic selectables remain as a LEFT OUTER JOIN of their
individual tables, which themselves are given anonymous alias names.  There
is also a right-nested JOIN produced.

When omitting the :paramref:`_orm.with_polymorphic.flat` parameter, the
usual behavior is that each polymorphic selectable is enclosed within a
subquery, producing a more verbose form::

    >>> manager_employee = with_polymorphic(Employee, [Manager], aliased=True)
    >>> engineer_employee = with_polymorphic(Employee, [Engineer], aliased=True)
    >>> stmt = (
    ...     select(manager_employee, engineer_employee)
    ...     .join(
    ...         engineer_employee,
    ...         engineer_employee.company_id == manager_employee.company_id,
    ...     )
    ...     .where(
    ...         or_(
    ...             manager_employee.name == "Mr. Krabs",
    ...             manager_employee.Manager.manager_name == "Eugene H. Krabs",
    ...         )
    ...     )
    ...     .order_by(engineer_employee.name, manager_employee.name)
    ... )
    >>> print(stmt)
    {printsql}SELECT anon_1.employee_id, anon_1.employee_name, anon_1.employee_type,
    anon_1.employee_company_id, anon_1.manager_id, anon_1.manager_manager_name, anon_2.employee_id AS employee_id_1,
    anon_2.employee_name AS employee_name_1, anon_2.employee_type AS employee_type_1,
    anon_2.employee_company_id AS employee_company_id_1, anon_2.engineer_id, anon_2.engineer_engineer_info
    FROM
    (SELECT employee.id AS employee_id, employee.name AS employee_name, employee.type AS employee_type,
    employee.company_id AS employee_company_id,
    manager.id AS manager_id, manager.manager_name AS manager_manager_name
    FROM employee LEFT OUTER JOIN manager ON employee.id = manager.id) AS anon_1
    JOIN
    (SELECT employee.id AS employee_id, employee.name AS employee_name, employee.type AS employee_type,
    employee.company_id AS employee_company_id, engineer.id AS engineer_id, engineer.engineer_info AS engineer_engineer_info
    FROM employee LEFT OUTER JOIN engineer ON employee.id = engineer.id) AS anon_2
    ON anon_2.employee_company_id = anon_1.employee_company_id
    WHERE anon_1.employee_name = :employee_name_2 OR anon_1.manager_manager_name = :manager_manager_name_1
    ORDER BY anon_2.employee_name, anon_1.employee_name

The above form historically has been more portable to backends that didn't necessarily
have support for right-nested JOINs, and it additionally may be appropriate when
the "polymorphic selectable" used by :func:`_orm.with_polymorphic` is not
a simple LEFT OUTER JOIN of tables, as is the case when using mappings such as
:ref:`concrete table inheritance <concrete_inheritance>` mappings as well as when
using alternative polymorphic selectables in general.


.. _with_polymorphic_mapper_config:

Configuring with_polymorphic() on mappers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As is the case with :func:`_orm.selectin_polymorphic`, the
:func:`_orm.with_polymorphic` construct also supports a mapper-configured
version which may be configured in two different ways, either on the base class
using the :paramref:`.mapper.with_polymorphic` parameter, or in a more modern
form using the :paramref:`_orm.Mapper.polymorphic_load` parameter on a
per-subclass basis, passing the value ``"inline"``.

.. warning::

   For joined inheritance mappings, prefer explicit use of
   :func:`_orm.with_polymorphic` within queries, or for implicit eager subclass
   loading use :paramref:`_orm.Mapper.polymorphic_load` with ``"selectin"``,
   instead of using the mapper-level :paramref:`.mapper.with_polymorphic`
   parameter described in this section. This parameter invokes complex
   heuristics intended to rewrite the FROM clauses within SELECT statements
   that can interfere with construction of more complex statements,
   particularly those with nested subqueries that refer to the same mapped
   entity.

For example, we may state our ``Employee`` mapping using
:paramref:`_orm.Mapper.polymorphic_load` as ``"inline"`` as below:

.. sourcecode:: python

    class Employee(Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        type = mapped_column(String(50))

        __mapper_args__ = {"polymorphic_identity": "employee", "polymorphic_on": type}


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        engineer_info = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_load": "inline",
            "polymorphic_identity": "engineer",
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        manager_name = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_load": "inline",
            "polymorphic_identity": "manager",
        }

With the above mapping, SELECT statements against the ``Employee`` class will
automatically assume the use of
``with_polymorphic(Employee, [Engineer, Manager])`` as the primary entity
when the statement is emitted::

    print(select(Employee))
    {printsql}SELECT employee.id, employee.name, employee.type, engineer.id AS id_1,
    engineer.engineer_info, manager.id AS id_2, manager.manager_name
    FROM employee
    LEFT OUTER JOIN engineer ON employee.id = engineer.id
    LEFT OUTER JOIN manager ON employee.id = manager.id

When using mapper-level "with polymorphic", queries can also refer to the
subclass entities directly, where they implicitly represent the joined tables
in the polymorphic query.  Above, we can freely refer to
``Manager`` and ``Engineer`` directly against the default ``Employee``
entity::

    print(
        select(Employee).where(
            or_(Manager.manager_name == "x", Engineer.engineer_info == "y")
        )
    )
    {printsql}SELECT employee.id, employee.name, employee.type, engineer.id AS id_1,
    engineer.engineer_info, manager.id AS id_2, manager.manager_name
    FROM employee
    LEFT OUTER JOIN engineer ON employee.id = engineer.id
    LEFT OUTER JOIN manager ON employee.id = manager.id
    WHERE manager.manager_name = :manager_name_1
    OR engineer.engineer_info = :engineer_info_1

However, if we needed to refer to the ``Employee`` entity or its sub
entities in separate, aliased contexts, we would again make direct use of
:func:`_orm.with_polymorphic` to define these aliased entities as illustrated
in :ref:`with_polymorphic_aliasing`.

For more centralized control over the polymorphic selectable, the more legacy
form of mapper-level polymorphic control may be used which is the
:paramref:`_orm.Mapper.with_polymorphic` parameter, configured on the base
class. This parameter accepts arguments that are comparable to the
:func:`_orm.with_polymorphic` construct, however common use with a joined
inheritance mapping is the plain asterisk, indicating all sub-tables should be
LEFT OUTER JOINED, as in:

.. sourcecode:: python

    class Employee(Base):
        __tablename__ = "employee"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String(50))
        type = mapped_column(String(50))

        __mapper_args__ = {
            "polymorphic_identity": "employee",
            "with_polymorphic": "*",
            "polymorphic_on": type,
        }


    class Engineer(Employee):
        __tablename__ = "engineer"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        engineer_info = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_identity": "engineer",
        }


    class Manager(Employee):
        __tablename__ = "manager"
        id = mapped_column(Integer, ForeignKey("employee.id"), primary_key=True)
        manager_name = mapped_column(String(30))

        __mapper_args__ = {
            "polymorphic_identity": "manager",
        }

Overall, the LEFT OUTER JOIN format used by :func:`_orm.with_polymorphic` and
by options such as :paramref:`_orm.Mapper.with_polymorphic` may be cumbersome
from a SQL and database optimizer point of view; for general loading of
subclass attributes in joined inheritance mappings, the
:func:`_orm.selectin_polymorphic` approach, or its mapper level equivalent of
setting :paramref:`_orm.Mapper.polymorphic_load` to ``"selectin"`` should
likely be preferred, making use of :func:`_orm.with_polymorphic` on a per-query
basis only as needed.

.. _inheritance_of_type:

Joining to specific sub-types or with_polymorphic() entities
------------------------------------------------------------

As a :func:`_orm.with_polymorphic` entity is a special case of :func:`_orm.aliased`,
in order to treat a polymorphic entity as the target of a join, specifically
when using a :func:`_orm.relationship` construct as the ON clause,
we use the same technique for regular aliases as detailed at
:ref:`orm_queryguide_joining_relationships_aliased`, most succinctly
using :meth:`_orm.PropComparator.of_type`.   In the example below we illustrate
a join from the parent ``Company`` entity along the one-to-many relationship
``Company.employees``, which is configured in the
:doc:`setup <_inheritance_setup>` to link to ``Employee`` objects,
using a :func:`_orm.with_polymorphic` entity as the target::

    >>> employee_plus_engineer = with_polymorphic(Employee, [Engineer])
    >>> stmt = (
    ...     select(Company.name, employee_plus_engineer.name)
    ...     .join(Company.employees.of_type(employee_plus_engineer))
    ...     .where(
    ...         or_(
    ...             employee_plus_engineer.name == "SpongeBob",
    ...             employee_plus_engineer.Engineer.engineer_info
    ...             == "Senior Customer Engagement Engineer",
    ...         )
    ...     )
    ... )
    >>> for company_name, emp_name in session.execute(stmt):
    ...     print(f"{company_name} {emp_name}")
    {execsql}SELECT company.name, employee.name AS name_1
    FROM company JOIN (employee LEFT OUTER JOIN engineer ON employee.id = engineer.id) ON company.id = employee.company_id
    WHERE employee.name = ? OR engineer.engineer_info = ?
    [...] ('SpongeBob', 'Senior Customer Engagement Engineer')
    {stop}Krusty Krab SpongeBob
    Krusty Krab Squidward

More directly, :meth:`_orm.PropComparator.of_type` is also used with inheritance
mappings of any kind to limit a join along a :func:`_orm.relationship` to a
particular sub-type of the :func:`_orm.relationship`'s target.  The above
query could be written strictly in terms of ``Engineer`` targets as follows::

    >>> stmt = (
    ...     select(Company.name, Engineer.name)
    ...     .join(Company.employees.of_type(Engineer))
    ...     .where(
    ...         or_(
    ...             Engineer.name == "SpongeBob",
    ...             Engineer.engineer_info == "Senior Customer Engagement Engineer",
    ...         )
    ...     )
    ... )
    >>> for company_name, emp_name in session.execute(stmt):
    ...     print(f"{company_name} {emp_name}")
    {execsql}SELECT company.name, employee.name AS name_1
    FROM company JOIN (employee JOIN engineer ON employee.id = engineer.id) ON company.id = employee.company_id
    WHERE employee.name = ? OR engineer.engineer_info = ?
    [...] ('SpongeBob', 'Senior Customer Engagement Engineer')
    {stop}Krusty Krab SpongeBob
    Krusty Krab Squidward

It can be observed above that joining to the ``Engineer`` target directly,
rather than the "polymorphic selectable" of ``with_polymorphic(Employee, [Engineer])``
has the useful characteristic of using an inner JOIN rather than a
LEFT OUTER JOIN, which is generally more performant from a SQL optimizer
point of view.

.. _eagerloading_polymorphic_subtypes:

Eager Loading of Polymorphic Subtypes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The use of :meth:`_orm.PropComparator.of_type` illustrated with the
:meth:`.Select.join` method in the previous section may also be applied
equivalently to :ref:`relationship loader options <orm_queryguide_relationship_loaders>`,
such as :func:`_orm.selectinload` and :func:`_orm.joinedload`.

As a basic example, if we wished to load ``Company`` objects, and additionally
eagerly load all elements of ``Company.employees`` using the
:func:`_orm.with_polymorphic` construct against the full hierarchy, we may write::

    >>> all_employees = with_polymorphic(Employee, "*")
    >>> stmt = select(Company).options(selectinload(Company.employees.of_type(all_employees)))
    >>> for company in session.scalars(stmt):
    ...     print(f"company: {company.name}")
    ...     print(f"employees: {company.employees}")
    {execsql}SELECT company.id, company.name
    FROM company
    [...] ()
    SELECT employee.company_id, employee.id, employee.name, employee.type,
    manager.id, manager.manager_name, engineer.id, engineer.engineer_info
    FROM employee
    LEFT OUTER JOIN manager ON employee.id = manager.id
    LEFT OUTER JOIN engineer ON employee.id = engineer.id
    WHERE employee.company_id IN (?)
    [...] (1,)
    company: Krusty Krab
    employees: [Manager('Mr. Krabs'), Engineer('SpongeBob'), Engineer('Squidward')]

The above query may be compared directly to the
:func:`_orm.selectin_polymorphic` version illustrated in the previous
section :ref:`polymorphic_selectin_as_loader_option_target`.

.. seealso::

    :ref:`polymorphic_selectin_as_loader_option_target` - illustrates the equivalent example
    as above using :func:`_orm.selectin_polymorphic` instead


.. _loading_single_inheritance:

SELECT Statements for Single Inheritance Mappings
-------------------------------------------------

..  Setup code, not for display

    >>> session.close()
    ROLLBACK
    >>> conn.close()

.. doctest-include _single_inheritance.rst

.. admonition:: Single Table Inheritance Setup

    This section discusses single table inheritance,
    described at :ref:`single_inheritance`, which uses a single table to
    represent multiple classes in a hierarchy.

    :doc:`View the ORM setup for this section <_single_inheritance>`.

In contrast to joined inheritance mappings, the construction of SELECT
statements for single inheritance mappings tends to be simpler since for
an all-single-inheritance hierarchy, there's only one table.

Regardless of whether or not the inheritance hierarchy is all single-inheritance
or has a mixture of joined and single inheritance, SELECT statements for
single inheritance differentiate queries against the base class vs. a subclass
by limiting the SELECT statement with additional WHERE criteria.

As an example, a query for the single-inheritance example mapping of
``Employee`` will load objects of type ``Manager``, ``Engineer`` and
``Employee`` using a simple SELECT of the table::

    >>> stmt = select(Employee).order_by(Employee.id)
    >>> for obj in session.scalars(stmt):
    ...     print(f"{obj}")
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type
    FROM employee ORDER BY employee.id
    [...] ()
    {stop}Manager('Mr. Krabs')
    Engineer('SpongeBob')
    Engineer('Squidward')

When a load is emitted for a specific subclass, additional criteria is
added to the SELECT that limits the rows, such as below where a SELECT against
the ``Engineer`` entity is performed::

    >>> stmt = select(Engineer).order_by(Engineer.id)
    >>> objects = session.scalars(stmt).all()
    {execsql}SELECT employee.id, employee.name, employee.type, employee.engineer_info
    FROM employee
    WHERE employee.type IN (?) ORDER BY employee.id
    [...] ('engineer',)
    {stop}>>> for obj in objects:
    ...     print(f"{obj}")
    Engineer('SpongeBob')
    Engineer('Squidward')



Optimizing Attribute Loads for Single Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  Setup code, not for display

    >>> session.close()
    ROLLBACK

The default behavior of single inheritance mappings regarding how attributes on
subclasses are SELECTed is similar to that of joined inheritance, in that
subclass-specific attributes still emit a second SELECT by default.  In
the example below, a single ``Employee`` of type ``Manager`` is loaded,
however since the requested class is ``Employee``, the ``Manager.manager_name``
attribute is not present by default, and an additional SELECT is emitted
when it's accessed::

    >>> mr_krabs = session.scalars(select(Employee).where(Employee.name == "Mr. Krabs")).one()
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type
    FROM employee
    WHERE employee.name = ?
    [...] ('Mr. Krabs',)
    {stop}>>> mr_krabs.manager_name
    {execsql}SELECT employee.manager_name
    FROM employee
    WHERE employee.id = ? AND employee.type IN (?)
    [...] (1, 'manager')
    {stop}'Eugene H. Krabs'

..  Setup code, not for display

    >>> session.close()
    ROLLBACK

To alter this behavior, the same general concepts used to eagerly load these
additional attributes used in joined inheritance loading apply to single
inheritance as well, including use of the :func:`_orm.selectin_polymorphic`
option as well as the :func:`_orm.with_polymorphic` option, the latter of which
simply includes the additional columns and from a SQL perspective is more
efficient for single-inheritance mappers::

    >>> employees = with_polymorphic(Employee, "*")
    >>> stmt = select(employees).order_by(employees.id)
    >>> objects = session.scalars(stmt).all()
    {execsql}BEGIN (implicit)
    SELECT employee.id, employee.name, employee.type,
    employee.manager_name, employee.engineer_info
    FROM employee ORDER BY employee.id
    [...] ()
    {stop}>>> for obj in objects:
    ...     print(f"{obj}")
    Manager('Mr. Krabs')
    Engineer('SpongeBob')
    Engineer('Squidward')
    >>> objects[0].manager_name
    'Eugene H. Krabs'

Since the overhead of loading single-inheritance subclass mappings is
usually minimal, it's therefore recommended that single inheritance mappings
include the :paramref:`_orm.Mapper.polymorphic_load` parameter with a
setting of ``"inline"`` for those subclasses where loading of their specific
subclass attributes is expected to be common.   An example illustrating the
:doc:`setup <_single_inheritance>`, modified to include this option,
is below::

    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class Employee(Base):
    ...     __tablename__ = "employee"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     type: Mapped[str]
    ...
    ...     def __repr__(self):
    ...         return f"{self.__class__.__name__}({self.name!r})"
    ...
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "employee",
    ...         "polymorphic_on": "type",
    ...     }
    >>> class Manager(Employee):
    ...     manager_name: Mapped[str] = mapped_column(nullable=True)
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "manager",
    ...         "polymorphic_load": "inline",
    ...     }
    >>> class Engineer(Employee):
    ...     engineer_info: Mapped[str] = mapped_column(nullable=True)
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "engineer",
    ...         "polymorphic_load": "inline",
    ...     }


With the above mapping, the ``Manager`` and ``Engineer`` classes will have
their columns included in SELECT statements against the ``Employee``
entity automatically::

    >>> print(select(Employee))
    {printsql}SELECT employee.id, employee.name, employee.type,
    employee.manager_name, employee.engineer_info
    FROM employee

Inheritance Loading API
-----------------------

.. autofunction:: sqlalchemy.orm.with_polymorphic

.. autofunction:: sqlalchemy.orm.selectin_polymorphic


..  Setup code, not for display

    >>> session.close()
    ROLLBACK
    >>> conn.close()

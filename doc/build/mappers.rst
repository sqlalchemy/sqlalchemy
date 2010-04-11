.. _datamapping_toplevel:

====================
Mapper Configuration
====================
This section references most major configurational patterns involving the :func:`~sqlalchemy.orm.mapper` and :func:`~sqlalchemy.orm.relationship` functions.  It assumes you've worked through :ref:`ormtutorial_toplevel` and know how to construct and use rudimentary mappers and relationships.

Mapper Configuration
====================

Customizing Column Properties
------------------------------

The default behavior of a ``mapper`` is to assemble all the columns in the mapped :class:`~sqlalchemy.schema.Table` into mapped object attributes.  This behavior can be modified in several ways, as well as enhanced by SQL expressions.

To load only a part of the columns referenced by a table as attributes, use the ``include_properties`` and ``exclude_properties`` arguments::

    mapper(User, users_table, include_properties=['user_id', 'user_name'])

    mapper(Address, addresses_table, exclude_properties=['street', 'city', 'state', 'zip'])

To change the name of the attribute mapped to a particular column, place the :class:`~sqlalchemy.schema.Column` object in the ``properties`` dictionary with the desired key::

    mapper(User, users_table, properties={
       'id': users_table.c.user_id,
       'name': users_table.c.user_name,
    })

To change the names of all attributes using a prefix, use the ``column_prefix`` option.  This is useful for classes which wish to add their own ``property`` accessors::

    mapper(User, users_table, column_prefix='_')

The above will place attribute names such as ``_user_id``, ``_user_name``, ``_password`` etc. on the mapped ``User`` class.

To place multiple columns which are known to be "synonymous" based on foreign key relationship or join condition into the same mapped attribute, put  them together using a list, as below where we map to a :class:`~sqlalchemy.sql.expression.Join`::

    # join users and addresses
    usersaddresses = sql.join(users_table, addresses_table, \
        users_table.c.user_id == addresses_table.c.user_id)

    mapper(User, usersaddresses, properties={
        'id':[users_table.c.user_id, addresses_table.c.user_id],
    })

Deferred Column Loading
------------------------

This feature allows particular columns of a table to not be loaded by default, instead being loaded later on when first referenced.  It is essentially "column-level lazy loading".   This feature is useful when one wants to avoid loading a large text or binary field into memory when it's not needed.  Individual columns can be lazy loaded by themselves or placed into groups that lazy-load together::

    book_excerpts = Table('books', db,
        Column('book_id', Integer, primary_key=True),
        Column('title', String(200), nullable=False),
        Column('summary', String(2000)),
        Column('excerpt', String),
        Column('photo', Binary)
    )

    class Book(object):
        pass

    # define a mapper that will load each of 'excerpt' and 'photo' in
    # separate, individual-row SELECT statements when each attribute
    # is first referenced on the individual object instance
    mapper(Book, book_excerpts, properties={
       'excerpt': deferred(book_excerpts.c.excerpt),
       'photo': deferred(book_excerpts.c.photo)
    })

Deferred columns can be placed into groups so that they load together::

    book_excerpts = Table('books', db,
      Column('book_id', Integer, primary_key=True),
      Column('title', String(200), nullable=False),
      Column('summary', String(2000)),
      Column('excerpt', String),
      Column('photo1', Binary),
      Column('photo2', Binary),
      Column('photo3', Binary)
    )

    class Book(object):
        pass

    # define a mapper with a 'photos' deferred group.  when one photo is referenced,
    # all three photos will be loaded in one SELECT statement.  The 'excerpt' will
    # be loaded separately when it is first referenced.
    mapper(Book, book_excerpts, properties = {
      'excerpt': deferred(book_excerpts.c.excerpt),
      'photo1': deferred(book_excerpts.c.photo1, group='photos'),
      'photo2': deferred(book_excerpts.c.photo2, group='photos'),
      'photo3': deferred(book_excerpts.c.photo3, group='photos')
    })

You can defer or undefer columns at the :class:`~sqlalchemy.orm.query.Query` level using the ``defer`` and ``undefer`` options::

    query = session.query(Book)
    query.options(defer('summary')).all()
    query.options(undefer('excerpt')).all()

And an entire "deferred group", i.e. which uses the ``group`` keyword argument to :func:`~sqlalchemy.orm.deferred()`, can be undeferred using :func:`~sqlalchemy.orm.undefer_group()`, sending in the group name::

    query = session.query(Book)
    query.options(undefer_group('photos')).all()

SQL Expressions as Mapped Attributes
-------------------------------------

To add a SQL clause composed of local or external columns as a read-only, mapped column attribute, use the :func:`~sqlalchemy.orm.column_property()` function.  Any scalar-returning :class:`~sqlalchemy.sql.expression.ClauseElement` may be used, as long as it has a ``name`` attribute; usually, you'll want to call ``label()`` to give it a specific name::

    mapper(User, users_table, properties={
        'fullname': column_property(
            (users_table.c.firstname + " " + users_table.c.lastname).label('fullname')
        )
    })

Correlated subqueries may be used as well:

.. sourcecode:: python+sql

    mapper(User, users_table, properties={
        'address_count': column_property(
                select(
                    [func.count(addresses_table.c.address_id)],
                    addresses_table.c.user_id==users_table.c.user_id
                ).label('address_count')
            )
    })

Changing Attribute Behavior
----------------------------


Simple Validators
~~~~~~~~~~~~~~~~~~


A quick way to add a "validation" routine to an attribute is to use the :func:`~sqlalchemy.orm.validates` decorator.  This is a shortcut for using the :class:`sqlalchemy.orm.util.Validator` attribute extension with individual column or relationship based attributes.   An attribute validator can raise an exception, halting the process of mutating the attribute's value, or can change the given value into something different.   Validators, like all attribute extensions, are only called by normal userland code; they are not issued when the ORM is populating the object.

.. sourcecode:: python+sql

    addresses_table = Table('addresses', metadata,
        Column('id', Integer, primary_key=True),
        Column('email', String)
    )

    class EmailAddress(object):
        @validates('email')
        def validate_email(self, key, address):
            assert '@' in address
            return address

    mapper(EmailAddress, addresses_table)

Validators also receive collection events, when items are added to a collection:

.. sourcecode:: python+sql

    class User(object):
        @validates('addresses')
        def validate_address(self, key, address):
            assert '@' in address.email
            return address

.. _synonyms:

Using Descriptors
~~~~~~~~~~~~~~~~~~

A more comprehensive way to produce modified behavior for an attribute is to use descriptors.   These are commonly used in Python using the ``property()`` function.   The standard SQLAlchemy technique for descriptors is to create a plain descriptor, and to have it read/write from a mapped attribute with a different name.  To have the descriptor named the same as a column, map the column under a different name, i.e.:

.. sourcecode:: python+sql

    class EmailAddress(object):
       def _set_email(self, email):
          self._email = email
       def _get_email(self):
          return self._email
       email = property(_get_email, _set_email)

    mapper(MyAddress, addresses_table, properties={
        '_email': addresses_table.c.email
    })

However, the approach above is not complete.  While our ``EmailAddress`` object will shuttle the value through the ``email`` descriptor and into the ``_email`` mapped attribute, the class level ``EmailAddress.email`` attribute does not have the usual expression semantics usable with :class:`~sqlalchemy.orm.query.Query`.  To provide these, we instead use the :func:`~sqlalchemy.orm.synonym` function as follows:

.. sourcecode:: python+sql

    mapper(EmailAddress, addresses_table, properties={
        'email': synonym('_email', map_column=True)
    })

The ``email`` attribute is now usable in the same way as any other mapped attribute, including filter expressions, get/set operations, etc.:

.. sourcecode:: python+sql

    address = session.query(EmailAddress).filter(EmailAddress.email == 'some address').one()

    address.email = 'some other address'
    session.flush()

    q = session.query(EmailAddress).filter_by(email='some other address')

If the mapped class does not provide a property, the :func:`~sqlalchemy.orm.synonym` construct will create a default getter/setter object automatically.

.. _custom_comparators:

Custom Comparators
~~~~~~~~~~~~~~~~~~~

The expressions returned by comparison operations, such as ``User.name=='ed'``, can be customized.  SQLAlchemy attributes generate these expressions using :class:`~sqlalchemy.orm.interfaces.PropComparator` objects, which provide common Python expression overrides including ``__eq__()``, ``__ne__()``, ``__lt__()``, and so on.  Any mapped attribute can be passed a user-defined class via the ``comparator_factory`` keyword argument, which subclasses the appropriate :class:`~sqlalchemy.orm.interfaces.PropComparator` in use, which can provide any or all of these methods:

.. sourcecode:: python+sql

    from sqlalchemy.orm.properties import ColumnProperty
    class MyComparator(ColumnProperty.Comparator):
        def __eq__(self, other):
            return func.lower(self.__clause_element__()) == func.lower(other)

    mapper(EmailAddress, addresses_table, properties={
        'email':column_property(addresses_table.c.email, comparator_factory=MyComparator)
    })

Above, comparisons on the ``email`` column are wrapped in the SQL lower() function to produce case-insensitive matching:

.. sourcecode:: python+sql

    >>> str(EmailAddress.email == 'SomeAddress@foo.com')
    lower(addresses.email) = lower(:lower_1)

The ``__clause_element__()`` method is provided by the base ``Comparator`` class in use, and represents the SQL element which best matches what this attribute represents.  For a column-based attribute, it's the mapped column.  For a composite attribute, it's a :class:`~sqlalchemy.sql.expression.ClauseList` consisting of each column represented.  For a relationship, it's the table mapped by the local mapper (not the remote mapper).  ``__clause_element__()`` should be honored by the custom comparator class in most cases since the resulting element will be applied any translations which are in effect, such as the correctly aliased member when using an ``aliased()`` construct or certain :func:`~sqlalchemy.orm.query.Query.with_polymorphic` scenarios.

There are four kinds of ``Comparator`` classes which may be subclassed, as according to the type of mapper property configured:

  * :func:`~sqlalchemy.orm.column_property` attribute - ``sqlalchemy.orm.properties.ColumnProperty.Comparator``
  * :func:`~sqlalchemy.orm.composite` attribute - ``sqlalchemy.orm.properties.CompositeProperty.Comparator``
  * :func:`~sqlalchemy.orm.relationship` attribute - ``sqlalchemy.orm.properties.RelationshipProperty.Comparator``
  * :func:`~sqlalchemy.orm.comparable_property` attribute - ``sqlalchemy.orm.interfaces.PropComparator``

When using :func:`~sqlalchemy.orm.comparable_property`, which is a mapper property that isn't tied to any column or mapped table, the ``__clause_element__()`` method of :class:`~sqlalchemy.orm.interfaces.PropComparator` should also be implemented.

The ``comparator_factory`` argument is accepted by all ``MapperProperty``-producing functions:  :func:`~sqlalchemy.orm.column_property`, :func:`~sqlalchemy.orm.composite`, :func:`~sqlalchemy.orm.comparable_property`, :func:`~sqlalchemy.orm.synonym`, :func:`~sqlalchemy.orm.relationship`, :func:`~sqlalchemy.orm.backref`, :func:`~sqlalchemy.orm.deferred`, and :func:`~sqlalchemy.orm.dynamic_loader`.

Composite Column Types
-----------------------

Sets of columns can be associated with a single datatype.  The ORM treats the group of columns like a single column which accepts and returns objects using the custom datatype you provide.  In this example, we'll create a table ``vertices`` which stores a pair of x/y coordinates, and a custom datatype ``Point`` which is a composite type of an x and y column:

.. sourcecode:: python+sql

    vertices = Table('vertices', metadata,
        Column('id', Integer, primary_key=True),
        Column('x1', Integer),
        Column('y1', Integer),
        Column('x2', Integer),
        Column('y2', Integer),
        )

The requirements for the custom datatype class are that it have a constructor which accepts positional arguments corresponding to its column format, and also provides a method ``__composite_values__()`` which returns the state of the object as a list or tuple, in order of its column-based attributes.  It also should supply adequate ``__eq__()`` and ``__ne__()`` methods which test the equality of two instances, and may optionally provide a ``__set_composite_values__`` method which is used to set internal state in some cases (typically when default values have been generated during a flush)::

    class Point(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y
        def __composite_values__(self):
            return [self.x, self.y]
        def __set_composite_values__(self, x, y):
            self.x = x
            self.y = y
        def __eq__(self, other):
            return other.x == self.x and other.y == self.y
        def __ne__(self, other):
            return not self.__eq__(other)

If ``__set_composite_values__()`` is not provided, the names of the mapped columns are taken as the names of attributes on the object, and ``setattr()`` is used to set data.

Setting up the mapping uses the :func:`~sqlalchemy.orm.composite()` function::

    class Vertex(object):
        pass

    mapper(Vertex, vertices, properties={
        'start': composite(Point, vertices.c.x1, vertices.c.y1),
        'end': composite(Point, vertices.c.x2, vertices.c.y2)
    })

We can now use the ``Vertex`` instances as well as querying as though the ``start`` and ``end`` attributes are regular scalar attributes::

    session = Session()
    v = Vertex(Point(3, 4), Point(5, 6))
    session.save(v)

    v2 = session.query(Vertex).filter(Vertex.start == Point(3, 4))

The "equals" comparison operation by default produces an AND of all corresponding columns equated to one another.  This can be changed using the ``comparator_factory``, described in :ref:`custom_comparators`::

    from sqlalchemy.orm.properties import CompositeProperty
    from sqlalchemy import sql

    class PointComparator(CompositeProperty.Comparator):
        def __gt__(self, other):
            """define the 'greater than' operation"""

            return sql.and_(*[a>b for a, b in
                              zip(self.__clause_element__().clauses,
                                  other.__composite_values__())])

    maper(Vertex, vertices, properties={
        'start': composite(Point, vertices.c.x1, vertices.c.y1, comparator_factory=PointComparator),
        'end': composite(Point, vertices.c.x2, vertices.c.y2, comparator_factory=PointComparator)
    })

Controlling Ordering
---------------------

The ORM does not generate ordering for any query unless explicitly configured.

The "default" ordering for a collection, which applies to list-based collections, can be configured using the ``order_by`` keyword argument on :func:`~sqlalchemy.orm.relationship`::

    mapper(Address, addresses_table)

    # order address objects by address id
    mapper(User, users_table, properties={
        'addresses': relationship(Address, order_by=addresses_table.c.address_id)
    })

Note that when using joined eager loaders with relationships, the tables used by the eager load's join are anonymously aliased.  You can only order by these columns if you specify it at the :func:`~sqlalchemy.orm.relationship` level.  To control ordering at the query level based on a related table, you ``join()`` to that relationship, then order by it::

    session.query(User).join('addresses').order_by(Address.street)

Ordering for rows loaded through :class:`~sqlalchemy.orm.query.Query` is usually specified using the ``order_by()`` generative method.  There is also an option to set a default ordering for Queries which are against a single mapped entity and where there was no explicit ``order_by()`` stated, which is the ``order_by`` keyword argument to ``mapper()``::

    # order by a column
    mapper(User, users_table, order_by=users_table.c.user_id)

    # order by multiple items
    mapper(User, users_table, order_by=[users_table.c.user_id, users_table.c.user_name.desc()])

Above, a :class:`~sqlalchemy.orm.query.Query` issued for the ``User`` class will use the value of the mapper's ``order_by`` setting if the :class:`~sqlalchemy.orm.query.Query` itself has no ordering specified.

.. _datamapping_inheritance:

Mapping Class Inheritance Hierarchies
--------------------------------------

SQLAlchemy supports three forms of inheritance:  *single table inheritance*, where several types of classes are stored in one table, *concrete table inheritance*, where each type of class is stored in its own table, and *joined table inheritance*, where the parent/child classes are stored in their own tables that are joined together in a select.  Whereas support for single and joined table inheritance is strong, concrete table inheritance is a less common scenario with some particular problems so is not quite as flexible.

When mappers are configured in an inheritance relationship, SQLAlchemy has the ability to load elements "polymorphically", meaning that a single query can return objects of multiple types.

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
            return self.__class__.__name__ + " " + self.name + " " +  self.manager_data

    class Engineer(Employee):
        def __init__(self, name, engineer_info):
            self.name = name
            self.engineer_info = engineer_info
        def __repr__(self):
            return self.__class__.__name__ + " " + self.name + " " +  self.engineer_info

Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~

In joined table inheritance, each class along a particular classes' list of parents is represented by a unique table.  The total set of attributes for a particular instance is represented as a join along all tables in its inheritance path.  Here, we first define a table to represent the ``Employee`` class.  This table will contain a primary key column (or columns), and a column for each attribute that's represented by ``Employee``.  In this case it's just ``name``::

    employees = Table('employees', metadata,
       Column('employee_id', Integer, primary_key=True),
       Column('name', String(50)),
       Column('type', String(30), nullable=False)
    )

The table also has a column called ``type``.  It is strongly advised in both single- and joined- table inheritance scenarios that the root table contains a column whose sole purpose is that of the **discriminator**; it stores a value which indicates the type of object represented within the row.  The column may be of any desired datatype.  While there are some "tricks" to work around the requirement that there be a discriminator column, they are more complicated to configure when one wishes to load polymorphically.

Next we define individual tables for each of ``Engineer`` and ``Manager``, which contain columns that represent the attributes unique to the subclass they represent.  Each table also must contain a primary key column (or columns), and in most cases a foreign key reference to the parent table.  It is  standard practice that the same column is used for both of these roles, and that the column is also named the same as that of the parent table.  However this is optional in SQLAlchemy; separate columns may be used for primary key and parent-relationship, the column may be named differently than that of the parent, and even a custom join condition can be specified between parent and child tables instead of using a foreign key::

    engineers = Table('engineers', metadata,
       Column('employee_id', Integer, ForeignKey('employees.employee_id'), primary_key=True),
       Column('engineer_info', String(50)),
    )

    managers = Table('managers', metadata,
       Column('employee_id', Integer, ForeignKey('employees.employee_id'), primary_key=True),
       Column('manager_data', String(50)),
    )

One natural effect of the joined table inheritance configuration is that the identity of any mapped object can be determined entirely from the base table.  This has obvious advantages, so SQLAlchemy always considers the primary key columns of a joined inheritance class to be those of the base table only, unless otherwise manually configured.  In other words, the ``employee_id`` column of both the ``engineers`` and ``managers`` table is not used to locate the ``Engineer`` or ``Manager`` object itself - only the value in ``employees.employee_id`` is considered, and the primary key in this case is non-composite.  ``engineers.employee_id`` and ``managers.employee_id`` are still of course critical to the proper operation of the pattern overall as they are used to locate the joined row, once the parent row has been determined, either through a distinct SELECT statement or all at once within a JOIN.

We then configure mappers as usual, except we use some additional arguments to indicate the inheritance relationship, the polymorphic discriminator column, and the **polymorphic identity** of each class; this is the value that will be stored in the polymorphic discriminator column.

.. sourcecode:: python+sql

    mapper(Employee, employees, polymorphic_on=employees.c.type, polymorphic_identity='employee')
    mapper(Engineer, engineers, inherits=Employee, polymorphic_identity='engineer')
    mapper(Manager, managers, inherits=Employee, polymorphic_identity='manager')

And that's it.  Querying against ``Employee`` will return a combination of ``Employee``, ``Engineer`` and ``Manager`` objects.   Newly saved ``Engineer``, ``Manager``, and ``Employee`` objects will automatically populate the ``employees.type`` column with ``engineer``, ``manager``, or ``employee``, as appropriate.

Controlling Which Tables are Queried
+++++++++++++++++++++++++++++++++++++

The :func:`~sqlalchemy.orm.query.Query.with_polymorphic` method of :class:`~sqlalchemy.orm.query.Query` affects the specific subclass tables which the Query selects from.  Normally, a query such as this:

.. sourcecode:: python+sql

    session.query(Employee).all()

...selects only from the ``employees`` table.   When loading fresh from the database, our joined-table setup will query from the parent table only, using SQL such as this:

.. sourcecode:: python+sql

    {opensql}
    SELECT employees.employee_id AS employees_employee_id, employees.name AS employees_name, employees.type AS employees_type
    FROM employees
    []

As attributes are requested from those ``Employee`` objects which are represented in either the ``engineers`` or ``managers`` child tables, a second load is issued for the columns in that related row, if the data was not already loaded.  So above, after accessing the objects you'd see further SQL issued along the lines of:

.. sourcecode:: python+sql

    {opensql}
    SELECT managers.employee_id AS managers_employee_id, managers.manager_data AS managers_manager_data
    FROM managers
    WHERE ? = managers.employee_id
    [5]
    SELECT engineers.employee_id AS engineers_employee_id, engineers.engineer_info AS engineers_engineer_info
    FROM engineers
    WHERE ? = engineers.employee_id
    [2]

This behavior works well when issuing searches for small numbers of items, such as when using ``get()``, since the full range of joined tables are not pulled in to the SQL statement unnecessarily.  But when querying a larger span of rows which are known to be of many types, you may want to actively join to some or all of the joined tables.  The ``with_polymorphic`` feature of :class:`~sqlalchemy.orm.query.Query` and ``mapper`` provides this.

Telling our query to polymorphically load ``Engineer`` and ``Manager`` objects:

.. sourcecode:: python+sql

    query = session.query(Employee).with_polymorphic([Engineer, Manager])

produces a query which joins the ``employees`` table to both the ``engineers`` and ``managers`` tables like the following:

.. sourcecode:: python+sql

    query.all()
    {opensql}
    SELECT employees.employee_id AS employees_employee_id, engineers.employee_id AS engineers_employee_id, managers.employee_id AS managers_employee_id, employees.name AS employees_name, employees.type AS employees_type, engineers.engineer_info AS engineers_engineer_info, managers.manager_data AS managers_manager_data
    FROM employees LEFT OUTER JOIN engineers ON employees.employee_id = engineers.employee_id LEFT OUTER JOIN managers ON employees.employee_id = managers.employee_id
    []

:func:`~sqlalchemy.orm.query.Query.with_polymorphic` accepts a single class or mapper, a list of classes/mappers, or the string ``'*'`` to indicate all subclasses:

.. sourcecode:: python+sql

    # join to the engineers table
    query.with_polymorphic(Engineer)

    # join to the engineers and managers tables
    query.with_polymorphic([Engineer, Manager])

    # join to all subclass tables
    query.with_polymorphic('*')

It also accepts a second argument ``selectable`` which replaces the automatic join creation and instead selects directly from the selectable given.  This feature is normally used with "concrete" inheritance, described later, but can be used with any kind of inheritance setup in the case that specialized SQL should be used to load polymorphically:

.. sourcecode:: python+sql

    # custom selectable
    query.with_polymorphic([Engineer, Manager], employees.outerjoin(managers).outerjoin(engineers))

:func:`~sqlalchemy.orm.query.Query.with_polymorphic` is also needed
when you wish to add filter criteria that are specific to one or more
subclasses; It makes the subclasses' columns available to the WHERE clause:

.. sourcecode:: python+sql

    session.query(Employee).with_polymorphic([Engineer, Manager]).\
        filter(or_(Engineer.engineer_info=='w', Manager.manager_data=='q'))

Note that if you only need to load a single subtype, such as just the ``Engineer`` objects, :func:`~sqlalchemy.orm.query.Query.with_polymorphic` is not needed since you would query against the ``Engineer`` class directly.

The mapper also accepts ``with_polymorphic`` as a configurational argument so that the joined-style load will be issued automatically.  This argument may be the string ``'*'``, a list of classes, or a tuple consisting of either, followed by a selectable.

.. sourcecode:: python+sql

    mapper(Employee, employees, polymorphic_on=employees.c.type, \
        polymorphic_identity='employee', with_polymorphic='*')
    mapper(Engineer, engineers, inherits=Employee, polymorphic_identity='engineer')
    mapper(Manager, managers, inherits=Employee, polymorphic_identity='manager')

The above mapping will produce a query similar to that of ``with_polymorphic('*')`` for every query of ``Employee`` objects.

Using :func:`~sqlalchemy.orm.query.Query.with_polymorphic` with :class:`~sqlalchemy.orm.query.Query` will override the mapper-level ``with_polymorphic`` setting.

Creating Joins to Specific Subtypes
++++++++++++++++++++++++++++++++++++

The :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` method is a helper which allows the construction of joins along :func:`~sqlalchemy.orm.relationship` paths while narrowing the criterion to specific subclasses.  Suppose the ``employees`` table represents a collection of employees which are associated with a ``Company`` object.  We'll add a ``company_id`` column to the ``employees`` table and a new table ``companies``:

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

When querying from ``Company`` onto the ``Employee`` relationship, the ``join()`` method as well as the ``any()`` and ``has()`` operators will create a join from ``companies`` to ``employees``, without including ``engineers`` or ``managers`` in the mix.  If we wish to have criterion which is specifically against the ``Engineer`` class, we can tell those methods to join or subquery against the joined table representing the subclass using the :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` operator:

.. sourcecode:: python+sql

    session.query(Company).join(Company.employees.of_type(Engineer)).filter(Engineer.engineer_info=='someinfo')

A longhand version of this would involve spelling out the full target selectable within a 2-tuple:

.. sourcecode:: python+sql

    session.query(Company).join((employees.join(engineers), Company.employees)).filter(Engineer.engineer_info=='someinfo')

Currently, :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` accepts a single class argument.  It may be expanded later on to accept multiple classes.  For now, to join to any group of subclasses, the longhand notation allows this flexibility:

.. sourcecode:: python+sql

    session.query(Company).join((employees.outerjoin(engineers).outerjoin(managers), Company.employees)).\
        filter(or_(Engineer.engineer_info=='someinfo', Manager.manager_data=='somedata'))

The ``any()`` and ``has()`` operators also can be used with :func:`~sqlalchemy.orm.interfaces.PropComparator.of_type` when the embedded criterion is in terms of a subclass:

.. sourcecode:: python+sql

    session.query(Company).filter(Company.employees.of_type(Engineer).any(Engineer.engineer_info=='someinfo')).all()

Note that the ``any()`` and ``has()`` are both shorthand for a correlated EXISTS query.  To build one by hand looks like:

.. sourcecode:: python+sql

    session.query(Company).filter(
        exists([1],
            and_(Engineer.engineer_info=='someinfo', employees.c.company_id==companies.c.company_id),
            from_obj=employees.join(engineers)
        )
    ).all()

The EXISTS subquery above selects from the join of ``employees`` to ``engineers``, and also specifies criterion which correlates the EXISTS subselect back to the parent ``companies`` table.

Single Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Single table inheritance is where the attributes of the base class as well as all subclasses are represented within a single table.  A column is present in the table for every attribute mapped to the base class and all subclasses; the columns which correspond to a single subclass are nullable.  This configuration looks much like joined-table inheritance except there's only one table.  In this case, a ``type`` column is required, as there would be no other way to discriminate between classes.  The table is specified in the base mapper only; for the inheriting classes, leave their ``table`` parameter blank:

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
    manager_mapper = mapper(Manager, inherits=employee_mapper, polymorphic_identity='manager')
    engineer_mapper = mapper(Engineer, inherits=employee_mapper, polymorphic_identity='engineer')

Note that the mappers for the derived classes Manager and Engineer omit the specification of their associated table, as it is inherited from the employee_mapper. Omitting the table specification for derived mappers in single-table inheritance is required.

.. _concrete_inheritance:

Concrete Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Notice in this case there is no ``type`` column.  If polymorphic loading is not required, there's no advantage to using ``inherits`` here; you just define a separate mapper for each class.

.. sourcecode:: python+sql

    mapper(Employee, employees_table)
    mapper(Manager, managers_table)
    mapper(Engineer, engineers_table)

To load polymorphically, the ``with_polymorphic`` argument is required, along with a selectable indicating how rows should be loaded.  In this case we must construct a UNION of all three tables.  SQLAlchemy includes a helper function to create these called :func:`~sqlalchemy.orm.util.polymorphic_union`, which will map all the different columns into a structure of selects with the same numbers and names of columns, and also generate a virtual ``type`` column for each subselect:

.. sourcecode:: python+sql

    pjoin = polymorphic_union({
        'employee': employees_table,
        'manager': managers_table,
        'engineer': engineers_table
    }, 'type', 'pjoin')

    employee_mapper = mapper(Employee, employees_table, with_polymorphic=('*', pjoin), \
        polymorphic_on=pjoin.c.type, polymorphic_identity='employee')
    manager_mapper = mapper(Manager, managers_table, inherits=employee_mapper, \
        concrete=True, polymorphic_identity='manager')
    engineer_mapper = mapper(Engineer, engineers_table, inherits=employee_mapper, \
        concrete=True, polymorphic_identity='engineer')

Upon select, the polymorphic union produces a query like this:

.. sourcecode:: python+sql

    session.query(Employee).all()
    {opensql}
    SELECT pjoin.type AS pjoin_type, pjoin.manager_data AS pjoin_manager_data, pjoin.employee_id AS pjoin_employee_id,
    pjoin.name AS pjoin_name, pjoin.engineer_info AS pjoin_engineer_info
    FROM (
        SELECT employees.employee_id AS employee_id, CAST(NULL AS VARCHAR(50)) AS manager_data, employees.name AS name,
        CAST(NULL AS VARCHAR(50)) AS engineer_info, 'employee' AS type
        FROM employees
    UNION ALL
        SELECT managers.employee_id AS employee_id, managers.manager_data AS manager_data, managers.name AS name,
        CAST(NULL AS VARCHAR(50)) AS engineer_info, 'manager' AS type
        FROM managers
    UNION ALL
        SELECT engineers.employee_id AS employee_id, CAST(NULL AS VARCHAR(50)) AS manager_data, engineers.name AS name,
        engineers.engineer_info AS engineer_info, 'engineer' AS type
        FROM engineers
    ) AS pjoin
    []

Using Relationships with Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both joined-table and single table inheritance scenarios produce mappings which are usable in :func:`~sqlalchemy.orm.relationship` functions; that is, it's possible to map a parent object to a child object which is polymorphic.  Similarly, inheriting mappers can have :func:`~sqlalchemy.orm.relationship` objects of their own at any level, which are inherited to each child class.  The only requirement for relationships is that there is a table relationship between parent and child.  An example is the following modification to the joined table inheritance example, which sets a bi-directional relationship between ``Employee`` and ``Company``:

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

SQLAlchemy has a lot of experience in this area; the optimized "outer join" approach can be used freely for parent and child relationships, eager loads are fully useable, :func:`~sqlalchemy.orm.aliased` objects and other techniques are fully supported as well.

In a concrete inheritance scenario, mapping relationships is more difficult since the distinct classes do not share a table.  In this case, you *can* establish a relationship from parent to child if a join condition can be constructed from parent to child, if each child table contains a foreign key to the parent:

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

    mapper(Employee, employees_table, with_polymorphic=('*', pjoin), polymorphic_on=pjoin.c.type, polymorphic_identity='employee')
    mapper(Manager, managers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='manager')
    mapper(Engineer, engineers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='engineer')
    mapper(Company, companies, properties={
        'employees': relationship(Employee)
    })

The big limitation with concrete table inheritance is that :func:`~sqlalchemy.orm.relationship` objects placed on each concrete mapper do **not** propagate to child mappers.  If you want to have the same :func:`~sqlalchemy.orm.relationship` objects set up on all concrete mappers, they must be configured manually on each.  To configure back references in such a configuration the ``back_populates`` keyword may be used instead of ``backref``, such as below where both ``A(object)`` and ``B(A)`` bidirectionally reference ``C``::

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
        'many_a':relationship(A, collection_class=set, back_populates='some_c'),
    })


Mapping a Class against Multiple Tables
----------------------------------------

Mappers can be constructed against arbitrary relational units (called ``Selectables``) as well as plain ``Tables``.  For example, The ``join`` keyword from the SQL package creates a neat selectable unit comprised of multiple tables, complete with its own composite primary key, which can be passed in to a mapper as the table.

.. sourcecode:: python+sql

    # a class
    class AddressUser(object):
        pass

    # define a Join
    j = join(users_table, addresses_table)

    # map to it - the identity of an AddressUser object will be
    # based on (user_id, address_id) since those are the primary keys involved
    mapper(AddressUser, j, properties={
        'user_id': [users_table.c.user_id, addresses_table.c.user_id]
    })

A second example:

.. sourcecode:: python+sql

    # many-to-many join on an association table
    j = join(users_table, userkeywords,
            users_table.c.user_id==userkeywords.c.user_id).join(keywords,
               userkeywords.c.keyword_id==keywords.c.keyword_id)

    # a class
    class KeywordUser(object):
        pass

    # map to it - the identity of a KeywordUser object will be
    # (user_id, keyword_id) since those are the primary keys involved
    mapper(KeywordUser, j, properties={
        'user_id': [users_table.c.user_id, userkeywords.c.user_id],
        'keyword_id': [userkeywords.c.keyword_id, keywords.c.keyword_id]
    })

In both examples above, "composite" columns were added as properties to the mappers; these are aggregations of multiple columns into one mapper property, which instructs the mapper to keep both of those columns set at the same value.

Mapping a Class against Arbitrary Selects
------------------------------------------


Similar to mapping against a join, a plain select() object can be used with a mapper as well.  Below, an example select which contains two aggregate functions and a group_by is mapped to a class:

.. sourcecode:: python+sql

    s = select([customers,
                func.count(orders).label('order_count'),
                func.max(orders.price).label('highest_order')],
                customers.c.customer_id==orders.c.customer_id,
                group_by=[c for c in customers.c]
                ).alias('somealias')
    class Customer(object):
        pass

    mapper(Customer, s)

Above, the "customers" table is joined against the "orders" table to produce a full row for each customer row, the total count of related rows in the "orders" table, and the highest price in the "orders" table, grouped against the full set of columns in the "customers" table.  That query is then mapped against the Customer class.  New instances of Customer will contain attributes for each column in the "customers" table as well as an "order_count" and "highest_order" attribute.  Updates to the Customer object will only be reflected in the "customers" table and not the "orders" table.  This is because the primary key columns of the "orders" table are not represented in this mapper and therefore the table is not affected by save or delete operations.

Multiple Mappers for One Class
-------------------------------

The first mapper created for a certain class is known as that class's "primary mapper."  Other mappers can be created as well on the "load side" - these are called **secondary mappers**.   This is a mapper that must be constructed with the keyword argument ``non_primary=True``, and represents a load-only mapper.  Objects that are loaded with a secondary mapper will have their save operation processed by the primary mapper.  It is also invalid to add new :func:`~sqlalchemy.orm.relationship` objects to a non-primary mapper. To use this mapper with the Session, specify it to the :class:`~sqlalchemy.orm.session.Session.query` method:

example:

.. sourcecode:: python+sql

    # primary mapper
    mapper(User, users_table)

    # make a secondary mapper to load User against a join
    othermapper = mapper(User, users_table.join(someothertable), non_primary=True)

    # select
    result = session.query(othermapper).select()

The "non primary mapper" is a rarely needed feature of SQLAlchemy; in most cases, the :class:`~sqlalchemy.orm.query.Query` object can produce any kind of query that's desired.  It's recommended that a straight :class:`~sqlalchemy.orm.query.Query` be used in place of a non-primary mapper unless the mapper approach is absolutely needed.  Current use cases for the "non primary mapper" are when you want to map the class to a particular select statement or view to which additional query criterion can be added, and for when the particular mapped select statement or view is to be placed in a :func:`~sqlalchemy.orm.relationship` of a parent mapper.

Multiple "Persistence" Mappers for One Class
---------------------------------------------

The non_primary mapper defines alternate mappers for the purposes of loading objects.  What if we want the same class to be *persisted* differently, such as to different tables ?   SQLAlchemy
refers to this as the "entity name" pattern, and in Python one can use a recipe which creates
anonymous subclasses which are distinctly mapped.  See the recipe at `Entity Name <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.

Constructors and Object Initialization
---------------------------------------

Mapping imposes no restrictions or requirements on the constructor (``__init__``) method for the class. You are free to require any arguments for the function
that you wish, assign attributes to the instance that are unknown to the ORM, and generally do anything else you would normally do when writing a constructor
for a Python class.

The SQLAlchemy ORM does not call ``__init__`` when recreating objects from database rows. The ORM's process is somewhat akin to the Python standard library's
``pickle`` module, invoking the low level ``__new__`` method and then quietly restoring attributes directly on the instance rather than calling ``__init__``.

If you need to do some setup on database-loaded instances before they're ready to use, you can use the ``@reconstructor`` decorator to tag a method as the ORM
counterpart to ``__init__``. SQLAlchemy will call this method with no arguments every time it loads or reconstructs one of your instances. This is useful for
recreating transient properties that are normally assigned in your ``__init__``::

    from sqlalchemy import orm

    class MyMappedClass(object):
        def __init__(self, data):
            self.data = data
            # we need stuff on all instances, but not in the database.
            self.stuff = []

        @orm.reconstructor
        def init_on_load(self):
            self.stuff = []

When ``obj = MyMappedClass()`` is executed, Python calls the ``__init__`` method as normal and the ``data`` argument is required. When instances are loaded
during a :class:`~sqlalchemy.orm.query.Query` operation as in ``query(MyMappedClass).one()``, ``init_on_load`` is called instead.

Any method may be tagged as the :func:`~sqlalchemy.orm.reconstructor`, even the ``__init__`` method. SQLAlchemy will call the reconstructor method with no arguments. Scalar
(non-collection) database-mapped attributes of the instance will be available for use within the function. Eagerly-loaded collections are generally not yet
available and will usually only contain the first element. ORM state changes made to objects at this stage will not be recorded for the next flush()
operation, so the activity within a reconstructor should be conservative.

While the ORM does not call your ``__init__`` method, it will modify the class's ``__init__`` slightly. The method is lightly wrapped to act as a trigger for
the ORM, allowing mappers to be compiled automatically and will fire a :func:`~sqlalchemy.orm.interfaces.MapperExtension.init_instance` event that :class:`~sqlalchemy.orm.interfaces.MapperExtension` objects may listen for.
:class:`~sqlalchemy.orm.interfaces.MapperExtension` objects can also listen for a ``reconstruct_instance`` event, analogous to the :func:`~sqlalchemy.orm.reconstructor` decorator above.

.. _extending_mapper:

Extending Mapper
-----------------

Mappers can have functionality augmented or replaced at many points in its execution via the usage of the MapperExtension class.  This class is just a series of "hooks" where various functionality takes place.  An application can make its own MapperExtension objects, overriding only the methods it needs.  Methods that are not overridden return the special value ``sqlalchemy.orm.EXT_CONTINUE`` to allow processing to continue to the next MapperExtension or simply proceed normally if there are no more extensions.

API documentation for MapperExtension: :class:`sqlalchemy.orm.interfaces.MapperExtension`

To use MapperExtension, make your own subclass of it and just send it off to a mapper::

    m = mapper(User, users_table, extension=MyExtension())

Multiple extensions will be chained together and processed in order; they are specified as a list::

    m = mapper(User, users_table, extension=[ext1, ext2, ext3])

.. _advdatamapping_relationship:

Relationship Configuration
==========================

Basic Relational Patterns
--------------------------

A quick walkthrough of the basic relational patterns.   Note that the :func:`~sqlalchemy.orm.relationship()` function is known as :func:`~sqlalchemy.orm.relation()`
in all SQLAlchemy versions prior to 0.6beta2, including the 0.5 and 0.4 series.

One To Many
~~~~~~~~~~~~

A one to many relationship places a foreign key in the child table referencing the parent.   SQLAlchemy creates the relationship as a collection on the parent object containing instances of the child object.

.. sourcecode:: python+sql

    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True))

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('parent.id')))

    class Parent(object):
        pass

    class Child(object):
        pass

    mapper(Parent, parent_table, properties={
        'children': relationship(Child)
    })

    mapper(Child, child_table)

To establish a bi-directional relationship in one-to-many, where the "reverse" side is a many to one, specify the ``backref`` option:

.. sourcecode:: python+sql

    mapper(Parent, parent_table, properties={
        'children': relationship(Child, backref='parent')
    })

    mapper(Child, child_table)

``Child`` will get a ``parent`` attribute with many-to-one semantics.

Many To One
~~~~~~~~~~~~


Many to one places a foreign key in the parent table referencing the child.  The mapping setup is identical to one-to-many, however SQLAlchemy creates the relationship as a scalar attribute on the parent object referencing a single instance of the child object.

.. sourcecode:: python+sql

    parent_table = Table('parent', metadata,
        Column('id', Integer, primary_key=True),
        Column('child_id', Integer, ForeignKey('child.id')))

    child_table = Table('child', metadata,
        Column('id', Integer, primary_key=True),
        )

    class Parent(object):
        pass

    class Child(object):
        pass

    mapper(Parent, parent_table, properties={
        'child': relationship(Child)
    })

    mapper(Child, child_table)

Backref behavior is available here as well, where ``backref="parents"`` will place a one-to-many collection on the ``Child`` class.

One To One
~~~~~~~~~~~


One To One is essentially a bi-directional relationship with a scalar attribute on both sides.  To achieve this, the ``uselist=False`` flag indicates the placement of a scalar attribute instead of a collection on the "many" side of the relationship.  To convert one-to-many into one-to-one:

.. sourcecode:: python+sql

    mapper(Parent, parent_table, properties={
        'child': relationship(Child, uselist=False, backref='parent')
    })

Or to turn many-to-one into one-to-one:

.. sourcecode:: python+sql

    mapper(Parent, parent_table, properties={
        'child': relationship(Child, backref=backref('parent', uselist=False))
    })

Many To Many
~~~~~~~~~~~~~


Many to Many adds an association table between two classes.  The association table is indicated by the ``secondary`` argument to :func:`~sqlalchemy.orm.relationship`.

.. sourcecode:: python+sql

    left_table = Table('left', metadata,
        Column('id', Integer, primary_key=True))

    right_table = Table('right', metadata,
        Column('id', Integer, primary_key=True))

    association_table = Table('association', metadata,
        Column('left_id', Integer, ForeignKey('left.id')),
        Column('right_id', Integer, ForeignKey('right.id')),
        )

    mapper(Parent, left_table, properties={
        'children': relationship(Child, secondary=association_table)
    })

    mapper(Child, right_table)

For a bi-directional relationship, both sides of the relationship contain a collection by default, which can be modified on either side via the ``uselist`` flag to be scalar.  The ``backref`` keyword will automatically use the same ``secondary`` argument for the reverse relationship:

.. sourcecode:: python+sql

    mapper(Parent, left_table, properties={
        'children': relationship(Child, secondary=association_table, backref='parents')
    })

.. _association_pattern:

Association Object
~~~~~~~~~~~~~~~~~~

The association object pattern is a variant on many-to-many:  it specifically is used when your association table contains additional columns beyond those which are foreign keys to the left and right tables.  Instead of using the ``secondary`` argument, you map a new class directly to the association table.  The left side of the relationship references the association object via one-to-many, and the association class references the right side via many-to-one.

.. sourcecode:: python+sql

    left_table = Table('left', metadata,
        Column('id', Integer, primary_key=True))

    right_table = Table('right', metadata,
        Column('id', Integer, primary_key=True))

    association_table = Table('association', metadata,
        Column('left_id', Integer, ForeignKey('left.id'), primary_key=True),
        Column('right_id', Integer, ForeignKey('right.id'), primary_key=True),
        Column('data', String(50))
        )

    mapper(Parent, left_table, properties={
        'children':relationship(Association)
    })

    mapper(Association, association_table, properties={
        'child':relationship(Child)
    })

    mapper(Child, right_table)

The bi-directional version adds backrefs to both relationships:

.. sourcecode:: python+sql

    mapper(Parent, left_table, properties={
        'children':relationship(Association, backref="parent")
    })

    mapper(Association, association_table, properties={
        'child':relationship(Child, backref="parent_assocs")
    })

    mapper(Child, right_table)

Working with the association pattern in its direct form requires that child objects are associated with an association instance before being appended to the parent; similarly, access from parent to child goes through the association object:

.. sourcecode:: python+sql

    # create parent, append a child via association
    p = Parent()
    a = Association()
    a.child = Child()
    p.children.append(a)

    # iterate through child objects via association, including association
    # attributes
    for assoc in p.children:
        print assoc.data
        print assoc.child

To enhance the association object pattern such that direct access to the ``Association`` object is optional, SQLAlchemy provides the :ref:`associationproxy`.

**Important Note**:  it is strongly advised that the ``secondary`` table argument not be combined with the Association Object pattern, unless the :func:`~sqlalchemy.orm.relationship` which contains the ``secondary`` argument is marked ``viewonly=True``.  Otherwise, SQLAlchemy may persist conflicting data to the underlying association table since it is represented by two conflicting mappings.  The Association Proxy pattern should be favored in the case where access to the underlying association data is only sometimes needed.

Adjacency List Relationships
-----------------------------


The **adjacency list** pattern is a common relational pattern whereby a table contains a foreign key reference to itself.  This is the most common and simple way to represent hierarchical data in flat tables.  The other way is the "nested sets" model, sometimes called "modified preorder".  Despite what many online articles say about modified preorder, the adjacency list model is probably the most appropriate pattern for the large majority of hierarchical storage needs, for reasons of concurrency, reduced complexity, and that modified preorder has little advantage over an application which can fully load subtrees into the application space.

SQLAlchemy commonly refers to an adjacency list relationship as a **self-referential mapper**.  In this example, we'll work with a single table called ``treenodes`` to represent a tree structure::

    nodes = Table('treenodes', metadata,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('treenodes.id')),
        Column('data', String(50)),
        )

A graph such as the following::

    root --+---> child1
           +---> child2 --+--> subchild1
           |              +--> subchild2
           +---> child3

Would be represented with data such as::

    id       parent_id     data
    ---      -------       ----
    1        NULL          root
    2        1             child1
    3        1             child2
    4        3             subchild1
    5        3             subchild2
    6        1             child3

SQLAlchemy's ``mapper()`` configuration for a self-referential one-to-many relationship is exactly like a "normal" one-to-many relationship.  When SQLAlchemy encounters the foreign key relationship from ``treenodes`` to ``treenodes``, it assumes one-to-many unless told otherwise:

.. sourcecode:: python+sql

    # entity class
    class Node(object):
        pass

    mapper(Node, nodes, properties={
        'children': relationship(Node)
    })

To create a many-to-one relationship from child to parent, an extra indicator of the "remote side" is added, which contains the :class:`~sqlalchemy.schema.Column` object or objects indicating the remote side of the relationship:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'parent': relationship(Node, remote_side=[nodes.c.id])
    })

And the bi-directional version combines both:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'children': relationship(Node, backref=backref('parent', remote_side=[nodes.c.id]))
    })

There are several examples included with SQLAlchemy illustrating self-referential strategies; these include :ref:`examples_adjacencylist` and :ref:`examples_xmlpersistence`.

Self-Referential Query Strategies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Querying self-referential structures is done in the same way as any other query in SQLAlchemy, such as below, we query for any node whose ``data`` attribute stores the value ``child2``:

.. sourcecode:: python+sql

    # get all nodes named 'child2'
    session.query(Node).filter(Node.data=='child2')

On the subject of joins, i.e. those described in `datamapping_joins`, self-referential structures require the usage of aliases so that the same table can be referenced multiple times within the FROM clause of the query.   Aliasing can be done either manually using the ``nodes`` :class:`~sqlalchemy.schema.Table` object as a source of aliases:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2'
    nodealias = nodes.alias()
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        filter(and_(Node.parent_id==nodealias.c.id, nodealias.c.data=='child2')).all()
    SELECT treenodes.id AS treenodes_id, treenodes.parent_id AS treenodes_parent_id, treenodes.data AS treenodes_data
    FROM treenodes, treenodes AS treenodes_1
    WHERE treenodes.data = ? AND treenodes.parent_id = treenodes_1.id AND treenodes_1.data = ?
    ['subchild1', 'child2']

or automatically, using ``join()`` with ``aliased=True``:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2'
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        join('parent', aliased=True).filter(Node.data=='child2').all()
    SELECT treenodes.id AS treenodes_id, treenodes.parent_id AS treenodes_parent_id, treenodes.data AS treenodes_data
    FROM treenodes JOIN treenodes AS treenodes_1 ON treenodes_1.id = treenodes.parent_id
    WHERE treenodes.data = ? AND treenodes_1.data = ?
    ['subchild1', 'child2']

To add criterion to multiple points along a longer join, use ``from_joinpoint=True``:

.. sourcecode:: python+sql

    # get all nodes named 'subchild1' with a parent named 'child2' and a grandparent 'root'
    {sql}session.query(Node).filter(Node.data=='subchild1').\
        join('parent', aliased=True).filter(Node.data=='child2').\
        join('parent', aliased=True, from_joinpoint=True).filter(Node.data=='root').all()
    SELECT treenodes.id AS treenodes_id, treenodes.parent_id AS treenodes_parent_id, treenodes.data AS treenodes_data
    FROM treenodes JOIN treenodes AS treenodes_1 ON treenodes_1.id = treenodes.parent_id JOIN treenodes AS treenodes_2 ON treenodes_2.id = treenodes_1.parent_id
    WHERE treenodes.data = ? AND treenodes_1.data = ? AND treenodes_2.data = ?
    ['subchild1', 'child2', 'root']

Configuring Eager Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~

Eager loading of relationships occurs using joins or outerjoins from parent to child table during a normal query operation, such that the parent and its child collection can be populated from a single SQL statement, or a second statement for all collections at once.  SQLAlchemy's joined and subquery eager loading uses aliased tables in all cases when joining to related items, so it is compatible with self-referential joining.  However, to use eager loading with a self-referential relationship, SQLAlchemy needs to be told how many levels deep it should join; otherwise the eager load will not take place.  This depth setting is configured via ``join_depth``:

.. sourcecode:: python+sql

    mapper(Node, nodes, properties={
        'children': relationship(Node, lazy='joined', join_depth=2)
    })

    {sql}session.query(Node).all()
    SELECT treenodes_1.id AS treenodes_1_id, treenodes_1.parent_id AS treenodes_1_parent_id, treenodes_1.data AS treenodes_1_data, treenodes_2.id AS treenodes_2_id, treenodes_2.parent_id AS treenodes_2_parent_id, treenodes_2.data AS treenodes_2_data, treenodes.id AS treenodes_id, treenodes.parent_id AS treenodes_parent_id, treenodes.data AS treenodes_data
    FROM treenodes LEFT OUTER JOIN treenodes AS treenodes_2 ON treenodes.id = treenodes_2.parent_id LEFT OUTER JOIN treenodes AS treenodes_1 ON treenodes_2.id = treenodes_1.parent_id
    []

Specifying Alternate Join Conditions to relationship()
------------------------------------------------------

The :func:`~sqlalchemy.orm.relationship` function uses the foreign key relationship between the parent and child tables to formulate the **primary join condition** between parent and child; in the case of a many-to-many relationship it also formulates the **secondary join condition**::

      one to many/many to one:
      ------------------------

      parent_table -->  parent_table.c.id == child_table.c.parent_id -->  child_table
                                     primaryjoin

      many to many:
      -------------

      parent_table -->  parent_table.c.id == secondary_table.c.parent_id -->
                                     primaryjoin

                        secondary_table.c.child_id == child_table.c.id --> child_table
                                    secondaryjoin

If you are working with a :class:`~sqlalchemy.schema.Table` which has no :class:`~sqlalchemy.schema.ForeignKey` objects on it (which can be the case when using reflected tables with MySQL), or if the join condition cannot be expressed by a simple foreign key relationship, use the ``primaryjoin`` and possibly ``secondaryjoin`` conditions to create the appropriate relationship.

In this example we create a relationship ``boston_addresses`` which will only load the user addresses with a city of "Boston":

.. sourcecode:: python+sql

    class User(object):
        pass
    class Address(object):
        pass

    mapper(Address, addresses_table)
    mapper(User, users_table, properties={
        'boston_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='Boston'))
    })

Many to many relationships can be customized by one or both of ``primaryjoin`` and ``secondaryjoin``, shown below with just the default many-to-many relationship explicitly set:

.. sourcecode:: python+sql

    class User(object):
        pass
    class Keyword(object):
        pass
    mapper(Keyword, keywords_table)
    mapper(User, users_table, properties={
        'keywords': relationship(Keyword, secondary=userkeywords_table,
            primaryjoin=users_table.c.user_id==userkeywords_table.c.user_id,
            secondaryjoin=userkeywords_table.c.keyword_id==keywords_table.c.keyword_id
            )
    })

Specifying Foreign Keys
~~~~~~~~~~~~~~~~~~~~~~~~


When using ``primaryjoin`` and ``secondaryjoin``, SQLAlchemy also needs to be aware of which columns in the relationship reference the other.  In most cases, a :class:`~sqlalchemy.schema.Table` construct will have :class:`~sqlalchemy.schema.ForeignKey` constructs which take care of this; however, in the case of reflected tables on a database that does not report FKs (like MySQL ISAM) or when using join conditions on columns that don't have foreign keys, the :func:`~sqlalchemy.orm.relationship` needs to be told specifically which columns are "foreign" using the ``foreign_keys`` collection:

.. sourcecode:: python+sql

    mapper(Address, addresses_table)
    mapper(User, users_table, properties={
        'addresses': relationship(Address, primaryjoin=
                    users_table.c.user_id==addresses_table.c.user_id,
                    foreign_keys=[addresses_table.c.user_id])
    })

Building Query-Enabled Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Very ambitious custom join conditions may fail to be directly persistable, and in some cases may not even load correctly.  To remove the persistence part of the equation, use the flag ``viewonly=True`` on the :func:`~sqlalchemy.orm.relationship`, which establishes it as a read-only attribute (data written to the collection will be ignored on flush()).  However, in extreme cases, consider using a regular Python property in conjunction with :class:`~sqlalchemy.orm.query.Query` as follows:

.. sourcecode:: python+sql

    class User(object):
        def _get_addresses(self):
            return object_session(self).query(Address).with_parent(self).filter(...).all()
        addresses = property(_get_addresses)

Multiple Relationships against the Same Parent/Child
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Theres no restriction on how many times you can relate from parent to child.  SQLAlchemy can usually figure out what you want, particularly if the join conditions are straightforward.  Below we add a ``newyork_addresses`` attribute to complement the ``boston_addresses`` attribute:

.. sourcecode:: python+sql

    mapper(User, users_table, properties={
        'boston_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='Boston')),
        'newyork_addresses': relationship(Address, primaryjoin=
                    and_(users_table.c.user_id==addresses_table.c.user_id,
                    addresses_table.c.city=='New York')),
    })

.. _alternate_collection_implementations:

Rows that point to themselves / Mutually Dependent Rows
-------------------------------------------------------

This is a very specific case where relationship() must perform an INSERT and a second UPDATE in order to properly populate a row (and vice versa an UPDATE and DELETE in order to delete without violating foreign key constraints).   The two use cases are:

 * A table contains a foreign key to itself, and a single row will have a foreign key value pointing to its own primary key.
 * Two tables each contain a foreign key referencing the other table, with a row in each table referencing the other.

For example::

              user
    ---------------------------------
    user_id    name   related_user_id
       1       'ed'          1

Or::

                 widget                                                  entry
    -------------------------------------------             ---------------------------------
    widget_id     name        favorite_entry_id             entry_id      name      widget_id
       1       'somewidget'          5                         5       'someentry'     1

In the first case, a row points to itself.  Technically, a database that uses sequences such as PostgreSQL or Oracle can INSERT the row at once using a previously generated value, but databases which rely upon autoincrement-style primary key identifiers cannot.  The :func:`~sqlalchemy.orm.relationship` always assumes a "parent/child" model of row population during flush, so unless you are populating the primary key/foreign key columns directly, :func:`~sqlalchemy.orm.relationship` needs to use two statements.

In the second case, the "widget" row must be inserted before any referring "entry" rows, but then the "favorite_entry_id" column of that "widget" row cannot be set until the "entry" rows have been generated.  In this case, it's typically impossible to insert the "widget" and "entry" rows using just two INSERT statements; an UPDATE must be performed in order to keep foreign key constraints fulfilled.   The exception is if the foreign keys are configured as "deferred until commit" (a feature some databases support) and if the identifiers were populated manually (again essentially bypassing :func:`~sqlalchemy.orm.relationship`).

To enable the UPDATE after INSERT / UPDATE before DELETE behavior on :func:`~sqlalchemy.orm.relationship`, use the ``post_update`` flag on *one* of the relationships, preferably the many-to-one side::

    mapper(Widget, widget, properties={
        'entries':relationship(Entry, primaryjoin=widget.c.widget_id==entry.c.widget_id),
        'favorite_entry':relationship(Entry, primaryjoin=widget.c.favorite_entry_id==entry.c.entry_id, post_update=True)
    })

When a structure using the above mapping is flushed, the "widget" row will be INSERTed minus the "favorite_entry_id" value, then all the "entry" rows will be INSERTed referencing the parent "widget" row, and then an UPDATE statement will populate the "favorite_entry_id" column of the "widget" table (it's one row at a time for the time being).


.. _advdatamapping_entitycollections:

Alternate Collection Implementations
-------------------------------------

Mapping a one-to-many or many-to-many relationship results in a collection of values accessible through an attribute on the parent instance.  By default, this collection is a ``list``:

.. sourcecode:: python+sql

    mapper(Parent, properties={
        children = relationship(Child)
    })

    parent = Parent()
    parent.children.append(Child())
    print parent.children[0]

Collections are not limited to lists.  Sets, mutable sequences and almost any other Python object that can act as a container can be used in place of the default list, by specifying the ``collection_class`` option on :func:`~sqlalchemy.orm.relationship`.

.. sourcecode:: python+sql

    # use a set
    mapper(Parent, properties={
        children = relationship(Child, collection_class=set)
    })

    parent = Parent()
    child = Child()
    parent.children.add(child)
    assert child in parent.children


Custom Collection Implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use your own types for collections as well.  For most cases, simply inherit from ``list`` or ``set`` and add the custom behavior.

Collections in SQLAlchemy are transparently *instrumented*.  Instrumentation means that normal operations on the collection are tracked and result in changes being written to the database at flush time.  Additionally, collection operations can fire *events* which indicate some secondary operation must take place.  Examples of a secondary operation include saving the child item in the parent's :class:`~sqlalchemy.orm.session.Session` (i.e. the ``save-update`` cascade), as well as synchronizing the state of a bi-directional relationship (i.e. a ``backref``).

The collections package understands the basic interface of lists, sets and dicts and will automatically apply instrumentation to those built-in types and their subclasses.  Object-derived types that implement a basic collection interface are detected and instrumented via duck-typing:

.. sourcecode:: python+sql

    class ListLike(object):
        def __init__(self):
            self.data = []
        def append(self, item):
            self.data.append(item)
        def remove(self, item):
            self.data.remove(item)
        def extend(self, items):
            self.data.extend(items)
        def __iter__(self):
            return iter(self.data)
        def foo(self):
            return 'foo'

``append``, ``remove``, and ``extend`` are known list-like methods, and will be instrumented automatically.  ``__iter__`` is not a mutator method and won't be instrumented, and ``foo`` won't be either.

Duck-typing (i.e. guesswork) isn't rock-solid, of course, so you can be explicit about the interface you are implementing by providing an ``__emulates__`` class attribute:

.. sourcecode:: python+sql

    class SetLike(object):
        __emulates__ = set

        def __init__(self):
            self.data = set()
        def append(self, item):
            self.data.add(item)
        def remove(self, item):
            self.data.remove(item)
        def __iter__(self):
            return iter(self.data)

This class looks list-like because of ``append``, but ``__emulates__`` forces it to set-like.  ``remove`` is known to be part of the set interface and will be instrumented.

But this class won't work quite yet: a little glue is needed to adapt it for use by SQLAlchemy.  The ORM needs to know which methods to use to append, remove and iterate over members of the collection.  When using a type like ``list`` or ``set``, the appropriate methods are well-known and used automatically when present. This set-like class does not provide the expected ``add`` method, so we must supply an explicit mapping for the ORM via a decorator.

Annotating Custom Collections via Decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Decorators can be used to tag the individual methods the ORM needs to manage collections.  Use them when your class doesn't quite meet the regular interface for its container type, or you simply would like to use a different method to get the job done.

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import collection

    class SetLike(object):
        __emulates__ = set

        def __init__(self):
            self.data = set()

        @collection.appender
        def append(self, item):
            self.data.add(item)

        def remove(self, item):
            self.data.remove(item)

        def __iter__(self):
            return iter(self.data)

And that's all that's needed to complete the example.  SQLAlchemy will add instances via the ``append`` method.  ``remove`` and ``__iter__`` are the default methods for sets and will be used for removing and iteration.  Default methods can be changed as well:

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import collection

    class MyList(list):
        @collection.remover
        def zark(self, item):
            # do something special...

        @collection.iterator
        def hey_use_this_instead_for_iteration(self):
            # ...

There is no requirement to be list-, or set-like at all.  Collection classes can be any shape, so long as they have the append, remove and iterate interface marked for SQLAlchemy's use.  Append and remove methods will be called with a mapped entity as the single argument, and iterator methods are called with no arguments and must return an iterator.

Dictionary-Based Collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


A ``dict`` can be used as a collection, but a keying strategy is needed to map entities loaded by the ORM to key, value pairs.  The :mod:`sqlalchemy.orm.collections` package provides several built-in types for dictionary-based collections:

.. sourcecode:: python+sql

    from sqlalchemy.orm.collections import column_mapped_collection, attribute_mapped_collection, mapped_collection

    mapper(Item, items_table, properties={
        # key by column
        'notes': relationship(Note, collection_class=column_mapped_collection(notes_table.c.keyword)),
        # or named attribute
        'notes2': relationship(Note, collection_class=attribute_mapped_collection('keyword')),
        # or any callable
        'notes3': relationship(Note, collection_class=mapped_collection(lambda entity: entity.a + entity.b))
    })

    # ...
    item = Item()
    item.notes['color'] = Note('color', 'blue')
    print item.notes['color']

These functions each provide a ``dict`` subclass with decorated ``set`` and ``remove`` methods and the keying strategy of your choice.

The :class:`sqlalchemy.orm.collections.MappedCollection` class can be used as a base class for your custom types or as a mix-in to quickly add ``dict`` collection support to other classes.  It uses a keying function to delegate to ``__setitem__`` and ``__delitem__``:

.. sourcecode:: python+sql

    from sqlalchemy.util import OrderedDict
    from sqlalchemy.orm.collections import MappedCollection

    class NodeMap(OrderedDict, MappedCollection):
        """Holds 'Node' objects, keyed by the 'name' attribute with insert order maintained."""

        def __init__(self, *args, **kw):
            MappedCollection.__init__(self, keyfunc=lambda node: node.name)
            OrderedDict.__init__(self, *args, **kw)

The ORM understands the ``dict`` interface just like lists and sets, and will automatically instrument all dict-like methods if you choose to subclass ``dict`` or provide dict-like collection behavior in a duck-typed class.  You must decorate appender and remover methods, however- there are no compatible methods in the basic dictionary interface for SQLAlchemy to use by default.  Iteration will go through ``itervalues()`` unless otherwise decorated.

Instrumentation and Custom Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Many custom types and existing library classes can be used as a entity collection type as-is without further ado.  However, it is important to note that the instrumentation process _will_ modify the type, adding decorators around methods automatically.

The decorations are lightweight and no-op outside of relationships, but they do add unneeded overhead when triggered elsewhere.  When using a library class as a collection, it can be good practice to use the "trivial subclass" trick to restrict the decorations to just your usage in relationships.  For example:

.. sourcecode:: python+sql

    class MyAwesomeList(some.great.library.AwesomeList):
        pass

    # ... relationship(..., collection_class=MyAwesomeList)

The ORM uses this approach for built-ins, quietly substituting a trivial subclass when a ``list``, ``set`` or ``dict`` is used directly.

The collections package provides additional decorators and support for authoring custom types.  See the :mod:`sqlalchemy.orm.collections` package for more information and discussion of advanced usage and Python 2.3-compatible decoration options.

.. _mapper_loader_strategies:

Configuring Loader Strategies: Lazy Loading, Eager Loading
-----------------------------------------------------------

.. note:: SQLAlchemy version 0.6beta3 introduces the :func:`~sqlalchemy.orm.joinedload`, :func:`~sqlalchemy.orm.joinedload_all`, :func:`~sqlalchemy.orm.subqueryload` and :func:`~sqlalchemy.orm.subqueryload_all` functions described in this section.  In previous versions, including 0.5 and 0.4, use :func:`~sqlalchemy.orm.eagerload` and :func:`~sqlalchemy.orm.eagerload_all`.  Additionally, the ``lazy`` keyword argument on :func:`~sqlalchemy.orm.relationship` accepts the values ``True``, ``False`` and ``None`` in previous versions, whereas in the latest 0.6 it also accepts the arguments ``select``, ``joined``, ``noload``, and ``subquery``.

In the :ref:`ormtutorial_toplevel`, we introduced the concept of **Eager Loading**.  We used an ``option`` in conjunction with the :class:`~sqlalchemy.orm.query.Query` object in order to indicate that a relationship should be loaded at the same time as the parent, within a single SQL query:

.. sourcecode:: python+sql

    {sql}>>> jack = session.query(User).options(joinedload('addresses')).filter_by(name='jack').all() #doctest: +NORMALIZE_WHITESPACE
    SELECT addresses_1.id AS addresses_1_id, addresses_1.email_address AS addresses_1_email_address,
    addresses_1.user_id AS addresses_1_user_id, users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ?
    ['jack']

By default, all inter-object relationships are **lazy loading**.  The scalar or collection attribute associated with a :func:`~sqlalchemy.orm.relationship` contains a trigger which fires the first time the attribute is accessed, which issues a SQL call at that point:

.. sourcecode:: python+sql

    {sql}>>> jack.addresses
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address, 
    addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id
    [5]
    {stop}[<Address(u'jack@google.com')>, <Address(u'j25@yahoo.com')>]

A second option for eager loading exists, called "subquery" loading.   This kind of eager loading emits an additional SQL statement for each collection requested, aggregated across all parent objects:

.. sourcecode:: python+sql
    
    {sql}>>>jack = session.query(User).options(subqueryload('addresses')).filter_by(name='jack').all() 
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, 
    users.password AS users_password 
    FROM users 
    WHERE users.name = ?
    ('jack',)
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address, 
    addresses.user_id AS addresses_user_id, anon_1.users_id AS anon_1_users_id 
    FROM (SELECT users.id AS users_id 
    FROM users 
    WHERE users.name = ?) AS anon_1 JOIN addresses ON anon_1.users_id = addresses.user_id 
    ORDER BY anon_1.users_id, addresses.id
    ('jack',)

The default **loader strategy** for any :func:`~sqlalchemy.orm.relationship` is configured by the ``lazy`` keyword argument, which defaults to ``select``.  Below we set it as ``joined`` so that the ``children`` relationship is eager loading, using a join:

.. sourcecode:: python+sql

    # load the 'children' collection using LEFT OUTER JOIN
    mapper(Parent, parent_table, properties={
        'children': relationship(Child, lazy='joined')
    })

We can also set it to eagerly load using a second query for all collections, using ``subquery``:

.. sourcecode:: python+sql

    # load the 'children' attribute using a join to a subquery
    mapper(Parent, parent_table, properties={
        'children': relationship(Child, lazy='subquery')
    })

When querying, all three choices of loader strategy are available on a per-query basis, using the :func:`~sqlalchemy.orm.joinedload`, :func:`~sqlalchemy.orm.subqueryload` and :func:`~sqlalchemy.orm.lazyload` query options:

.. sourcecode:: python+sql

    # set children to load lazily
    session.query(Parent).options(lazyload('children')).all()

    # set children to load eagerly with a join
    session.query(Parent).options(joinedload('children')).all()

    # set children to load eagerly with a second statement
    session.query(Parent).options(subqueryload('children')).all()

To reference a relationship that is deeper than one level, separate the names by periods:

.. sourcecode:: python+sql

    session.query(Parent).options(joinedload('foo.bar.bat')).all()

When using dot-separated names with :func:`~sqlalchemy.orm.joinedload` or :func:`~sqlalchemy.orm.subqueryload`, option applies **only** to the actual attribute named, and **not** its ancestors.  For example, suppose a mapping from ``A`` to ``B`` to ``C``, where the relationships, named ``atob`` and ``btoc``, are both lazy-loading.  A statement like the following:

.. sourcecode:: python+sql

    session.query(A).options(joinedload('atob.btoc')).all()

will load only ``A`` objects to start.  When the ``atob`` attribute on each ``A`` is accessed, the returned ``B`` objects will *eagerly* load their ``C`` objects.

Therefore, to modify the eager load to load both ``atob`` as well as ``btoc``, place joinedloads for both:

.. sourcecode:: python+sql

    session.query(A).options(joinedload('atob'), joinedload('atob.btoc')).all()

or more simply just use :func:`~sqlalchemy.orm.joinedload_all` or :func:`~sqlalchemy.orm.subqueryload_all`:

.. sourcecode:: python+sql

    session.query(A).options(joinedload_all('atob.btoc')).all()

There are two other loader strategies available, **dynamic loading** and **no loading**; these are described in :ref:`largecollections`.

What Kind of Loading to Use ?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Which type of loading to use typically comes down to optimizing the tradeoff between number of SQL executions, complexity of SQL emitted, and amount of data fetched.   Lets take two examples, a :func:`~sqlalchemy.orm.relationship` which references a collection, and a :func:`~sqlalchemy.orm.relationship` that references a scalar many-to-one reference.

* One to Many Collection

 * When using the default lazy loading, if you load 100 objects, and then access a collection on each of
   them, a total of 101 SQL statements will be emitted, although each statement will typically be a
   simple SELECT without any joins.
   
 * When using joined loading, the load of 100 objects and their collections will emit only one SQL
   statement.  However, the 
   total number of rows fetched will be equal to the sum of the size of all the collections, plus one 
   extra row for each parent object that has an empty collection.  Each row will also contain the full
   set of columns represented by the parents, repeated for each collection item - SQLAlchemy does not
   re-fetch these columns other than those of the primary key, however most DBAPIs (with some 
   exceptions) will transmit the full data of each parent over the wire to the client connection in 
   any case.  Therefore joined eager loading only makes sense when the size of the collections are 
   relatively small.  The LEFT OUTER JOIN can also be performance intensive compared to an INNER join.
   
 * When using subquery loading, the load of 100 objects will emit two SQL statements.  The second
   statement will fetch a total number of rows equal to the sum of the size of all collections.  An
   INNER JOIN is used, and a minimum of parent columns are requested, only the primary keys.  So a 
   subquery load makes sense when the collections are larger.
   
 * When multiple levels of depth are used with joined or subquery loading, loading collections-within-
   collections will multiply the total number of rows fetched in a cartesian fashion.  Both forms
   of eager loading always join from the original parent class.
   
* Many to One Reference

 * When using the default lazy loading, a load of 100 objects will like in the case of the collection
   emit as many as 101 SQL statements.  However - there is a significant exception to this, in that
   if the many-to-one reference is a simple foreign key reference to the target's primary key, each
   reference will be checked first in the current identity map using ``query.get()``.  So here, 
   if the collection of objects references a relatively small set of target objects, or the full set
   of possible target objects have already been loaded into the session and are strongly referenced,
   using the default of `lazy='select'` is by far the most efficient way to go.
  
 * When using joined loading, the load of 100 objects will emit only one SQL statement.   The join
   will be a LEFT OUTER JOIN, and the total number of rows will be equal to 100 in all cases.  
   If you know that each parent definitely has a child (i.e. the foreign
   key reference is NOT NULL), the joined load can be configured with ``innerjoin=True``, which is
   usually specified within the :func:`~sqlalchemy.orm.relationship`.   For a load of objects where
   there are many possible target references which may have not been loaded already, joined loading
   with an INNER JOIN is extremely efficient.
   
 * Subquery loading will issue a second load for all the child objects, so for a load of 100 objects
   there would be two SQL statements emitted.  There's probably not much advantage here over
   joined loading, however, except perhaps that subquery loading can use an INNER JOIN in all cases
   whereas joined loading requires that the foreign key is NOT NULL.

Routing Explicit Joins/Statements into Eagerly Loaded Collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The behavior of :func:`~sqlalchemy.orm.joinedload()` is such that joins are created automatically, the results of which are routed into collections and scalar references on loaded objects.  It is often the case that a query already includes the necessary joins which represent a particular collection or scalar reference, and the joins added by the joinedload feature are redundant - yet you'd still like the collections/references to be populated.

For this SQLAlchemy supplies the :func:`~sqlalchemy.orm.contains_eager()` option.  This option is used in the same manner as the :func:`~sqlalchemy.orm.joinedload()` option except it is assumed that the :class:`~sqlalchemy.orm.query.Query` will specify the appropriate joins explicitly.  Below it's used with a ``from_statement`` load::

    # mapping is the users->addresses mapping
    mapper(User, users_table, properties={
        'addresses': relationship(Address, addresses_table)
    })

    # define a query on USERS with an outer join to ADDRESSES
    statement = users_table.outerjoin(addresses_table).select().apply_labels()

    # construct a Query object which expects the "addresses" results
    query = session.query(User).options(contains_eager('addresses'))

    # get results normally
    r = query.from_statement(statement)

It works just as well with an inline ``Query.join()`` or ``Query.outerjoin()``::

    session.query(User).outerjoin(User.addresses).options(contains_eager(User.addresses)).all()

If the "eager" portion of the statement is "aliased", the ``alias`` keyword argument to :func:`~sqlalchemy.orm.contains_eager` may be used to indicate it.  This is a string alias name or reference to an actual :class:`~sqlalchemy.sql.expression.Alias` (or other selectable) object:

.. sourcecode:: python+sql

    # use an alias of the Address entity
    adalias = aliased(Address)

    # construct a Query object which expects the "addresses" results
    query = session.query(User).\
        outerjoin((adalias, User.addresses)).\
        options(contains_eager(User.addresses, alias=adalias))

    # get results normally
    {sql}r = query.all()
    SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, adalias.address_id AS adalias_address_id,
    adalias.user_id AS adalias_user_id, adalias.email_address AS adalias_email_address, (...other columns...)
    FROM users LEFT OUTER JOIN email_addresses AS email_addresses_1 ON users.user_id = email_addresses_1.user_id

The ``alias`` argument is used only as a source of columns to match up to the result set.  You can use it even to match up the result to arbitrary label names in a string SQL statement, by passing a selectable() which links those labels to the mapped :class:`~sqlalchemy.schema.Table`::

    # label the columns of the addresses table
    eager_columns = select([
                        addresses.c.address_id.label('a1'),
                        addresses.c.email_address.label('a2'),
                        addresses.c.user_id.label('a3')])

    # select from a raw SQL statement which uses those label names for the
    # addresses table.  contains_eager() matches them up.
    query = session.query(User).\
        from_statement("select users.*, addresses.address_id as a1, "
                "addresses.email_address as a2, addresses.user_id as a3 "
                "from users left outer join addresses on users.user_id=addresses.user_id").\
        options(contains_eager(User.addresses, alias=eager_columns))

The path given as the argument to :func:`~sqlalchemy.orm.contains_eager` needs to be a full path from the starting entity.  For example if we were loading ``Users->orders->Order->items->Item``, the string version would look like::

    query(User).options(contains_eager('orders', 'items'))

Or using the class-bound descriptor::

    query(User).options(contains_eager(User.orders, Order.items))

A variant on :func:`~sqlalchemy.orm.contains_eager` is the ``contains_alias()`` option, which is used in the rare case that the parent object is loaded from an alias within a user-defined SELECT statement::

    # define an aliased UNION called 'ulist'
    statement = users.select(users.c.user_id==7).union(users.select(users.c.user_id>7)).alias('ulist')

    # add on an eager load of "addresses"
    statement = statement.outerjoin(addresses).select().apply_labels()

    # create query, indicating "ulist" is an alias for the main table, "addresses" property should
    # be eager loaded
    query = session.query(User).options(contains_alias('ulist'), contains_eager('addresses'))

    # results
    r = query.from_statement(statement)

.. _largecollections:

Working with Large Collections
-------------------------------

The default behavior of :func:`~sqlalchemy.orm.relationship` is to fully load the collection of items in, as according to the loading strategy of the relationship.  Additionally, the Session by default only knows how to delete objects which are actually present within the session.  When a parent instance is marked for deletion and flushed, the Session loads its full list of child items in so that they may either be deleted as well, or have their foreign key value set to null; this is to avoid constraint violations.  For large collections of child items, there are several strategies to bypass full loading of child items both at load time as well as deletion time.

Dynamic Relationship Loaders
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The most useful by far is the :func:`~sqlalchemy.orm.dynamic_loader` relationship.  This is a variant of :func:`~sqlalchemy.orm.relationship` which returns a :class:`~sqlalchemy.orm.query.Query` object in place of a collection when accessed.  :func:`~sqlalchemy.orm.query.Query.filter` criterion may be applied as well as limits and offsets, either explicitly or via array slices:

.. sourcecode:: python+sql

    mapper(User, users_table, properties={
        'posts': dynamic_loader(Post)
    })

    jack = session.query(User).get(id)

    # filter Jack's blog posts
    posts = jack.posts.filter(Post.headline=='this is a post')

    # apply array slices
    posts = jack.posts[5:20]

The dynamic relationship supports limited write operations, via the ``append()`` and ``remove()`` methods.  Since the read side of the dynamic relationship always queries the database, changes to the underlying collection will not be visible until the data has been flushed:

.. sourcecode:: python+sql

    oldpost = jack.posts.filter(Post.headline=='old post').one()
    jack.posts.remove(oldpost)

    jack.posts.append(Post('new post'))

To place a dynamic relationship on a backref, use ``lazy='dynamic'``:

.. sourcecode:: python+sql

    mapper(Post, posts_table, properties={
        'user': relationship(User, backref=backref('posts', lazy='dynamic'))
    })

Note that eager/lazy loading options cannot be used in conjunction dynamic relationships at this time.

Setting Noload
~~~~~~~~~~~~~~~

The opposite of the dynamic relationship is simply "noload", specified using ``lazy='noload'``:

.. sourcecode:: python+sql

    mapper(MyClass, table, properties={
        'children': relationship(MyOtherClass, lazy='noload')
    })

Above, the ``children`` collection is fully writeable, and changes to it will be persisted to the database as well as locally available for reading at the time they are added.  However when instances of  ``MyClass`` are freshly loaded from the database, the ``children`` collection stays empty.

Using Passive Deletes
~~~~~~~~~~~~~~~~~~~~~~

Use ``passive_deletes=True`` to disable child object loading on a DELETE operation, in conjunction with "ON DELETE (CASCADE|SET NULL)" on your database to automatically cascade deletes to child objects.   Note that "ON DELETE" is not supported on SQLite, and requires ``InnoDB`` tables when using MySQL:

.. sourcecode:: python+sql

        mytable = Table('mytable', meta,
            Column('id', Integer, primary_key=True),
            )

        myothertable = Table('myothertable', meta,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer),
            ForeignKeyConstraint(['parent_id'], ['mytable.id'], ondelete="CASCADE"),
            )

        mapper(MyOtherClass, myothertable)

        mapper(MyClass, mytable, properties={
            'children': relationship(MyOtherClass, cascade="all, delete-orphan", passive_deletes=True)
        })

When ``passive_deletes`` is applied, the ``children`` relationship will not be loaded into memory when an instance of ``MyClass`` is marked for deletion.  The ``cascade="all, delete-orphan"`` *will* take effect for instances of ``MyOtherClass`` which are currently present in the session; however for instances of ``MyOtherClass`` which are not loaded, SQLAlchemy assumes that "ON DELETE CASCADE" rules will ensure that those rows are deleted by the database and that no foreign key violation will occur.

Mutable Primary Keys / Update Cascades
---------------------------------------

When the primary key of an entity changes, related items which reference the primary key must also be updated as well.  For databases which enforce referential integrity, it's required to use the database's ON UPDATE CASCADE functionality in order to propagate primary key changes.  For those which don't, the ``passive_updates`` flag can be set to ``False`` which instructs SQLAlchemy to issue UPDATE statements individually.  The ``passive_updates`` flag can also be ``False`` in conjunction with ON UPDATE CASCADE functionality, although in that case it issues UPDATE statements unnecessarily.

A typical mutable primary key setup might look like:

.. sourcecode:: python+sql

    users = Table('users', metadata,
        Column('username', String(50), primary_key=True),
        Column('fullname', String(100)))

    addresses = Table('addresses', metadata,
        Column('email', String(50), primary_key=True),
        Column('username', String(50), ForeignKey('users.username', onupdate="cascade")))

    class User(object):
        pass
    class Address(object):
        pass

    mapper(User, users, properties={
        'addresses': relationship(Address, passive_updates=False)
    })
    mapper(Address, addresses)

passive_updates is set to ``True`` by default.  Foreign key references to non-primary key columns are supported as well.


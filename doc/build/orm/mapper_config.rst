.. module:: sqlalchemy.orm

.. _mapper_config_toplevel:

====================
Mapper Configuration
====================

This section describes a variety of configurational patterns that are usable
with mappers. It assumes you've worked through :ref:`ormtutorial_toplevel` and
know how to construct and use rudimentary mappers and relationships.

Note that all patterns here apply both to the usage of explicit
:func:`~.orm.mapper` and :class:`.Table` objects as well as when using the
:mod:`sqlalchemy.ext.declarative` extension. Any example in this section which
takes a form such as::

    mapper(User, users_table, primary_key=[users_table.c.id])

Would translate into declarative as::

    class User(Base):
        __table__ = users_table
        __mapper_args__ = {
            'primary_key':[users_table.c.id]
        }

Or if using ``__tablename__``, :class:`.Column` objects are declared inline
with the class definition. These are usable as is within ``__mapper_args__``::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer)

        __mapper_args__ = {
            'primary_key':[id]
        }


Customizing Column Properties
==============================

The default behavior of :func:`~.orm.mapper` is to assemble all the columns in
the mapped :class:`.Table` into mapped object attributes. This behavior can be
modified in several ways, as well as enhanced by SQL expressions.

Mapping a Subset of Table Columns
---------------------------------

To reference a subset of columns referenced by a table as mapped attributes,
use the ``include_properties`` or ``exclude_properties`` arguments. For
example::

    mapper(User, users_table, include_properties=['user_id', 'user_name'])

...will map the ``User`` class to the ``users_table`` table, only including
the "user_id" and "user_name" columns - the rest are not refererenced.
Similarly::

    mapper(Address, addresses_table, 
                exclude_properties=['street', 'city', 'state', 'zip'])

...will map the ``Address`` class to the ``addresses_table`` table, including
all columns present except "street", "city", "state", and "zip".

When this mapping is used, the columns that are not included will not be
referenced in any SELECT statements emitted by :class:`.Query`, nor will there
be any mapped attribute on the mapped class which represents the column;
assigning an attribute of that name will have no effect beyond that of
a normal Python attribute assignment.

In some cases, multiple columns may have the same name, such as when
mapping to a join of two or more tables that share some column name.  To 
exclude or include individual columns, :class:`.Column` objects
may also be placed within the "include_properties" and "exclude_properties"
collections (new feature as of 0.6.4)::

    mapper(UserAddress, users_table.join(addresses_table),
                exclude_properties=[addresses_table.c.id],
                primary_key=[users_table.c.id]
            )

It should be noted that insert and update defaults configured on individal
:class:`.Column` objects, such as those configured by the "default",
"update", "server_default" and "server_onupdate" arguments, will continue
to function normally even if those :class:`.Column` objects are not mapped.
This functionality is part of the SQL expression and execution system and
occurs below the level of the ORM.


Attribute Names for Mapped Columns
----------------------------------

To change the name of the attribute mapped to a particular column, place the
:class:`~sqlalchemy.schema.Column` object in the ``properties`` dictionary
with the desired key::

    mapper(User, users_table, properties={
       'id': users_table.c.user_id,
       'name': users_table.c.user_name,
    })

When using :mod:`~sqlalchemy.ext.declarative`, the above configuration is more
succinct - place the full column name in the :class:`.Column` definition,
using the desired attribute name in the class definition::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column('user_id', Integer, primary_key=True)
        name = Column('user_name', String(50))

To change the names of all attributes using a prefix, use the
``column_prefix`` option.  This is useful for some schemes that would like
to declare alternate attributes::

    mapper(User, users_table, column_prefix='_')

The above will place attribute names such as ``_user_id``, ``_user_name``,
``_password`` etc. on the mapped ``User`` class.


Mapping Multiple Columns to a Single Attribute
----------------------------------------------

To place multiple columns which are known to be "synonymous" based on foreign
key relationship or join condition into the same mapped attribute, put them
together using a list, as below where we map to a :func:`~.expression.join`::

    from sqlalchemy.sql import join

    # join users and addresses
    usersaddresses = join(users_table, addresses_table, \
        users_table.c.user_id == addresses_table.c.user_id)

    # user_id columns are equated under the 'user_id' attribute
    mapper(User, usersaddresses, properties={
        'id':[users_table.c.user_id, addresses_table.c.user_id],
    })

For further examples on this particular use case, see :ref:`maptojoin`.

Using column_property for column level options
-----------------------------------------------

The establishment of a :class:`.Column` on a :func:`.mapper` can be further
customized using the :func:`.column_property` function, as specified
to the ``properties`` dictionary.   This function is 
usually invoked implicitly for each mapped :class:`.Column`.  Explicit usage
looks like::

    from sqlalchemy.orm import mapper, column_property

    mapper(User, users, properties={
        'name':column_property(users.c.name, active_history=True)
    })

or with declarative::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = column_property(Column(String(50)), active_history=True)

Further examples of :func:`.column_property` are at :ref:`mapper_sql_expressions`.

.. autofunction:: column_property

.. _deferred:

Deferred Column Loading
========================

This feature allows particular columns of a table to not be loaded by default,
instead being loaded later on when first referenced. It is essentially
"column-level lazy loading". This feature is useful when one wants to avoid
loading a large text or binary field into memory when it's not needed.
Individual columns can be lazy loaded by themselves or placed into groups that
lazy-load together::

    book_excerpts = Table('books', metadata,
        Column('book_id', Integer, primary_key=True),
        Column('title', String(200), nullable=False),
        Column('summary', String(2000)),
        Column('excerpt', Text),
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

With declarative, :class:`.Column` objects can be declared directly inside of :func:`deferred`::

    class Book(Base):
        __tablename__ = 'books'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = Column(String(2000))
        excerpt = deferred(Column(Text))
        photo = deferred(Column(Binary))

Deferred columns can be associted with a "group" name, so that they load
together when any of them are first accessed::

    book_excerpts = Table('books', metadata,
      Column('book_id', Integer, primary_key=True),
      Column('title', String(200), nullable=False),
      Column('summary', String(2000)),
      Column('excerpt', Text),
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

You can defer or undefer columns at the :class:`~sqlalchemy.orm.query.Query`
level using the :func:`.defer` and :func:`.undefer` query options::

    query = session.query(Book)
    query.options(defer('summary')).all()
    query.options(undefer('excerpt')).all()

And an entire "deferred group", i.e. which uses the ``group`` keyword argument
to :func:`~sqlalchemy.orm.deferred()`, can be undeferred using
:func:`.undefer_group()`, sending in the group name::

    query = session.query(Book)
    query.options(undefer_group('photos')).all()

Column Deferral API
-------------------

.. autofunction:: deferred

.. autofunction:: defer

.. autofunction:: undefer

.. autofunction:: undefer_group

.. _mapper_sql_expressions:

SQL Expressions as Mapped Attributes
=====================================

Any SQL expression that relates to the primary mapped selectable can be mapped as a 
read-only attribute which will be bundled into the SELECT emitted
for the target mapper when rows are loaded.   This effect is achieved
using the :func:`.column_property` function.  Any
scalar-returning
:class:`.ClauseElement` may be
used.  Unlike older versions of SQLAlchemy, there is no :func:`~.sql.expression.label` requirement::

    from sqlalchemy.orm import column_property

    mapper(User, users_table, properties={
        'fullname': column_property(
            users_table.c.firstname + " " + users_table.c.lastname
        )
    })

Correlated subqueries may be used as well::

    from sqlalchemy.orm import column_property
    from sqlalchemy import select, func

    mapper(User, users_table, properties={
        'address_count': column_property(
                select([func.count(addresses_table.c.address_id)]).\
                where(addresses_table.c.user_id==users_table.c.user_id)
            )
    })

The declarative form of the above is described in :ref:`declarative_sql_expressions`.

Note that :func:`.column_property` is used to provide the effect of a SQL
expression that is actively rendered into the SELECT generated for a
particular mapped class.  Alternatively, for the typical attribute that
represents a composed value, its usually simpler to define it as a Python
property which is evaluated as it is invoked on instances after they've been
loaded::

    class User(object):
        @property
        def fullname(self):
            return self.firstname + " " + self.lastname

To invoke a SQL statement from an instance that's already been loaded, the
session associated with the instance can be acquired using
:func:`~.session.object_session` which will provide the appropriate
transactional context from which to emit a statement::

    from sqlalchemy.orm import object_session
    from sqlalchemy import select, func

    class User(object):
        @property
        def address_count(self):
            return object_session(self).\
                scalar(
                    select([func.count(addresses_table.c.address_id)]).\
                        where(addresses_table.c.user_id==self.user_id)
                )

See also :ref:`synonyms` for details on building expression-enabled
descriptors on mapped classes, which are invoked independently of the
mapping.

Changing Attribute Behavior
============================

Simple Validators
-----------------

A quick way to add a "validation" routine to an attribute is to use the
:func:`~sqlalchemy.orm.validates` decorator. An attribute validator can raise
an exception, halting the process of mutating the attribute's value, or can
change the given value into something different. Validators, like all
attribute extensions, are only called by normal userland code; they are not
issued when the ORM is populating the object.

.. sourcecode:: python+sql

    from sqlalchemy.orm import validates

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

.. autofunction:: validates

.. _synonyms:

Using Descriptors
-----------------

A more comprehensive way to produce modified behavior for an attribute is to
use descriptors. These are commonly used in Python using the ``property()``
function. The standard SQLAlchemy technique for descriptors is to create a
plain descriptor, and to have it read/write from a mapped attribute with a
different name. Below we illustrate this using Python 2.6-style properties::

    from sqlalchemy.orm import mapper

    class EmailAddress(object):

        @property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

    mapper(EmailAddress, addresses_table, properties={
        '_email': addresses_table.c.email
    })

The approach above will work, but there's more we can add. While our
``EmailAddress`` object will shuttle the value through the ``email``
descriptor and into the ``_email`` mapped attribute, the class level
``EmailAddress.email`` attribute does not have the usual expression semantics
usable with :class:`.Query`. To provide these, we instead use the
:mod:`~sqlalchemy.ext.hybrid` extension as follows::

    from sqlalchemy.ext.hybrid import hybrid_property

    class EmailAddress(object):

        @hybrid_property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

The ``email`` attribute now provides a SQL expression when used at the class level:

.. sourcecode:: python+sql

    from sqlalchemy.orm import Session
    session = Session()

    {sql}address = session.query(EmailAddress).filter(EmailAddress.email == 'address@example.com').one()
    SELECT addresses.email AS addresses_email, addresses.id AS addresses_id 
    FROM addresses 
    WHERE addresses.email = ?
    ('address@example.com',)
    {stop}

    address.email = 'otheraddress@example.com'
    {sql}session.commit()
    UPDATE addresses SET email=? WHERE addresses.id = ?
    ('otheraddress@example.com', 1)
    COMMIT
    {stop}

The :class:`~.hybrid_property` also allows us to change the behavior of the attribute, including 
defining separate behaviors when the attribute is accessed at the instance level versus at 
the class/expression level, using the :meth:`.hybrid_property.expression` modifier.  Such
as, if we wanted to add a host name automatically, we might define two sets of string manipulation
logic::

    class EmailAddress(object):
        @hybrid_property
        def email(self):
            """Return the value of _email up until the last twelve 
            characters."""

            return self._email[:-12]

        @email.setter
        def email(self, email):
            """Set the value of _email, tacking on the twelve character 
            value @example.com."""

            self._email = email + "@example.com"

        @email.expression
        def email(cls):
            """Produce a SQL expression that represents the value 
            of the _email column, minus the last twelve characters."""

            return func.substr(cls._email, 0, func.length(cls._email) - 12)

Above, accessing the ``email`` property of an instance of ``EmailAddress`` will return the value of 
the ``_email`` attribute, removing
or adding the hostname ``@example.com`` from the value.   When we query against the ``email`` attribute,
a SQL function is rendered which produces the same effect:

.. sourcecode:: python+sql

    {sql}address = session.query(EmailAddress).filter(EmailAddress.email == 'address').one()
    SELECT addresses.email AS addresses_email, addresses.id AS addresses_id 
    FROM addresses 
    WHERE substr(addresses.email, ?, length(addresses.email) - ?) = ?
    (0, 12, 'address')
    {stop}



Read more about Hybrids at :ref:`hybrids_toplevel`.

Synonyms
--------

Synonyms are a mapper-level construct that applies expression behavior to a descriptor
based attribute.  The functionality of synonym is superceded as of 0.7 by hybrid attributes.

.. autofunction:: synonym

.. _custom_comparators:

Custom Comparators
------------------

The expressions returned by comparison operations, such as
``User.name=='ed'``, can be customized, by implementing an object that
explicitly defines each comparison method needed. 

This is a relatively rare use case which generally applies only to 
highly customized types.  Usually, custom SQL behaviors can be 
associated with a mapped class by composing together the classes'
existing mapped attributes with other expression components, 
using either mapped SQL expressions as those described in
:ref:`mapper_sql_expressions`, or so-called "hybrid" attributes
as described at :ref:`hybrids_toplevel`.  Those approaches should be 
considered first before resorting to custom comparison objects.

Each of :func:`.column_property`, :func:`~.composite`, :func:`.relationship`,
and :func:`.comparable_property` accept an argument called
``comparator_factory``.   A subclass of :class:`.PropComparator` can be provided
for this argument, which can then reimplement basic Python comparison methods
such as ``__eq__()``, ``__ne__()``, ``__lt__()``, and so on. 

It's best to subclass the :class:`.PropComparator` subclass provided by
each type of property.  For example, to allow a column-mapped attribute to
do case-insensitive comparison::

    from sqlalchemy.orm.properties import ColumnProperty
    from sqlalchemy.sql import func

    class MyComparator(ColumnProperty.Comparator):
        def __eq__(self, other):
            return func.lower(self.__clause_element__()) == func.lower(other)

    mapper(EmailAddress, addresses_table, properties={
        'email':column_property(addresses_table.c.email,
                                comparator_factory=MyComparator)
    })

Above, comparisons on the ``email`` column are wrapped in the SQL lower()
function to produce case-insensitive matching::

    >>> str(EmailAddress.email == 'SomeAddress@foo.com')
    lower(addresses.email) = lower(:lower_1)

When building a :class:`.PropComparator`, the ``__clause_element__()`` method
should be used in order to acquire the underlying mapped column.  This will 
return a column that is appropriately wrapped in any kind of subquery
or aliasing that has been applied in the context of the generated SQL statement.

.. autofunction:: comparable_property

.. _mapper_composite:

Composite Column Types
=======================

Sets of columns can be associated with a single user-defined datatype. The ORM
provides a single attribute which represents the group of columns using the
class you provide.

.. note::
    As of SQLAlchemy 0.7, composites have been simplified such that 
    they no longer "conceal" the underlying column based attributes.  Additionally,
    in-place mutation is no longer automatic; see the section below on
    enabling mutability to support tracking of in-place changes.

A simple example represents pairs of columns as a "Point" object.
Starting with a table that represents two points as x1/y1 and x2/y2::

    from sqlalchemy import Table, Column

    vertices = Table('vertices', metadata,
        Column('id', Integer, primary_key=True),
        Column('x1', Integer),
        Column('y1', Integer),
        Column('x2', Integer),
        Column('y2', Integer),
        )

We create a new class, ``Point``, that will represent each x/y as a 
pair::

    class Point(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def __composite_values__(self):
            return self.x, self.y

        def __eq__(self, other):
            return isinstance(other, Point) and \
                other.x == self.x and \
                other.y == self.y

        def __ne__(self, other):
            return not self.__eq__(other)

The requirements for the custom datatype class are that it have a
constructor which accepts positional arguments corresponding to its column
format, and also provides a method ``__composite_values__()`` which
returns the state of the object as a list or tuple, in order of its
column-based attributes. It also should supply adequate ``__eq__()`` and
``__ne__()`` methods which test the equality of two instances.

The :func:`.composite` function is then used in the mapping::

    from sqlalchemy.orm import composite

    class Vertex(object):
        pass

    mapper(Vertex, vertices, properties={
        'start': composite(Point, vertices.c.x1, vertices.c.y1),
        'end': composite(Point, vertices.c.x2, vertices.c.y2)
    })

When using :mod:`sqlalchemy.ext.declarative`, the individual 
:class:`.Column` objects may optionally be bundled into the 
:func:`.composite` call, ensuring that they are named::

    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()

    class Vertex(Base):
        __tablename__ = 'vertices'
        id = Column(Integer, primary_key=True)
        start = composite(Point, Column('x1', Integer), Column('y1', Integer))
        end = composite(Point, Column('x2', Integer), Column('y2', Integer))

Using either configurational approach, we can now use the ``Vertex`` instances
as well as querying as though the ``start`` and ``end`` attributes are regular
scalar attributes::

    session = Session()
    v = Vertex(Point(3, 4), Point(5, 6))
    session.add(v)

    v2 = session.query(Vertex).filter(Vertex.start == Point(3, 4))

.. autofunction:: composite

Tracking In-Place Mutations on Composites
-----------------------------------------

As of SQLAlchemy 0.7, in-place changes to an existing composite value are 
not tracked automatically.  Instead, the composite class needs to provide
events to its parent object explicitly.   This task is largely automated 
via the usage of the :class:`.MutableComposite` mixin, which uses events
to associate each user-defined composite object with all parent associations.
Please see the example in :ref:`mutable_composites`.

Redefining Comparison Operations for Composites
-----------------------------------------------

The "equals" comparison operation by default produces an AND of all
corresponding columns equated to one another. This can be changed using
the ``comparator_factory``, described in :ref:`custom_comparators`.
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

    maper(Vertex, vertices, properties={
        'start': composite(Point, vertices.c.x1, vertices.c.y1,
                                    comparator_factory=PointComparator),
        'end': composite(Point, vertices.c.x2, vertices.c.y2,
                                    comparator_factory=PointComparator)
    })

.. _maptojoin:

Mapping a Class against Multiple Tables
========================================

Mappers can be constructed against arbitrary relational units (called
``Selectables``) as well as plain ``Tables``. For example, The ``join``
keyword from the SQL package creates a neat selectable unit comprised of
multiple tables, complete with its own composite primary key, which can be
passed in to a mapper as the table.

.. sourcecode:: python+sql

    from sqlalchemy.orm import mapper
    from sqlalchemy.sql import join

    class AddressUser(object):
        pass

    # define a Join
    j = join(users_table, addresses_table)

    # map to it - the identity of an AddressUser object will be
    # based on (user_id, address_id) since those are the primary keys involved
    mapper(AddressUser, j, properties={
        'user_id': [users_table.c.user_id, addresses_table.c.user_id]
    })

Note that the list of columns is equivalent to the usage of :func:`.column_property`
with multiple columns::

    from sqlalchemy.orm import mapper, column_property

    mapper(AddressUser, j, properties={
        'user_id': column_property(users_table.c.user_id, addresses_table.c.user_id)
    })

The usage of :func:`.column_property` is required when using declarative to map 
to multiple columns, since the declarative class parser won't recognize a plain 
list of columns::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class AddressUser(Base):
        __table__ = j

        user_id = column_property(users_table.c.user_id, addresses_table.c.user_id)

A second example::

    from sqlalchemy.sql import join

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

In both examples above, "composite" columns were added as properties to the
mappers; these are aggregations of multiple columns into one mapper property,
which instructs the mapper to keep both of those columns set at the same
value.


Mapping a Class against Arbitrary Selects
=========================================

Similar to mapping against a join, a plain select() object can be used with a mapper as well.  Below, an example select which contains two aggregate functions and a group_by is mapped to a class:

.. sourcecode:: python+sql

    from sqlalchemy.sql import select

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
==============================

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
=============================================

The non_primary mapper defines alternate mappers for the purposes of loading objects.  What if we want the same class to be *persisted* differently, such as to different tables ?   SQLAlchemy
refers to this as the "entity name" pattern, and in Python one can use a recipe which creates
anonymous subclasses which are distinctly mapped.  See the recipe at `Entity Name <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.

Constructors and Object Initialization
=======================================

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

.. autofunction:: reconstructor

Class Mapping API
=================

.. autofunction:: mapper

.. autofunction:: object_mapper

.. autofunction:: class_mapper

.. autofunction:: compile_mappers

.. autofunction:: configure_mappers

.. autofunction:: clear_mappers

.. autofunction:: sqlalchemy.orm.util.identity_key

.. autofunction:: sqlalchemy.orm.util.polymorphic_union

.. autoclass:: sqlalchemy.orm.mapper.Mapper
   :members:


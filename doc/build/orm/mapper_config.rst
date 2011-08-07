.. module:: sqlalchemy.orm

.. _mapper_config_toplevel:

====================
Mapper Configuration
====================

This section describes a variety of configurational patterns that are usable
with mappers. It assumes you've worked through :ref:`ormtutorial_toplevel` and
know how to construct and use rudimentary mappers and relationships.

.. _classical_mapping:

Classical Mappings
==================

Recall from :ref:`ormtutorial_toplevel` that we normally use the :ref:`declarative_toplevel`
system to define mappings.   The "Classical Mapping" refers to the process
of defining :class:`.Table` metadata and mapped class via :func:`.mapper` separately.
While direct usage of :func:`.mapper` is not prominent in modern SQLAlchemy, 
the function can be used
to create alternative mappings for an already mapped class, offers greater configurational
flexibility in certain highly circular configurations, and is also at the base of
alternative configurational systems not based upon Declarative.   Many SQLAlchemy
applications in fact use classical mappings directly for configuration.

The ``User`` example in the tutorial, using classical mapping, defines the 
:class:`.Table`, class, and :func:`.mapper` of the class separately.  Below we illustrate
the full ``User``/``Address`` example using this style::

    from sqlalchemy import Table, MetaData, Column, ForeignKey, Integer, String
    from sqlalchemy.orm import mapper, relationship

    metadata = MetaData()

    user = Table('user', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(50)),
                Column('fullname', String(50)),
                Column('password', String(12))
            )

    address = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String(50))
                )

    class User(object):
        def __init__(self, name, fullname, password):
            self.name = name
            self.fullname = fullname
            self.password = password

    class Address(object):
        def __init__(self, email_address):
            self.email_address = email_address


    mapper(User, user, properties={
        'addresses':relationship(Address, order_by=address.c.id, backref="user")
    })
    mapper(Address, address)

When the above is complete we now have a :class:`.Table`/:func:`.mapper` setup the same as that
set up using Declarative in the tutorial.   Note that the mappings do not have the
benefit of the instrumented ``User`` and ``Address`` classes available, nor is the "string" argument
system of :func:`.relationship` available, as this is a feature of Declarative.  The ``order_by`` 
argument of the ``User.addresses`` relationship is defined in terms of the actual ``address``
table instead of the ``Address`` class.

It's also worth noting that the "Classical" and "Declarative" mapping systems are not in 
any way exclusive of each other.    The two can be mixed freely - below we can
define a new class ``Order`` using a declarative base, which links back to ``User``- 
no problem, except that we can't specify ``User`` as a string since it's not available
in the "base" registry::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Order(Base):
        __tablename__ = 'order'

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey('user.id'))
        order_number = Column(String(50))
        user = relationship(User, backref="orders")

This reference document uses a mix of Declarative and classical mappings for
examples. However, all patterns here apply both to the usage of explicit
:func:`~.orm.mapper` and :class:`.Table` objects as well as when using
Declarative, where options that are specific to the :func:`.mapper` function
can be specified with Declarative via the ``__mapper__`` attribute. Any
example in this section which takes a form such as::

    mapper(User, user_table, primary_key=[user_table.c.id])

Would translate into declarative as::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {
            'primary_key':[user_table.c.id]
        }

:class:`.Column` objects which are declared inline can also
be used directly in ``__mapper_args__``::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer)

        __mapper_args__ = {
            'primary_key':[id]
        }


Customizing Column Properties
==============================

The default behavior of :func:`~.orm.mapper` is to assemble all the columns in
the mapped :class:`.Table` into mapped object attributes. This behavior can be
modified in several ways, as well as enhanced by SQL expressions.

Naming Columns Distinctly from Attribute Names
----------------------------------------------

A mapping by default shares the same name for a
:class:`.Column` as that of the mapped attribute.
The name assigned to the :class:`.Column` can be different,
as we illustrate here in a Declarative mapping::

    class User(Base):
        __tablename__ = 'user'
        id = Column('user_id', Integer, primary_key=True)
        name = Column('user_name', String(50))

Where above ``User.id`` resolves to a column named ``user_id``
and ``User.name`` resolves to a column named ``user_name``.

In a classical mapping, the :class:`.Column` objects
can be placed directly in the ``properties`` dictionary
using an alternate key::

    mapper(User, user_table, properties={
       'id': user_table.c.user_id,
       'name': user_table.c.user_name,
    })

When mapping to an already constructed :class:`.Table`,
a prefix can be specified using the ``column_prefix``
option, which will cause the automated mapping of
each :class:`.Column` to name the attribute starting
with the given prefix, prepended to the actual :class:`.Column`
name::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {'column_prefix':'_'}

The above will place attribute names such as ``_user_id``, ``_user_name``,
``_password`` etc. on the mapped ``User`` class.

The classical version of the above::

    mapper(User, user_table, column_prefix='_')

Mapping Multiple Columns to a Single Attribute
----------------------------------------------

To place multiple columns which are known to be "synonymous" based on foreign
key relationship or join condition into the same mapped attribute, 
they can be mapped as a list.  Below we map to a :func:`~.expression.join`::

    from sqlalchemy import join, Table, Column, String, Integer, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    user_table = Table('user', Base.metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(50)),
                Column('fullname', String(50)),
                Column('password', String(12))
            )

    address_table = Table('address', Base.metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String(50))
                )

    # "user JOIN address ON user.id=address.user_id"
    useraddress = join(user_table, address_table, \
        user_table.c.id == address_table.c.user_id)

    class User(Base):
        __table__ = useraddress

        # assign "user.id", "address.user_id" to the
        # "id" attribute
        id = [user_table.c.id, address_table.c.user_id]

        # assign "address.id" to the "address_id"
        # attribute, to avoid name conflicts
        address_id = address_table.c.id

In the above mapping, the value assigned to ``user.id`` will
also be persisted to the ``address.user_id`` column during a
flush.  The two columns are also not independently queryable
from the perspective of the mapped class (they of course are 
still available from their original tables).

Classical version::

    mapper(User, useraddress, properties={
        'id':[user_table.c.id, address_table.c.user_id],
        'address_id':address_table.c.id
    })

For further examples on this particular use case, see :ref:`maptojoin`.

Using column_property for column level options
-----------------------------------------------

The mapping of a :class:`.Column` with a particular :func:`.mapper` can be 
customized using the :func:`.orm.column_property` function.  This function
explicitly creates the :class:`.ColumnProperty` object which handles the job of 
mapping a :class:`.Column`, instead of relying upon the :func:`.mapper`
function to create it automatically.   Used with Declarative,
the :class:`.Column` can be embedded directly into the
function::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = column_property(Column(String(50)), active_history=True)

Or with a classical mapping, in the ``properties`` dictionary::

    from sqlalchemy.orm import column_property

    mapper(User, user, properties={
        'name':column_property(user.c.name, active_history=True)
    })

Further examples of :func:`.orm.column_property` are at :ref:`mapper_sql_expressions`.

.. autofunction:: column_property

Mapping a Subset of Table Columns
---------------------------------

To reference a subset of columns referenced by a table as mapped attributes,
use the ``include_properties`` or ``exclude_properties`` arguments. For
example::

    mapper(User, user_table, include_properties=['user_id', 'user_name'])

...will map the ``User`` class to the ``user_table`` table, only including
the "user_id" and "user_name" columns - the rest are not refererenced.
Similarly::

    mapper(Address, address_table, 
                exclude_properties=['street', 'city', 'state', 'zip'])

...will map the ``Address`` class to the ``address_table`` table, including
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

    mapper(UserAddress, user_table.join(addresse_table),
                exclude_properties=[address_table.c.id],
                primary_key=[user_table.c.id]
            )

It should be noted that insert and update defaults configured on individal
:class:`.Column` objects, such as those configured by the "default",
"update", "server_default" and "server_onupdate" arguments, will continue
to function normally even if those :class:`.Column` objects are not mapped.
This functionality is part of the SQL expression and execution system and
occurs below the level of the ORM.

.. _deferred:

Deferred Column Loading
========================

This feature allows particular columns of a table be loaded only
upon direct access, instead of when the entity is queried using 
:class:`.Query`.  This feature is useful when one wants to avoid
loading a large text or binary field into memory when it's not needed.
Individual columns can be lazy loaded by themselves or placed into groups that
lazy-load together, using the :func:`.orm.deferred` function to 
mark them as "deferred". In the example below, we define a mapping that will load each of
``.excerpt`` and ``.photo`` in separate, individual-row SELECT statements when each
attribute is first referenced on the individual object instance::

    from sqlalchemy.orm import deferred
    from sqlalchemy import Integer, String, Text, Binary, Column

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = Column(String(2000))
        excerpt = deferred(Column(Text))
        photo = deferred(Column(Binary))

Classical mappings as always place the usage of :func:`.orm.deferred` in the
``properties`` dictionary against the table-bound :class:`.Column`::

    mapper(Book, book_table, properties={
        'photo':deferred(book_table.c.photo)
    })

Deferred columns can be associated with a "group" name, so that they load
together when any of them are first accessed.  The example below defines a
mapping with a ``photos`` deferred group.  When one ``.photo`` is accessed, all three
photos will be loaded in one SELECT statement. The ``.excerpt`` will be loaded
separately when it is accessed::

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = Column(String(2000))
        excerpt = deferred(Column(Text))
        photo1 = deferred(Column(Binary), group='photos')
        photo2 = deferred(Column(Binary), group='photos')
        photo3 = deferred(Column(Binary), group='photos')

You can defer or undefer columns at the :class:`~sqlalchemy.orm.query.Query`
level using the :func:`.orm.defer` and :func:`.orm.undefer` query options::

    from sqlalchemy.orm import defer, undefer

    query = session.query(Book)
    query.options(defer('summary')).all()
    query.options(undefer('excerpt')).all()

And an entire "deferred group", i.e. which uses the ``group`` keyword argument
to :func:`.orm.deferred`, can be undeferred using
:func:`.orm.undefer_group`, sending in the group name::

    from sqlalchemy.orm import undefer_group

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

Any SQL expression that relates to the primary mapped selectable can be mapped
as a read-only attribute which will be bundled into the SELECT emitted for the
target mapper when rows are loaded. This effect is achieved using the
:func:`.orm.column_property` function. Any scalar-returning
:class:`.ClauseElement` may be used::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))
        fullname = column_property(firstname + " " + lastname)

Correlated subqueries may be used as well.  Below we use the :func:`.select`
construct to create a SELECT that links together the count of ``Address``
objects available for a particular ``User``::

    from sqlalchemy.orm import column_property
    from sqlalchemy import select, func
    from sqlalchemy import Column, Integer, String, ForeignKey

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'))

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        address_count = column_property(
            select([func.count(Address.id)]).\
                where(Address.user_id==id)
        )

If import issues prevent the :func:`.column_property` from being defined
inline with the class, it can be assigned to the class after both
are configured.   In Declarative this has the effect of calling :meth:`.Mapper.add_property`
to add an additional property after the fact::

    User.address_count = column_property(
            select([func.count(Address.id)]).\
                where(Address.user_id==User.id)
        ) 

For many-to-many relationships, use :func:`.and_` to join the fields of the
association table to both tables in a relation, illustrated
here with a classical mapping::

    from sqlalchemy import and_

    mapper(Author, authors, properties={
        'book_count': column_property(
                            select([func.count(books.c.id)], 
                                and_(
                                    book_authors.c.author_id==authors.c.id,
                                    book_authors.c.book_id==books.c.id
                                )))
        })

Alternatives to column_property()
---------------------------------

:func:`.orm.column_property` is used to provide the effect of a SQL expression
that is actively rendered into the SELECT generated for a particular mapped
class. For the typical attribute that represents a composed value, it's often
simpler and more efficient to just define it as a Python property, which is
evaluated as it is invoked on instances after they've been loaded::

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @property
        def fullname(self):
            return self.firstname + " " + self.lastname

To emit SQL queries from within a @property, the
:class:`.Session` associated with the instance can be acquired using
:func:`~.session.object_session`, which will provide the appropriate
transactional context from which to emit a statement::

    from sqlalchemy.orm import object_session
    from sqlalchemy import select, func

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @property
        def address_count(self):
            return object_session(self).\
                scalar(
                    select([func.count(Address.id)]).\
                        where(Address.user_id==self.id)
                )

For more information on using descriptors, including how they can 
be smoothly integrated into SQL expressions, see :ref:`synonyms`.

Changing Attribute Behavior
============================

.. _simple_validators:

Simple Validators
-----------------

A quick way to add a "validation" routine to an attribute is to use the
:func:`~sqlalchemy.orm.validates` decorator. An attribute validator can raise
an exception, halting the process of mutating the attribute's value, or can
change the given value into something different. Validators, like all
attribute extensions, are only called by normal userland code; they are not
issued when the ORM is populating the object::

    from sqlalchemy.orm import validates

    class EmailAddress(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        email = Column(String)

        @validates('email')
        def validate_email(self, key, address):
            assert '@' in address
            return address

Validators also receive collection events, when items are added to a
collection::

    from sqlalchemy.orm import validates

    class User(Base):
        # ...

        addresses = relationship("Address")

        @validates('addresses')
        def validate_address(self, key, address):
            assert '@' in address.email
            return address

Note that the :func:`~.validates` decorator is a convenience function built on 
top of attribute events.   An application that requires more control over
configuration of attribute change behavior can make use of this system,
described at :class:`~.AttributeEvents`.

.. autofunction:: validates

.. _synonyms:

Using Descriptors
-----------------

A more comprehensive way to produce modified behavior for an attribute is to
use descriptors. These are commonly used in Python using the ``property()``
function. The standard SQLAlchemy technique for descriptors is to create a
plain descriptor, and to have it read/write from a mapped attribute with a
different name. Below we illustrate this using Python 2.6-style properties::

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        # name the attribute with an underscore,
        # different from the column name
        _email = Column("email", String)

        # then create an ".email" attribute
        # to get/set "._email"
        @property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

The approach above will work, but there's more we can add. While our
``EmailAddress`` object will shuttle the value through the ``email``
descriptor and into the ``_email`` mapped attribute, the class level
``EmailAddress.email`` attribute does not have the usual expression semantics
usable with :class:`.Query`. To provide these, we instead use the
:mod:`~sqlalchemy.ext.hybrid` extension as follows::

    from sqlalchemy.ext.hybrid import hybrid_property

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        _email = Column("email", String)

        @hybrid_property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

The ``.email`` attribute, in addition to providing getter/setter behavior when we have an
instance of ``EmailAddress``, also provides a SQL expression when used at the class level,
that is, from the ``EmailAddress`` class directly:

.. sourcecode:: python+sql

    from sqlalchemy.orm import Session
    session = Session()

    {sql}address = session.query(EmailAddress).\
                     filter(EmailAddress.email == 'address@example.com').\
                     one()
    SELECT address.email AS address_email, address.id AS address_id 
    FROM address 
    WHERE address.email = ?
    ('address@example.com',)
    {stop}

    address.email = 'otheraddress@example.com'
    {sql}session.commit()
    UPDATE address SET email=? WHERE address.id = ?
    ('otheraddress@example.com', 1)
    COMMIT
    {stop}

The :class:`~.hybrid_property` also allows us to change the behavior of the
attribute, including defining separate behaviors when the attribute is
accessed at the instance level versus at the class/expression level, using the
:meth:`.hybrid_property.expression` modifier. Such as, if we wanted to add a
host name automatically, we might define two sets of string manipulation
logic::

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        _email = Column("email", String)

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

Above, accessing the ``email`` property of an instance of ``EmailAddress``
will return the value of the ``_email`` attribute, removing or adding the
hostname ``@example.com`` from the value. When we query against the ``email``
attribute, a SQL function is rendered which produces the same effect:

.. sourcecode:: python+sql

    {sql}address = session.query(EmailAddress).filter(EmailAddress.email == 'address').one()
    SELECT address.email AS address_email, address.id AS address_id 
    FROM address 
    WHERE substr(address.email, ?, length(address.email) - ?) = ?
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

Each of :func:`.orm.column_property`, :func:`~.composite`, :func:`.relationship`,
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

    mapper(EmailAddress, address_table, properties={
        'email':column_property(address_table.c.email,
                                comparator_factory=MyComparator)
    })

Above, comparisons on the ``email`` column are wrapped in the SQL lower()
function to produce case-insensitive matching::

    >>> str(EmailAddress.email == 'SomeAddress@foo.com')
    lower(address.email) = lower(:lower_1)

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

We will create a mapping to a table ``vertice``, which represents two points
as ``x1/y1`` and ``x2/y2``. These are created normally as :class:`.Column`
objects. Then, the :func:`.composite` function is used to assign new
attributes that will represent sets of columns via the ``Point`` class::

    from sqlalchemy import Column, Integer
    from sqlalchemy.orm import composite
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Vertex(Base):
        __tablename__ = 'vertice'

        id = Column(Integer, primary_key=True)
        x1 = Column(Integer)
        y1 = Column(Integer)
        x2 = Column(Integer)
        y2 = Column(Integer)

        start = composite(Point, x1, y1)
        end = composite(Point, x2, y2)

A classical mapping above would define each :func:`.composite`
against the existing table::

    mapper(Vertex, vertice_table, properties={
        'start':composite(Point, vertice_table.c.x1, vertice_table.c.y1),
        'end':composite(Point, vertice_table.c.x2, vertice_table.c.y2),
    })

We can now persist and use ``Vertex`` instances, as well as query for them,
using the ``.start`` and ``.end`` attributes against ad-hoc ``Point`` instances:

.. sourcecode:: python+sql

    >>> v = Vertex(start=Point(3, 4), end=Point(5, 6))
    >>> session.add(v)
    >>> q = session.query(Vertex).filter(Vertex.start == Point(3, 4))
    {sql}>>> print q.first().start
    BEGIN (implicit)
    INSERT INTO vertice (x1, y1, x2, y2) VALUES (?, ?, ?, ?)
    (3, 4, 5, 6)
    SELECT vertice.id AS vertice_id, 
            vertice.x1 AS vertice_x1, 
            vertice.y1 AS vertice_y1, 
            vertice.x2 AS vertice_x2, 
            vertice.y2 AS vertice_y2 
    FROM vertice 
    WHERE vertice.x1 = ? AND vertice.y1 = ?
     LIMIT ? OFFSET ?
    (3, 4, 1, 0)
    {stop}Point(x=3, y=4)

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

    class Vertex(Base):
        ___tablename__ = 'vertice'

        id = Column(Integer, primary_key=True)
        x1 = Column(Integer)
        y1 = Column(Integer)
        x2 = Column(Integer)
        y2 = Column(Integer)

        start = composite(Point, x1, y1, 
                            comparator_factory=PointComparator)
        end = composite(Point, x2, y2, 
                            comparator_factory=PointComparator)

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
    j = join(user_table, address_table)

    # map to it - the identity of an AddressUser object will be
    # based on (user_id, address_id) since those are the primary keys involved
    mapper(AddressUser, j, properties={
        'user_id': [user_table.c.user_id, address_table.c.user_id]
    })

Note that the list of columns is equivalent to the usage of :func:`.orm.column_property`
with multiple columns::

    from sqlalchemy.orm import mapper, column_property

    mapper(AddressUser, j, properties={
        'user_id': column_property(user_table.c.user_id, address_table.c.user_id)
    })

The usage of :func:`.orm.column_property` is required when using declarative to map 
to multiple columns, since the declarative class parser won't recognize a plain 
list of columns::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class AddressUser(Base):
        __table__ = j

        user_id = column_property(user_table.c.user_id, address_table.c.user_id)

A second example::

    from sqlalchemy.sql import join

    # many-to-many join on an association table
    j = join(user_table, userkeywords,
            user_table.c.user_id==userkeywords.c.user_id).join(keywords,
               userkeywords.c.keyword_id==keywords.c.keyword_id)

    # a class
    class KeywordUser(object):
        pass

    # map to it - the identity of a KeywordUser object will be
    # (user_id, keyword_id) since those are the primary keys involved
    mapper(KeywordUser, j, properties={
        'user_id': [user_table.c.user_id, userkeywords.c.user_id],
        'keyword_id': [userkeywords.c.keyword_id, keywords.c.keyword_id]
    })

In both examples above, "composite" columns were added as properties to the
mappers; these are aggregations of multiple columns into one mapper property,
which instructs the mapper to keep both of those columns set at the same
value.


Mapping a Class against Arbitrary Selects
=========================================

Similar to mapping against a join, a plain :func:`~.expression.select` object can be used with a
mapper as well. Below, an example select which contains two aggregate
functions and a group_by is mapped to a class::

    from sqlalchemy import select, func

    subq = select([
                func.count(orders.c.id).label('order_count'), 
                func.max(orders.c.price).label('highest_order'), 
                orders.c.customer_id
                ]).group_by(orders.c.customer_id).alias()

    s = select([customers,subq]).\
                where(customers.c.customer_id==subq.c.customer_id)
    class Customer(object):
        pass

    mapper(Customer, s)

Above, the "customers" table is joined against the "orders" table to produce a
full row for each customer row, the total count of related rows in the
"orders" table, and the highest price in the "orders" table. That query is then mapped
against the Customer class. New instances of Customer will contain attributes
for each column in the "customers" table as well as an "order_count" and
"highest_order" attribute. Updates to the Customer object will only be
reflected in the "customers" table and not the "orders" table. This is because
the primary key columns of the "orders" table are not represented in this
mapper and therefore the table is not affected by save or delete operations.

Multiple Mappers for One Class
==============================

The first mapper created for a certain class is known as that class's "primary
mapper." Other mappers can be created as well on the "load side" - these are
called **secondary mappers**. This is a mapper that must be constructed with
the keyword argument ``non_primary=True``, and represents a load-only mapper.
Objects that are loaded with a secondary mapper will have their save operation
processed by the primary mapper. It is also invalid to add new
:func:`~sqlalchemy.orm.relationship` objects to a non-primary mapper. To use
this mapper with the Session, specify it to the
:class:`~sqlalchemy.orm.session.Session.query` method:

example:

.. sourcecode:: python+sql

    # primary mapper
    mapper(User, user_table)

    # make a secondary mapper to load User against a join
    othermapper = mapper(User, user_table.join(someothertable), non_primary=True)

    # select
    result = session.query(othermapper).select()

The "non primary mapper" is a rarely needed feature of SQLAlchemy; in most
cases, the :class:`~sqlalchemy.orm.query.Query` object can produce any kind of
query that's desired. It's recommended that a straight
:class:`~sqlalchemy.orm.query.Query` be used in place of a non-primary mapper
unless the mapper approach is absolutely needed. Current use cases for the
"non primary mapper" are when you want to map the class to a particular select
statement or view to which additional query criterion can be added, and for
when the particular mapped select statement or view is to be placed in a
:func:`~sqlalchemy.orm.relationship` of a parent mapper.

Multiple "Persistence" Mappers for One Class
=============================================

The non_primary mapper defines alternate mappers for the purposes of loading
objects. What if we want the same class to be *persisted* differently, such as
to different tables ? SQLAlchemy refers to this as the "entity name" pattern,
and in Python one can use a recipe which creates anonymous subclasses which
are distinctly mapped. See the recipe at `Entity Name
<http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.

Constructors and Object Initialization
=======================================

Mapping imposes no restrictions or requirements on the constructor
(``__init__``) method for the class. You are free to require any arguments for
the function that you wish, assign attributes to the instance that are unknown
to the ORM, and generally do anything else you would normally do when writing
a constructor for a Python class.

The SQLAlchemy ORM does not call ``__init__`` when recreating objects from
database rows. The ORM's process is somewhat akin to the Python standard
library's ``pickle`` module, invoking the low level ``__new__`` method and
then quietly restoring attributes directly on the instance rather than calling
``__init__``.

If you need to do some setup on database-loaded instances before they're ready
to use, you can use the ``@reconstructor`` decorator to tag a method as the
ORM counterpart to ``__init__``. SQLAlchemy will call this method with no
arguments every time it loads or reconstructs one of your instances. This is
useful for recreating transient properties that are normally assigned in your
``__init__``::

    from sqlalchemy import orm

    class MyMappedClass(object):
        def __init__(self, data):
            self.data = data
            # we need stuff on all instances, but not in the database.
            self.stuff = []

        @orm.reconstructor
        def init_on_load(self):
            self.stuff = []

When ``obj = MyMappedClass()`` is executed, Python calls the ``__init__``
method as normal and the ``data`` argument is required. When instances are
loaded during a :class:`~sqlalchemy.orm.query.Query` operation as in
``query(MyMappedClass).one()``, ``init_on_load`` is called instead.

Any method may be tagged as the :func:`~sqlalchemy.orm.reconstructor`, even
the ``__init__`` method. SQLAlchemy will call the reconstructor method with no
arguments. Scalar (non-collection) database-mapped attributes of the instance
will be available for use within the function. Eagerly-loaded collections are
generally not yet available and will usually only contain the first element.
ORM state changes made to objects at this stage will not be recorded for the
next flush() operation, so the activity within a reconstructor should be
conservative.

While the ORM does not call your ``__init__`` method, it will modify the
class's ``__init__`` slightly. The method is lightly wrapped to act as a
trigger for the ORM, allowing mappers to be compiled automatically and will
fire a :func:`~sqlalchemy.orm.interfaces.MapperExtension.init_instance` event
that :class:`~sqlalchemy.orm.interfaces.MapperExtension` objects may listen
for. :class:`~sqlalchemy.orm.interfaces.MapperExtension` objects can also
listen for a ``reconstruct_instance`` event, analogous to the
:func:`~sqlalchemy.orm.reconstructor` decorator above.

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


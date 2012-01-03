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

A *Classical Mapping* refers to the configuration of a mapped class using the
:func:`.mapper` function, without using the Declarative system.   As an example,
start with the declarative mapping introduced in :ref:`ormtutorial_toplevel`::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

In "classical" form, the table metadata is created separately with the :class:`.Table`
construct, then associated with the ``User`` class via the :func:`.mapper` function::

    from sqlalchemy import Table, MetaData, Column, ForeignKey, Integer, String
    from sqlalchemy.orm import mapper

    metadata = MetaData()

    user = Table('user', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(50)),
                Column('fullname', String(50)),
                Column('password', String(12))
            )

    class User(object):
        def __init__(self, name, fullname, password):
            self.name = name
            self.fullname = fullname
            self.password = password

    mapper(User, user)

Information about mapped attributes, such as relationships to other classes, are provided
via the ``properties`` dictionary.  The example below illustrates a second :class:`.Table` 
object, mapped to a class called ``Address``, then linked to ``User`` via :func:`.relationship`::

    address = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String(50))
                )

    mapper(User, user, properties={
        'addresses' : relationship(Address, backref='user', order_by=address.c.id)
    })

    mapper(Address, address)

When using classical mappings, classes must be provided directly without the benefit
of the "string lookup" system provided by Declarative.  SQL expressions are typically
specified in terms of the :class:`.Table` objects, i.e. ``address.c.id`` above
for the ``Address`` relationship, and not ``Address.id``, as ``Address`` may not
yet be linked to table metadata, nor can we specify a string here.

Some examples in the documentation still use the classical approach, but note that
the classical as well as Declarative approaches are **fully interchangeable**.  Both 
systems ultimately create the same configuration, consisting of a :class:`.Table`, 
user-defined class, linked together with a :func:`.mapper`.  When we talk about
"the behavior of :func:`.mapper`", this includes when using the Declarative system
as well - it's still used, just behind the scenes.

Customizing Column Properties
==============================

The default behavior of :func:`~.orm.mapper` is to assemble all the columns in
the mapped :class:`.Table` into mapped object attributes, each of which are
named according to the name of the column itself (specifically, the ``key``
attribute of :class:`.Column`).  This behavior can be
modified in several ways.

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

When mapping to an existing table, the :class:`.Column` object
can be referenced directly::

    class User(Base):
        __table__ = user_table
        id = user_table.c.user_id
        name = user_table.c.user_name

Or in a classical mapping, placed in the ``properties`` dictionary
with the desired key::

    mapper(User, user_table, properties={
       'id': user_table.c.user_id,
       'name': user_table.c.user_name,
    })

.. _column_prefix:

Naming All Columns with a Prefix
--------------------------------

A way to automate the assignment of a prefix to 
the mapped attribute names relative to the column name
is to use ``column_prefix``::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {'column_prefix':'_'}

The above will place attribute names such as ``_user_id``, ``_user_name``,
``_password`` etc. on the mapped ``User`` class.

The classical version of the above::

    mapper(User, user_table, column_prefix='_')

Using column_property for column level options
-----------------------------------------------

Options can be specified when mapping a :class:`.Column` using the
:func:`.column_property` function.  This function
explicitly creates the :class:`.ColumnProperty` used by the
:func:`.mapper` to keep track of the :class:`.Column`; normally, the
:func:`.mapper` creates this automatically.   Using :func:`.column_property`,
we can pass additional arguments about how we'd like the :class:`.Column`
to be mapped.   Below, we pass an option ``active_history``,
which specifies that a change to this column's value should
result in the former value being loaded first::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = column_property(Column(String(50)), active_history=True)

:func:`.column_property` is also used to map a single attribute to 
multiple columns.  This use case arises when mapping to a :func:`~.expression.join`
which has attributes which are equated to each other::

    class User(Base):
        __table__ = user.join(address)

        # assign "user.id", "address.user_id" to the
        # "id" attribute
        id = column_property(user_table.c.id, address_table.c.user_id)

For more examples featuring this usage, see :ref:`maptojoin`.

Another place where :func:`.column_property` is needed is to specify SQL expressions as 
mapped attributes, such as below where we create an attribute ``fullname``
that is the string concatenation of the ``firstname`` and ``lastname``
columns::

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))
        fullname = column_property(firstname + " " + lastname)

See examples of this usage at :ref:`mapper_sql_expressions`.

.. autofunction:: column_property

.. _include_exclude_cols:

Mapping a Subset of Table Columns
---------------------------------

Sometimes, a :class:`.Table` object was made available using the 
reflection process described at :ref:`metadata_reflection` to load 
the table's structure from the database.
For such a table that has lots of columns that don't need to be referenced
in the application, the ``include_properties`` or ``exclude_properties`` 
arguments can specify that only a subset of columns should be mapped.
For example::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {
            'include_properties' :['user_id', 'user_name']
        }

...will map the ``User`` class to the ``user_table`` table, only including
the ``user_id`` and ``user_name`` columns - the rest are not referenced.
Similarly::

    class Address(Base):
        __table__ = address_table
        __mapper_args__ = {
            'exclude_properties' : ['street', 'city', 'state', 'zip']
        }

...will map the ``Address`` class to the ``address_table`` table, including
all columns present except ``street``, ``city``, ``state``, and ``zip``.

When this mapping is used, the columns that are not included will not be
referenced in any SELECT statements emitted by :class:`.Query`, nor will there
be any mapped attribute on the mapped class which represents the column;
assigning an attribute of that name will have no effect beyond that of
a normal Python attribute assignment.

In some cases, multiple columns may have the same name, such as when
mapping to a join of two or more tables that share some column name.
``include_properties`` and ``exclude_properties`` can also accommodate
:class:`.Column` objects to more accurately describe which columns
should be included or excluded::

    class UserAddress(Base):
        __table__ = user_table.join(addresses_table)
        __mapper_args__ = {
            'exclude_properties' :[address_table.c.id],
            'primary_key' : [user_table.c.id]
        }

.. note:: 

   insert and update defaults configured on individual
   :class:`.Column` objects, i.e. those described at :ref:`metadata_defaults`
   including those configured by the ``default``, ``update``,
   ``server_default`` and ``server_onupdate`` arguments, will continue to
   function normally even if those :class:`.Column` objects are not mapped.
   This is because in the case of ``default`` and ``update``, the
   :class:`.Column` object is still present on the underlying
   :class:`.Table`, thus allowing the default functions to take place when
   the ORM emits an INSERT or UPDATE, and in the case of ``server_default``
   and ``server_onupdate``, the relational database itself maintains these
   functions.


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

Attributes on a mapped class can be linked to SQL expressions, which can 
be used in queries. 

Using a Hybrid
--------------

The easiest and most flexible way to link relatively simple SQL expressions to a class is to use a so-called
"hybrid attribute",
described in the section :ref:`hybrids_toplevel`.  The hybrid provides
for an expression that works at both the Python level as well as at the 
SQL expression level.  For example, below we map a class ``User``,
containing attributes ``firstname`` and ``lastname``, and include a hybrid that
will provide for us the ``fullname``, which is the string concatenation of the two::

    from sqlalchemy.ext.hybrid import hybrid_property

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @hybrid_property
        def fullname(self):
            return self.firstname + " " + self.lastname

Above, the ``fullname`` attribute is interpreted at both the instance and 
class level, so that it is available from an instance::

    some_user = session.query(User).first()
    print some_user.fullname

as well as usable wtihin queries::

    some_user = session.query(User).filter(User.fullname == "John Smith").first()

The string concatenation example is a simple one, where the Python expression
can be dual purposed at the instance and class level.  Often, the SQL expression
must be distinguished from the Python expression, which can be achieved using
:meth:`.hybrid_property.expression`.  Below we illustrate the case where a conditional
needs to be present inside the hybrid, using the ``if`` statement in Python and the
:func:`.sql.expression.case` construct for SQL expressions::

    from sqlalchemy.ext.hybrid import hybrid_property
    from sqlalchemy.sql import case

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @hybrid_property
        def fullname(self):
            if self.firstname is not None:
                return self.firstname + " " + self.lastname
            else:
                return self.lastname

        @fullname.expression
        def fullname(cls):
            return case([
                (cls.firstname != None, cls.firstname + " " + cls.lastname),
            ], else_ = cls.lastname)

Using column_property
---------------------

The :func:`.orm.column_property` function can be used to map a SQL
expression in a manner similar to a regularly mapped :class:`.Column`.   
With this technique, the attribute is loaded
along with all other column-mapped attributes at load time.  This is in some
cases an advantage over the usage of hybrids, as the value can be loaded
up front at the same time as the parent row of the object, particularly if
the expression is one which links to other tables (typically as a correlated
subquery) to access data that wouldn't normally be 
available on an already loaded object.

Disadvantages to using :func:`.orm.column_property` for SQL expressions include that 
the expression must be compatible with the SELECT statement emitted for the class 
as a whole, and there are also some configurational quirks which can occur 
when using :func:`.orm.column_property` from declarative mixins.

Our "fullname" example can be expressed using :func:`.orm.column_property` as 
follows::

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

Using a plain descriptor
-------------------------

In cases where a SQL query more elaborate than what :func:`.orm.column_property`
or :class:`.hybrid_property` can provide must be emitted, a regular Python 
function accessed as an attribute can be used, assuming the expression
only needs to be available on an already-loaded instance.   The function 
is decorated with Python's own ``@property`` decorator to mark it as a read-only
attribute.   Within the function, :func:`.object_session`
is used to locate the :class:`.Session` corresponding to the current object,
which is then used to emit a query::

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

The plain descriptor approach is useful as a last resort, but is less performant
in the usual case than both the hybrid and column property approaches, in that
it needs to emit a SQL query upon each access.

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

Using Descriptors and Hybrids
-----------------------------

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
using the techniques described in :ref:`mapper_sql_expressions`.  
Those approaches should be considered first before resorting to custom comparison objects.

Each of :func:`.orm.column_property`, :func:`~.composite`, :func:`.relationship`,
and :func:`.comparable_property` accept an argument called
``comparator_factory``.   A subclass of :class:`.PropComparator` can be provided
for this argument, which can then reimplement basic Python comparison methods
such as ``__eq__()``, ``__ne__()``, ``__lt__()``, and so on. 

It's best to subclass the :class:`.PropComparator` subclass provided by
each type of property.  For example, to allow a column-mapped attribute to
do case-insensitive comparison::

    from sqlalchemy.orm.properties import ColumnProperty
    from sqlalchemy.sql import func, Column, Integer, String

    class MyComparator(ColumnProperty.Comparator):
        def __eq__(self, other):
            return func.lower(self.__clause_element__()) == func.lower(other)

    class EmailAddress(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = column_property(
                        Column('email', String),
                        comparator_factory=MyComparator
                    )

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
*selectables*) in addition to plain tables. For example, the :func:`~.expression.join`
function creates a selectable unit comprised of
multiple tables, complete with its own composite primary key, which can be
mapped in the same way as a :class:`.Table`::

    from sqlalchemy import Table, Column, Integer, \
            String, MetaData, join, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import column_property

    metadata = MetaData()

    # define two Table objects
    user_table = Table('user', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String),
            )

    address_table = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String)
                )

    # define a join between them.  This
    # takes place across the user.id and address.user_id
    # columns.
    user_address_join = join(user_table, address_table)

    Base = declarative_base()

    # map to it
    class AddressUser(Base):
        __table__ = user_address_join

        id = column_property(user_table.c.id, address_table.c.user_id)
        address_id = address_table.c.id

In the example above, the join expresses columns for both the
``user`` and the ``address`` table.  The ``user.id`` and ``address.user_id``
columns are equated by foreign key, so in the mapping they are defined
as one attribute, ``AddressUser.id``, using :func:`.column_property` to
indicate a specialized column mapping.   Based on this part of the 
configuration, the mapping will copy
new primary key values from ``user.id`` into the ``address.user_id`` column
when a flush occurs.

Additionally, the ``address.id`` column is mapped explicitly to 
an attribute named ``address_id``.   This is to **disambiguate** the 
mapping of the ``address.id`` column from the same-named ``AddressUser.id`` 
attribute, which here has been assigned to refer to the ``user`` table
combined with the ``address.user_id`` foreign key.

The natural primary key of the above mapping is the composite of
``(user.id, address.id)``, as these are the primary key columns of the
``user`` and ``address`` table combined together.  The identity of an 
``AddressUser`` object will be in terms of these two values, and
is represented from an ``AddressUser`` object as 
``(AddressUser.id, AddressUser.address_id)``.


Mapping a Class against Arbitrary Selects
=========================================

Similar to mapping against a join, a plain :func:`~.expression.select` object can be used with a
mapper as well.  The example fragment below illustrates mapping a class
called ``Customer`` to a :func:`~.expression.select` which includes a join to a 
subquery::

    from sqlalchemy import select, func

    subq = select([
                func.count(orders.c.id).label('order_count'), 
                func.max(orders.c.price).label('highest_order'), 
                orders.c.customer_id
                ]).group_by(orders.c.customer_id).alias()

    customer_select = select([customers,subq]).\
                where(customers.c.customer_id==subq.c.customer_id)

    class Customer(Base):
        __table__ = customer_select

Above, the full row represented by ``customer_select`` will be all the
columns of the ``customers`` table, in addition to those columns
exposed by the ``subq`` subquery, which are ``order_count``, 
``highest_order``, and ``customer_id``.  Mapping the ``Customer``
class to this selectable then creates a class which will contain
those attributes.

When the ORM persists new instances of ``Customer``, only the 
``customers`` table will actually receive an INSERT.  This is because the
primary key of the ``orders`` table is not represented in the mapping;  the ORM
will only emit an INSERT into a table for which it has mapped the primary
key.

Multiple Mappers for One Class
==============================

In modern SQLAlchemy, a particular class is only mapped by one :func:`.mapper`
at a time.  The rationale here is that the :func:`.mapper` modifies the class itself, not only
persisting it towards a particular :class:`.Table`, but also *instrumenting*
attributes upon the class which are structured specifically according to the
table metadata.

One potential use case for another mapper to exist at the same time is if we 
wanted to load instances of our class not just from the immediate :class:`.Table`
to which it is mapped, but from another selectable that is a derivation of that
:class:`.Table`.   While there technically is a way to create such a :func:`.mapper`,
using the ``non_primary=True`` option, this approach is virtually never needed.
Instead, we use the functionality of the :class:`.Query` object to achieve this, 
using a method such as :meth:`.Query.select_from`
or :meth:`.Query.from_statement` to specify a derived selectable.

Another potential use is if we genuinely want instances of our class to
be persisted into different tables at different times; certain kinds of 
data sharding configurations may persist a particular class into tables
that are identical in structure except for their name.   For this kind of 
pattern, Python offers a better approach than the complexity of mapping
the same class multiple times, which is to instead create new mapped classes
for each target table.    SQLAlchemy refers to this as the "entity name"
pattern, which is described as a recipe at `Entity Name
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
method as normal and the ``data`` argument is required.  When instances are
loaded during a :class:`~sqlalchemy.orm.query.Query` operation as in
``query(MyMappedClass).one()``, ``init_on_load`` is called.

Any method may be tagged as the :func:`~sqlalchemy.orm.reconstructor`, even
the ``__init__`` method. SQLAlchemy will call the reconstructor method with no
arguments. Scalar (non-collection) database-mapped attributes of the instance
will be available for use within the function. Eagerly-loaded collections are
generally not yet available and will usually only contain the first element.
ORM state changes made to objects at this stage will not be recorded for the
next flush() operation, so the activity within a reconstructor should be
conservative.

:func:`~sqlalchemy.orm.reconstructor` is a shortcut into a larger system
of "instance level" events, which can be subscribed to using the 
event API - see :class:`.InstanceEvents` for the full API description
of these events.

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


.. _declarative_mixins:

Mixin and Custom Base Classes
=============================

A common need when using :mod:`~sqlalchemy.ext.declarative` is to
share some functionality, such as a set of common columns, some common
table options, or other mapped properties, across many
classes.  The standard Python idioms for this is to have the classes
inherit from a base which includes these common features.

When using :mod:`~sqlalchemy.ext.declarative`, this idiom is allowed
via the usage of a custom declarative base class, as well as a "mixin" class
which is inherited from in addition to the primary base.  Declarative
includes several helper features to make this work in terms of how
mappings are declared.   An example of some commonly mixed-in
idioms is below::

    from sqlalchemy.ext.declarative import declared_attr

    class MyMixin(object):

        @declared_attr
        def __tablename__(cls):
            return cls.__name__.lower()

        __table_args__ = {'mysql_engine': 'InnoDB'}
        __mapper_args__= {'always_refresh': True}

        id =  Column(Integer, primary_key=True)

    class MyModel(MyMixin, Base):
        name = Column(String(1000))

Where above, the class ``MyModel`` will contain an "id" column
as the primary key, a ``__tablename__`` attribute that derives
from the name of the class itself, as well as ``__table_args__``
and ``__mapper_args__`` defined by the ``MyMixin`` mixin class.

There's no fixed convention over whether ``MyMixin`` precedes
``Base`` or not.  Normal Python method resolution rules apply, and
the above example would work just as well with::

    class MyModel(Base, MyMixin):
        name = Column(String(1000))

This works because ``Base`` here doesn't define any of the
variables that ``MyMixin`` defines, i.e. ``__tablename__``,
``__table_args__``, ``id``, etc.   If the ``Base`` did define
an attribute of the same name, the class placed first in the
inherits list would determine which attribute is used on the
newly defined class.

Augmenting the Base
~~~~~~~~~~~~~~~~~~~

In addition to using a pure mixin, most of the techniques in this
section can also be applied to the base class itself, for patterns that
should apply to all classes derived from a particular base.  This is achieved
using the ``cls`` argument of the :func:`.declarative_base` function::

    from sqlalchemy.ext.declarative import declared_attr

    class Base(object):
        @declared_attr
        def __tablename__(cls):
            return cls.__name__.lower()

        __table_args__ = {'mysql_engine': 'InnoDB'}

        id =  Column(Integer, primary_key=True)

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base(cls=Base)

    class MyModel(Base):
        name = Column(String(1000))

Where above, ``MyModel`` and all other classes that derive from ``Base`` will
have a table name derived from the class name, an ``id`` primary key column,
as well as the "InnoDB" engine for MySQL.

Mixing in Columns
~~~~~~~~~~~~~~~~~

The most basic way to specify a column on a mixin is by simple
declaration::

    class TimestampMixin(object):
        created_at = Column(DateTime, default=func.now())

    class MyModel(TimestampMixin, Base):
        __tablename__ = 'test'

        id =  Column(Integer, primary_key=True)
        name = Column(String(1000))

Where above, all declarative classes that include ``TimestampMixin``
will also have a column ``created_at`` that applies a timestamp to
all row insertions.

Those familiar with the SQLAlchemy expression language know that
the object identity of clause elements defines their role in a schema.
Two ``Table`` objects ``a`` and ``b`` may both have a column called
``id``, but the way these are differentiated is that ``a.c.id``
and ``b.c.id`` are two distinct Python objects, referencing their
parent tables ``a`` and ``b`` respectively.

In the case of the mixin column, it seems that only one
:class:`.Column` object is explicitly created, yet the ultimate
``created_at`` column above must exist as a distinct Python object
for each separate destination class.  To accomplish this, the declarative
extension creates a **copy** of each :class:`.Column` object encountered on
a class that is detected as a mixin.

This copy mechanism is limited to simple columns that have no foreign
keys, as a :class:`.ForeignKey` itself contains references to columns
which can't be properly recreated at this level.  For columns that
have foreign keys, as well as for the variety of mapper-level constructs
that require destination-explicit context, the
:class:`~.declared_attr` decorator is provided so that
patterns common to many classes can be defined as callables::

    from sqlalchemy.ext.declarative import declared_attr

    class ReferenceAddressMixin(object):
        @declared_attr
        def address_id(cls):
            return Column(Integer, ForeignKey('address.id'))

    class User(ReferenceAddressMixin, Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)

Where above, the ``address_id`` class-level callable is executed at the
point at which the ``User`` class is constructed, and the declarative
extension can use the resulting :class:`.Column` object as returned by
the method without the need to copy it.

.. versionchanged:: 0.6.5  Rename ``sqlalchemy.util.classproperty``
    into :class:`~.declared_attr`.

Columns generated by :class:`~.declared_attr` can also be
referenced by ``__mapper_args__`` to a limited degree, currently
by ``polymorphic_on`` and ``version_id_col``; the declarative extension
will resolve them at class construction time::

    class MyMixin:
        @declared_attr
        def type_(cls):
            return Column(String(50))

        __mapper_args__= {'polymorphic_on':type_}

    class MyModel(MyMixin, Base):
        __tablename__='test'
        id =  Column(Integer, primary_key=True)


Mixing in Relationships
~~~~~~~~~~~~~~~~~~~~~~~

Relationships created by :func:`~sqlalchemy.orm.relationship` are provided
with declarative mixin classes exclusively using the
:class:`.declared_attr` approach, eliminating any ambiguity
which could arise when copying a relationship and its possibly column-bound
contents. Below is an example which combines a foreign key column and a
relationship so that two classes ``Foo`` and ``Bar`` can both be configured to
reference a common target class via many-to-one::

    class RefTargetMixin(object):
        @declared_attr
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))

        @declared_attr
        def target(cls):
            return relationship("Target")

    class Foo(RefTargetMixin, Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)

    class Bar(RefTargetMixin, Base):
        __tablename__ = 'bar'
        id = Column(Integer, primary_key=True)

    class Target(Base):
        __tablename__ = 'target'
        id = Column(Integer, primary_key=True)


Using Advanced Relationship Arguments (e.g. ``primaryjoin``, etc.)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`~sqlalchemy.orm.relationship` definitions which require explicit
primaryjoin, order_by etc. expressions should in all but the most
simplistic cases use **late bound** forms
for these arguments, meaning, using either the string form or a lambda.
The reason for this is that the related :class:`.Column` objects which are to
be configured using ``@declared_attr`` are not available to another
``@declared_attr`` attribute; while the methods will work and return new
:class:`.Column` objects, those are not the :class:`.Column` objects that
Declarative will be using as it calls the methods on its own, thus using
*different* :class:`.Column` objects.

The canonical example is the primaryjoin condition that depends upon
another mixed-in column::

    class RefTargetMixin(object):
        @declared_attr
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))

        @declared_attr
        def target(cls):
            return relationship(Target,
                primaryjoin=Target.id==cls.target_id   # this is *incorrect*
            )

Mapping a class using the above mixin, we will get an error like::

    sqlalchemy.exc.InvalidRequestError: this ForeignKey's parent column is not
    yet associated with a Table.

This is because the ``target_id`` :class:`.Column` we've called upon in our
``target()`` method is not the same :class:`.Column` that declarative is
actually going to map to our table.

The condition above is resolved using a lambda::

    class RefTargetMixin(object):
        @declared_attr
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))

        @declared_attr
        def target(cls):
            return relationship(Target,
                primaryjoin=lambda: Target.id==cls.target_id
            )

or alternatively, the string form (which ultimately generates a lambda)::

    class RefTargetMixin(object):
        @declared_attr
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))

        @declared_attr
        def target(cls):
            return relationship("Target",
                primaryjoin="Target.id==%s.target_id" % cls.__name__
            )

Mixing in deferred(), column_property(), and other MapperProperty classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like :func:`~sqlalchemy.orm.relationship`, all
:class:`~sqlalchemy.orm.interfaces.MapperProperty` subclasses such as
:func:`~sqlalchemy.orm.deferred`, :func:`~sqlalchemy.orm.column_property`,
etc. ultimately involve references to columns, and therefore, when
used with declarative mixins, have the :class:`.declared_attr`
requirement so that no reliance on copying is needed::

    class SomethingMixin(object):

        @declared_attr
        def dprop(cls):
            return deferred(Column(Integer))

    class Something(SomethingMixin, Base):
        __tablename__ = "something"

The :func:`.column_property` or other construct may refer
to other columns from the mixin.  These are copied ahead of time before
the :class:`.declared_attr` is invoked::

    class SomethingMixin(object):
        x = Column(Integer)

        y = Column(Integer)

        @declared_attr
        def x_plus_y(cls):
            return column_property(cls.x + cls.y)


.. versionchanged:: 1.0.0 mixin columns are copied to the final mapped class
   so that :class:`.declared_attr` methods can access the actual column
   that will be mapped.

Mixing in Association Proxy and Other Attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mixins can specify user-defined attributes as well as other extension
units such as :func:`.association_proxy`.   The usage of
:class:`.declared_attr` is required in those cases where the attribute must
be tailored specifically to the target subclass.   An example is when
constructing multiple :func:`.association_proxy` attributes which each
target a different type of child object.  Below is an
:func:`.association_proxy` / mixin example which provides a scalar list of
string values to an implementing class::

    from sqlalchemy import Column, Integer, ForeignKey, String
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.declarative import declarative_base, declared_attr

    Base = declarative_base()

    class HasStringCollection(object):
        @declared_attr
        def _strings(cls):
            class StringAttribute(Base):
                __tablename__ = cls.string_table_name
                id = Column(Integer, primary_key=True)
                value = Column(String(50), nullable=False)
                parent_id = Column(Integer,
                                ForeignKey('%s.id' % cls.__tablename__),
                                nullable=False)
                def __init__(self, value):
                    self.value = value

            return relationship(StringAttribute)

        @declared_attr
        def strings(cls):
            return association_proxy('_strings', 'value')

    class TypeA(HasStringCollection, Base):
        __tablename__ = 'type_a'
        string_table_name = 'type_a_strings'
        id = Column(Integer(), primary_key=True)

    class TypeB(HasStringCollection, Base):
        __tablename__ = 'type_b'
        string_table_name = 'type_b_strings'
        id = Column(Integer(), primary_key=True)

Above, the ``HasStringCollection`` mixin produces a :func:`.relationship`
which refers to a newly generated class called ``StringAttribute``.  The
``StringAttribute`` class is generated with its own :class:`.Table`
definition which is local to the parent class making usage of the
``HasStringCollection`` mixin.  It also produces an :func:`.association_proxy`
object which proxies references to the ``strings`` attribute onto the ``value``
attribute of each ``StringAttribute`` instance.

``TypeA`` or ``TypeB`` can be instantiated given the constructor
argument ``strings``, a list of strings::

    ta = TypeA(strings=['foo', 'bar'])
    tb = TypeA(strings=['bat', 'bar'])

This list will generate a collection
of ``StringAttribute`` objects, which are persisted into a table that's
local to either the ``type_a_strings`` or ``type_b_strings`` table::

    >>> print(ta._strings)
    [<__main__.StringAttribute object at 0x10151cd90>,
        <__main__.StringAttribute object at 0x10151ce10>]

When constructing the :func:`.association_proxy`, the
:class:`.declared_attr` decorator must be used so that a distinct
:func:`.association_proxy` object is created for each of the ``TypeA``
and ``TypeB`` classes.

.. versionadded:: 0.8 :class:`.declared_attr` is usable with non-mapped
   attributes, including user-defined attributes as well as
   :func:`.association_proxy`.

.. _decl_mixin_inheritance:

Controlling table inheritance with mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``__tablename__`` attribute may be used to provide a function that
will determine the name of the table used for each class in an inheritance
hierarchy, as well as whether a class has its own distinct table.

This is achieved using the :class:`.declared_attr` indicator in conjunction
with a method named ``__tablename__()``.   Declarative will always
invoke :class:`.declared_attr` for the special names
``__tablename__``, ``__mapper_args__`` and ``__table_args__``
function **for each mapped class in the hierarchy**.   The function therefore
needs to expect to receive each class individually and to provide the
correct answer for each.

For example, to create a mixin that gives every class a simple table
name based on class name::

    from sqlalchemy.ext.declarative import declared_attr

    class Tablename:
        @declared_attr
        def __tablename__(cls):
            return cls.__name__.lower()

    class Person(Tablename, Base):
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = None
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

Alternatively, we can modify our ``__tablename__`` function to return
``None`` for subclasses, using :func:`.has_inherited_table`.  This has
the effect of those subclasses being mapped with single table inheritance
against the parent::

    from sqlalchemy.ext.declarative import declared_attr
    from sqlalchemy.ext.declarative import has_inherited_table

    class Tablename(object):
        @declared_attr
        def __tablename__(cls):
            if has_inherited_table(cls):
                return None
            return cls.__name__.lower()

    class Person(Tablename, Base):
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        primary_language = Column(String(50))
        __mapper_args__ = {'polymorphic_identity': 'engineer'}

.. _mixin_inheritance_columns:

Mixing in Columns in Inheritance Scenarios
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In constrast to how ``__tablename__`` and other special names are handled when
used with :class:`.declared_attr`, when we mix in columns and properties (e.g.
relationships, column properties, etc.), the function is
invoked for the **base class only** in the hierarchy.  Below, only the
``Person`` class will receive a column
called ``id``; the mapping will fail on ``Engineer``, which is not given
a primary key::

    class HasId(object):
        @declared_attr
        def id(cls):
            return Column('id', Integer, primary_key=True)

    class Person(HasId, Base):
        __tablename__ = 'person'
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = 'engineer'
        primary_language = Column(String(50))
        __mapper_args__ = {'polymorphic_identity': 'engineer'}

It is usually the case in joined-table inheritance that we want distinctly
named columns on each subclass.  However in this case, we may want to have
an ``id`` column on every table, and have them refer to each other via
foreign key.  We can achieve this as a mixin by using the
:attr:`.declared_attr.cascading` modifier, which indicates that the
function should be invoked **for each class in the hierarchy**, just like
it does for ``__tablename__``::

    class HasIdMixin(object):
        @declared_attr.cascading
        def id(cls):
            if has_inherited_table(cls):
                return Column(ForeignKey('person.id'), primary_key=True)
            else:
                return Column(Integer, primary_key=True)

    class Person(HasIdMixin, Base):
        __tablename__ = 'person'
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = 'engineer'
        primary_language = Column(String(50))
        __mapper_args__ = {'polymorphic_identity': 'engineer'}


.. versionadded:: 1.0.0 added :attr:`.declared_attr.cascading`.

Combining Table/Mapper Arguments from Multiple Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the case of ``__table_args__`` or ``__mapper_args__``
specified with declarative mixins, you may want to combine
some parameters from several mixins with those you wish to
define on the class iteself. The
:class:`.declared_attr` decorator can be used
here to create user-defined collation routines that pull
from multiple collections::

    from sqlalchemy.ext.declarative import declared_attr

    class MySQLSettings(object):
        __table_args__ = {'mysql_engine':'InnoDB'}

    class MyOtherMixin(object):
        __table_args__ = {'info':'foo'}

    class MyModel(MySQLSettings, MyOtherMixin, Base):
        __tablename__='my_model'

        @declared_attr
        def __table_args__(cls):
            args = dict()
            args.update(MySQLSettings.__table_args__)
            args.update(MyOtherMixin.__table_args__)
            return args

        id =  Column(Integer, primary_key=True)

Creating Indexes with Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To define a named, potentially multicolumn :class:`.Index` that applies to all
tables derived from a mixin, use the "inline" form of :class:`.Index` and
establish it as part of ``__table_args__``::

    class MyMixin(object):
        a =  Column(Integer)
        b =  Column(Integer)

        @declared_attr
        def __table_args__(cls):
            return (Index('test_idx_%s' % cls.__tablename__, 'a', 'b'),)

    class MyModel(MyMixin, Base):
        __tablename__ = 'atable'
        c =  Column(Integer,primary_key=True)

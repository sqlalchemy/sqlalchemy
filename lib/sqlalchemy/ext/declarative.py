"""
Synopsis
========

SQLAlchemy object-relational configuration involves the use of
:class:`~sqlalchemy.schema.Table`, :func:`~sqlalchemy.orm.mapper`, and
class objects to define the three areas of configuration.
:mod:`~sqlalchemy.ext.declarative` allows all three types of
configuration to be expressed declaratively on an individual
mapped class.  Regular SQLAlchemy schema elements and ORM constructs
are used in most cases.

As a simple example::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name =  Column(String(50))

Above, the :func:`declarative_base` callable returns a new base class from which
all mapped classes should inherit.  When the class definition is completed, a
new :class:`~sqlalchemy.schema.Table` and
:class:`~sqlalchemy.orm.mapper` will have been generated, accessible
via the ``__table__`` and ``__mapper__`` attributes on the ``SomeClass`` class.

Defining Attributes
===================

In the above example, the :class:`~sqlalchemy.schema.Column` objects are
automatically named with the name of the attribute to which they are
assigned.

They can also be explicitly named, and that name does not have to be
the same as name assigned on the class.
The column will be assigned to the :class:`~sqlalchemy.schema.Table` using the
given name, and mapped to the class using the attribute name::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column("some_table_id", Integer, primary_key=True)
        name = Column("name", String(50))
    
Attributes may be added to the class after its construction, and they will be
added to the underlying :class:`~sqlalchemy.schema.Table` and
:func:`~sqlalchemy.orm.mapper()` definitions as appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relationship(RelatedInfo)

Classes which are mapped explicitly using
:func:`~sqlalchemy.orm.mapper()` can interact freely with declarative
classes.

It is recommended, though not required, that all tables 
share the same underlying :class:`~sqlalchemy.schema.MetaData` object,
so that string-configured :class:`~sqlalchemy.schema.ForeignKey`
references can be resolved without issue.

Association of Metadata and Engine
==================================

The :func:`declarative_base` base class contains a
:class:`~sqlalchemy.schema.MetaData` object where newly 
defined :class:`~sqlalchemy.schema.Table` objects are collected.  This
is accessed via the :class:`~sqlalchemy.schema.MetaData` class level
accessor, so to create tables we can say:: 

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

The :class:`~sqlalchemy.engine.base.Engine` created above may also be
directly associated with the declarative base class using the ``bind``
keyword argument, where it will be associated with the underlying
:class:`~sqlalchemy.schema.MetaData` object and allow SQL operations 
involving that metadata and its tables to make use of that engine
automatically::

    Base = declarative_base(bind=create_engine('sqlite://'))

Alternatively, by way of the normal
:class:`~sqlalchemy.schema.MetaData` behavior, the ``bind`` attribute
of the class level accessor can be assigned at any time as follows::

    Base.metadata.bind = create_engine('sqlite://')

The :func:`declarative_base` can also receive a pre-created
:class:`~sqlalchemy.schema.MetaData` object, which allows a
declarative setup to be associated with an already 
existing traditional collection of :class:`~sqlalchemy.schema.Table`
objects:: 

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Configuring Relationships
=========================

Relationships to other classes are done in the usual way, with the added
feature that the class specified to :func:`~sqlalchemy.orm.relationship`
may be a string name.  The "class registry" associated with ``Base``
is used at mapper compilation time to resolve the name into the actual
class object, which is expected to have been defined once the mapper
configuration is used:: 

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        addresses = relationship("Address", backref="user")

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))

Column constructs, since they are just that, are immediately usable,
as below where we define a primary join condition on the ``Address``
class using them:: 

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relationship(User, primaryjoin=user_id == User.id)

In addition to the main argument for :func:`~sqlalchemy.orm.relationship`,
other arguments which depend upon the columns present on an as-yet
undefined class may also be specified as strings.  These strings are
evaluated as Python expressions.  The full namespace available within
this evaluation includes all classes mapped for this declarative base,
as well as the contents of the ``sqlalchemy`` package, including
expression functions like :func:`~sqlalchemy.sql.expression.desc` and
:attr:`~sqlalchemy.sql.expression.func`:: 

    class User(Base):
        # ....
        addresses = relationship("Address",
                             order_by="desc(Address.email)", 
                             primaryjoin="Address.user_id==User.id")

As an alternative to string-based attributes, attributes may also be 
defined after all classes have been created.  Just add them to the target
class after the fact::

    User.addresses = relationship(Address,
                              primaryjoin=Address.user_id==User.id)

Configuring Many-to-Many Relationships
======================================

There's nothing special about many-to-many with declarative.  The
``secondary`` argument to :func:`~sqlalchemy.orm.relationship` still
requires a :class:`~sqlalchemy.schema.Table` object, not a declarative
class. The :class:`~sqlalchemy.schema.Table` should share the same
:class:`~sqlalchemy.schema.MetaData` object used by the declarative
base:: 

    keywords = Table(
        'keywords', Base.metadata,
        Column('author_id', Integer, ForeignKey('authors.id')),
        Column('keyword_id', Integer, ForeignKey('keywords.id'))
        )
                
    class Author(Base):
        __tablename__ = 'authors'
        id = Column(Integer, primary_key=True)
        keywords = relationship("Keyword", secondary=keywords)

You should generally **not** map a class and also specify its table in
a many-to-many relationship, since the ORM may issue duplicate INSERT and
DELETE statements. 


Defining Synonyms
=================

Synonyms are introduced in :ref:`synonyms`. To define a getter/setter
which proxies to an underlying attribute, use
:func:`~sqlalchemy.orm.synonym` with the ``descriptor`` argument::

    class MyClass(Base):
        __tablename__ = 'sometable'

        _attr = Column('attr', String)

        def _get_attr(self):
            return self._some_attr
        def _set_attr(self, attr):
            self._some_attr = attr
        attr = synonym('_attr', descriptor=property(_get_attr, _set_attr))

The above synonym is then usable as an instance attribute as well as a
class-level expression construct::

    x = MyClass()
    x.attr = "some value"
    session.query(MyClass).filter(MyClass.attr == 'some other value').all()

For simple getters, the :func:`synonym_for` decorator can be used in
conjunction with ``@property``::

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)

        @synonym_for('_attr')
        @property
        def attr(self):
            return self._some_attr

Similarly, :func:`comparable_using` is a front end for the
:func:`~sqlalchemy.orm.comparable_property` ORM function::

    class MyClass(Base):
        __tablename__ = 'sometable'

        name = Column('name', String)

        @comparable_using(MyUpperCaseComparator)
        @property
        def uc_name(self):
            return self.name.upper()

Table Configuration
===================

Table arguments other than the name, metadata, and mapped Column
arguments are specified using the ``__table_args__`` class attribute.
This attribute accommodates both positional as well as keyword
arguments that are normally sent to the
:class:`~sqlalchemy.schema.Table` constructor.
The attribute can be specified in one of two forms. One is as a
dictionary:: 

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = {'mysql_engine':'InnoDB'}

The other, a tuple of the form
``(arg1, arg2, ..., {kwarg1:value, ...})``, which allows positional
arguments to be specified as well (usually constraints):: 

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                {'autoload':True}
                )

Note that the keyword parameters dictionary is required in the tuple
form even if empty.

As an alternative to ``__tablename__``, a direct
:class:`~sqlalchemy.schema.Table` construct may be used.  The
:class:`~sqlalchemy.schema.Column` objects, which in this case require
their names, will be added to the mapping just like a regular mapping
to a table:: 

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

Mapper Configuration
====================

Configuration of mappers is done with the
:func:`~sqlalchemy.orm.mapper` function and all the possible mapper
configuration parameters can be found in the documentation for that
function.

:func:`~sqlalchemy.orm.mapper` is still used by declaratively mapped
classes and keyword parameters to the function can be passed by
placing them in the ``__mapper_args__`` class variable::

    class Widget(Base):
        __tablename__ = 'widgets'
        id = Column(Integer, primary_key=True)
        
        __mapper_args__ = {'extension': MyWidgetExtension()}

Inheritance Configuration
=========================

Declarative supports all three forms of inheritance as intuitively
as possible.  The ``inherits`` mapper keyword argument is not needed
as declarative will determine this from the class itself.   The various
"polymorphic" keyword arguments are specified using ``__mapper_args__``.

Joined Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Joined table inheritance is defined as a subclass that defines its own 
table::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        id = Column(Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

Note that above, the ``Engineer.id`` attribute, since it shares the
same attribute name as the ``Person.id`` attribute, will in fact
represent the ``people.id`` and ``engineers.id`` columns together, and
will render inside a query as ``"people.id"``. 
To provide the ``Engineer`` class with an attribute that represents
only the ``engineers.id`` column, give it a different attribute name::

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        engineer_id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

Single Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Single table inheritance is defined as a subclass that does not have
its own table; you just leave out the ``__table__`` and ``__tablename__``
attributes:: 

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

When the above mappers are configured, the ``Person`` class is mapped
to the ``people`` table *before* the ``primary_language`` column is
defined, and this column will not be included in its own mapping.
When ``Engineer`` then defines the ``primary_language`` column, the
column is added to the ``people`` table so that it is included in the
mapping for ``Engineer`` and is also part of the table's full set of
columns.  Columns which are not mapped to ``Person`` are also excluded
from any other single or joined inheriting classes using the
``exclude_properties`` mapper argument.  Below, ``Manager`` will have
all the attributes of ``Person`` and ``Manager`` but *not* the
``primary_language`` attribute of ``Engineer``::

    class Manager(Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}
        golf_swing = Column(String(50))

The attribute exclusion logic is provided by the
``exclude_properties`` mapper argument, and declarative's default
behavior can be disabled by passing an explicit ``exclude_properties``
collection (empty or otherwise) to the ``__mapper_args__``.

Concrete Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~~~

Concrete is defined as a subclass which has its own table and sets the
``concrete`` keyword argument to ``True``::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        
    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'concrete':True}
        id = Column(Integer, primary_key=True)
        primary_language = Column(String(50))
        name = Column(String(50))

Usage of an abstract base class is a little less straightforward as it
requires usage of :func:`~sqlalchemy.orm.util.polymorphic_union`::

    engineers = Table('engineers', Base.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', String(50)),
                    Column('primary_language', String(50))
                )
    managers = Table('managers', Base.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', String(50)),
                    Column('golf_swing', String(50))
                )
    
    punion = polymorphic_union({
        'engineer':engineers,
        'manager':managers
    }, 'type', 'punion')
    
    class Person(Base):
        __table__ = punion
        __mapper_args__ = {'polymorphic_on':punion.c.type}
        
    class Engineer(Person):
        __table__ = engineers
        __mapper_args__ = {'polymorphic_identity':'engineer', 'concrete':True}

    class Manager(Person):
        __table__ = managers
        __mapper_args__ = {'polymorphic_identity':'manager', 'concrete':True}
    

Mixin Classes
==============

A common need when using :mod:`~sqlalchemy.ext.declarative` is to
share some functionality, often a set of columns, across many
classes. The normal Python idiom would be to put this common code into
a base class and have all the other classes subclass this class.

When using :mod:`~sqlalchemy.ext.declarative`, this need is met by
using a "mixin class". A mixin class is one that isn't mapped to a
table and doesn't subclass the declarative :class:`Base`. For example::

    class MyMixin(object):
    
        __table_args__ = {'mysql_engine': 'InnoDB'}
        __mapper_args__= {'always_refresh': True}
        
        id =  Column(Integer, primary_key=True)


    class MyModel(Base,MyMixin):
        __tablename__ = 'test'

        name = Column(String(1000))

Where above, the class ``MyModel`` will contain an "id" column
as well as ``__table_args__`` and ``__mapper_args__`` defined
by the ``MyMixin`` mixin class.

Mixing in Columns
~~~~~~~~~~~~~~~~~

The most basic way to specify a column on a mixin is by simple 
declaration::

    class TimestampMixin(object):
        created_at = Column(DateTime, default=func.now())

    class MyModel(Base, TimestampMixin):
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
:class:`Column` object is explicitly created, yet the ultimate 
``created_at`` column above must exist as a distinct Python object
for each separate destination class.  To accomplish this, the declarative
extension creates a **copy** of each :class:`Column` object encountered on 
a class that is detected as a mixin.

This copy mechanism is limited to simple columns that have no foreign
keys, as a :class:`ForeignKey` itself contains references to columns
which can't be properly recreated at this level.  For columns that 
have foreign keys, as well as for the variety of mapper-level constructs
that require destination-explicit context, the
:func:`~sqlalchemy.util.classproperty` decorator is provided so that
patterns common to many classes can be defined as callables::

    from sqlalchemy.util import classproperty
    
    class ReferenceAddressMixin(object):
        @classproperty
        def address_id(cls):
            return Column(Integer, ForeignKey('address.id'))
            
    class User(Base, ReferenceAddressMixin):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        
Where above, the ``address_id`` class-level callable is executed at the 
point at which the ``User`` class is constructed, and the declarative
extension can use the resulting :class:`Column` object as returned by
the method without the need to copy it.

Columns generated by :func:`~sqlalchemy.util.classproperty` can also be
referenced by ``__mapper_args__`` to a limited degree, currently 
by ``polymorphic_on`` and ``version_id_col``, by specifying the 
classdecorator itself into the dictionary - the declarative extension
will resolve them at class construction time::

    class MyMixin:
        @classproperty
        def type_(cls):
            return Column(String(50))

        __mapper_args__= {'polymorphic_on':type_}

    class MyModel(Base,MyMixin):
        __tablename__='test'
        id =  Column(Integer, primary_key=True)
    
.. note:: The usage of :func:`~sqlalchemy.util.classproperty` with mixin 
   columns is a new feature as of SQLAlchemy 0.6.2.

Mixing in Relationships
~~~~~~~~~~~~~~~~~~~~~~~

Relationships created by :func:`~sqlalchemy.orm.relationship` are provided
exclusively using the :func:`~sqlalchemy.util.classproperty` approach,
eliminating any ambiguity which could arise when copying a relationship
and its possibly column-bound contents.  Below is an example which 
combines a foreign key column and a relationship so that two classes
``Foo`` and ``Bar`` can both be configured to reference a common
target class via many-to-one::

    class RefTargetMixin(object):
        @classproperty
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))
    
        @classproperty
        def target(cls):
            return relationship("Target")
    
    class Foo(Base, RefTargetMixin):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
    
    class Bar(Base, RefTargetMixin):
        __tablename__ = 'bar'
        id = Column(Integer, primary_key=True)
                    
    class Target(Base):
        __tablename__ = 'target'
        id = Column(Integer, primary_key=True)

:func:`~sqlalchemy.orm.relationship` definitions which require explicit
primaryjoin, order_by etc. expressions should use the string forms 
for these arguments, so that they are evaluated as late as possible.  
To reference the mixin class in these expressions, use the given ``cls``
to get it's name::

    class RefTargetMixin(object):
        @classproperty
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))
        
        @classproperty
        def target(cls):
            return relationship("Target",
                primaryjoin="Target.id==%s.target_id" % cls.__name__
            )

.. note:: The usage of :func:`~sqlalchemy.util.classproperty` with mixin 
   relationships is a new feature as of SQLAlchemy 0.6.2.


Mixing in deferred(), column_property(), etc.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like :func:`~sqlalchemy.orm.relationship`, all :class:`~sqlalchemy.orm.interfaces.MapperProperty`
subclasses such as :func:`~sqlalchemy.orm.deferred`, 
:func:`~sqlalchemy.orm.column_property`, etc. ultimately involve references
to columns, and therefore have the :func:`~sqlalchemy.util.classproperty` requirement so that no reliance on copying is needed::

    class SomethingMixin(object):

        @classproperty
        def dprop(cls):
            return deferred(Column(Integer))

    class Something(Base, SomethingMixin):
        __tablename__ = "something"

.. note:: The usage of :func:`~sqlalchemy.util.classproperty` with mixin 
   mapper properties is a new feature as of SQLAlchemy 0.6.2.


Controlling table inheritance with mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``__tablename__`` attribute in conjunction with the hierarchy of
the classes involved controls what type of table inheritance, if any,
is configured by the declarative extension.

If the ``__tablename__`` is computed by a mixin, you may need to
control which classes get the computed attribute in order to get the
type of table inheritance you require.

For example, if you had a mixin that computes ``__tablename__`` but
where you wanted to use that mixin in a single table inheritance
hierarchy, you can explicitly specify ``__tablename__`` as ``None`` to
indicate that the class should not have a table mapped::

    from sqlalchemy.util import classproperty

    class Tablename:
        @classproperty
        def __tablename__(cls):
            return cls.__name__.lower()

    class Person(Base,Tablename):
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = None
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

Alternatively, you can make the mixin intelligent enough to only
return a ``__tablename__`` in the event that no table is already
mapped in the inheritance hierarchy. To help with this, a
:func:`~sqlalchemy.ext.declarative.has_inherited_table` helper
function is provided that returns ``True`` if a parent class already
has a mapped table. 

As an example, here's a mixin that will only allow single table
inheritance::

    from sqlalchemy.util import classproperty
    from sqlalchemy.ext.declarative import has_inherited_table

    class Tablename:
        @classproperty
        def __tablename__(cls):
            if has_inherited_table(cls):
                return None
            return cls.__name__.lower()

    class Person(Base,Tablename):
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = None
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

If you want to use a similar pattern with a mix of single and joined
table inheritance, you would need a slightly different mixin and use
it on any joined table child classes in addition to their parent
classes::

    from sqlalchemy.util import classproperty
    from sqlalchemy.ext.declarative import has_inherited_table

    class Tablename:
        @classproperty
        def __tablename__(cls):
            if (decl.has_inherited_table(cls) and
                TableNameMixin not in cls.__bases__):
                return None
            return cls.__name__.lower()

    class Person(Base,Tablename):
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        # This is single table inheritance
        __tablename__ = None
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

    class Manager(Person,Tablename):
        # This is joinded table inheritance
        __tablename__ = None
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        preferred_recreation = Column(String(50))

Combining Table/Mapper Arguments from Multiple Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the case of ``__table_args__`` or ``__mapper_args__``, you may want
to combine some parameters from several mixins with those you wish to
define on the class iteself.  The 
:func:`~sqlalchemy.util.classproperty` decorator can be used here
to create user-defined collation routines that pull from multiple
collections::

    from sqlalchemy.util import classproperty

    class MySQLSettings:
        __table_args__ = {'mysql_engine':'InnoDB'}             

    class MyOtherMixin:
        __table_args__ = {'info':'foo'}

    class MyModel(Base,MySQLSettings,MyOtherMixin):
        __tablename__='my_model'

        @classproperty
        def __table_args__(self):
            args = dict()
            args.update(MySQLSettings.__table_args__)
            args.update(MyOtherMixin.__table_args__)
            return args

        id =  Column(Integer, primary_key=True)

Class Constructor
=================

As a convenience feature, the :func:`declarative_base` sets a default
constructor on classes which takes keyword arguments, and assigns them
to the named attributes::

    e = Engineer(primary_language='python')

Sessions
========

Note that ``declarative`` does nothing special with sessions, and is
only intended as an easier way to configure mappers and
:class:`~sqlalchemy.schema.Table` objects.  A typical application
setup using :func:`~sqlalchemy.orm.scoped_session` might look like::  

    engine = create_engine('postgresql://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=engine))
    Base = declarative_base()

Mapped instances then make usage of
:class:`~sqlalchemy.orm.session.Session` in the usual way. 

"""

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.orm import synonym as _orm_synonym, mapper, comparable_property, class_mapper
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.properties import RelationshipProperty, ColumnProperty
from sqlalchemy.orm.util import _is_mapped_class
from sqlalchemy import util, exceptions
from sqlalchemy.sql import util as sql_util


__all__ = 'declarative_base', 'synonym_for', 'comparable_using', 'instrument_declarative'

def instrument_declarative(cls, registry, metadata):
    """Given a class, configure the class declaratively,
    using the given registry, which can be any dictionary, and
    MetaData object. 
    
    """
    if '_decl_class_registry' in cls.__dict__:
        raise exceptions.InvalidRequestError(
                            "Class %r already has been "
                            "instrumented declaratively" % cls)
    cls._decl_class_registry = registry
    cls.metadata = metadata
    _as_declarative(cls, cls.__name__, cls.__dict__)

def has_inherited_table(cls):
    """Given a class, return True if any of the classes it inherits from has a mapped
    table, otherwise return False.
    """
    for class_ in cls.__mro__:
        if getattr(class_,'__table__',None) is not None:
            return True
    return False

def _as_declarative(cls, classname, dict_):

    # dict_ will be a dictproxy, which we can't write to, and we need to!
    dict_ = dict(dict_)

    column_copies = {}
    potential_columns = {}
    
    mapper_args = {}
    table_args = inherited_table_args = None
    tablename = None
    parent_columns = ()
    
    for base in cls.__mro__:
        if _is_mapped_class(base):
            parent_columns = base.__table__.c.keys()
        else:
            for name,obj in vars(base).items():
                if name == '__mapper_args__':
                    if not mapper_args:
                        mapper_args = cls.__mapper_args__
                elif name == '__tablename__':
                    if not tablename:
                        tablename = cls.__tablename__
                elif name == '__table_args__':
                    if not table_args:                        
                        table_args = cls.__table_args__
                        if base is not cls:
                            inherited_table_args = True
                elif base is not cls:
                    # we're a mixin.
                    
                    if isinstance(obj, Column):
                        if obj.foreign_keys:
                            raise exceptions.InvalidRequestError(
                            "Columns with foreign keys to other columns "
                            "must be declared as @classproperty callables "
                            "on declarative mixin classes. ")
                        if name not in dict_ and not (
                                '__table__' in dict_ and 
                                name in dict_['__table__'].c
                                ):
                            potential_columns[name] = \
                                    column_copies[obj] = \
                                    obj.copy()
                            column_copies[obj]._creation_order = \
                                    obj._creation_order
                    elif isinstance(obj, MapperProperty):
                        raise exceptions.InvalidRequestError(
                            "Mapper properties (i.e. deferred,"
                            "column_property(), relationship(), etc.) must "
                            "be declared as @classproperty callables "
                            "on declarative mixin classes.")
                    elif isinstance(obj, util.classproperty):
                        dict_[name] = ret = \
                                column_copies[obj] = getattr(cls, name)
                        if isinstance(ret, (Column, MapperProperty)) and \
                            ret.doc is None:
                            ret.doc = obj.__doc__

    # apply inherited columns as we should
    for k, v in potential_columns.items():
        if tablename or k not in parent_columns:
            dict_[k] = v
            
    if inherited_table_args and not tablename:
        table_args = None

    # make sure that column copies are used rather 
    # than the original columns from any mixins
    for k, v in mapper_args.iteritems():
        mapper_args[k] = column_copies.get(v,v)
    
    cls._decl_class_registry[classname] = cls
    our_stuff = util.OrderedDict()
    for k in dict_:
        value = dict_[k]
        if (isinstance(value, tuple) and len(value) == 1 and
            isinstance(value[0], (Column, MapperProperty))):
            util.warn("Ignoring declarative-like tuple value of attribute "
                      "%s: possibly a copy-and-paste error with a comma "
                      "left at the end of the line?" % k)
            continue
        if not isinstance(value, (Column, MapperProperty)):
            continue
        prop = _deferred_relationship(cls, value)
        our_stuff[k] = prop

    # set up attributes in the order they were created
    our_stuff.sort(key=lambda key: our_stuff[key]._creation_order)

    # extract columns from the class dict
    cols = []
    for key, c in our_stuff.iteritems():
        if isinstance(c, ColumnProperty):
            for col in c.columns:
                if isinstance(col, Column) and col.table is None:
                    _undefer_column_name(key, col)
                    cols.append(col)
        elif isinstance(c, Column):
            _undefer_column_name(key, c)
            cols.append(c)
            # if the column is the same name as the key, 
            # remove it from the explicit properties dict.
            # the normal rules for assigning column-based properties
            # will take over, including precedence of columns
            # in multi-column ColumnProperties.
            if key == c.key:
                del our_stuff[key]

    table = None
    if '__table__' not in dict_:
        if tablename is not None:
            
            if isinstance(table_args, dict):
                args, table_kw = (), table_args
            elif isinstance(table_args, tuple):
                args = table_args[0:-1]
                table_kw = table_args[-1]
                if len(table_args) < 2 or not isinstance(table_kw, dict):
                    raise exceptions.ArgumentError(
                                "Tuple form of __table_args__ is "
                                "(arg1, arg2, arg3, ..., {'kw1':val1, 'kw2':val2, ...})"
                            )
            else:
                args, table_kw = (), {}

            autoload = dict_.get('__autoload__')
            if autoload:
                table_kw['autoload'] = True

            cls.__table__ = table = Table(tablename, cls.metadata,
                                          *(tuple(cols) + tuple(args)), **table_kw)
    else:
        table = cls.__table__
        if cols:
            for c in cols:
                if not table.c.contains_column(c):
                    raise exceptions.ArgumentError(
                        "Can't add additional column %r when specifying __table__" % key
                        )
    
    if 'inherits' not in mapper_args:
        for c in cls.__bases__:
            if _is_mapped_class(c):
                mapper_args['inherits'] = cls._decl_class_registry.get(c.__name__, None)
                break

    if hasattr(cls, '__mapper_cls__'):
        mapper_cls = util.unbound_method_to_callable(cls.__mapper_cls__)
    else:
        mapper_cls = mapper

    if table is None and 'inherits' not in mapper_args:
        raise exceptions.InvalidRequestError(
            "Class %r does not have a __table__ or __tablename__ "
            "specified and does not inherit from an existing table-mapped class." % cls
            )

    elif 'inherits' in mapper_args and not mapper_args.get('concrete', False):
        inherited_mapper = class_mapper(mapper_args['inherits'], compile=False)
        inherited_table = inherited_mapper.local_table
        if 'inherit_condition' not in mapper_args and table is not None:
            # figure out the inherit condition with relaxed rules
            # about nonexistent tables, to allow for ForeignKeys to
            # not-yet-defined tables (since we know for sure that our
            # parent table is defined within the same MetaData)
            mapper_args['inherit_condition'] = sql_util.join_condition(
                mapper_args['inherits'].__table__, table,
                ignore_nonexistent_tables=True)

        if table is None:
            # single table inheritance.
            # ensure no table args
            if table_args:
                raise exceptions.ArgumentError(
                    "Can't place __table_args__ on an inherited class with no table."
                    )
        
            # add any columns declared here to the inherited table.
            for c in cols:
                if c.primary_key:
                    raise exceptions.ArgumentError(
                        "Can't place primary key columns on an inherited class with no table."
                        )
                if c.name in inherited_table.c:
                    raise exceptions.ArgumentError(
                                "Column '%s' on class %s conflicts with existing column '%s'" % 
                                (c, cls, inherited_table.c[c.name])
                            )
                inherited_table.append_column(c)
    
        # single or joined inheritance
        # exclude any cols on the inherited table which are not mapped on the
        # parent class, to avoid
        # mapping columns specific to sibling/nephew classes
        inherited_mapper = class_mapper(mapper_args['inherits'], compile=False)
        inherited_table = inherited_mapper.local_table
        
        if 'exclude_properties' not in mapper_args:
            mapper_args['exclude_properties'] = exclude_properties = \
                set([c.key for c in inherited_table.c
                     if c not in inherited_mapper._columntoproperty])
            exclude_properties.difference_update([c.key for c in cols])
    
    cls.__mapper__ = mapper_cls(cls, table, properties=our_stuff, **mapper_args)

class DeclarativeMeta(type):
    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' in cls.__dict__:
            return type.__init__(cls, classname, bases, dict_)

        _as_declarative(cls, classname, cls.__dict__)
        return type.__init__(cls, classname, bases, dict_)

    def __setattr__(cls, key, value):
        if '__mapper__' in cls.__dict__:
            if isinstance(value, Column):
                _undefer_column_name(key, value)
                cls.__table__.append_column(value)
                cls.__mapper__.add_property(key, value)
            elif isinstance(value, ColumnProperty):
                for col in value.columns:
                    if isinstance(col, Column) and col.table is None:
                        _undefer_column_name(key, col)
                        cls.__table__.append_column(col)
                cls.__mapper__.add_property(key, value)
            elif isinstance(value, MapperProperty):
                cls.__mapper__.add_property(key, _deferred_relationship(cls, value))
            else:
                type.__setattr__(cls, key, value)
        else:
            type.__setattr__(cls, key, value)


class _GetColumns(object):
    def __init__(self, cls):
        self.cls = cls
    def __getattr__(self, key):
        
        mapper = class_mapper(self.cls, compile=False)
        if mapper:
            prop = mapper.get_property(key, raiseerr=False)
            if prop is None:
                raise exceptions.InvalidRequestError(
                                        "Class %r does not have a mapped column named %r"
                                        % (self.cls, key))
            elif not isinstance(prop, ColumnProperty):
                raise exceptions.InvalidRequestError(
                                        "Property %r is not an instance of"
                                        " ColumnProperty (i.e. does not correspond"
                                        " directly to a Column)." % key)
        return getattr(self.cls, key)


def _deferred_relationship(cls, prop):
    def resolve_arg(arg):
        import sqlalchemy
        
        def access_cls(key):
            if key in cls._decl_class_registry:
                return _GetColumns(cls._decl_class_registry[key])
            elif key in cls.metadata.tables:
                return cls.metadata.tables[key]
            else:
                return sqlalchemy.__dict__[key]

        d = util.PopulateDict(access_cls)
        def return_cls():
            try:
                x = eval(arg, globals(), d)
                
                if isinstance(x, _GetColumns):
                    return x.cls
                else:
                    return x
            except NameError, n:
                raise exceptions.InvalidRequestError(
                    "When compiling mapper %s, expression %r failed to locate a name (%r). "
                    "If this is a class name, consider adding this relationship() to the %r "
                    "class after both dependent classes have been defined." % (
                    prop.parent, arg, n.args[0], cls))
        return return_cls

    if isinstance(prop, RelationshipProperty):
        for attr in ('argument', 'order_by', 'primaryjoin', 'secondaryjoin',
                     'secondary', '_foreign_keys', 'remote_side'):
            v = getattr(prop, attr)
            if isinstance(v, basestring):
                setattr(prop, attr, resolve_arg(v))

        if prop.backref and isinstance(prop.backref, tuple):
            key, kwargs = prop.backref
            for attr in ('primaryjoin', 'secondaryjoin', 'secondary',
                         'foreign_keys', 'remote_side', 'order_by'):
               if attr in kwargs and isinstance(kwargs[attr], basestring):
                   kwargs[attr] = resolve_arg(kwargs[attr])


    return prop

def synonym_for(name, map_column=False):
    """Decorator, make a Python @property a query synonym for a column.

    A decorator version of :func:`~sqlalchemy.orm.synonym`.  The function being
    decorated is the 'descriptor', otherwise passes its arguments through
    to synonym()::

      @synonym_for('col')
      @property
      def prop(self):
          return 'special sauce'

    The regular ``synonym()`` is also usable directly in a declarative setting
    and may be convenient for read/write properties::

      prop = synonym('col', descriptor=property(_read_prop, _write_prop))

    """
    def decorate(fn):
        return _orm_synonym(name, map_column=map_column, descriptor=fn)
    return decorate

def comparable_using(comparator_factory):
    """Decorator, allow a Python @property to be used in query criteria.

    This is a  decorator front end to
    :func:`~sqlalchemy.orm.comparable_property` that passes
    through the comparator_factory and the function being decorated::

      @comparable_using(MyComparatorType)
      @property
      def prop(self):
          return 'special sauce'

    The regular ``comparable_property()`` is also usable directly in a
    declarative setting and may be convenient for read/write properties::

      prop = comparable_property(MyComparatorType)

    """
    def decorate(fn):
        return comparable_property(comparator_factory, fn)
    return decorate

def _declarative_constructor(self, **kwargs):
    """A simple constructor that allows initialization from kwargs.

    Sets attributes on the constructed instance using the names and
    values in ``kwargs``.

    Only keys that are present as
    attributes of the instance's class are allowed. These could be,
    for example, any mapped columns or relationships.
    """
    for k in kwargs:
        if not hasattr(type(self), k):
            raise TypeError(
                "%r is an invalid keyword argument for %s" %
                (k, type(self).__name__))
        setattr(self, k, kwargs[k])
_declarative_constructor.__name__ = '__init__'

def declarative_base(bind=None, metadata=None, mapper=None, cls=object,
                     name='Base', constructor=_declarative_constructor,
                     metaclass=DeclarativeMeta):
    """Construct a base class for declarative class definitions.

    The new base class will be given a metaclass that produces
    appropriate :class:`~sqlalchemy.schema.Table` objects and makes
    the appropriate :func:`~sqlalchemy.orm.mapper` calls based on the
    information provided declaratively in the class and any subclasses
    of the class.

    :param bind: An optional
      :class:`~sqlalchemy.engine.base.Connectable`, will be assigned
      the ``bind`` attribute on the :class:`~sqlalchemy.MetaData` 
      instance. 
      

    :param metadata:
      An optional :class:`~sqlalchemy.MetaData` instance.  All
      :class:`~sqlalchemy.schema.Table` objects implicitly declared by
      subclasses of the base will share this MetaData.  A MetaData instance
      will be created if none is provided.  The
      :class:`~sqlalchemy.MetaData` instance will be available via the
      `metadata` attribute of the generated declarative base class.

    :param mapper:
      An optional callable, defaults to :func:`~sqlalchemy.orm.mapper`.  Will be
      used to map subclasses to their Tables.

    :param cls:
      Defaults to :class:`object`.  A type to use as the base for the generated
      declarative base class.  May be a class or tuple of classes.

    :param name:
      Defaults to ``Base``.  The display name for the generated
      class.  Customizing this is not required, but can improve clarity in
      tracebacks and debugging.

    :param constructor:
      Defaults to
      :func:`~sqlalchemy.ext.declarative._declarative_constructor`, an
      __init__ implementation that assigns \**kwargs for declared
      fields and relationships to an instance.  If ``None`` is supplied,
      no __init__ will be provided and construction will fall back to
      cls.__init__ by way of the normal Python semantics.

    :param metaclass:
      Defaults to :class:`DeclarativeMeta`.  A metaclass or __metaclass__
      compatible callable to use as the meta type of the generated
      declarative base class.

    """
    lcl_metadata = metadata or MetaData()
    if bind:
        lcl_metadata.bind = bind

    bases = not isinstance(cls, tuple) and (cls,) or cls
    class_dict = dict(_decl_class_registry=dict(),
                      metadata=lcl_metadata)

    if constructor:
        class_dict['__init__'] = constructor
    if mapper:
        class_dict['__mapper_cls__'] = mapper

    return metaclass(name, bases, class_dict)

def _undefer_column_name(key, column):
    if column.key is None:
        column.key = key
    if column.name is None:
        column.name = key

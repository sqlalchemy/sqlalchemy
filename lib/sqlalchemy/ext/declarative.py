"""A simple declarative layer for SQLAlchemy ORM.

Synopsis
========

SQLAlchemy object-relational configuration involves the usage of :class:`~sqlalchemy.schema.Table`,
:func:`~sqlalchemy.orm.mapper`, and class objects to define the three areas of configuration.
``declarative`` moves these three types of configuration underneath the individual
mapped class.  Regular SQLAlchemy schema and ORM constructs are used in most
cases::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name =  Column(String(50))

Above, the :func:`declarative_base` callable produces a new base class from which
all mapped classes inherit from.  When the class definition is completed, a
new :class:`~sqlalchemy.schema.Table` and :class:`~sqlalchemy.orm.mapper` have been generated, accessible via the
``__table__`` and ``__mapper__`` attributes on the ``SomeClass`` class.

Defining Attributes
===================

:class:`~sqlalchemy.schema.Column` objects may be explicitly named, 
including using a different name than the attribute in which they are associated.
The column will be assigned to the :class:`~sqlalchemy.schema.Table` using the
given name, and mapped to the class using the attribute name::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column("some_table_id", Integer, primary_key=True)
        name = Column("name", String(50))
    
Otherwise, you may omit the names from the Column definitions.  
Declarative will set the ``name`` attribute on the column when the class
is initialized::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))

Attributes may be added to the class after its construction, and they will be
added to the underlying :class:`~sqlalchemy.schema.Table` and :func:`~sqlalchemy.orm.mapper()` definitions as
appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relation(RelatedInfo)

Classes which are mapped explicitly using :func:`~sqlalchemy.orm.mapper()` can interact freely
with declarative classes.   It is recommended, though not required, that all tables 
share the same underlying :class:`~sqlalchemy.schema.MetaData` object, so that 
string-configured :class:`~sqlalchemy.schema.ForeignKey` references can be resolved without issue.

Association of Metadata and Engine
==================================

The :func:`declarative_base` base class contains a :class:`~sqlalchemy.schema.MetaData` object where newly
defined :class:`~sqlalchemy.schema.Table` objects are collected.  This is accessed via the
:class:`~sqlalchemy.schema.MetaData` class level accessor, so to create tables we can say::

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

The :class:`~sqlalchemy.engine.base.Engine` created above may also be directly associated with the
declarative base class using the ``bind`` keyword argument, where it will be
associated with the underlying :class:`~sqlalchemy.schema.MetaData` object and allow SQL operations
involving that metadata and its tables to make use of that engine
automatically::

    Base = declarative_base(bind=create_engine('sqlite://'))

Or, as :class:`~sqlalchemy.schema.MetaData` allows, at any time using the ``bind`` attribute::

    Base.metadata.bind = create_engine('sqlite://')

The :func:`declarative_base` can also receive a pre-created :class:`~sqlalchemy.schema.MetaData` object,
which allows a declarative setup to be associated with an already existing
traditional collection of :class:`~sqlalchemy.schema.Table` objects::

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Configuring Relations
=====================

Relations to other classes are done in the usual way, with the added feature
that the class specified to :func:`~sqlalchemy.orm.relation()` may be a string name.  The "class
registry" associated with ``Base`` is used at mapper compilation time to
resolve the name into the actual class object, which is expected to have been
defined once the mapper configuration is used::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        addresses = relation("Address", backref="user")

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))

Column constructs, since they are just that, are immediately usable, as below
where we define a primary join condition on the ``Address`` class using them::

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relation(User, primaryjoin=user_id == User.id)

In addition to the main argument for :func:`~sqlalchemy.orm.relation`, other arguments
which depend upon the columns present on an as-yet undefined class
may also be specified as strings.  These strings are evaluated as
Python expressions.  The full namespace available within this 
evaluation includes all classes mapped for this declarative base,
as well as the contents of the ``sqlalchemy`` package, including 
expression functions like :func:`~sqlalchemy.sql.expression.desc` and :attr:`~sqlalchemy.sql.expression.func`::

    class User(Base):
        # ....
        addresses = relation("Address", order_by="desc(Address.email)", 
            primaryjoin="Address.user_id==User.id")

As an alternative to string-based attributes, attributes may also be 
defined after all classes have been created.  Just add them to the target
class after the fact::

    User.addresses = relation(Address, primaryjoin=Address.user_id == User.id)

Configuring Many-to-Many Relations
==================================

There's nothing special about many-to-many with declarative.  The ``secondary``
argument to :func:`~sqlalchemy.orm.relation` still requires a :class:`~sqlalchemy.schema.Table` object, not a declarative class.
The :class:`~sqlalchemy.schema.Table` should share the same :class:`~sqlalchemy.schema.MetaData` object used by the declarative base::

    keywords = Table('keywords', Base.metadata,
                        Column('author_id', Integer, ForeignKey('authors.id')),
                        Column('keyword_id', Integer, ForeignKey('keywords.id'))
                )
                
    class Author(Base):
        __tablename__ = 'authors'
        id = Column(Integer, primary_key=True)
        keywords = relation("Keyword", secondary=keywords)

You should generally **not** map a class and also specify its table in a many-to-many
relation, since the ORM may issue duplicate INSERT and DELETE statements.


Defining Synonyms
=================

Synonyms are introduced in :ref:`synonyms`. To define a getter/setter which 
proxies to an underlying attribute, use :func:`~sqlalchemy.orm.synonym` with the 
``descriptor`` argument::

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

For simple getters, the :func:`synonym_for` decorator can be used in conjunction
with ``@property``::

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)

        @synonym_for('_attr')
        @property
        def attr(self):
            return self._some_attr

Similarly, :func:`comparable_using` is a front end for the :func:`~sqlalchemy.orm.comparable_property` 
ORM function::

    class MyClass(Base):
        __tablename__ = 'sometable'

        name = Column('name', String)

        @comparable_using(MyUpperCaseComparator)
        @property
        def uc_name(self):
            return self.name.upper()

Table Configuration
===================

Table arguments other than the name, metadata, and mapped Column arguments 
are specified using the ``__table_args__`` class attribute.   This attribute
accommodates both positional as well as keyword arguments that are normally 
sent to the :class:`~sqlalchemy.schema.Table` constructor.   The attribute can be specified
in one of two forms.  One is as a dictionary::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = {'mysql_engine':'InnoDB'}

The other, a tuple of the form ``(arg1, arg2, ..., {kwarg1:value, ...})``, which 
allows positional arguments to be specified as well (usually constraints)::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                {'autoload':True}
                )

Note that the dictionary is required in the tuple form even if empty.

As an alternative to ``__tablename__``, a direct :class:`~sqlalchemy.schema.Table` 
construct may be used.  The :class:`~sqlalchemy.schema.Column` objects, which 
in this case require their names, will be
added to the mapping just like a regular mapping to a table::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

Mapper Configuration
====================

Mapper arguments are specified using the ``__mapper_args__`` class variable,
which is a dictionary that accepts the same names as the :class:`~sqlalchemy.orm.mapper`
function accepts as keywords::

    class Widget(Base):
        __tablename__ = 'widgets'
        id = Column(Integer, primary_key=True)
        
        __mapper_args__ = {'extension': MyWidgetExtension()}

Inheritance Configuration
=========================

Declarative supports all three forms of inheritance as intuitively
as possible.  The ``inherits`` mapper keyword argument is not needed,
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

Note that above, the ``Engineer.id`` attribute, since it shares the same
attribute name as the ``Person.id`` attribute, will in fact represent the ``people.id``
and ``engineers.id`` columns together, and will render inside a query as ``"people.id"``.
To provide the ``Engineer`` class with an attribute that represents only the
``engineers.id`` column, give it a different attribute name::

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        engineer_id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

Single Table Inheritance
~~~~~~~~~~~~~~~~~~~~~~~~

Single table inheritance is defined as a subclass that does not have its 
own table; you just leave out the ``__table__`` and ``__tablename__`` attributes::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        primary_language = Column(String(50))

When the above mappers are configured, the ``Person`` class is mapped to the ``people``
table *before* the ``primary_language`` column is defined, and this column will not be included
in its own mapping.   When ``Engineer`` then defines the ``primary_language`` 
column, the column is added to the ``people`` table so that it is included in the mapping 
for ``Engineer`` and is also part of the table's full set of columns.   
Columns which are not mapped to ``Person`` are also excluded from any other 
single or joined inheriting classes using the ``exclude_properties`` mapper argument.
Below, ``Manager`` will have all the attributes of ``Person`` and ``Manager`` but *not*
the ``primary_language`` attribute of ``Engineer``::

    class Manager(Person):
        __mapper_args__ = {'polymorphic_identity': 'manager'}
        golf_swing = Column(String(50))

The attribute exclusion logic is provided by the ``exclude_properties`` mapper argument,
and declarative's default behavior can be disabled by passing an explicit 
``exclude_properties`` collection (empty or otherwise) to the ``__mapper_args__``.

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

Usage of an abstract base class is a little less straightforward as it requires 
usage of :func:`~sqlalchemy.orm.util.polymorphic_union`::

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
    
Class Usage
===========

As a convenience feature, the :func:`declarative_base` sets a default
constructor on classes which takes keyword arguments, and assigns them to the
named attributes::

    e = Engineer(primary_language='python')

Note that ``declarative`` has no integration built in with sessions, and is
only intended as an optional syntax for the regular usage of mappers and Table
objects.  A typical application setup using :func:`~sqlalchemy.orm.scoped_session` might look
like::

    engine = create_engine('postgres://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=engine))
    Base = declarative_base()

Mapped instances then make usage of :class:`~sqlalchemy.orm.session.Session` in the usual way.

"""

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.orm import synonym as _orm_synonym, mapper, comparable_property, class_mapper
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.properties import PropertyLoader, ColumnProperty
from sqlalchemy.orm.util import _is_mapped_class
from sqlalchemy import util, exceptions
from sqlalchemy.sql import util as sql_util


__all__ = 'declarative_base', 'synonym_for', 'comparable_using', 'instrument_declarative'

def instrument_declarative(cls, registry, metadata):
    """Given a class, configure the class declaratively,
    using the given registry (any dictionary) and MetaData object.
    This operation does not assume any kind of class hierarchy.
    
    """
    if '_decl_class_registry' in cls.__dict__:
        raise exceptions.InvalidRequestError("Class %r already has been instrumented declaratively" % cls)
    cls._decl_class_registry = registry
    cls.metadata = metadata
    _as_declarative(cls, cls.__name__, cls.__dict__)
    
def _as_declarative(cls, classname, dict_):
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
        prop = _deferred_relation(cls, value)
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
    if '__table__' not in cls.__dict__:
        if '__tablename__' in cls.__dict__:
            tablename = cls.__tablename__
            
            table_args = cls.__dict__.get('__table_args__')
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

            autoload = cls.__dict__.get('__autoload__')
            if autoload:
                table_kw['autoload'] = True

            cls.__table__ = table = Table(tablename, cls.metadata,
                                          *(tuple(cols) + tuple(args)), **table_kw)
    else:
        table = cls.__table__
        if cols:
            for c in cols:
                if not table.c.contains_column(c):
                    raise exceptions.ArgumentError("Can't add additional column %r when specifying __table__" % key)
            
    mapper_args = getattr(cls, '__mapper_args__', {})
    if 'inherits' not in mapper_args:
        for c in cls.__bases__:
            if _is_mapped_class(c):
                mapper_args['inherits'] = cls._decl_class_registry.get(c.__name__, None)
                break

    if hasattr(cls, '__mapper_cls__'):
        mapper_cls = util.unbound_method_to_callable(cls.__mapper_cls__)
    else:
        mapper_cls = mapper

    if not table and 'inherits' not in mapper_args:
        raise exceptions.InvalidRequestError("Class %r does not have a __table__ or __tablename__ "
                    "specified and does not inherit from an existing table-mapped class." % cls)

    elif 'inherits' in mapper_args and not mapper_args.get('concrete', False):
        inherited_mapper = class_mapper(mapper_args['inherits'], compile=False)
        inherited_table = inherited_mapper.local_table
        if 'inherit_condition' not in mapper_args and table:
            # figure out the inherit condition with relaxed rules
            # about nonexistent tables, to allow for ForeignKeys to
            # not-yet-defined tables (since we know for sure that our
            # parent table is defined within the same MetaData)
            mapper_args['inherit_condition'] = sql_util.join_condition(
                mapper_args['inherits'].__table__, table,
                ignore_nonexistent_tables=True)

        if not table:
            # single table inheritance.
            # ensure no table args
            table_args = cls.__dict__.get('__table_args__')
            if table_args is not None:
                raise exceptions.ArgumentError("Can't place __table_args__ on an inherited class with no table.")
        
            # add any columns declared here to the inherited table.
            for c in cols:
                if c.primary_key:
                    raise exceptions.ArgumentError("Can't place primary key columns on an inherited class with no table.")
                inherited_table.append_column(c)
    
        # single or joined inheritance
        # exclude any cols on the inherited table which are not mapped on the parent class, to avoid
        # mapping columns specific to sibling/nephew classes
        inherited_mapper = class_mapper(mapper_args['inherits'], compile=False)
        inherited_table = inherited_mapper.local_table
        
        if 'exclude_properties' not in mapper_args:
            mapper_args['exclude_properties'] = exclude_properties = \
                set([c.key for c in inherited_table.c if c not in inherited_mapper._columntoproperty])
            exclude_properties.difference_update([c.key for c in cols])
    
    cls.__mapper__ = mapper_cls(cls, table, properties=our_stuff, **mapper_args)

class DeclarativeMeta(type):
    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' in cls.__dict__:
            return type.__init__(cls, classname, bases, dict_)
        
        _as_declarative(cls, classname, dict_)
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
                cls.__mapper__.add_property(key, _deferred_relation(cls, value))
            else:
                type.__setattr__(cls, key, value)
        else:
            type.__setattr__(cls, key, value)


class _GetColumns(object):
    def __init__(self, cls):
        self.cls = cls
    def __getattr__(self, key):
        mapper = class_mapper(self.cls, compile=False)
        if not mapper:
            return getattr(self.cls, key)
        else:
            prop = mapper.get_property(key)
            if not isinstance(prop, ColumnProperty):
                raise exceptions.InvalidRequestError("Property %r is not an instance of ColumnProperty (i.e. does not correspnd directly to a Column)." % key)
            return prop.columns[0]


def _deferred_relation(cls, prop):
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
                    "If this is a class name, consider adding this relation() to the %r "
                    "class after both dependent classes have been defined." % (
                    prop.parent, arg, n.args[0], cls))
        return return_cls

    if isinstance(prop, PropertyLoader):
        for attr in ('argument', 'order_by', 'primaryjoin', 'secondaryjoin', 'secondary', '_foreign_keys', 'remote_side'):
            v = getattr(prop, attr)
            if isinstance(v, basestring):
                setattr(prop, attr, resolve_arg(v))

        if prop.backref:
            for attr in ('primaryjoin', 'secondaryjoin', 'secondary', 'foreign_keys', 'remote_side', 'order_by'):
               if attr in prop.backref.kwargs and isinstance(prop.backref.kwargs[attr], basestring):
                   prop.backref.kwargs[attr] = resolve_arg(prop.backref.kwargs[attr])


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

    A decorator front end to :func:`~sqlalchemy.orm.comparable_property`, passes
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

    Sets kwargs on the constructed instance.  Only keys that are present as
    attributes of type(self) are allowed (for example, any mapped column or
    relation).
    
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
                     metaclass=DeclarativeMeta, engine=None):
    """Construct a base class for declarative class definitions.

    The new base class will be given a metaclass that invokes
    :func:`instrument_declarative()` upon each subclass definition, and routes
    later Column- and Mapper-related attribute assignments made on the class
    into Table and Mapper assignments.  

    :param bind: An optional :class:`~sqlalchemy.engine.base.Connectable`, will be assigned 
      the ``bind`` attribute on the :class:`~sqlalchemy.MetaData` instance.
      The `engine` keyword argument is a deprecated synonym for `bind`.

    :param metadata:
      An optional :class:`~sqlalchemy.MetaData` instance.  All :class:`~sqlalchemy.schema.Table` 
      objects implicitly declared by
      subclasses of the base will share this MetaData.  A MetaData instance
      will be create if none is provided.  The MetaData instance will be
      available via the `metadata` attribute of the generated declarative
      base class.

    :param mapper:
      An optional callable, defaults to :func:`~sqlalchemy.orm.mapper`.  Will be
      used to map subclasses to their Tables.

    :param cls:
      Defaults to :class:`object`.  A type to use as the base for the generated
      declarative base class.  May be a type or tuple of types.

    :param name:
      Defaults to ``Base``.  The display name for the generated
      class.  Customizing this is not required, but can improve clarity in
      tracebacks and debugging.

    :param constructor:
      Defaults to declarative._declarative_constructor, an __init__
      implementation that assigns \**kwargs for declared fields and relations
      to an instance.  If ``None`` is supplied, no __init__ will be installed
      and construction will fall back to cls.__init__ with normal Python
      semantics.

    :param metaclass:
      Defaults to :class:`DeclarativeMeta`.  A metaclass or __metaclass__
      compatible callable to use as the meta type of the generated
      declarative base class.

    """
    lcl_metadata = metadata or MetaData()
    if bind or engine:
        lcl_metadata.bind = bind or engine

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

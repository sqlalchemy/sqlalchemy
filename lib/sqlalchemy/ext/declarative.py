"""A simple declarative layer for SQLAlchemy ORM.

SQLAlchemy object-relational configuration involves the usage of Table,
mapper(), and class objects to define the three areas of configuration.
declarative moves these three types of configuration underneath the individual
mapped class.  Regular SQLAlchemy schema and ORM constructs are used in most
cases::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column('id', Integer, primary_key=True)
        name =  Column('name', String(50))

Above, the ``declarative_base`` callable produces a new base class from which
all mapped classes inherit from.  When the class definition is completed, a
new ``Table`` and ``mapper()`` have been generated, accessible via the
``__table__`` and ``__mapper__`` attributes on the ``SomeClass`` class.

You may omit the names from the Column definitions.  Declarative will fill
them in for you::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))

Attributes may be added to the class after its construction, and they will be
added to the underlying ``Table`` and ``mapper()`` definitions as
appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relation(RelatedInfo)

Classes which are mapped explicitly using ``mapper()`` can interact freely
with declarative classes.

The ``declarative_base`` base class contains a ``MetaData`` object where newly
defined ``Table`` objects are collected.  This is accessed via the
``metadata`` class level accessor, so to create tables we can say::

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

The ``Engine`` created above may also be directly associated with the
declarative base class using the ``engine`` keyword argument, where it will be
associated with the underlying ``MetaData`` object and allow SQL operations
involving that metadata and its tables to make use of that engine
automatically::

    Base = declarative_base(engine=create_engine('sqlite://'))

Or, as ``MetaData`` allows, at any time using the ``bind`` attribute::

    Base.metadata.bind = create_engine('sqlite://')

The ``declarative_base`` can also receive a pre-created ``MetaData`` object,
which allows a declarative setup to be associated with an already existing
traditional collection of ``Table`` objects::

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Relations to other classes are done in the usual way, with the added feature
that the class specified to ``relation()`` may be a string name.  The "class
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

    class Address(Base)
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relation(User, primaryjoin=user_id == User.id)

In addition to the main argument for ``relation``, other arguments
which depend upon the columns present on an as-yet undefined class
may also be specified as strings.  These strings are evaluated as
Python expressions.  The full namespace available within this 
evaluation includes all classes mapped for this declarative base,
as well as the contents of the ``sqlalchemy`` package, including 
expression functions like ``desc`` and ``func``::

    class User(Base):
        # ....
        addresses = relation("Address", order_by="desc(Address.email)", 
            primaryjoin="Address.user_id==User.id")

As an alternative to string-based attributes, attributes may also be 
defined after all classes have been created.  Just add them to the target
class after the fact::

    User.addresses = relation(Address, primaryjoin=Address.user_id == User.id)

Synonyms are one area where ``declarative`` needs to slightly change the usual
SQLAlchemy configurational syntax.  To define a getter/setter which proxies to
an underlying attribute, use ``synonym`` with the ``descriptor`` argument::

    class MyClass(Base):
        __tablename__ = 'sometable'

        _attr = Column('attr', String)

        def _get_attr(self):
            return self._some_attr
        def _set_attr(self, attr)
            self._some_attr = attr
        attr = synonym('_attr', descriptor=property(_get_attr, _set_attr))

The above synonym is then usable as an instance attribute as well as a
class-level expression construct::

    x = MyClass()
    x.attr = "some value"
    session.query(MyClass).filter(MyClass.attr == 'some other value').all()

The `synonym_for` decorator can accomplish the same task::

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)

        @synonym_for('_attr')
        @property
        def attr(self):
            return self._some_attr

Similarly, `comparable_using` is a front end for the `comparable_property` ORM function::

    class MyClass(Base):
        __tablename__ = 'sometable'

        name = Column('name', String)

        @comparable_using(MyUpperCaseComparator)
        @property
        def uc_name(self):
            return self.name.upper()

As an alternative to ``__tablename__``, a direct ``Table`` construct may be
used.  The ``Column`` objects, which in this case require their names, will be
added to the mapping just like a regular mapping to a table::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

Other table-based attributes include ``__table_args__``, which is
either a dictionary as in::

    class MyClass(Base)
        __tablename__ = 'sometable'
        __table_args__ = {'mysql_engine':'InnoDB'}
        
or a dictionary-containing tuple in the form 
``(arg1, arg2, ..., {kwarg1:value, ...})``, as in::

    class MyClass(Base)
        __tablename__ = 'sometable'
        __table_args__ = (ForeignKeyConstraint(['id'], ['remote_table.id']), {'autoload':True})

Mapper arguments are specified using the ``__mapper_args__`` class variable.
Note that the column objects declared on the class are immediately usable, as
in this joined-table inheritance example::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column(String(50))
        __mapper_args__ = {'polymorphic_on': discriminator}

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity': 'engineer'}
        id = Column(Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

For single-table inheritance, the ``__tablename__`` and ``__table__`` class
variables are optional on a class when the class inherits from another mapped
class.

As a convenience feature, the ``declarative_base()`` sets a default
constructor on classes which takes keyword arguments, and assigns them to the
named attributes::

    e = Engineer(primary_language='python')

Note that ``declarative`` has no integration built in with sessions, and is
only intended as an optional syntax for the regular usage of mappers and Table
objects.  A typical application setup using ``scoped_session`` might look
like::

    engine = create_engine('postgres://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=engine))
    Base = declarative_base()

Mapped instances then make usage of ``Session`` in the usual way.

"""
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.orm import synonym as _orm_synonym, mapper, comparable_property, class_mapper
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.properties import PropertyLoader, ColumnProperty
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
            else:
                args, table_kw = (), {}

            autoload = cls.__dict__.get('__autoload__')
            if autoload:
                table_kw['autoload'] = True

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
            cls.__table__ = table = Table(tablename, cls.metadata,
                                          *(tuple(cols) + tuple(args)), **table_kw)
    else:
        table = cls.__table__

    mapper_args = getattr(cls, '__mapper_args__', {})
    if 'inherits' not in mapper_args:
        inherits = cls.__mro__[1]
        inherits = cls._decl_class_registry.get(inherits.__name__, None)
        if inherits:
            mapper_args['inherits'] = inherits
            if not mapper_args.get('concrete', False) and table and 'inherit_condition' not in mapper_args:
                # figure out the inherit condition with relaxed rules
                # about nonexistent tables, to allow for ForeignKeys to
                # not-yet-defined tables (since we know for sure that our
                # parent table is defined within the same MetaData)
                mapper_args['inherit_condition'] = sql_util.join_condition(
                    inherits.__table__, table,
                    ignore_nonexistent_tables=True)

    if hasattr(cls, '__mapper_cls__'):
        mapper_cls = util.unbound_method_to_callable(cls.__mapper_cls__)
    else:
        mapper_cls = mapper

    cls.__mapper__ = mapper_cls(cls, table, properties=our_stuff,
                                **mapper_args)

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
            try:
                return _GetColumns(cls._decl_class_registry[key])
            except KeyError:
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
            for attr in ('primaryjoin', 'secondaryjoin'):
               if attr in prop.backref.kwargs and isinstance(prop.backref.kwargs[attr], basestring):
                   prop.backref.kwargs[attr] = resolve_arg(prop.backref.kwargs[attr])


    return prop

def synonym_for(name, map_column=False):
    """Decorator, make a Python @property a query synonym for a column.

    A decorator version of [sqlalchemy.orm#synonym()].  The function being
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

    A decorator front end to [sqlalchemy.orm#comparable_property()], passes
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
    `instrument_declarative()` upon each subclass definition, and routes
    later Column- and Mapper-related attribute assignments made on the class
    into Table and Mapper assignments.  See the `declarative` module
    documentation for examples.

    bind
      An optional `Connectable`, will be assigned to the `metadata.bind`.
      The `engine` keyword argument is a deprecated synonym for `bind`.

    metadata
      An optional `MetaData` instance.  All Tables implicitly declared by
      subclasses of the base will share this MetaData.  A MetaData instance
      will be create if none is provided.  The MetaData instance will be
      available via the `metadata` attribute of the generated declarative
      base class.

    mapper
      An optional callable, defaults to `sqlalchemy.orm.mapper`.  Will be
      used to map subclasses to their Tables.

    cls
      Defaults to `object`.  A type to use as the base for the generated
      declarative base class.  May be a type or tuple of types.

    name
      Defaults to 'Base', Python's internal display name for the generated
      class.  Customizing this is not required, but can improve clarity in
      tracebacks and debugging.

    constructor
      Defaults to declarative._declarative_constructor, an __init__
      implementation that assigns \**kwargs for declared fields and relations
      to an instance.  If `None` is supplied, no __init__ will be installed
      and construction will fall back to cls.__init__ with normal Python
      semantics.

    metaclass
      Defaults to `DeclarativeMeta`.  A metaclass or __metaclass__
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

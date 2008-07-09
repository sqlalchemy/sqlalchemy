"""A simple declarative layer for SQLAlchemy ORM.

SQLAlchemy object-relational configuration involves the usage of Table,
mapper(), and class objects to define the three areas of configuration.
declarative moves these three types of configuration underneath the
individual mapped class.  Regular SQLAlchemy schema and ORM constructs are
used in most cases::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column('id', Integer, primary_key=True)
        name =  Column('name', String(50))

Above, the ``declarative_base`` callable produces a new base class from
which all mapped classes inherit from.  When the class definition is
completed, a new ``Table`` and ``mapper()`` have been generated, accessible
via the ``__table__`` and ``__mapper__`` attributes on the ``SomeClass``
class.

You may omit the names from the Column definitions.  Declarative will fill
them in for you::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))

Attributes may be added to the class after its construction, and they will
be added to the underlying ``Table`` and ``mapper()`` definitions as
appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relation(RelatedInfo)

Classes which are mapped explicitly using ``mapper()`` can interact freely
with declarative classes.  

The ``declarative_base`` base class contains a
``MetaData`` object where newly defined ``Table`` objects are collected.  
This is accessed via the ``metadata`` class level accessor, so to 
create tables we can say::

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

The ``Engine`` created above may also be directly associated with the 
declarative base class using the ``engine`` keyword argument, where it will 
be associated with the underlying ``MetaData`` object and allow SQL 
operations involving that metadata and its tables to make use of that
engine automatically::

    Base = declarative_base(engine=create_engine('sqlite://'))

Or, as ``MetaData`` allows, at any time using the ``bind`` attribute::

    Base.metadata.bind = create_engine('sqlite://')

The ``declarative_base`` can also receive a pre-created ``MetaData``
object, which allows a declarative setup to be associated with an already existing traditional collection of ``Table`` objects::

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Relations to other classes are done in the usual way, with the added feature
that the class specified to ``relation()`` may be a string name.  The "class
registry" associated with ``Base`` is used at mapper compilation time to
resolve the name into the actual class object, which is expected to have
been defined once the mapper configuration is used::

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

Column constructs, since they are just that, are immediately usable, as
below where we define a primary join condition on the ``Address`` class
using them::

    class Address(Base)
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relation(User, primaryjoin=user_id==User.id)

When an explicit join condition or other configuration which depends 
on multiple classes cannot be defined immediately due to some classes
not yet being available, these can be defined after all classes have
been created.  Attributes which are added to the class after
its creation are associated with the Table/mapping in the same
way as if they had been defined inline::

    User.addresses = relation(Address, primaryjoin=Address.user_id==User.id)
 
Synonyms are one area where ``declarative`` needs to slightly change the
usual SQLAlchemy configurational syntax.  To define a getter/setter which
proxies to an underlying attribute, use ``synonym`` with the ``descriptor``
argument::

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

As an alternative to ``__tablename__``, a direct ``Table`` construct may be
used.  The ``Column`` objects, which in this case require their names,
will be added to the mapping just like a regular mapping to a table::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

This is the preferred approach when using reflected tables, as below::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata, autoload=True)

Mapper arguments are specified using the ``__mapper_args__`` class variable.
Note that the column objects declared on the class are immediately usable,
as in this joined-table inheritance example::

    class Person(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True)
        discriminator = Column(String(50))
        __mapper_args__ = {'polymorphic_on':discriminator}

    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity':'engineer'}
        id = Column(Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column(String(50))

For single-table inheritance, the ``__tablename__`` and ``__table__`` class
variables are optional on a class when the class inherits from another
mapped class.

As a convenience feature, the ``declarative_base()`` sets a default
constructor on classes which takes keyword arguments, and assigns them to
the named attributes::

    e = Engineer(primary_language='python')

Note that ``declarative`` has no integration built in with sessions, and is
only intended as an optional syntax for the regular usage of mappers and
Table objects.  A typical application setup using ``scoped_session`` might
look like::

    engine = create_engine('postgres://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(transactional=True, autoflush=False, bind=engine))
    Base = declarative_base()

Mapped instances then make usage of ``Session`` in the usual way.
"""

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.orm import synonym as _orm_synonym, mapper, comparable_property
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.properties import PropertyLoader, ColumnProperty
from sqlalchemy import util, exceptions
from sqlalchemy.sql import util as sql_util


__all__ = ['declarative_base', 'synonym_for', 'comparable_using',
           'declared_synonym']


class DeclarativeMeta(type):
    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' in cls.__dict__:
            return type.__init__(cls, classname, bases, dict_)

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
        our_stuff.sort(lambda x, y: cmp(our_stuff[x]._creation_order, our_stuff[y]._creation_order))

        table = None
        if '__table__' not in cls.__dict__:
            if '__tablename__' in cls.__dict__:
                tablename = cls.__tablename__
                autoload = cls.__dict__.get('__autoload__')
                if autoload:
                    table_kw = {'autoload': True}
                else:
                    table_kw = {}
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
                cls.__table__ = table = Table(tablename, cls.metadata,
                                              *cols, **table_kw)
        else:
            table = cls.__table__

        mapper_args = getattr(cls, '__mapper_args__', {})
        if 'inherits' not in mapper_args:
            inherits = cls.__mro__[1]
            inherits = cls._decl_class_registry.get(inherits.__name__, None)
            if inherits:
                mapper_args['inherits'] = inherits
                if not mapper_args.get('concrete', False) and table:
                    # figure out the inherit condition with relaxed rules about nonexistent tables,
                    # to allow for ForeignKeys to not-yet-defined tables (since we know for sure that our parent
                    # table is defined within the same MetaData)
                    mapper_args['inherit_condition'] = sql_util.join_condition(inherits.__table__, table, ignore_nonexistent_tables=True)
        
        if hasattr(cls, '__mapper_cls__'):
            mapper_cls = util.unbound_method_to_callable(cls.__mapper_cls__)
        else:
            mapper_cls = mapper
        cls.__mapper__ = mapper_cls(cls, table, properties=our_stuff, **mapper_args)
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

def _deferred_relation(cls, prop):
    if isinstance(prop, PropertyLoader) and isinstance(prop.argument, basestring):
        arg = prop.argument
        def return_cls():
            try:
                return cls._decl_class_registry[arg]
            except KeyError:
                raise exceptions.InvalidRequestError("When compiling mapper %s, could not locate a declarative class named %r.  Consider adding this property to the %r class after both dependent classes have been defined." % (prop.parent, arg, prop.parent.class_))
        prop.argument = return_cls

    return prop

def declared_synonym(prop, name):
    """Deprecated.  Use synonym(name, descriptor=prop)."""
    return _orm_synonym(name, descriptor=prop)
declared_synonym = util.deprecated(None, False)(declared_synonym)

def synonym_for(name, map_column=False):
    """Decorator, make a Python @property a query synonym for a column.

    A decorator version of [sqlalchemy.orm#synonym()].  The function being
    decorated is the 'descriptor', otherwise passes its arguments through
    to synonym()::

      @synonym_for('col')
      @property
      def prop(self):
          return 'special sauce'

    The regular ``synonym()`` is also usable directly in a declarative
    setting and may be convenient for read/write properties::

      prop = synonym('col', descriptor=property(_read_prop, _write_prop))

    """
    def decorate(fn):
        return _orm_synonym(name, map_column=map_column, descriptor=fn)
    return decorate


def comparable_using(comparator_factory):
    """Decorator, allow a Python @property to be used in query criteria.

    A decorator front end to [sqlalchemy.orm#comparable_property()], passes
    throgh the comparator_factory and the function being decorated::

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

def declarative_base(engine=None, metadata=None, mapper=None):
    lcl_metadata = metadata or MetaData()
    if engine:
        lcl_metadata.bind = engine
    class Base(object):
        __metaclass__ = DeclarativeMeta
        metadata = lcl_metadata
        if mapper:
            __mapper_cls__ = mapper
        _decl_class_registry = {}
        def __init__(self, **kwargs):
            for k in kwargs:
                if not hasattr(type(self), k):
                    raise TypeError('%r is an invalid keyword argument for %s' %
                                    (k, type(self).__name__))
                setattr(self, k, kwargs[k])
    return Base

def _undefer_column_name(key, column):
    if column.key is None:
        column.key = key
    if column.name is None:
        column.name = key

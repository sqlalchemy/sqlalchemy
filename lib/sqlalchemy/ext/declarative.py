"""A simple declarative layer for SQLAlchemy ORM.

SQLAlchemy object-relational configuration involves the usage of Table,
mapper(), and class objects to define the three areas of configuration.
declarative moves these three types of configuration underneath the 
individual mapped class.   Regular SQLAlchemy schema and ORM 
constructs are used in most cases::

    from sqlalchemy.ext.declarative import declarative_base, declared_synonym
    
    engine = create_engine('sqlite://')
    Base = declarative_base(engine)
    
    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column('id', Integer, primary_key=True)
        name =  Column('name', String(50))

Above, the ``declarative_base`` callable produces a new base class from which all
mapped classes inherit from.  When the class definition is completed, a new 
``Table`` and ``mapper()`` have been generated, accessible via the ``__table__``
and ``__mapper__`` attributes on the ``SomeClass`` class.

Attributes may be added to the class after its construction, and they will
be added to the underlying ``Table`` and ``mapper()`` definitions as appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relation(RelatedInfo)

Classes which are mapped explicitly using ``mapper()`` can interact freely with 
declarative classes.  The ``declarative_base`` base class contains a ``MetaData`` 
object as well as a dictionary of all classes created against the base.  
So to access the above metadata and create tables we can say::

    Base.metadata.create_all()
    
The ``declarative_base`` can also receive a pre-created ``MetaData`` object::

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Relations to other classes are done in the usual way, with the added feature
that the class specified to ``relation()`` may be a string name.  The 
"class registry" associated with ``Base`` is used at mapper compilation time
to resolve the name into the actual class object, which is expected to have been
defined once the mapper configuration is used::

    class User(Base):
        __tablename__ = 'users'

        id = Column('id', Integer, primary_key=True)
        name = Column('name', String(50))
        addresses = relation("Address", backref="user")
    
    class Address(Base):
        __tablename__ = 'addresses'

        id = Column('id', Integer, primary_key=True)
        email = Column('email', String(50))
        user_id = Column('user_id', Integer, ForeignKey('users.id'))

Column constructs, since they are just that, are immediately usable, as
below where we define a primary join condition on the ``Address`` class
using them::

    class Address(Base)
        __tablename__ = 'addresses'

        id = Column('id', Integer, primary_key=True)
        email = Column('email', String(50))
        user_id = Column('user_id', Integer, ForeignKey('users.id'))
        user = relation(User, primaryjoin=user_id==User.id)

Synonyms are one area where ``declarative`` needs to slightly change the usual
SQLAlchemy configurational syntax.  To define a getter/setter which proxies
to an underlying attribute, use ``declared_synonym``::

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)
        
        def _get_attr(self):
            return self._some_attr
        def _set_attr(self, attr)
            self._some_attr = attr
        attr = declared_synonym(property(_get_attr, _set_attr), '_attr')
        
The above synonym is then usable as an instance attribute as well as a class-level
expression construct::

    x = MyClass()
    x.attr = "some value"
    session.query(MyClass).filter(MyClass.attr == 'some other value').all()
        
As an alternative to ``__tablename__``, a direct ``Table`` construct may be used::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

This is the preferred approach when using reflected tables, as below::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata, autoload=True)

Mapper arguments are specified using the ``__mapper_args__`` class variable.  
Note that the column objects declared on the class are immediately usable, as 
in this joined-table inheritance example::

    class Person(Base):
        __tablename__ = 'people'
        id = Column('id', Integer, primary_key=True)
        discriminator = Column('type', String(50))
        __mapper_args__ = {'polymorphic_on':discriminator}
    
    class Engineer(Person):
        __tablename__ = 'engineers'
        __mapper_args__ = {'polymorphic_identity':'engineer'}
        id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        primary_language = Column('primary_language', String(50))
        
For single-table inheritance, the ``__tablename__`` and ``__table__`` class
variables are optional on a class when the class inherits from another mapped
class.

As a convenience feature, the ``declarative_base()`` sets a default constructor
on classes which takes keyword arguments, and assigns them to the named attributes::

    e = Engineer(primary_language='python')

Note that ``declarative`` has no integration built in with sessions, and is only
intended as an optional syntax for the regular usage of mappers and Table objects.
A typical application setup using ``scoped_session`` might look like::

    engine = create_engine('postgres://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(transactional=True, autoflush=False, bind=engine))
    Base = declarative_base()
    
Mapped instances then make usage of ``Session`` in the usual way.

"""
from sqlalchemy.schema import Table, SchemaItem, Column, MetaData
from sqlalchemy.orm import synonym as _orm_synonym, mapper
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.properties import PropertyLoader

__all__ = ['declarative_base', 'declared_synonym']

class DeclarativeMeta(type):
    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' in cls.__dict__:
            return type.__init__(cls, classname, bases, dict_)
        
        cls._decl_class_registry[classname] = cls
        our_stuff = {}
        for k in dict_:
            value = dict_[k]
            if not isinstance(value, (Column, MapperProperty, declared_synonym)):
                continue
            if isinstance(value, declared_synonym):
                value._setup(cls, k, our_stuff)
            else:
                prop = _deferred_relation(cls, value)
                our_stuff[k] = prop
        
        table = None
        if '__table__' not in cls.__dict__:
            if '__tablename__' in cls.__dict__:
                tablename = cls.__tablename__
                autoload = cls.__dict__.get('__autoload__')
                if autoload:
                    table_kw = {'autoload': True}
                else:
                    table_kw = {}
                cls.__table__ = table = Table(tablename, cls.metadata, *[
                    c for c in our_stuff.values() if isinstance(c, Column)
                ], **table_kw)
        else:
            table = cls.__table__
        
        inherits = cls.__mro__[1]
        inherits = cls._decl_class_registry.get(inherits.__name__, None)
        mapper_args = getattr(cls, '__mapper_args__', {})
        
        cls.__mapper__ = mapper(cls, table, inherits=inherits, properties=our_stuff, **mapper_args)
        return type.__init__(cls, classname, bases, dict_)
    
    def __setattr__(cls, key, value):
        if '__mapper__' in cls.__dict__:
            if isinstance(value, Column):
                cls.__table__.append_column(value)
                cls.__mapper__.add_property(key, value)
            elif isinstance(value, MapperProperty):
                cls.__mapper__.add_property(key, _deferred_relation(cls, value))
            elif isinstance(value, declared_synonym):
                value._setup(cls, key, None)
            else:
                type.__setattr__(cls, key, value)
        else:
            type.__setattr__(cls, key, value)

def _deferred_relation(cls, prop):
    if isinstance(prop, PropertyLoader) and isinstance(prop.argument, basestring):
        arg = prop.argument
        def return_cls():
            return cls._decl_class_registry[arg]
        prop.argument = return_cls

    return prop

class declared_synonym(object):
    def __init__(self, prop, name, mapperprop=None):
        self.prop = prop
        self.name = name
        self.mapperprop = mapperprop
        
    def _setup(self, cls, key, init_dict):
        prop = self.mapperprop or getattr(cls, self.name)
        prop = _deferred_relation(cls, prop)
        setattr(cls, key, self.prop)
        if init_dict is not None:
            init_dict[self.name] = prop
            init_dict[key] = _orm_synonym(self.name)
        else:
            setattr(cls, self.name, prop)
            setattr(cls, key, _orm_synonym(self.name))
        
        
def declarative_base(engine=None, metadata=None):
    lcl_metadata = metadata or MetaData()
    class Base(object):
        __metaclass__ = DeclarativeMeta
        metadata = lcl_metadata
        if engine:
            metadata.bind = engine
        _decl_class_registry = {}
        def __init__(self, **kwargs):
            for k in kwargs:
                if not hasattr(type(self), k):
                    raise TypeError('%r is an invalid keyword argument for %s' %
                                    (k, type(self).__name__))
                setattr(self, k, kwargs[k])
    return Base


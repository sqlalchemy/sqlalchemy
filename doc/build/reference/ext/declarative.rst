declarative
===========

:author: Mike Bayer
:version: 0.4.4 or greater

``declarative`` intends to be a fully featured replacement for the very old ``activemapper`` extension.  Its goal is to redefine the organization of class, ``Table``, and ``mapper()`` constructs such that they can all be defined "at once" underneath a class declaration.   Unlike ``activemapper``, it does not redefine normal SQLAlchemy configurational semantics - regular ``Column``, ``relation()`` and other schema or ORM constructs are used in almost all cases.

``declarative`` is a so-called "micro declarative layer"; it does not generate table or column names and requires almost as fully verbose a configuration as that of straight tables and mappers.  As an alternative, the `Elixir <http://elixir.ematia.de/>`_ project is a full community-supported declarative layer for SQLAlchemy, and is recommended for its active-record-like semantics, its convention-based configuration, and plugin capabilities.

SQLAlchemy object-relational configuration involves the usage of Table, mapper(), and class objects to define the three areas of configuration.
declarative moves these three types of configuration underneath the individual mapped class. Regular SQLAlchemy schema and ORM constructs are used
in most cases:

.. sourcecode:: python+sql

    from sqlalchemy.ext.declarative import declarative_base
    
    Base = declarative_base()
    
    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column('id', Integer, primary_key=True)
        name =  Column('name', String(50))

Above, the ``declarative_base`` callable produces a new base class from which all mapped classes inherit from. When the class definition is
completed, a new ``Table`` and ``mapper()`` have been generated, accessible via the ``__table__`` and ``__mapper__`` attributes on the
``SomeClass`` class.

You may omit the names from the Column definitions.  Declarative will fill
them in for you:

.. sourcecode:: python+sql

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))

Attributes may be added to the class after its construction, and they will be added to the underlying ``Table`` and ``mapper()`` definitions as
appropriate:

.. sourcecode:: python+sql

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relation(RelatedInfo)

Classes which are mapped explicitly using ``mapper()`` can interact freely with declarative classes. 

The ``declarative_base`` base class contains a ``MetaData`` object where newly defined ``Table`` objects are collected.  This is accessed via the ````metadata```` class level accessor, so to create tables we can say:

.. sourcecode:: python+sql

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

The ``Engine`` created above may also be directly associated with the declarative base class using the ``bind`` keyword argument, where it will be associated with the underlying ``MetaData`` object and allow SQL operations involving that metadata and its tables to make use of that engine automatically:

.. sourcecode:: python+sql

    Base = declarative_base(bind=create_engine('sqlite://'))

Or, as ``MetaData`` allows, at any time using the ``bind`` attribute:

.. sourcecode:: python+sql

    Base.metadata.bind = create_engine('sqlite://')
 
The ``declarative_base`` can also receive a pre-created ``MetaData`` object, which allows a declarative setup to be associated with an already existing traditional collection of ``Table`` objects:

.. sourcecode:: python+sql

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)

Relations to other classes are done in the usual way, with the added feature that the class specified to ``relation()`` may be a string name. The
"class registry" associated with ``Base`` is used at mapper compilation time to resolve the name into the actual class object, which is expected to
have been defined once the mapper configuration is used:

.. sourcecode:: python+sql

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

Column constructs, since they are just that, are immediately usable, as below where we define a primary join condition on the ``Address`` class
using them:

.. sourcecode:: python+sql

    class Address(Base)
        __tablename__ = 'addresses'

        id = Column('id', Integer, primary_key=True)
        email = Column('email', String(50))
        user_id = Column('user_id', Integer, ForeignKey('users.id'))
        user = relation(User, primaryjoin=user_id==User.id)

In addition to the main argument for ``relation``, other arguments
which depend upon the columns present on an as-yet undefined class
may also be specified as strings.  These strings are evaluated as
Python expressions.  The full namespace available within this 
evaluation includes all classes mapped for this declarative base,
as well as the contents of the ``sqlalchemy`` package, including 
expression functions like ``desc`` and ``func``:

.. sourcecode:: python+sql

    class User(Base):
        # ....
        addresses = relation("Address", order_by="desc(Address.email)", 
            primaryjoin="Address.user_id==User.id")

As an alternative to string-based attributes, attributes may also be 
defined after all classes have been created.  Just add them to the target
class after the fact:

.. sourcecode:: python+sql

    User.addresses = relation(Address, primaryjoin=Address.user_id==User.id)

Synonyms are one area where ``declarative`` needs to slightly change the usual SQLAlchemy configurational syntax. To define a
getter/setter which proxies to an underlying attribute, use ``synonym`` with the ``descriptor`` argument:

.. sourcecode:: python+sql

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)
        
        def _get_attr(self):
            return self._some_attr
        def _set_attr(self, attr):
            self._some_attr = attr
        attr = synonym('_attr', descriptor=property(_get_attr, _set_attr))
        
The above synonym is then usable as an instance attribute as well as a class-level expression construct:

.. sourcecode:: python+sql

    x = MyClass()
    x.attr = "some value"
    session.query(MyClass).filter(MyClass.attr == 'some other value').all()

The ``synonym_for`` decorator can accomplish the same task:

.. sourcecode:: python+sql

    class MyClass(Base):
        __tablename__ = 'sometable'
        
        _attr = Column('attr', String)

        @synonym_for('_attr')
        @property
        def attr(self):
            return self._some_attr

Similarly, ``comparable_using`` is a front end for the ``comparable_property`` ORM function:

.. sourcecode:: python+sql

    class MyClass(Base):
        __tablename__ = 'sometable'

        name = Column('name', String)

        @comparable_using(MyUpperCaseComparator)
        @property
        def uc_name(self):
            return self.name.upper()

As an alternative to ``__tablename__``, a direct ``Table`` construct may be used.  The ``Column`` objects, which in this case require their names, will be added to the mapping just like a regular mapping to a table:

.. sourcecode:: python+sql

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

Other table-based attributes include ``__table_args__``, which is
either a dictionary as in:

.. sourcecode:: python+sql

    class MyClass(Base)
        __tablename__ = 'sometable'
        __table_args__ = {'mysql_engine':'InnoDB'}

or a dictionary-containing tuple in the form 
``(arg1, arg2, ..., {kwarg1:value, ...})``, as in:

.. sourcecode:: python+sql

    class MyClass(Base)
        __tablename__ = 'sometable'
        __table_args__ = (ForeignKeyConstraint(['id'], ['remote_table.id']), {'autoload':True})

Mapper arguments are specified using the ``__mapper_args__`` class variable. Note that the column objects declared on the class are immediately
usable, as in this joined-table inheritance example:

.. sourcecode:: python+sql

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
        
For single-table inheritance, the ``__tablename__`` and ``__table__`` class variables are optional on a class when the class inherits from another
mapped class.

As a convenience feature, the ``declarative_base()`` sets a default constructor on classes which takes keyword arguments, and assigns them to the
named attributes:

.. sourcecode:: python+sql

    e = Engineer(primary_language='python')

Note that ``declarative`` has no integration built in with sessions, and is only intended as an optional syntax for the regular usage of mappers
and Table objects. A typical application setup using ``scoped_session`` might look like:

.. sourcecode:: python+sql

    engine = create_engine('postgres://scott:tiger@localhost/test')
    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base = declarative_base()

Mapped instances then make usage of ``Session`` in the usual way.

.. automodule:: sqlalchemy.ext.declarative
   :members:
   :undoc-members:

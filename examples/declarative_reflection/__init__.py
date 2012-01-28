"""
Illustrates how to mix table reflection with Declarative, such that
the reflection process itself can take place **after** all classes
are defined.  Declarative classes can also override column
definitions loaded from the database.

At the core of this example is the ability to change how Declarative
assigns mappings to classes.   The ``__mapper_cls__`` special attribute
is overridden to provide a function that gathers mapping requirements
as they are established, without actually creating the mapping.
Then, a second class-level method ``prepare()`` is used to iterate
through all mapping configurations collected, reflect the tables
named within and generate the actual mappers.

The example is new in 0.7.5 and makes usage of the new
``autoload_replace`` flag on :class:`.Table` to allow declared
classes to override reflected columns.

Usage example::

    Base = declarative_base(cls=DeclarativeReflectedBase)

    class Foo(Base):
        __tablename__ = 'foo'
        bars = relationship("Bar")

    class Bar(Base):
        __tablename__ = 'bar'

        # illustrate overriding of "bar.foo_id" to have 
        # a foreign key constraint otherwise not
        # reflected, such as when using MySQL
        foo_id = Column(Integer, ForeignKey('foo.id'))

    Base.prepare(e)

    s = Session(e)

    s.add_all([
        Foo(bars=[Bar(data='b1'), Bar(data='b2')], data='f1'),
        Foo(bars=[Bar(data='b3'), Bar(data='b4')], data='f2')
    ])
    s.commit()
 

"""

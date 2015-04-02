.. _declarative_table_args:

===================
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

The other, a tuple, where each argument is positional
(usually constraints)::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                )

Keyword arguments can be specified with the above form by
specifying the last argument as a dictionary::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                {'autoload':True}
                )

Using a Hybrid Approach with __table__
=======================================

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

``__table__`` provides a more focused point of control for establishing
table metadata, while still getting most of the benefits of using declarative.
An application that uses reflection might want to load table metadata elsewhere
and pass it to declarative classes::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()
    Base.metadata.reflect(some_engine)

    class User(Base):
        __table__ = metadata.tables['user']

    class Address(Base):
        __table__ = metadata.tables['address']

Some configuration schemes may find it more appropriate to use ``__table__``,
such as those which already take advantage of the data-driven nature of
:class:`.Table` to customize and/or automate schema definition.

Note that when the ``__table__`` approach is used, the object is immediately
usable as a plain :class:`.Table` within the class declaration body itself,
as a Python class is only another syntactical block.  Below this is illustrated
by using the ``id`` column in the ``primaryjoin`` condition of a
:func:`.relationship`::

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

        widgets = relationship(Widget,
                    primaryjoin=Widget.myclass_id==__table__.c.id)

Similarly, mapped attributes which refer to ``__table__`` can be placed inline,
as below where we assign the ``name`` column to the attribute ``_name``,
generating a synonym for ``name``::

    from sqlalchemy.ext.declarative import synonym_for

    class MyClass(Base):
        __table__ = Table('my_table', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )

        _name = __table__.c.name

        @synonym_for("_name")
        def name(self):
            return "Name: %s" % _name

Using Reflection with Declarative
=================================

It's easy to set up a :class:`.Table` that uses ``autoload=True``
in conjunction with a mapped class::

    class MyClass(Base):
        __table__ = Table('mytable', Base.metadata,
                        autoload=True, autoload_with=some_engine)

However, one improvement that can be made here is to not
require the :class:`.Engine` to be available when classes are
being first declared.   To achieve this, use the
:class:`.DeferredReflection` mixin, which sets up mappings
only after a special ``prepare(engine)`` step is called::

    from sqlalchemy.ext.declarative import declarative_base, DeferredReflection

    Base = declarative_base(cls=DeferredReflection)

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

.. versionadded:: 0.8
   Added :class:`.DeferredReflection`.

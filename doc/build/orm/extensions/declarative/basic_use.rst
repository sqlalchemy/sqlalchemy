=========
Basic Use
=========

SQLAlchemy object-relational configuration involves the
combination of :class:`.Table`, :func:`.mapper`, and class
objects to define a mapped class.
:mod:`~sqlalchemy.ext.declarative` allows all three to be
expressed at once within the class declaration. As much as
possible, regular SQLAlchemy schema and ORM constructs are
used directly, so that configuration between "classical" ORM
usage and declarative remain highly similar.

As a simple example::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)
        name =  Column(String(50))

Above, the :func:`declarative_base` callable returns a new base class from
which all mapped classes should inherit. When the class definition is
completed, a new :class:`.Table` and :func:`.mapper` will have been generated.

The resulting table and mapper are accessible via
``__table__`` and ``__mapper__`` attributes on the
``SomeClass`` class::

    # access the mapped Table
    SomeClass.__table__

    # access the Mapper
    SomeClass.__mapper__

Defining Attributes
===================

In the previous example, the :class:`.Column` objects are
automatically named with the name of the attribute to which they are
assigned.

To name columns explicitly with a name distinct from their mapped attribute,
just give the column a name.  Below, column "some_table_id" is mapped to the
"id" attribute of `SomeClass`, but in SQL will be represented as
"some_table_id"::

    class SomeClass(Base):
        __tablename__ = 'some_table'
        id = Column("some_table_id", Integer, primary_key=True)

Attributes may be added to the class after its construction, and they will be
added to the underlying :class:`.Table` and
:func:`.mapper` definitions as appropriate::

    SomeClass.data = Column('data', Unicode)
    SomeClass.related = relationship(RelatedInfo)

Classes which are constructed using declarative can interact freely
with classes that are mapped explicitly with :func:`.mapper`.

It is recommended, though not required, that all tables
share the same underlying :class:`~sqlalchemy.schema.MetaData` object,
so that string-configured :class:`~sqlalchemy.schema.ForeignKey`
references can be resolved without issue.

Accessing the MetaData
=======================

The :func:`declarative_base` base class contains a
:class:`.MetaData` object where newly defined
:class:`.Table` objects are collected. This object is
intended to be accessed directly for
:class:`.MetaData`-specific operations. Such as, to issue
CREATE statements for all tables::

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

:func:`declarative_base` can also receive a pre-existing
:class:`.MetaData` object, which allows a
declarative setup to be associated with an already
existing traditional collection of :class:`~sqlalchemy.schema.Table`
objects::

    mymetadata = MetaData()
    Base = declarative_base(metadata=mymetadata)


Class Constructor
=================

As a convenience feature, the :func:`declarative_base` sets a default
constructor on classes which takes keyword arguments, and assigns them
to the named attributes::

    e = Engineer(primary_language='python')

Mapper Configuration
====================

Declarative makes use of the :func:`~.orm.mapper` function internally
when it creates the mapping to the declared table.   The options
for :func:`~.orm.mapper` are passed directly through via the
``__mapper_args__`` class attribute.  As always, arguments which reference
locally mapped columns can reference them directly from within the
class declaration::

    from datetime import datetime

    class Widget(Base):
        __tablename__ = 'widgets'

        id = Column(Integer, primary_key=True)
        timestamp = Column(DateTime, nullable=False)

        __mapper_args__ = {
                        'version_id_col': timestamp,
                        'version_id_generator': lambda v:datetime.now()
                    }


.. _declarative_sql_expressions:

Defining SQL Expressions
========================

See :ref:`mapper_sql_expressions` for examples on declaratively
mapping attributes to SQL expressions.


=================================
Additional Persistence Techniques
=================================

.. _flush_embedded_sql_expressions:

Embedding SQL Insert/Update Expressions into a Flush
=====================================================

This feature allows the value of a database column to be set to a SQL
expression instead of a literal value. It's especially useful for atomic
updates, calling stored procedures, etc. All you do is assign an expression to
an attribute::

    class SomeClass(object):
        pass
    mapper(SomeClass, some_table)

    someobject = session.query(SomeClass).get(5)

    # set 'value' attribute to a SQL expression adding one
    someobject.value = some_table.c.value + 1

    # issues "UPDATE some_table SET value=value+1"
    session.commit()

This technique works both for INSERT and UPDATE statements. After the
flush/commit operation, the ``value`` attribute on ``someobject`` above is
expired, so that when next accessed the newly generated value will be loaded
from the database.

.. _session_sql_expressions:

Using SQL Expressions with Sessions
====================================

SQL expressions and strings can be executed via the
:class:`~sqlalchemy.orm.session.Session` within its transactional context.
This is most easily accomplished using the
:meth:`~.Session.execute` method, which returns a
:class:`~sqlalchemy.engine.ResultProxy` in the same manner as an
:class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection`::

    Session = sessionmaker(bind=engine)
    session = Session()

    # execute a string statement
    result = session.execute("select * from table where id=:id", {'id':7})

    # execute a SQL expression construct
    result = session.execute(select([mytable]).where(mytable.c.id==7))

The current :class:`~sqlalchemy.engine.Connection` held by the
:class:`~sqlalchemy.orm.session.Session` is accessible using the
:meth:`~.Session.connection` method::

    connection = session.connection()

The examples above deal with a :class:`~sqlalchemy.orm.session.Session` that's
bound to a single :class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection`. To execute statements using a
:class:`~sqlalchemy.orm.session.Session` which is bound either to multiple
engines, or none at all (i.e. relies upon bound metadata), both
:meth:`~.Session.execute` and
:meth:`~.Session.connection` accept a ``mapper`` keyword
argument, which is passed a mapped class or
:class:`~sqlalchemy.orm.mapper.Mapper` instance, which is used to locate the
proper context for the desired engine::

    Session = sessionmaker()
    session = Session()

    # need to specify mapper or class when executing
    result = session.execute("select * from table where id=:id", {'id':7}, mapper=MyMappedClass)

    result = session.execute(select([mytable], mytable.c.id==7), mapper=MyMappedClass)

    connection = session.connection(MyMappedClass)

.. _session_partitioning:

Partitioning Strategies
=======================

Simple Vertical Partitioning
----------------------------

Vertical partitioning places different kinds of objects, or different tables,
across multiple databases::

    engine1 = create_engine('postgresql://db1')
    engine2 = create_engine('postgresql://db2')

    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

    session = Session()

Above, operations against either class will make usage of the :class:`.Engine`
linked to that class.   Upon a flush operation, similar rules take place
to ensure each class is written to the right database.

The transactions among the multiple databases can optionally be coordinated
via two phase commit, if the underlying backend supports it.  See
:ref:`session_twophase` for an example.

Custom Vertical Partitioning
----------------------------

More comprehensive rule-based class-level partitioning can be built by
overriding the :meth:`.Session.get_bind` method.   Below we illustrate
a custom :class:`.Session` which delivers the following rules:

1. Flush operations are delivered to the engine named ``master``.

2. Operations on objects that subclass ``MyOtherClass`` all
   occur on the ``other`` engine.

3. Read operations for all other classes occur on a random
   choice of the ``slave1`` or ``slave2`` database.

::

    engines = {
        'master':create_engine("sqlite:///master.db"),
        'other':create_engine("sqlite:///other.db"),
        'slave1':create_engine("sqlite:///slave1.db"),
        'slave2':create_engine("sqlite:///slave2.db"),
    }

    from sqlalchemy.orm import Session, sessionmaker
    import random

    class RoutingSession(Session):
        def get_bind(self, mapper=None, clause=None):
            if mapper and issubclass(mapper.class_, MyOtherClass):
                return engines['other']
            elif self._flushing:
                return engines['master']
            else:
                return engines[
                    random.choice(['slave1','slave2'])
                ]

The above :class:`.Session` class is plugged in using the ``class_``
argument to :class:`.sessionmaker`::

    Session = sessionmaker(class_=RoutingSession)

This approach can be combined with multiple :class:`.MetaData` objects,
using an approach such as that of using the declarative ``__abstract__``
keyword, described at :ref:`declarative_abstract`.

Horizontal Partitioning
-----------------------

Horizontal partitioning partitions the rows of a single table (or a set of
tables) across multiple databases.

See the "sharding" example: :ref:`examples_sharding`.


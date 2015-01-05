=================
Types of Mappings
=================

Modern SQLAlchemy features two distinct styles of mapper configuration.
The "Classical" style is SQLAlchemy's original mapping API, whereas
"Declarative" is the richer and more succinct system that builds on top
of "Classical".   Both styles may be used interchangeably, as the end
result of each is exactly the same - a user-defined class mapped by the
:func:`.mapper` function onto a selectable unit, typically a :class:`.Table`.

Declarative Mapping
===================

The *Declarative Mapping* is the typical way that
mappings are constructed in modern SQLAlchemy.
Making use of the :ref:`declarative_toplevel`
system, the components of the user-defined class as well as the
:class:`.Table` metadata to which the class is mapped are defined
at once::

    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, ForeignKey

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

Above, a basic single-table mapping with four columns.   Additional
attributes, such as relationships to other mapped classes, are also
declared inline within the class definition::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

        addresses = relationship("Address", backref="user", order_by="Address.id")

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey('user.id'))
        email_address = Column(String)

The declarative mapping system is introduced in the
:ref:`ormtutorial_toplevel`.  For additional details on how this system
works, see :ref:`declarative_toplevel`.

.. _classical_mapping:

Classical Mappings
==================

A *Classical Mapping* refers to the configuration of a mapped class using the
:func:`.mapper` function, without using the Declarative system.  This is
SQLAlchemy's original class mapping API, and is still the base mapping
system provided by the ORM.

In "classical" form, the table metadata is created separately with the
:class:`.Table` construct, then associated with the ``User`` class via
the :func:`.mapper` function::

    from sqlalchemy import Table, MetaData, Column, Integer, String, ForeignKey
    from sqlalchemy.orm import mapper

    metadata = MetaData()

    user = Table('user', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(50)),
                Column('fullname', String(50)),
                Column('password', String(12))
            )

    class User(object):
        def __init__(self, name, fullname, password):
            self.name = name
            self.fullname = fullname
            self.password = password

    mapper(User, user)

Information about mapped attributes, such as relationships to other classes, are provided
via the ``properties`` dictionary.  The example below illustrates a second :class:`.Table`
object, mapped to a class called ``Address``, then linked to ``User`` via :func:`.relationship`::

    address = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String(50))
                )

    mapper(User, user, properties={
        'addresses' : relationship(Address, backref='user', order_by=address.c.id)
    })

    mapper(Address, address)

When using classical mappings, classes must be provided directly without the benefit
of the "string lookup" system provided by Declarative.  SQL expressions are typically
specified in terms of the :class:`.Table` objects, i.e. ``address.c.id`` above
for the ``Address`` relationship, and not ``Address.id``, as ``Address`` may not
yet be linked to table metadata, nor can we specify a string here.

Some examples in the documentation still use the classical approach, but note that
the classical as well as Declarative approaches are **fully interchangeable**.  Both
systems ultimately create the same configuration, consisting of a :class:`.Table`,
user-defined class, linked together with a :func:`.mapper`.  When we talk about
"the behavior of :func:`.mapper`", this includes when using the Declarative system
as well - it's still used, just behind the scenes.

.. _orm_declarative_styles_toplevel:

==========================
Declarative Mapping Styles
==========================

As introduced at :ref:`orm_declarative_mapping`, the **Declarative Mapping** is
the typical way that mappings are constructed in modern SQLAlchemy.   This
section will provide an overview of forms that may be used for Declarative
mapper configuration.


.. _orm_declarative_generated_base_class:

Using a Generated Base Class
----------------------------

The most common approach is to generate a "base" class using the
:func:`_orm.declarative_base` function::

    from sqlalchemy.orm import declarative_base

    # declarative base class
    Base = declarative_base()

The declarative base class may also be created from an existing
:class:`_orm.registry`, by using the :meth:`_orm.registry.generate_base`
method::

    from sqlalchemy.orm import registry

    reg = registry()

    # declarative base class
    Base = reg.generate_base()

With the declarative base class, new mapped classes are declared as subclasses
of the base::

    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.orm import declarative_base

    # declarative base class
    Base = declarative_base()


    # an example mapping using the base
    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        nickname = Column(String)

Above, the :func:`_orm.declarative_base` function returns a new base class from
which new classes to be mapped may inherit from, as above a new mapped
class ``User`` is constructed.

For each subclass constructed, the body of the class then follows the
declarative mapping approach which defines both a :class:`_schema.Table`
as well as a :class:`_orm.Mapper` object behind the scenes which comprise
a full mapping.

.. seealso::

    :ref:`orm_declarative_table_config_toplevel`

    :ref:`orm_declarative_mapper_config_toplevel`


.. _orm_explicit_declarative_base:

Creating an Explicit Base Non-Dynamically (for use with mypy, similar)
----------------------------------------------------------------------

SQLAlchemy includes a :ref:`Mypy plugin <mypy_toplevel>` that automatically
accommodates for the dynamically generated ``Base`` class delivered by
SQLAlchemy functions like :func:`_orm.declarative_base`. For the **SQLAlchemy
1.4 series only**, this plugin works along with a new set of typing stubs
published at `sqlalchemy2-stubs <https://pypi.org/project/sqlalchemy2-stubs>`_.

When this plugin is not in use, or when using other :pep:`484` tools which
may not know how to interpret this class, the declarative base class may
be produced in a fully explicit fashion using the
:class:`_orm.DeclarativeMeta` directly as follows::

    from sqlalchemy.orm import registry
    from sqlalchemy.orm.decl_api import DeclarativeMeta

    mapper_registry = registry()


    class Base(metaclass=DeclarativeMeta):
        __abstract__ = True

        registry = mapper_registry
        metadata = mapper_registry.metadata

        __init__ = mapper_registry.constructor

The above ``Base`` is equivalent to one created using the
:meth:`_orm.registry.generate_base` method and will be fully understood by
type analysis tools without the use of plugins.

.. seealso::

    :ref:`mypy_toplevel` - background on the Mypy plugin which applies the
    above structure automatically when running Mypy.


.. _orm_declarative_decorator:

Declarative Mapping using a Decorator (no declarative base)
------------------------------------------------------------

As an alternative to using the "declarative base" class is to apply
declarative mapping to a class explicitly, using either an imperative technique
similar to that of a "classical" mapping, or more succinctly by using
a decorator.  The :meth:`_orm.registry.mapped` function is a class decorator
that can be applied to any Python class with no hierarchy in place.  The
Python class otherwise is configured in declarative style normally::

    from sqlalchemy import Column, ForeignKey, Integer, String, Text
    from sqlalchemy.orm import registry, relationship

    mapper_registry = registry()


    @mapper_registry.mapped
    class User:
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", back_populates="user")


    @mapper_registry.mapped
    class Address:
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey("user.id"))
        email_address = Column(String)

        user = relationship("User", back_populates="addresses")

Above, the same :class:`_orm.registry` that we'd use to generate a declarative
base class via its :meth:`_orm.registry.generate_base` method may also apply
a declarative-style mapping to a class without using a base.   When using
the above style, the mapping of a particular class will **only** proceed
if the decorator is applied to that class directly.   For inheritance
mappings, the decorator should be applied to each subclass::

    from sqlalchemy.orm import registry

    mapper_registry = registry()


    @mapper_registry.mapped
    class Person:
        __tablename__ = "person"

        person_id = Column(Integer, primary_key=True)
        type = Column(String, nullable=False)

        __mapper_args__ = {
            "polymorphic_on": type,
            "polymorphic_identity": "person",
        }


    @mapper_registry.mapped
    class Employee(Person):
        __tablename__ = "employee"

        person_id = Column(ForeignKey("person.person_id"), primary_key=True)

        __mapper_args__ = {
            "polymorphic_identity": "employee",
        }

Both the "declarative table" and "imperative table" styles of declarative
mapping may be used with the above mapping style.

The decorator form of mapping is particularly useful when combining a
SQLAlchemy declarative mapping with other forms of class declaration, notably
the Python ``dataclasses`` module.  See the next section.


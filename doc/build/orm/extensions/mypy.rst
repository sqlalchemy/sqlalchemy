.. _mypy_toplevel:

Mypy  / Pep-484 Support for ORM Mappings
========================================

Support for :pep:`484` typing annotations as well as the
MyPy_ type checking tool when using SQLAlchemy
:ref:`declarative <orm_declarative_mapper_config_toplevel>` mappings
that refer to the :class:`_schema.Column` object directly, rather than
the :func:`_orm.mapped_column` construct introduced in SQLAlchemy 2.0.

.. topic:: SQLAlchemy Mypy Plugin Status Update

   **Updated December 2022**

   For SQLAlchemy 2.0, the Mypy plugin continues to work at the level at which
   it reached in the SQLAlchemy 1.4 release.  However, SQLAlchemy 2.0,
   when released, will feature an
   :ref:`all new typing system <whatsnew_20_orm_declarative_typing>`
   for ORM Declarative models that removes the need for the Mypy plugin and
   delivers much more consistent behavior with generally superior capabilities.
   Note that this new capability is **not
   part of SQLAlchemy 1.4, it is only in SQLAlchemy 2.0, which is out with beta
   releases as of December 2022**.

   The SQLAlchemy Mypy plugin, while it has technically never left the "alpha"
   stage, should **now be considered as deprecated in SQLAlchemy 2.0, even
   though it is still necessary for full Mypy support when using
   SQLAlchemy 1.4**.

   The Mypy plugin itself does not solve the issue of supplying correct typing
   with other typing tools such as Pylance/Pyright, Pytype, Pycharm, etc, which
   cannot make use of Mypy plugins. Additionally, Mypy plugins are extremely
   difficult to develop, maintain and test, as a Mypy plugin must be deeply
   integrated with Mypy's internal datastructures and processes, which itself
   are not stable within the Mypy project itself. The SQLAlchemy Mypy plugin
   has lots of limitations when used with code that deviates from very basic
   patterns which are reported regularly.

   For these reasons, new non-regression issues reported against the Mypy
   plugin are unlikely to be fixed. When SQLAlchemy 2.0 is released, it will
   continue to include the plugin, which will have been updated to continue to
   function as well as it does in SQLAlchemy 1.4, when running under SQLAlchemy
   2.0. **Existing code that passes Mypy checks using the plugin with
   SQLAlchemy 1.4 installed will continue to pass all checks in SQLAlchemy 2.0
   without any changes required, provided the plugin is still used. The
   upcoming API to be released with SQLAlchemy 2.0 is fully backwards
   compatible with the SQLAlchemy 1.4 API and Mypy plugin behavior.**

   End-user code that passes all checks under SQLAlchemy 1.4 with the Mypy
   plugin will be able to incrementally migrate to the new structures, once
   that code is running exclusively on SQLAlchemy 2.0.  See the section
   :ref:`whatsnew_20_orm_declarative_typing` for background on how this
   migration may proceed.

   Code that is running exclusively on SQLAlchemy version
   2.0 and has fully migrated to the new declarative constructs will enjoy full
   compliance with pep-484 as well as working correctly within IDEs and other
   typing tools, without the need for plugins.


Installation
------------

For **SQLAlchemy 2.0 only**: No stubs should be installed and packages
like sqlalchemy-stubs_ and sqlalchemy2-stubs_ should be fully uninstalled.

The Mypy_ package itself is a dependency.

Mypy may be installed using the "mypy" extras hook using pip:

.. sourcecode:: text

    pip install sqlalchemy[mypy]

The plugin itself is configured as described in
`Configuring mypy to use Plugins <https://mypy.readthedocs.io/en/latest/extending_mypy.html#configuring-mypy-to-use-plugins>`_,
using the ``sqlalchemy.ext.mypy.plugin`` module name, such as within
``setup.cfg``::

    [mypy]
    plugins = sqlalchemy.ext.mypy.plugin

.. _sqlalchemy-stubs: https://github.com/dropbox/sqlalchemy-stubs

.. _sqlalchemy2-stubs: https://github.com/sqlalchemy/sqlalchemy2-stubs

What the Plugin Does
--------------------

The primary purpose of the Mypy plugin is to intercept and alter the static
definition of SQLAlchemy
:ref:`declarative mappings <orm_declarative_mapper_config_toplevel>` so that
they match up to how they are structured after they have been
:term:`instrumented` by their :class:`_orm.Mapper` objects. This allows both
the class structure itself as well as code that uses the class to make sense to
the Mypy tool, which otherwise would not be the case based on how declarative
mappings currently function.    The plugin is not unlike similar plugins
that are required for libraries like
`dataclasses <https://docs.python.org/3/library/dataclasses.html>`_ which
alter classes dynamically at runtime.

To cover the major areas where this occurs, consider the following ORM
mapping, using the typical example of the ``User`` class::

    from sqlalchemy import Column, Integer, String, select
    from sqlalchemy.orm import declarative_base

    # "Base" is a class that is created dynamically from the
    # declarative_base() function
    Base = declarative_base()


    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)


    # "some_user" is an instance of the User class, which
    # accepts "id" and "name" kwargs based on the mapping
    some_user = User(id=5, name="user")

    # it has an attribute called .name that's a string
    print(f"Username: {some_user.name}")

    # a select() construct makes use of SQL expressions derived from the
    # User class itself
    select_stmt = select(User).where(User.id.in_([3, 4, 5])).where(User.name.contains("s"))

Above, the steps that the Mypy extension can take include:

* Interpretation of the ``Base`` dynamic class generated by
  :func:`_orm.declarative_base`, so that classes which inherit from it
  are known to be mapped.  It also can accommodate the class decorator
  approach described at :ref:`orm_declarative_decorator`.

* Type inference for ORM mapped attributes that are defined in declarative
  "inline" style, in the above example the ``id`` and ``name`` attributes of
  the ``User`` class. This includes that an instance of ``User`` will use
  ``int`` for ``id`` and ``str`` for ``name``. It also includes that when the
  ``User.id`` and ``User.name`` class-level attributes are accessed, as they
  are above in the ``select()`` statement, they are compatible with SQL
  expression behavior, which is derived from the
  :class:`_orm.InstrumentedAttribute` attribute descriptor class.

* Application of an ``__init__()`` method to mapped classes that do not
  already include an explicit constructor, which accepts keyword arguments
  of specific types for all mapped attributes detected.

When the Mypy plugin processes the above file, the resulting static class
definition and Python code passed to the Mypy tool is equivalent to the
following::

    from sqlalchemy import Column, Integer, String, select
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm.decl_api import DeclarativeMeta


    class Base(metaclass=DeclarativeMeta):
        __abstract__ = True


    class User(Base):
        __tablename__ = "user"

        id: Mapped[Optional[int]] = Mapped._special_method(
            Column(Integer, primary_key=True)
        )
        name: Mapped[Optional[str]] = Mapped._special_method(Column(String))

        def __init__(self, id: Optional[int] = ..., name: Optional[str] = ...) -> None:
            ...


    some_user = User(id=5, name="user")

    print(f"Username: {some_user.name}")

    select_stmt = select(User).where(User.id.in_([3, 4, 5])).where(User.name.contains("s"))

The key steps which have been taken above include:

* The ``Base`` class is now defined in terms of the :class:`_orm.DeclarativeMeta`
  class explicitly, rather than being a dynamic class.

* The ``id`` and ``name`` attributes are defined in terms of the
  :class:`_orm.Mapped` class, which represents a Python descriptor that
  exhibits different behaviors at the class vs. instance levels.  The
  :class:`_orm.Mapped` class is now the base class for the :class:`_orm.InstrumentedAttribute`
  class that is used for all ORM mapped attributes.

  :class:`_orm.Mapped` is defined as a generic class against arbitrary Python
  types, meaning specific occurrences of :class:`_orm.Mapped` are associated
  with a specific Python type, such as ``Mapped[Optional[int]]`` and
  ``Mapped[Optional[str]]`` above.

* The right-hand side of the declarative mapped attribute assignments are
  **removed**, as this resembles the operation that the :class:`_orm.Mapper`
  class would normally be doing, which is that it would be replacing these
  attributes with specific instances of :class:`_orm.InstrumentedAttribute`.
  The original expression is moved into a function call that will allow it to
  still be type-checked without conflicting with the left-hand side of the
  expression. For Mypy purposes, the left-hand typing annotation is sufficient
  for the attribute's behavior to be understood.

* A type stub for the ``User.__init__()`` method is added which includes the
  correct keywords and datatypes.

Usage
------

The following subsections will address individual uses cases that have
so far been considered for pep-484 compliance.


Introspection of Columns based on TypeEngine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For mapped columns that include an explicit datatype, when they are mapped
as inline attributes, the mapped type will be introspected automatically::

    class MyClass(Base):
        # ...

        id = Column(Integer, primary_key=True)
        name = Column("employee_name", String(50), nullable=False)
        other_name = Column(String(50))

Above, the ultimate class-level datatypes of ``id``, ``name`` and
``other_name`` will be introspected as ``Mapped[Optional[int]]``,
``Mapped[Optional[str]]`` and ``Mapped[Optional[str]]``. The types are by
default **always** considered to be ``Optional``, even for the primary key and
non-nullable column. The reason is because while the database columns "id" and
"name" can't be NULL, the Python attributes ``id`` and ``name`` most certainly
can be ``None`` without an explicit constructor::

    >>> m1 = MyClass()
    >>> m1.id
    None

The types of the above columns can be stated **explicitly**, providing the
two advantages of clearer self-documentation as well as being able to
control which types are optional::

    class MyClass(Base):
        # ...

        id: int = Column(Integer, primary_key=True)
        name: str = Column("employee_name", String(50), nullable=False)
        other_name: Optional[str] = Column(String(50))

The Mypy plugin will accept the above ``int``, ``str`` and ``Optional[str]``
and convert them to include the ``Mapped[]`` type surrounding them.  The
``Mapped[]`` construct may also be used explicitly::

    from sqlalchemy.orm import Mapped


    class MyClass(Base):
        # ...

        id: Mapped[int] = Column(Integer, primary_key=True)
        name: Mapped[str] = Column("employee_name", String(50), nullable=False)
        other_name: Mapped[Optional[str]] = Column(String(50))

When the type is non-optional, it simply means that the attribute as accessed
from an instance of ``MyClass`` will be considered to be non-None::

    mc = MyClass(...)

    # will pass mypy --strict
    name: str = mc.name

For optional attributes, Mypy considers that the type must include None
or otherwise be ``Optional``::

    mc = MyClass(...)

    # will pass mypy --strict
    other_name: Optional[str] = mc.name

Whether or not the mapped attribute is typed as ``Optional``, the
generation of the ``__init__()`` method will **still consider all keywords
to be optional**.  This is again matching what the SQLAlchemy ORM actually
does when it creates the constructor, and should not be confused with the
behavior of a validating system such as Python ``dataclasses`` which will
generate a constructor that matches the annotations in terms of optional
vs. required attributes.


Columns that Don't have an Explicit Type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Columns that include a :class:`_schema.ForeignKey` modifier do not need
to specify a datatype in a SQLAlchemy declarative mapping.  For
this type of attribute, the Mypy plugin will inform the user that it
needs an explicit type to be sent::

    # .. other imports
    from sqlalchemy.sql.schema import ForeignKey

    Base = declarative_base()


    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)


    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey("user.id"))

The plugin will deliver the message as follows:

.. sourcecode:: text

    $ mypy test3.py --strict
    test3.py:20: error: [SQLAlchemy Mypy plugin] Can't infer type from
    ORM mapped expression assigned to attribute 'user_id'; please specify a
    Python type or Mapped[<python type>] on the left hand side.
    Found 1 error in 1 file (checked 1 source file)

To resolve, apply an explicit type annotation to the ``Address.user_id``
column::

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id: int = Column(ForeignKey("user.id"))

Mapping Columns with Imperative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In :ref:`imperative table style <orm_imperative_table_configuration>`, the
:class:`_schema.Column` definitions are given inside of a :class:`_schema.Table`
construct which is separate from the mapped attributes themselves.  The Mypy
plugin does not consider this :class:`_schema.Table`, but instead supports that
the attributes can be explicitly stated with a complete annotation that
**must** use the :class:`_orm.Mapped` class to identify them as mapped attributes::

    class MyClass(Base):
        __table__ = Table(
            "mytable",
            Base.metadata,
            Column(Integer, primary_key=True),
            Column("employee_name", String(50), nullable=False),
            Column(String(50)),
        )

        id: Mapped[int]
        name: Mapped[str]
        other_name: Mapped[Optional[str]]

The above :class:`_orm.Mapped` annotations are considered as mapped columns and
will be included in the default constructor, as well as provide the correct
typing profile for ``MyClass`` both at the class level and the instance level.

Mapping Relationships
^^^^^^^^^^^^^^^^^^^^^^

The plugin has limited support for using type inference to detect the types
for relationships.    For all those cases where it can't detect the type,
it will emit an informative error message, and in all cases the appropriate
type may be provided explicitly, either with the :class:`_orm.Mapped`
class or optionally omitting it for an inline declaration.     The plugin
also needs to determine whether or not the relationship refers to a collection
or a scalar, and for that it relies upon the explicit value of
the :paramref:`_orm.relationship.uselist` and/or :paramref:`_orm.relationship.collection_class`
parameters.  An explicit type is needed if neither of these parameters are
present, as well as if the target type of the :func:`_orm.relationship`
is a string or callable, and not a class::

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)


    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id: int = Column(ForeignKey("user.id"))

        user = relationship(User)

The above mapping will produce the following error:

.. sourcecode:: text

    test3.py:22: error: [SQLAlchemy Mypy plugin] Can't infer scalar or
    collection for ORM mapped expression assigned to attribute 'user'
    if both 'uselist' and 'collection_class' arguments are absent from the
    relationship(); please specify a type annotation on the left hand side.
    Found 1 error in 1 file (checked 1 source file)

The error can be resolved either by using ``relationship(User, uselist=False)``
or by providing the type, in this case the scalar ``User`` object::

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id: int = Column(ForeignKey("user.id"))

        user: User = relationship(User)

For collections, a similar pattern applies, where in the absence of
``uselist=True`` or a :paramref:`_orm.relationship.collection_class`,
a collection annotation such as ``List`` may be used.   It is also fully
appropriate to use the string name of the class in the annotation as supported
by pep-484, ensuring the class is imported with in
the `TYPE_CHECKING block <https://www.python.org/dev/peps/pep-0484/#runtime-or-type-checking>`_
as appropriate::

    from typing import TYPE_CHECKING, List

    from .mymodel import Base

    if TYPE_CHECKING:
        # if the target of the relationship is in another module
        # that cannot normally be imported at runtime
        from .myaddressmodel import Address


    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        addresses: List["Address"] = relationship("Address")

As is the case with columns, the :class:`_orm.Mapped` class may also be
applied explicitly::

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses: Mapped[List["Address"]] = relationship("Address", back_populates="user")


    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, primary_key=True)
        user_id: int = Column(ForeignKey("user.id"))

        user: Mapped[User] = relationship(User, back_populates="addresses")

.. _mypy_declarative_mixins:

Using @declared_attr and Declarative Mixins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_orm.declared_attr` class allows Declarative mapped attributes to
be declared in class level functions, and is particularly useful when using
:ref:`declarative mixins <orm_mixins_toplevel>`. For these functions, the return
type of the function should be annotated using either the ``Mapped[]``
construct or by indicating the exact kind of object returned by the function.
Additionally, "mixin" classes that are not otherwise mapped (i.e. don't extend
from a :func:`_orm.declarative_base` class nor are they mapped with a method
such as :meth:`_orm.registry.mapped`) should be decorated with the
:func:`_orm.declarative_mixin` decorator, which provides a hint to the Mypy
plugin that a particular class intends to serve as a declarative mixin::

    from sqlalchemy.orm import declarative_mixin, declared_attr


    @declarative_mixin
    class HasUpdatedAt:
        @declared_attr
        def updated_at(cls) -> Column[DateTime]:  # uses Column
            return Column(DateTime)


    @declarative_mixin
    class HasCompany:
        @declared_attr
        def company_id(cls) -> Mapped[int]:  # uses Mapped
            return Column(ForeignKey("company.id"))

        @declared_attr
        def company(cls) -> Mapped["Company"]:
            return relationship("Company")


    class Employee(HasUpdatedAt, HasCompany, Base):
        __tablename__ = "employee"

        id = Column(Integer, primary_key=True)
        name = Column(String)

Note the mismatch between the actual return type of a method like
``HasCompany.company`` vs. what is annotated.  The Mypy plugin converts
all ``@declared_attr`` functions into simple annotated attributes to avoid
this complexity::

    # what Mypy sees
    class HasCompany:
        company_id: Mapped[int]
        company: Mapped["Company"]

Combining with Dataclasses or Other Type-Sensitive Attribute Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The examples of Python dataclasses integration at :ref:`orm_declarative_dataclasses`
presents a problem; Python dataclasses expect an explicit type that it will
use to build the class, and the value given in each assignment statement
is significant.    That is, a class as follows has to be stated exactly
as it is in order to be accepted by dataclasses::

    mapper_registry: registry = registry()


    @mapper_registry.mapped
    @dataclass
    class User:
        __table__ = Table(
            "user",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("fullname", String(50)),
            Column("nickname", String(12)),
        )
        id: int = field(init=False)
        name: Optional[str] = None
        fullname: Optional[str] = None
        nickname: Optional[str] = None
        addresses: List[Address] = field(default_factory=list)

        __mapper_args__ = {  # type: ignore
            "properties": {"addresses": relationship("Address")}
        }

We can't apply our ``Mapped[]`` types to the attributes ``id``, ``name``,
etc. because they will be rejected by the ``@dataclass`` decorator.   Additionally,
Mypy has another plugin for dataclasses explicitly which can also get in the
way of what we're doing.

The above class will actually pass Mypy's type checking without issue; the
only thing we are missing is the ability for attributes on ``User`` to be
used in SQL expressions, such as::

    stmt = select(User.name).where(User.id.in_([1, 2, 3]))

To provide a workaround for this, the Mypy plugin has an additional feature
whereby we can specify an extra attribute ``_mypy_mapped_attrs``, that is
a list that encloses the class-level objects or their string names.
This attribute can be conditional within the ``TYPE_CHECKING`` variable::

    @mapper_registry.mapped
    @dataclass
    class User:
        __table__ = Table(
            "user",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("fullname", String(50)),
            Column("nickname", String(12)),
        )
        id: int = field(init=False)
        name: Optional[str] = None
        fullname: Optional[str]
        nickname: Optional[str]
        addresses: List[Address] = field(default_factory=list)

        if TYPE_CHECKING:
            _mypy_mapped_attrs = [id, name, "fullname", "nickname", addresses]

        __mapper_args__ = {  # type: ignore
            "properties": {"addresses": relationship("Address")}
        }

With the above recipe, the attributes listed in ``_mypy_mapped_attrs``
will be applied with the :class:`_orm.Mapped` typing information so that the
``User`` class will behave as a SQLAlchemy mapped class when used in a
class-bound context.

.. _Mypy: https://mypy.readthedocs.io/

.. _orm_dataclasses_toplevel:

======================================
Integration with dataclasses and attrs
======================================

SQLAlchemy as of version 2.0 features "native dataclass" integration where
an :ref:`Annotated Declarative Table <orm_declarative_mapped_column>`
mapping may be turned into a Python dataclass_ by adding a single mixin
or decorator to mapped classes.

.. versionadded:: 2.0 Integrated dataclass creation with ORM Declarative classes

There are also patterns available that allow existing dataclasses to be
mapped, as well as to map classes instrumented by the
attrs_ third party integration library.

.. _orm_declarative_native_dataclasses:

Declarative Dataclass Mapping
-----------------------------

SQLAlchemy :ref:`Annotated Declarative Table <orm_declarative_mapped_column>`
mappings may be augmented with an additional
mixin class or decorator directive, which will add an additional step to
the Declarative process after the mapping is complete that will convert
the mapped class **in-place** into a Python dataclass_, before completing
the mapping process which applies ORM-specific :term:`instrumentation`
to the class.   The most prominent behavioral addition this provides is
generation of an ``__init__()`` method with fine-grained control over
positional and keyword arguments with or without defaults, as well as
generation of methods like ``__repr__()`` and ``__eq__()``.

From a :pep:`484` typing perspective, the class is recognized
as having Dataclass-specific behaviors, most notably  by taking advantage of :pep:`681`
"Dataclass Transforms", which allows typing tools to consider the class
as though it were explicitly decorated using the ``@dataclasses.dataclass``
decorator.

.. note::  Support for :pep:`681` in typing tools as of **April 4, 2023** is
   limited and is currently known to be supported by Pyright_ as well
   as Mypy_ as of **version 1.2**.  Note that Mypy 1.1.1 introduced
   :pep:`681` support but did not correctly accommodate Python descriptors
   which will lead to errors when using SQLAlchemy's ORM mapping scheme.

   .. seealso::

      https://peps.python.org/pep-0681/#the-dataclass-transform-decorator - background
      on how libraries like SQLAlchemy enable :pep:`681` support


Dataclass conversion may be added to any Declarative class either by adding the
:class:`_orm.MappedAsDataclass` mixin to a :class:`_orm.DeclarativeBase` class
hierarchy, or for decorator mapping by using the
:meth:`_orm.registry.mapped_as_dataclass` class decorator.

The :class:`_orm.MappedAsDataclass` mixin may be applied either
to the Declarative ``Base`` class or any superclass, as in the example
below::


    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import MappedAsDataclass


    class Base(MappedAsDataclass, DeclarativeBase):
        """subclasses will be converted to dataclasses"""


    class User(Base):
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

Or may be applied directly to classes that extend from the Declarative base::

    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import MappedAsDataclass


    class Base(DeclarativeBase):
        pass


    class User(MappedAsDataclass, Base):
        """User class will be converted to a dataclass"""

        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

When using the decorator form, only the :meth:`_orm.registry.mapped_as_dataclass`
decorator is supported::

    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry


    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

Class level feature configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Support for dataclasses features is partial.  Currently **supported** are
the ``init``, ``repr``, ``eq``, ``order`` and ``unsafe_hash`` features,
``match_args`` and ``kw_only`` are supported on Python 3.10+.
Currently **not supported** are the ``frozen`` and ``slots`` features.

When using the mixin class form with :class:`_orm.MappedAsDataclass`,
class configuration arguments are passed as class-level parameters::

    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import MappedAsDataclass


    class Base(DeclarativeBase):
        pass


    class User(MappedAsDataclass, Base, repr=False, unsafe_hash=True):
        """User class will be converted to a dataclass"""

        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

When using the decorator form with :meth:`_orm.registry.mapped_as_dataclass`,
class configuration arguments are passed to the decorator directly::

    from sqlalchemy.orm import registry
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    reg = registry()


    @reg.mapped_as_dataclass(unsafe_hash=True)
    class User:
        """User class will be converted to a dataclass"""

        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

For background on dataclass class options, see the dataclasses_ documentation
at `@dataclasses.dataclass <https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass>`_.

Attribute Configuration
^^^^^^^^^^^^^^^^^^^^^^^

SQLAlchemy native dataclasses differ from normal dataclasses in that
attributes to be mapped are described using the :class:`_orm.Mapped`
generic annotation container in all cases.    Mappings follow the same
forms as those documented at :ref:`orm_declarative_table`, and all
features of :func:`_orm.mapped_column` and :class:`_orm.Mapped` are supported.

Additionally, ORM attribute configuration constructs including
:func:`_orm.mapped_column`, :func:`_orm.relationship` and :func:`_orm.composite`
support **per-attribute field options**, including ``init``, ``default``,
``default_factory`` and ``repr``.  The names of these arguments is fixed
as specified in :pep:`681`.   Functionality is equivalent to dataclasses:

* ``init``, as in :paramref:`_orm.mapped_column.init`,
  :paramref:`_orm.relationship.init`, if False indicates the field should
  not be part of the ``__init__()`` method
* ``default``, as in :paramref:`_orm.mapped_column.default`,
  :paramref:`_orm.relationship.default`
  indicates a default value for the field as given as a keyword argument
  in the ``__init__()`` method.
* ``default_factory``, as in :paramref:`_orm.mapped_column.default_factory`,
  :paramref:`_orm.relationship.default_factory`, indicates a callable function
  that will be invoked to generate a new default value for a parameter
  if not passed explicitly to the ``__init__()`` method.
* ``repr`` True by default, indicates the field should be part of the generated
  ``__repr__()`` method


Another key difference from dataclasses is that default values for attributes
**must** be configured using the ``default`` parameter of the ORM construct,
such as ``mapped_column(default=None)``.   A syntax that resembles dataclass
syntax which accepts simple Python values as defaults without using
``@dataclases.field()`` is not supported.

As an example using :func:`_orm.mapped_column`, the mapping below will
produce an ``__init__()`` method that accepts only the fields ``name`` and
``fullname``, where ``name`` is required and may be passed positionally,
and ``fullname`` is optional.  The ``id`` field, which we expect to be
database-generated, is not part of the constructor at all::

    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]
        fullname: Mapped[str] = mapped_column(default=None)


    # 'fullname' is optional keyword argument
    u1 = User("name")

Column Defaults
~~~~~~~~~~~~~~~

In order to accommodate the name overlap of the ``default`` argument with
the existing :paramref:`_schema.Column.default` parameter of the  :class:`_schema.Column`
construct, the :func:`_orm.mapped_column` construct disambiguates the two
names by adding a new parameter :paramref:`_orm.mapped_column.insert_default`,
which will be populated directly into the
:paramref:`_schema.Column.default` parameter of  :class:`_schema.Column`,
independently of what may be set on
:paramref:`_orm.mapped_column.default`, which is always used for the
dataclasses configuration.  For example, to configure a datetime column with
a :paramref:`_schema.Column.default` set to the ``func.utc_timestamp()`` SQL function,
but where the parameter is optional in the constructor::

    from datetime import datetime

    from sqlalchemy import func
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        created_at: Mapped[datetime] = mapped_column(
            insert_default=func.utc_timestamp(), default=None
        )

With the above mapping, an ``INSERT`` for a new ``User`` object where no
parameter for ``created_at`` were passed proceeds as:

.. sourcecode:: pycon+sql

    >>> with Session(e) as session:
    ...     session.add(User())
    ...     session.commit()
    {execsql}BEGIN (implicit)
    INSERT INTO user_account (created_at) VALUES (utc_timestamp())
    [generated in 0.00010s] ()
    COMMIT



Integration with Annotated
~~~~~~~~~~~~~~~~~~~~~~~~~~

The approach introduced at :ref:`orm_declarative_mapped_column_pep593`
illustrates how to use :pep:`593` ``Annotated`` objects to package whole
:func:`_orm.mapped_column` constructs for re-use.  While ``Annotated`` objects
can be combined with the use of dataclasses, **dataclass-specific keyword
arguments unfortunately cannot be used within the Annotated construct**.  This
includes :pep:`681`-specific arguments ``init``, ``default``, ``repr``, and
``default_factory``, which **must** be present in a :func:`_orm.mapped_column`
or similar construct inline with the class attribute.

.. versionchanged:: 2.0.14/2.0.22  the ``Annotated`` construct when used with
   an ORM construct like :func:`_orm.mapped_column` cannot accommodate dataclass
   field parameters such as ``init`` and ``repr`` - this use goes against the
   design of Python dataclasses and is not supported by :pep:`681`, and therefore
   is also rejected by the SQLAlchemy ORM at runtime.   A deprecation warning
   is now emitted and the attribute will be ignored.

As an example, the ``init=False`` parameter below will be ignored and additionally
emit a deprecation warning::

    from typing import Annotated

    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    # typing tools as well as SQLAlchemy will ignore init=False here
    intpk = Annotated[int, mapped_column(init=False, primary_key=True)]

    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"
        id: Mapped[intpk]


    # typing error as well as runtime error: Argument missing for parameter "id"
    u1 = User()

Instead, :func:`_orm.mapped_column` must be present on the right side
as well with an explicit setting for :paramref:`_orm.mapped_column.init`;
the other arguments can remain within the ``Annotated`` construct::

    from typing import Annotated

    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    intpk = Annotated[int, mapped_column(primary_key=True)]

    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"

        # init=False and other pep-681 arguments must be inline
        id: Mapped[intpk] = mapped_column(init=False)


    u1 = User()

.. _orm_declarative_dc_mixins:

Using mixins and abstract superclasses
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any mixins or base classes that are used in a :class:`_orm.MappedAsDataclass`
mapped class which include :class:`_orm.Mapped` attributes must themselves be
part of a :class:`_orm.MappedAsDataclass`
hierarchy, such as in the example below using a mixin::


    class Mixin(MappedAsDataclass):
        create_user: Mapped[int] = mapped_column()
        update_user: Mapped[Optional[int]] = mapped_column(default=None, init=False)


    class Base(DeclarativeBase, MappedAsDataclass):
        pass


    class User(Base, Mixin):
        __tablename__ = "sys_user"

        uid: Mapped[str] = mapped_column(
            String(50), init=False, default_factory=uuid4, primary_key=True
        )
        username: Mapped[str] = mapped_column()
        email: Mapped[str] = mapped_column()

Python type checkers which support :pep:`681` will otherwise not consider
attributes from non-dataclass mixins to be part of the dataclass.

.. deprecated:: 2.0.8  Using mixins and abstract bases within
   :class:`_orm.MappedAsDataclass` or
   :meth:`_orm.registry.mapped_as_dataclass` hierarchies which are not
   themselves dataclasses is deprecated, as these fields are not supported
   by :pep:`681` as belonging to the dataclass.  A warning is emitted for this
   case which will later be an error.

   .. seealso::

       :ref:`error_dcmx` - background on rationale




Relationship Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_orm.Mapped` annotation in combination with
:func:`_orm.relationship` is used in the same way as described at
:ref:`relationship_patterns`.    When specifying a collection-based
:func:`_orm.relationship` as an optional keyword argument, the
:paramref:`_orm.relationship.default_factory` parameter must be passed and it
must refer to the collection class that's to be used.  Many-to-one and
scalar object references may make use of
:paramref:`_orm.relationship.default` if the default value is to be ``None``::

    from typing import List

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    reg = registry()


    @reg.mapped_as_dataclass
    class Parent:
        __tablename__ = "parent"
        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(
            default_factory=list, back_populates="parent"
        )


    @reg.mapped_as_dataclass
    class Child:
        __tablename__ = "child"
        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent.id"))
        parent: Mapped["Parent"] = relationship(default=None)

The above mapping will generate an empty list for ``Parent.children`` when a
new ``Parent()`` object is constructed without passing ``children``, and
similarly a ``None`` value for ``Child.parent`` when a new ``Child()`` object
is constructed without passing ``parent``.

While the :paramref:`_orm.relationship.default_factory` can be automatically
derived from the given collection class of the :func:`_orm.relationship`
itself, this would break compatibility with dataclasses, as the presence
of :paramref:`_orm.relationship.default_factory` or
:paramref:`_orm.relationship.default` is what determines if the parameter is
to be required or optional when rendered into the ``__init__()`` method.

.. _orm_declarative_native_dataclasses_non_mapped_fields:

Using Non-Mapped Dataclass Fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using Declarative dataclasses, non-mapped fields may be used on the
class as well, which will be part of the dataclass construction process but
will not be mapped.   Any field that does not use :class:`.Mapped` will
be ignored by the mapping process.   In the example below, the fields
``ctrl_one`` and ``ctrl_two`` will be part of the instance-level state
of the object, but will not be persisted by the ORM::


    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    reg = registry()


    @reg.mapped_as_dataclass
    class Data:
        __tablename__ = "data"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        status: Mapped[str]

        ctrl_one: Optional[str] = None
        ctrl_two: Optional[str] = None

Instance of ``Data`` above can be created as::

    d1 = Data(status="s1", ctrl_one="ctrl1", ctrl_two="ctrl2")

A more real world example might be to make use of the Dataclasses
``InitVar`` feature in conjunction with the ``__post_init__()`` feature to
receive init-only fields that can be used to compose persisted data.
In the example below, the ``User``
class is declared using ``id``, ``name`` and ``password_hash`` as mapped features,
but makes use of init-only ``password`` and ``repeat_password`` fields to
represent the user creation process (note: to run this example, replace
the function ``your_crypt_function_here()`` with a third party crypt
function, such as `bcrypt <https://pypi.org/project/bcrypt/>`_ or
`argon2-cffi <https://pypi.org/project/argon2-cffi/>`_)::

    from dataclasses import InitVar
    from typing import Optional

    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import registry

    reg = registry()


    @reg.mapped_as_dataclass
    class User:
        __tablename__ = "user_account"

        id: Mapped[int] = mapped_column(init=False, primary_key=True)
        name: Mapped[str]

        password: InitVar[str]
        repeat_password: InitVar[str]

        password_hash: Mapped[str] = mapped_column(init=False, nullable=False)

        def __post_init__(self, password: str, repeat_password: str):
            if password != repeat_password:
                raise ValueError("passwords do not match")

            self.password_hash = your_crypt_function_here(password)

The above object is created with parameters ``password`` and
``repeat_password``, which are consumed up front so that the ``password_hash``
variable may be generated::

    >>> u1 = User(name="some_user", password="xyz", repeat_password="xyz")
    >>> u1.password_hash
    '$6$9ppc... (example crypted string....)'

.. versionchanged:: 2.0.0rc1  When using :meth:`_orm.registry.mapped_as_dataclass`
   or :class:`.MappedAsDataclass`, fields that do not include the
   :class:`.Mapped` annotation may be included, which will be treated as part
   of the resulting dataclass but not be mapped, without the need to
   also indicate the ``__allow_unmapped__`` class attribute.  Previous 2.0
   beta releases would require this attribute to be explicitly present,
   even though the purpose of this attribute was only to allow legacy
   ORM typed mappings to continue to function.

.. _dataclasses_pydantic:

Integrating with Alternate Dataclass Providers such as Pydantic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    The dataclass layer of Pydantic is **not fully compatible** with
    SQLAlchemy's class instrumentation without additional internal changes,
    and many features such as related collections may not work correctly.

    For Pydantic compatibility, please consider the
    `SQLModel <https://sqlmodel.tiangolo.com>`_ ORM which is built with
    Pydantic on top of SQLAlchemy ORM, which includes special implementation
    details which **explicitly resolve** these incompatibilities.

SQLAlchemy's :class:`_orm.MappedAsDataclass` class
and :meth:`_orm.registry.mapped_as_dataclass` method call directly into
the Python standard library ``dataclasses.dataclass`` class decorator, after
the declarative mapping process has been applied to the class.  This
function call may be swapped out for alternateive dataclasses providers,
such as that of Pydantic, using the ``dataclass_callable`` parameter
accepted by :class:`_orm.MappedAsDataclass` as a class keyword argument
as well as by :meth:`_orm.registry.mapped_as_dataclass`::

    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import MappedAsDataclass
    from sqlalchemy.orm import registry


    class Base(
        MappedAsDataclass,
        DeclarativeBase,
        dataclass_callable=pydantic.dataclasses.dataclass,
    ):
        pass


    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

The above ``User`` class will be applied as a dataclass, using Pydantic's
``pydantic.dataclasses.dataclasses`` callable.     The process is available
both for mapped classes as well as mixins that extend from
:class:`_orm.MappedAsDataclass` or which have
:meth:`_orm.registry.mapped_as_dataclass` applied directly.

.. versionadded:: 2.0.4 Added the ``dataclass_callable`` class and method
   parameters for :class:`_orm.MappedAsDataclass` and
   :meth:`_orm.registry.mapped_as_dataclass`, and adjusted some of the
   dataclass internals to accommodate more strict dataclass functions such as
   that of Pydantic.


.. _orm_declarative_dataclasses:

Applying ORM Mappings to an existing dataclass (legacy dataclass use)
---------------------------------------------------------------------

.. legacy::

   The approaches described here are superseded by
   the :ref:`orm_declarative_native_dataclasses` feature new in the 2.0
   series of SQLAlchemy.  This newer version of the feature builds upon
   the dataclass support first added in version 1.4, which is described
   in this section.

To map an existing dataclass, SQLAlchemy's "inline" declarative directives
cannot be used directly; ORM directives are assigned using one of three
techniques:

* Using "Declarative with Imperative Table", the table / column to be mapped
  is defined using a :class:`_schema.Table` object assigned to the
  ``__table__`` attribute of the class; relationships are defined within
  ``__mapper_args__`` dictionary.  The class is mapped using the
  :meth:`_orm.registry.mapped` decorator.   An example is below at
  :ref:`orm_declarative_dataclasses_imperative_table`.

* Using full "Declarative", the Declarative-interpreted directives such as
  :class:`_schema.Column`, :func:`_orm.relationship` are added to the
  ``.metadata`` dictionary of the ``dataclasses.field()`` construct, where
  they are consumed by the declarative process.  The class is again
  mapped using the :meth:`_orm.registry.mapped` decorator.  See the example
  below at :ref:`orm_declarative_dataclasses_declarative_table`.

* An "Imperative" mapping can be applied to an existing dataclass using
  the :meth:`_orm.registry.map_imperatively` method to produce the mapping
  in exactly the same way as described at :ref:`orm_imperative_mapping`.
  This is illustrated below at :ref:`orm_imperative_dataclasses`.

The general process by which SQLAlchemy applies mappings to a dataclass
is the same as that of an ordinary class, but also includes that
SQLAlchemy will detect class-level attributes that were part of the
dataclasses declaration process and replace them at runtime with
the usual SQLAlchemy ORM mapped attributes.   The ``__init__`` method that
would have been generated by dataclasses is left intact, as is the same
for all the other methods that dataclasses generates such as
``__eq__()``, ``__repr__()``, etc.

.. _orm_declarative_dataclasses_imperative_table:

Mapping pre-existing dataclasses using Declarative With Imperative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example of a mapping using ``@dataclass`` using
:ref:`orm_imperative_table_configuration` is below. A complete
:class:`_schema.Table` object is constructed explicitly and assigned to the
``__table__`` attribute. Instance fields are defined using normal dataclass
syntaxes. Additional :class:`.MapperProperty`
definitions such as :func:`.relationship`, are placed in the
:ref:`__mapper_args__ <orm_declarative_mapper_options>` class-level
dictionary underneath the ``properties`` key, corresponding to the
:paramref:`_orm.Mapper.properties` parameter::

    from __future__ import annotations

    from dataclasses import dataclass, field
    from typing import List, Optional

    from sqlalchemy import Column, ForeignKey, Integer, String, Table
    from sqlalchemy.orm import registry, relationship

    mapper_registry = registry()


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
            "properties": {
                "addresses": relationship("Address"),
            }
        }


    @mapper_registry.mapped
    @dataclass
    class Address:
        __table__ = Table(
            "address",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("user.id")),
            Column("email_address", String(50)),
        )
        id: int = field(init=False)
        user_id: int = field(init=False)
        email_address: Optional[str] = None

In the above example, the ``User.id``, ``Address.id``, and ``Address.user_id``
attributes are defined as ``field(init=False)``. This means that parameters for
these won't be added to ``__init__()`` methods, but
:class:`.Session` will still be able to set them after getting their values
during flush from autoincrement or other default value generator.   To
allow them to be specified in the constructor explicitly, they would instead
be given a default value of ``None``.

For a :func:`_orm.relationship` to be declared separately, it needs to be
specified directly within the :paramref:`_orm.Mapper.properties` dictionary
which itself is specified within the ``__mapper_args__`` dictionary, so that it
is passed to the constructor for :class:`_orm.Mapper`. An alternative to this
approach is in the next example.


.. warning::
    Declaring a dataclass ``field()`` setting a ``default`` together with ``init=False``
    will not work as would be expected with a totally plain dataclass,
    since the SQLAlchemy class instrumentation will replace
    the default value set on the class by the dataclass creation process.
    Use ``default_factory`` instead. This adaptation is done automatically when
    making use of :ref:`orm_declarative_native_dataclasses`.

.. _orm_declarative_dataclasses_declarative_table:

Mapping pre-existing dataclasses using Declarative-style fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. legacy:: This approach to Declarative mapping with
   dataclasses should be considered as legacy.  It will remain supported
   however is unlikely to offer any advantages against the new
   approach detailed at :ref:`orm_declarative_native_dataclasses`.

   Note that **mapped_column() is not supported with this use**;
   the :class:`_schema.Column` construct should continue to be used to declare
   table metadata within the ``metadata`` field of ``dataclasses.field()``.

The fully declarative approach requires that :class:`_schema.Column` objects
are declared as class attributes, which when using dataclasses would conflict
with the dataclass-level attributes.  An approach to combine these together
is to make use of the ``metadata`` attribute on the ``dataclass.field``
object, where SQLAlchemy-specific mapping information may be supplied.
Declarative supports extraction of these parameters when the class
specifies the attribute ``__sa_dataclass_metadata_key__``.  This also
provides a more succinct method of indicating the :func:`_orm.relationship`
association::


    from __future__ import annotations

    from dataclasses import dataclass, field
    from typing import List

    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.orm import registry, relationship

    mapper_registry = registry()


    @mapper_registry.mapped
    @dataclass
    class User:
        __tablename__ = "user"

        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})
        name: str = field(default=None, metadata={"sa": Column(String(50))})
        fullname: str = field(default=None, metadata={"sa": Column(String(50))})
        nickname: str = field(default=None, metadata={"sa": Column(String(12))})
        addresses: List[Address] = field(
            default_factory=list, metadata={"sa": relationship("Address")}
        )


    @mapper_registry.mapped
    @dataclass
    class Address:
        __tablename__ = "address"
        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})
        user_id: int = field(init=False, metadata={"sa": Column(ForeignKey("user.id"))})
        email_address: str = field(default=None, metadata={"sa": Column(String(50))})

.. _orm_declarative_dataclasses_mixin:

Using Declarative Mixins with pre-existing dataclasses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the section :ref:`orm_mixins_toplevel`, Declarative Mixin classes
are introduced.  One requirement of declarative mixins is that certain
constructs that can't be easily duplicated must be given as callables,
using the :class:`_orm.declared_attr` decorator, such as in the
example at :ref:`orm_declarative_mixins_relationships`::

    class RefTargetMixin:
        @declared_attr
        def target_id(cls) -> Mapped[int]:
            return mapped_column("target_id", ForeignKey("target.id"))

        @declared_attr
        def target(cls):
            return relationship("Target")

This form is supported within the Dataclasses ``field()`` object by using
a lambda to indicate the SQLAlchemy construct inside the ``field()``.
Using :func:`_orm.declared_attr` to surround the lambda is optional.
If we wanted to produce our ``User`` class above where the ORM fields
came from a mixin that is itself a dataclass, the form would be::

    @dataclass
    class UserMixin:
        __tablename__ = "user"

        __sa_dataclass_metadata_key__ = "sa"

        id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})

        addresses: List[Address] = field(
            default_factory=list, metadata={"sa": lambda: relationship("Address")}
        )


    @dataclass
    class AddressMixin:
        __tablename__ = "address"
        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(init=False, metadata={"sa": Column(Integer, primary_key=True)})
        user_id: int = field(
            init=False, metadata={"sa": lambda: Column(ForeignKey("user.id"))}
        )
        email_address: str = field(default=None, metadata={"sa": Column(String(50))})


    @mapper_registry.mapped
    class User(UserMixin):
        pass


    @mapper_registry.mapped
    class Address(AddressMixin):
        pass

.. versionadded:: 1.4.2  Added support for "declared attr" style mixin attributes,
   namely :func:`_orm.relationship` constructs as well as :class:`_schema.Column`
   objects with foreign key declarations, to be used within "Dataclasses
   with Declarative Table" style mappings.



.. _orm_imperative_dataclasses:

Mapping pre-existing dataclasses using Imperative Mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As described previously, a class which is set up as a dataclass using the
``@dataclass`` decorator can then be further decorated using the
:meth:`_orm.registry.mapped` decorator in order to apply declarative-style
mapping to the class. As an alternative to using the
:meth:`_orm.registry.mapped` decorator, we may also pass the class through the
:meth:`_orm.registry.map_imperatively` method instead, so that we may pass all
:class:`_schema.Table` and :class:`_orm.Mapper` configuration imperatively to
the function rather than having them defined on the class itself as class
variables::

    from __future__ import annotations

    from dataclasses import dataclass
    from dataclasses import field
    from typing import List

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import MetaData
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()


    @dataclass
    class User:
        id: int = field(init=False)
        name: str = None
        fullname: str = None
        nickname: str = None
        addresses: List[Address] = field(default_factory=list)


    @dataclass
    class Address:
        id: int = field(init=False)
        user_id: int = field(init=False)
        email_address: str = None


    metadata_obj = MetaData()

    user = Table(
        "user",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("fullname", String(50)),
        Column("nickname", String(12)),
    )

    address = Table(
        "address",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("user.id")),
        Column("email_address", String(50)),
    )

    mapper_registry.map_imperatively(
        User,
        user,
        properties={
            "addresses": relationship(Address, backref="user", order_by=address.c.id),
        },
    )

    mapper_registry.map_imperatively(Address, address)

The same warning mentioned in :ref:`orm_declarative_dataclasses_imperative_table`
applies when using this mapping style.

.. _orm_declarative_attrs_imperative_table:

Applying ORM mappings to an existing attrs class
-------------------------------------------------

.. warning:: The ``attrs`` library is not part of SQLAlchemy's continuous
   integration testing, and compatibility with this library may change without
   notice due to incompatibilities introduced by either side.


The attrs_ library is a popular third party library that provides similar
features as dataclasses, with many additional features provided not
found in ordinary dataclasses.

A class augmented with attrs_ uses the ``@define`` decorator. This decorator
initiates a process to scan the class for attributes that define the class'
behavior, which are then used to generate methods, documentation, and
annotations.

The SQLAlchemy ORM supports mapping an attrs_ class using **Imperative** mapping.
The general form of this style is equivalent to the
:ref:`orm_imperative_dataclasses` mapping form used with
dataclasses, where the class construction uses ``attrs`` alone, with ORM mappings
applied after the fact without any class attribute scanning.

The ``@define`` decorator of attrs_ by default replaces the annotated class
with a new __slots__ based class, which is not supported. When using the old
style annotation ``@attr.s`` or using ``define(slots=False)``, the class
does not get replaced. Furthermore ``attrs`` removes its own class-bound attributes
after the decorator runs, so that SQLAlchemy's mapping process takes over these
attributes without any issue. Both decorators, ``@attr.s`` and ``@define(slots=False)``
work with SQLAlchemy.

.. versionchanged:: 2.0  SQLAlchemy integration with ``attrs`` works only
   with imperative mapping style, that is, not using Declarative.
   The introduction of ORM Annotated Declarative style is not cross-compatible
   with ``attrs``.

The ``attrs`` class is built first.  The SQLAlchemy ORM mapping can be
applied after the fact using :meth:`_orm.registry.map_imperatively`::

    from __future__ import annotations

    from typing import List

    from attrs import define
    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import MetaData
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()


    @define(slots=False)
    class User:
        id: int
        name: str
        fullname: str
        nickname: str
        addresses: List[Address]


    @define(slots=False)
    class Address:
        id: int
        user_id: int
        email_address: Optional[str]


    metadata_obj = MetaData()

    user = Table(
        "user",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("fullname", String(50)),
        Column("nickname", String(12)),
    )

    address = Table(
        "address",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("user.id")),
        Column("email_address", String(50)),
    )

    mapper_registry.map_imperatively(
        User,
        user,
        properties={
            "addresses": relationship(Address, backref="user", order_by=address.c.id),
        },
    )

    mapper_registry.map_imperatively(Address, address)

.. _dataclass: https://docs.python.org/3/library/dataclasses.html
.. _dataclasses: https://docs.python.org/3/library/dataclasses.html
.. _attrs: https://pypi.org/project/attrs/
.. _mypy: https://mypy.readthedocs.io/en/stable/
.. _pyright: https://github.com/microsoft/pyright

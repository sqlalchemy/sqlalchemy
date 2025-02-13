.. _orm_mixins_toplevel:

Composing Mapped Hierarchies with Mixins
========================================

A common need when mapping classes using the :ref:`Declarative
<orm_declarative_mapping>` style is to share common functionality, such as
particular columns, table or mapper options, naming schemes, or other mapped
properties, across many classes.  When using declarative mappings, this idiom
is supported via the use of :term:`mixin classes`, as well as via augmenting the declarative base
class itself.

.. tip::  In addition to mixin classes, common column options may also be
   shared among many classes using :pep:`593` ``Annotated`` types; see
   :ref:`orm_declarative_mapped_column_type_map_pep593` and
   :ref:`orm_declarative_mapped_column_pep593` for background on these
   SQLAlchemy 2.0 features.

An example of some commonly mixed-in idioms is below::

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class CommonMixin:
        """define a series of common elements that may be applied to mapped
        classes using this class as a mixin class."""

        @declared_attr.directive
        def __tablename__(cls) -> str:
            return cls.__name__.lower()

        __table_args__ = {"mysql_engine": "InnoDB"}
        __mapper_args__ = {"eager_defaults": True}

        id: Mapped[int] = mapped_column(primary_key=True)


    class HasLogRecord:
        """mark classes that have a many-to-one relationship to the
        ``LogRecord`` class."""

        log_record_id: Mapped[int] = mapped_column(ForeignKey("logrecord.id"))

        @declared_attr
        def log_record(self) -> Mapped["LogRecord"]:
            return relationship("LogRecord")


    class LogRecord(CommonMixin, Base):
        log_info: Mapped[str]


    class MyModel(CommonMixin, HasLogRecord, Base):
        name: Mapped[str]

The above example illustrates a class ``MyModel`` which includes two mixins
``CommonMixin`` and ``HasLogRecord`` in its bases, as well as a supplementary
class ``LogRecord`` which also includes ``CommonMixin``, demonstrating a
variety of constructs that are supported on mixins and base classes, including:

* columns declared using :func:`_orm.mapped_column`, :class:`_orm.Mapped`
  or :class:`_schema.Column` are copied from mixins or base classes onto
  the target class to be mapped; above this is illustrated via the
  column attributes ``CommonMixin.id`` and ``HasLogRecord.log_record_id``.
* Declarative directives such as ``__table_args__`` and ``__mapper_args__``
  can be assigned to a mixin or base class, where they will take effect
  automatically for any classes which inherit from the mixin or base.
  The above example illustrates this using
  the ``__table_args__`` and ``__mapper_args__`` attributes.
* All Declarative directives, including all of ``__tablename__``, ``__table__``,
  ``__table_args__`` and ``__mapper_args__``,  may be implemented using
  user-defined class methods, which are decorated with the
  :class:`_orm.declared_attr` decorator (specifically the
  :attr:`_orm.declared_attr.directive` sub-member, more on that in a moment).
  Above, this is illustrated using a ``def __tablename__(cls)`` classmethod that
  generates a :class:`.Table` name dynamically; when applied to the
  ``MyModel`` class, the table name will be generated as ``"mymodel"``, and
  when applied to the ``LogRecord`` class, the table name will be generated
  as ``"logrecord"``.
* Other ORM properties such as :func:`_orm.relationship` can be generated
  on the target class to be mapped using user-defined class methods also
  decorated with the :class:`_orm.declared_attr` decorator.  Above, this is
  illustrated by generating a many-to-one :func:`_orm.relationship` to a mapped
  object called ``LogRecord``.

The features above may all be demonstrated using a :func:`_sql.select`
example:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> print(select(MyModel).join(MyModel.log_record))
    {printsql}SELECT mymodel.name, mymodel.id, mymodel.log_record_id
    FROM mymodel JOIN logrecord ON logrecord.id = mymodel.log_record_id

.. tip:: The examples of :class:`_orm.declared_attr` will attempt to illustrate
   the correct :pep:`484` annotations for each method example.  The use of annotations with
   :class:`_orm.declared_attr` functions are **completely optional**, and
   are not
   consumed by Declarative; however, these annotations are required in order
   to pass Mypy ``--strict`` type checking.

   Additionally, the :attr:`_orm.declared_attr.directive` sub-member
   illustrated above is optional as well, and is only significant for
   :pep:`484` typing tools, as it adjusts for the expected return type when
   creating methods to override Declarative directives such as
   ``__tablename__``, ``__mapper_args__`` and ``__table_args__``.

   .. versionadded:: 2.0  As part of :pep:`484` typing support for the
      SQLAlchemy ORM, added the :attr:`_orm.declared_attr.directive` to
      :class:`_orm.declared_attr` to distinguish between :class:`_orm.Mapped`
      attributes and Declarative configurational attributes

There's no fixed convention for the order of mixins and base classes.
Normal Python method resolution rules apply, and
the above example would work just as well with::

    class MyModel(Base, HasLogRecord, CommonMixin):
        name: Mapped[str] = mapped_column()

This works because ``Base`` here doesn't define any of the variables that
``CommonMixin`` or ``HasLogRecord`` defines, i.e. ``__tablename__``,
``__table_args__``, ``id``, etc. If the ``Base`` did define an attribute of the
same name, the class placed first in the inherits list would determine which
attribute is used on the newly defined class.

.. tip::  While the above example is using
   :ref:`Annotated Declarative Table <orm_declarative_mapped_column>` form
   based on the :class:`_orm.Mapped` annotation class, mixin classes also work
   perfectly well with non-annotated and legacy Declarative forms, such as when
   using :class:`_schema.Column` directly instead of
   :func:`_orm.mapped_column`.

.. versionchanged:: 2.0 For users coming from the 1.4 series of SQLAlchemy
   who may have been using the :ref:`mypy plugin <mypy_toplevel>`, the
   :func:`_orm.declarative_mixin` class decorator is no longer needed
   to mark declarative mixins, assuming the mypy plugin is no longer in use.


Augmenting the Base
~~~~~~~~~~~~~~~~~~~

In addition to using a pure mixin, most of the techniques in this
section can also be applied to the base class directly, for patterns that
should apply to all classes derived from a particular base.  The example
below illustrates some of the previous section's example in terms of the
``Base`` class::

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        """define a series of common elements that may be applied to mapped
        classes using this class as a base class."""

        @declared_attr.directive
        def __tablename__(cls) -> str:
            return cls.__name__.lower()

        __table_args__ = {"mysql_engine": "InnoDB"}
        __mapper_args__ = {"eager_defaults": True}

        id: Mapped[int] = mapped_column(primary_key=True)


    class HasLogRecord:
        """mark classes that have a many-to-one relationship to the
        ``LogRecord`` class."""

        log_record_id: Mapped[int] = mapped_column(ForeignKey("logrecord.id"))

        @declared_attr
        def log_record(self) -> Mapped["LogRecord"]:
            return relationship("LogRecord")


    class LogRecord(Base):
        log_info: Mapped[str]


    class MyModel(HasLogRecord, Base):
        name: Mapped[str]

Where above, ``MyModel`` as well as ``LogRecord``, in deriving from
``Base``, will both have their table name derived from their class name,
a primary key column named ``id``, as well as the above table and mapper
arguments defined by ``Base.__table_args__`` and ``Base.__mapper_args__``.

When using legacy :func:`_orm.declarative_base` or :meth:`_orm.registry.generate_base`,
the :paramref:`_orm.declarative_base.cls` parameter may be used as follows
to generate an equivalent effect, as illustrated in the non-annotated
example below::

    # legacy declarative_base() use

    from sqlalchemy import Integer, String
    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import declarative_base
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base:
        """define a series of common elements that may be applied to mapped
        classes using this class as a base class."""

        @declared_attr.directive
        def __tablename__(cls):
            return cls.__name__.lower()

        __table_args__ = {"mysql_engine": "InnoDB"}
        __mapper_args__ = {"eager_defaults": True}

        id = mapped_column(Integer, primary_key=True)


    Base = declarative_base(cls=Base)


    class HasLogRecord:
        """mark classes that have a many-to-one relationship to the
        ``LogRecord`` class."""

        log_record_id = mapped_column(ForeignKey("logrecord.id"))

        @declared_attr
        def log_record(self):
            return relationship("LogRecord")


    class LogRecord(Base):
        log_info = mapped_column(String)


    class MyModel(HasLogRecord, Base):
        name = mapped_column(String)

Mixing in Columns
~~~~~~~~~~~~~~~~~

Columns can be indicated in mixins assuming the
:ref:`Declarative table <orm_declarative_table>` style of configuration
is in use (as opposed to
:ref:`imperative table <orm_imperative_table_configuration>` configuration),
so that columns declared on the mixin can then be copied to be
part of the :class:`_schema.Table` that the Declarative process generates.
All three of the :func:`_orm.mapped_column`, :class:`_orm.Mapped`,
and :class:`_schema.Column` constructs may be declared inline in a
declarative mixin::

    class TimestampMixin:
        created_at: Mapped[datetime] = mapped_column(default=func.now())
        updated_at: Mapped[datetime]


    class MyModel(TimestampMixin, Base):
        __tablename__ = "test"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

Where above, all declarative classes that include ``TimestampMixin``
in their class bases will automatically include a column ``created_at``
that applies a timestamp to all row insertions, as well as an ``updated_at``
column, which does not include a default for the purposes of the example
(if it did, we would use the :paramref:`_schema.Column.onupdate` parameter
which is accepted by :func:`_orm.mapped_column`).  These column constructs
are always **copied from the originating mixin or base class**, so that the
same mixin/base class may be applied to any number of target classes
which will each have their own column constructs.

All Declarative column forms are supported by mixins, including:

* **Annotated attributes**  - with or without :func:`_orm.mapped_column` present::

    class TimestampMixin:
        created_at: Mapped[datetime] = mapped_column(default=func.now())
        updated_at: Mapped[datetime]

* **mapped_column** - with or without :class:`_orm.Mapped` present::

    class TimestampMixin:
        created_at = mapped_column(default=func.now())
        updated_at: Mapped[datetime] = mapped_column()

* **Column** - legacy Declarative form::

    class TimestampMixin:
        created_at = Column(DateTime, default=func.now())
        updated_at = Column(DateTime)

In each of the above forms, Declarative handles the column-based attributes
on the mixin class by creating a **copy** of the construct, which is then
applied to the target class.

.. versionchanged:: 2.0 The declarative API can now accommodate
   :class:`_schema.Column` objects as well as :func:`_orm.mapped_column`
   constructs of any form when using mixins without the need to use
   :func:`_orm.declared_attr`.  Previous limitations which prevented columns
   with :class:`_schema.ForeignKey` elements from being used directly
   in mixins have been removed.


.. _orm_declarative_mixins_relationships:

Mixing in Relationships
~~~~~~~~~~~~~~~~~~~~~~~

Relationships created by :func:`~sqlalchemy.orm.relationship` are provided
with declarative mixin classes exclusively using the
:class:`_orm.declared_attr` approach, eliminating any ambiguity
which could arise when copying a relationship and its possibly column-bound
contents. Below is an example which combines a foreign key column and a
relationship so that two classes ``Foo`` and ``Bar`` can both be configured to
reference a common target class via many-to-one::

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class RefTargetMixin:
        target_id: Mapped[int] = mapped_column(ForeignKey("target.id"))

        @declared_attr
        def target(cls) -> Mapped["Target"]:
            return relationship("Target")


    class Foo(RefTargetMixin, Base):
        __tablename__ = "foo"
        id: Mapped[int] = mapped_column(primary_key=True)


    class Bar(RefTargetMixin, Base):
        __tablename__ = "bar"
        id: Mapped[int] = mapped_column(primary_key=True)


    class Target(Base):
        __tablename__ = "target"
        id: Mapped[int] = mapped_column(primary_key=True)

With the above mapping, each of ``Foo`` and ``Bar`` contain a relationship
to ``Target`` accessed along the ``.target`` attribute:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> print(select(Foo).join(Foo.target))
    {printsql}SELECT foo.id, foo.target_id
    FROM foo JOIN target ON target.id = foo.target_id{stop}
    >>> print(select(Bar).join(Bar.target))
    {printsql}SELECT bar.id, bar.target_id
    FROM bar JOIN target ON target.id = bar.target_id{stop}

Special arguments such as :paramref:`_orm.relationship.primaryjoin` may also
be used within mixed-in classmethods, which often need to refer to the class
that's being mapped.  For schemes that need to refer to locally mapped columns, in
ordinary cases these columns are made available by Declarative as attributes
on the mapped class which is passed as the ``cls`` argument to the
decorated classmethod.  Using this feature, we could for
example rewrite the ``RefTargetMixin.target`` method using an
explicit primaryjoin which refers to pending mapped columns on both
``Target`` and ``cls``::

    class Target(Base):
        __tablename__ = "target"
        id: Mapped[int] = mapped_column(primary_key=True)


    class RefTargetMixin:
        target_id: Mapped[int] = mapped_column(ForeignKey("target.id"))

        @declared_attr
        def target(cls) -> Mapped["Target"]:
            # illustrates explicit 'primaryjoin' argument
            return relationship("Target", primaryjoin=Target.id == cls.target_id)

.. _orm_declarative_mixins_mapperproperty:

Mixing in :func:`_orm.column_property` and other :class:`_orm.MapperProperty` classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like :func:`_orm.relationship`, other
:class:`_orm.MapperProperty` subclasses such as
:func:`_orm.column_property` also need to have class-local copies generated
when used by mixins, so are also declared within functions that are
decorated by :class:`_orm.declared_attr`.   Within the function,
other ordinary mapped columns that were declared with :func:`_orm.mapped_column`,
:class:`_orm.Mapped`, or :class:`_schema.Column` will be made available from the ``cls`` argument
so that they may be used to compose new attributes, as in the example below which adds two
columns together::

    from sqlalchemy.orm import column_property
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class SomethingMixin:
        x: Mapped[int]
        y: Mapped[int]

        @declared_attr
        def x_plus_y(cls) -> Mapped[int]:
            return column_property(cls.x + cls.y)


    class Something(SomethingMixin, Base):
        __tablename__ = "something"

        id: Mapped[int] = mapped_column(primary_key=True)

Above, we may make use of ``Something.x_plus_y`` in a statement where
it produces the full expression:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select
    >>> print(select(Something.x_plus_y))
    {printsql}SELECT something.x + something.y AS anon_1
    FROM something

.. tip::  The :class:`_orm.declared_attr` decorator causes the decorated callable
   to behave exactly as a classmethod.  However, typing tools like Pylance_
   may not be able to recognize this, which can sometimes cause it to complain
   about access to the ``cls`` variable inside the body of the function.  To
   resolve this issue when it occurs, the ``@classmethod`` decorator may be
   combined directly with :class:`_orm.declared_attr` as::


      class SomethingMixin:
          x: Mapped[int]
          y: Mapped[int]

          @declared_attr
          @classmethod
          def x_plus_y(cls) -> Mapped[int]:
              return column_property(cls.x + cls.y)

   .. versionadded:: 2.0 - :class:`_orm.declared_attr` can accommodate a
      function decorated with ``@classmethod`` to help with :pep:`484`
      integration where needed.


.. _decl_mixin_inheritance:

Using Mixins and Base Classes with Mapped Inheritance Patterns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When dealing with mapper inheritance patterns as documented at
:ref:`inheritance_toplevel`, some additional capabilities are present
when using :class:`_orm.declared_attr` either with mixin classes, or when
augmenting both mapped and un-mapped superclasses in a class hierarchy.

When defining functions decorated by :class:`_orm.declared_attr` on mixins or
base classes to be interpreted by subclasses in a mapped inheritance hierarchy,
there is an important distinction
made between functions that generate the special names used by Declarative such
as ``__tablename__``, ``__mapper_args__`` vs. those that may generate ordinary
mapped attributes such as :func:`_orm.mapped_column` and
:func:`_orm.relationship`.  Functions that define **Declarative directives** are
**invoked for each subclass in a hierarchy**, whereas functions that
generate **mapped attributes** are **invoked only for the first mapped
superclass in a hierarchy**.

The rationale for this difference in behavior is based on the fact that
mapped properties are already inheritable by classes, such as a particular
column on a superclass' mapped table should not be duplicated to that of a
subclass as well, whereas elements that are specific to a particular
class or its mapped table are not inheritable, such as the name of the
table that is locally mapped.

The difference in behavior between these two use cases is demonstrated
in the following two sections.

Using :func:`_orm.declared_attr` with inheriting :class:`.Table` and :class:`.Mapper` arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A common recipe with mixins is to create a ``def __tablename__(cls)``
function that generates a name for the mapped :class:`.Table` dynamically.

This recipe can be used to generate table names for an inheriting mapper
hierarchy as in the example below which creates a mixin that gives every class a simple table
name based on class name.  The recipe is illustrated below where a table name
is generated for the ``Person`` mapped class and the ``Engineer`` subclass
of ``Person``, but not for the ``Manager`` subclass of ``Person``::

    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class Tablename:
        @declared_attr.directive
        def __tablename__(cls) -> Optional[str]:
            return cls.__name__.lower()


    class Person(Tablename, Base):
        id: Mapped[int] = mapped_column(primary_key=True)
        discriminator: Mapped[str]
        __mapper_args__ = {"polymorphic_on": "discriminator"}


    class Engineer(Person):
        id: Mapped[int] = mapped_column(ForeignKey("person.id"), primary_key=True)

        primary_language: Mapped[str]

        __mapper_args__ = {"polymorphic_identity": "engineer"}


    class Manager(Person):
        @declared_attr.directive
        def __tablename__(cls) -> Optional[str]:
            """override __tablename__ so that Manager is single-inheritance to Person"""

            return None

        __mapper_args__ = {"polymorphic_identity": "manager"}

In the above example, both the ``Person`` base class as well as the
``Engineer`` class, being subclasses of the ``Tablename`` mixin class which
generates new table names, will have a generated ``__tablename__``
attribute, which to
Declarative indicates that each class should have its own :class:`.Table`
generated to which it will be mapped.   For the ``Engineer`` subclass, the style of inheritance
applied is :ref:`joined table inheritance <joined_inheritance>`, as it
will be mapped to a table ``engineer`` that joins to the base ``person``
table.  Any other subclasses that inherit from ``Person`` will also have
this style of inheritance applied by default (and within this particular example, would need to
each specify a primary key column; more on that in the next section).

By contrast, the ``Manager`` subclass of ``Person`` **overrides** the
``__tablename__`` classmethod to return ``None``.   This indicates to
Declarative that this class should **not** have a :class:`.Table` generated,
and will instead make use exclusively of the base :class:`.Table` to which
``Person`` is mapped.  For the ``Manager`` subclass, the style of inheritance
applied is :ref:`single table inheritance <single_inheritance>`.

The example above illustrates that Declarative directives like
``__tablename__`` are necessarily **applied to each subclass** individually,
as each mapped class needs to state which :class:`.Table` it will be mapped
towards, or if it will map itself to the inheriting superclass' :class:`.Table`.

If we instead wanted to **reverse** the default table scheme illustrated
above, so that
single table inheritance were the default and joined table inheritance
could be defined only when a ``__tablename__`` directive were supplied to
override it, we can make use of
Declarative helpers within the top-most ``__tablename__()`` method, in this
case a helper called :func:`.has_inherited_table`.  This function will
return ``True`` if a superclass is already mapped to a :class:`.Table`.
We may use this helper within the base-most ``__tablename__()`` classmethod
so that we may **conditionally** return ``None`` for the table name,
if a table is already present, thus indicating single-table inheritance
for inheriting subclasses by default::

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import has_inherited_table
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class Tablename:
        @declared_attr.directive
        def __tablename__(cls):
            if has_inherited_table(cls):
                return None
            return cls.__name__.lower()


    class Person(Tablename, Base):
        id: Mapped[int] = mapped_column(primary_key=True)
        discriminator: Mapped[str]
        __mapper_args__ = {"polymorphic_on": "discriminator"}


    class Engineer(Person):
        @declared_attr.directive
        def __tablename__(cls):
            """override __tablename__ so that Engineer is joined-inheritance to Person"""

            return cls.__name__.lower()

        id: Mapped[int] = mapped_column(ForeignKey("person.id"), primary_key=True)

        primary_language: Mapped[str]

        __mapper_args__ = {"polymorphic_identity": "engineer"}


    class Manager(Person):
        __mapper_args__ = {"polymorphic_identity": "manager"}

.. _mixin_inheritance_columns:

Using :func:`_orm.declared_attr` to generate table-specific inheriting columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In contrast to how ``__tablename__`` and other special names are handled when
used with :class:`_orm.declared_attr`, when we mix in columns and properties (e.g.
relationships, column properties, etc.), the function is
invoked for the **base class only** in the hierarchy, unless the
:class:`_orm.declared_attr` directive is used in combination with the
:attr:`_orm.declared_attr.cascading` sub-directive.  Below, only the
``Person`` class will receive a column
called ``id``; the mapping will fail on ``Engineer``, which is not given
a primary key::

    class HasId:
        id: Mapped[int] = mapped_column(primary_key=True)


    class Person(HasId, Base):
        __tablename__ = "person"

        discriminator: Mapped[str]
        __mapper_args__ = {"polymorphic_on": "discriminator"}


    # this mapping will fail, as there's no primary key
    class Engineer(Person):
        __tablename__ = "engineer"

        primary_language: Mapped[str]
        __mapper_args__ = {"polymorphic_identity": "engineer"}

It is usually the case in joined-table inheritance that we want distinctly
named columns on each subclass.  However in this case, we may want to have
an ``id`` column on every table, and have them refer to each other via
foreign key.  We can achieve this as a mixin by using the
:attr:`.declared_attr.cascading` modifier, which indicates that the
function should be invoked **for each class in the hierarchy**, in *almost*
(see warning below) the same way as it does for ``__tablename__``::

    class HasIdMixin:
        @declared_attr.cascading
        def id(cls) -> Mapped[int]:
            if has_inherited_table(cls):
                return mapped_column(ForeignKey("person.id"), primary_key=True)
            else:
                return mapped_column(Integer, primary_key=True)


    class Person(HasIdMixin, Base):
        __tablename__ = "person"

        discriminator: Mapped[str]
        __mapper_args__ = {"polymorphic_on": "discriminator"}


    class Engineer(Person):
        __tablename__ = "engineer"

        primary_language: Mapped[str]
        __mapper_args__ = {"polymorphic_identity": "engineer"}

.. warning::

    The :attr:`.declared_attr.cascading` feature currently does
    **not** allow for a subclass to override the attribute with a different
    function or value.  This is a current limitation in the mechanics of
    how ``@declared_attr`` is resolved, and a warning is emitted if
    this condition is detected.   This limitation only applies to
    ORM mapped columns, relationships, and other :class:`.MapperProperty`
    styles of attribute.  It does **not** apply to Declarative directives
    such as ``__tablename__``, ``__mapper_args__``, etc., which
    resolve in a different way internally than that of
    :attr:`.declared_attr.cascading`.


Combining Table/Mapper Arguments from Multiple Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the case of ``__table_args__`` or ``__mapper_args__``
specified with declarative mixins, you may want to combine
some parameters from several mixins with those you wish to
define on the class itself. The
:class:`_orm.declared_attr` decorator can be used
here to create user-defined collation routines that pull
from multiple collections::

    from sqlalchemy.orm import declared_attr


    class MySQLSettings:
        __table_args__ = {"mysql_engine": "InnoDB"}


    class MyOtherMixin:
        __table_args__ = {"info": "foo"}


    class MyModel(MySQLSettings, MyOtherMixin, Base):
        __tablename__ = "my_model"

        @declared_attr.directive
        def __table_args__(cls):
            args = dict()
            args.update(MySQLSettings.__table_args__)
            args.update(MyOtherMixin.__table_args__)
            return args

        id = mapped_column(Integer, primary_key=True)

.. _orm_mixins_named_constraints:

Creating Indexes and Constraints with Naming Conventions on Mixins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using named constraints such as :class:`.Index`, :class:`.UniqueConstraint`,
:class:`.CheckConstraint`, where each object is to be unique to a specific
table descending from a mixin, requires that an individual instance of each
object is created per actual mapped class.

As a simple example, to define a named, potentially multicolumn :class:`.Index`
that applies to all tables derived from a mixin, use the "inline" form of
:class:`.Index` and establish it as part of ``__table_args__``, using
:class:`.declared_attr` to establish ``__table_args__()`` as a class method
that will be invoked for each subclass::

    class MyMixin:
        a = mapped_column(Integer)
        b = mapped_column(Integer)

        @declared_attr.directive
        def __table_args__(cls):
            return (Index(f"test_idx_{cls.__tablename__}", "a", "b"),)


    class MyModelA(MyMixin, Base):
        __tablename__ = "table_a"
        id = mapped_column(Integer, primary_key=True)


    class MyModelB(MyMixin, Base):
        __tablename__ = "table_b"
        id = mapped_column(Integer, primary_key=True)

The above example would generate two tables ``"table_a"`` and ``"table_b"``, with
indexes ``"test_idx_table_a"`` and ``"test_idx_table_b"``

Typically, in modern SQLAlchemy we would use a naming convention,
as documented at :ref:`constraint_naming_conventions`.   While naming conventions
take place automatically using the :paramref:`_schema.MetaData.naming_convention`
as new :class:`.Constraint` objects are created, as this convention is applied
at object construction time based on the parent :class:`.Table` for a particular
:class:`.Constraint`, a distinct :class:`.Constraint` object needs to be created
for each inheriting subclass with its own :class:`.Table`, again using
:class:`.declared_attr` with ``__table_args__()``, below illustrated using
an abstract mapped base::

    from uuid import UUID

    from sqlalchemy import CheckConstraint
    from sqlalchemy import create_engine
    from sqlalchemy import MetaData
    from sqlalchemy import UniqueConstraint
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column

    constraint_naming_conventions = {
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }


    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=constraint_naming_conventions)


    class MyAbstractBase(Base):
        __abstract__ = True

        @declared_attr.directive
        def __table_args__(cls):
            return (
                UniqueConstraint("uuid"),
                CheckConstraint("x > 0 OR y < 100", name="xy_chk"),
            )

        id: Mapped[int] = mapped_column(primary_key=True)
        uuid: Mapped[UUID]
        x: Mapped[int]
        y: Mapped[int]


    class ModelAlpha(MyAbstractBase):
        __tablename__ = "alpha"


    class ModelBeta(MyAbstractBase):
        __tablename__ = "beta"

The above mapping will generate DDL that includes table-specific names
for all constraints, including primary key, CHECK constraint, unique
constraint:

.. sourcecode:: sql

    CREATE TABLE alpha (
        id INTEGER NOT NULL,
        uuid CHAR(32) NOT NULL,
        x INTEGER NOT NULL,
        y INTEGER NOT NULL,
        CONSTRAINT pk_alpha PRIMARY KEY (id),
        CONSTRAINT uq_alpha_uuid UNIQUE (uuid),
        CONSTRAINT ck_alpha_xy_chk CHECK (x > 0 OR y < 100)
    )


    CREATE TABLE beta (
        id INTEGER NOT NULL,
        uuid CHAR(32) NOT NULL,
        x INTEGER NOT NULL,
        y INTEGER NOT NULL,
        CONSTRAINT pk_beta PRIMARY KEY (id),
        CONSTRAINT uq_beta_uuid UNIQUE (uuid),
        CONSTRAINT ck_beta_xy_chk CHECK (x > 0 OR y < 100)
    )



.. _Pylance: https://github.com/microsoft/pylance-release


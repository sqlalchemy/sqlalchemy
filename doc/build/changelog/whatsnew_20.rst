.. _whatsnew_20_toplevel:

=============================
What's New in SQLAlchemy 2.0?
=============================

.. admonition:: Note for Readers

    SQLAlchemy 2.0's transition documents are separated into **two**
    documents - one which details major API shifts from the 1.x to 2.x
    series, and the other which details new features and behaviors relative
    to SQLAlchemy 1.4:

    * :ref:`migration_20_toplevel` - 1.x to 2.x API shifts
    * :ref:`whatsnew_20_toplevel` - this document, new features and behaviors for SQLAlchemy 2.0

    Readers who have not yet updated their 1.4 application to follow
    SQLAlchemy 2.0 engine and ORM conventions may navigate to
    :ref:`migration_20_toplevel` for a guide to ensuring SQLAlchemy 2.0
    compatibility, which is a prerequisite for having working code under
    version 2.0.


.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.4
    and SQLAlchemy version 2.0, **independent** of the major changes between
    :term:`1.x style` and :term:`2.0 style` usage.   Readers should start
    with the :ref:`migration_20_toplevel` document to get an overall picture
    of the major compatibility changes between the 1.x and 2.x series.

    Aside from the major 1.x->2.x migration path, the next largest
    paradigm shift in SQLAlchemy 2.0 is deep integration with :pep:`484` typing
    practices and current capabilities, particularly within the ORM. New
    type-driven ORM declarative styles inspired by Python dataclasses_, as well
    as new integrations with dataclasses themselves, complement an overall
    approach that no longer requires stubs and also goes very far towards
    providing a type-aware method chain from SQL statement to result set.

    The prominence of Python typing is significant not only so that type checkers
    like mypy_ can run without plugins; more significantly it allows IDEs
    like vscode_ and pycharm_ to take a much more active role in assisting
    with the composition of a SQLAlchemy application.


.. _typeshed: https://github.com/python/typeshed

.. _dataclasses: https://docs.python.org/3/library/dataclasses.html

.. _mypy: https://mypy.readthedocs.io/en/stable/

.. _vscode: https://code.visualstudio.com/

.. _pylance: https://github.com/microsoft/pylance-release

.. _pycharm: https://www.jetbrains.com/pycharm/


New Typing Support in Core and ORM - Stubs / Extensions no longer used
=======================================================================


The approach to typing for Core and ORM has been completely reworked, compared
to the interim approach that was provided in version 1.4 via the
sqlalchemy2-stubs_ package.   The new approach begins at the most fundamental
element in SQLAlchemy which is the :class:`_schema.Column`, or more
accurately the :class:`.ColumnElement` that underlies all SQL
expressions that have a type.   This expression-level typing then extends into the area of
statement construction, statement execution, and result sets, and finally into the ORM
where new :ref:`declarative <orm_declarative_mapper_config_toplevel>` forms allow
for fully typed ORM models that integrate all the way from statement to
result set.

SQL Expression / Statement / Result Set Typing
----------------------------------------------

This section provides background and examples for SQLAlchemy's new
SQL expression typing approach, which extends from base :class:`.ColumnElement`
constructs through SQL statements and result sets and into realm of ORM mapping.

Rationale and Overview
^^^^^^^^^^^^^^^^^^^^^^

.. tip::

  This section is an architectural discussion. Skip ahead to
  :ref:`whatsnew_20_expression_typing_examples` to just see what the new typing
  looks like.

In sqlalchemy2-stubs_, SQL expressions were typed as generics_ that then
referred to a :class:`.TypeEngine` object such as :class:`.Integer`,
:class:`.DateTime`, or :class:`.String` as their generic argument
(such as ``Column[Integer]``). This was itself a departure from what
the original Dropbox sqlalchemy-stubs_ package did, where
:class:`.Column` and its foundational constructs were directly generic on
Python types, such as ``int``, ``datetime`` and ``str``.   It was hoped
that since :class:`.Integer` / :class:`.DateTime` / :class:`.String` themselves
are generic against ``int`` / ``datetime`` / ``str``, there would be ways
to maintain both levels of information and to be able to extract the Python
type from a column expression via the :class:`.TypeEngine` as an intermediary
construct.  However, this is not the case, as :pep:`484`
doesn't really have a rich enough feature set for this to be viable,
lacking capabilities such as
`higher kinded TypeVars <https://github.com/python/typing/issues/548>`_.

So after a `deep assessment <https://github.com/python/typing/discussions/999>`_
of the current capabilities of :pep:`484`, SQLAlchemy 2.0 has realized the
original wisdom of sqlalchemy-stubs_ in this area and returned to linking
column expressions directly to Python types.  This does mean that if one
has SQL expressions to different subtypes, like ``Column(VARCHAR)`` vs.
``Column(Unicode)``, the specifics of those two :class:`.String` subtypes
is not carried along as the type only carries along ``str``,
but in practice this is usually not an issue and it is generally vastly more
useful that the Python type is immediately present, as it represents the
in-Python data one will be storing and receiving for this column directly.

Concretely, this means that an expression like ``Column('id', Integer)``
is typed as ``Column[int]``.    This allows for a viable pipeline of
SQLAlchemy construct -> Python datatype to be set up, without the need for
typing plugins.  Crucially, it allows full interoperability with
the ORM's paradigm of using :func:`_sql.select` and :class:`_engine.Row`
constructs that reference ORM mapped class types (e.g. a :class:`_engine.Row`
containing instances of user-mapped instances, such as the ``User`` and
``Address`` examples used in our tutorials).   While Python typing currently has very limited
support for customization of tuple-types (where :pep:`646`, the first pep that
attempts to deal with tuple-like objects, was `intentionally limited
in its functionality <https://mail.python.org/archives/list/typing-sig@python.org/message/G2PNHRR32JMFD3JR7ACA2NDKWTDSEPUG/>`_
and by itself is not yet viable for arbitrary tuple
manipulation),
a fairly decent approach has been devised that allows for basic
:func:`_sql.select()` -> :class:`_engine.Result` -> :class:`_engine.Row` typing
to function, including for ORM classes, where at the point at which a
:class:`_engine.Row` object is to be unpacked into individual column entries,
a small typing-oriented accessor is added that allows the individual Python
values to maintain the Python type linked to the SQL expression from which
they originated (translation: it works).

.. _sqlalchemy-stubs: https://github.com/dropbox/sqlalchemy-stubs

.. _sqlalchemy2-stubs: https://github.com/sqlalchemy/sqlalchemy2-stubs

.. _generics: https://peps.python.org/pep-0484/#generics

.. _whatsnew_20_expression_typing_examples:

SQL Expression Typing - Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A brief tour of typing behaviors.  Comments
indicate what one would see hovering over the code in vscode_ (or roughly
what typing tools would display when using the `reveal_type() <https://mypy.readthedocs.io/en/latest/common_issues.html?highlight=reveal_type#reveal-type>`_
helper):

* Simple Python Types Assigned to SQL Expressions

  ::

    # (variable) str_col: ColumnClause[str]
    str_col = column("a", String)

    # (variable) int_col: ColumnClause[int]
    int_col = column("a", Integer)

    # (variable) expr1: ColumnElement[str]
    expr1 = str_col + "x"

    # (variable) expr2: ColumnElement[int]
    expr2 = int_col + 10

    # (variable) expr3: ColumnElement[bool]
    expr3 = int_col == 15

* Individual SQL expressions assigned to :func:`_sql.select` constructs, as well as any
  row-returning construct, including row-returning DML
  such as :class:`_sql.Insert` with :meth:`_sql.Insert.returning`, are packed
  into a ``Tuple[]`` type which retains the Python type for each element.

  ::

    # (variable) stmt: Select[Tuple[str, int]]
    stmt = select(str_col, int_col)

    # (variable) stmt: ReturningInsert[Tuple[str, int]]
    ins_stmt = insert(table('t')).returning(str_col, int_col)

* The ``Tuple[]`` type from any row returning construct, when invoked with an
  ``.execute()`` method, carries through to :class:`_engine.Result`
  and :class:`_engine.Row`.  In order to unpack the :class:`_engine.Row`
  object as a tuple, the :meth:`_engine.Row.tuple` or :attr:`_engine.Row.t`
  accessor essentially casts the :class:`_engine.Row` into the corresponding
  ``Tuple[]`` (though remains the same :class:`_engine.Row` object at runtime).

  ::

    with engine.connect() as conn:

        # (variable) stmt: Select[Tuple[str, int]]
        stmt = select(str_col, int_col)

        # (variable) result: Result[Tuple[str, int]]
        result = conn.execute(stmt)

        # (variable) row: Row[Tuple[str, int]] | None
        row = result.first()

        if row is not None:
          # for typed tuple unpacking or indexed access,
          # use row.tuple() or row.t  (this is the small typing-oriented accessor)
          strval, intval = row.t

          # (variable) strval: str
          strval

          # (variable) intval: int
          intval

* Scalar values for single-column statements do the right thing with
  methods like :meth:`_engine.Connection.scalar`, :meth:`_engine.Result.scalars`,
  etc.

  ::

    # (variable) data: Sequence[str]
    data = connection.execute(select(str_col)).scalars().all()

* The above support for row-returning constructs works the best with
  ORM mapped classes, as a mapped class can list out specific types
  for its members.  The example below sets up a class using
  :ref:`new type-aware syntaxes <whatsnew_20_orm_declarative_typing>`,
  described in the following section::

      from sqlalchemy.orm import DeclarativeBase
      from sqlalchemy.orm import Mapped
      from sqlalchemy.orm import mapped_column


      class Base(DeclarativeBase):
          pass


      class User(Base):
          __tablename__ = 'user_account'

          id: Mapped[int] = mapped_column(primary_key=True)
          name: Mapped[str]
          addresses: Mapped[List["Address"]] = relationship()

      class Address(Base):
          __tablename__ = "address"

          id: Mapped[int] = mapped_column(primary_key=True)
          email_address: Mapped[str]
          user_id = mapped_column(ForeignKey("user_account.id"))


  With the above mapping, the attributes are typed and express themselves
  all the way from statement to result set::

      with Session(engine) as session:

          # (variable) stmt: Select[Tuple[int, str]]
          stmt_1 = select(User.id, User.name)

          # (variable) result_1: Result[Tuple[int, str]]
          result_1 = session.execute(stmt_1)

          # (variable) intval: int
          # (variable) strval: str
          intval, strval = result_1.one().t

  Mapped classes themselves are also types, and behave the same way, such
  as a SELECT against two mapped classes::

      with Session(engine) as session:

          # (variable) stmt: Select[Tuple[User, Address]]
          stmt_2 = select(User, Address).join_from(User, Address)

          # (variable) result_2: Result[Tuple[User, Address]]
          result_2 = session.execute(stmt_2)

          # (variable) user_obj: User
          # (variable) address_obj: Address
          user_obj, address_obj = result_2.one().t

  When selecting mapped classes, constructs like :class:`_orm.aliased` work
  as well, maintaining the column-level attributes of the original mapped
  class as well as the return type expected from a statement::

      with Session(engine) as session:

          # this is in fact an Annotated type, but typing tools don't
          # generally display this

          # (variable) u1: Type[User]
          u1 = aliased(User)

          # (variable) stmt: Select[Tuple[User, User, str]]
          stmt = select(User, u1, User.name).filter(User.id == 5)

          # (variable) result: Result[Tuple[User, User, str]]
          result = session.execute(stmt)

* Core Table does not yet have a decent way to maintain typing of
  :class:`_schema.Column` objects when accessing them via the :attr:`.Table.c` accessor.

  Since :class:`.Table` is set up as an instance of a class, and the
  :attr:`.Table.c` accessor typically accesses :class:`.Column` objects
  dynamically by name, there's not yet an established typing approach for this; some
  alternative syntax would be needed.

* ORM classes, scalars, etc. work great.

  The typical use case of selecting ORM classes, as scalars or tuples,
  all works, both 2.0 and 1.x style queries, getting back the exact type
  either by itself or contained within the appropriate container such
  as ``Sequence[]``, ``List[]`` or ``Iterator[]``::

      # (variable) users1: Sequence[User]
      users1 = session.scalars(select(User)).all()

      # (variable) user: User
      user = session.query(User).one()

      # (variable) user_iter: Iterator[User]
      user_iter = iter(session.scalars(select(User)))

* Legacy :class:`_orm.Query` gains tuple typing as well.

  The typing support for :class:`_orm.Query` goes well beyond what
  sqlalchemy-stubs_ or sqlalchemy2-stubs_ offered, where both scalar-object
  as well as tuple-typed :class:`_orm.Query` objects will retain result level
  typing for most cases::

      # (variable) q1: RowReturningQuery[Tuple[int, str]]
      q1 = session.query(User.id, User.name)

      # (variable) rows: List[Row[Tuple[int, str]]]
      rows = q1.all()

      # (variable) q2: Query[User]
      q2 = session.query(User)

      # (variable) users: List[User]
      users = q2.all()

the catch - all stubs must be uninstalled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A key caveat with the typing support is that **all SQLAlchemy stubs packages
must be uninstalled** for typing to work.   When running mypy_ against a
Python virtualenv, this is only a matter of uninstalling those packages.
However, a SQLAlchemy stubs package is also currently part of typeshed_, which
itself is bundled into some typing tools such as Pylance_, so it may be
necessary in some cases to locate the files for these packages and delete them,
if they are in fact interfering with the new typing working correctly.

Once SQLAlchemy 2.0 is released in final status, typeshed will remove
SQLAlchemy from its own stubs source.



.. _whatsnew_20_orm_declarative_typing:

ORM Declarative Models
----------------------

SQLAlchemy 1.4 introduced the first SQLAlchemy-native ORM typing support
using a combination of sqlalchemy2-stubs_ and the :ref:`Mypy Plugin <mypy_toplevel>`.
In SQLAlchemy 2.0, the Mypy plugin **remains available, and has been updated
to work with SQLAlchemy 2.0's typing system**.  However, it should now be
considered **deprecated**, as applications now have a straightforward path to adopting the
new typing support that does not use plugins or stubs.

Overview
^^^^^^^^

The fundamental approach for the new system is that mapped column declarations,
when using a fully :ref:`Declarative <orm_declarative_table>` model (that is,
not :ref:`hybrid declarative <orm_imperative_table_configuration>` or
:ref:`imperative <orm_imperative_mapping>` configurations, which are unchanged),
are first derived at runtime by inspecting the type annotation on the left side
of each attribute declaration, if present.  Left hand type annotations are
expected to be contained within the
:class:`_orm.Mapped` generic type, otherwise the attribute is not considered
to be a mapped attribute.  The attribute declaration may then refer to
the :func:`_orm.mapped_column` construct on the right hand side, which is used
to provide additional Core-level schema information about the
:class:`_schema.Column` to be produced and mapped. This right hand side
declaration is optional if a :class:`_orm.Mapped` annotation is present on the
left side; if no annotation is present on the left side, then the
:func:`_orm.mapped_column` may be used as an exact replacement for the
:class:`_schema.Column` directive where it will provide for more accurate (but
not exact) typing behavior of the attribute, even though no annotation is
present.

The approach is inspired by the approach of Python dataclasses_ which starts
with an annotation on the left, then allows for an optional
``dataclasses.field()`` specification on the right; the key difference from the
dataclasses approach is that SQLAlchemy's approach is strictly **opt-in**,
where existing mappings that use :class:`_schema.Column` without any type
annotations continue to work as they always have, and the
:func:`_orm.mapped_column` construct may be used as a direct replacement for
:class:`_schema.Column` without any explicit type annotations. Only for exact
attribute-level Python types to be present is the use of explicit annotations
with :class:`_orm.Mapped` required. These annotations may be used on an
as-needed, per-attribute basis for those attributes where specific types are
helpful; non-annotated attributes that use :func:`_orm.mapped_column` will be
typed as ``Any`` at the instance level.

Migrating an Existing Mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transitioning to the new ORM approach begins as more verbose, but becomes more
succinct than was previously possible as the available new features are used
fully. The following steps detail a typical transition and then continue
on to illustrate some more options.


Step one - :func:`_orm.declarative_base` is superseded by :class:`_orm.DeclarativeBase`.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One observed limitation in Python typing is that there seems to be
no ability to have a class dynamically generated from a function which then
is understood by typing tools as a base for new classes.  To solve this problem
without plugins, the usual call to :func:`_orm.declarative_base` can be replaced
with using the :class:`_orm.DeclarativeBase` class, which produces the same
``Base`` object as usual, except that typing tools understand it::

    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

Step two - replace Declarative use of :class:`_schema.Column` with :func:`_orm.mapped_column`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_orm.mapped_column` is an ORM-typing aware construct that can
be swapped directly for the use of :class:`_schema.Column`.  Given a
1.x style mapping as::

  from sqlalchemy import Column
  from sqlalchemy.orm import relationship
  from sqlalchemy.orm import DeclarativeBase

  class Base(DeclarativeBase):
      pass

  class User(Base):
      __tablename__ = 'user_account'

      id = Column(Integer, primary_key=True)
      name = Column(String(30), nullable=False)
      fullname = Column(String)
      addresses = relationship("Address", back_populates="user")

  class Address(Base):
      __tablename__ = "address"

      id = Column(Integer, primary_key=True)
      email_address = Column(String, nullable=False)
      user_id = Column(ForeignKey("user_account.id"), nullable=False)
      user = relationship("User", back_populates="addresses")

We replace :class:`_schema.Column` with :func:`_orm.mapped_column`; no
arguments need to change::

  from sqlalchemy.orm import DeclarativeBase
  from sqlalchemy.orm import mapped_column
  from sqlalchemy.orm import relationship

  class Base(DeclarativeBase):
      pass

  class User(Base):
      __tablename__ = 'user_account'

      id = mapped_column(Integer, primary_key=True)
      name = mapped_column(String(30), nullable=False)
      fullname = mapped_column(String)
      addresses = relationship("Address", back_populates="user")

  class Address(Base):
      __tablename__ = "address"

      id = mapped_column(Integer, primary_key=True)
      email_address = mapped_column(String, nullable=False)
      user_id = mapped_column(ForeignKey("user_account.id"), nullable=False)
      user = relationship("User", back_populates="addresses")

The individual columns above are **not yet typed with Python types**,
and are instead typed as ``Mapped[Any]``; this is because we can declare any
column either with ``Optional`` or not, and there's no way to have a
"guess" in place that won't cause typing errors when we type it
explicitly.

However, at this step, our above mapping has appropriate :term:`descriptor` types
set up for all attributes and may be used in queries as well as for
instance-level manipulation, all of which will **pass mypy --strict mode** with no
plugins.

Step three - apply exact Python types as needed using :class:`_orm.Mapped`.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This can be done for all attributes for which exact typing is desired;
attributes that are fine being left as ``Any`` may be skipped.   For
context we also illustrate :class:`_orm.Mapped` being used for a
:func:`_orm.relationship` where we apply an exact type.
The mapping within this interim step
will be more verbose, however with proficiency, this step can
be combined with subsequent steps to update mappings more directly::

  from typing import List
  from typing import Optional
  from sqlalchemy.orm import DeclarativeBase
  from sqlalchemy.orm import Mapped
  from sqlalchemy.orm import mapped_column
  from sqlalchemy.orm import relationship

  class Base(DeclarativeBase):
      pass

  class User(Base):
      __tablename__ = 'user_account'

      id: Mapped[int] = mapped_column(Integer, primary_key=True)
      name: Mapped[str] = mapped_column(String(30), nullable=False)
      fullname: Mapped[Optional[str]] = mapped_column(String)
      addresses: Mapped[List["Address"]] = relationship("Address", back_populates="user")

  class Address(Base):
      __tablename__ = "address"

      id: Mapped[int] = mapped_column(Integer, primary_key=True)
      email_address: Mapped[str] = mapped_column(String, nullable=False)
      user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"), nullable=False)
      user: Mapped["User"] = relationship("User", back_populates="addresses")

At this point, our ORM mapping is fully typed and will produce exact-typed
:func:`_sql.select`, :class:`_orm.Query` and :class:`_engine.Result`
constructs.   We now can proceed to pare down redundancy in the mapping
declaration.

Step four - remove :func:`_orm.mapped_column` directives where no longer needed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All ``nullable`` parameters can be implied using ``Optional[]``; in
the absence of ``Optional[]``, ``nullable`` defaults to ``False``. All SQL
types without arguments such as ``Integer`` and ``String`` can be expressed
as a Python annotation alone. A :func:`_orm.mapped_column` directive with no
parameters can be removed entirely. :func:`_orm.relationship` now derives its
class from the left hand annotation, supporting forward references as well
(as :func:`_orm.relationship` has supported string-based forward references
for ten years already ;) )::

  from typing import List
  from typing import Optional
  from sqlalchemy.orm import DeclarativeBase
  from sqlalchemy.orm import Mapped
  from sqlalchemy.orm import mapped_column
  from sqlalchemy.orm import relationship

  class Base(DeclarativeBase):
      pass

  class User(Base):
      __tablename__ = 'user_account'

      id: Mapped[int] = mapped_column(primary_key=True)
      name: Mapped[str] = mapped_column(String(30))
      fullname: Mapped[Optional[str]]
      addresses: Mapped[List["Address"]] = relationship(back_populates="user")

  class Address(Base):
      __tablename__ = "address"

      id: Mapped[int] = mapped_column(primary_key=True)
      email_address: Mapped[str]
      user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
      user: Mapped["User"] = relationship(back_populates="addresses")


Step five - make use of pep-593 ``Annotated`` to package common directives into types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a radical new
capability that presents an alternative, or complementary approach, to
:ref:`declarative mixins <orm_mixins_toplevel>` as a means to provide type
oriented configuration, and also replaces the need for
:class:`_orm.declared_attr` decorated functions in most cases.

First, the Declarative mapping allows the mapping of Python type to
SQL type, such as ``str`` to :class:`_types.String`, to be customized
using :paramref:`_orm.registry.type_annotation_map`.   Using :pep:`593`
``Annotated`` allows us to create variants of a particular Python type so that
the same type, such as ``str``, may be used which each provide variants
of :class:`_types.String`, as below where use of an ``Annotated`` ``str`` called
``str50`` will indicate ``String(50)``::

  from typing_extensions import Annotated
  from sqlalchemy.orm import DeclarativeBase

  str50 = Annotated[str, 50]

  # declarative base with a type-level override, using a type that is
  # expected to be used in multiple places
  class Base(DeclarativeBase):
      registry = registry(type_annotation_map={
          str50: String(50),
      })


Second, Declarative will extract full
:func:`_orm.mapped_column` definitions from the left hand type if
``Annotated[]`` is used, by passing a :func:`_orm.mapped_column` construct
as any argument to the ``Annotated[]`` construct (credit to `@adriangb01 <https://twitter.com/adriangb01/status/1532841383647657988>`_
for illustrating this idea).   This capability may be extended in future releases
to also include :func:`_orm.relationship`, :func:`_orm.composite` and other
constructs, but currently is limited to :func:`_orm.mapped_column`.  The
example below adds additional ``Annotated`` types in addition to our
``str50`` example to illustrate this feature::

  from typing_extensions import Annotated
  from typing import List
  from typing import Optional
  from sqlalchemy import ForeignKey
  from sqlalchemy import String
  from sqlalchemy.orm import DeclarativeBase
  from sqlalchemy.orm import Mapped
  from sqlalchemy.orm import mapped_column
  from sqlalchemy.orm import relationship

  # declarative base from previous example
  str50 = Annotated[str, 50]

  class Base(DeclarativeBase):
      registry = registry(type_annotation_map={
          str50: String(50),
      })

  # set up mapped_column() overrides, using whole column styles that are
  # expected to be used in multiple places
  intpk = Annotated[int, mapped_column(primary_key=True)]
  user_fk = Annotated[int, mapped_column(ForeignKey('user_account.id'))]


  class User(Base):
      __tablename__ = 'user_account'

      id: Mapped[intpk]
      name: Mapped[str50]
      fullname: Mapped[Optional[str]]
      addresses: Mapped[List["Address"]] = relationship(back_populates="user")

  class Address(Base):
      __tablename__ = "address"

      id: Mapped[intpk]
      email_address: Mapped[str50]
      user_id: Mapped[user_fk]
      user: Mapped["User"] = relationship(back_populates="addresses")

Above, columns that are mapped with ``Mapped[str50]``, ``Mapped[intpk]``,
or ``Mapped[user_fk]`` draw from both the
:paramref:`_orm.registry.type_annotation_map` as well as the
``Annotated`` construct directly in order to re-use pre-established typing
and column configurations.

Step six - turn mapped classes into dataclasses_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can turn mapped classes into dataclasses_, where a key advantage
is that we can build a strictly-typed ``__init__()`` method with explicit
positional, keyword only, and default arguments, not to mention we get methods
such as ``__str__()`` and ``__repr__()`` for free. The next section
:ref:`whatsnew_20_dataclasses` illustrates further transformation of the above
model.


Typing is supported from step 3 onwards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the above examples, any example from "step 3" on forward will include
that the attributes
of the model are typed
and will populate through to :func:`_sql.select`, :class:`_orm.Query`,
and :class:`_engine.Row` objects::

    # (variable) stmt: Select[Tuple[int, str]]
    stmt = select(User.id, User.name)

    with Session(e) as sess:
        for row in sess.execute(stmt):
            # (variable) row: Row[Tuple[int, str]]
            print(row)

        # (variable) users: Sequence[User]
        users = sess.scalars(select(User)).all()

        # (variable) users_legacy: List[User]
        users_legacy = sess.query(User).all()

.. seealso::

    :ref:`orm_declarative_table` - Updated Declarative documentation for
    Declarative generation and mapping of :class:`.Table` columns.

.. _whatsnew_20_dataclasses:

Native Support for Dataclasses Mapped as ORM Models
-----------------------------------------------------

The new ORM Declarative features introduced above at
:ref:`whatsnew_20_orm_declarative_typing` introduced the
new :func:`_orm.mapped_column` construct and illustrated type-centric
mapping with optional use of :pep:`593` ``Annotated``.  We can take
the mapping one step further by integrating with with Python
dataclasses_.   This new feature is made possible via :pep:`681` which
allows for type checkers to recognize classes that are dataclass compatible,
or are fully dataclasses, but were declared through alternate APIs.

Using the dataclasses feature, mapped classes gain an ``__init__()`` method
that supports positional arguments as well as customizable default values
for optional keyword arguments.  As mentioned previously, dataclasses also
generate many useful methods such as ``__str__()``, ``__eq__()``.  Dataclass
serialization methods such as
`dataclasses.asdict() <https://docs.python.org/3/library/dataclasses.html#dataclasses.asdict>`_ and
`dataclasses.astuple() <https://docs.python.org/3/library/dataclasses.html#dataclasses.astuple>`_
also work, but don't currently accommodate for self-referential structures, which
makes them less viable for mappings that have bidirectional relationships.

SQLAlchemy's current integration approach converts the user-defined class
into a **real dataclass** to provide runtime functionality; the feature
makes use of the existing dataclass feature introduced in SQLAlchemy 1.4 at
:ref:`change_5027` to produce an equivalent runtime mapping with a fully integrated
configuration style, which is also more correctly typed than was possible
with the previous approach.

To support dataclasses in compliance with :pep:`681`, ORM constructs like
:func:`_orm.mapped_column` and :func:`_orm.relationship` accept additional
:pep:`681` arguments ``init``, ``default``, and ``default_factory`` which
are passed along to the dataclass creation process.  These
arguments currently must be present in an explicit directive on the right side,
just as they would be used with ``dataclasses.field()``; they currently
can't be local to an ``Annotated`` construct on the left side.   To support
the convenient use of ``Annotated`` while still supporting dataclass
configuration, :func:`_orm.mapped_column` can merge
a minimal set of right-hand arguments with that of an existing
:func:`_orm.mapped_column` construct located on the left side within an ``Annotated``
construct, so that most of the succinctness is maintained, as will be seen
below.

To enable dataclasses using class inheritance we make
use of the :class:`.MappedAsDataclass` mixin, either directly on each class, or
on the ``Base`` class, as illustrated below where we further modify the
example mapping from "Step 5" of :ref:`whatsnew_20_orm_declarative_typing`::

    from typing_extensions import Annotated
    from typing import List
    from typing import Optional
    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import MappedAsDataclass
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(MappedAsDataclass, DeclarativeBase):
        """subclasses will be converted to dataclasses"""

    intpk = Annotated[int, mapped_column(primary_key=True)]
    str30 = Annotated[str, mapped_column(String(30))]
    user_fk = Annotated[int, mapped_column(ForeignKey("user_account.id"))]


    class User(Base):
        __tablename__ = "user_account"

        id: Mapped[intpk] = mapped_column(init=False)
        name: Mapped[str30]
        fullname: Mapped[Optional[str]] = mapped_column(default=None)
        addresses: Mapped[List["Address"]] = relationship(
            back_populates="user", default_factory=list
        )


    class Address(Base):
        __tablename__ = "address"

        id: Mapped[intpk] = mapped_column(init=False)
        email_address: Mapped[str]
        user_id: Mapped[user_fk] = mapped_column(init=False)
        user: Mapped["User"] = relationship(
            back_populates="addresses", default=None
        )

The above mapping has used the ``@dataclasses.dataclass`` decorator directly
on each mapped class at the same time that the declarative mapping was
set up, internally setting up each ``dataclasses.field()`` directive as
indicated.   ``User`` / ``Address`` structures can be created using
positional arguments as configured::

    >>> u1 = User("username", fullname="full name", addresses=[Address("email@address")])
    >>> u1
    User(id=None, name='username', fullname='full name', addresses=[Address(id=None, email_address='email@address', user_id=None, user=...)])


.. seealso::

    :ref:`orm_declarative_native_dataclasses`


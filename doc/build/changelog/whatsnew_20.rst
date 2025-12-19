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
-----------------------------------------------------------------------


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

.. tip:: Typing support should be considered **beta level** software
   for the 2.0 series. Typing details are subject to change however
   significant backwards-incompatible changes are not planned.

.. _change_result_typing_20:

SQL Expression / Statement / Result Set Typing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    ins_stmt = insert(table("t")).returning(str_col, int_col)

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
          __tablename__ = "user_account"

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
~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy 1.4 introduced the first SQLAlchemy-native ORM typing support
using a combination of sqlalchemy2-stubs_ and the Mypy Plugin.
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

.. _whatsnew_20_orm_typing_migration:

Migrating an Existing Mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transitioning to the new ORM approach begins as more verbose, but becomes more
succinct than was previously possible as the available new features are used
fully. The following steps detail a typical transition and then continue
on to illustrate some more options.


Step one - :func:`_orm.declarative_base` is superseded by :class:`_orm.DeclarativeBase`.
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The :func:`_orm.mapped_column` is an ORM-typing aware construct that can
be swapped directly for the use of :class:`_schema.Column`.  Given a
1.x style mapping as::

    from sqlalchemy import Column
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm import DeclarativeBase


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user_account"

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
        __tablename__ = "user_account"

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
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
        __tablename__ = "user_account"

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
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
        __tablename__ = "user_account"

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
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
        type_annotation_map = {
            str50: String(50),
        }

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
        type_annotation_map = {
            str50: String(50),
        }


    # set up mapped_column() overrides, using whole column styles that are
    # expected to be used in multiple places
    intpk = Annotated[int, mapped_column(primary_key=True)]
    user_fk = Annotated[int, mapped_column(ForeignKey("user_account.id"))]


    class User(Base):
        __tablename__ = "user_account"

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
``Annotated`` construct directly in order to reuse pre-established typing
and column configurations.

Optional step - turn mapped classes into dataclasses_
+++++++++++++++++++++++++++++++++++++++++++++++++++++

We can turn mapped classes into dataclasses_, where a key advantage
is that we can build a strictly-typed ``__init__()`` method with explicit
positional, keyword only, and default arguments, not to mention we get methods
such as ``__str__()`` and ``__repr__()`` for free. The next section
:ref:`whatsnew_20_dataclasses` illustrates further transformation of the above
model.


Typing is supported from step 3 onwards
+++++++++++++++++++++++++++++++++++++++

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

.. _whatsnew_20_mypy_legacy_models:

Using Legacy Mypy-Typed Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy applications that use the Mypy plugin with
explicit annotations that don't use :class:`_orm.Mapped` in their annotations
are subject to errors under the new system, as such annotations are flagged as
errors when using constructs such as :func:`_orm.relationship`.

The section :ref:`migration_20_step_six` illustrates how to temporarily
disable these errors from being raised for a legacy ORM model that uses
explicit annotations.

.. seealso::

    :ref:`migration_20_step_six`


.. _whatsnew_20_dataclasses:

Native Support for Dataclasses Mapped as ORM Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The new ORM Declarative features introduced above at
:ref:`whatsnew_20_orm_declarative_typing` introduced the
new :func:`_orm.mapped_column` construct and illustrated type-centric
mapping with optional use of :pep:`593` ``Annotated``.  We can take
the mapping one step further by integrating this with Python
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
        user: Mapped["User"] = relationship(back_populates="addresses", default=None)

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


.. _change_6047:

Optimized ORM bulk insert now implemented for all backends other than MySQL
----------------------------------------------------------------------------

The dramatic performance improvement introduced in the 1.4 series and described
at :ref:`change_5263` has now been generalized to all included backends that
support RETURNING, which is all backends other than MySQL: SQLite, MariaDB,
PostgreSQL (all drivers), and Oracle; SQL Server has support but is
temporarily disabled in version 2.0.9 [#]_. While the original feature
was most critical for the psycopg2 driver which otherwise had major performance
issues when using ``cursor.executemany()``, the change is also critical for
other PostgreSQL drivers such as asyncpg, as when using RETURNING,
single-statement INSERT statements are still unacceptably slow, as well
as when using SQL Server that also seems to have very slow executemany
speed for INSERT statements regardless of whether or not RETURNING is used.

The performance of the new feature provides an almost across-the-board
order of magnitude performance increase for basically every driver when
INSERTing ORM objects that don't have a pre-assigned primary key value, as
indicated in the table below, in most cases specific to the use of RETURNING
which is not normally supported with executemany().

The psycopg2 "fast execution helper" approach consists of transforming an
INSERT..RETURNING statement with a single parameter set into a single
statement that INSERTs many parameter sets, using multiple "VALUES..."
clauses so that it can accommodate many parameter sets at once.
Parameter sets are then typically batched into groups of 1000
or similar, so that no single INSERT statement is excessively large, and the
INSERT statement is then invoked for each batch of parameters, rather than
for each individual parameter set.  Primary key values and server defaults
are returned by RETURNING, which continues to work as each statement execution
is invoked using ``cursor.execute()``, rather than ``cursor.executemany()``.

This allows many rows to be inserted in one statement while also being able to
return newly-generated primary key values as well as SQL and server defaults.
SQLAlchemy historically has always needed to invoke one statement per parameter
set, as it relied upon Python DBAPI Features such as ``cursor.lastrowid`` which
do not support multiple rows.

With most databases now offering RETURNING (with the conspicuous exception of
MySQL, given that MariaDB supports it), the new change generalizes the psycopg2
"fast execution helper" approach to all dialects that support RETURNING, which
now includes SQlite and MariaDB, and for which no other approach for
"executemany plus RETURNING" is possible, which includes SQLite, MariaDB, and all
PG drivers. The cx_Oracle and oracledb drivers used for Oracle
support RETURNING with executemany natively, and this has also been implemented
to provide equivalent performance improvements.  With SQLite and MariaDB now
offering RETURNING support, ORM use of ``cursor.lastrowid`` is nearly a thing
of the past, with only MySQL still relying upon it.

For INSERT statements that don't use RETURNING, traditional executemany()
behavior is used for most backends, with the current exception of psycopg2,
which has very slow executemany() performance overall
and are still improved by the "insertmanyvalues" approach.

Benchmarks
~~~~~~~~~~

SQLAlchemy includes a :ref:`Performance Suite <examples_performance>` within
the ``examples/`` directory, where we can make use of the ``bulk_insert``
suite to benchmark INSERTs of many rows using both Core and ORM in different
ways.

For the tests below, we are inserting **100,000 objects**, and in all cases we
actually have 100,000 real Python ORM objects in memory, either created up
front or generated on the fly. All databases other than SQLite are run over a
local network connection, not localhost; this causes the "slower" results to be
extremely slow.

Operations that are improved by this feature include:

* unit of work flushes for objects added to the session using
  :meth:`_orm.Session.add` and :meth:`_orm.Session.add_all`.
* The new :ref:`ORM Bulk Insert Statement <orm_queryguide_bulk_insert>` feature,
  which improves upon the experimental version of this feature first introduced
  in SQLAlchemy 1.4.
* the :class:`_orm.Session` "bulk" operations described at
  :ref:`bulk_operations`, which are superseded by the above mentioned
  ORM Bulk Insert feature.

To get a sense of the scale of the operation, below are performance
measurements using the ``test_flush_no_pk`` performance suite, which
historically represents SQLAlchemy's worst-case INSERT performance task,
where objects that don't have primary key values need to be INSERTed, and
then the newly generated primary key values must be fetched so that the
objects can be used for subsequent flush operations, such as establishment
within relationships, flushing joined-inheritance models, etc::

    @Profiler.profile
    def test_flush_no_pk(n):
        """INSERT statements via the ORM (batched with RETURNING if available),
        fetching generated row id"""
        session = Session(bind=engine)
        for chunk in range(0, n, 1000):
            session.add_all(
                [
                    Customer(
                        name="customer name %d" % i,
                        description="customer description %d" % i,
                    )
                    for i in range(chunk, chunk + 1000)
                ]
            )
            session.flush()
        session.commit()

This test can be run from any SQLAlchemy source tree as follows:

.. sourcecode:: text

    python -m examples.performance.bulk_inserts --test test_flush_no_pk

The table below summarizes performance measurements with
the latest 1.4 series of SQLAlchemy compared to 2.0, both running
the same test:

============================   ====================    ====================
Driver                         SQLA 1.4 Time (secs)    SQLA 2.0 Time (secs)
----------------------------   --------------------    --------------------
sqlite+pysqlite2 (memory)      6.204843                3.554856
postgresql+asyncpg (network)   88.292285               4.561492
postgresql+psycopg (network)   N/A (psycopg3)          4.861368
mssql+pyodbc (network)         158.396667              4.825139
oracle+cx_Oracle (network)     92.603953               4.809520
mariadb+mysqldb (network)      71.705197               4.075377
============================   ====================    ====================



.. note::

   .. [#] The feature is was temporarily disabled for SQL Server in
      SQLAlchemy 2.0.9 due to issues with row ordering when RETURNING is used.
      In SQLAlchemy 2.0.10, the feature is re-enabled, with special
      case handling for the unit of work's requirement for RETURNING to be
      ordered.

Two additional drivers have no change in performance; the psycopg2 drivers,
for which fast executemany was already implemented in SQLAlchemy 1.4,
and MySQL, which continues to not offer RETURNING support:

=============================   ====================    ====================
Driver                          SQLA 1.4 Time (secs)    SQLA 2.0 Time (secs)
-----------------------------   --------------------    --------------------
postgresql+psycopg2 (network)   4.704876                4.699883
mysql+mysqldb (network)         77.281997               76.132995
=============================   ====================    ====================

Summary of Changes
~~~~~~~~~~~~~~~~~~

The following bullets list the individual changes made within 2.0 in order to
get all drivers to this state:

* RETURNING implemented for SQLite - :ticket:`6195`
* RETURNING implemented for MariaDB - :ticket:`7011`
* Fix multi-row RETURNING for Oracle - :ticket:`6245`
* make insert() executemany() support RETURNING for as many dialects as
  possible, usually with VALUES() - :ticket:`6047`
* Emit a warning when RETURNING w/ executemany is used for non-supporting
  backend (currently no RETURNING backend has this limitation) - :ticket:`7907`
* The ORM :paramref:`_orm.Mapper.eager_defaults` parameter now defaults to a
  a new setting ``"auto"``, which will enable "eager defaults" automatically
  for INSERT statements, when the backend in use supports RETURNING with
  "insertmanyvalues".  See :ref:`orm_server_defaults` for documentation.


.. seealso::

    :ref:`engine_insertmanyvalues` - Documentation and background on the
    new feature as well as how to configure it

.. _change_8360:

ORM-enabled Insert, Upsert, Update and Delete Statements, with ORM RETURNING
-----------------------------------------------------------------------------

SQLAlchemy 1.4 ported the features of the legacy :class:`_orm.Query` object to
:term:`2.0 style` execution, which meant that the :class:`.Select` construct
could be passed to :meth:`_orm.Session.execute` to deliver ORM results. Support
was also added for :class:`.Update` and :class:`.Delete` to be passed to
:meth:`_orm.Session.execute`, to the degree that they could provide
implementations of :meth:`_orm.Query.update` and :meth:`_orm.Query.delete`.

The major missing element has been support for the :class:`_dml.Insert` construct.
The 1.4 documentation addressed this with some recipes for "inserts" and "upserts"
with use of :meth:`.Select.from_statement` to integrate RETURNING
into an ORM context.  2.0 now fully closes the gap by integrating direct support for
:class:`_dml.Insert` as an enhanced version of the :meth:`_orm.Session.bulk_insert_mappings`
method, along with full ORM RETURNING support for all DML structures.

Bulk Insert with RETURNING
~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`_dml.Insert` can be passed to :meth:`_orm.Session.execute`, with
or without :meth:`_dml.Insert.returning`, which when passed with a
separate parameter list will invoke the same process as was previously
implemented by
:meth:`_orm.Session.bulk_insert_mappings`, with additional enhancements.  This will optimize the
batching of rows making use of the new :ref:`fast insertmany <change_6047>`
feature, while also adding support for
heterogeneous parameter sets and multiple-table mappings like joined table
inheritance::

    >>> users = session.scalars(
    ...     insert(User).returning(User),
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ],
    ... )
    >>> print(users.all())
    [User(name='spongebob', fullname='Spongebob Squarepants'),
     User(name='sandy', fullname='Sandy Cheeks'),
     User(name='patrick', fullname='Patrick Star'),
     User(name='squidward', fullname='Squidward Tentacles'),
     User(name='ehkrabs', fullname='Eugene H. Krabs')]

RETURNING is supported for all of these use cases, where the ORM will construct
a full result set from multiple statement invocations.

.. seealso::

    :ref:`orm_queryguide_bulk_insert`

Bulk UPDATE
~~~~~~~~~~~

In a similar manner as that of :class:`_dml.Insert`, passing the
:class:`_dml.Update` construct along with a parameter list that includes
primary key values to :meth:`_orm.Session.execute` will invoke the same process
as previously supported by the :meth:`_orm.Session.bulk_update_mappings`
method.  This feature does not however support RETURNING, as it uses
a SQL UPDATE statement that is invoked using DBAPI :term:`executemany`::

    >>> from sqlalchemy import update
    >>> session.execute(
    ...     update(User),
    ...     [
    ...         {"id": 1, "fullname": "Spongebob Squarepants"},
    ...         {"id": 3, "fullname": "Patrick Star"},
    ...     ],
    ... )

.. seealso::

    :ref:`orm_queryguide_bulk_update`

INSERT / upsert ... VALUES ... RETURNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using :class:`_dml.Insert` with :meth:`_dml.Insert.values`, the set of
parameters may include SQL expressions. Additionally, upsert variants
such as those for SQLite, PostgreSQL and MariaDB are also supported.
These statements may now include :meth:`_dml.Insert.returning` clauses
with column expressions or full ORM entities::

    >>> from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
    >>> stmt = sqlite_upsert(User).values(
    ...     [
    ...         {"name": "spongebob", "fullname": "Spongebob Squarepants"},
    ...         {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...         {"name": "patrick", "fullname": "Patrick Star"},
    ...         {"name": "squidward", "fullname": "Squidward Tentacles"},
    ...         {"name": "ehkrabs", "fullname": "Eugene H. Krabs"},
    ...     ]
    ... )
    >>> stmt = stmt.on_conflict_do_update(
    ...     index_elements=[User.name], set_=dict(fullname=stmt.excluded.fullname)
    ... )
    >>> result = session.scalars(stmt.returning(User))
    >>> print(result.all())
    [User(name='spongebob', fullname='Spongebob Squarepants'),
    User(name='sandy', fullname='Sandy Cheeks'),
    User(name='patrick', fullname='Patrick Star'),
    User(name='squidward', fullname='Squidward Tentacles'),
    User(name='ehkrabs', fullname='Eugene H. Krabs')]

.. seealso::

    :ref:`orm_queryguide_insert_values`

    :ref:`orm_queryguide_upsert`

ORM UPDATE / DELETE with WHERE ... RETURNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy 1.4 also had some modest support for the RETURNING feature to be
used with the :func:`_dml.update` and :func:`_dml.delete` constructs, when
used with :meth:`_orm.Session.execute`.  This support has now been upgraded
to be fully native, including that the ``fetch`` synchronization strategy
may also proceed whether or not explicit use of RETURNING is present::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(User)
    ...     .where(User.name == "squidward")
    ...     .values(name="spongebob")
    ...     .returning(User)
    ... )
    >>> result = session.scalars(stmt, execution_options={"synchronize_session": "fetch"})
    >>> print(result.all())


.. seealso::

    :ref:`orm_queryguide_update_delete_where`

    :ref:`orm_queryguide_update_delete_where_returning`

Improved ``synchronize_session`` behavior for ORM UPDATE / DELETE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default strategy for :ref:`synchronize_session <orm_queryguide_update_delete_sync>`
is now a new value ``"auto"``.  This strategy will attempt to use the
``"evaluate"`` strategy and then automatically fall back to the ``"fetch"``
strategy.   For all backends other than MySQL / MariaDB, ``"fetch"`` uses
RETURNING to fetch UPDATE/DELETEd primary key identifiers within the
same statement, so is generally more efficient than previous versions
(in 1.4, RETURNING was only available for PostgreSQL, SQL Server).

.. seealso::

    :ref:`orm_queryguide_update_delete_sync`

Summary of Changes
~~~~~~~~~~~~~~~~~~

Listed tickets for new ORM DML with RETURNING features:

* convert ``insert()`` at ORM level to interpret ``values()`` in an ORM
  context - :ticket:`7864`
* evaluate feasibility of dml.returning(Entity) to deliver ORM expressions,
  automatically apply select().from_statement equiv - :ticket:`7865`
* given ORM insert, try to carry the bulk methods along, re: inheritance -
  :ticket:`8360`

.. _change_7123:

New "Write Only" relationship strategy supersedes "dynamic"
-----------------------------------------------------------

The ``lazy="dynamic"`` loader strategy becomes legacy, in that it is hardcoded
to make use of legacy :class:`_orm.Query`. This loader strategy is both not
compatible with asyncio, and additionally has many behaviors that implicitly
iterate its contents, which defeat the original purpose of the "dynamic"
relationship as being for very large collections that should not be implicitly
fully loaded into memory at any time.

The "dynamic" strategy is now superseded by a new strategy
``lazy="write_only"``.  Configuration of "write only" may be achieved using
the :paramref:`_orm.relationship.lazy` parameter of :func:`_orm.relationship`,
or when using :ref:`type annotated mappings <whatsnew_20_orm_declarative_typing>`,
indicating the :class:`.WriteOnlyMapped` annotation as the mapping style::

    from sqlalchemy.orm import WriteOnlyMapped


    class Base(DeclarativeBase):
        pass


    class Account(Base):
        __tablename__ = "account"
        id: Mapped[int] = mapped_column(primary_key=True)
        identifier: Mapped[str]
        account_transactions: WriteOnlyMapped["AccountTransaction"] = relationship(
            cascade="all, delete-orphan",
            passive_deletes=True,
            order_by="AccountTransaction.timestamp",
        )


    class AccountTransaction(Base):
        __tablename__ = "account_transaction"
        id: Mapped[int] = mapped_column(primary_key=True)
        account_id: Mapped[int] = mapped_column(
            ForeignKey("account.id", ondelete="cascade")
        )
        description: Mapped[str]
        amount: Mapped[Decimal]
        timestamp: Mapped[datetime] = mapped_column(default=func.now())

The write-only-mapped collection resembles ``lazy="dynamic"`` in that
the collection may be assigned up front, and also has methods such as
:meth:`_orm.WriteOnlyCollection.add` and :meth:`_orm.WriteOnlyCollection.remove`
to modify the collection on an individual item basis::

    new_account = Account(
        identifier="account_01",
        account_transactions=[
            AccountTransaction(description="initial deposit", amount=Decimal("500.00")),
            AccountTransaction(description="transfer", amount=Decimal("1000.00")),
            AccountTransaction(description="withdrawal", amount=Decimal("-29.50")),
        ],
    )

    new_account.account_transactions.add(
        AccountTransaction(description="transfer", amount=Decimal("2000.00"))
    )

The bigger difference is on the database loading side, where the collection
has no ability to load objects from the database directly; instead,
SQL construction methods such as :meth:`_orm.WriteOnlyCollection.select` are used to
produce SQL constructs such as :class:`_sql.Select` which are then executed
using :term:`2.0 style` to load the desired objects in an explicit way::

    account_transactions = session.scalars(
        existing_account.account_transactions.select()
        .where(AccountTransaction.amount < 0)
        .limit(10)
    ).all()

The :class:`_orm.WriteOnlyCollection` also integrates with the new
:ref:`ORM bulk dml <change_8360>` features, including support for bulk INSERT
and UPDATE/DELETE with WHERE criteria, all including RETURNING support as
well.   See the complete documentation at :ref:`write_only_relationship`.

.. seealso::

    :ref:`write_only_relationship`

New pep-484 / type annotated mapping support for Dynamic Relationships
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even though "dynamic" relationships are legacy in 2.0, as these patterns
are expected to have a long lifespan,
:ref:`type annotated mapping <whatsnew_20_orm_declarative_typing>` support
is now added for "dynamic" relationships in the same way that its available
for the new ``lazy="write_only"`` approach, using the :class:`_orm.DynamicMapped`
annotation::

    from sqlalchemy.orm import DynamicMapped


    class Base(DeclarativeBase):
        pass


    class Account(Base):
        __tablename__ = "account"
        id: Mapped[int] = mapped_column(primary_key=True)
        identifier: Mapped[str]
        account_transactions: DynamicMapped["AccountTransaction"] = relationship(
            cascade="all, delete-orphan",
            passive_deletes=True,
            order_by="AccountTransaction.timestamp",
        )


    class AccountTransaction(Base):
        __tablename__ = "account_transaction"
        id: Mapped[int] = mapped_column(primary_key=True)
        account_id: Mapped[int] = mapped_column(
            ForeignKey("account.id", ondelete="cascade")
        )
        description: Mapped[str]
        amount: Mapped[Decimal]
        timestamp: Mapped[datetime] = mapped_column(default=func.now())

The above mapping will provide an ``Account.account_transactions`` collection
that is typed as returning the :class:`_orm.AppenderQuery` collection type,
including its element type, e.g. ``AppenderQuery[AccountTransaction]``.  This
then allows iteration and queries to yield objects which are typed
as ``AccountTransaction``.

.. seealso::

    :ref:`dynamic_relationship`


:ticket:`7123`


.. _change_7311:

Installation is now fully pep-517 enabled
------------------------------------------

The source distribution now includes a ``pyproject.toml`` file to allow for
complete :pep:`517` support. In particular this allows a local source build
using ``pip`` to automatically install the Cython_ optional dependency.

:ticket:`7311`

.. _change_7256:

C Extensions now ported to Cython
----------------------------------

The SQLAlchemy C extensions have been replaced with all new extensions written
in Cython_. While Cython was evaluated back in 2010 when the C extensions were
first created, the nature and focus of the C extensions in use today has
changed quite a bit from that time. At the same time, Cython has apparently
evolved significantly, as has the Python build / distribution toolchain which
made it feasible for us to revisit it.

The move to Cython provides dramatic new advantages with
no apparent downsides:

* The Cython extensions that replace specific C extensions have all benchmarked
  as **faster**, often slightly, but sometimes significantly, than
  virtually all the C code that SQLAlchemy previously
  included. While this seems amazing, it appears to be a product of
  non-obvious optimizations within Cython's implementation that would not be
  present in a direct Python to C port of a function, as was particularly the
  case for many of the custom collection types added to the C extensions.

* Cython extensions are much easier to write, maintain and debug compared to
  raw C code, and in most cases are line-per-line equivalent to the Python
  code.   It is expected that many more elements of SQLAlchemy will be
  ported to Cython in the coming releases which should open many new doors
  to performance improvements that were previously out of reach.

* Cython is very mature and widely used, including being the basis of some
  of the prominent database drivers supported by SQLAlchemy including
  ``asyncpg``, ``psycopg3`` and ``asyncmy``.

Like the previous C extensions, the Cython extensions are pre-built within
SQLAlchemy's wheel distributions which are automatically available to ``pip``
from PyPi.  Manual build instructions are also unchanged with the exception
of the Cython requirement.

.. seealso::

    :ref:`c_extensions`


:ticket:`7256`


.. _change_4379:

Major Architectural, Performance and API Enhancements for Database Reflection
-----------------------------------------------------------------------------

The internal system by which :class:`.Table` objects and their components are
:ref:`reflected <metadata_reflection>` has been completely rearchitected to
allow high performance bulk reflection of thousands of tables at once for
participating dialects. Currently, the **PostgreSQL** and **Oracle** dialects
participate in the new architecture, where the PostgreSQL dialect can now
reflect a large series of :class:`.Table` objects nearly three times faster,
and the Oracle dialect can now reflect a large series of :class:`.Table`
objects ten times faster.

The rearchitecture applies most directly to dialects that make use of SELECT
queries against system catalog tables to reflect tables, and the remaining
included dialect that can benefit from this approach will be the SQL Server
dialect. The MySQL/MariaDB and SQLite dialects by contrast make use of
non-relational systems to reflect database tables, and were not subject to a
pre-existing performance issue.

The new API is backwards compatible with the previous system, and should
require no changes to third party dialects to retain compatibility; third party
dialects can also opt into the new system by implementing batched queries for
schema reflection.

Along with this change, the API and behavior of the :class:`.Inspector`
object has been improved and enhanced with more consistent cross-dialect
behaviors as well as new methods and new performance features.

Performance Overview
~~~~~~~~~~~~~~~~~~~~

The source distribution includes a script
``test/perf/many_table_reflection.py`` which benches both existing reflection
features as well as new ones. A limited set of its tests may be run on older
versions of SQLAlchemy, where here we use it to illustrate differences in
performance to invoke ``metadata.reflect()`` to reflect 250 :class:`.Table`
objects at once over a local network connection:

===========================  ==================================  ====================    ====================
Dialect                      Operation                           SQLA 1.4 Time (secs)    SQLA 2.0 Time (secs)
---------------------------  ----------------------------------  --------------------    --------------------
postgresql+psycopg2          ``metadata.reflect()``, 250 tables  8.2                     3.3
oracle+cx_oracle             ``metadata.reflect()``, 250 tables  60.4                    6.8
===========================  ==================================  ====================    ====================



Behavioral Changes for ``Inspector()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For SQLAlchemy-included dialects for SQLite, PostgreSQL, MySQL/MariaDB,
Oracle, and SQL Server, the :meth:`.Inspector.has_table`,
:meth:`.Inspector.has_sequence`, :meth:`.Inspector.has_index`,
:meth:`.Inspector.get_table_names` and
:meth:`.Inspector.get_sequence_names` now all behave consistently in terms
of caching: they all fully cache their result after being called the first
time for a particular :class:`.Inspector` object. Programs that create or
drop tables/sequences while calling upon the same :class:`.Inspector`
object will not receive updated status after the state of the database has
changed. A call to :meth:`.Inspector.clear_cache` or a new
:class:`.Inspector` should be used when DDL changes are to be executed.
Previously, the :meth:`.Inspector.has_table`,
:meth:`.Inspector.has_sequence` methods did not implement caching nor did
the :class:`.Inspector` support caching for these methods, while the
:meth:`.Inspector.get_table_names` and
:meth:`.Inspector.get_sequence_names` methods were, leading to inconsistent
results between the two types of method.

Behavior for third party dialects is dependent on whether or not they
implement the "reflection cache" decorator for the dialect-level
implementation of these methods.

New Methods and Improvements for ``Inspector()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* added a method
  :meth:`.Inspector.has_schema` that returns if a schema
  is present in the target database
* added a method :meth:`.Inspector.has_index` that returns if a table has
  a particular index.
* Inspection methods such as :meth:`.Inspector.get_columns` that work
  on a single table at a time should now all consistently
  raise :class:`_exc.NoSuchTableError` if a
  table or view is not found; this change is specific to individual
  dialects, so may not be the case for existing third-party dialects.
* Separated the handling of "views" and "materialized views", as in
  real world use cases, these two constructs make use of different DDL
  for CREATE and DROP; this includes that there are now separate
  :meth:`.Inspector.get_view_names` and
  :meth:`.Inspector.get_materialized_view_names` methods.


:ticket:`4379`


.. _ticket_6842:

Dialect support for psycopg 3 (a.k.a. "psycopg")
-------------------------------------------------

Added dialect support for the `psycopg 3 <https://pypi.org/project/psycopg/>`_
DBAPI, which despite the number "3" now goes by the package name ``psycopg``,
superseding the previous ``psycopg2`` package that for the time being remains
SQLAlchemy's "default" driver for the ``postgresql`` dialects. ``psycopg`` is a
completely reworked and modernized database adapter for PostgreSQL which
supports concepts such as prepared statements as well as Python asyncio.

``psycopg`` is the first DBAPI supported by SQLAlchemy which provides
both a pep-249 synchronous API as well as an asyncio driver.  The same
``psycopg`` database URL may be used with the :func:`_sa.create_engine`
and :func:`_asyncio.create_async_engine` engine-creation functions, and the
corresponding sync or asyncio version of the dialect will be selected
automatically.

.. seealso::

    :ref:`postgresql_psycopg`


.. _ticket_8054:

Dialect support for oracledb
----------------------------

Added dialect support for the `oracledb <https://pypi.org/project/oracledb/>`_
DBAPI, which is the renamed, new major release of the popular cx_Oracle driver.

.. seealso::

    :ref:`oracledb`

.. _ticket_7631:

New Conditional DDL for Constraints and Indexes
-----------------------------------------------

A new method :meth:`_schema.Constraint.ddl_if` and :meth:`_schema.Index.ddl_if`
allows constructs such as :class:`_schema.CheckConstraint`, :class:`_schema.UniqueConstraint`
and :class:`_schema.Index` to be rendered conditionally for a given
:class:`_schema.Table`, based on the same kinds of criteria that are accepted
by the :meth:`_schema.DDLElement.execute_if` method.  In the example below,
the CHECK constraint and index will only be produced against a PostgreSQL
backend::

    meta = MetaData()


    my_table = Table(
        "my_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("num", Integer),
        Column("data", String),
        Index("my_pg_index", "data").ddl_if(dialect="postgresql"),
        CheckConstraint("num > 5").ddl_if(dialect="postgresql"),
    )

    e1 = create_engine("sqlite://", echo=True)
    meta.create_all(e1)  # will not generate CHECK and INDEX


    e2 = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    meta.create_all(e2)  # will generate CHECK and INDEX

.. seealso::

    :ref:`schema_ddl_ddl_if`

:ticket:`7631`

.. _change_5052:

DATE, TIME, DATETIME datatypes now support literal rendering on all backends
-----------------------------------------------------------------------------

Literal rendering is now implemented for date and time types for backend
specific compilation, including PostgreSQL and Oracle:

.. sourcecode:: pycon+sql

    >>> import datetime

    >>> from sqlalchemy import DATETIME
    >>> from sqlalchemy import literal
    >>> from sqlalchemy.dialects import oracle
    >>> from sqlalchemy.dialects import postgresql

    >>> date_literal = literal(datetime.datetime.now(), DATETIME)

    >>> print(
    ...     date_literal.compile(
    ...         dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    ...     )
    ... )
    {printsql}'2022-12-17 11:02:13.575789'{stop}

    >>> print(
    ...     date_literal.compile(
    ...         dialect=oracle.dialect(), compile_kwargs={"literal_binds": True}
    ...     )
    ... )
    {printsql}TO_TIMESTAMP('2022-12-17 11:02:13.575789', 'YYYY-MM-DD HH24:MI:SS.FF'){stop}

Previously, such literal rendering only worked when stringifying statements
without any dialect given; when attempting to render with a dialect-specific
type, a ``NotImplementedError`` would be raised, up until
SQLAlchemy 1.4.45 where this became a :class:`.CompileError` (part of
:ticket:`8800`).

The default rendering is modified ISO-8601 rendering (i.e. ISO-8601 with the T
converted to a space) when using ``literal_binds`` with the SQL compilers
provided by the PostgreSQL, MySQL, MariaDB, MSSQL, Oracle dialects. For Oracle,
the ISO format is wrapped inside of an appropriate TO_DATE() function call.
The rendering for SQLite is unchanged as this dialect always included string
rendering for date values.



:ticket:`5052`

.. _change_8710:

Context Manager Support for ``Result``, ``AsyncResult``
-------------------------------------------------------

The :class:`.Result` object now supports context manager use, which will
ensure the object and its underlying cursor is closed at the end of the block.
This is useful in particular with server side cursors, where it's important that
the open cursor object is closed at the end of an operation, even if user-defined
exceptions have occurred::

    with engine.connect() as conn:
        with conn.execution_options(yield_per=100).execute(
            text("select * from table")
        ) as result:
            for row in result:
                print(f"{row}")

With asyncio use, the :class:`.AsyncResult` and :class:`.AsyncConnection` have
been altered to provide for optional async context manager use, as in::

    async with async_engine.connect() as conn:
        async with conn.execution_options(yield_per=100).execute(
            text("select * from table")
        ) as result:
            for row in result:
                print(f"{row}")

:ticket:`8710`

Behavioral Changes
------------------

This section covers behavioral changes made in SQLAlchemy 2.0 which are
not otherwise part of the major 1.4->2.0 migration path; changes here are
not expected to have significant effects on backwards compatibility.


.. _change_9015:

New transaction join modes for ``Session``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The behavior of "joining an external transaction into a Session" has been
revised and improved, allowing explicit control over how the
:class:`_orm.Session` will accommodate an incoming :class:`_engine.Connection`
that already has a transaction and possibly a savepoint already established.
The new parameter :paramref:`_orm.Session.join_transaction_mode` includes a
series of option values which can accommodate the existing transaction in
several ways, most importantly allowing a :class:`_orm.Session` to operate in a
fully transactional style using savepoints exclusively, while leaving the
externally initiated transaction non-committed and active under all
circumstances, allowing test suites to rollback all changes that take place
within tests.

The primary improvement this allows is that the recipe documented at
:ref:`session_external_transaction`, which also changed from SQLAlchemy 1.3
to 1.4, is now simplified to no longer require explicit use of an event
handler or any mention of an explicit savepoint; by using
``join_transaction_mode="create_savepoint"``, the :class:`_orm.Session` will
never affect the state of an incoming transaction, and will instead create a
savepoint (i.e. "nested transaction") as its root transaction.

The following illustrates part of the example given at
:ref:`session_external_transaction`; see that section for a full example::

    class SomeTest(TestCase):
        def setUp(self):
            # connect to the database
            self.connection = engine.connect()

            # begin a non-ORM transaction
            self.trans = self.connection.begin()

            # bind an individual Session to the connection, selecting
            # "create_savepoint" join_transaction_mode
            self.session = Session(
                bind=self.connection, join_transaction_mode="create_savepoint"
            )

        def tearDown(self):
            self.session.close()

            # rollback non-ORM transaction
            self.trans.rollback()

            # return connection to the Engine
            self.connection.close()

The default mode selected for :paramref:`_orm.Session.join_transaction_mode`
is ``"conditional_savepoint"``, which uses ``"create_savepoint"`` behavior
if the given :class:`_engine.Connection` is itself already on a savepoint.
If the given :class:`_engine.Connection` is in a transaction but not a
savepoint, the :class:`_orm.Session` will propagate "rollback" calls
but not "commit" calls, but will not begin a new savepoint on its own.  This
behavior is chosen by default for its maximum compatibility with
older SQLAlchemy versions as well as that it does not start a new SAVEPOINT
unless the given driver is already making use of SAVEPOINT, as support
for SAVEPOINT varies not only with specific backend and driver but also
configurationally.

The following illustrates a case that worked in SQLAlchemy 1.3, stopped working
in SQLAlchemy 1.4, and is now restored in SQLAlchemy 2.0::

    engine = create_engine("...")

    # setup outer connection with a transaction and a SAVEPOINT
    conn = engine.connect()
    trans = conn.begin()
    nested = conn.begin_nested()

    # bind a Session to that connection and operate upon it, including
    # a commit
    session = Session(conn)
    session.connection()
    session.commit()
    session.close()

    # assert both SAVEPOINT and transaction remain active
    assert nested.is_active
    nested.rollback()
    trans.rollback()

Where above, a :class:`_orm.Session` is joined to a :class:`_engine.Connection`
that has a savepoint started on it; the state of these two units remains
unchanged after the :class:`_orm.Session` has worked with the transaction. In
SQLAlchemy 1.3, the above case worked because the :class:`_orm.Session` would
begin a "subtransaction" upon the :class:`_engine.Connection`, which would
allow the outer savepoint / transaction to remain unaffected for simple cases
as above. Since subtransactions were deprecated in 1.4 and are now removed in
2.0, this behavior was no longer available. The new default behavior improves
upon the behavior of "subtransactions" by using a real, second SAVEPOINT
instead, so that even calls to :meth:`_orm.Session.rollback` prevent the
:class:`_orm.Session` from "breaking out" into the externally initiated
SAVEPOINT or transaction.

New code that is joining a transaction-started :class:`_engine.Connection` into
a :class:`_orm.Session` should however select a
:paramref:`_orm.Session.join_transaction_mode` explicitly, so that the desired
behavior is explicitly defined.

:ticket:`9015`


.. _Cython: https://cython.org/

.. _change_8567:

``str(engine.url)`` will obfuscate the password by default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To avoid leakage of database passwords, calling ``str()`` on a
:class:`.URL` will now enable the password obfuscation feature by default.
Previously, this obfuscation would be in place for ``__repr__()`` calls
but not ``__str__()``.   This change will impact applications and test suites
that attempt to invoke :func:`_sa.create_engine` given the stringified URL
from another engine, such as::

    >>> e1 = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")
    >>> e2 = create_engine(str(e1.url))

The above engine ``e2`` will not have the correct password; it will have the
obfuscated string ``"***"``.

The preferred approach for the above pattern is to pass the
:class:`.URL` object directly, there's no need to stringify::

    >>> e1 = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")
    >>> e2 = create_engine(e1.url)

Otherwise, for a stringified URL with cleartext password, use the
:meth:`_url.URL.render_as_string` method, passing the
:paramref:`_url.URL.render_as_string.hide_password` parameter
as ``False``::

    >>> e1 = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")
    >>> url_string = e1.url.render_as_string(hide_password=False)
    >>> e2 = create_engine(url_string)


:ticket:`8567`

.. _change_8925:

Stricter rules for replacement of Columns in Table objects with same-names, keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stricter rules are in place for appending of :class:`.Column` objects to
:class:`.Table` objects, both moving some previous deprecation warnings to
exceptions, and preventing some previous scenarios that would cause
duplicate columns to appear in tables, when
:paramref:`.Table.extend_existing` were set to ``True``, for both
programmatic :class:`.Table` construction as well as during reflection
operations.

* Under no circumstances should a :class:`.Table` object ever have two or more
  :class:`.Column` objects with the same name, regardless of what .key they
  have.  An edge case where this was still possible was identified and fixed.

* Adding a :class:`.Column` to a :class:`.Table` that has the same name or
  key as an existing :class:`.Column` will always raise
  :class:`.DuplicateColumnError` (a new subclass of :class:`.ArgumentError` in
  2.0.0b4) unless additional parameters are present;
  :paramref:`.Table.append_column.replace_existing` for
  :meth:`.Table.append_column`, and :paramref:`.Table.extend_existing` for
  construction of a same-named :class:`.Table` as an existing one, with or
  without reflection being used. Previously, there was a deprecation warning in
  place for this scenario.

* A warning is now emitted if a :class:`.Table` is created, that does
  include :paramref:`.Table.extend_existing`, where an incoming
  :class:`.Column` that has no separate :attr:`.Column.key` would fully
  replace an existing :class:`.Column` that does have a key, which suggests
  the operation is not what the user intended.  This can happen particularly
  during a secondary reflection step, such as ``metadata.reflect(extend_existing=True)``.
  The warning suggests that the :paramref:`.Table.autoload_replace` parameter
  be set to ``False`` to prevent this. Previously, in 1.4 and earlier, the
  incoming column would be added **in addition** to the existing column.
  This was a bug and is a behavioral change in 2.0 (as of 2.0.0b4), as the
  previous key will **no longer be present** in the column collection
  when this occurs.


:ticket:`8925`

.. _change_9297:

ORM Declarative Applies Column Orders Differently; Control behavior using ``sort_order``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declarative has changed the system by which mapped columns that originate from
mixin or abstract base classes are sorted along with the columns that are on the
declared class itself to place columns from the declared class first, followed
by mixin columns.  The following mapping::

    class Foo:
        col1 = mapped_column(Integer)
        col3 = mapped_column(Integer)


    class Bar:
        col2 = mapped_column(Integer)
        col4 = mapped_column(Integer)


    class Model(Base, Foo, Bar):
        id = mapped_column(Integer, primary_key=True)
        __tablename__ = "model"

Produces a CREATE TABLE as follows on 1.4:

.. sourcecode:: sql

    CREATE TABLE model (
      col1 INTEGER,
      col3 INTEGER,
      col2 INTEGER,
      col4 INTEGER,
      id INTEGER NOT NULL,
      PRIMARY KEY (id)
    )

Whereas on 2.0 it produces:

.. sourcecode:: sql

    CREATE TABLE model (
      id INTEGER NOT NULL,
      col1 INTEGER,
      col3 INTEGER,
      col2 INTEGER,
      col4 INTEGER,
      PRIMARY KEY (id)
    )

For the specific case above, this can be seen as an improvement, as the primary
key columns on the ``Model`` are now where one would typically prefer.  However,
this is no comfort for the application that defined models the other way
around, as::

    class Foo:
        id = mapped_column(Integer, primary_key=True)
        col1 = mapped_column(Integer)
        col3 = mapped_column(Integer)


    class Model(Foo, Base):
        col2 = mapped_column(Integer)
        col4 = mapped_column(Integer)
        __tablename__ = "model"

This now produces CREATE TABLE output as:

.. sourcecode:: sql

    CREATE TABLE model (
      col2 INTEGER,
      col4 INTEGER,
      id INTEGER NOT NULL,
      col1 INTEGER,
      col3 INTEGER,
      PRIMARY KEY (id)
    )

To solve this issue, SQLAlchemy 2.0.4 introduces a new parameter on
:func:`_orm.mapped_column` called :paramref:`_orm.mapped_column.sort_order`,
which is an integer value, defaulting to ``0``,
that can be set to a positive or negative value so that columns are placed
before or after other columns, as in the example below::

    class Foo:
        id = mapped_column(Integer, primary_key=True, sort_order=-10)
        col1 = mapped_column(Integer, sort_order=-1)
        col3 = mapped_column(Integer)


    class Model(Foo, Base):
        col2 = mapped_column(Integer)
        col4 = mapped_column(Integer)
        __tablename__ = "model"

The above model places "id" before all others and "col1" after "id":

.. sourcecode:: sql

    CREATE TABLE model (
      id INTEGER NOT NULL,
      col1 INTEGER,
      col2 INTEGER,
      col4 INTEGER,
      col3 INTEGER,
      PRIMARY KEY (id)
    )

Future SQLAlchemy releases may opt to provide an explicit ordering hint for the
:class:`_orm.mapped_column` construct, as this ordering is ORM specific.

.. _change_7211:

The ``Sequence`` construct reverts to not having any explicit default "start" value; impacts MS SQL Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prior to SQLAlchemy 1.4, the :class:`.Sequence` construct would emit only
simple ``CREATE SEQUENCE`` DDL, if no additional arguments were specified:

.. sourcecode:: pycon+sql

    >>> # SQLAlchemy 1.3 (and 2.0)
    >>> from sqlalchemy import Sequence
    >>> from sqlalchemy.schema import CreateSequence
    >>> print(CreateSequence(Sequence("my_seq")))
    {printsql}CREATE SEQUENCE my_seq

However, as :class:`.Sequence` support was added for MS SQL Server, where the
default start value is inconveniently set to ``-2**63``,
version 1.4 decided to default the DDL to emit a start value of 1, if
:paramref:`.Sequence.start` were not otherwise provided:

.. sourcecode:: pycon+sql

    >>> # SQLAlchemy 1.4 (only)
    >>> from sqlalchemy import Sequence
    >>> from sqlalchemy.schema import CreateSequence
    >>> print(CreateSequence(Sequence("my_seq")))
    {printsql}CREATE SEQUENCE my_seq START WITH 1

This change has introduced other complexities, including that when
the :paramref:`.Sequence.min_value` parameter is included, this default of
``1`` should in fact default to what :paramref:`.Sequence.min_value`
states, else a min_value that's below the start_value may be seen as
contradictory.     As looking at this issue started to become a bit of a
rabbit hole of other various edge cases, we decided to instead revert this
change and restore the original behavior of :class:`.Sequence` which is
to have no opinion, and just emit CREATE SEQUENCE, allowing the database
itself to make its decisions on how the various parameters of ``SEQUENCE``
should interact with each other.

Therefore, to ensure that the start value is 1 on all backends,
**the start value of 1 may be indicated explicitly**, as below:

.. sourcecode:: pycon+sql

    >>> # All SQLAlchemy versions
    >>> from sqlalchemy import Sequence
    >>> from sqlalchemy.schema import CreateSequence
    >>> print(CreateSequence(Sequence("my_seq", start=1)))
    {printsql}CREATE SEQUENCE my_seq START WITH 1

Beyond all of that, for autogeneration of integer primary keys on modern
backends including PostgreSQL, Oracle, SQL Server, the :class:`.Identity`
construct should be preferred, which also works the same way in 1.4 and 2.0
with no changes in behavior.


:ticket:`7211`


.. _change_6980:

"with_variant()" clones the original TypeEngine rather than changing the type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :meth:`_sqltypes.TypeEngine.with_variant` method, which is used to apply
alternate per-database behaviors to a particular type, now returns a copy of
the original :class:`_sqltypes.TypeEngine` object with the variant information
stored internally, rather than wrapping it inside the ``Variant`` class.

While the previous ``Variant`` approach was able to maintain all the in-Python
behaviors of the original type using dynamic attribute getters, the improvement
here is that when calling upon a variant, the returned type remains an instance
of the original type, which works more smoothly with type checkers such as mypy
and pylance.  Given a program as below::

    import typing

    from sqlalchemy import String
    from sqlalchemy.dialects.mysql import VARCHAR

    type_ = String(255).with_variant(VARCHAR(255, charset="utf8mb4"), "mysql", "mariadb")

    if typing.TYPE_CHECKING:
        reveal_type(type_)

A type checker like pyright will now report the type as:

.. sourcecode:: text

    info: Type of "type_" is "String"

In addition, as illustrated above, multiple dialect names may be passed for
single type, in particular this is helpful for the pair of ``"mysql"`` and
``"mariadb"`` dialects which are considered separately as of SQLAlchemy 1.4.

:ticket:`6980`


.. _change_4926:

Python division operator performs true division for all backends; added floor division
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Core expression language now supports both "true division" (i.e. the ``/``
Python operator) and "floor division" (i.e. the ``//`` Python operator)
including backend-specific behaviors to normalize different databases in this
regard.

Given a "true division" operation against two integer values::

    expr = literal(5, Integer) / literal(10, Integer)

The SQL division operator on PostgreSQL for example normally acts as "floor division"
when used against integers, meaning the above result would return the integer
"0".  For this and similar backends, SQLAlchemy now renders the SQL using
a form which is equivalent towards:

.. sourcecode:: sql

    %(param_1)s / CAST(%(param_2)s AS NUMERIC)

With ``param_1=5``, ``param_2=10``, so that the return expression will be of type
NUMERIC, typically as the Python value ``decimal.Decimal("0.5")``.

Given a "floor division" operation against two integer values::

    expr = literal(5, Integer) // literal(10, Integer)

The SQL division operator on MySQL and Oracle for example normally acts
as "true division" when used against integers, meaning the above result
would return the floating point value "0.5".  For these and similar backends,
SQLAlchemy now renders the SQL using a form which is equivalent towards:

.. sourcecode:: sql

    FLOOR(%(param_1)s / %(param_2)s)

With param_1=5, param_2=10, so that the return expression will be of type
INTEGER, as the Python value ``0``.

The backwards-incompatible change here would be if an application using
PostgreSQL, SQL Server, or SQLite which relied on the Python "truediv" operator
to return an integer value in all cases.  Applications which rely upon this
behavior should instead use the Python "floor division" operator ``//``
for these operations, or for forwards compatibility when using a previous
SQLAlchemy version, the floor function::

    expr = func.floor(literal(5, Integer) / literal(10, Integer))

The above form would be needed on any SQLAlchemy version prior to 2.0
in order to provide backend-agnostic floor division.

:ticket:`4926`

.. _change_7433:

Session raises proactively when illegal concurrent or reentrant access is detected
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`_orm.Session` can now trap more errors related to illegal concurrent
state changes within multithreaded or other concurrent scenarios as well as for
event hooks which perform unexpected state changes.

One error that's been known to occur when a :class:`_orm.Session` is used in
multiple threads simultaneously is
``AttributeError: 'NoneType' object has no attribute 'twophase'``, which is
completely cryptic. This error occurs when a thread calls
:meth:`_orm.Session.commit` which internally invokes the
:meth:`_orm.SessionTransaction.close` method to end the transactional context,
at the same time that another thread is in progress running a query
as from :meth:`_orm.Session.execute`.  Within :meth:`_orm.Session.execute`,
the internal method that acquires a database connection for the current
transaction first begins by asserting that the session is "active", but
after this assertion passes, the concurrent call to :meth:`_orm.Session.close`
interferes with this state which leads to the undefined condition above.

The change applies guards to all state-changing methods surrounding the
:class:`_orm.SessionTransaction` object so that in the above case, the
:meth:`_orm.Session.commit` method will instead fail as it will seek to change
the state to one that is disallowed for the duration of the already-in-progress
method that wants to get the current connection to run a database query.

Using the test script illustrated at :ticket:`7433`, the previous
error case looks like:

.. sourcecode:: text

    Traceback (most recent call last):
    File "/home/classic/dev/sqlalchemy/test3.py", line 30, in worker
        sess.execute(select(A)).all()
    File "/home/classic/tmp/sqlalchemy/lib/sqlalchemy/orm/session.py", line 1691, in execute
        conn = self._connection_for_bind(bind)
    File "/home/classic/tmp/sqlalchemy/lib/sqlalchemy/orm/session.py", line 1532, in _connection_for_bind
        return self._transaction._connection_for_bind(
    File "/home/classic/tmp/sqlalchemy/lib/sqlalchemy/orm/session.py", line 754, in _connection_for_bind
        if self.session.twophase and self._parent is None:
    AttributeError: 'NoneType' object has no attribute 'twophase'

Where the ``_connection_for_bind()`` method isn't able to continue since
concurrent access placed it into an invalid state.  Using the new approach, the
originator of the state change throws the error instead:

.. sourcecode:: text

    File "/home/classic/dev/sqlalchemy/lib/sqlalchemy/orm/session.py", line 1785, in close
       self._close_impl(invalidate=False)
    File "/home/classic/dev/sqlalchemy/lib/sqlalchemy/orm/session.py", line 1827, in _close_impl
       transaction.close(invalidate)
    File "<string>", line 2, in close
    File "/home/classic/dev/sqlalchemy/lib/sqlalchemy/orm/session.py", line 506, in _go
       raise sa_exc.InvalidRequestError(
    sqlalchemy.exc.InvalidRequestError: Method 'close()' can't be called here;
    method '_connection_for_bind()' is already in progress and this would cause
    an unexpected state change to symbol('CLOSED')

The state transition checks intentionally don't use explicit locks to detect
concurrent thread activity, instead relying upon simple attribute set / value
test operations that inherently fail when unexpected concurrent changes occur.
The rationale is that the approach can detect illegal state changes that occur
entirely within a single thread, such as an event handler that runs on session
transaction events calls a state-changing method that's not expected, or under
asyncio if a particular :class:`_orm.Session` were shared among multiple
asyncio tasks, as well as when using patching-style concurrency approaches
such as gevent.

:ticket:`7433`


.. _change_7490:

The SQLite dialect uses QueuePool for file-based databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The SQLite dialect now defaults to :class:`_pool.QueuePool` when a file
based database is used. This is set along with setting the
``check_same_thread`` parameter to ``False``. It has been observed that the
previous approach of defaulting to :class:`_pool.NullPool`, which does not
hold onto database connections after they are released, did in fact have a
measurable negative performance impact. As always, the pool class is
customizable via the :paramref:`_sa.create_engine.poolclass` parameter.

.. versionchanged:: 2.0.38 - an equivalent change is also made for the
   ``aiosqlite`` dialect, using :class:`._pool.AsyncAdaptedQueuePool` instead
   of :class:`._pool.NullPool`.  The ``aiosqlite`` dialect was not included
   in the initial change in error.

.. seealso::

    :ref:`pysqlite_threading_pooling`


:ticket:`7490`

.. _change_5465_oracle:

New Oracle FLOAT type with binary precision; decimal precision not accepted directly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A new datatype :class:`_oracle.FLOAT` has been added to the Oracle dialect, to
accompany the addition of :class:`_sqltypes.Double` and database-specific
:class:`_sqltypes.DOUBLE`, :class:`_sqltypes.DOUBLE_PRECISION` and
:class:`_sqltypes.REAL` datatypes. Oracle's ``FLOAT`` accepts a so-called
"binary precision" parameter that per Oracle documentation is roughly a
standard "precision" value divided by 0.3103::

    from sqlalchemy.dialects import oracle

    Table("some_table", metadata, Column("value", oracle.FLOAT(126)))

A binary precision value of 126 is synonymous with using the
:class:`_sqltypes.DOUBLE_PRECISION` datatype, and a value of 63 is equivalent
to using the :class:`_sqltypes.REAL` datatype.  Other precision values are
specific to the :class:`_oracle.FLOAT` type itself.

The SQLAlchemy :class:`_sqltypes.Float` datatype also accepts a "precision"
parameter, but this is decimal precision which is not accepted by
Oracle.  Rather than attempting to guess the conversion, the Oracle dialect
will now raise an informative error if :class:`_sqltypes.Float` is used with
a precision value against the Oracle backend.  To specify a
:class:`_sqltypes.Float` datatype with an explicit precision value for
supporting backends, while also supporting other backends, use
the :meth:`_types.TypeEngine.with_variant` method as follows::

    from sqlalchemy.types import Float
    from sqlalchemy.dialects import oracle

    Table(
        "some_table",
        metadata,
        Column("value", Float(5).with_variant(oracle.FLOAT(16), "oracle")),
    )

.. _change_7156:

New RANGE / MULTIRANGE support and changes for PostgreSQL backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RANGE / MULTIRANGE support has been fully implemented for psycopg2, psycopg3,
and asyncpg dialects.  The new support uses a new SQLAlchemy-specific
:class:`_postgresql.Range` object that is agnostic of the different backends
and does not require the use of backend-specific imports or extension
steps.  For multirange support, lists of :class:`_postgresql.Range`
objects are used.

Code that used the previous psycopg2-specific types should be modified
to use :class:`_postgresql.Range`, which presents a compatible interface.

The :class:`_postgresql.Range` object also features comparison support which
mirrors that of PostgreSQL.  Implemented so far are :meth:`_postgresql.Range.contains`
and :meth:`_postgresql.Range.contained_by` methods which work in the same way as
the PostgreSQL ``@>`` and ``<@``.  Additional operator support may be added
in future releases.

See the documentation at :ref:`postgresql_ranges` for background on
using the new feature.


.. seealso::

    :ref:`postgresql_ranges`

:ticket:`7156`
:ticket:`8706`

.. _change_7086:

``match()`` operator on PostgreSQL uses ``plainto_tsquery()`` rather than ``to_tsquery()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :meth:`.Operators.match` function now renders
``col @@ plainto_tsquery(expr)`` on the PostgreSQL backend, rather than
``col @@ to_tsquery()``.  ``plainto_tsquery()`` accepts plain text whereas
``to_tsquery()`` accepts specialized query symbols, and is therefore less
cross-compatible with other backends.

All PostgreSQL search functions and operators are available through use of
:data:`.func` to generate PostgreSQL-specific functions and
:meth:`.Operators.bool_op` (a boolean-typed version of :meth:`.Operators.op`)
to generate arbitrary operators, in the same manner as they are available
in previous versions.  See the examples at :ref:`postgresql_match`.

Existing SQLAlchemy projects that make use of PG-specific directives within
:meth:`.Operators.match` should make use of ``func.to_tsquery()`` directly.
To render SQL in exactly the same form as would be present
in 1.4, see the version note at :ref:`postgresql_simple_match`.



:ticket:`7086`

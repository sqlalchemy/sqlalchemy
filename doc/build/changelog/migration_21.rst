.. _whatsnew_21_toplevel:

=============================
What's New in SQLAlchemy 2.1?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 2.0 and
    version 2.1.


.. _change_10635:

``Row`` now represents individual column types directly without ``Tuple``
--------------------------------------------------------------------------

SQLAlchemy 2.0 implemented a broad array of :pep:`484` typing throughout
all components, including a new ability for row-returning statements such
as :func:`_sql.select` to maintain track of individual column types, which
were then passed through the execution phase onto the :class:`_engine.Result`
object and then to the individual :class:`_engine.Row` objects.   Described
at :ref:`change_result_typing_20`, this approach solved several issues
with statement / row typing, but some remained unsolvable.  In 2.1, one
of those issues, that the individual column types needed to be packaged
into a ``typing.Tuple``, is now resolved using new :pep:`646` integration,
which allows for tuple-like types that are not actually typed as ``Tuple``.

In SQLAlchemy 2.0, a statement such as::

    stmt = select(column("x", Integer), column("y", String))

Would be typed as::

    Select[Tuple[int, str]]

In 2.1, it's now typed as::

    Select[int, str]

When executing ``stmt``, the :class:`_engine.Result` and :class:`_engine.Row`
objects will be typed as ``Result[int, str]`` and ``Row[int, str]``, respectively.
The prior workaround using :attr:`_engine.Row._t` to type as a real ``Tuple``
is no longer needed and projects can migrate off this pattern.

Mypy users will need to make use of **Mypy 1.7 or greater** for pep-646
integration to be available.

Limitations
^^^^^^^^^^^

Not yet solved by pep-646 or any other pep is the ability for an arbitrary
number of expressions within :class:`_sql.Select` and others to be mapped to
row objects, without stating each argument position explicitly within typing
annotations.   To work around this issue, SQLAlchemy makes use of automated
"stub generation" tools to generate hardcoded mappings of different numbers of
positional arguments to constructs like :func:`_sql.select` to resolve to
individual ``Unpack[]`` expressions (in SQLAlchemy 2.0, this generation
produced ``Tuple[]`` annotations instead).  This means that there are arbitrary
limits on how many specific column expressions will be typed within the
:class:`_engine.Row` object, without restoring to ``Any`` for remaining
expressions; for :func:`_sql.select`, it's currently ten expressions, and
for DML expressions like :func:`_dml.insert` that use :meth:`_dml.Insert.returning`,
it's eight.    If and when a new pep that provides a ``Map`` operator
to pep-646 is proposed, this limitation can be lifted. [1]_  Originally, it was
mistakenly assumed that this limitation prevented pep-646 from being usable at all,
however, the ``Unpack`` construct does in fact replace everything that
was done using ``Tuple`` in 2.0.

An additional limitation for which there is no proposed solution is that
there's no way for the name-based attributes on :class:`_engine.Row` to be
automatically typed, so these continue to be typed as ``Any`` (e.g. ``row.x``
and ``row.y`` for the above example).   With current language features,
this could only be fixed by having an explicit class-based construct that
allows one to compose an explicit :class:`_engine.Row` with explicit fields
up front, which would be verbose and not automatic.

.. [1] https://github.com/python/typing/discussions/1001#discussioncomment-1897813

:ticket:`10635`


.. _change_10197:

Asyncio "greenlet" dependency no longer installs by default
------------------------------------------------------------

SQLAlchemy 1.4 and 2.0 used a complex expression to determine if the
``greenlet`` dependency, needed by the :ref:`asyncio <asyncio_toplevel>`
extension, could be installed from pypi using a pre-built wheel instead
of having to build from source.   This because the source build of ``greenlet``
is not always trivial on some platforms.

Disadvantages to this approach included that SQLAlchemy needed to track
exactly which versions of ``greenlet`` were published as wheels on pypi;
the setup expression led to problems with some package management tools
such as ``poetry``; it was not possible to install SQLAlchemy **without**
``greenlet`` being installed, even though this is completely feasible
if the asyncio extension is not used.

These problems are all solved by keeping ``greenlet`` entirely within the
``[asyncio]`` target.  The only downside is that users of the asyncio extension
need to be aware of this extra installation dependency.

:ticket:`10197`


.. _change_10050:

ORM Relationship allows callable for back_populates
---------------------------------------------------

To help produce code that is more amenable to IDE-level linting and type
checking, the :paramref:`_orm.relationship.back_populates` parameter now
accepts both direct references to a class-bound attribute as well as
lambdas which do the same::

    class A(Base):
        __tablename__ = "a"

        id: Mapped[int] = mapped_column(primary_key=True)

        # use a lambda: to link to B.a directly when it exists
        bs: Mapped[list[B]] = relationship(back_populates=lambda: B.a)


    class B(Base):
        __tablename__ = "b"
        id: Mapped[int] = mapped_column(primary_key=True)
        a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

        # A.bs already exists, so can link directly
        a: Mapped[A] = relationship(back_populates=A.bs)

:ticket:`10050`

.. _change_12168:

ORM Mapped Dataclasses no longer populate implicit ``default`` in ``__dict__``
------------------------------------------------------------------------------

This behavioral change addresses a widely reported issue with SQLAlchemy's
:ref:`orm_declarative_native_dataclasses` feature that was introduced in 2.0.
SQLAlchemy ORM has always featured a behavior where a particular attribute on
an ORM mapped class will have different behaviors depending on if it has an
actively set value, including if that value is ``None``, versus if the
attribute is not set at all.  When Declarative Dataclass Mapping was introduced, the
:paramref:`_orm.mapped_column.default` parameter introduced a new capability
which is to set up a dataclass-level default to be present in the generated
``__init__`` method. This had the unfortunate side effect of breaking various
popular workflows, the most prominent of which is creating an ORM object with
the foreign key value in lieu of a many-to-one reference::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class Parent(Base):
        __tablename__ = "parent"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

        related_id: Mapped[int | None] = mapped_column(ForeignKey("child.id"), default=None)
        related: Mapped[Child | None] = relationship(default=None)


    class Child(Base):
        __tablename__ = "child"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

In the above mapping, the ``__init__`` method generated for ``Parent``
would in Python code look like this::


    def __init__(self, related_id=None, related=None): ...

This means that creating a new ``Parent`` with ``related_id`` only would populate
both ``related_id`` and ``related`` in ``__dict__``::

    # 2.0 behavior; will INSERT NULL for related_id due to the presence
    # of related=None
    >>> p1 = Parent(related_id=5)
    >>> p1.__dict__
    {'related_id': 5, 'related': None, '_sa_instance_state': ...}

The ``None`` value for ``'related'`` means that SQLAlchemy favors the non-present
related ``Child`` over the present value for ``'related_id'``, which would be
discarded, and ``NULL`` would be inserted for ``'related_id'`` instead.

In the new behavior, the ``__init__`` method instead looks like the example below,
using a special constant ``DONT_SET`` indicating a non-present value for ``'related'``
should be ignored.  This allows the class to behave more closely to how
SQLAlchemy ORM mapped classes traditionally operate::

    def __init__(self, related_id=DONT_SET, related=DONT_SET): ...

We then get a ``__dict__`` setup that will follow the expected behavior of
omitting ``related`` from ``__dict__`` and later running an INSERT with
``related_id=5``::

    # 2.1 behavior; will INSERT 5 for related_id
    >>> p1 = Parent(related_id=5)
    >>> p1.__dict__
    {'related_id': 5, '_sa_instance_state': ...}

Dataclass defaults are delivered via descriptor instead of __dict__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above behavior goes a step further, which is that in order to
honor default values that are something other than ``None``, the value of the
dataclass-level default (i.e. set using any of the
:paramref:`_orm.mapped_column.default`,
:paramref:`_orm.column_property.default`, or :paramref:`_orm.deferred.default`
parameters) is directed to be delivered at the
Python :term:`descriptor` level using mechanisms in SQLAlchemy's attribute
system that normally return ``None`` for un-popualted columns, so that even though the default is not
populated into ``__dict__``, it's still delivered when the attribute is
accessed.  This behavior is based on what Python dataclasses itself does
when a default is indicated for a field that also includes ``init=False``.

In the example below, an immutable default ``"default_status"``
is applied to a column called ``status``::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class SomeObject(Base):
        __tablename__ = "parent"

        id: Mapped[int] = mapped_column(primary_key=True, init=False)

        status: Mapped[str] = mapped_column(default="default_status")

In the above mapping, constructing ``SomeObject`` with no parameters will
deliver no values inside of ``__dict__``, but will deliver the default
value via descriptor::

    # object is constructed with no value for ``status``
    >>> s1 = SomeObject()

    # the default value is not placed in ``__dict__``
    >>> s1.__dict__
    {'_sa_instance_state': ...}

    # but the default value is delivered at the object level via descriptor
    >>> s1.status
    'default_status'

    # the value still remains unpopulated in ``__dict__``
    >>> s1.__dict__
    {'_sa_instance_state': ...}

The value passed
as :paramref:`_orm.mapped_column.default` is also assigned as was the
case before to the :paramref:`_schema.Column.default` parameter of the
underlying :class:`_schema.Column`, where it takes
place as a Python-level default for INSERT statements.  So while ``__dict__``
is never populated with the default value on the object, the INSERT
still includes the value in the parameter set.  This essentially modifies
the Declarative Dataclass Mapping system to work more like traditional
ORM mapped classes, where a "default" means just that, a column level
default.

Dataclass defaults are accessible on objects even without init
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As the new behavior makes use of descriptors in a similar way as Python
dataclasses do themselves when ``init=False``, the new feature implements
this behavior as well.   This is an all new behavior where an ORM mapped
class can deliver a default value for fields even if they are not part of
the ``__init__()`` method at all.  In the mapping below, the ``status``
field is configured with ``init=False``, meaning it's not part of the
constructor at all::

    class Base(MappedAsDataclass, DeclarativeBase):
        pass


    class SomeObject(Base):
        __tablename__ = "parent"
        id: Mapped[int] = mapped_column(primary_key=True, init=False)
        status: Mapped[str] = mapped_column(default="default_status", init=False)

When we construct ``SomeObject()`` with no arguments, the default is accessible
on the instance, delivered via descriptor::

    >>> so = SomeObject()
    >>> so.status
    default_status

Related Changes
^^^^^^^^^^^^^^^

This change includes the following API changes:

* The :paramref:`_orm.relationship.default` parameter, when present, only
  accepts a value of ``None``, and is only accepted when the relationship is
  ultimately a many-to-one relationship or one that establishes
  :paramref:`_orm.relationship.uselist` as ``False``.
* The :paramref:`_orm.mapped_column.default` and :paramref:`_orm.mapped_column.insert_default`
  parameters are mutually exclusive, and only one may be passed at a time.
  The behavior of the two parameters is equivalent at the :class:`_schema.Column`
  level, however at the Declarative Dataclass Mapping level, only
  :paramref:`_orm.mapped_column.default` actually sets the dataclass-level
  default with descriptor access; using :paramref:`_orm.mapped_column.insert_default`
  will have the effect of the object attribute defaulting to ``None`` on the
  instance until the INSERT takes place, in the same way it works on traditional
  ORM mapped classes.

:ticket:`12168`


.. _change_11234:

URL stringify and parse now supports URL escaping for the "database" portion
----------------------------------------------------------------------------

A URL that includes URL-escaped characters in the database portion will
now parse with conversion of those escaped characters::

    >>> from sqlalchemy import make_url
    >>> u = make_url("driver://user:pass@host/database%3Fname")
    >>> u.database
    'database?name'

Previously, such characters would not be unescaped::

    >>> # pre-2.1 behavior
    >>> from sqlalchemy import make_url
    >>> u = make_url("driver://user:pass@host/database%3Fname")
    >>> u.database
    'database%3Fname'

This change also applies to the stringify side; most special characters in
the database name will be URL escaped, omitting a few such as plus signs and
slashes::

    >>> from sqlalchemy import URL
    >>> u = URL.create("driver", database="a?b=c")
    >>> str(u)
    'driver:///a%3Fb%3Dc'

Where the above URL correctly round-trips to itself::

    >>> make_url(str(u))
    driver:///a%3Fb%3Dc
    >>> make_url(str(u)).database == u.database
    True


Whereas previously, special characters applied programmatically would not
be escaped in the result, leading to a URL that does not represent the
original database portion.  Below, `b=c` is part of the query string and
not the database portion::

    >>> from sqlalchemy import URL
    >>> u = URL.create("driver", database="a?b=c")
    >>> str(u)
    'driver:///a?b=c'

:ticket:`11234`

.. _change_11250:

Potential breaking change to odbc_connect= handling for mssql+pyodbc
--------------------------------------------------------------------

Fixed a mssql+pyodbc issue where valid plus signs in an already-unquoted
``odbc_connect=`` (raw DBAPI) connection string were replaced with spaces.

Previously, the pyodbc connector would always pass the odbc_connect value
to unquote_plus(), even if it was not required. So, if the (unquoted)
odbc_connect value contained ``PWD=pass+word`` that would get changed to
``PWD=pass word``, and the login would fail. One workaround was to quote
just the plus sign — ``PWD=pass%2Bword`` — which would then get unquoted
to ``PWD=pass+word``.

Implementations using the above workaround with :meth:`_engine.URL.create`
to specify a plus sign in the ``PWD=`` argument of an odbc_connect string
will have to remove the workaround and just pass the ``PWD=`` value as it
would appear in a valid ODBC connection string (i.e., the same as would be
required if using the connection string directly with ``pyodbc.connect()``).

:ticket:`11250`

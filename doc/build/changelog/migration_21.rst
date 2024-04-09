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

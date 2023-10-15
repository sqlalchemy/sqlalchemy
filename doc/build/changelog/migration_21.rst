.. _whatsnew_21_toplevel:

=============================
What's New in SQLAlchemy 2.1?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 2.0 and
    version 2.1.


.. _change_10197:

Asyncio "greenlet" dependency no longer installs by default
------------------------------------------------------------

SQLAlchemy 1.4 and 2.0 used a complex expression to determine if the
``greenlet`` dependency, needed by the :ref:`asyncio <asyncio_toplevel>`
extension, could be installed from pypi using a pre-built wheel instead
of having to build from source.   This because the source build of ``greenlet``
is not always trivial on some platforms.

Disadantages to this approach included that SQLAlchemy needed to track
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


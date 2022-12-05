:orphan:

========================================
Setup for ORM Queryguide: Column Loading
========================================

This page illustrates the mappings and fixture data used by the
:doc:`columns` document of the :ref:`queryguide_toplevel`.

..  sourcecode:: python

    >>> from typing import List
    >>> from typing import Optional
    >>>
    >>> from sqlalchemy import Column
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import ForeignKey
    >>> from sqlalchemy import LargeBinary
    >>> from sqlalchemy import Table
    >>> from sqlalchemy import Text
    >>> from sqlalchemy.orm import DeclarativeBase
    >>> from sqlalchemy.orm import Mapped
    >>> from sqlalchemy.orm import mapped_column
    >>> from sqlalchemy.orm import relationship
    >>> from sqlalchemy.orm import Session
    >>>
    >>>
    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class User(Base):
    ...     __tablename__ = "user_account"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     fullname: Mapped[Optional[str]]
    ...     books: Mapped[List["Book"]] = relationship(back_populates="owner")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"
    >>> class Book(Base):
    ...     __tablename__ = "book"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     owner_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     title: Mapped[str]
    ...     summary: Mapped[str] = mapped_column(Text)
    ...     cover_photo: Mapped[bytes] = mapped_column(LargeBinary)
    ...     owner: Mapped["User"] = relationship(back_populates="books")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Book(id={self.id!r}, title={self.title!r})"
    >>> engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
    >>> Base.metadata.create_all(engine)
    BEGIN ...
    >>> conn = engine.connect()
    >>> session = Session(conn)
    >>> session.add_all(
    ...     [
    ...         User(
    ...             name="spongebob",
    ...             fullname="Spongebob Squarepants",
    ...             books=[
    ...                 Book(
    ...                     title="100 Years of Krabby Patties",
    ...                     summary="some long summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...                 Book(
    ...                     title="Sea Catch 22",
    ...                     summary="another long summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...                 Book(
    ...                     title="The Sea Grapes of Wrath",
    ...                     summary="yet another summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...             ],
    ...         ),
    ...         User(
    ...             name="sandy",
    ...             fullname="Sandy Cheeks",
    ...             books=[
    ...                 Book(
    ...                     title="A Nut Like No Other",
    ...                     summary="some long summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...                 Book(
    ...                     title="Geodesic Domes: A Retrospective",
    ...                     summary="another long summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...                 Book(
    ...                     title="Rocketry for Squirrels",
    ...                     summary="yet another summary",
    ...                     cover_photo=b"binary_image_data",
    ...                 ),
    ...             ],
    ...         ),
    ...     ]
    ... )
    >>> session.commit()
    BEGIN ... COMMIT
    >>> session.close()
    >>> conn.begin()
    BEGIN ...

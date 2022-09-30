:orphan:

======================================
Setup for ORM Queryguide: DML
======================================

This page illustrates the mappings and fixture data used by the
:doc:`dml` document of the :ref:`queryguide_toplevel`.

..  sourcecode:: python

    >>> from typing import List
    >>> from typing import Optional
    >>> import datetime
    >>>
    >>> from sqlalchemy import Column
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import ForeignKey
    >>> from sqlalchemy import Table
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
    ...     name: Mapped[str] = mapped_column(unique=True)
    ...     fullname: Mapped[Optional[str]]
    ...     species: Mapped[Optional[str]]
    ...     addresses: Mapped[List["Address"]] = relationship(back_populates="user")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"User(name={self.name!r}, fullname={self.fullname!r})"
    >>> class Address(Base):
    ...     __tablename__ = "address"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    ...     email_address: Mapped[str]
    ...     user: Mapped[User] = relationship(back_populates="addresses")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Address(email_address={self.email_address!r})"
    >>> class LogRecord(Base):
    ...     __tablename__ = "log_record"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     message: Mapped[str]
    ...     code: Mapped[str]
    ...     timestamp: Mapped[datetime.datetime]
    ...
    ...     def __repr__(self):
    ...         return f"LogRecord({self.message!r}, {self.code!r}, {self.timestamp!r})"

    >>> class Employee(Base):
    ...     __tablename__ = "employee"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     type: Mapped[str]
    ...
    ...     def __repr__(self):
    ...         return f"{self.__class__.__name__}({self.name!r})"
    ...
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "employee",
    ...         "polymorphic_on": "type",
    ...     }
    >>> class Manager(Employee):
    ...     __tablename__ = "manager"
    ...     id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
    ...     manager_name: Mapped[str]
    ...
    ...     def __repr__(self):
    ...         return f"{self.__class__.__name__}({self.name!r}, manager_name={self.manager_name!r})"
    ...
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "manager",
    ...     }
    >>> class Engineer(Employee):
    ...     __tablename__ = "engineer"
    ...     id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
    ...     engineer_info: Mapped[str]
    ...
    ...     def __repr__(self):
    ...         return f"{self.__class__.__name__}({self.name!r}, engineer_info={self.engineer_info!r})"
    ...
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "engineer",
    ...     }

    >>> engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
    >>> Base.metadata.create_all(engine)
    BEGIN ...
    >>> conn = engine.connect()
    >>> session = Session(conn)
    >>> conn.begin()
    BEGIN ...

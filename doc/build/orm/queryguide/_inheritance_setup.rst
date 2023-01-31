:orphan:

============================================
Setup for ORM Queryguide: Joined Inheritance
============================================

This page illustrates the mappings and fixture data used by the
:ref:`joined_inheritance` examples in the :doc:`inheritance` document of
the :ref:`queryguide_toplevel`.

..  sourcecode:: python


    >>> from typing import List
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import ForeignKey
    >>> from sqlalchemy.orm import DeclarativeBase
    >>> from sqlalchemy.orm import Mapped
    >>> from sqlalchemy.orm import mapped_column
    >>> from sqlalchemy.orm import relationship
    >>> from sqlalchemy.orm import Session
    >>>
    >>>
    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class Company(Base):
    ...     __tablename__ = "company"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     employees: Mapped[List["Employee"]] = relationship(back_populates="company")
    >>>
    >>> class Employee(Base):
    ...     __tablename__ = "employee"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str]
    ...     type: Mapped[str]
    ...     company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    ...     company: Mapped[Company] = relationship(back_populates="employees")
    ...
    ...     def __repr__(self):
    ...         return f"{self.__class__.__name__}({self.name!r})"
    ...
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "employee",
    ...         "polymorphic_on": "type",
    ...     }
    >>>
    >>> class Manager(Employee):
    ...     __tablename__ = "manager"
    ...     id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
    ...     manager_name: Mapped[str]
    ...     paperwork: Mapped[List["Paperwork"]] = relationship()
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "manager",
    ...     }
    >>> class Paperwork(Base):
    ...     __tablename__ = "paperwork"
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     manager_id: Mapped[int] = mapped_column(ForeignKey("manager.id"))
    ...     document_name: Mapped[str]
    ...
    ...     def __repr__(self):
    ...         return f"Paperwork({self.document_name!r})"
    >>>
    >>> class Engineer(Employee):
    ...     __tablename__ = "engineer"
    ...     id: Mapped[int] = mapped_column(ForeignKey("employee.id"), primary_key=True)
    ...     engineer_info: Mapped[str]
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "engineer",
    ...     }
    >>>
    >>> engine = create_engine("sqlite://", echo=True)
    >>>
    >>> Base.metadata.create_all(engine)
    BEGIN ...

    >>> conn = engine.connect()
    >>> from sqlalchemy.orm import Session
    >>> session = Session(conn)
    >>> session.add(
    ...     Company(
    ...         name="Krusty Krab",
    ...         employees=[
    ...             Manager(
    ...                 name="Mr. Krabs",
    ...                 manager_name="Eugene H. Krabs",
    ...                 paperwork=[
    ...                     Paperwork(document_name="Secret Recipes"),
    ...                     Paperwork(document_name="Krabby Patty Orders"),
    ...                 ],
    ...             ),
    ...             Engineer(name="SpongeBob", engineer_info="Krabby Patty Master"),
    ...             Engineer(
    ...                 name="Squidward",
    ...                 engineer_info="Senior Customer Engagement Engineer",
    ...             ),
    ...         ],
    ...     )
    ... )
    >>> session.commit()
    BEGIN ...


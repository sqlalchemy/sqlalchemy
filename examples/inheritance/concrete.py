"""Concrete-table (table-per-class) inheritance example."""
from __future__ import annotations

from typing import Annotated

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic


intpk = Annotated[int, mapped_column(primary_key=True)]
str50 = Annotated[str, mapped_column(String(50))]


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "company"
    id: Mapped[intpk]
    name: Mapped[str50]

    employees: Mapped[list[Person]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"Company {self.name}"


class Person(ConcreteBase, Base):
    __tablename__ = "person"
    id: Mapped[intpk]
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {
        "polymorphic_identity": "person",
    }

    def __repr__(self):
        return f"Ordinary person {self.name}"


class Engineer(Person):
    __tablename__ = "engineer"

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]
    status: Mapped[str50]
    engineer_name: Mapped[str50]
    primary_language: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "engineer", "concrete": True}


class Manager(Person):
    __tablename__ = "manager"

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
    name: Mapped[str50]
    status: Mapped[str50]
    manager_name: Mapped[str50]

    company: Mapped[Company] = relationship(back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "manager", "concrete": True}

    def __repr__(self):
        return (
            f"Manager {self.name}, status {self.status}, "
            f"manager_name {self.manager_name}"
        )


engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)

with Session(engine) as session:
    c = Company(
        name="company1",
        employees=[
            Manager(
                name="mr krabs",
                status="AAB",
                manager_name="manager1",
            ),
            Engineer(
                name="spongebob",
                status="BBA",
                engineer_name="engineer1",
                primary_language="java",
            ),
            Person(name="joesmith"),
            Engineer(
                name="patrick",
                status="CGG",
                engineer_name="engineer2",
                primary_language="python",
            ),
            Manager(name="jsmith", status="ABA", manager_name="manager2"),
        ],
    )
    session.add(c)

    session.commit()

    for e in c.employees:
        print(e)

    spongebob = session.scalars(
        select(Person).filter_by(name="spongebob")
    ).one()
    spongebob2 = session.scalars(
        select(Engineer).filter_by(name="spongebob")
    ).one()
    assert spongebob is spongebob2

    spongebob2.engineer_name = "hes spongebob!"

    session.commit()

    # query using with_polymorphic.
    # when using ConcreteBase, use "*" to use the default selectable
    # setting specific entities won't work right now.
    eng_manager = with_polymorphic(Person, "*")
    print(
        session.scalars(
            select(eng_manager).filter(
                or_(
                    eng_manager.Engineer.engineer_name == "engineer1",
                    eng_manager.Manager.manager_name == "manager2",
                )
            )
        ).all()
    )

    # illustrate join from Company.
    print(
        session.scalars(
            select(Company)
            .join(Company.employees.of_type(eng_manager))
            .filter(
                or_(
                    eng_manager.Engineer.engineer_name == "engineer1",
                    eng_manager.Manager.manager_name == "manager2",
                )
            )
        ).all()
    )

    session.commit()

"""Concrete-table (table-per-class) inheritance example."""

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import String
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic


Base = declarative_base()


class Company(Base):
    __tablename__ = "company"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

    employees = relationship(
        "Person", back_populates="company", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return "Company %s" % self.name


class Person(ConcreteBase, Base):
    __tablename__ = "person"
    id = Column(Integer, primary_key=True)
    company_id = Column(ForeignKey("company.id"))
    name = Column(String(50))

    company = relationship("Company", back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "person"}

    def __repr__(self):
        return "Ordinary person %s" % self.name


class Engineer(Person):
    __tablename__ = "engineer"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    company_id = Column(ForeignKey("company.id"))
    status = Column(String(30))
    engineer_name = Column(String(30))
    primary_language = Column(String(30))

    company = relationship("Company", back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "engineer", "concrete": True}

    def __repr__(self):
        return (
            "Engineer %s, status %s, engineer_name %s, "
            "primary_language %s"
            % (
                self.name,
                self.status,
                self.engineer_name,
                self.primary_language,
            )
        )


class Manager(Person):
    __tablename__ = "manager"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    company_id = Column(ForeignKey("company.id"))
    status = Column(String(30))
    manager_name = Column(String(30))

    company = relationship("Company", back_populates="employees")

    __mapper_args__ = {"polymorphic_identity": "manager", "concrete": True}

    def __repr__(self):
        return "Manager %s, status %s, manager_name %s" % (
            self.name,
            self.status,
            self.manager_name,
        )


engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)

session = Session(engine)

c = Company(
    name="company1",
    employees=[
        Manager(
            name="pointy haired boss", status="AAB", manager_name="manager1"
        ),
        Engineer(
            name="dilbert",
            status="BBA",
            engineer_name="engineer1",
            primary_language="java",
        ),
        Person(name="joesmith"),
        Engineer(
            name="wally",
            status="CGG",
            engineer_name="engineer2",
            primary_language="python",
        ),
        Manager(name="jsmith", status="ABA", manager_name="manager2"),
    ],
)
session.add(c)

session.commit()

c = session.query(Company).get(1)
for e in c.employees:
    print(e, inspect(e).key, e.company)
assert set([e.name for e in c.employees]) == set(
    ["pointy haired boss", "dilbert", "joesmith", "wally", "jsmith"]
)
print("\n")

dilbert = session.query(Person).filter_by(name="dilbert").one()
dilbert2 = session.query(Engineer).filter_by(name="dilbert").one()
assert dilbert is dilbert2

dilbert.engineer_name = "hes dilbert!"

session.commit()

c = session.query(Company).get(1)
for e in c.employees:
    print(e)

# query using with_polymorphic.
eng_manager = with_polymorphic(Person, [Engineer, Manager])
print(
    session.query(eng_manager)
    .filter(
        or_(
            eng_manager.Engineer.engineer_name == "engineer1",
            eng_manager.Manager.manager_name == "manager2",
        )
    )
    .all()
)

# illustrate join from Company
eng_manager = with_polymorphic(Person, [Engineer, Manager])
print(
    session.query(Company)
    .join(Company.employees.of_type(eng_manager))
    .filter(
        or_(
            eng_manager.Engineer.engineer_name == "engineer1",
            eng_manager.Manager.manager_name == "manager2",
        )
    )
    .all()
)

session.commit()

from typing import assert_type
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.util import AliasedClass


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "company"
    company_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


class Employee(Base):
    __tablename__ = "employee"
    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "employee",
    }
    employee_id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.company_id"))
    name: Mapped[str]
    type: Mapped[str]


class Manager(Employee):
    __tablename__ = "manager"
    __mapper_args__ = {
        "polymorphic_identity": "manager",
    }
    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employee.employee_id"), primary_key=True
    )
    manager_data: Mapped[str]


class Engineer(Employee):
    __tablename__ = "engineer"
    __mapper_args__ = {
        "polymorphic_identity": "engineer",
    }
    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employee.employee_id"), primary_key=True
    )
    engineer_info: Mapped[str]


engine = create_engine("sqlite://")


def test_with_polymorphic_aliased_in_join() -> None:
    """Test that with_polymorphic with aliased=True can be used in join().

    Relates to discussion #13075.
    """
    manager_employee = with_polymorphic(
        Employee, [Manager], aliased=True, flat=True
    )
    engineer_employee = with_polymorphic(
        Employee, [Engineer], aliased=True, flat=True
    )

    if TYPE_CHECKING:
        assert_type(manager_employee, AliasedClass[Employee])
        assert_type(engineer_employee, AliasedClass[Employee])

    # Should not produce a type error
    stmt = select(manager_employee, engineer_employee).join(
        engineer_employee,
        engineer_employee.company_id == manager_employee.company_id,
    )

    with Session(engine) as session:
        session.execute(stmt)


def test_with_polymorphic_aliased_in_row_mapping() -> None:
    """Test that with_polymorphic result can be used as key in row._mapping.

    Relates to discussion #13075.
    """
    manager_employee = with_polymorphic(
        Employee, [Manager], aliased=True, flat=True
    )
    engineer_employee = with_polymorphic(
        Employee, [Engineer], aliased=True, flat=True
    )

    stmt = select(manager_employee, engineer_employee).join(
        engineer_employee,
        engineer_employee.company_id == manager_employee.company_id,
    )

    with Session(engine) as session:
        for row in session.execute(stmt):
            # Should not produce a type error - discussion #13075
            _ = row._mapping[manager_employee]
            _ = row._mapping[engineer_employee]


def test_with_polymorphic_non_aliased_in_row_mapping() -> None:
    """Test with_polymorphic without aliased=True in row._mapping."""
    poly_employee = with_polymorphic(Employee, [Manager, Engineer])

    stmt = select(poly_employee)

    with Session(engine) as session:
        for row in session.execute(stmt):
            _ = row._mapping[poly_employee]

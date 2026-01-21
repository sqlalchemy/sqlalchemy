"""Illustrates a generic association which persists association
objects within individual tables, each one generated to persist
those objects on behalf of a particular parent class.

This configuration has the advantage that each type of parent
maintains its "Address" rows separately, so that collection
size for one type of parent will have no impact on other types
of parent.   Navigation between parent and "Address" is simple,
direct, and bidirectional.

This recipe is the most efficient (speed wise and storage wise)
and simple of all of them.

The creation of many related tables may seem at first like an issue
but there really isn't any - the management and targeting of these tables
is completely automated.

"""

from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    """Base class which provides automated table name
    and surrogate primary key column.

    """

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    id: Mapped[int] = mapped_column(primary_key=True)


class Address:
    """Define columns that will be present in each
    'Address' table.

    This is a declarative mixin, so additional mapped
    attributes beyond simple columns specified here
    should be set up using @declared_attr.

    """

    street: Mapped[str]
    city: Mapped[str]
    zip: Mapped[str]

    def __repr__(self) -> str:
        return "%s(street=%r, city=%r, zip=%r)" % (
            self.__class__.__name__,
            self.street,
            self.city,
            self.zip,
        )


if TYPE_CHECKING:

    class AddressWithParent(Address):
        """Type stub for Address subclasses created by HasAddresses.

        Inherits street, city, zip from Address.

        Allows mypy to understand when <class>.Address is created,
        it will have `parent_id` and `parent` attributes.
        If you won't use `parent_id` attribute directly,
        there's no need to specify here, included for completeness.
        """

        parent_id: int
        parent: HasAddresses


class HasAddresses:
    """HasAddresses mixin, creates a new Address class
    for each parent.

    """

    @declared_attr
    def addresses(cls: type[Any]) -> Mapped[list[AddressWithParent]]:
        cls.Address = type(
            f"{cls.__name__}Address",
            (Address, Base),
            dict(
                __tablename__=f"{cls.__tablename__}_address",
                parent_id=mapped_column(
                    Integer, ForeignKey(f"{cls.__tablename__}.id")
                ),
                parent=relationship(cls, overlaps="addresses"),
            ),
        )
        return relationship(cls.Address)


class Customer(HasAddresses, Base):
    name: Mapped[str]


class Supplier(HasAddresses, Base):
    company_name: Mapped[str]


engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)

session = Session(engine)

session.add_all(
    [
        Customer(
            name="customer 1",
            addresses=[
                Customer.Address(
                    street="123 anywhere street", city="New York", zip="10110"
                ),
                Customer.Address(
                    street="40 main street", city="San Francisco", zip="95732"
                ),
            ],
        ),
        Supplier(
            company_name="Ace Hammers",
            addresses=[
                Supplier.Address(
                    street="2569 west elm", city="Detroit", zip="56785"
                )
            ],
        ),
    ]
)

session.commit()

for customer in session.query(Customer):
    for address in customer.addresses:
        print(address)
        print(address.parent)

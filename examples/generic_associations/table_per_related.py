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
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


@as_declarative()
class Base(object):
    """Base class which provides automated table name
    and surrogate primary key column.

    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)


class Address(object):
    """Define columns that will be present in each
    'Address' table.

    This is a declarative mixin, so additional mapped
    attributes beyond simple columns specified here
    should be set up using @declared_attr.

    """

    street = Column(String)
    city = Column(String)
    zip = Column(String)

    def __repr__(self):
        return "%s(street=%r, city=%r, zip=%r)" % (
            self.__class__.__name__,
            self.street,
            self.city,
            self.zip,
        )


class HasAddresses(object):
    """HasAddresses mixin, creates a new Address class
    for each parent.

    """

    @declared_attr
    def addresses(cls):
        cls.Address = type(
            "%sAddress" % cls.__name__,
            (Address, Base),
            dict(
                __tablename__="%s_address" % cls.__tablename__,
                parent_id=Column(
                    Integer, ForeignKey("%s.id" % cls.__tablename__)
                ),
                parent=relationship(cls),
            ),
        )
        return relationship(cls.Address)


class Customer(HasAddresses, Base):
    name = Column(String)


class Supplier(HasAddresses, Base):
    company_name = Column(String)


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

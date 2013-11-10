"""discriminator_on_related.py

Illustrates a mixin which provides a generic association
using a single target table and a single association table,
referred to by all parent tables.  The association table
contains a "discriminator" column which determines what type of
parent object associates to each particular row in the association
table.

SQLAlchemy's single-table-inheritance feature is used
to target different association types.

This configuration attempts to simulate a so-called "generic foreign key"
as closely as possible without actually foregoing the use of real
foreign keys.   Unlike table-per-related and table-per-association,
it uses a fixed number of tables to serve any number of potential parent
objects, but is also slightly more complex.

"""
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import create_engine, Integer, Column, \
                    String, ForeignKey
from sqlalchemy.orm import Session, relationship, backref
from sqlalchemy.ext.associationproxy import association_proxy

@as_declarative()
class Base(object):
    """Base class which provides automated table name
    and surrogate primary key column.

    """
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    id = Column(Integer, primary_key=True)

class AddressAssociation(Base):
    """Associates a collection of Address objects
    with a particular parent.

    """
    __tablename__ = "address_association"

    discriminator = Column(String)
    """Refers to the type of parent."""

    __mapper_args__ = {"polymorphic_on": discriminator}

class Address(Base):
    """The Address class.

    This represents all address records in a
    single table.

    """
    association_id = Column(Integer, ForeignKey("address_association.id"))
    street = Column(String)
    city = Column(String)
    zip = Column(String)
    association = relationship("AddressAssociation", backref="addresses")

    parent = association_proxy("association", "parent")

    def __repr__(self):
        return "%s(street=%r, city=%r, zip=%r)" % \
            (self.__class__.__name__, self.street,
            self.city, self.zip)

class HasAddresses(object):
    """HasAddresses mixin, creates a relationship to
    the address_association table for each parent.

    """
    @declared_attr
    def address_association_id(cls):
        return Column(Integer, ForeignKey("address_association.id"))

    @declared_attr
    def address_association(cls):
        name = cls.__name__
        discriminator = name.lower()

        assoc_cls = type(
                        "%sAddressAssociation" % name,
                        (AddressAssociation, ),
                        dict(
                            __mapper_args__={
                                "polymorphic_identity": discriminator
                            }
                        )
                    )

        cls.addresses = association_proxy(
                    "address_association", "addresses",
                    creator=lambda addresses: assoc_cls(addresses=addresses)
                )
        return relationship(assoc_cls,
                    backref=backref("parent", uselist=False))


class Customer(HasAddresses, Base):
    name = Column(String)

class Supplier(HasAddresses, Base):
    company_name = Column(String)

engine = create_engine('sqlite://', echo=True)
Base.metadata.create_all(engine)

session = Session(engine)

session.add_all([
    Customer(
        name='customer 1',
        addresses=[
            Address(
                    street='123 anywhere street',
                    city="New York",
                    zip="10110"),
            Address(
                    street='40 main street',
                    city="San Francisco",
                    zip="95732")
        ]
    ),
    Supplier(
        company_name="Ace Hammers",
        addresses=[
            Address(
                    street='2569 west elm',
                    city="Detroit",
                    zip="56785")
        ]
    ),
])

session.commit()

for customer in session.query(Customer):
    for address in customer.addresses:
        print(address)
        print(address.parent)
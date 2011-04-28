"""discriminator_on_related.py

The HasAddresses mixin will provide a relationship
to the fixed Address table based on a fixed association table.

The association table will also contain a "discriminator"
which determines what type of parent object associates to the
Address row.

This is a "polymorphic association".   Even though a "discriminator"
that refers to a particular table is present, the extra association
table is used so that traditional foreign key constraints may be used.

This configuration has the advantage that a fixed set of tables
are used, with no extra-table-per-parent needed.   The individual 
Address record can also locate its parent with no need to scan 
amongst many tables.

"""
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import create_engine, Integer, Column, \
                    String, ForeignKey, Table
from sqlalchemy.orm import Session, relationship, backref
from sqlalchemy.ext.associationproxy import association_proxy

class Base(object):
    """Base class which provides automated table name
    and surrogate primary key column.
    
    """
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    id = Column(Integer, primary_key=True)
Base = declarative_base(cls=Base)

class AddressAssociation(Base):
    """Associates a collection of Address objects
    with a particular parent.
    
    """
    __tablename__ = "address_association"

    @classmethod
    def creator(cls, discriminator):
        """Provide a 'creator' function to use with 
        the association proxy."""

        return lambda addresses:AddressAssociation(
                                addresses=addresses, 
                                discriminator=discriminator)

    discriminator = Column(String)
    """Refers to the type of parent."""

    @property
    def parent(self):
        """Return the parent object."""
        return getattr(self, "%s_parent" % self.discriminator)

class Address(Base):
    """The Address class.   
    
    This represents all address records in a 
    single table.
    
    """
    association_id = Column(Integer, 
                        ForeignKey("address_association.id")
                    )
    street = Column(String)
    city = Column(String)
    zip = Column(String)
    association = relationship(
                    "AddressAssociation", 
                    backref="addresses")

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
        return Column(Integer, 
                                ForeignKey("address_association.id"))

    @declared_attr
    def address_association(cls):
        discriminator = cls.__name__.lower()
        cls.addresses= association_proxy(
                    "address_association", "addresses",
                    creator=AddressAssociation.creator(discriminator)
                )
        return relationship("AddressAssociation", 
                    backref=backref("%s_parent" % discriminator, 
                                        uselist=False))


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
        print address
        print address.parent
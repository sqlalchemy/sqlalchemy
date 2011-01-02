"""Model.   We are modeling Person objects with a collection
of Address objects.  Each Address has a PostalCode, which 
in turn references a City and then a Country:

Person --(1..n)--> Address
Address --(has a)--> PostalCode
PostalCode --(has a)--> City
City --(has a)--> Country

"""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from caching_query import FromCache, RelationshipCache
from environment import Base, bootstrap

class Country(Base):
    __tablename__ = 'country'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)

    def __init__(self, name):
        self.name = name

class City(Base):
    __tablename__ = 'city'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    country_id = Column(Integer, ForeignKey('country.id'), nullable=False)
    country = relationship(Country)

    def __init__(self, name, country):
        self.name = name
        self.country = country

class PostalCode(Base):
    __tablename__ = 'postal_code'

    id = Column(Integer, primary_key=True)
    code = Column(String(10), nullable=False)
    city_id = Column(Integer, ForeignKey('city.id'), nullable=False)
    city = relationship(City)

    @property
    def country(self):
        return self.city.country

    def __init__(self, code, city):
        self.code = code
        self.city = city

class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    street = Column(String(200), nullable=False)
    postal_code_id = Column(Integer, ForeignKey('postal_code.id'))
    postal_code = relationship(PostalCode)

    @property
    def city(self):
        return self.postal_code.city

    @property
    def country(self):
        return self.postal_code.country

    def __str__(self):
        return "%s\t"\
              "%s, %s\t"\
              "%s" % (self.street, self.city.name, 
                self.postal_code.code, self.country.name)

class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    addresses = relationship(Address, collection_class=set)

    def __init__(self, name, *addresses):
        self.name = name
        self.addresses = set(addresses)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Person(name=%r)" % self.name

    def format_full(self):
        return "\t".join([str(x) for x in [self] + list(self.addresses)])

# Caching options.   A set of three RelationshipCache options
# which can be applied to Query(), causing the "lazy load"
# of these attributes to be loaded from cache.
cache_address_bits = RelationshipCache("default", "byid", PostalCode.city).\
                and_(
                    RelationshipCache("default", "byid", City.country)
                ).and_(
                    RelationshipCache("default", "byid", Address.postal_code)
                )

bootstrap()
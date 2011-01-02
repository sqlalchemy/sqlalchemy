"""fixture_data.py

Installs some sample data.   Here we have a handful of postal codes for a few US/
Canadian cities.   Then, 100 Person records are installed, each with a
randomly selected postal code.

"""
from environment import Session, Base
from model import City, Country, PostalCode, Person, Address
import random

def install():
    Base.metadata.create_all(Session().bind)

    data = [
        ('Chicago', 'United States', ('60601', '60602', '60603', '60604')),
        ('Montreal', 'Canada', ('H2S 3K9', 'H2B 1V4', 'H7G 2T8')),
        ('Edmonton', 'Canada', ('T5J 1R9', 'T5J 1Z4', 'T5H 1P6')),
        ('New York', 'United States', ('10001', '10002', '10003', '10004', '10005', '10006')),
        ('San Francisco', 'United States', ('94102', '94103', '94104', '94105', '94107', '94108'))
    ]

    countries = {}
    all_post_codes = []
    for city, country, postcodes in data:
        try:
            country = countries[country]
        except KeyError:
            countries[country] = country = Country(country)

        city = City(city, country)
        pc = [PostalCode(code, city) for code in postcodes]
        Session.add_all(pc)
        all_post_codes.extend(pc)

    for i in xrange(1, 51):
        person = Person(
                    "person %.2d" % i,
                    Address(
                        street="street %.2d" % i, 
                        postal_code=all_post_codes[random.randint(0, len(all_post_codes) - 1)]
                    )
                )
        Session.add(person)

    Session.commit()

    # start the demo fresh
    Session.remove()
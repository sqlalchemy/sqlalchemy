from activemapper           import ActiveMapper, column, one_to_many, one_to_one
from sqlalchemy             import objectstore
from sqlalchemy             import and_, or_
from sqlalchemy             import ForeignKey, String, Integer, DateTime
from datetime               import datetime

import unittest
import activemapper

#
# application-level model objects
#

class Person(ActiveMapper):
    class mapping:
        id          = column(Integer, primary_key=True)
        full_name   = column(String)
        first_name  = column(String)
        middle_name = column(String)
        last_name   = column(String)
        birth_date  = column(DateTime)
        ssn         = column(String)
        gender      = column(String)
        home_phone  = column(String)
        cell_phone  = column(String)
        work_phone  = column(String)
        prefs_id    = column(Integer, foreign_key=ForeignKey('preferences.id'))
        addresses   = one_to_many('Address', colname='person_id', backref='person')
        preferences = one_to_one('Preferences', colname='pref_id', backref='person')
    
    def __str__(self):
        s =  '%s\n' % self.full_name
        s += '  * birthdate: %s\n' % (self.birth_date or 'not provided')
        s += '  * fave color: %s\n' % (self.preferences.favorite_color or 'Unknown')
        s += '  * personality: %s\n' % (self.preferences.personality_type or 'Unknown')
        
        for address in self.addresses:
            s += '  * address: %s\n' % address.address_1
            s += '             %s, %s %s\n' % (address.city, address.state, address.postal_code)
        
        return s


class Preferences(ActiveMapper):
    class mapping:
        __table__        = 'preferences'
        id               = column(Integer, primary_key=True)
        favorite_color   = column(String)
        personality_type = column(String)


class Address(ActiveMapper):
    class mapping:
        id          = column(Integer, primary_key=True)
        type        = column(String)
        address_1   = column(String)
        city        = column(String)
        state       = column(String)
        postal_code = column(String)
        person_id   = column(Integer, foreign_key=ForeignKey('person.id'))



class testcase(unittest.TestCase):    
    
    def tearDown(self):
        people = Person.select()
        for person in people: person.delete()
        
        addresses = Address.select()
        for address in addresses: address.delete()
        
        preferences = Preferences.select()
        for preference in preferences: preference.delete()
        
        objectstore.commit()
        objectstore.clear()
    
    def create_person_one(self):
        # create a person
        p1 = Person(
                full_name='Jonathan LaCour',
                birth_date=datetime(1979, 10, 12),
                preferences=Preferences(
                                favorite_color='Green',
                                personality_type='ENTP'
                            ),
                addresses=[
                    Address(
                        address_1='123 Some Great Road.',
                        city='Atlanta',
                        state='GA',
                        postal_code='30338'
                    ),
                    Address(
                        address_1='435 Franklin Road.',
                        city='Atlanta',
                        state='GA',
                        postal_code='30342'
                    )
                ]
             )
        return p1
    
    
    def create_person_two(self):
        p2 = Person(
                full_name='Lacey LaCour',
                addresses=[
                    Address(
                        address_1='123 Some Great Road.',
                        city='Atlanta',
                        state='GA',
                        postal_code='30338'
                    ),
                    Address(
                        address_1='200 Main Street',
                        city='Roswell',
                        state='GA',
                        postal_code='30075'
                    )
                ]
             )
        # I don't like that I have to do this... and putting
        # a "self.preferences = Preferences()" into the __init__
        # of Person also doens't seem to fix this
        p2.preferences = Preferences()
        
        return p2
    
    
    def test_create(self):
        p1 = self.create_person_one()
        
        objectstore.commit()
        objectstore.clear()
        
        results = Person.select()
        
        self.assertEquals(len(results), 1)
        
        person = results[0]
        self.assertEquals(person.id, p1.id)
        self.assertEquals(len(person.addresses), 2)
        self.assertEquals(person.addresses[0].postal_code, '30338')
    
    
    def test_delete(self):
        p1 = self.create_person_one()
        
        objectstore.commit()
        objectstore.clear()
        
        results = Person.select()
        self.assertEquals(len(results), 1)
        
        results[0].delete()
        objectstore.commit()
        objectstore.clear()
        
        results = Person.select()
        self.assertEquals(len(results), 0)
    
    
    def test_multiple(self):
        p1 = self.create_person_one()
        p2 = self.create_person_two()
        
        objectstore.commit()
        objectstore.clear()
        
        # select and make sure we get back two results
        people = Person.select()
        self.assertEquals(len(people), 2)
                
        # make sure that our backwards relationships work
        self.assertEquals(people[0].addresses[0].person.id, p1.id)
        self.assertEquals(people[1].addresses[0].person.id, p2.id)
        
        # try a more complex select
        results = Person.select(
            or_(
                and_(
                    Address.c.person_id == Person.c.id,
                    Address.c.postal_code.like('30075')
                ),
                and_(
                    Person.c.prefs_id == Preferences.c.id,
                    Preferences.c.favorite_color == 'Green'
                )
            )
        )
        self.assertEquals(len(results), 2)
        
    
    def test_oneway_backref(self):
        # FIXME: I don't know why, but it seems that my backwards relationship
        #        on preferences still ends up being a list even though I pass
        #        in uselist=False...
        p1 = self.create_person_one()
        self.assertEquals(p1.preferences.person, p1)
        p1.delete()
        
        objectstore.commit()
        objectstore.clear()
    
    
    def test_select_by(self):
        # FIXME: either I don't understand select_by, or it doesn't work.
        
        p1 = self.create_person_one()
        p2 = self.create_person_two()
        
        objectstore.commit()
        objectstore.clear()
        
        results = Person.select_by(
            Address.c.postal_code.like('30075')
        )
        self.assertEquals(len(results), 1)


    
if __name__ == '__main__':
    # go ahead and setup the database connection, and create the tables
    activemapper.engine.connect('sqlite:///', echo=False)
    activemapper.create_tables()
    
    # launch the unit tests
    unittest.main()
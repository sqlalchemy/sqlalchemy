import testbase
from sqlalchemy.ext.activemapper           import ActiveMapper, column, one_to_many, one_to_one, objectstore
from sqlalchemy             import and_, or_, clear_mappers
from sqlalchemy             import ForeignKey, String, Integer, DateTime
from datetime               import datetime

import sqlalchemy.ext.activemapper as activemapper


class testcase(testbase.PersistTest):
    def setUpAll(self):
        global Person, Preferences, Address
        
        class Person(ActiveMapper):
            class mapping:
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
                addresses   = one_to_many('Address', colname='person_id', backref='person', order_by=['state', 'city', 'postal_code'])
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
                favorite_color   = column(String)
                personality_type = column(String)

        class Address(ActiveMapper):
            class mapping:
                # note that in other objects, the 'id' primary key is 
                # automatically added -- if you specify a primary key,
                # then ActiveMapper will not add an integer primary key
                # for you.
                id          = column(Integer, primary_key=True)
                type        = column(String)
                address_1   = column(String)
                city        = column(String)
                state       = column(String)
                postal_code = column(String)
                person_id   = column(Integer, foreign_key=ForeignKey('person.id'))

        activemapper.metadata.connect(testbase.db)
        activemapper.create_tables()

    def tearDownAll(self):
        clear_mappers()
        activemapper.drop_tables()
        
    def tearDown(self):
        for t in activemapper.metadata.table_iterator(reverse=True):
            t.delete().execute()
    
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
        objectstore.flush()
        objectstore.clear()
        
        results = Person.select()
        
        self.assertEquals(len(results), 1)
        
        person = results[0]
        self.assertEquals(person.id, p1.id)
        self.assertEquals(len(person.addresses), 2)
        self.assertEquals(person.addresses[0].postal_code, '30338')
    
    
    def test_delete(self):
        p1 = self.create_person_one()
        
        objectstore.flush()
        objectstore.clear()
        
        results = Person.select()
        self.assertEquals(len(results), 1)
        
        results[0].delete()
        objectstore.flush()
        objectstore.clear()
        
        results = Person.select()
        self.assertEquals(len(results), 0)
    
    
    def test_multiple(self):
        p1 = self.create_person_one()
        p2 = self.create_person_two()
        
        objectstore.flush()
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
        # FIXED: the backref is a new PropertyLoader which needs its own "uselist".
        # uses a function which I dont think existed when you first wrote ActiveMapper.
        p1 = self.create_person_one()
        self.assertEquals(p1.preferences.person, p1)
        p1.delete()
        
        objectstore.flush()
        objectstore.clear()
    
    
    def test_select_by(self):
        # FIXME: either I don't understand select_by, or it doesn't work.
        # FIXED (as good as we can for now): yup....everyone thinks it works that way....it only
        # generates joins for keyword arguments, not ColumnClause args.  would need a new layer of
        # "MapperClause" objects to use properties in expressions. (MB)
        
        p1 = self.create_person_one()
        p2 = self.create_person_two()
        
        objectstore.flush()
        objectstore.clear()
        
        results = Person.select(
            Address.c.postal_code.like('30075') &
            Person.join_to('addresses')
        )
        self.assertEquals(len(results), 1)

    
if __name__ == '__main__':
    unittest.main()

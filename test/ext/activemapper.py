import testbase
from datetime import datetime

from sqlalchemy.ext.activemapper           import ActiveMapper, column, one_to_many, one_to_one, many_to_many, objectstore
from sqlalchemy             import and_, or_, exceptions
from sqlalchemy             import ForeignKey, String, Integer, DateTime, Table, Column
from sqlalchemy.orm         import clear_mappers, backref, create_session, class_mapper
import sqlalchemy.ext.activemapper as activemapper
import sqlalchemy
from testlib import *


class testcase(PersistTest):
    def setUpAll(self):
        clear_mappers()
        objectstore.clear()
        global Person, Preferences, Address
        
        class Person(ActiveMapper):
            class mapping:
                __version_id_col__ = 'row_version'
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
                row_version = column(Integer, default=0)
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
        
        results = Person.query.select()
        
        self.assertEquals(len(results), 1)
        
        person = results[0]
        self.assertEquals(person.id, p1.id)
        self.assertEquals(len(person.addresses), 2)
        self.assertEquals(person.addresses[0].postal_code, '30338')

    @testing.unsupported('mysql')
    def test_update(self):
        p1 = self.create_person_one()
        objectstore.flush()
        objectstore.clear()
        
        person = Person.query.select()[0]
        person.gender = 'F'
        objectstore.flush()
        objectstore.clear()
        self.assertEquals(person.row_version, 2)

        person = Person.query.select()[0]
        person.gender = 'M'
        objectstore.flush()
        objectstore.clear()
        self.assertEquals(person.row_version, 3)

        #TODO: check that a concurrent modification raises exception
        p1 = Person.query.select()[0]
        s1 = objectstore.session
        s2 = create_session()
        objectstore.context.current = s2
        p2 = Person.query.select()[0]
        p1.first_name = "jack"
        p2.first_name = "ed"
        objectstore.flush()
        try:
            objectstore.context.current = s1
            objectstore.flush()
            # Only dialects with a sane rowcount can detect the ConcurrentModificationError
            if testbase.db.dialect.supports_sane_rowcount:
                assert False
        except exceptions.ConcurrentModificationError:
            pass
        
    
    def test_delete(self):
        p1 = self.create_person_one()
        
        objectstore.flush()
        objectstore.clear()
        
        results = Person.query.select()
        self.assertEquals(len(results), 1)
        
        results[0].delete()
        objectstore.flush()
        objectstore.clear()
        
        results = Person.query.select()
        self.assertEquals(len(results), 0)
    
    
    def test_multiple(self):
        p1 = self.create_person_one()
        p2 = self.create_person_two()
        
        objectstore.flush()
        objectstore.clear()
        
        # select and make sure we get back two results
        people = Person.query.select()
        self.assertEquals(len(people), 2)
                
        # make sure that our backwards relationships work
        self.assertEquals(people[0].addresses[0].person.id, p1.id)
        self.assertEquals(people[1].addresses[0].person.id, p2.id)
        
        # try a more complex select
        results = Person.query.select(
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
        
        results = Person.query.join('addresses').select(
            Address.c.postal_code.like('30075') 
        )
        self.assertEquals(len(results), 1)

        self.assertEquals(Person.query.count(), 2)

class testmanytomany(PersistTest):
     def setUpAll(self):
         clear_mappers()
         objectstore.clear()
         global secondarytable, foo, baz
         secondarytable = Table("secondarytable",
             activemapper.metadata,
             Column("foo_id", Integer, ForeignKey("foo.id"),primary_key=True),
             Column("baz_id", Integer, ForeignKey("baz.id"),primary_key=True))

         class foo(activemapper.ActiveMapper):
             class mapping:
                 name = column(String(30))
#                 bazrel = many_to_many('baz', secondarytable, backref='foorel')

         class baz(activemapper.ActiveMapper):
             class mapping:
                 name = column(String(30))
                 foorel = many_to_many("foo", secondarytable, backref='bazrel')

         activemapper.metadata.connect(testbase.db)
         activemapper.create_tables()

     # Create a couple of activemapper objects
     def create_objects(self):
         return foo(name='foo1'), baz(name='baz1')

     def tearDownAll(self):
         clear_mappers()
         activemapper.drop_tables()
         objectstore.clear()
     def testbasic(self):
         # Set up activemapper objects
         foo1, baz1 = self.create_objects()

         objectstore.flush()
         objectstore.clear()

         foo1 = foo.query.get_by(name='foo1')
         baz1 = baz.query.get_by(name='baz1')
         
         # Just checking ...
         assert (foo1.name == 'foo1')
         assert (baz1.name == 'baz1')

         # Diagnostics ...
         # import sys
         # sys.stderr.write("\nbazrel missing from dir(foo1):\n%s\n"  % dir(foo1))
         # sys.stderr.write("\nbazrel in foo1 relations:\n%s\n" %  foo1.relations)

         # Optimistically based on activemapper one_to_many test, try  to append
         # baz1 to foo1.bazrel - (AttributeError: 'foo' object has no attribute 'bazrel')
         foo1.bazrel.append(baz1)
         assert (foo1.bazrel == [baz1])
        
class testselfreferential(PersistTest):
    def setUpAll(self):
        clear_mappers()
        objectstore.clear()
        global TreeNode
        class TreeNode(activemapper.ActiveMapper):
            class mapping:
                id = column(Integer, primary_key=True)
                name = column(String(30))
                parent_id = column(Integer, foreign_key=ForeignKey('treenode.id'))
                children = one_to_many('TreeNode', colname='id', backref='parent')
                
        activemapper.metadata.connect(testbase.db)
        activemapper.create_tables()
    def tearDownAll(self):
        clear_mappers()
        activemapper.drop_tables()

    def testbasic(self):
        t = TreeNode(name='node1')
        t.children.append(TreeNode(name='node2'))
        t.children.append(TreeNode(name='node3'))
        objectstore.flush()
        objectstore.clear()
        
        t = TreeNode.query.get_by(name='node1')
        assert (t.name == 'node1')
        assert (t.children[0].name == 'node2')
        assert (t.children[1].name == 'node3')
        assert (t.children[1].parent is t)

        objectstore.clear()
        t = TreeNode.query.get_by(name='node3')
        assert (t.parent is TreeNode.query.get_by(name='node1'))
        
if __name__ == '__main__':
    testbase.main()

"""test attribute/instance expiration, deferral of attributes, etc."""

import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *
import gc

class ExpireTest(FixtureTest):
    keep_mappers = False
    refresh_data = True

    def test_expire(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(7)
        assert len(u.addresses) == 1
        u.name = 'foo'
        del u.addresses[0]
        sess.expire(u)

        assert 'name' not in u.__dict__

        def go():
            assert u.name == 'jack'
        self.assert_sql_count(testing.db, go, 1)
        assert 'name' in u.__dict__

        u.name = 'foo'
        sess.flush()
        # change the value in the DB
        users.update(users.c.id==7, values=dict(name='jack')).execute()
        sess.expire(u)
        # object isnt refreshed yet, using dict to bypass trigger
        assert u.__dict__.get('name') != 'jack'

        sess.query(User).all()
        # test that it refreshed
        assert u.__dict__['name'] == 'jack'

        def go():
            assert u.name == 'jack'
        self.assert_sql_count(testing.db, go, 0)

    def test_expire_doesntload_on_set(self):
        mapper(User, users)

        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u, attribute_names=['name'])
        def go():
            u.name = 'somenewname'
        self.assert_sql_count(testing.db, go, 0)
        sess.flush()
        sess.clear()
        assert sess.query(User).get(7).name == 'somenewname'

    def test_no_session(self):
        mapper(User, users)
        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u, attribute_names=['name'])
        sess.expunge(u)
        try:
            u.name
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Instance <class 'testlib.fixtures.User'> is not bound to a Session, and no contextual session is established; attribute refresh operation cannot proceed"
    
    def test_no_instance_key(self):
        # this tests an artificial condition such that 
        # an instance is pending, but has expired attributes.  this
        # is actually part of a larger behavior when postfetch needs to 
        # occur during a flush() on an instance that was just inserted
        mapper(User, users)
        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u, attribute_names=['name'])
        sess.expunge(u)
        del u._instance_key
        assert 'name' not in u.__dict__
        sess.save(u)
        assert u.name == 'jack'
        
    def test_expire_preserves_changes(self):
        """test that the expire load operation doesn't revert post-expire changes"""

        mapper(Order, orders)
        sess = create_session()
        o = sess.query(Order).get(3)
        sess.expire(o)

        o.description = "order 3 modified"
        def go():
            assert o.isopen == 1
        self.assert_sql_count(testing.db, go, 1)
        assert o.description == 'order 3 modified'

        del o.description
        assert "description" not in o.__dict__
        sess.expire(o, ['isopen'])
        sess.query(Order).all()
        assert o.isopen == 1
        assert "description" not in o.__dict__

        assert o.description is None

        o.isopen=15
        sess.expire(o, ['isopen', 'description'])
        o.description = 'some new description'
        sess.query(Order).all()
        assert o.isopen == 1
        assert o.description == 'some new description'

        sess.expire(o, ['isopen', 'description'])
        sess.query(Order).all()
        del o.isopen
        def go():
            assert o.isopen is None
        self.assert_sql_count(testing.db, go, 0)

        o.isopen=14
        sess.expire(o)
        o.description = 'another new description'
        sess.query(Order).all()
        assert o.isopen == 1
        assert o.description == 'another new description'
        
        
    def test_expire_committed(self):
        """test that the committed state of the attribute receives the most recent DB data"""
        mapper(Order, orders)

        sess = create_session()
        o = sess.query(Order).get(3)
        sess.expire(o)

        orders.update(id=3).execute(description='order 3 modified')
        assert o.isopen == 1
        assert o._state.dict['description'] == 'order 3 modified'
        def go():
            sess.flush()
        self.assert_sql_count(testing.db, go, 0)

    def test_expire_cascade(self):
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all, refresh-expire")
        })
        mapper(Address, addresses)
        s = create_session()
        u = s.get(User, 8)
        assert u.addresses[0].email_address == 'ed@wood.com'

        u.addresses[0].email_address = 'someotheraddress'
        s.expire(u)
        u.name
        print u._state.dict
        assert u.addresses[0].email_address == 'ed@wood.com'

    def test_expired_lazy(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u)
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address == 'jack@bean.com'
            assert u.name == 'jack'
        # two loads
        self.assert_sql_count(testing.db, go, 2)
        assert 'name' in u.__dict__
        assert 'addresses' in u.__dict__

    def test_expired_eager(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(7)

        sess.expire(u)
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address == 'jack@bean.com'
            assert u.name == 'jack'
        # two loads, since relation() + scalar are
        # separate right now on per-attribute load
        self.assert_sql_count(testing.db, go, 2)
        assert 'name' in u.__dict__
        assert 'addresses' in u.__dict__

        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__
    
        def go():
            sess.query(User).filter_by(id=7).one()
            assert u.addresses[0].email_address == 'jack@bean.com'
            assert u.name == 'jack'
        # one load, since relation() + scalar are
        # together when eager load used with Query
        self.assert_sql_count(testing.db, go, 1)
            
    def test_relation_changes_preserved(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)
        sess = create_session()
        u = sess.query(User).get(8)
        sess.expire(u, ['name', 'addresses'])
        u.addresses
        assert 'name' not in u.__dict__
        del u.addresses[1]
        u.name
        assert 'name' in u.__dict__
        assert len(u.addresses) == 2

    def test_eagerload_props_dontload(self):
        # relations currently have to load separately from scalar instances.  the use case is:
        # expire "addresses".  then access it.  lazy load fires off to load "addresses", but needs
        # foreign key or primary key attributes in order to lazy load; hits those attributes,
        # such as below it hits "u.id".  "u.id" triggers full unexpire operation, eagerloads
        # addresses since lazy=False.  this is all wihtin lazy load which fires unconditionally;
        # so an unnecessary eagerload (or lazyload) was issued.  would prefer not to complicate
        # lazyloading to "figure out" that the operation should be aborted right now.
        
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)
        sess = create_session()
        u = sess.query(User).get(8)
        sess.expire(u)
        u.id
        assert 'addresses' not in u.__dict__
        u.addresses
        assert 'addresses' in u.__dict__
        
    def test_expire_synonym(self):
        mapper(User, users, properties={
            'uname':synonym('name')
        })
        
        sess = create_session()
        u = sess.query(User).get(7)
        assert 'name' in u.__dict__
        assert u.uname == u.name
        
        sess.expire(u)
        assert 'name' not in u.__dict__
        
        users.update(users.c.id==7).execute(name='jack2')
        assert u.name == 'jack2'
        assert u.uname == 'jack2'
        assert 'name' in u.__dict__
        
        # this wont work unless we add API hooks through the attr. system
        # to provide "expire" behavior on a synonym
        #sess.expire(u, ['uname'])
        #users.update(users.c.id==7).execute(name='jack3')
        #assert u.uname == 'jack3'
        
    def test_partial_expire(self):
        mapper(Order, orders)

        sess = create_session()
        o = sess.query(Order).get(3)

        sess.expire(o, attribute_names=['description'])
        assert 'id' in o.__dict__
        assert 'description' not in o.__dict__
        assert o._state.dict['isopen'] == 1

        orders.update(orders.c.id==3).execute(description='order 3 modified')

        def go():
            assert o.description == 'order 3 modified'
        self.assert_sql_count(testing.db, go, 1)
        assert o._state.dict['description'] == 'order 3 modified'

        o.isopen = 5
        sess.expire(o, attribute_names=['description'])
        assert 'id' in o.__dict__
        assert 'description' not in o.__dict__
        assert o.__dict__['isopen'] == 5
        assert o._state.committed_state['isopen'] == 1

        def go():
            assert o.description == 'order 3 modified'
        self.assert_sql_count(testing.db, go, 1)
        assert o.__dict__['isopen'] == 5
        assert o._state.dict['description'] == 'order 3 modified'
        assert o._state.committed_state['isopen'] == 1

        sess.flush()

        sess.expire(o, attribute_names=['id', 'isopen', 'description'])
        assert 'id' not in o.__dict__
        assert 'isopen' not in o.__dict__
        assert 'description' not in o.__dict__
        def go():
            assert o.description == 'order 3 modified'
            assert o.id == 3
            assert o.isopen == 5
        self.assert_sql_count(testing.db, go, 1)

    def test_partial_expire_lazy(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)

        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        # hit the lazy loader.  just does the lazy load,
        # doesnt do the overall refresh
        def go():
            assert u.addresses[0].email_address=='ed@wood.com'
        self.assert_sql_count(testing.db, go, 1)

        assert 'name' not in u.__dict__

        # check that mods to expired lazy-load attributes
        # only do the lazy load
        sess.expire(u, ['name', 'addresses'])
        def go():
            u.addresses = [Address(id=10, email_address='foo@bar.com')]
        self.assert_sql_count(testing.db, go, 1)

        sess.flush()

        # flush has occurred, and addresses was modified,
        # so the addresses collection got committed and is
        # longer expired
        def go():
            assert u.addresses[0].email_address=='foo@bar.com'
            assert len(u.addresses) == 1
        self.assert_sql_count(testing.db, go, 0)

        # but the name attribute was never loaded and so
        # still loads
        def go():
            assert u.name == 'ed'
        self.assert_sql_count(testing.db, go, 1)

    def test_partial_expire_eager(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)

        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__

        def go():
            assert u.addresses[0].email_address=='ed@wood.com'
        self.assert_sql_count(testing.db, go, 1)

        # check that mods to expired eager-load attributes
        # do the refresh
        sess.expire(u, ['name', 'addresses'])
        def go():
            u.addresses = [Address(id=10, email_address='foo@bar.com')]
        self.assert_sql_count(testing.db, go, 1)
        sess.flush()

        # this should ideally trigger the whole load
        # but currently it works like the lazy case
        def go():
            assert u.addresses[0].email_address=='foo@bar.com'
            assert len(u.addresses) == 1
        self.assert_sql_count(testing.db, go, 0)

        def go():
            assert u.name == 'ed'
        # scalar attributes have their own load
        self.assert_sql_count(testing.db, go, 1)
        # ideally, this was already loaded, but we arent
        # doing it that way right now
        #self.assert_sql_count(testing.db, go, 0)

    def test_relations_load_on_query(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)
        assert 'name' in u.__dict__
        u.addresses
        assert 'addresses' in u.__dict__

        sess.expire(u, ['name', 'addresses'])
        assert 'name' not in u.__dict__
        assert 'addresses' not in u.__dict__
        sess.query(User).options(eagerload('addresses')).filter_by(id=8).all()
        assert 'name' in u.__dict__
        assert 'addresses' in u.__dict__
        
    def test_partial_expire_deferred(self):
        mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })

        sess = create_session()
        o = sess.query(Order).get(3)
        sess.expire(o, ['description', 'isopen'])
        assert 'isopen' not in o.__dict__
        assert 'description' not in o.__dict__

        # test that expired attribute access refreshes
        # the deferred
        def go():
            assert o.isopen == 1
            assert o.description == 'order 3'
        self.assert_sql_count(testing.db, go, 1)

        sess.expire(o, ['description', 'isopen'])
        assert 'isopen' not in o.__dict__
        assert 'description' not in o.__dict__
        # test that the deferred attribute triggers the full
        # reload
        def go():
            assert o.description == 'order 3'
            assert o.isopen == 1
        self.assert_sql_count(testing.db, go, 1)

        clear_mappers()

        mapper(Order, orders)
        sess.clear()

        # same tests, using deferred at the options level
        o = sess.query(Order).options(defer('description')).get(3)

        assert 'description' not in o.__dict__

        # sanity check
        def go():
            assert o.description == 'order 3'
        self.assert_sql_count(testing.db, go, 1)

        assert 'description' in o.__dict__
        assert 'isopen' in o.__dict__
        sess.expire(o, ['description', 'isopen'])
        assert 'isopen' not in o.__dict__
        assert 'description' not in o.__dict__

        # test that expired attribute access refreshes
        # the deferred
        def go():
            assert o.isopen == 1
            assert o.description == 'order 3'
        self.assert_sql_count(testing.db, go, 1)
        sess.expire(o, ['description', 'isopen'])

        assert 'isopen' not in o.__dict__
        assert 'description' not in o.__dict__
        # test that the deferred attribute triggers the full
        # reload
        def go():
            assert o.description == 'order 3'
            assert o.isopen == 1
        self.assert_sql_count(testing.db, go, 1)
    
    def test_eagerload_query_refreshes(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        u = sess.query(User).get(8)
        assert len(u.addresses) == 3
        sess.expire(u)
        assert 'addresses' not in u.__dict__
        print "-------------------------------------------"
        sess.query(User).filter_by(id=8).all()
        assert 'addresses' in u.__dict__
        assert len(u.addresses) == 3
        
    def test_expire_all(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=False),
            })
        mapper(Address, addresses)

        sess = create_session()
        userlist = sess.query(User).all()
        assert fixtures.user_address_result == userlist
        assert len(list(sess)) == 9
        sess.expire_all()
        gc.collect()
        assert len(list(sess)) == 4 # since addresses were gc'ed
        
        userlist = sess.query(User).all()
        u = userlist[1]
        assert fixtures.user_address_result == userlist
        assert len(list(sess)) == 9
        
class PolymorphicExpireTest(ORMTest):
    keep_data = True
    
    def define_tables(self, metadata):
        global people, engineers, Person, Engineer

        people = Table('people', metadata,
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
          )
        
        class Person(Base):
            pass
        class Engineer(Person):
            pass
            
    def insert_data(self):
        people.insert().execute(
            {'person_id':1, 'name':'person1', 'type':'person'},
            {'person_id':2, 'name':'engineer1', 'type':'engineer'},
            {'person_id':3, 'name':'engineer2', 'type':'engineer'},
        )
        engineers.insert().execute(
            {'person_id':2, 'status':'new engineer'},
            {'person_id':3, 'status':'old engineer'},
        )

    def test_poly_select(self):
        mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer')
        
        sess = create_session()
        [p1, e1, e2] = sess.query(Person).order_by(people.c.person_id).all()
        
        sess.expire(p1)
        sess.expire(e1, ['status'])
        sess.expire(e2)
        
        for p in [p1, e2]:
            assert 'name' not in p.__dict__
        
        assert 'name' in e1.__dict__
        assert 'status' not in e2.__dict__
        assert 'status' not in e1.__dict__
        
        e1.name = 'new engineer name'
        
        def go():
            sess.query(Person).all()
        self.assert_sql_count(testing.db, go, 3)
        
        for p in [p1, e1, e2]:
            assert 'name' in p.__dict__
        
        assert 'status' in e2.__dict__
        assert 'status' in e1.__dict__
        def go():
            assert e1.name == 'new engineer name'
            assert e2.name == 'engineer2'
            assert e1.status == 'new engineer'
        self.assert_sql_count(testing.db, go, 0)
        self.assertEquals(Engineer.name.get_history(e1), (['new engineer name'], [], ['engineer1']))
        
    def test_poly_deferred(self):
        mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person', polymorphic_fetch='deferred')
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer')

        sess = create_session()
        [p1, e1, e2] = sess.query(Person).order_by(people.c.person_id).all()

        sess.expire(p1)
        sess.expire(e1, ['status'])
        sess.expire(e2)

        for p in [p1, e2]:
            assert 'name' not in p.__dict__

        assert 'name' in e1.__dict__
        assert 'status' not in e2.__dict__
        assert 'status' not in e1.__dict__

        e1.name = 'new engineer name'

        def go():
            sess.query(Person).all()
        self.assert_sql_count(testing.db, go, 1)
        
        for p in [p1, e1, e2]:
            assert 'name' in p.__dict__

        assert 'status' not in e2.__dict__
        assert 'status' not in e1.__dict__

        def go():
            assert e1.name == 'new engineer name'
            assert e2.name == 'engineer2'
            assert e1.status == 'new engineer'
            assert e2.status == 'old engineer'
        self.assert_sql_count(testing.db, go, 2)
        self.assertEquals(Engineer.name.get_history(e1), (['new engineer name'], [], ['engineer1']))
        
    
class RefreshTest(FixtureTest):
    keep_mappers = False
    refresh_data = True

    def test_refresh(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), backref='user')
        })
        s = create_session()
        u = s.get(User, 7)
        u.name = 'foo'
        a = Address()
        assert object_session(a) is None
        u.addresses.append(a)
        assert a.email_address is None
        assert id(a) in [id(x) for x in u.addresses]

        s.refresh(u)

        # its refreshed, so not dirty
        assert u not in s.dirty

        # username is back to the DB
        assert u.name == 'jack'

        assert id(a) not in [id(x) for x in u.addresses]

        u.name = 'foo'
        u.addresses.append(a)
        # now its dirty
        assert u in s.dirty
        assert u.name == 'foo'
        assert id(a) in [id(x) for x in u.addresses]
        s.expire(u)

        # get the attribute, it refreshes
        print "OK------"
#        print u.__dict__
#        print u._state.callables
        assert u.name == 'jack'
        assert id(a) not in [id(x) for x in u.addresses]

    def test_refresh_expired(self):
        mapper(User, users)
        s = create_session()
        u = s.get(User, 7)
        s.expire(u)
        assert 'name' not in u.__dict__
        s.refresh(u)
        assert u.name == 'jack'

    def test_refresh_with_lazy(self):
        """test that when a lazy loader is set as a trigger on an object's attribute
        (at the attribute level, not the class level), a refresh() operation doesnt
        fire the lazy loader or create any problems"""

        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        q = s.query(User).options(lazyload('addresses'))
        u = q.filter(users.c.id==8).first()
        def go():
            s.refresh(u)
        self.assert_sql_count(testing.db, go, 1)


    def test_refresh_with_eager(self):
        """test that a refresh/expire operation loads rows properly and sends correct "isnew" state to eager loaders"""

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })

        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.refresh(u)
        assert len(u.addresses) == 3

        s = create_session()
        u = s.get(User, 8)
        assert len(u.addresses) == 3
        s.expire(u)
        assert len(u.addresses) == 3

    @testing.fails_on('maxdb')
    def test_refresh2(self):
        """test a hang condition that was occuring on expire/refresh"""

        s = create_session()
        mapper(Address, addresses)

        mapper(User, users, properties = dict(addresses=relation(Address,cascade="all, delete-orphan",lazy=False)) )

        u=User()
        u.name='Justin'
        a = Address(id=10, email_address='lala')
        u.addresses.append(a)

        s.save(u)
        s.flush()
        s.clear()
        u = s.query(User).filter(User.name=='Justin').one()

        s.expire(u)
        assert u.name == 'Justin'

        s.refresh(u)

if __name__ == '__main__':
    testenv.main()

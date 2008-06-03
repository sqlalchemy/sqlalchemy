import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from sqlalchemy.orm import mapperlib
from sqlalchemy.util import OrderedSet
from testlib import *
from testlib import fixtures
from testlib.tables import *
import testlib.tables as tables

class MergeTest(TestBase, AssertsExecutionResults):
    """tests session.merge() functionality"""
    def setUpAll(self):
        tables.create()

    def tearDownAll(self):
        tables.drop()

    def tearDown(self):
        clear_mappers()
        tables.delete()

    def test_transient_to_pending(self):
        class User(fixtures.Base):
            pass
        mapper(User, users)
        sess = create_session()

        u = User(user_id=7, user_name='fred')
        u2 = sess.merge(u)
        assert u2 in sess
        self.assertEquals(u2, User(user_id=7, user_name='fred'))
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(User).first(), User(user_id=7, user_name='fred'))
    
    def test_transient_to_pending_collection(self):
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
        mapper(User, users, properties={'addresses':relation(Address, backref='user', collection_class=OrderedSet)})
        mapper(Address, addresses)

        u = User(user_id=7, user_name='fred', addresses=OrderedSet([
            Address(address_id=1, email_address='fred1'),
            Address(address_id=2, email_address='fred2'),
        ]))
        sess = create_session()
        sess.merge(u)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(User).one(), 
            User(user_id=7, user_name='fred', addresses=OrderedSet([
                Address(address_id=1, email_address='fred1'),
                Address(address_id=2, email_address='fred2'),
            ]))
        )
        
    def test_transient_to_persistent(self):
        class User(fixtures.Base):
            pass
        mapper(User, users)
        sess = create_session()
        u = User(user_id=7, user_name='fred')
        sess.save(u)
        sess.flush()
        sess.clear()
        
        u2 = User(user_id=7, user_name='fred jones')
        u2 = sess.merge(u2)
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(User).first(), User(user_id=7, user_name='fred jones'))
        
    def test_transient_to_persistent_collection(self):
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
        mapper(User, users, properties={'addresses':relation(Address, backref='user', collection_class=OrderedSet)})
        mapper(Address, addresses)
        
        u = User(user_id=7, user_name='fred', addresses=OrderedSet([
            Address(address_id=1, email_address='fred1'),
            Address(address_id=2, email_address='fred2'),
        ]))
        sess = create_session()
        sess.save(u)
        sess.flush()
        sess.clear()
        
        u = User(user_id=7, user_name='fred', addresses=OrderedSet([
            Address(address_id=3, email_address='fred3'),
            Address(address_id=4, email_address='fred4'),
        ]))
        
        u = sess.merge(u)
        self.assertEquals(u, 
            User(user_id=7, user_name='fred', addresses=OrderedSet([
                Address(address_id=3, email_address='fred3'),
                Address(address_id=4, email_address='fred4'),
            ]))
        )
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(User).one(), 
            User(user_id=7, user_name='fred', addresses=OrderedSet([
                Address(address_id=3, email_address='fred3'),
                Address(address_id=4, email_address='fred4'),
            ]))
        )
        
    def test_detached_to_persistent_collection(self):
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
        mapper(User, users, properties={'addresses':relation(Address, backref='user', collection_class=OrderedSet)})
        mapper(Address, addresses)
        
        a = Address(address_id=1, email_address='fred1')
        u = User(user_id=7, user_name='fred', addresses=OrderedSet([
            a,
            Address(address_id=2, email_address='fred2'),
        ]))
        sess = create_session()
        sess.save(u)
        sess.flush()
        sess.clear()
        
        u.user_name='fred jones'
        u.addresses.add(Address(address_id=3, email_address='fred3'))
        u.addresses.remove(a)
        
        u = sess.merge(u)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(User).first(), 
            User(user_id=7, user_name='fred jones', addresses=OrderedSet([
                Address(address_id=2, email_address='fred2'),
                Address(address_id=3, email_address='fred3'),
            ]))
        )
        
    def test_unsaved_cascade(self):
        """test merge of a transient entity with two child transient entities, with a bidirectional relation."""
        
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
            
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), cascade="all", backref="user")
        })
        sess = create_session()
        u = User(user_id=7, user_name='fred')
        a1 = Address(email_address='foo@bar.com')
        a2 = Address(email_address='hoho@bar.com')
        u.addresses.append(a1)
        u.addresses.append(a2)

        u2 = sess.merge(u)
        self.assertEquals(u, User(user_id=7, user_name='fred', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@bar.com')]))
        self.assertEquals(u2, User(user_id=7, user_name='fred', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@bar.com')]))
        sess.flush()
        sess.clear()
        u2 = sess.query(User).get(7)
        self.assertEquals(u2, User(user_id=7, user_name='fred', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@bar.com')]))

    def test_attribute_cascade(self):
        """test merge of a persistent entity with two child persistent entities."""

        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), backref='user')
        })
        sess = create_session()

        # set up data and save
        u = User(user_id=7, user_name='fred', addresses=[
            Address(email_address='foo@bar.com'),
            Address(email_address = 'hoho@la.com')
        ])
        sess.save(u)
        sess.flush()

        # assert data was saved
        sess2 = create_session()
        u2 = sess2.query(User).get(7)
        self.assertEquals(u2, User(user_id=7, user_name='fred', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@la.com')]))

        # make local changes to data
        u.user_name = 'fred2'
        u.addresses[1].email_address = 'hoho@lalala.com'

        # new session, merge modified data into session
        sess3 = create_session()
        u3 = sess3.merge(u)

        # ensure local changes are pending
        self.assertEquals(u3, User(user_id=7, user_name='fred2', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@lalala.com')]))
        
        # save merged data
        sess3.flush()

        # assert modified/merged data was saved
        sess.clear()
        u = sess.query(User).get(7)
        self.assertEquals(u, User(user_id=7, user_name='fred2', addresses=[Address(email_address='foo@bar.com'), Address(email_address='hoho@lalala.com')]))

        # merge persistent object into another session
        sess4 = create_session()
        u = sess4.merge(u)
        assert len(u.addresses)
        for a in u.addresses:
            assert a.user is u
        def go():
            sess4.flush()
        # no changes; therefore flush should do nothing
        self.assert_sql_count(testing.db, go, 0)

        # test with "dontload" merge
        sess5 = create_session()
        u = sess5.merge(u, dont_load=True)
        assert len(u.addresses)
        for a in u.addresses:
            assert a.user is u
        def go():
            sess5.flush()
        # no changes; therefore flush should do nothing
        # but also, dont_load wipes out any difference in committed state,
        # so no flush at all
        self.assert_sql_count(testing.db, go, 0)

        sess4 = create_session()
        u = sess4.merge(u, dont_load=True)
        # post merge change
        u.addresses[1].email_address='afafds'
        def go():
            sess4.flush()
        # afafds change flushes
        self.assert_sql_count(testing.db, go, 1)

        sess5 = create_session()
        u2 = sess5.query(User).get(u.user_id)
        assert u2.user_name == 'fred2'
        assert u2.addresses[1].email_address == 'afafds'

    def test_one_to_many_cascade(self):

        mapper(Order, orders, properties={
            'items':relation(mapper(Item, orderitems))
        })

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses)),
            'orders':relation(Order, backref='customer')
        })

        sess = create_session()
        u = User()
        u.user_name='fred'
        o = Order()
        i1 = Item()
        i1.item_name='item 1'
        i2 = Item()
        i2.item_name = 'item 2'
        o.description = 'order description'
        o.items.append(i1)
        o.items.append(i2)
        u.orders.append(o)

        sess.save(u)
        sess.flush()

        sess2 = create_session()
        u2 = sess2.query(User).get(u.user_id)
        u.orders[0].items[1].item_name = 'item 2 modified'
        sess2.merge(u)
        assert u2.orders[0].items[1].item_name == 'item 2 modified'

        sess2 = create_session()
        o2 = sess2.query(Order).get(o.order_id)
        o.customer.user_name = 'also fred'
        sess2.merge(o)
        assert o2.customer.user_name == 'also fred'

    def test_one_to_one_cascade(self):

        mapper(User, users, properties={
            'address':relation(mapper(Address, addresses),uselist = False)
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        u.address = a1

        sess.save(u)
        sess.flush()

        sess2 = create_session()
        u2 = sess2.query(User).get(7)
        u2.user_name = 'fred2'
        u2.address.email_address = 'hoho@lalala.com'

        u3 = sess.merge(u2)
    
    def test_transient_dontload(self):
        mapper(User, users)

        sess = create_session()
        u = User()
        self.assertRaisesMessage(exceptions.InvalidRequestError, "dont_load=True option does not support", sess.merge, u, dont_load=True)


    def test_dontload_with_backrefs(self):
        """test that dontload populates relations in both directions without requiring a load"""
        
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), backref='user')
        })
        
        u = User(user_id=7, user_name='fred', addresses=[Address(email_address='ad1'), Address(email_address='ad2')])
        sess = create_session()
        sess.save(u)
        sess.flush()
        sess.close()
        assert 'user' in u.addresses[1].__dict__
        
        sess = create_session()
        u2 = sess.merge(u, dont_load=True)
        assert 'user' in u2.addresses[1].__dict__
        self.assertEquals(u2.addresses[1].user, User(user_id=7, user_name='fred'))
        
        sess.expire(u2.addresses[1], ['user'])
        assert 'user' not in u2.addresses[1].__dict__
        sess.close()

        sess = create_session()
        u = sess.merge(u2, dont_load=True)
        assert 'user' not in u.addresses[1].__dict__
        self.assertEquals(u.addresses[1].user, User(user_id=7, user_name='fred'))
        
        
    def test_dontload_with_eager(self):
        """this test illustrates that with dont_load=True, we can't just
        copy the committed_state of the merged instance over; since it references collection objects
        which themselves are to be merged.  This committed_state would instead need to be piecemeal
        'converted' to represent the correct objects.
        However, at the moment I'd rather not support this use case; if you are merging with dont_load=True,
        you're typically dealing with caching and the merged objects shouldnt be "dirty".
        """

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses))
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        u.addresses.append(a1)

        sess.save(u)
        sess.flush()

        sess2 = create_session()
        u2 = sess2.query(User).options(eagerload('addresses')).get(7)

        sess3 = create_session()
        u3 = sess3.merge(u2, dont_load=True)
        def go():
            sess3.flush()
        self.assert_sql_count(testing.db, go, 0)

    def test_dont_load_disallows_dirty(self):
        """dont_load doesnt support 'dirty' objects right now (see test_dont_load_with_eager()).
        Therefore lets assert it."""

        mapper(User, users)
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        sess.save(u)
        sess.flush()

        u.user_name = 'ed'
        sess2 = create_session()
        try:
            sess2.merge(u, dont_load=True)
            assert False
        except exceptions.InvalidRequestError, e:
            assert "merge() with dont_load=True option does not support objects marked as 'dirty'.  flush() all changes on mapped instances before merging with dont_load=True." in str(e)

        u2 = sess2.query(User).get(7)

        sess3 = create_session()
        u3 = sess3.merge(u2, dont_load=True)
        assert not sess3.dirty
        def go():
            sess3.flush()
        self.assert_sql_count(testing.db, go, 0)

    def test_dont_load_sets_entityname(self):
        """test that a dont_load-merged entity has entity_name set, has_mapper() passes, and lazyloads work"""
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses),uselist = True)
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        u.addresses.append(a1)

        sess.save(u)
        sess.flush()
        sess.clear()

        # reload 'u' such that its addresses list hasn't loaded
        u = sess.query(User).get(7)

        sess2 = create_session()
        u2 = sess2.merge(u, dont_load=True)
        assert not sess2.dirty
        # assert merged instance has a mapper and lazy load proceeds
        assert hasattr(u2, '_entity_name')
        assert mapperlib.has_mapper(u2)
        def go():
            assert u2.addresses != []
            assert len(u2.addresses) == 1
        self.assert_sql_count(testing.db, go, 1)

    def test_dont_load_sets_backrefs(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses),backref='user')
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        u.addresses.append(a1)

        sess.save(u)
        sess.flush()

        assert u.addresses[0].user is u

        sess2 = create_session()
        u2 = sess2.merge(u, dont_load=True)
        assert not sess2.dirty
        def go():
            assert u2.addresses[0].user is u2
        self.assert_sql_count(testing.db, go, 0)

    def test_dont_load_preserves_parents(self):
        """test that merge with dont_load does not trigger a 'delete-orphan' operation.

        merge with dont_load sets attributes without using events.  this means the
        'hasparent' flag is not propagated to the newly merged instance.  in fact this
        works out OK, because the '_state.parents' collection on the newly
        merged instance is empty; since the mapper doesn't see an active 'False' setting
        in this collection when _is_orphan() is called, it does not count as an orphan
        (i.e. this is the 'optimistic' logic in mapper._is_orphan().)
        """

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses),backref='user', cascade="all, delete-orphan")
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        u.addresses.append(a1)
        sess.save(u)
        sess.flush()

        assert u.addresses[0].user is u

        sess2 = create_session()
        u2 = sess2.merge(u, dont_load=True)
        assert not sess2.dirty
        a2 = u2.addresses[0]
        a2.email_address='somenewaddress'
        assert not object_mapper(a2)._is_orphan(a2)
        sess2.flush()
        sess2.clear()
        assert sess2.query(User).get(u2.user_id).addresses[0].email_address == 'somenewaddress'

        # this use case is not supported; this is with a pending Address on the pre-merged
        # object, and we currently dont support 'dirty' objects being merged with dont_load=True.
        # in this case, the empty '_state.parents' collection would be an issue,
        # since the optimistic flag is False in _is_orphan() for pending instances.
        # so if we start supporting 'dirty' with dont_load=True, this test will need to pass
        sess = create_session()
        u = sess.query(User).get(7)
        u.addresses.append(Address())
        sess2 = create_session()
        try:
            u2 = sess2.merge(u, dont_load=True)
            assert False

            # if dont_load is changed to support dirty objects, this code needs to pass
            a2 = u2.addresses[0]
            a2.email_address='somenewaddress'
            assert not object_mapper(a2)._is_orphan(a2)
            sess2.flush()
            sess2.clear()
            assert sess2.query(User).get(u2.user_id).addresses[0].email_address == 'somenewaddress'
        except exceptions.InvalidRequestError, e:
            assert "dont_load=True option does not support" in str(e)

    def test_synonym_comparable(self):
        class User(object):

           class Comparator(PropComparator):
               pass

           def _getValue(self):
               return self._value

           def _setValue(self, value):
               setattr(self, '_value', value)

           value = property(_getValue, _setValue)

        mapper(User, users, properties={
            'uid':synonym('id'),
            'foobar':comparable_property(User.Comparator,User.value),
        })
        
        sess = create_session()
        u = User()
        u.name = 'ed'
        sess.save(u)
        sess.flush()
        sess.expunge(u)
        sess.merge(u)

if __name__ == "__main__":
    testenv.main()

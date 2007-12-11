import testbase
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from sqlalchemy.orm import mapperlib
from testlib import *
from testlib.tables import *
import testlib.tables as tables

class MergeTest(AssertMixin):
    """tests session.merge() functionality"""
    def setUpAll(self):
        tables.create()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
        tables.delete()
    def setUp(self):
        pass
        
    def test_unsaved(self):
        """test merge of a single transient entity."""
        mapper(User, users)
        sess = create_session()
        
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        u2 = sess.merge(u)
        assert u2 in sess
        assert u2.user_id == 7
        assert u2.user_name == 'fred'
        sess.flush()
        sess.clear()
        u2 = sess.query(User).get(7)
        assert u2.user_name == 'fred'

    def test_unsaved_cascade(self):
        """test merge of a transient entity with two child transient entities."""
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), cascade="all")
        })
        sess = create_session()
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        a2 = Address()
        a2.email_address = 'hoho@la.com'
        u.addresses.append(a1)
        u.addresses.append(a2)
        
        u2 = sess.merge(u)
        self.assert_result([u], User, {'user_id':7, 'user_name':'fred', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@la.com'}])})
        self.assert_result([u2], User, {'user_id':7, 'user_name':'fred', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@la.com'}])})
        sess.flush()
        sess.clear()
        u2 = sess.query(User).get(7)
        self.assert_result([u2], User, {'user_id':7, 'user_name':'fred', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@la.com'}])})

    def test_saved_cascade(self):
        """test merge of a persistent entity with two child persistent entities."""
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), backref='user')
        })
        sess = create_session()
        
        # set up data and save
        u = User()
        u.user_id = 7
        u.user_name = "fred"
        a1 = Address()
        a1.email_address='foo@bar.com'
        a2 = Address()
        a2.email_address = 'hoho@la.com'
        u.addresses.append(a1)
        u.addresses.append(a2)
        sess.save(u)
        sess.flush()

        # assert data was saved
        sess2 = create_session()
        u2 = sess2.query(User).get(7)
        self.assert_result([u2], User, {'user_id':7, 'user_name':'fred', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@la.com'}])})
        
        # make local changes to data
        u.user_name = 'fred2'
        u.addresses[1].email_address = 'hoho@lalala.com'
        
        # new session, merge modified data into session
        sess3 = create_session()
        u3 = sess3.merge(u)
        # insure local changes are pending
        self.assert_result([u3], User, {'user_id':7, 'user_name':'fred2', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@lalala.com'}])})
        
        # save merged data
        sess3.flush()
        
        # assert modified/merged data was saved
        sess.clear()
        u = sess.query(User).get(7)
        self.assert_result([u], User, {'user_id':7, 'user_name':'fred2', 'addresses':(Address, [{'email_address':'foo@bar.com'}, {'email_address':'hoho@lalala.com'}])})

        # merge persistent object into another session
        sess4 = create_session()
        u = sess4.merge(u)
        assert len(u.addresses)
        for a in u.addresses:
            assert a.user is u
        def go():
            sess4.flush()
        # no changes; therefore flush should do nothing
        self.assert_sql_count(testbase.db, go, 0)
        
        # test with "dontload" merge
        sess5 = create_session()
        print "------------------"
        u = sess5.merge(u, dont_load=True)
        assert len(u.addresses)
        for a in u.addresses:
            assert a.user is u
        def go():
            sess5.flush()
        # no changes; therefore flush should do nothing
        # but also, dont_load wipes out any difference in committed state, 
        # so no flush at all
        self.assert_sql_count(testbase.db, go, 0)

        sess4 = create_session()
        u = sess4.merge(u, dont_load=True)
        # post merge change
        u.addresses[1].email_address='afafds'
        def go():
            sess4.flush()
        # afafds change flushes
        self.assert_sql_count(testbase.db, go, 1)
        
        sess5 = create_session()
        u2 = sess5.query(User).get(u.user_id)
        assert u2.user_name == 'fred2'
        assert u2.addresses[1].email_address == 'afafds'

    def test_saved_cascade_2(self):
        """tests a more involved merge"""
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
        
    def test_saved_cascade_3(self):
        """test merge of a persistent entity with one_to_one relationship"""

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

    def test_noload_with_eager(self):
        """this test illustrates that with noload=True, we can't just
        copy the committed_state of the merged instance over; since it references collection objects
        which themselves are to be merged.  This committed_state would instead need to be piecemeal 
        'converted' to represent the correct objects.  
        However, at the moment I'd rather not support this use case; if you are merging with dont_load=True,
        you're typically dealing with caching and the merged objects shouldnt be "dirty".
        """
        
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

        sess2 = create_session()
        u2 = sess2.query(User).options(eagerload('addresses')).get(7)
        
        sess3 = create_session()
        u3 = sess3.merge(u2, dont_load=True)
        def go():
            sess3.flush()
        self.assert_sql_count(testbase.db, go, 0)

    def test_noload_disallows_dirty(self):
        """noload doesnt support 'dirty' objects right now (see test_noload_with_eager()). 
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
        self.assert_sql_count(testbase.db, go, 0)
        
    def test_noload_sets_entityname(self):
        """test that a noload-merged entity has entity_name set, has_mapper() passes, and lazyloads work"""
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
        self.assert_sql_count(testbase.db, go, 1)

    def test_noload_sets_backrefs(self):
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
        self.assert_sql_count(testbase.db, go, 0)
    
    def test_noload_preserves_parents(self):
        """test that merge with noload does not trigger a 'delete-orphan' operation.
        
        merge with noload sets attributes without using events.  this means the
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
         
        
if __name__ == "__main__":    
    testbase.main()

from testbase import PersistTest, AssertMixin
import testbase
from sqlalchemy import *
from tables import *
import tables

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
        
if __name__ == "__main__":    
    testbase.main()

                
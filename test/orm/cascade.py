import testenv; testenv.configure_for_tests()

from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib import fixtures

class O2MCascadeTest(fixtures.FixtureTest):
    keep_mappers = True
    keep_data = False
    refresh_data = False

    def setup_mappers(self):
        global User, Address, Order, users, orders, addresses
        from testlib.fixtures import User, Address, Order, users, orders, addresses
                
        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, cascade="all, delete-orphan"),
            orders = relation(
                mapper(Order, orders), cascade="all, delete-orphan")
        ))
    
    def test_list_assignment(self):
        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()
        sess.clear()
        
        u = sess.query(User).get(u.id)
        self.assertEquals(u, User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')]))
        
        u.orders=[Order(description="order 3"), Order(description="order 4")]
        sess.flush()
        sess.clear()
        
        u = sess.query(User).get(u.id)
        self.assertEquals(u, User(name='jack', orders=[Order(description="order 3"), Order(description="order 4")]))

        self.assertEquals(sess.query(Order).all(), [Order(description="order 3"), Order(description="order 4")])
        o5 = Order(description="order 5")
        sess.save(o5)
        try:
            sess.flush()
            assert False
        except exceptions.FlushError, e:
            assert "is an orphan" in str(e)

    def test_delete(self):
        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()

        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 0

    def test_delete_unloaded_collections(self):
        """test that unloaded collections are still included in a delete-cascade by default."""

        sess = create_session()
        u = User(name='jack', addresses=[Address(email_address="address1"), Address(email_address="address2")])
        sess.save(u)
        sess.flush()
        sess.clear()
        assert addresses.count().scalar() == 2
        assert users.count().scalar() == 1
        
        u = sess.query(User).get(u.id)
        
        assert 'addresses' not in u.__dict__
        sess.delete(u)
        sess.flush()
        assert addresses.count().scalar() == 0
        assert users.count().scalar() == 0

    def test_cascades_onlycollection(self):
        """test that cascade only reaches instances that are still part of the collection,
        not those that have been removed"""

        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()
        
        o = u.orders[0]
        del u.orders[0]
        sess.delete(u)
        assert u in sess.deleted
        assert o not in sess.deleted
        assert o in sess

        u2 = User(name='newuser', orders=[o])
        sess.save(u2)
        sess.flush()
        sess.clear()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 1
        self.assertEquals(sess.query(User).all(), [User(name='newuser', orders=[Order(description='someorder')])])

    def test_cascade_delete_plusorphans(self):
        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2

        del u.orders[0]
        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 0
            
    def test_collection_orphans(self):
        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()

        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2

        u.orders[:] = []

        sess.flush()

        assert users.count().scalar() == 1
        assert orders.count().scalar() == 0

class O2MCascadeNoOrphanTest(fixtures.FixtureTest):
    keep_mappers = True
    keep_data = False
    refresh_data = False

    def setup_mappers(self):
        global User, Address, Order, users, orders, addresses
        from testlib.fixtures import User, Address, Order, users, orders, addresses
                
        mapper(User, users, properties = dict(
            orders = relation(
                mapper(Order, orders), cascade="all")
        ))
    
    def test_cascade_delete_noorphans(self):
        sess = create_session()
        u = User(name='jack', orders=[Order(description='someorder'), Order(description='someotherorder')])
        sess.save(u)
        sess.flush()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2
        
        del u.orders[0]
        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 1
        

class M2OCascadeTest(ORMTest):
    keep_mappers = True
    
    def define_tables(self, metadata):
        global extra, prefs, users

        extra = Table("extra", metadata,
            Column("id", Integer, Sequence("extra_id_seq", optional=True), primary_key=True),
            Column("prefs_id", Integer, ForeignKey("prefs.id"))
        )
        prefs = Table('prefs', metadata,
            Column('id', Integer, Sequence('prefs_id_seq', optional=True), primary_key=True),
            Column('data', String(40)))

        users = Table('users', metadata,
            Column('id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('name', String(40)),
            Column('pref_id', Integer, ForeignKey('prefs.id'))
        )
        
    def setup_mappers(self):
        global User, Pref, Extra
        class User(fixtures.Base):
            pass
        class Pref(fixtures.Base):
            pass
        class Extra(fixtures.Base):
            pass

        mapper(Extra, extra)
        mapper(Pref, prefs, properties=dict(
            extra = relation(Extra, cascade="all, delete")
        ))
        mapper(User, users, properties = dict(
            pref = relation(Pref, lazy=False, cascade="all, delete-orphan")
        ))

    def setUp(self):
        u1 = User(name='ed', pref=Pref(data="pref 1", extra=[Extra()]))
        u2 = User(name='jack', pref=Pref(data="pref 2", extra=[Extra()]))
        u3 = User(name="foo", pref=Pref(data="pref 3", extra=[Extra()]))
        sess = create_session()
        sess.save(u1)
        sess.save(u2)
        sess.save(u3)
        sess.flush()
        sess.close()

    @testing.fails_on('maxdb')
    def test_orphan(self):
        sess = create_session()
        assert prefs.count().scalar() == 3
        assert extra.count().scalar() == 3
        jack = sess.query(User).filter_by(name="jack").one()
        jack.pref = None
        sess.flush()
        assert prefs.count().scalar() == 2
        assert extra.count().scalar() == 2

    @testing.fails_on('maxdb')
    def test_orphan_on_update(self):
        sess = create_session()
        jack = sess.query(User).filter_by(name="jack").one()
        p = jack.pref
        e = jack.pref.extra[0]
        sess.clear()

        jack.pref = None
        sess.update(jack)
        sess.update(p)
        sess.update(e)
        assert p in sess
        assert e in sess
        sess.flush()
        assert prefs.count().scalar() == 2
        assert extra.count().scalar() == 2
    
    def test_pending_expunge(self):
        sess = create_session()
        someuser = User(name='someuser')
        sess.save(someuser)
        sess.flush()
        someuser.pref = p1 = Pref(data='somepref')
        assert p1 in sess
        someuser.pref = Pref(data='someotherpref')
        assert p1 not in sess
        sess.flush()
        self.assertEquals(sess.query(Pref).with_parent(someuser).all(), [Pref(data="someotherpref")])

        
    def test_double_assignment(self):
        """test that double assignment doesn't accidentally reset the 'parent' flag."""

        sess = create_session()
        jack = sess.query(User).filter_by(name="jack").one()

        newpref = Pref(data="newpref")
        jack.pref = newpref
        jack.pref = newpref
        sess.flush()
        self.assertEquals(sess.query(Pref).all(), [Pref(data="pref 1"), Pref(data="pref 3"), Pref(data="newpref")])

class M2OCascadeDeleteTest(ORMTest):
    keep_mappers = True
    
    def define_tables(self, metadata):
        global t1, t2, t3
        t1 = Table('t1', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)), Column('t2id', Integer, ForeignKey('t2.id')))
        t2 = Table('t2', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)), Column('t3id', Integer, ForeignKey('t3.id')))
        t3 = Table('t3', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)))
    
    def setup_mappers(self):
        global T1, T2, T3
        class T1(fixtures.Base):pass
        class T2(fixtures.Base):pass
        class T3(fixtures.Base):pass
        
        mapper(T1, t1, properties={'t2':relation(T2, cascade="all")})
        mapper(T2, t2, properties={'t3':relation(T3, cascade="all")})
        mapper(T3, t3)

    def test_cascade_delete(self):
        sess = create_session()

        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.save(x)
        sess.flush()
        
        sess.delete(x)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])

    def test_cascade_delete_postappend_onelevel(self):
        sess = create_session()

        x1 = T1(data='t1', )
        x2 = T2(data='t2')
        x3 = T3(data='t3')
        sess.save(x1)
        sess.save(x2)
        sess.save(x3)
        sess.flush()
        
        sess.delete(x1)
        x1.t2 = x2
        x2.t3 = x3
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])

    def test_cascade_delete_postappend_twolevel(self):
        sess = create_session()

        x1 = T1(data='t1', t2=T2(data='t2'))
        x3 = T3(data='t3')
        sess.save(x1)
        sess.save(x3)
        sess.flush()

        sess.delete(x1)
        x1.t2.t3 = x3
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])
        
    def test_preserves_orphans_onelevel(self):
        sess = create_session()

        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.save(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [T2()])
        self.assertEquals(sess.query(T3).all(), [T3()])

    @testing.future
    def test_preserves_orphans_onelevel_postremove(self):
        sess = create_session()

        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.save(x2)
        sess.flush()

        sess.delete(x2)
        x2.t2 = None
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [T2()])
        self.assertEquals(sess.query(T3).all(), [T3()])

    def test_preserves_orphans_twolevel(self):
        sess = create_session()

        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.save(x)
        sess.flush()
        
        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [T3()])

class M2OCascadeDeleteOrphanTest(ORMTest):
    keep_mappers = True

    def define_tables(self, metadata):
        global t1, t2, t3
        t1 = Table('t1', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)), Column('t2id', Integer, ForeignKey('t2.id')))
        t2 = Table('t2', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)), Column('t3id', Integer, ForeignKey('t3.id')))
        t3 = Table('t3', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)))

    def setup_mappers(self):
        global T1, T2, T3
        class T1(fixtures.Base):pass
        class T2(fixtures.Base):pass
        class T3(fixtures.Base):pass

        mapper(T1, t1, properties={'t2':relation(T2, cascade="all, delete-orphan")})
        mapper(T2, t2, properties={'t3':relation(T3, cascade="all, delete-orphan")})
        mapper(T3, t3)

    def test_cascade_delete(self):
        sess = create_session()

        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.save(x)
        sess.flush()

        sess.delete(x)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])

    def test_deletes_orphans_onelevel(self):
        sess = create_session()

        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.save(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])

    def test_deletes_orphans_twolevel(self):
        sess = create_session()

        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.save(x)
        sess.flush()

        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [])
        self.assertEquals(sess.query(T2).all(), [])
        self.assertEquals(sess.query(T3).all(), [])

    def test_finds_orphans_twolevel(self):
        sess = create_session()

        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.save(x)
        sess.flush()

        x.t2.t3 = None
        sess.flush()
        self.assertEquals(sess.query(T1).all(), [T1()])
        self.assertEquals(sess.query(T2).all(), [T2()])
        self.assertEquals(sess.query(T3).all(), [])

class M2MCascadeTest(ORMTest):
    def define_tables(self, metadata):
        global a, b, atob, c
        a = Table('a', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30))
            )
        b = Table('b', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30))
            )
        atob = Table('atob', metadata,
            Column('aid', Integer, ForeignKey('a.id')),
            Column('bid', Integer, ForeignKey('b.id'))
            )
        c = Table('c', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('bid', Integer, ForeignKey('b.id'))
            )
            
    def test_delete_orphan(self):
        class A(fixtures.Base):
            pass
        class B(fixtures.Base):
            pass

        mapper(A, a, properties={
            # if no backref here, delete-orphan failed until [ticket:427] was fixed
            'bs':relation(B, secondary=atob, cascade="all, delete-orphan")
        })
        mapper(B, b)

        sess = create_session()
        b1 = B(data='b1')
        a1 = A(data='a1', bs=[b1])
        sess.save(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1

    def test_delete_orphan_cascades(self):
        class A(fixtures.Base):
            pass
        class B(fixtures.Base):
            pass
        class C(fixtures.Base):
            pass

        mapper(A, a, properties={
            # if no backref here, delete-orphan failed until [ticket:427] was fixed
            'bs':relation(B, secondary=atob, cascade="all, delete-orphan")
        })
        mapper(B, b, properties={'cs':relation(C, cascade="all, delete-orphan")})
        mapper(C, c)

        sess = create_session()
        b1 = B(data='b1', cs=[C(data='c1')])
        a1 = A(data='a1', bs=[b1])
        sess.save(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1
        assert c.count().scalar() == 0
        
    def test_cascade_delete(self):
        class A(fixtures.Base):
            pass
        class B(fixtures.Base):
            pass

        mapper(A, a, properties={
            'bs':relation(B, secondary=atob, cascade="all, delete-orphan")
        })
        mapper(B, b)

        sess = create_session()
        a1 = A(data='a1', bs=[B(data='b1')])
        sess.save(a1)
        sess.flush()

        sess.delete(a1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 0

class UnsavedOrphansTest(ORMTest):
    """tests regarding pending entities that are orphans"""

    def define_tables(self, metadata):
        global users, addresses, User, Address
        users = Table('users', metadata,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('name', String(40)),
        )

        addresses = Table('email_addresses', metadata,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(40)),
        )
        class User(fixtures.Base):pass
        class Address(fixtures.Base):pass

    def test_pending_standalone_orphan(self):
        """test that an entity that never had a parent on a delete-orphan cascade cant be saved."""

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()
        a = Address()
        s.save(a)
        try:
            s.flush()
        except exceptions.FlushError, e:
            pass
        assert a.address_id is None, "Error: address should not be persistent"

    def test_pending_collection_expunge(self):
        """test that removing a pending item from a collection expunges it from the session."""

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()

        u = User()
        s.save(u)
        s.flush()
        a = Address()

        u.addresses.append(a)
        assert a in s
        
        u.addresses.remove(a)
        assert a not in s

        s.delete(u)
        s.flush() 

        assert a.address_id is None, "Error: address should not be persistent"
    
    def test_nonorphans_ok(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete", backref="user")
        ))
        s = create_session()
        u = User(name='u1', addresses=[Address(email_address='ad1')])
        s.save(u)
        a1 = u.addresses[0]
        u.addresses.remove(a1)
        assert a1 in s
        s.flush()
        s.clear()
        self.assertEquals(s.query(Address).all(), [Address(email_address='ad1')])
        
class UnsavedOrphansTest2(ORMTest):
    """same test as UnsavedOrphans only three levels deep"""

    def define_tables(self, meta):
        global orders, items, attributes
        orders = Table('orders', meta,
            Column('id', Integer, Sequence('order_id_seq'), primary_key = True),
            Column('name', VARCHAR(50)),

        )
        items = Table('items', meta,
            Column('id', Integer, Sequence('item_id_seq'), primary_key = True),
            Column('order_id', Integer, ForeignKey(orders.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )
        attributes = Table('attributes', meta,
            Column('id', Integer, Sequence('attribute_id_seq'), primary_key = True),
            Column('item_id', Integer, ForeignKey(items.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )

    def test_pending_expunge(self):
        class Order(fixtures.Base):
            pass
        class Item(fixtures.Base):
            pass
        class Attribute(fixtures.Base):
            pass

        mapper(Attribute, attributes)
        mapper(Item, items, properties=dict(
            attributes=relation(Attribute, cascade="all,delete-orphan", backref="item")
        ))
        mapper(Order, orders, properties=dict(
            items=relation(Item, cascade="all,delete-orphan", backref="order")
        ))

        s = create_session()
        order = Order(name="order1")
        s.save(order)

        attr = Attribute(name="attr1")
        item = Item(name="item1", attributes=[attr])

        order.items.append(item)
        order.items.remove(item) 
        
        assert item not in s
        assert attr not in s
        
        s.flush()
        assert orders.count().scalar() == 1
        assert items.count().scalar() == 0
        assert attributes.count().scalar() == 0

class UnsavedOrphansTest3(ORMTest):
    """test not expuning double parents"""

    def define_tables(self, meta):
        global sales_reps, accounts, customers
        sales_reps = Table('sales_reps', meta,
            Column('sales_rep_id', Integer, Sequence('sales_rep_id_seq'), primary_key = True),
            Column('name', String(50)),
        )
        accounts = Table('accounts', meta,
            Column('account_id', Integer, Sequence('account_id_seq'), primary_key = True),
            Column('balance', Integer),
        )
        customers = Table('customers', meta,
            Column('customer_id', Integer, Sequence('customer_id_seq'), primary_key = True),
            Column('name', String(50)),
            Column('sales_rep_id', Integer, ForeignKey('sales_reps.sales_rep_id')),
            Column('account_id', Integer, ForeignKey('accounts.account_id')),
        )

    def test_double_parent_expunge(self):
        """test that removing a pending item from a collection expunges it from the session."""
        class Customer(fixtures.Base):
            pass
        class Account(fixtures.Base):
            pass
        class SalesRep(fixtures.Base):
            pass

        mapper(Customer, customers)
        mapper(Account, accounts, properties=dict(
            customers=relation(Customer, cascade="all,delete-orphan", backref="account")
        ))
        mapper(SalesRep, sales_reps, properties=dict(
            customers=relation(Customer, cascade="all,delete-orphan", backref="sales_rep")
        ))
        s = create_session()

        a = Account(balance=0)
        sr = SalesRep(name="John")
        [s.save(x) for x in [a,sr]]
        s.flush()
        
        c = Customer(name="Jane")

        a.customers.append(c)
        sr.customers.append(c)
        assert c in s
        
        a.customers.remove(c)
        assert c in s, "Should not expunge customer yet, still has one parent"

        sr.customers.remove(c)
        assert c not in s, "Should expunge customer when both parents are gone"

class DoubleParentOrphanTest(ORMTest):
    """test orphan detection for an entity with two parent relations"""

    def define_tables(self, metadata):
        global address_table, businesses, homes
        address_table = Table('addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('street', String(30)),
        )

        homes = Table('homes', metadata,
            Column('home_id', Integer, primary_key=True, key="id"),
            Column('description', String(30)),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )

        businesses = Table('businesses', metadata,
            Column('business_id', Integer, primary_key=True, key="id"),
            Column('description', String(30), key="description"),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )
        
    def test_non_orphan(self):
        """test that an entity can have two parent delete-orphan cascades, and persists normally."""

        class Address(fixtures.Base):
            pass
        class Home(fixtures.Base):
            pass
        class Business(fixtures.Base):
            pass
        
        mapper(Address, address_table)
        mapper(Home, homes, properties={'address':relation(Address, cascade="all,delete-orphan")})
        mapper(Business, businesses, properties={'address':relation(Address, cascade="all,delete-orphan")})

        session = create_session()
        h1 = Home(description='home1', address=Address(street='address1'))
        b1 = Business(description='business1', address=Address(street='address2'))
        [session.save(x) for x in [h1,b1]]
        session.flush()
        session.clear()
        
        self.assertEquals(session.query(Home).get(h1.id), Home(description='home1', address=Address(street='address1')))
        self.assertEquals(session.query(Business).get(b1.id), Business(description='business1', address=Address(street='address2')))

    def test_orphan(self):
        """test that an entity can have two parent delete-orphan cascades, and is detected as an orphan
        when saved without a parent."""

        class Address(fixtures.Base):
            pass
        class Home(fixtures.Base):
            pass
        class Business(fixtures.Base):
            pass
        
        mapper(Address, address_table)
        mapper(Home, homes, properties={'address':relation(Address, cascade="all,delete-orphan")})
        mapper(Business, businesses, properties={'address':relation(Address, cascade="all,delete-orphan")})

        session = create_session()
        a1 = Address()
        session.save(a1)
        try:
            session.flush()
            assert False
        except exceptions.FlushError, e:
            assert True

class CollectionAssignmentOrphanTest(ORMTest):
    def define_tables(self, metadata):
        global table_a, table_b

        table_a = Table('a', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(30)))
        table_b = Table('b', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(30)),
                        Column('a_id', Integer, ForeignKey('a.id')))

    def test_basic(self):
        class A(fixtures.Base):
            pass
        class B(fixtures.Base):
            pass

        mapper(A, table_a, properties={
            'bs':relation(B, cascade="all, delete-orphan")
            })
        mapper(B, table_b)

        a1 = A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')])

        sess = create_session()
        sess.save(a1)
        sess.flush()

        sess.clear()
        
        self.assertEquals(sess.query(A).get(a1.id), A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')]))

        a1 = sess.query(A).get(a1.id)
        assert not class_mapper(B)._is_orphan(a1.bs[0])
        a1.bs[0].foo='b2modified'
        a1.bs[1].foo='b3modified'
        sess.flush()

        sess.clear()
        self.assertEquals(sess.query(A).get(a1.id), A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')]))

if __name__ == "__main__":
    testenv.main()

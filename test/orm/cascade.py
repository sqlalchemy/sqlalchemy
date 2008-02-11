import testenv; testenv.configure_for_tests()

from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext
from testlib import *
import testlib.tables as tables

class O2MCascadeTest(TestBase, AssertsExecutionResults):
    def tearDown(self):
        tables.delete()

    def tearDownAll(self):
        clear_mappers()
        tables.drop()

    def setUpAll(self):
        global data
        tables.create()
        mapper(tables.User, tables.users, properties = dict(
            address = relation(mapper(tables.Address, tables.addresses), lazy=True, uselist = False, cascade="all, delete-orphan"),
            orders = relation(
                mapper(tables.Order, tables.orders, properties = dict (
                    items = relation(mapper(tables.Item, tables.orderitems), lazy=True, uselist =True, cascade="all, delete-orphan")
                )),
                lazy = True, uselist = True, cascade="all, delete-orphan")
        ))

    def setUp(self):
        global data
        data = [tables.User,
            {'user_name' : 'ed',
                'address' : (tables.Address, {'email_address' : 'foo@bar.com'}),
                'orders' : (tables.Order, [
                    {'description' : 'eds 1st order', 'items' : (tables.Item, [{'item_name' : 'eds o1 item'}, {'item_name' : 'eds other o1 item'}])},
                    {'description' : 'eds 2nd order', 'items' : (tables.Item, [{'item_name' : 'eds o2 item'}, {'item_name' : 'eds other o2 item'}])}
                 ])
            },
            {'user_name' : 'jack',
                'address' : (tables.Address, {'email_address' : 'jack@jack.com'}),
                'orders' : (tables.Order, [
                    {'description' : 'jacks 1st order', 'items' : (tables.Item, [{'item_name' : 'im a lumberjack'}, {'item_name' : 'and im ok'}])}
                 ])
            },
            {'user_name' : 'foo',
                'address' : (tables.Address, {'email_address': 'hi@lala.com'}),
                'orders' : (tables.Order, [
                    {'description' : 'foo order', 'items' : (tables.Item, [])},
                    {'description' : 'foo order 2', 'items' : (tables.Item, [{'item_name' : 'hi'}])},
                    {'description' : 'foo order three', 'items' : (tables.Item, [{'item_name' : 'there'}])}
                ])
            }
        ]

        sess = create_session()
        for elem in data[1:]:
            u = tables.User()
            sess.save(u)
            u.user_name = elem['user_name']
            u.address = tables.Address()
            u.address.email_address = elem['address'][1]['email_address']
            u.orders = []
            for order in elem['orders'][1]:
                o = tables.Order()
                o.isopen = None
                o.description = order['description']
                u.orders.append(o)
                o.items = []
                for item in order['items'][1]:
                    i = tables.Item()
                    i.item_name = item['item_name']
                    o.items.append(i)

        sess.flush()
        sess.clear()

    def testassignlist(self):
        sess = create_session()
        u = tables.User()
        u.user_name = 'jack'
        o1 = tables.Order()
        o1.description ='someorder'
        o2 = tables.Order()
        o2.description = 'someotherorder'
        l = [o1, o2]
        sess.save(u)
        u.orders = l
        assert o1 in sess
        assert o2 in sess
        sess.flush()
        sess.clear()

        u = sess.query(tables.User).get(u.user_id)
        o3 = tables.Order()
        o3.description='order3'
        o4 = tables.Order()
        o4.description = 'order4'
        u.orders = [o3, o4]
        assert o3 in sess
        assert o4 in sess
        sess.flush()

        o5 = tables.Order()
        o5.description='order5'
        sess.save(o5)
        try:
            sess.flush()
            assert False
        except exceptions.FlushError, e:
            assert "is an orphan" in str(e)


    def testdelete(self):
        sess = create_session()
        l = sess.query(tables.User).all()
        for u in l:
            print repr(u.orders)
        self.assert_result(l, data[0], *data[1:])

        ids = (l[0].user_id, l[2].user_id)
        sess.delete(l[0])
        sess.delete(l[2])

        sess.flush()
        assert tables.orders.count(tables.orders.c.user_id.in_(ids)).scalar() == 0
        assert tables.orderitems.count(tables.orders.c.user_id.in_(ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 0
        assert tables.addresses.count(tables.addresses.c.user_id.in_(ids)).scalar() == 0
        assert tables.users.count(tables.users.c.user_id.in_(ids)).scalar() == 0

    def testdelete2(self):
        """test that unloaded collections are still included in a delete-cascade by default."""

        sess = create_session()
        u = sess.query(tables.User).filter_by(user_name='ed').one()
        # assert 'addresses' collection not loaded
        assert 'addresses' not in u.__dict__
        sess.delete(u)
        sess.flush()
        assert tables.addresses.count(tables.addresses.c.email_address=='foo@bar.com').scalar() == 0
        assert tables.orderitems.count(tables.orderitems.c.item_name.like('eds%')).scalar() == 0

    def testcascadecollection(self):
        """test that cascade only reaches instances that are still part of the collection,
        not those that have been removed"""
        sess = create_session()

        u = tables.User()
        u.user_name = 'newuser'
        o = tables.Order()
        o.description = "some description"
        u.orders.append(o)
        sess.save(u)
        sess.flush()

        u.orders.remove(o)
        sess.delete(u)
        assert u in sess.deleted
        assert o not in sess.deleted


    def testorphan(self):
        sess = create_session()
        l = sess.query(tables.User).all()
        jack = l[1]
        jack.orders[:] = []

        ids = [jack.user_id]
        self.assert_(tables.orders.count(tables.orders.c.user_id.in_(ids)).scalar() == 1)
        self.assert_(tables.orderitems.count(tables.orders.c.user_id.in_(ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 2)

        sess.flush()

        self.assert_(tables.orders.count(tables.orders.c.user_id.in_(ids)).scalar() == 0)
        self.assert_(tables.orderitems.count(tables.orders.c.user_id.in_(ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 0)


class M2OCascadeTest(TestBase, AssertsExecutionResults):
    def tearDown(self):
        ctx.current.clear()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()

    @testing.uses_deprecated('SessionContext')
    def setUpAll(self):
        global ctx, data, metadata, User, Pref, Extra
        ctx = SessionContext(create_session)
        metadata = MetaData(testing.db)
        extra = Table("extra", metadata,
            Column("extra_id", Integer, Sequence("extra_id_seq", optional=True), primary_key=True),
            Column("prefs_id", Integer, ForeignKey("prefs.prefs_id"))
        )
        prefs = Table('prefs', metadata,
            Column('prefs_id', Integer, Sequence('prefs_id_seq', optional=True), primary_key=True),
            Column('prefs_data', String(40)))

        users = Table('users', metadata,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(40)),
            Column('pref_id', Integer, ForeignKey('prefs.prefs_id'))
        )
        class User(object):
            def __init__(self, name):
                self.user_name = name
        class Pref(object):
            def __init__(self, data):
                self.prefs_data = data
        class Extra(object):
            pass
        metadata.create_all()
        mapper(Extra, extra)
        mapper(Pref, prefs, properties=dict(
            extra = relation(Extra, cascade="all, delete")
        ))
        mapper(User, users, properties = dict(
            pref = relation(Pref, lazy=False, cascade="all, delete-orphan")
        ))

    def setUp(self):
        u1 = User("ed")
        u1.pref = Pref("pref 1")
        u2 = User("jack")
        u2.pref = Pref("pref 2")
        u3 = User("foo")
        u3.pref = Pref("pref 3")
        u1.pref.extra.append(Extra())
        u2.pref.extra.append(Extra())
        u2.pref.extra.append(Extra())

        ctx.current.save(u1)
        ctx.current.save(u2)
        ctx.current.save(u3)
        ctx.current.flush()
        ctx.current.clear()

    @testing.fails_on('maxdb')
    def testorphan(self):
        jack = ctx.current.query(User).filter_by(user_name='jack').one()
        p = jack.pref
        e = jack.pref.extra[0]
        jack.pref = None
        ctx.current.flush()
        assert p not in ctx.current
        assert e not in ctx.current

    @testing.fails_on('maxdb')
    def testorphan2(self):
        jack = ctx.current.query(User).filter_by(user_name='jack').one()
        p = jack.pref
        e = jack.pref.extra[0]
        ctx.current.clear()

        jack.pref = None
        ctx.current.update(jack)
        ctx.current.update(p)
        ctx.current.update(e)
        assert p in ctx.current
        assert e in ctx.current
        ctx.current.flush()
        assert p not in ctx.current
        assert e not in ctx.current

    def testorphan3(self):
        """test that double assignment doesn't accidentally reset the 'parent' flag."""

        jack = ctx.current.query(User).filter_by(user_name='jack').one()
        newpref = Pref("newpref")
        jack.pref = newpref
        jack.pref = newpref
        ctx.current.flush()



class M2MCascadeTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global metadata, a, b, atob
        metadata = MetaData(testing.db)
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
        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    def testdeleteorphan(self):
        class A(object):
            def __init__(self, data):
                self.data = data
        class B(object):
            def __init__(self, data):
                self.data = data

        mapper(A, a, properties={
            # if no backref here, delete-orphan failed until [ticket:427] was fixed
            'bs':relation(B, secondary=atob, cascade="all, delete-orphan")
        })
        mapper(B, b)

        sess = create_session()
        a1 = A('a1')
        b1 = B('b1')
        a1.bs.append(b1)
        sess.save(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1

    def testcascadedelete(self):
        class A(object):
            def __init__(self, data):
                self.data = data
        class B(object):
            def __init__(self, data):
                self.data = data

        mapper(A, a, properties={
            'bs':relation(B, secondary=atob, cascade="all, delete-orphan")
        })
        mapper(B, b)

        sess = create_session()
        a1 = A('a1')
        b1 = B('b1')
        a1.bs.append(b1)
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
            Column('user_name', String(40)),
        )

        addresses = Table('email_addresses', metadata,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(40)),
        )
        class User(object):pass
        class Address(object):pass

    def test_pending_orphan(self):
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

    def test_delete_new_object(self):
        """test that an entity which is attached then detached from its
        parent with a delete-orphan cascade gets counted as an orphan"""

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()

        u = User()
        s.save(u)
        s.flush()
        a = Address()
        assert a not in s.new
        u.addresses.append(a)
        u.addresses.remove(a)
        s.delete(u)
        try:
            s.flush() # (erroneously) causes "a" to be persisted
            assert False
        except exceptions.FlushError:
            assert True
        assert a.address_id is None, "Error: address should not be persistent"


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

    def testdeletechildwithchild(self):
        """test that an entity which is attached then detached from its
        parent with a delete-orphan cascade gets counted as an orphan, as well
        as its own child instances"""

        class Order(object): pass
        class Item(object): pass
        class Attribute(object): pass

        attrMapper = mapper(Attribute, attributes)
        itemMapper = mapper(Item, items, properties=dict(
            attributes=relation(attrMapper, cascade="all,delete-orphan", backref="item")
        ))
        orderMapper = mapper(Order, orders, properties=dict(
            items=relation(itemMapper, cascade="all,delete-orphan", backref="order")
        ))

        s = create_session( )
        order = Order()
        s.save(order)

        item = Item()
        attr = Attribute()
        item.attributes.append(attr)

        order.items.append(item)
        order.items.remove(item) # item is an orphan, but attr is not so flush() tries to save attr
        try:
            s.flush()
            assert False
        except exceptions.FlushError, e:
            print e
            assert True

        assert item.id is None
        assert attr.id is None

class DoubleParentOrphanTest(TestBase, AssertsExecutionResults):
    """test orphan detection for an entity with two parent relations"""

    def setUpAll(self):
        global metadata, address_table, businesses, homes
        metadata = MetaData(testing.db)
        address_table = Table('addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('street', String(30)),
        )

        homes = Table('homes', metadata,
            Column('home_id', Integer, primary_key=True),
            Column('description', String(30)),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )

        businesses = Table('businesses', metadata,
            Column('business_id', Integer, primary_key=True, key="id"),
            Column('description', String(30), key="description"),
            Column('address_id', Integer, ForeignKey('addresses.address_id'), nullable=False),
        )
        metadata.create_all()
    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        metadata.drop_all()
    def test_non_orphan(self):
        """test that an entity can have two parent delete-orphan cascades, and persists normally."""

        class Address(object):pass
        class Home(object):pass
        class Business(object):pass
        mapper(Address, address_table)
        mapper(Home, homes, properties={'address':relation(Address, cascade="all,delete-orphan")})
        mapper(Business, businesses, properties={'address':relation(Address, cascade="all,delete-orphan")})

        session = create_session()
        a1 = Address()
        a2 = Address()
        h1 = Home()
        b1 = Business()
        h1.address = a1
        b1.address = a2
        [session.save(x) for x in [h1,b1]]
        session.flush()

    def test_orphan(self):
        """test that an entity can have two parent delete-orphan cascades, and is detected as an orphan
        when saved without a parent."""

        class Address(object):pass
        class Home(object):pass
        class Business(object):pass
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

class CollectionAssignmentOrphanTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global metadata, table_a, table_b

        metadata = MetaData(testing.db)
        table_a = Table('a', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('foo', String(30)))
        table_b = Table('b', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('foo', String(30)),
                        Column('a_id', Integer, ForeignKey('a.id')))
        metadata.create_all()

    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        metadata.drop_all()

    def test_basic(self):
        class A(object):
            def __init__(self, foo):
                self.foo = foo
        class B(object):
            def __init__(self, foo):
                self.foo = foo

        mapper(A, table_a, properties={
            'bs':relation(B, cascade="all, delete-orphan")
            })
        mapper(B, table_b)

        a1 = A('a1')
        a1.bs.append(B('b1'))
        a1.bs.append(B('b2'))
        a1.bs.append(B('b3'))

        sess = create_session()
        sess.save(a1)
        sess.flush()

        assert table_b.count(table_b.c.a_id == None).scalar() == 0

        assert table_b.count().scalar() == 3

        a1 = sess.query(A).get(a1.id)
        assert len(a1.bs) == 3
        a1.bs = list(a1.bs)
        assert not class_mapper(B)._is_orphan(a1.bs[0])
        a1.bs[0].foo='b2modified'
        a1.bs[1].foo='b3modified'
        sess.flush()

        assert table_b.count().scalar() == 3

if __name__ == "__main__":
    testenv.main()

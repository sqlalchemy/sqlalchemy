import testbase, tables
import unittest, sys, datetime

from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy import *

class O2MCascadeTest(testbase.AssertMixin):
    def tearDown(self):
        ctx.current.clear()
        tables.delete()

    def tearDownAll(self):
        clear_mappers()
        tables.drop()

    def setUpAll(self):
        global ctx, data
        ctx = SessionContext(lambda: create_session( ))
        tables.create()
        mapper(tables.User, tables.users, properties = dict(
            address = relation(mapper(tables.Address, tables.addresses), lazy = False, uselist = False, private = True),
            orders = relation(
                mapper(tables.Order, tables.orders, properties = dict (
                    items = relation(mapper(tables.Item, tables.orderitems), lazy = False, uselist =True, private = True)
                )), 
                lazy = True, uselist = True, private = True)
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

        for elem in data[1:]:
            u = tables.User()
            ctx.current.save(u)
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

        ctx.current.flush()
        ctx.current.clear()

        
    def testdelete(self):
        l = ctx.current.query(tables.User).select()
        for u in l:
            self.echo( repr(u.orders))
        self.assert_result(l, data[0], *data[1:])

        self.echo("\n\n\n")
        ids = (l[0].user_id, l[2].user_id)
        ctx.current.delete(l[0])
        ctx.current.delete(l[2])

        ctx.current.flush()
        self.assert_(tables.orders.count(tables.orders.c.user_id.in_(*ids)).scalar() == 0)
        self.assert_(tables.orderitems.count(tables.orders.c.user_id.in_(*ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 0)
        self.assert_(tables.addresses.count(tables.addresses.c.user_id.in_(*ids)).scalar() == 0)
        self.assert_(tables.users.count(tables.users.c.user_id.in_(*ids)).scalar() == 0)
    

    def testorphan(self):
        l = ctx.current.query(tables.User).select()
        jack = l[1]
        jack.orders[:] = []

        ids = [jack.user_id]
        self.assert_(tables.orders.count(tables.orders.c.user_id.in_(*ids)).scalar() == 1)
        self.assert_(tables.orderitems.count(tables.orders.c.user_id.in_(*ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 2)

        ctx.current.flush()

        self.assert_(tables.orders.count(tables.orders.c.user_id.in_(*ids)).scalar() == 0)
        self.assert_(tables.orderitems.count(tables.orders.c.user_id.in_(*ids)  &(tables.orderitems.c.order_id==tables.orders.c.order_id)).scalar() == 0)


class M2OCascadeTest(testbase.AssertMixin):
    def tearDown(self):
        ctx.current.clear()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
            
    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()
        
    def setUpAll(self):
        global ctx, data, metadata, User, Pref
        ctx = SessionContext(create_session)
        metadata = BoundMetaData(testbase.db)
        prefs = Table('prefs', metadata, 
            Column('prefs_id', Integer, Sequence('prefs_id_seq', optional=True), primary_key=True),
            Column('prefs_data', String(40)))
            
        users = Table('users', metadata,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(40)),
            Column('pref_id', Integer, ForeignKey('prefs.prefs_id'))
        )
        class User(object):
            pass
        class Pref(object):
            pass
        metadata.create_all()
        mapper(User, users, properties = dict(
            pref = relation(mapper(Pref, prefs), lazy=False, cascade="all, delete-orphan")
        ))

    def setUp(self):
        global data
        data = [User,
            {'user_name' : 'ed', 
                'pref' : (Pref, {'prefs_data' : 'pref 1'}),
            },
            {'user_name' : 'jack', 
                'pref' : (Pref, {'prefs_data' : 'pref 2'}),
            },
            {'user_name' : 'foo', 
                'pref' : (Pref, {'prefs_data' : 'pref 3'}),
            }        
        ]

        for elem in data[1:]:
            u = User()
            ctx.current.save(u)
            u.user_name = elem['user_name']
            u.pref = Pref()
            u.pref.prefs_data = elem['pref'][1]['prefs_data']

        ctx.current.flush()
        ctx.current.clear()

    def testorphan(self):
        l = ctx.current.query(User).select()
        jack = l[1]
        jack.pref = None
        ctx.current.flush()

class M2MCascadeTest(testbase.AssertMixin):
    def setUpAll(self):
        global metadata, a, b, atob
        metadata = BoundMetaData(testbase.db)
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
        
if __name__ == "__main__":
    testbase.main()        

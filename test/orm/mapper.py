from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
import sqlalchemy.exceptions as exceptions

from tables import *
import tables

user_result = [{'user_id' : 7}, {'user_id' : 8}, {'user_id' : 9}]
user_address_result = [
{'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
{'user_id' : 8, 'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}])},
{'user_id' : 9, 'addresses' : (Address, [])}
]
user_address_orders_result = [{'user_id' : 7, 
    'addresses' : (Address, [{'address_id' : 1}]),
    'orders' : (Order, [{'order_id' : 1}, {'order_id' : 3},{'order_id' : 5},])
},

{'user_id' : 8, 
    'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}]),
    'orders' : (Order, [])
},
{'user_id' : 9, 
    'addresses' : (Address, []),
    'orders' : (Order, [{'order_id' : 2},{'order_id' : 4}])
}]

user_all_result = [
{'user_id' : 7, 
    'addresses' : (Address, [{'address_id' : 1}]),
    'orders' : (Order, [
        {'order_id' : 1, 'items': (Item, [])}, 
        {'order_id' : 3, 'items': (Item, [{'item_id':3, 'item_name':'item 3'}, {'item_id':4, 'item_name':'item 4'}, {'item_id':5, 'item_name':'item 5'}])},
        {'order_id' : 5, 'items': (Item, [])},
        ])
},
{'user_id' : 8, 
    'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}]),
    'orders' : (Order, [])
},
{'user_id' : 9, 
    'addresses' : (Address, []),
    'orders' : (Order, [
        {'order_id' : 2, 'items': (Item, [{'item_id':1, 'item_name':'item 1'}, {'item_id':2, 'item_name':'item 2'}])},
        {'order_id' : 4, 'items': (Item, [])}
    ])
}]

item_keyword_result = [
{'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
{'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2, 'name':'red'}, {'keyword_id' : 5, 'name':'small'}, {'keyword_id' : 7, 'name':'square'}])},
{'item_id' : 3, 'keywords' : (Keyword, [{'keyword_id' : 3,'name':'green'}, {'keyword_id' : 4,'name':'big'}, {'keyword_id' : 6,'name':'round'}])},
{'item_id' : 4, 'keywords' : (Keyword, [])},
{'item_id' : 5, 'keywords' : (Keyword, [])}
]


class MapperSuperTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        tables.create()
        tables.data()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        tables.drop()
        db.echo = testbase.echo
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass
    
class MapperTest(MapperSuperTest):
    def testget(self):
        s = create_session()
        mapper(User, users)
        self.assert_(s.get(User, 19) is None)
        u = s.get(User, 7)
        u2 = s.get(User, 7)
        self.assert_(u is u2)
        s.clear()
        u2 = s.get(User, 7)
        self.assert_(u is not u2)

    def testunicodeget(self):
        """tests that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail 
        on postgres, mysql and oracle unless it is converted to an encoded string"""
        metadata = BoundMetaData(db)
        table = Table('foo', metadata, 
            Column('id', Unicode(10), primary_key=True),
            Column('data', Unicode(40)))
        try:
            table.create()
            class LocalFoo(object):pass
            mapper(LocalFoo, table)
            crit = 'petit voix m\xe2\x80\x99a '.decode('utf-8')
            print repr(crit)
            create_session().query(LocalFoo).get(crit)
        finally:
            table.drop()

    def testrefresh(self):
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        s = create_session()
        u = s.get(User, 7)
        u.user_name = 'foo'
        a = Address()
        import sqlalchemy.orm.session
        assert sqlalchemy.orm.session.object_session(a) is None
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.refresh(u)
        
        # its refreshed, so not dirty
        self.assert_(u not in s.dirty)
        
        # username is back to the DB
        self.assert_(u.user_name == 'jack')
        
        self.assert_(a not in u.addresses)
        
        u.user_name = 'foo'
        u.addresses.append(a)
        # now its dirty
        self.assert_(u in s.dirty)
        self.assert_(u.user_name == 'foo')
        self.assert_(a in u.addresses)
        s.expire(u)

        # get the attribute, it refreshes
        self.assert_(u.user_name == 'jack')
        self.assert_(a not in u.addresses)

    def testbadconstructor(self):
        """tests that if the construction of a mapped class fails, the instnace does not get placed in the session"""
        class Foo(object):
            def __init__(self, one, two):
                pass
        mapper(Foo, users)
        sess = create_session()
        try:
            Foo('one', _sa_session=sess)
            assert False
        except:
            assert len(list(sess)) == 0
        try:
            Foo('one')
            assert False
        except TypeError, e:
            pass
            
    def testrefresh_lazy(self):
        """tests that when a lazy loader is set as a trigger on an object's attribute (at the attribute level, not the class level), a refresh() operation doesnt fire the lazy loader or create any problems"""
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses))})
        q2 = s.query(User).options(lazyload('addresses'))
        u = q2.selectfirst(users.c.user_id==8)
        def go():
            s.refresh(u)
        self.assert_sql_count(db, go, 1)

    def testexpire(self):
        s = create_session()
        mapper(User, users, properties={'addresses':relation(mapper(Address, addresses), lazy=False)})
        u = s.get(User, 7)
        assert(len(u.addresses) == 1)
        u.user_name = 'foo'
        del u.addresses[0]
        s.expire(u)
        # test plain expire
        self.assert_(u.user_name =='jack')
        self.assert_(len(u.addresses) == 1)
        
        # we're changing the database here, so if this test fails in the middle,
        # it'll screw up the other tests which are hardcoded to 7/'jack'
        u.user_name = 'foo'
        s.flush()
        # change the value in the DB
        users.update(users.c.user_id==7, values=dict(user_name='jack')).execute()
        s.expire(u)
        # object isnt refreshed yet, using dict to bypass trigger
        self.assert_(u.__dict__.get('user_name') != 'jack')
        # do a select
        s.query(User).select()
        # test that it refreshed
        self.assert_(u.__dict__['user_name'] == 'jack')
        
        # object should be back to normal now, 
        # this should *not* produce a SELECT statement (not tested here though....)
        self.assert_(u.user_name =='jack')
        
    def testrefresh2(self):
        s = create_session()
        mapper(Address, addresses)

        mapper(User, users, properties = dict(addresses=relation(Address,private=True,lazy=False)) )

        u=User()
        u.user_name='Justin'
        a = Address()
        a.address_id=17  # to work around the hardcoded IDs in this test suite....
        u.addresses.append(a)
        s.flush()
        s.clear()
        u = s.query(User).selectfirst()
        print u.user_name

        #ok so far
        s.expire(u)        #hangs when
        print u.user_name #this line runs

        s.refresh(u) #hangs
        
    def testmagic(self):
        mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        sess = create_session()
        l = sess.query(User).select_by(user_name='fred')
        self.assert_result(l, User, *[{'user_id':9}])
        u = l[0]
        
        u2 = sess.query(User).get_by_user_name('fred')
        self.assert_(u is u2)
        
        l = sess.query(User).select_by(email_address='ed@bettyboop.com')
        self.assert_result(l, User, *[{'user_id':8}])

        l = sess.query(User).select_by(User.c.user_name=='fred', addresses.c.email_address!='ed@bettyboop.com', user_id=9)

    def testprops(self):
        """tests the various attributes of the properties attached to classes"""
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        }).compile()
        self.assert_(User.addresses.property is m.props['addresses'])
        
    def testload(self):
        """tests loading rows with a mapper and producing object instances"""
        mapper(User, users)
        l = create_session().query(User).select()
        self.assert_result(l, User, *user_result)
        l = create_session().query(User).select(users.c.user_name.endswith('ed'))
        self.assert_result(l, User, *user_result[1:3])

    def testjoinvia(self):
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems))
            }))
        })

        q = create_session().query(m)

        l = q.select((orderitems.c.item_name=='item 4') & q.join_via(['orders', 'items']))
        self.assert_result(l, User, user_result[0])
        
        l = q.select_by(item_name='item 4')
        self.assert_result(l, User, user_result[0])

        l = q.select((orderitems.c.item_name=='item 4') & q.join_to('item_name'))
        self.assert_result(l, User, user_result[0])

        l = q.select((orderitems.c.item_name=='item 4') & q.join_to('items'))
        self.assert_result(l, User, user_result[0])
        
    def testorderby(self):
        # TODO: make a unit test out of these various combinations
#        m = mapper(User, users, order_by=desc(users.c.user_name))
        mapper(User, users, order_by=None)
#        mapper(User, users)
        
#        l = create_session().query(User).select(order_by=[desc(users.c.user_name), asc(users.c.user_id)])
        l = create_session().query(User).select()
#        l = create_session().query(User).select(order_by=[])
#        l = create_session().query(User).select(order_by=None)
        
        
    @testbase.unsupported('firebird') 
    def testfunction(self):
        """tests mapping to a SELECT statement that has functions in it."""
        s = select([users, (users.c.user_id * 2).label('concat'), func.count(addresses.c.address_id).label('count')],
        users.c.user_id==addresses.c.user_id, group_by=[c for c in users.c]).alias('myselect')
        mapper(User, s)
        sess = create_session()
        l = sess.query(User).select()
        for u in l:
            print "User", u.user_id, u.user_name, u.concat, u.count
        assert l[0].concat == l[0].user_id * 2 == 14
        assert l[1].concat == l[1].user_id * 2 == 16
        
    @testbase.unsupported('firebird') 
    def testcount(self):
        mapper(User, users)
        q = create_session().query(User)
        self.assert_(q.count()==3)
        self.assert_(q.count(users.c.user_id.in_(8,9))==2)
        self.assert_(q.count_by(user_name='fred')==1)
            
    def testmultitable(self):
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, primary_key=[users.c.user_id])
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *user_result[0:2])

    def testoverride(self):
        # assert that overriding a column raises an error
        try:
            m = mapper(User, users, properties = {
                    'user_name' : relation(mapper(Address, addresses)),
                }).compile()
            self.assert_(False, "should have raised ArgumentError")
        except exceptions.ArgumentError, e:
            self.assert_(True)
        
        clear_mappers()
        # assert that allow_column_override cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses))
            }, allow_column_override=True)
            
        clear_mappers()
        # assert that the column being named else where also cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses)),
                'foo' : users.c.user_name,
            })

    def testeageroptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        l = sess.query(User).options(eagerload('addresses')).select()

        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 0)

    def testeagerdegrade(self):
        """tests that an eager relation automatically degrades to a lazy relation if eager columns are not available"""
        sess = create_session()
        usermapper = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        )).compile()

        # first test straight eager load, 1 statement
        def go():
            l = usermapper.query(sess).select()
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 1)
        
        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        def go():
            r = users.select().execute()
            l = usermapper.instances(r, sess)
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 4)
        
    def testlazyoptions(self):
        """tests that an eager relation can be upgraded to a lazy relation via the options method"""
        sess = create_session()
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        l = sess.query(User).options(lazyload('addresses')).select()
        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 3)

    def testlatecompile(self):
        """tests mappers compiling late in the game"""
        
        mapper(User, users, properties = {'orders': relation(Order)})
        mapper(Item, orderitems, properties={'keywords':relation(Keyword, secondary=itemkeywords)})
        mapper(Keyword, keywords)
        mapper(Order, orders, properties={'items':relation(Item)})
        
        sess = create_session()
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(db, go, 3)

    def testdeepoptions(self):
        mapper(User, users,
            properties = {
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
                    }))
                }))
            })
            
        sess = create_session()
        q2 = sess.query(User).options(eagerload('orders.items.keywords'))
        u = sess.query(User).select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(db, go, 3)
        sess.clear()
        u = q2.select()
        self.assert_sql_count(db, go, 2)
        
class InheritanceTest(MapperSuperTest):

    def testinherits(self):
        class _Order(object):
            pass
        ordermapper = mapper(_Order, orders)
            
        class _User(object):
            pass
        usermapper = mapper(_User, users, properties = dict(
                orders = relation(ordermapper, lazy = False)
            ))

        class AddressUser(_User):
            pass
        mapper(AddressUser, addresses, inherits = usermapper)
        
        sess = create_session()
        q = sess.query(AddressUser)    
        l = q.select()
        
        jack = l[0]
        self.assert_(jack.user_name=='jack')
        jack.email_address = 'jack@gmail.com'
        sess.flush()
        sess.clear()
        au = q.get_by(user_name='jack')
        self.assert_(au.email_address == 'jack@gmail.com')

    def testinherits2(self):
        class _Order(object):
            pass
        class _Address(object):
            pass
        class AddressUser(_Address):
            pass
        ordermapper = mapper(_Order, orders)
        addressmapper = mapper(_Address, addresses)
        usermapper = mapper(AddressUser, users, inherits = addressmapper,
            properties = {
                'orders' : relation(ordermapper, lazy=False)
            })
        sess = create_session()
        l = sess.query(usermapper).select()
        jack = l[0]
        self.assert_(jack.user_name=='jack')
        jack.email_address = 'jack@gmail.com'
        sess.flush()
        sess.clear()
        au = sess.query(usermapper).get_by(user_name='jack')
        self.assert_(au.email_address == 'jack@gmail.com')
            
    
class DeferredTest(MapperSuperTest):

    def testbasic(self):
        """tests a basic "deferred" load"""
        
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        o = Order()
        self.assert_(o.description is None)

        q = create_session().query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.description

        orderby = str(orders.default_order_by()[0].compile(engine=db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.description AS orders_description FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
    
    def testsave(self):
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        sess = create_session()
        q = sess.query(m)
        l = q.select()
        o2 = l[2]
        o2.isopen = 1
        sess.flush()
        
    def testgroup(self):
        """tests deferred load with a group"""
        
        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })
        q = create_session().query(m)
        def go():
            l = q.select()
            o2 = l[2]
            print o2.opened, o2.description, o2.userident

        orderby = str(orders.default_order_by()[0].compile(db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        
    def testoptions(self):
        """tests using options on a mapper to create deferred and undeferred columns"""
        m = mapper(Order, orders)
        sess = create_session()
        q = sess.query(m)
        q2 = q.options(defer('user_id'))
        def go():
            l = q2.select()
            print l[2].user_id
            
        orderby = str(orders.default_order_by()[0].compile(db))
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
            ("SELECT orders.user_id AS orders_user_id FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        sess.clear()
        q3 = q2.options(undefer('user_id'))
        def go():
            l = q3.select()
            print l[3].user_id
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY %s" % orderby, {}),
        ])

        
    def testdeepoptions(self):
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems, properties={
                    'item_name':deferred(orderitems.c.item_name)
                }))
            }))
        })
        sess = create_session()
        q = sess.query(m)
        l = q.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(db, go, 1)
        self.assert_(item.item_name == 'item 4')
        sess.clear()
        q2 = q.options(undefer('orders.items.item_name'))
        l = q2.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(db, go, 0)
        self.assert_(item.item_name == 'item 4')
    
    
class LazyTest(MapperSuperTest):

    def testbasic(self):
        """tests a basic one-to-many lazy load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        q = create_session().query(m)
        l = q.select(users.c.user_id == 7)
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            )

    def testorderby(self):
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = True, order_by=addresses.c.email_address),
        ))
        q = create_session().query(m)
        l = q.select()

        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@bettyboop.com'}, {'email_address':'ed@lala.com'}, {'email_address':'ed@wood.com'}])},
            {'user_id' : 9, 'addresses' : (Address, [])}
            )

    def testorderby_select(self):
        """tests that a regular mapper select on a single table can order by a relation to a second table"""
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = True),
        ))
        q = create_session().query(m)
        l = q.select(users.c.user_id==addresses.c.user_id, order_by=addresses.c.email_address)

        self.assert_result(l, User,
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@wood.com'}, {'email_address':'ed@bettyboop.com'}, {'email_address':'ed@lala.com'}, ])},
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
        )
        
    def testorderby_desc(self):
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = True, order_by=[desc(addresses.c.email_address)]),
        ))
        q = create_session().query(m)
        l = q.select()

        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@wood.com'}, {'email_address':'ed@lala.com'}, {'email_address':'ed@bettyboop.com'}])},
            {'user_id' : 9, 'addresses' : (Address, [])},
            )

    def testlimit(self):
        ordermapper = mapper(Order, orders, properties = dict(
                items = relation(mapper(Item, orderitems), lazy = True)
            ))

        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            orders = relation(ordermapper, primaryjoin = users.c.user_id==orders.c.user_id, lazy = True),
        ))
        sess= create_session()
        q = sess.query(m)
        l = q.select(limit=2, offset=1)
        self.assert_result(l, User, *user_all_result[1:3])
        # use a union all to get a lot of rows to join against
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        print [key for key in s.c.keys()]
        l = q.select(s.c.u2_user_id==User.c.user_id, distinct=True)
        self.assert_result(l, User, *user_all_result)
        
        sess.clear()
        m = mapper(Item, orderitems, is_primary=True, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        
        l = sess.query(m).select((Item.c.item_name=='item 2') | (Item.c.item_name=='item 5') | (Item.c.item_name=='item 3'), order_by=[Item.c.item_id], limit=2)        
        self.assert_result(l, Item, *[item_keyword_result[1], item_keyword_result[2]])

    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False)
        ))
        q = create_session().query(m)
        l = q.select(users.c.user_id == 7)
        self.assert_result(l, User, {'user_id':7, 'address' : (Address, {'address_id':1})})

    def testbackwardsonetoone(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        q = create_session().query(m)
        l = q.select(addresses.c.address_id == 1)
        self.echo(repr(l))
        print repr(l[0].__dict__)
        self.echo(repr(l[0].user))
        self.assert_(l[0].user is not None)


    def testdouble(self):
        """tests lazy loading with two relations simulatneously, from the same table, using aliases.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True),
            open_orders = relation(mapper(Order, openorders, entity_name='open'), primaryjoin = and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = True),
            closed_orders = relation(mapper(Order, closedorders,entity_name='closed'), primaryjoin = and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = True)
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User,
            {'user_id' : 7, 
                'addresses' : (Address, [{'address_id' : 1}]),
                'open_orders' : (Order, [{'order_id' : 3}]),
                'closed_orders' : (Order, [{'order_id' : 1},{'order_id' : 5},])
            },
            {'user_id' : 8, 
                'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}]),
                'open_orders' : (Order, []),
                'closed_orders' : (Order, [])
            },
            {'user_id' : 9, 
                'addresses' : (Address, []),
                'open_orders' : (Order, [{'order_id' : 4}]),
                'closed_orders' : (Order, [{'order_id' : 2}])
            }
            )

    def testmanytomany(self):
        """tests a many-to-many lazy load"""
        mapper(Item, orderitems, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        q = create_session().query(Item)
        l = q.select()
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
            {'item_id' : 3, 'keywords' : (Keyword, [{'keyword_id' : 3}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 4, 'keywords' : (Keyword, [])},
            {'item_id' : 5, 'keywords' : (Keyword, [])}
        )
        l = q.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, Item.c.item_id==itemkeywords.c.item_id))
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
        )

class EagerTest(MapperSuperTest):
    def testbasic(self):
        testbase.db.echo="debug"
        """tests a basic one-to-many eager load"""
        m = mapper(Address, addresses)
        
        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = False),
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *user_address_result)
        
    def testorderby(self):
        m = mapper(Address, addresses)
        
        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = False, order_by=addresses.c.email_address),
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@bettyboop.com'}, {'email_address':'ed@lala.com'}, {'email_address':'ed@wood.com'}])},
            {'user_id' : 9, 'addresses' : (Address, [])}
            )

    def testorderby_desc(self):
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = False, order_by=[desc(addresses.c.email_address)]),
        ))
        q = create_session().query(m)
        l = q.select()

        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@wood.com'},{'email_address':'ed@lala.com'},  {'email_address':'ed@bettyboop.com'}, ])},
            {'user_id' : 9, 'addresses' : (Address, [])},
            )
    
    def testlimit(self):
        ordermapper = mapper(Order, orders, properties = dict(
                items = relation(mapper(Item, orderitems), lazy = False)
            ))

        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            orders = relation(ordermapper, primaryjoin = users.c.user_id==orders.c.user_id, lazy = False),
        ))
        sess = create_session()
        q = sess.query(m)
        
        l = q.select(limit=2, offset=1)
        self.assert_result(l, User, *user_all_result[1:3])
        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get the same
        # result with or without the distinct, just via less or more rows.
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        l = q.select(s.c.u2_user_id==User.c.user_id, distinct=True)
        self.assert_result(l, User, *user_all_result)
        sess.clear()
        m = mapper(Item, orderitems, is_primary=True, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = False, order_by=[keywords.c.keyword_id]),
            ))
        q = sess.query(m)
        l = q.select((Item.c.item_name=='item 2') | (Item.c.item_name=='item 5') | (Item.c.item_name=='item 3'), order_by=[Item.c.item_id], limit=2)        
        self.assert_result(l, Item, *[item_keyword_result[1], item_keyword_result[2]])
        
        
        
    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = False, uselist = False)
        ))
        q = create_session().query(m)
        l = q.select(users.c.user_id == 7)
        self.assert_result(l, User,
            {'user_id' : 7, 'address' : (Address, {'address_id' : 1, 'email_address': 'jack@bean.com'})},
            )

    def testbackwardsonetoone(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = False)
        )).compile()
        self.echo(repr(m.props['user'].uselist))
        q = create_session().query(m)
        l = q.select(addresses.c.address_id == 1)
        self.assert_result(l, Address, 
            {'address_id' : 1, 'email_address' : 'jack@bean.com', 
                'user' : (User, {'user_id' : 7, 'user_name' : 'jack'}) 
            },
        )

    def testwithrepeat(self):
        """tests a one-to-many eager load where we also query on joined criterion, where the joined
        criterion is using the same tables that are used within the eager load.  the mapper must insure that the 
        criterion doesnt interfere with the eager load criterion."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), primaryjoin = users.c.user_id==addresses.c.user_id, lazy = False)
        ))
        q = create_session().query(m)
        l = q.select(and_(addresses.c.email_address == 'ed@lala.com', addresses.c.user_id==users.c.user_id))
        self.assert_result(l, User,
            {'user_id' : 8, 'addresses' : (Address, [{'address_id' : 2, 'email_address':'ed@wood.com'}, {'address_id':3, 'email_address':'ed@bettyboop.com'}, {'address_id':4, 'email_address':'ed@lala.com'}])},
        )
        

    def testcompile(self):
        """tests deferred operation of a pre-compiled mapper statement"""
        session = create_session()
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        s = session.query(m).compile(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.user_id))
        c = s.compile()
        self.echo("\n" + str(c) + repr(c.get_params()))
        
        l = m.instances(s.execute(emailad = 'jack@bean.com'), session)
        self.echo(repr(l))
        
    def testmulti(self):
        """tests eager loading with two relations simultaneously"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), primaryjoin = users.c.user_id==addresses.c.user_id, lazy = False),
            orders = relation(mapper(Order, orders), lazy = False),
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User,
            {'user_id' : 7, 
                'addresses' : (Address, [{'address_id' : 1}]),
                'orders' : (Order, [{'order_id' : 1}, {'order_id' : 3},{'order_id' : 5},])
            },
            {'user_id' : 8, 
                'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}]),
                'orders' : (Order, [])
            },
            {'user_id' : 9, 
                'addresses' : (Address, []),
                'orders' : (Order, [{'order_id' : 2},{'order_id' : 4}])
            }
            )

    def testdouble(self):
        """tests eager loading with two relations simulatneously, from the same table.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        ordermapper = mapper(Order, orders)
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            open_orders = relation(mapper(Order, openorders, non_primary=True), primaryjoin = and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = False),
            closed_orders = relation(mapper(Order, closedorders, non_primary=True), primaryjoin = and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = False)
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User,
            {'user_id' : 7, 
                'addresses' : (Address, [{'address_id' : 1}]),
                'open_orders' : (Order, [{'order_id' : 3}]),
                'closed_orders' : (Order, [{'order_id' : 1},{'order_id' : 5},])
            },
            {'user_id' : 8, 
                'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}, {'address_id' : 4}]),
                'open_orders' : (Order, []),
                'closed_orders' : (Order, [])
            },
            {'user_id' : 9, 
                'addresses' : (Address, []),
                'open_orders' : (Order, [{'order_id' : 4}]),
                'closed_orders' : (Order, [{'order_id' : 2}])
            }
            )

    def testnested(self):
        """tests eager loading of a parent item with two types of child items,
        where one of those child items eager loads its own child items."""
        ordermapper = mapper(Order, orders, properties = dict(
                items = relation(mapper(Item, orderitems), lazy = False)
            ))

        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            orders = relation(ordermapper, primaryjoin = users.c.user_id==orders.c.user_id, lazy = False),
        ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, User, *user_all_result)
    
    def testmanytomany(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy=False, order_by=[keywords.c.keyword_id]),
            ))
        q = create_session().query(m)
        l = q.select()
        self.assert_result(l, Item, *item_keyword_result)
        
        l = q.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
        )
    
    def testmanytomanyoptions(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy=True, order_by=[keywords.c.keyword_id]),
            ))
        m2 = m.options(eagerload('keywords'))
        q = create_session().query(m2)
        def go():
            l = q.select()
            self.assert_result(l, Item, *item_keyword_result)
        self.assert_sql_count(db, go, 1)
        
        def go():
            l = q.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
            self.assert_result(l, Item, 
                {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
                {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
            )
        self.assert_sql_count(db, go, 1)
        
    def testoneandmany(self):
        """tests eager load for a parent object with a child object that 
        contains a many-to-many relationship to a third object."""
        items = orderitems

        m = mapper(Item, items, 
            properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = False, order_by=[keywords.c.keyword_id]),
            ))

        m = mapper(Order, orders, properties = dict(
                items = relation(m, lazy = False)
            ))
        q = create_session().query(m)
        l = q.select("orders.order_id in (1,2,3)")
        self.assert_result(l, Order,
            {'order_id' : 1, 'items': (Item, [])}, 
            {'order_id' : 2, 'items': (Item, [
                {'item_id':1, 'item_name':'item 1', 'keywords': (Keyword, [{'keyword_id':2, 'name':'red'}, {'keyword_id':4, 'name':'big'}, {'keyword_id' : 6, 'name':'round'}])}, 
                {'item_id':2, 'item_name':'item 2','keywords' : (Keyword, [{'keyword_id' : 2, 'name':'red'}, {'keyword_id' : 5, 'name':'small'}, {'keyword_id' : 7, 'name':'square'}])}
               ])},
            {'order_id' : 3, 'items': (Item, [
                {'item_id':3, 'item_name':'item 3', 'keywords' : (Keyword, [{'keyword_id' : 3, 'name':'green'}, {'keyword_id' : 4, 'name':'big'}, {'keyword_id' : 6, 'name':'round'}])}, 
                {'item_id':4, 'item_name':'item 4'}, 
                {'item_id':5, 'item_name':'item 5'}
               ])},
        )
        
        
if __name__ == "__main__":    
    testbase.main()

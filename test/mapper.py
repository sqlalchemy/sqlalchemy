from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *


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
        objectstore.clear()
        clear_mappers()
    def setUp(self):
        pass
    
class MapperTest(MapperSuperTest):
    def testget(self):
        m = mapper(User, users)
        self.assert_(m.get(19) is None)
        u = m.get(7)
        u2 = m.get(7)
        self.assert_(u is u2)
        objectstore.clear()
        u2 = m.get(7)
        self.assert_(u is not u2)

    def testrefresh(self):
        m = mapper(User, users)
        u = m.get(7)
        u.user_name = 'foo'
        objectstore.refresh(u)
        
        # its refreshed, so not dirty
        self.assert_(u not in objectstore.get_session().uow.dirty)
        
        # username is back to the DB
        self.assert_(u.user_name == 'jack')
        
        u.user_name = 'foo'
        # now its dirty
        self.assert_(u in objectstore.get_session().uow.dirty)
        self.assert_(u.user_name == 'foo')
        objectstore.expire(u)

        # expired, but not refreshed yet.  still dirty
        self.assert_(u in objectstore.get_session().uow.dirty)
        # get the attribute, it refreshes
        self.assert_(u.user_name == 'jack')
        # not dirty anymore
        self.assert_(u not in objectstore.get_session().uow.dirty)
        
    def testmagic(self):
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        l = m.select_by(user_name='fred')
        self.assert_result(l, User, *[{'user_id':9}])
        u = l[0]
        
        u2 = m.get_by_user_name('fred')
        self.assert_(u is u2)
        
        l = m.select_by(email_address='ed@bettyboop.com')
        self.assert_result(l, User, *[{'user_id':8}])

        l = m.select_by(User.c.user_name=='fred', addresses.c.email_address!='ed@bettyboop.com', user_id=9)

    def testprops(self):
        """tests the various attributes of the properties attached to classes"""
        m = mapper(User, users, properties = {
            'addresses' : relation(mapper(Address, addresses))
        })
        self.assert_(User.addresses.property is m.props['addresses'])
        
    def testload(self):
        """tests loading rows with a mapper and producing object instances"""
        m = mapper(User, users)
        l = m.select()
        self.assert_result(l, User, *user_result)
        l = m.select(users.c.user_name.endswith('ed'))
        self.assert_result(l, User, *user_result[1:3])

    def testorderby(self):
        # TODO: make a unit test out of these various combinations
#        m = mapper(User, users, order_by=desc(users.c.user_name))
        m = mapper(User, users, order_by=None)
#        m = mapper(User, users)
        
#        l = m.select(order_by=[desc(users.c.user_name), asc(users.c.user_id)])
        l = m.select()
#        l = m.select(order_by=[])
#        l = m.select(order_by=None)
        
        
    def testfunction(self):
        """tests mapping to a SELECT statement that has functions in it."""
        s = select([users, (users.c.user_id * 2).label('concat'), func.count(addresses.c.address_id).label('count')],
        users.c.user_id==addresses.c.user_id, group_by=[c for c in users.c]).alias('myselect')
        m = mapper(User, s, primarytable=users)
        print [c.key for c in m.c]
        l = m.select()
        for u in l:
            print "User", u.user_id, u.user_name, u.concat, u.count
        #l[1].user_name='asdf'
        #objectstore.commit()
    
    def testcount(self):
        m = mapper(User, users)
        self.assert_(m.count()==3)
        self.assert_(m.count(users.c.user_id.in_(8,9))==2)
        self.assert_(m.count_by(user_name='fred')==1)
            
    def testmultitable(self):
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, primarytable = users, primary_key=[users.c.user_id])
        l = m.select()
        self.assert_result(l, User, *user_result[0:2])

    def testoverride(self):
        # assert that overriding a column raises an error
        try:
            m = mapper(User, users, properties = {
                    'user_name' : relation(mapper(Address, addresses)),
                })
            self.assert_(False, "should have raised ArgumentError")
        except ArgumentError, e:
            self.assert_(True)
            
        # assert that allow_column_override cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses))
            }, allow_column_override=True)
            
        # assert that the column being named else where also cancels the error
        m = mapper(User, users, properties = {
                'user_name' : relation(mapper(Address, addresses)),
                'foo' : users.c.user_name,
            })

    def testeageroptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        l = m.options(eagerload('addresses')).select()

        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 0)

    def testlazyoptions(self):
        """tests that an eager relation can be upgraded to a lazy relation via the options method"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        l = m.options(lazyload('addresses')).select()
        def go():
            self.assert_result(l, User, *user_address_result)
        self.assert_sql_count(db, go, 3)

    def testdeepoptions(self):
        m = mapper(User, users,
            properties = {
                'orders': relation(mapper(Order, orders, properties = {
                    'items' : relation(mapper(Item, orderitems, properties = {
                        'keywords' : relation(mapper(Keyword, keywords), itemkeywords)
                    }))
                }))
            })
            
        m2 = m.options(eagerload('orders.items.keywords'))
        u = m.select()
        def go():
            print u[0].orders[1].items[0].keywords[1]
        self.assert_sql_count(db, go, 3)
        objectstore.clear()
        u = m2.select()
        self.assert_sql_count(db, go, 2)
        
class PropertyTest(MapperSuperTest):
    def testbasic(self):
        """tests that you can create mappers inline with class definitions"""
        class _Address(object):
            pass
        assign_mapper(_Address, addresses)
            
        class _User(object):
            pass
        assign_mapper(_User, users, properties = dict(
                addresses = relation(_Address.mapper, lazy = False)
            ), is_primary = True)
        
        l = _User.mapper.select(_User.c.user_name == 'fred')
        self.echo(repr(l))
        

    def testinherits(self):
        class _Order(object):
            pass
        assign_mapper(_Order, orders)
            
        class _User(object):
            pass
        assign_mapper(_User, users, properties = dict(
                orders = relation(_Order.mapper, lazy = False)
            ))

        class AddressUser(_User):
            pass
        assign_mapper(AddressUser, addresses, inherits = _User.mapper)
            
        l = AddressUser.mapper.select()
        
        jack = l[0]
        self.assert_(jack.user_name=='jack')
        jack.email_address = 'jack@gmail.com'
        objectstore.commit()
        objectstore.clear()
        au = AddressUser.mapper.get_by(user_name='jack')
        self.assert_(au.email_address == 'jack@gmail.com')

    def testinherits2(self):
        class _Order(object):
            pass
        class _Address(object):
            pass
        class AddressUser(_Address):
            pass
        assign_mapper(_Order, orders)
        assign_mapper(_Address, addresses)
        assign_mapper(AddressUser, users, inherits = _Address.mapper,
            properties = {
                'orders' : relation(_Order.mapper, lazy=False)
            })
        l = AddressUser.mapper.select()
        jack = l[0]
        self.assert_(jack.user_name=='jack')
        jack.email_address = 'jack@gmail.com'
        objectstore.commit()
        objectstore.clear()
        au = AddressUser.mapper.get_by(user_name='jack')
        self.assert_(au.email_address == 'jack@gmail.com')
            
    
class DeferredTest(MapperSuperTest):

    def testbasic(self):
        """tests a basic "deferred" load"""
        
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        o = Order()
        self.assert_(o.description is None)
        
        def go():
            l = m.select()
            o2 = l[2]
            print o2.description

        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.isopen AS orders_isopen FROM orders ORDER BY orders.%s" % orders.default_order_by()[0].key, {}),
            ("SELECT orders.description AS orders_description FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
    
    def testsave(self):
        m = mapper(Order, orders, properties={
            'description':deferred(orders.c.description)
        })
        
        l = m.select()
        o2 = l[2]
        o2.isopen = 1
        objectstore.commit()
        
    def testgroup(self):
        """tests deferred load with a group"""
        
        m = mapper(Order, orders, properties = {
            'userident':deferred(orders.c.user_id, group='primary'),
            'description':deferred(orders.c.description, group='primary'),
            'opened':deferred(orders.c.isopen, group='primary')
        })

        def go():
            l = m.select()
            o2 = l[2]
            print o2.opened, o2.description, o2.userident
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id FROM orders ORDER BY orders.%s" % orders.default_order_by()[0].key, {}),
            ("SELECT orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        
    def testoptions(self):
        """tests using options on a mapper to create deferred and undeferred columns"""
        m = mapper(Order, orders)
        m2 = m.options(defer('user_id'))
        def go():
            l = m2.select()
            print l[2].user_id
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY orders.%s" % orders.default_order_by()[0].key, {}),
            ("SELECT orders.user_id AS orders_user_id FROM orders WHERE orders.order_id = :orders_order_id", {'orders_order_id':3})
        ])
        objectstore.clear()
        m3 = m2.options(undefer('user_id'))
        def go():
            l = m3.select()
            print l[3].user_id
        self.assert_sql(db, go, [
            ("SELECT orders.order_id AS orders_order_id, orders.user_id AS orders_user_id, orders.description AS orders_description, orders.isopen AS orders_isopen FROM orders ORDER BY orders.%s" % orders.default_order_by()[0].key, {}),
        ])

    def testdeepoptions(self):
        m = mapper(User, users, properties={
            'orders':relation(mapper(Order, orders, properties={
                'items':relation(mapper(Item, orderitems, properties={
                    'item_name':deferred(orderitems.c.item_name)
                }))
            }))
        })
        l = m.select()
        item = l[0].orders[1].items[1]
        def go():
            print item.item_name
        self.assert_sql_count(db, go, 1)
        self.assert_(item.item_name == 'item 4')
        objectstore.clear()
        m2 = m.options(undefer('orders.items.item_name'))
        l = m2.select()
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
        l = m.select(users.c.user_id == 7)
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            )

    def testorderby(self):
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = True, order_by=addresses.c.email_address),
        ))
        l = m.select()

        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'email_address' : 'jack@bean.com'}])},
            {'user_id' : 8, 'addresses' : (Address, [{'email_address':'ed@bettyboop.com'}, {'email_address':'ed@lala.com'}, {'email_address':'ed@wood.com'}])},
            {'user_id' : 9, 'addresses' : (Address, [])}
            )

    def testorderby_desc(self):
        m = mapper(Address, addresses)

        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = True, order_by=[desc(addresses.c.email_address)]),
        ))
        l = m.select()

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
        l = m.select(limit=2, offset=1)
        self.assert_result(l, User, *user_all_result[1:3])
        # use a union all to get a lot of rows to join against
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        print [key for key in s.c.keys()]
        l = m.select(s.c.u2_user_id==User.c.user_id, distinct=True)
        self.assert_result(l, User, *user_all_result)
        
        objectstore.clear()
        m = mapper(Item, orderitems, is_primary=True, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        l = m.select((Item.c.item_name=='item 2') | (Item.c.item_name=='item 5') | (Item.c.item_name=='item 3'), order_by=[Item.c.item_id], limit=2)        
        self.assert_result(l, Item, *[item_keyword_result[1], item_keyword_result[2]])

    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False)
        ))
        l = m.select(users.c.user_id == 7)
        self.echo(repr(l))
        self.echo(repr(l[0].address))

    def testbackwardsonetoone(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users, properties = {'id':users.c.user_id}), lazy = True)
        ))
        l = m.select(addresses.c.address_id == 1)
        self.echo(repr(l))
        print repr(l[0].__dict__)
        self.echo(repr(l[0].user))
        self.assert_(l[0].user is not None)


    def testdouble(self):
        """tests lazy loading with two relations simulatneously, from the same table, using aliases.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            open_orders = relation(mapper(Order, openorders), primaryjoin = and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = True),
            closed_orders = relation(mapper(Order, closedorders), primaryjoin = and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = True)
        ))
        l = m.select()
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
        assign_mapper(Item, orderitems, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = True),
            ))
        l = Item.mapper.select()
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
            {'item_id' : 3, 'keywords' : (Keyword, [{'keyword_id' : 3}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 4, 'keywords' : (Keyword, [])},
            {'item_id' : 5, 'keywords' : (Keyword, [])}
        )
        l = Item.mapper.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, Item.c.item_id==itemkeywords.c.item_id))
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
        )

class EagerTest(MapperSuperTest):
    def testbasic(self):
        """tests a basic one-to-many eager load"""
        
        m = mapper(Address, addresses)
        
        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = False),
        ))
        l = m.select()
        self.assert_result(l, User, *user_address_result)
        
    def testorderby(self):
        m = mapper(Address, addresses)
        
        m = mapper(User, users, properties = dict(
            addresses = relation(m, lazy = False, order_by=addresses.c.email_address),
        ))
        l = m.select()
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
        l = m.select()

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
        l = m.select(limit=2, offset=1)
        self.assert_result(l, User, *user_all_result[1:3])
        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get the same
        # result with or without the distinct, just via less or more rows.
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        l = m.select(s.c.u2_user_id==User.c.user_id, distinct=True)
        self.assert_result(l, User, *user_all_result)
        objectstore.clear()
        m = mapper(Item, orderitems, is_primary=True, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = False, order_by=[keywords.c.keyword_id]),
            ))
        l = m.select((Item.c.item_name=='item 2') | (Item.c.item_name=='item 5') | (Item.c.item_name=='item 3'), order_by=[Item.c.item_id], limit=2)        
        self.assert_result(l, Item, *[item_keyword_result[1], item_keyword_result[2]])
        
        
        
    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = False, uselist = False)
        ))
        l = m.select(users.c.user_id == 7)
        self.assert_result(l, User,
            {'user_id' : 7, 'address' : (Address, {'address_id' : 1, 'email_address': 'jack@bean.com'})},
            )

    def testbackwardsonetoone(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = False)
        ))
        self.echo(repr(m.props['user'].uselist))
        l = m.select(addresses.c.address_id == 1)
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
        l = m.select(and_(addresses.c.email_address == 'ed@lala.com', addresses.c.user_id==users.c.user_id))
        self.assert_result(l, User,
            {'user_id' : 8, 'addresses' : (Address, [{'address_id' : 2, 'email_address':'ed@wood.com'}, {'address_id':3, 'email_address':'ed@bettyboop.com'}, {'address_id':4, 'email_address':'ed@lala.com'}])},
        )
        

    def testcompile(self):
        """tests deferred operation of a pre-compiled mapper statement"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
        ))
        s = m.compile(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.user_id))
        c = s.compile()
        self.echo("\n" + str(c) + repr(c.get_params()))
        
        l = m.instances(s.execute(emailad = 'jack@bean.com'))
        self.echo(repr(l))
        
    def testmulti(self):
        """tests eager loading with two relations simultaneously"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), primaryjoin = users.c.user_id==addresses.c.user_id, lazy = False),
            orders = relation(mapper(Order, orders), lazy = False),
        ))
        l = m.select()
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
            open_orders = relation(mapper(Order, openorders), primaryjoin = and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = False),
            closed_orders = relation(mapper(Order, closedorders), primaryjoin = and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = False)
        ))
        l = m.select()
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
        l = m.select()
        self.assert_result(l, User, *user_all_result)
    
    def testmanytomany(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy=False, order_by=[keywords.c.keyword_id]),
            ))
        l = m.select()
        self.assert_result(l, Item, *item_keyword_result)
        
#        l = m.select()
        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
        )
    
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
        l = m.select("orders.order_id in (1,2,3)")
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

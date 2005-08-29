from testbase import PersistTest
import unittest, sys, os

execfile("test/tables.py")

class User(object):
    def __repr__(self):
        return (
"""
objid: %d
User ID: %s
User Name: %s
email address ?: %s
Addresses: %s
Orders: %s
Open Orders %s
Closed Orderss %s
------------------
""" % tuple([id(self), self.user_id, repr(self.user_name), repr(getattr(self, 'email_address', None))] + [repr(getattr(self, attr, None)) for attr in ('addresses', 'orders', 'orders_open', 'orders_closed')])
)

class Address(object):
    def __repr__(self):
        return "Address: " + repr(self.address_id) + " " + repr(self.user_id) + " " + repr(self.email_address)

class Order(object):
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item(object):
    def __repr__(self):
        return "Item: " + repr(self.item_name) + " " +repr(getattr(self, 'keywords', None))
    
class Keyword(object):
    def __repr__(self):
        return "Keyword: %s/%s" % (repr(self.keyword_id),repr(self.name))

class AssertMixin(PersistTest):
    def assert_result(self, result, class_, *objects):
        print repr(result)
        self.assert_list(result, class_, objects)
    def assert_list(self, result, class_, list):
        for i in range(0, len(list)):
            self.assert_row(class_, result[i], list[i])
    def assert_row(self, class_, rowobj, desc):
        self.assert_(rowobj.__class__ is class_, "item class is not " + repr(class_))
        for key, value in desc.iteritems():
            if isinstance(value, tuple):
                self.assert_list(getattr(rowobj, key), value[0], value[1])
            else:
                self.assert_(getattr(rowobj, key) == value, "attribute %s value %s does not match %s" % (key, getattr(rowobj, key), value))
        
class MapperTest(AssertMixin):
    
    def setUp(self):
        globalidentity().clear()

    def testget(self):
        m = mapper(User, users, echo = True)
        self.assert_(m.get(19) is None)
        u = m.get(7)
        u2 = m.get(7)
        self.assert_(u is u2)

    def testload(self):
        """tests loading rows with a mapper and producing object instances"""
        m = mapper(User, users)
        l = m.select()
        self.assert_result(l, User, {'user_id' : 7}, {'user_id' : 8}, {'user_id' : 9})
        l = m.select(users.c.user_name.endswith('ed'))
        self.assert_result(l, User, {'user_id' : 8}, {'user_id' : 9})

    def testmultitable(self):
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, table = users)
        l = m.select()
        print repr(l)

    def testeageroptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ), echo = True)
        l = m.options(eagerload('addresses')).select()
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            {'user_id' : 8, 'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}])},
            {'user_id' : 9, 'addresses' : (Address, [])}
            )

    def testlazyoptions(self):
        """tests that an eager relation can be upgraded to a lazy relation via the options method"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = False)
        ), echo = True)
        l = m.options(lazyload('addresses')).select()
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            {'user_id' : 8, 'addresses' : (Address, [{'address_id' : 2}, {'address_id' : 3}])},
            {'user_id' : 9, 'addresses' : (Address, [])}
            )
    
class LazyTest(AssertMixin):
    def setUp(self):
        globalidentity().clear()

    def testbasic(self):
        """tests a basic one-to-many lazy load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ), echo = True)
        l = m.select(users.c.user_id == 7)
        self.assert_result(l, User,
            {'user_id' : 7, 'addresses' : (Address, [{'address_id' : 1}])},
            )

    def testmanytomany(self):
        """tests a many-to-many lazy load"""
        items = orderitems

        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords, itemkeywords, lazy = True),
            ), echo = True)
        l = m.select()
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
            {'item_id' : 3, 'keywords' : (Keyword, [{'keyword_id' : 3}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 4, 'keywords' : (Keyword, [])},
            {'item_id' : 5, 'keywords' : (Keyword, [])}
        )

        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        self.assert_result(l, Item, 
            {'item_id' : 1, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 4}, {'keyword_id' : 6}])},
            {'item_id' : 2, 'keywords' : (Keyword, [{'keyword_id' : 2}, {'keyword_id' : 5}, {'keyword_id' : 7}])},
        )

class EagerTest(PersistTest):
    
    def setUp(self):
        globalidentity().clear()

    def testbasic(self):
        """tests a basic one-to-many eager load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = False),
        ), echo = True)
        l = m.select()
        print repr(l)

    def testwithrepeat(self):
        """tests a one-to-many eager load where we also query on joined criterion, where the joined
        criterion is using the same tables that are used within the eager load.  the mapper must insure that the 
        criterion doesnt interfere with the eager load criterion."""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, primaryjoin = users.c.user_id==addresses.c.user_id, lazy = False)
        ), echo = True)
        l = m.select(and_(addresses.c.email_address == 'ed@lala.com', addresses.c.user_id==users.c.user_id))
        print repr(l)

    def testcompile(self):
        """tests deferred operation of a pre-compiled mapper statement"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = False)
        ))
        s = m.compile(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.user_id))
        c = s.compile()
        print "\n" + str(c) + repr(c.get_params())
        
        l = m.instances(s.execute(emailad = 'jack@bean.com'))
        print repr(l)
        
    def testmulti(self):
        """tests eager loading with two relations simultaneously"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, primaryjoin = users.c.user_id==addresses.c.user_id, lazy = False),
            orders = relation(Order, orders, lazy = False),
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testdouble(self):
        """tests eager loading with two relations simulatneously, from the same table.  you
        have to use aliases for this less frequent type of operation."""
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        m = mapper(User, users, properties = dict(
            orders_open = relation(Order, openorders, primaryjoin = and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = False),
            orders_closed = relation(Order, closedorders, primaryjoin = and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = False)
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testnested(self):
        """tests eager loading, where one of the eager loaded items also eager loads its own 
        child items."""
        ordermapper = mapper(Order, orders, properties = dict(
                items = relation(Item, orderitems, lazy = False)
            ))

        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = False),
            orders = relation(ordermapper, primaryjoin = users.c.user_id==orders.c.user_id, lazy = False),
        ))
        l = m.select()
        print repr(l)
    
    def testmanytomany(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords, itemkeywords, lazy = False),
            ), echo = True)
        l = m.select()
        print repr(l)
        
        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        print repr(l)
    
    def testoneandmany(self):
        items = orderitems

        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords, itemkeywords, lazy = False),
            ))
        m = mapper(Order, orders, properties = dict(
                items = relation(m, lazy = False)
            ), echo = True)
        l = m.select("orders.order_id in (1,2,3)")
        print repr(l)

class SaveTest(PersistTest):
        

    def testsave(self):
        # save two users
        u = User()
        u.user_name = 'savetester'
        u2 = User()
        u2.user_name = 'savetester2'
        m = mapper(User, users, echo=True)
        m.save(u)
        m.save(u2)

        # assert the first one retreives the same from the identity map
        nu = m.get(u.user_id)
        self.assert_(u is nu)

        # clear out the identity map, so next get forces a SELECT
        m.identitymap.clear()

        # check it again, identity should be different but ids the same
        nu = m.get(u.user_id)
        self.assert_(u is not nu and u.user_id == nu.user_id and nu.user_name == 'savetester')

        # change first users name and save
        u.user_name = 'modifiedname'
        m.save(u)

        # select both
        userlist = m.select(users.c.user_id.in_(u.user_id, u2.user_id))
        # making a slight assumption here about the IN clause mechanics with regards to ordering
        self.assert_(u.user_id == userlist[0].user_id and userlist[0].user_name == 'modifiedname')
        self.assert_(u2.user_id == userlist[1].user_id and userlist[1].user_name == 'savetester2')

    def testsavemultitable(self):
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, table = users, echo = True, properties = dict(email = ColumnProperty(addresses.c.email_address), foo_id = ColumnProperty(users.c.user_id, addresses.c.user_id)))
        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'
        m.save(u)

        usertable = engine.ResultProxy(users.select(users.c.user_id.in_(10)).execute()).fetchall()
        self.assert_(usertable[0].row == (10, 'multitester'))
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(4)).execute()).fetchall()
        self.assert_(addresstable[0].row == (4, 10, 'multi@test.org'))

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        m.save(u)
        usertable = engine.ResultProxy(users.select(users.c.user_id.in_(10)).execute()).fetchall()
        self.assert_(usertable[0].row == (10, 'imnew'))
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(4)).execute()).fetchall()
        self.assert_(addresstable[0].row == (4, 10, 'lala@hey.com'))

if __name__ == "__main__":
    unittest.main()

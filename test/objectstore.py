from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy.mapper import *
import StringIO
import sqlalchemy.objectstore as objectstore
import testbase

from tables import *
import tables


class HistoryTest(AssertMixin):
    def testattr(self):
        m = mapper(User, users, properties = dict(addresses = relation(Address, addresses)))
        u = User()
        u.user_id = 7
        u.user_name = 'afdas'
        u.addresses.append(Address())
        u.addresses[0].email_address = 'hi'
        u.addresses.append(Address())
        u.addresses[1].email_address = 'there'
        self.echo(repr(u.__dict__))
        self.echo(repr(u.addresses))
        objectstore.uow().rollback_object(u)
        self.echo(repr(u.__dict__))
        
class SaveTest(AssertMixin):

    def setUpAll(self):
        db.echo = False
        tables.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        db.commit()
        tables.drop()
        db.echo = testbase.echo
        
    def setUp(self):
        db.echo = False
        # remove all history/identity maps etc.
        objectstore.clear()
        # remove all mapperes
        clear_mappers()
        keywords.insert().execute(
            dict(name='blue'),
            dict(name='red'),
            dict(name='green'),
            dict(name='big'),
            dict(name='small'),
            dict(name='round'),
            dict(name='square')
        )
        db.commit()        
        db.echo = testbase.echo

    def tearDown(self):
        db.echo = False
        db.commit()
        tables.delete()
        db.echo = testbase.echo
        
    def testbasic(self):
        # save two users
        u = User()
        u.user_name = 'savetester'

        m = mapper(User, users)
        u2 = User()
        u2.user_name = 'savetester2'

        objectstore.uow().register_new(u)
        
        objectstore.uow().commit(u)
        objectstore.uow().commit()

        # assert the first one retreives the same from the identity map
        nu = m.get(u.user_id)
        self.echo( "U: " + repr(u) + "NU: " + repr(nu))
        self.assert_(u is nu)
        
        # clear out the identity map, so next get forces a SELECT
        objectstore.clear()

        # check it again, identity should be different but ids the same
        nu = m.get(u.user_id)
        self.assert_(u is not nu and u.user_id == nu.user_id and nu.user_name == 'savetester')

        # change first users name and save
        u.user_name = 'modifiedname'
        objectstore.uow().commit()

        # select both
        #objectstore.clear()
        userlist = m.select(users.c.user_id.in_(u.user_id, u2.user_id), order_by=[users.c.user_name])
        print repr(u.user_id), repr(userlist[0].user_id), repr(userlist[0].user_name)
        self.assert_(u.user_id == userlist[0].user_id and userlist[0].user_name == 'modifiedname')
        self.assert_(u2.user_id == userlist[1].user_id and userlist[1].user_name == 'savetester2')

    def testmultitable(self):
        """tests a save of an object where each instance spans two tables. also tests
        redefinition of the keynames for the column properties."""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, primarytable = users,  
            properties = dict(
                email = ColumnProperty(addresses.c.email_address), 
                foo_id = ColumnProperty(users.c.user_id, addresses.c.user_id)
                )
            )
            
        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'

        objectstore.uow().commit()

        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assert_(usertable[0].row == (u.foo_id, 'multitester'))
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assert_(addresstable[0].row == (u.address_id, u.foo_id, 'multi@test.org'))

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        objectstore.uow().commit()

        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assert_(usertable[0].row == (u.foo_id, 'imnew'))
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assert_(addresstable[0].row == (u.address_id, u.foo_id, 'lala@hey.com'))

        u = m.select(users.c.user_id==u.foo_id)[0]
        self.echo( repr(u.__dict__))

    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(Address, addresses, lazy = True, uselist = False)
        ))
        u = User()
        u.user_name = 'one2onetester'
        u.address = Address()
        u.address.email_address = 'myonlyaddress@foo.com'
        objectstore.uow().commit()
        u.user_name = 'imnew'
        objectstore.uow().commit()
        u.address.email_address = 'imnew@foo.com'
        objectstore.uow().commit()

    def testdelete(self):
        m = mapper(User, users, properties = dict(
            address = relation(Address, addresses, lazy = True, uselist = False, private = False)
        ))
        u = User()
        a = Address()
        u.user_name = 'one2onetester'
        u.address = a
        u.address.email_address = 'myonlyaddress@foo.com'
        objectstore.uow().commit()
        self.echo("\n\n\n")
        objectstore.uow().register_deleted(u)
        objectstore.uow().commit()
        self.assert_(a.address_id is not None and a.user_id is None and not objectstore.uow().identity_map.has_key(u._instance_key) and objectstore.uow().identity_map.has_key(a._instance_key))

    def testcascadingdelete(self):
        m = mapper(User, users, properties = dict(
            address = relation(Address, addresses, lazy = False, uselist = False, private = True),
            orders = relation(
                mapper(Order, orders, properties = dict (
                    items = relation(Item, orderitems, lazy = False, uselist =True, private = True)
                )), 
                lazy = True, uselist = True, private = True)
        ))

        data = [User,
            {'user_name' : 'ed', 
                'address' : (Address, {'email_address' : 'foo@bar.com'}),
                'orders' : (Order, [
                    {'description' : 'eds 1st order', 'items' : (Item, [{'item_name' : 'eds o1 item'}, {'item_name' : 'eds other o1 item'}])}, 
                    {'description' : 'eds 2nd order', 'items' : (Item, [{'item_name' : 'eds o2 item'}, {'item_name' : 'eds other o2 item'}])}
                 ])
            },
            {'user_name' : 'jack', 
                'address' : (Address, {'email_address' : 'jack@jack.com'}),
                'orders' : (Order, [
                    {'description' : 'jacks 1st order', 'items' : (Item, [{'item_name' : 'im a lumberjack'}, {'item_name' : 'and im ok'}])}
                 ])
            },
            {'user_name' : 'foo', 
                'address' : (Address, {'email_address': 'hi@lala.com'}),
                'orders' : (Order, [
                    {'description' : 'foo order', 'items' : (Item, [])}, 
                    {'description' : 'foo order 2', 'items' : (Item, [{'item_name' : 'hi'}])}, 
                    {'description' : 'foo order three', 'items' : (Item, [{'item_name' : 'there'}])}
                ])
            }        
        ]
        
        for elem in data[1:]:
            u = User()
            u.user_name = elem['user_name']
            u.address = Address()
            u.address.email_address = elem['address'][1]['email_address']
            u.orders = []
            for order in elem['orders'][1]:
                o = Order()
                o.isopen = None
                o.description = order['description']
                u.orders.append(o)
                o.items = []
                for item in order['items'][1]:
                    i = Item()
                    i.item_name = item['item_name']
                    o.items.append(i)
                
        objectstore.uow().commit()
        objectstore.clear()

        l = m.select()
        for u in l:
            self.echo( repr(u.orders))
        self.assert_result(l, data[0], *data[1:])
        
        self.echo("\n\n\n")
        objectstore.uow().register_deleted(l[0])
        objectstore.uow().register_deleted(l[2])
        objectstore.commit()
        return
        res = self.capture_exec(db, lambda: objectstore.uow().commit())
        state = None
        
        for line in res.split('\n'):
            if line == "DELETE FROM items WHERE items.item_id = :item_id":
                self.assert_(state is None or state == 'addresses')
            elif line == "DELETE FROM orders WHERE orders.order_id = :order_id":
                state = 'orders'
            elif line == "DELETE FROM email_addresses WHERE email_addresses.address_id = :address_id":
                if state is None:
                    state = 'addresses'
            elif line == "DELETE FROM users WHERE users.user_id = :user_id":
                self.assert_(state is not None)
        
    def testbackwardsonetoone(self):
        # test 'backwards'
#        m = mapper(Address, addresses, properties = dict(
#            user = relation(User, users, foreignkey = addresses.c.user_id, primaryjoin = users.c.user_id == addresses.c.user_id, lazy = True, uselist = False)
#        ))
        # TODO: put assertion in here !!!
        m = mapper(Address, addresses, properties = dict(
            user = relation(User, users, lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
            {'user_name' : 'n4knd' , 'email_address' : 'asf3@bar.org'},
            {'user_name' : 'v88f4' , 'email_address' : 'adsd5@llala.net'},
            {'user_name' : 'asdf8d' , 'email_address' : 'theater@foo.com'}
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)
            
        objectstore.uow().commit()
        objects[2].email_address = 'imnew@foo.bar'
        objects[3].user = User()
        objects[3].user.user_name = 'imnewlyadded'
        self.assert_sql(db, lambda: objectstore.uow().commit(), [
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'imnewlyadded'}
                ),
                (
                    "UPDATE email_addresses SET user_id=:user_id, email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id",
                    lambda: [
                        {'email_address': 'imnew@foo.bar', 'user_id': objects[2].user.user_id, 'email_addresses_address_id': objects[2].address_id}, 
                        {'email_address': 'adsd5@llala.net', 'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}
                    ]
                )
        ])
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.address_id, addresses.c.address_id==a.address_id)).execute()
        self.echo( repr(l.fetchone().row))

    def testbackwardsnonmatch(self):
        u2 = Table('users_nm', db,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(20)),
        )

        a2 = Table('email_addresses_nm', db,
            Column('address_id', Integer, primary_key = True),
            Column('rel_user_id', Integer, ForeignKey(u2.c.user_id)),
            Column('email_address', String(20)),
        )
        u2.create()
        a2.create()
        m = mapper(Address, a2, properties = dict(
            user = relation(User, u2, lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)
        self.assert_sql(db, lambda: objectstore.commit(), [
                (
                    "INSERT INTO users_nm (user_name) VALUES (:user_name)",
                    {'user_name': 'thesub'}
                ),
                (
                    "INSERT INTO users_nm (user_name) VALUES (:user_name)",
                    {'user_name': 'assdkfj'}
                ),
                (
                "INSERT INTO email_addresses_nm (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 1, 'email_address': 'bar@foo.com'}
                ),
                (
                "INSERT INTO email_addresses_nm (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 2, 'email_address': 'thesdf@asdf.com'}
                )
                ]
        )
        a2.drop()
        u2.drop()
        db.commit()
        
        

    def testonetomany(self):
        """test basic save of one to many."""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ))
        u = User()
        u.user_name = 'one2manytester'
        u.addresses = []
        a = Address()
        a.email_address = 'one2many@test.org'
        u.addresses.append(a)
        a2 = Address()
        a2.email_address = 'lala@test.org'
        u.addresses.append(a2)
        self.echo( repr(u.addresses))
        self.echo( repr(u.addresses.added_items()))
        objectstore.uow().commit()

        usertable = users.select(users.c.user_id.in_(u.user_id)).execute().fetchall()
        self.assert_(usertable[0].row == (u.user_id, 'one2manytester'))
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id), order_by=[addresses.c.email_address]).execute().fetchall()
        self.assert_(addresstable[0].row == (a2.address_id, u.user_id, 'lala@test.org'))
        self.assert_(addresstable[1].row == (a.address_id, u.user_id, 'one2many@test.org'))

        userid = u.user_id
        addressid = a2.address_id
        
        a2.email_address = 'somethingnew@foo.com'

        objectstore.uow().commit()

        
        addresstable = addresses.select(addresses.c.address_id == addressid).execute().fetchall()
        self.assert_(addresstable[0].row == (addressid, userid, 'somethingnew@foo.com'))
        self.assert_(u.user_id == userid and a2.address_id == addressid)

    def testmapperswitch(self):
        """test that, if we change mappers, the new one gets used fully.  not sure if 
        i want it to work that way, but probably."""
        users.insert().execute(
            dict(user_id = 7, user_name = 'jack'),
            dict(user_id = 8, user_name = 'ed'),
            dict(user_id = 9, user_name = 'fred')
        )
        db.commit()

        # mapper with just users table
        User.mapper = assignmapper(users)
        User.mapper.select()
        oldmapper = User.mapper
        # now a mapper with the users table plus a relation to the addresses
        User.mapper = assignmapper(users, properties = dict(
            addresses = relation(Address, addresses, lazy = False)
        ))
        self.assert_(oldmapper is not User.mapper)
        u = User.mapper.select()
        u[0].addresses.append(Address())
        u[0].addresses[0].email_address='hi'
        
        # insure that upon commit, the new mapper with the address relation is used
        self.assert_sql(db, lambda: objectstore.commit(), 
                [
                    (
                    "INSERT INTO email_addresses (user_id, email_address) VALUES (:user_id, :email_address)",
                    {'email_address': 'hi', 'user_id': 7}
                    ),
                ]
        )

    def testchildmanipulations(self):
        """digs deeper into modifying the child items of an object to insure the correct
        updates take place"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u1.addresses = []
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1.addresses.append(a1)
        u2 = User()
        u2.user_name = 'user2'
        u2.addresses = []
        a2 = Address()
        a2.email_address = 'emailaddress2'
        u2.addresses.append(a2)

        a3 = Address()
        a3.email_address = 'emailaddress3'

        objectstore.commit()
        
        self.echo("\n\n\n")
        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.user_name = 'user2modified'
        u1.addresses.append(a3)
        del u1.addresses[0]
        u1.addresses.foo = True
        self.assert_sql(db, lambda: objectstore.commit(), 
                [
                    (
                        "UPDATE users SET user_name=:user_name WHERE users.user_id = :users_user_id",
                        [{'users_user_id': u2.user_id, 'user_name': 'user2modified'}]
                    ),
                    (
                        "UPDATE email_addresses SET user_id=:user_id, email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id",
                        [
                            {'email_address': 'emailaddress3', 'user_id': u1.user_id, 'email_addresses_address_id': a3.address_id}, 
                            {'email_address': 'emailaddress1', 'user_id': None, 'email_addresses_address_id': a1.address_id}
                        ]
                    )
                ])

    def testbackwardsmanipulations(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(User, users, lazy = True, uselist = False)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'
        
        a1.user = u1
        objectstore.commit()

        self.echo("\n\n\n")
        objectstore.delete(u1)
        a1.user = None
        objectstore.commit()

    def _testalias(self):
        """tests that an alias of a table can be used in a mapper. 
        the mapper has to locate the original table and columns to keep it all straight."""
        ualias = Alias(users, 'ualias')
        m = mapper(User, ualias)
        u = User()
        u.user_name = 'testalias'
        m.save(u)
        
        u2 = m.select(ualias.c.user_id == u.user_id)[0]
        self.assert_(u2 is u)

    def _testremove(self):
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ))
        u = User()
        u.user_name = 'one2manytester'
        u.addresses = []
        a = Address()
        a.email_address = 'one2many@test.org'
        u.addresses.append(a)
        a2 = Address()
        a2.email_address = 'lala@test.org'
        u.addresses.append(a2)
        m.save(u)
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id)).execute().fetchall()
        self.echo( repr(addresstable[0].row))
        self.assert_(addresstable[0].row == (a.address_id, u.user_id, 'one2many@test.org'))
        self.assert_(addresstable[1].row == (a2.address_id, u.user_id, 'lala@test.org'))
        del u.addresses[1]
        m.save(u)
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id)).execute().fetchall()
        self.echo( repr(addresstable))
        self.assert_(addresstable[0].row == (a.address_id, u.user_id, 'one2many@test.org'))
        self.assert_(addresstable[1].row == (a2.address_id, None, 'lala@test.org'))

    def testmanytomany(self):
        items = orderitems

        items.select().execute()
        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords, itemkeywords, lazy = False),
            ))

        keywordmapper = mapper(Keyword, keywords)

        data = [Item,
            {'item_name': 'mm_item1', 'keywords' : (Keyword,[{'name': 'big'},{'name': 'green'}, {'name': 'purple'},{'name': 'round'}])},
            {'item_name': 'mm_item2', 'keywords' : (Keyword,[{'name':'blue'}, {'name':'imnew'},{'name':'round'}, {'name':'small'}])},
            {'item_name': 'mm_item3', 'keywords' : (Keyword,[])},
            {'item_name': 'mm_item4', 'keywords' : (Keyword,[{'name':'big'}, {'name':'blue'},])},
            {'item_name': 'mm_item5', 'keywords' : (Keyword,[{'name':'big'},{'name':'exacting'},{'name':'green'}])},
            {'item_name': 'mm_item6', 'keywords' : (Keyword,[{'name':'red'},{'name':'round'},{'name':'small'}])},
        ]
        objects = []
        for elem in data[1:]:
            item = Item()
            objects.append(item)
            item.item_name = elem['item_name']
            item.keywords = []
            if len(elem['keywords'][1]):
                klist = keywordmapper.select(keywords.c.name.in_(*[e['name'] for e in elem['keywords'][1]]))
            else:
                klist = []
            khash = {}
            for k in klist:
                khash[k.name] = k
            for kname in [e['name'] for e in elem['keywords'][1]]:
                try:
                    k = khash[kname]
                except KeyError:
                    k = Keyword()
                    k.name = kname
                item.keywords.append(k)

        objectstore.uow().commit()
        
        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
        self.assert_result(l, *data)

        print "\n\n\n"
        objects[4].item_name = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql(db, lambda:objectstore.commit(), [
            (
                "INSERT INTO keywords (name) VALUES (:name)", 
                {'name': 'yellow'}
            ),
            (
                "UPDATE items SET order_id=:order_id, item_name=:item_name WHERE items.item_id = :items_item_id",
                [{'item_name': 'item4updated', 'order_id': None, 'items_item_id': objects[4].item_id}]
            ),
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ])

        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].keyword_id
        del objects[5].keywords[1]
        self.assert_sql(db, lambda:objectstore.commit(), [
                (
                    "DELETE FROM itemkeywords WHERE itemkeywords.item_id = :item_id AND itemkeywords.keyword_id = :keyword_id",
                    [{'item_id': objects[5].item_id, 'keyword_id': dkid}]
                ),
                (   
                    "INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
                    lambda: [{'item_id': objects[2].item_id, 'keyword_id': k.keyword_id}]
                )
        ])
        
    def testassociation(self):
        class IKAssociation(object):
            def __repr__(self):
                return "\nIKAssociation " + repr(self.item_id) + " " + repr(self.keyword)

        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, items, properties = dict(
                keywords = relation(IKAssociation, itemkeywords, lazy = False, properties = dict(
                    keyword = relation(Keyword, keywords, lazy = False, uselist = False)
                ), primary_keys = [itemkeywords.c.item_id, itemkeywords.c.keyword_id])
            ))

        data = [Item,
            {'item_name': 'a_item1', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'green'})}, 
                                                        {'keyword' : (Keyword, {'name': 'purple'})},
                                                        {'keyword' : (Keyword, {'name': 'round'})}
                                                    ]
                                                 ) 
            },
            {'item_name': 'a_item2', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'huge'})},
                                                        {'keyword' : (Keyword, {'name': 'violet'})}, 
                                                        {'keyword' : (Keyword, {'name': 'yellow'})}
                                                    ]
                                                 ) 
            },
            {'item_name': 'a_item3', 'keywords' : (IKAssociation, 
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'blue'})}, 
                                                    ]
                                                 ) 
            }
        ]
        for elem in data[1:]:
            item = Item()
            item.item_name = elem['item_name']
            item.keywords = []
            for kname in [e['keyword'][1]['name'] for e in elem['keywords'][1]]:
                try:
                    k = keywordmapper.select(keywords.c.name == kname)[0]
                except IndexError:
                    k = Keyword()
                    k.name= kname
                ik = IKAssociation()
                ik.keyword = k
                item.keywords.append(ik)

        objectstore.uow().commit()
        objectstore.clear()
        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
        self.assert_result(l, *data)

    def testbidirectional(self):
        m1 = mapper(User, users, properties={
            'addresses':relation(Address, addresses, lazy=True, private=True)
        }, is_primary=True)
        
        m2 = mapper(Address, addresses, properties = dict(
            user = relation(User, users, lazy = False)
        ), is_primary=True)
 
        u = User()
        print repr(u.__dict__.get('addresses', None))
        u.user_name = 'test'
        a = Address()
        a.email_address = 'testaddress'
        a.user = u
        objectstore.commit()
        print repr(u.__dict__.get('addresses', None))
#        objectstore.clear()
        objectstore.delete(u)
        objectstore.commit()
        

if __name__ == "__main__":
    unittest.main()

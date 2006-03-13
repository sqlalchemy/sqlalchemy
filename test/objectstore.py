from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables

class HistoryTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        users.create()
        addresses.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        addresses.drop()
        users.drop()
        db.echo = testbase.echo
    def setUp(self):
        objectstore.clear()
        clear_mappers()
        
    def testattr(self):
        """tests the rolling back of scalar and list attributes.  this kind of thing
        should be tested mostly in attributes.py which tests independently of the ORM 
        objects, but I think here we are going for
        the Mapper not interfering with it."""
        m = mapper(User, users, properties = dict(addresses = relation(mapper(Address, addresses))))
        u = User()
        u.user_id = 7
        u.user_name = 'afdas'
        u.addresses.append(Address())
        u.addresses[0].email_address = 'hi'
        u.addresses.append(Address())
        u.addresses[1].email_address = 'there'
        data = [User,
            {'user_name' : 'afdas',
             'addresses' : (Address, [{'email_address':'hi'}, {'email_address':'there'}])
            },
        ]
        self.assert_result([u], data[0], *data[1:])

        self.echo(repr(u.addresses))
        objectstore.uow().rollback_object(u)
        data = [User,
            {'user_name' : None,
             'addresses' : (Address, [])
            },
        ]
        self.assert_result([u], data[0], *data[1:])

    def testbackref(self):
        class User(object):pass
        class Address(object):pass
        am = mapper(Address, addresses)
        m = mapper(User, users, properties = dict(
            addresses = relation(am, backref='user', lazy=False))
        )
        
        u = User()
        a = Address()
        a.user = u
        #print repr(a.__class__._attribute_manager.get_history(a, 'user').added_items())
        #print repr(u.addresses.added_items())
        self.assert_(u.addresses == [a])
        objectstore.commit()

        objectstore.clear()
        u = m.select()[0]
        print u.addresses[0].user

class SessionTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        users.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        users.drop()
        db.echo = testbase.echo
    def setUp(self):
        objectstore.get_session().clear()
        clear_mappers()
        tables.user_data()
        #db.echo = "debug"
    def tearDown(self):
        tables.delete_user_data()
        
    def test_nested_begin_commit(self):
        """test nested session.begin/commit"""
        class User(object):pass
        m = mapper(User, users)
        def name_of(id):
            return users.select(users.c.user_id == id).execute().fetchone().user_name
        name1 = "Oliver Twist"
        name2 = 'Mr. Bumble'
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        s = objectstore.get_session()
        trans = s.begin()
        trans2 = s.begin()
        m.get(7).user_name = name1
        trans3 = s.begin()
        m.get(8).user_name = name2
        trans3.commit()
        s.commit() # should do nothing
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans2.commit()
        s.commit()  # should do nothing
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans.commit()
        self.assert_(name_of(7) == name1, msg="user_name should be %s" % name1)
        self.assert_(name_of(8) == name2, msg="user_name should be %s" % name2)

    def test_nested_rollback(self):
        class User(object):pass
        m = mapper(User, users)
        def name_of(id):
            return users.select(users.c.user_id == id).execute().fetchone().user_name
        name1 = "Oliver Twist"
        name2 = 'Mr. Bumble'
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        s = objectstore.get_session()
        trans = s.begin()
        trans2 = s.begin()
        m.get(7).user_name = name1
        trans3 = s.begin()
        m.get(8).user_name = name2
        trans3.rollback()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans2.commit()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)
        trans.commit()
        self.assert_(name_of(7) != name1, msg="user_name should not be %s" % name1)
        self.assert_(name_of(8) != name2, msg="user_name should not be %s" % name2)

class UnicodeTest(AssertMixin):
    def setUpAll(self):
        global uni_table
        uni_table = Table('uni_test', db,
            Column('id',  Integer, primary_key=True),
            Column('txt', Unicode(50))).create()

    def tearDownAll(self):
        uni_table.drop()
        uni_table.deregister()

    def testbasic(self):
        class Test(object):
            pass
        assign_mapper(Test, uni_table)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(id=1, txt = txt)
        self.assert_(t1.txt == txt)
        objectstore.commit()
        self.assert_(t1.txt == txt)


class PKTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        global table
        global table2
        global table3
        table = Table(
            'multi', db, 
            Column('multi_id', Integer, Sequence("multi_id_seq", optional=True), primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('value', String(100))
        )
        
        table2 = Table('multi2', db,
            Column('pk_col_1', String(30), primary_key=True),
            Column('pk_col_2', String(30), primary_key=True),
            Column('data', String(30), )
            )
        table3 = Table('multi3', db,
            Column('pri_code', String(30), key='primary', primary_key=True),
            Column('sec_code', String(30), key='secondary', primary_key=True),
            Column('date_assigned', Date, key='assigned', primary_key=True),
            Column('data', String(30), )
            )
        table.create()
        table2.create()
        table3.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        table.drop()
        table2.drop()
        table3.drop()
        db.echo = testbase.echo
    def setUp(self):
        objectstore.clear()
        clear_mappers()
    def testprimarykey(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table)
        e = Entry()
        e.name = 'entry1'
        e.value = 'this is entry 1'
        e.multi_rev = 2
        objectstore.commit()
        objectstore.clear()
        e2 = Entry.mapper.get(e.multi_id, 2)
        self.assert_(e is not e2 and e._instance_key == e2._instance_key)
    def testmanualpk(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table2)
        e = Entry()
        e.pk_col_1 = 'pk1'
        e.pk_col_2 = 'pk1_related'
        e.data = 'im the data'
        objectstore.commit()
    def testkeypks(self):
        import datetime
        class Entity(object):
            pass
        Entity.mapper = mapper(Entity, table3)
        e = Entity()
        e.primary = 'pk1'
        e.secondary = 'pk2'
        e.assigned = datetime.date.today()
        e.data = 'some more data'
        objectstore.commit()
        
class DefaultTest(AssertMixin):
    """tests that when saving objects whose table contains DefaultGenerators, either python-side, preexec or database-side,
    the newly saved instances receive all the default values either through a post-fetch or getting the pre-exec'ed 
    defaults back from the engine."""
    def setUpAll(self):
        #db.echo = 'debug'
        use_string_defaults = db.engine.__module__.endswith('postgres') or db.engine.__module__.endswith('oracle') or db.engine.__module__.endswith('sqlite')

        if use_string_defaults:
            hohotype = String(30)
            self.hohoval = "im hoho"
            self.althohoval = "im different hoho"
        else:
            hohotype = Integer
            self.hohoval = 9
            self.althohoval = 15
        self.table = Table('default_test', db,
        Column('id', Integer, Sequence("dt_seq", optional=True), primary_key=True),
        Column('hoho', hohotype, PassiveDefault(str(self.hohoval))),
        Column('counter', Integer, PassiveDefault("7")),
        Column('foober', String(30), default="im foober", onupdate="im the update")
        )
        self.table.create()
    def tearDownAll(self):
        self.table.drop()
    def setUp(self):
        self.table = Table('default_test', db)
    def testinsert(self):
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho(hoho=self.althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=self.althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober='im the new foober')
        objectstore.commit()
        self.assert_(h1.hoho==self.althohoval)
        self.assert_(h3.hoho==self.althohoval)
        self.assert_(h2.hoho==h4.hoho==h5.hoho==self.hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        self.assert_(h5.foober=='im the new foober')
        objectstore.clear()
        l = Hoho.mapper.select()
        (h1, h2, h3, h4, h5) = l
        self.assert_(h1.hoho==self.althohoval)
        self.assert_(h3.hoho==self.althohoval)
        self.assert_(h2.hoho==h4.hoho==h5.hoho==self.hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        self.assert_(h5.foober=='im the new foober')
    
    def testinsertnopostfetch(self):
        # populates the PassiveDefaults explicitly so there is no "post-update"
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho(hoho="15", counter="15")
        objectstore.commit()
        self.assert_(h1.hoho=="15")
        self.assert_(h1.counter=="15")
        self.assert_(h1.foober=="im foober")
        
    def testupdate(self):
        class Hoho(object):pass
        assign_mapper(Hoho, self.table)
        h1 = Hoho()
        objectstore.commit()
        self.assert_(h1.foober == 'im foober')
        h1.counter = 19
        objectstore.commit()
        self.assert_(h1.foober == 'im the update')
        
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

        self.assert_(len(objectstore.uow().new) == 0)
        self.assert_(len(objectstore.uow().dirty) == 0)
        self.assert_(len(objectstore.uow().modified_lists) == 0)
        
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

    def testinherits(self):
        m1 = mapper(User, users)
        
        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and joins on the user_id column
        AddressUser.mapper = mapper(
                AddressUser,
                addresses, inherits=m1
                )
        
        au = AddressUser()
        objectstore.commit()
        objectstore.clear()
        l = AddressUser.mapper.selectone()
        self.assert_(l.user_id == au.user_id and l.address_id == au.address_id)
    
    def testmultitable(self):
        """tests a save of an object where each instance spans two tables. also tests
        redefinition of the keynames for the column properties."""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        print usersaddresses._get_col_by_original(users.c.user_id)
        print repr(usersaddresses._orig_cols)
        m = mapper(User, usersaddresses, primarytable = users,  
            properties = dict(
                email = addresses.c.email_address, 
                foo_id = [users.c.user_id, addresses.c.user_id],
                )
            )
            
        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'

        objectstore.uow().commit()

        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'multitester'])
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'multi@test.org'])

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        objectstore.uow().commit()

        usertable = users.select(users.c.user_id.in_(u.foo_id)).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'imnew'])
        addresstable = addresses.select(addresses.c.address_id.in_(u.address_id)).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'lala@hey.com'])

        u = m.select(users.c.user_id==u.foo_id)[0]
        self.echo( repr(u.__dict__))

    def testonetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False)
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
            address = relation(mapper(Address, addresses), lazy = True, uselist = False, private = False)
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
            address = relation(mapper(Address, addresses), lazy = False, uselist = False, private = True),
            orders = relation(
                mapper(Order, orders, properties = dict (
                    items = relation(mapper(Item, orderitems), lazy = False, uselist =True, private = True)
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
            user = relation(mapper(User, users), lazy = True, uselist = False)
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
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda: [{'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}]
                ,
                
                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda: [{'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}]
                },
                
        ],
        with_sequences=[
                (
                    "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                    lambda:{'user_name': 'imnewlyadded', 'user_id':db.last_inserted_ids()[0]}
                ),
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda: [{'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}]
                ,
                
                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda: [{'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}]
                },
                
        ])
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.address_id, addresses.c.address_id==a.address_id)).execute()
        self.echo( repr(l.fetchone().values()))

        

    def testonetomany(self):
        """test basic save of one to many."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
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
        self.assertEqual(usertable[0].values(), [u.user_id, 'one2manytester'])
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id), order_by=[addresses.c.email_address]).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [a2.address_id, u.user_id, 'lala@test.org'])
        self.assertEqual(addresstable[1].values(), [a.address_id, u.user_id, 'one2many@test.org'])

        userid = u.user_id
        addressid = a2.address_id
        
        a2.email_address = 'somethingnew@foo.com'

        objectstore.uow().commit()

        
        addresstable = addresses.select(addresses.c.address_id == addressid).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [addressid, userid, 'somethingnew@foo.com'])
        self.assert_(u.user_id == userid and a2.address_id == addressid)

    def testmapperswitch(self):
        """test that, if we change mappers, the new one gets used fully. """
        users.insert().execute(
            dict(user_id = 7, user_name = 'jack'),
            dict(user_id = 8, user_name = 'ed'),
            dict(user_id = 9, user_name = 'fred')
        )
        db.commit()

        # mapper with just users table
        assign_mapper(User, users)
        User.mapper.select()
        oldmapper = User.mapper
        # now a mapper with the users table plus a relation to the addresses
        assign_mapper(User, users, is_primary=True, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False)
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
                ],
                with_sequences=[
                    (
                    "INSERT INTO email_addresses (address_id, user_id, email_address) VALUES (:address_id, :user_id, :email_address)",
                    lambda:{'email_address': 'hi', 'user_id': 7, 'address_id':db.last_inserted_ids()[0]}
                    ),
                ]
        )

    def testchildmanipulations(self):
        """digs deeper into modifying the child items of an object to insure the correct
        updates take place"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
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
        self.assert_sql(db, lambda: objectstore.commit(), 
                [
                    (
                        "UPDATE users SET user_name=:user_name WHERE users.user_id = :users_user_id",
                        [{'users_user_id': u2.user_id, 'user_name': 'user2modified'}]
                    ),
                    (
                        "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        [{'user_id': u1.user_id, 'email_addresses_address_id': a3.address_id}]
                    ),
                    ("UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        [{'user_id': None, 'email_addresses_address_id': a1.address_id}]
                    )
                ])

    def testbackwardsmanipulations(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
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
            addresses = relation(mapper(Address, addresses), lazy = True)
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
        self.echo( repr(addresstable[0].values()))
        self.assertEqual(addresstable[0].values(), [a.address_id, u.user_id, 'one2many@test.org'])
        self.assertEqual(addresstable[1].values(), [a2.address_id, u.user_id, 'lala@test.org'])
        del u.addresses[1]
        m.save(u)
        addresstable = addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id)).execute().fetchall()
        self.echo( repr(addresstable))
        self.assertEqual(addresstable[0].values(), [a.address_id, u.user_id, 'one2many@test.org'])
        self.assertEqual(addresstable[1].values(), [a2.address_id, None, 'lala@test.org'])

    def testmanytomany(self):
        items = orderitems

        items.select().execute()
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy = False),
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

        objects[4].item_name = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql(db, lambda:objectstore.commit(), [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                [{'item_name': 'item4updated', 'items_item_id': objects[4].item_id}]
            ,
                "INSERT INTO keywords (name) VALUES (:name)":
                {'name': 'yellow'}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ],
        
        with_sequences = [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                [{'item_name': 'item4updated', 'items_item_id': objects[4].item_id}]
            ,
                "INSERT INTO keywords (keyword_id, name) VALUES (:keyword_id, :name)":
                lambda: {'name': 'yellow', 'keyword_id':db.last_inserted_ids()[0]}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ]
        )
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
        
        objectstore.delete(objects[3])
        objectstore.commit()
        
    def testassociation(self):
        class IKAssociation(object):
            def __repr__(self):
                return "\nIKAssociation " + repr(self.item_id) + " " + repr(self.keyword)

        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        # note that we are breaking a rule here, making a second mapper(Keyword, keywords)
        # the reorganization of mapper construction affected this, but was fixed again
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(IKAssociation, itemkeywords, properties = dict(
                    keyword = relation(mapper(Keyword, keywords), lazy = False, uselist = False)
                ), primary_key = [itemkeywords.c.item_id, itemkeywords.c.keyword_id]),
                lazy = False)
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
        m1 = mapper(User, users, is_primary=True)
        
        m2 = mapper(Address, addresses, properties = dict(
            user = relation(m1, lazy = False, backref='addresses')
        ), is_primary=True)
        
 
        u = User()
        print repr(u.addresses)
        u.user_name = 'test'
        a = Address()
        a.email_address = 'testaddress'
        a.user = u
        objectstore.commit()
        print repr(u.addresses)
        print repr(u.addresses)
        x = False
        try:
            u.addresses.append('hi')
            x = True
        except:
            pass
            
        if x:
            self.assert_(False, "User addresses element should be read-only")
        
        objectstore.delete(u)
        objectstore.commit()

    def testdoublerelation(self):
        m2 = mapper(Address, addresses)
        m = mapper(User, users, properties={
            'boston_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Address.c.email_address.like('%boston%'))),
            'newyork_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Address.c.email_address.like('%newyork%'))),
        })
        u = User()
        a = Address()
        a.email_address = 'foo@boston.com'
        b = Address()
        b.email_address = 'bar@newyork.com'
        
        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)
        objectstore.commit()
    
class SaveTest2(AssertMixin):

    def setUp(self):
        db.echo = False
        objectstore.clear()
        clear_mappers()
        self.users = Table('users', db,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(20)),
            redefine=True
        )

        self.addresses = Table('email_addresses', db,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('rel_user_id', Integer, ForeignKey(self.users.c.user_id)),
            Column('email_address', String(20)),
            redefine=True
        )
        x = sql.Join(self.users, self.addresses)
#        raise repr(self.users) + repr(self.users.primary_key)
#        raise repr(self.addresses) + repr(self.addresses.foreign_keys)
        self.users.create()
        self.addresses.create()
        db.echo = testbase.echo

    def tearDown(self):
        db.echo = False
        self.addresses.drop()
        self.users.drop()
        db.echo = testbase.echo
    
    def testbackwardsnonmatch(self):
        m = mapper(Address, self.addresses, properties = dict(
            user = relation(mapper(User, self.users), lazy = True, uselist = False)
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
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'thesub'}
                ),
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'assdkfj'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 1, 'email_address': 'bar@foo.com'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 2, 'email_address': 'thesdf@asdf.com'}
                )
                ],
                
                with_sequences = [
                        (
                            "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda: {'user_name': 'thesub', 'user_id':db.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda: {'user_name': 'assdkfj', 'user_id':db.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda:{'rel_user_id': 1, 'email_address': 'bar@foo.com', 'address_id':db.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda:{'rel_user_id': 2, 'email_address': 'thesdf@asdf.com', 'address_id':db.last_inserted_ids()[0]}
                        )
                        ]
        )


if __name__ == "__main__":
    testbase.main()        

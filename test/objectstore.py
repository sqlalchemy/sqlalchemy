from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy.mapper import *
import sqlalchemy.objectstore as objectstore

#ECHO = True
ECHO = False
DATA = False
execfile("test/tables.py")

keywords.insert().execute(
    dict(keyword_id=1, name='blue'),
    dict(keyword_id=2, name='red'),
    dict(keyword_id=3, name='green'),
    dict(keyword_id=4, name='big'),
    dict(keyword_id=5, name='small'),
    dict(keyword_id=6, name='round'),
    dict(keyword_id=7, name='square')
)

db.connection().commit()

db.echo = True

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
        print repr(u.__dict__)
        print repr(u.addresses)
        objectstore.uow().rollback_object(u)
        print repr(u.__dict__)
        
class SaveTest(AssertMixin):

    def setUp(self):
        objectstore.clear()
        clear_mappers()
        
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
        self.assert_(u.user_id == userlist[0].user_id and userlist[0].user_name == 'modifiedname')
        self.assert_(u2.user_id == userlist[1].user_id and userlist[1].user_name == 'savetester2')

    def testmultitable(self):
        """tests a save of an object where each instance spans two tables. also tests
        redefinition of the keynames for the column properties."""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses, table = users,  
            properties = dict(
                email = ColumnProperty(addresses.c.email_address), 
                foo_id = ColumnProperty(users.c.user_id, addresses.c.user_id)
                )
            )
            
        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'

        objectstore.uow().commit()

        usertable = engine.ResultProxy(users.select(users.c.user_id.in_(u.foo_id)).execute()).fetchall()
        self.assert_(usertable[0].row == (u.foo_id, 'multitester'))
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(u.address_id)).execute()).fetchall()
        self.assert_(addresstable[0].row == (u.address_id, u.foo_id, 'multi@test.org'))

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        objectstore.uow().commit()

        usertable = engine.ResultProxy(users.select(users.c.user_id.in_(u.foo_id)).execute()).fetchall()
        self.assert_(usertable[0].row == (u.foo_id, 'imnew'))
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(u.address_id)).execute()).fetchall()
        self.assert_(addresstable[0].row == (u.address_id, u.foo_id, 'lala@hey.com'))

        u = m.select(users.c.user_id==u.foo_id)[0]
        print repr(u.__dict__)

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
            address = relation(Address, addresses, lazy = True, uselist = False)
        ))
        u = User()
        u.user_name = 'one2onetester'
        u.address = Address()
        u.address.email_address = 'myonlyaddress@foo.com'
        objectstore.uow().commit()

        print "OK"
        objectstore.uow().register_deleted(u)
        objectstore.uow().commit()
        
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
        
        objectstore.uow().commit()
        return
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.address_id, addresses.c.address_id==a.address_id)).execute()
        r = engine.ResultProxy(l)
        print repr(r.fetchone().row)
        
    def testonetomany(self):
        """test basic save of one to many."""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy = True)
        ))
        print "TESTONETOMANY, mapper is " + repr(id(m))
        u = User()
        u.user_name = 'one2manytester'
        u.addresses = []
        a = Address()
        a.email_address = 'one2many@test.org'
        u.addresses.append(a)
        a2 = Address()
        a2.email_address = 'lala@test.org'
        u.addresses.append(a2)
        print repr(u.addresses)
        print repr(u.addresses.added_items())
        objectstore.uow().commit()

        usertable = engine.ResultProxy(users.select(users.c.user_id.in_(u.user_id)).execute()).fetchall()
        self.assert_(usertable[0].row == (u.user_id, 'one2manytester'))
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id), order_by=[addresses.c.email_address]).execute()).fetchall()
        self.assert_(addresstable[0].row == (a2.address_id, u.user_id, 'lala@test.org'))
        self.assert_(addresstable[1].row == (a.address_id, u.user_id, 'one2many@test.org'))

        userid = u.user_id
        addressid = a2.address_id
        
        a2.email_address = 'somethingnew@foo.com'

        objectstore.uow().commit()
        
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id == addressid).execute()).fetchall()
        self.assert_(addresstable[0].row == (addressid, userid, 'somethingnew@foo.com'))
        self.assert_(u.user_id == userid and a2.address_id == addressid)

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
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id)).execute()).fetchall()
        print repr(addresstable[0].row)
        self.assert_(addresstable[0].row == (a.address_id, u.user_id, 'one2many@test.org'))
        self.assert_(addresstable[1].row == (a2.address_id, u.user_id, 'lala@test.org'))
        del u.addresses[1]
        m.save(u)
        addresstable = engine.ResultProxy(addresses.select(addresses.c.address_id.in_(a.address_id, a2.address_id)).execute()).fetchall()
        print repr(addresstable)
        self.assert_(addresstable[0].row == (a.address_id, u.user_id, 'one2many@test.org'))
        self.assert_(addresstable[1].row == (a2.address_id, None, 'lala@test.org'))

    def testmanytomany(self):
        items = orderitems

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
        print "OK!"
        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
        self.assert_result(l, *data)
        print "OK!"

        objects[4].item_name = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        
        objectstore.uow().commit()
        print "OK!"
        return
        objects[2].keywords.append(k)
        print "added: " + repr(objects[2].keywords.added_items())
        objectstore.uow().commit()
        
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

        l = m.select(items.c.item_name.in_(*[e['item_name'] for e in data[1:]]), order_by=[items.c.item_name, keywords.c.name])
        self.assert_result(l, *data)

if __name__ == "__main__":
    unittest.main()

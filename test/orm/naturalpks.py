import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exceptions

from testlib.fixtures import *
from testlib import *

"""test primary key changing capabilities and passive/non-passive cascading updates."""

class NaturalPKTest(ORMTest):
    def define_tables(self, metadata):
        global users, addresses, items, users_to_items
        
        users = Table('users', metadata,
            Column('username', String(50), primary_key=True),
            Column('fullname', String(100)))
            
        addresses = Table('addresses', metadata,
            Column('email', String(50), primary_key=True),
            Column('username', String(50), ForeignKey('users.username', onupdate="cascade")))
            
        items = Table('items', metadata,
            Column('itemname', String(50), primary_key=True),
            Column('description', String(100)))
            
        users_to_items = Table('userstoitems', metadata,
            Column('username', String(50), ForeignKey('users.username', onupdate='cascade'), primary_key=True),
            Column('itemname', String(50), ForeignKey('items.itemname', onupdate='cascade'), primary_key=True),
        )
        
    def test_entity(self):
        mapper(User, users)
        
        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        
        sess.save(u1)
        sess.flush()
        assert sess.get(User, 'jack') is u1
        
        u1.username = 'ed'
        sess.flush()
        
        def go():
            assert sess.get(User, 'ed') is u1
        self.assert_sql_count(testbase.db, go, 0)

        assert sess.get(User, 'jack') is None
        
        sess.clear()
        u1 = sess.query(User).get('ed')
        self.assertEquals(User(username='ed', fullname='jack'), u1)

    def test_expiry(self):
        mapper(User, users)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')

        sess.save(u1)
        sess.flush()
        assert sess.get(User, 'jack') is u1

        users.update(values={u1.c.username:'jack'}).execute(username='ed')
        
        try:
            # expire/refresh works off of primary key.  the PK is gone
            # in this case so theres no way to look it up.  criterion-
            # based session invalidation could solve this [ticket:911]
            sess.expire(u1)
            u1.username
            assert False
        except exceptions.InvalidRequestError, e:
            assert "Could not refresh instance" in str(e)

        sess.clear()
        assert sess.get(User, 'jack') is None
        assert sess.get(User, 'ed').fullname == 'jack'
    
    @testing.unsupported('sqlite','mysql')
    def test_onetomany_passive(self):
        self._test_onetomany(True)
    
    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)
        
    def _test_onetomany(self, passive_updates):
        mapper(User, users, properties={
            'addresses':relation(Address, passive_updates=passive_updates)
        })
        mapper(Address, addresses)
    
        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        u1.addresses.append(Address(email='jack1'))
        u1.addresses.append(Address(email='jack2'))
        sess.save(u1)
        sess.flush()
    
        assert sess.get(Address, 'jack1') is u1.addresses[0]
    
        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'
        
        sess.clear()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())
        
        u1 = sess.get(User, 'ed')
        u1.username = 'jack'
        def go():
            sess.flush()
        if not passive_updates:
            self.assert_sql_count(testbase.db, go, 4) # test passive_updates=False; load addresses, update user, update 2 addresses
        else:
            self.assert_sql_count(testbase.db, go, 1) # test passive_updates=True; update user
        sess.clear()
        assert User(username='jack', addresses=[Address(username='jack'), Address(username='jack')]) == sess.get(User, 'jack')
    
        u1 = sess.get(User, 'jack')
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.clear()
        assert sess.get(Address, 'jack1').username is None
        u1 = sess.get(User, 'fred')
        self.assertEquals(User(username='fred', fullname='jack'), u1)

    @testing.unsupported('sqlite', 'mysql')
    def test_manytoone_passive(self):
        self._test_manytoone(True)

    def test_manytoone_nonpassive(self):
        self._test_manytoone(False)
    
    def _test_manytoone(self, passive_updates):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relation(User, passive_updates=passive_updates)
        })
        
        sess = create_session()
        a1 = Address(email='jack1')
        a2 = Address(email='jack2')
        
        u1 = User(username='jack', fullname='jack')
        a1.user = u1
        a2.user = u1
        sess.save(a1)
        sess.save(a2)
        sess.flush()
        
        u1.username = 'ed'
        
        print id(a1), id(a2), id(u1)
        print u1._state.parents
        def go():
            sess.flush()
        if passive_updates:
            self.assert_sql_count(testbase.db, go, 1)
        else:
            self.assert_sql_count(testbase.db, go, 3)
        
        def go():
            sess.flush()
        self.assert_sql_count(testbase.db, go, 0)
        
        assert a1.username == a2.username == 'ed'
        sess.clear()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

    @testing.unsupported('sqlite', 'mysql')
    def test_bidirectional_passive(self):
        self._test_bidirectional(True)

    def test_bidirectional_nonpassive(self):
        self._test_bidirectional(False)

    def _test_bidirectional(self, passive_updates):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relation(User, passive_updates=passive_updates, backref='addresses')
        })

        sess = create_session()
        a1 = Address(email='jack1')
        a2 = Address(email='jack2')

        u1 = User(username='jack', fullname='jack')
        a1.user = u1
        a2.user = u1
        sess.save(a1)
        sess.save(a2)
        sess.flush()

        u1.username = 'ed'
        (ad1, ad2) = sess.query(Address).all()
        self.assertEquals([Address(username='jack'), Address(username='jack')], [ad1, ad2])
        def go():
            sess.flush()
        if passive_updates:
            self.assert_sql_count(testbase.db, go, 1)
        else:
            self.assert_sql_count(testbase.db, go, 3)
        self.assertEquals([Address(username='ed'), Address(username='ed')], [ad1, ad2])
        sess.clear()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.get(User, 'ed')
        assert len(u1.addresses) == 2    # load addresses
        u1.username = 'fred'
        print "--------------------------------"
        def go():
            sess.flush()
        # check that the passive_updates is on on the other side
        if passive_updates:
            self.assert_sql_count(testbase.db, go, 1)
        else:
            self.assert_sql_count(testbase.db, go, 3)
        sess.clear()
        self.assertEquals([Address(username='fred'), Address(username='fred')], sess.query(Address).all())
        
        
    @testing.unsupported('sqlite', 'mysql')
    def test_manytomany_passive(self):
        self._test_manytomany(True)
    
    def test_manytomany_nonpassive(self):
        self._test_manytomany(False)
        
    def _test_manytomany(self, passive_updates):
        mapper(User, users, properties={
            'items':relation(Item, secondary=users_to_items, backref='users', passive_updates=passive_updates)
        })
        mapper(Item, items)
        
        sess = create_session()
        u1 = User(username='jack')
        u2 = User(username='fred')
        i1 = Item(itemname='item1')
        i2 = Item(itemname='item2')
        
        u1.items.append(i1)
        u1.items.append(i2)
        i2.users.append(u2)
        sess.save(u1)
        sess.save(u2)
        sess.flush()

        r = sess.query(Item).all()
        # fixtures.Base can't handle a comparison with the backrefs involved....
        self.assertEquals(Item(itemname='item1'), r[0])
        self.assertEquals(['jack'], [u.username for u in r[0].users])
        self.assertEquals(Item(itemname='item2'), r[1])
        self.assertEquals(['jack', 'fred'], [u.username for u in r[1].users])
        
        u2.username='ed'
        def go():
            sess.flush()
        go()
        def go():
            sess.flush()
        self.assert_sql_count(testbase.db, go, 0)
        
        sess.clear()
        r = sess.query(Item).all()
        self.assertEquals(Item(itemname='item1'), r[0])
        self.assertEquals(['jack'], [u.username for u in r[0].users])
        self.assertEquals(Item(itemname='item2'), r[1])
        self.assertEquals(['ed', 'jack'], sorted([u.username for u in r[1].users]))
        
class SelfRefTest(ORMTest):
    def define_tables(self, metadata):
        global nodes, Node
        
        nodes = Table('nodes', metadata,
            Column('name', String(50), primary_key=True),
            Column('parent', String(50), ForeignKey('nodes.name', onupdate='cascade'))
            )
            
        class Node(Base):
            pass
    
    def test_onetomany(self):
        mapper(Node, nodes, properties={
            'children':relation(Node, backref=backref('parentnode', remote_side=nodes.c.name, passive_updates=False), passive_updates=False)
        })
        
        sess = create_session()
        n1 = Node(name='n1')
        n1.children.append(Node(name='n11'))
        n1.children.append(Node(name='n12'))
        n1.children.append(Node(name='n13'))
        sess.save(n1)
        sess.flush()
        
        n1.name = 'new n1'
        sess.flush()
        self.assertEquals(n1.children[1].parent, 'new n1')
        self.assertEquals(['new n1', 'new n1', 'new n1'], [n.parent for n in sess.query(Node).filter(Node.name.in_(['n11', 'n12', 'n13']))])
        
        
class NonPKCascadeTest(ORMTest):
    def define_tables(self, metadata):
        global users, addresses

        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('username', String(50), unique=True),
            Column('fullname', String(100)))

        addresses = Table('addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('email', String(50)),
            Column('username', String(50), ForeignKey('users.username', onupdate="cascade")))

    @testing.unsupported('sqlite','mysql')
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def _test_onetomany(self, passive_updates):
        mapper(User, users, properties={
            'addresses':relation(Address, passive_updates=passive_updates)
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        u1.addresses.append(Address(email='jack1'))
        u1.addresses.append(Address(email='jack2'))
        sess.save(u1)
        sess.flush()
        a1 = u1.addresses[0]
        
        self.assertEquals(select([addresses.c.username]).execute().fetchall(), [('jack',), ('jack',)])
        
        assert sess.get(Address, a1.id) is u1.addresses[0]

        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'
        self.assertEquals(select([addresses.c.username]).execute().fetchall(), [('ed',), ('ed',)])

        sess.clear()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.get(User, u1.id)
        u1.username = 'jack'
        def go():
            sess.flush()
        if not passive_updates:
            self.assert_sql_count(testbase.db, go, 4) # test passive_updates=False; load addresses, update user, update 2 addresses
        else:
            self.assert_sql_count(testbase.db, go, 1) # test passive_updates=True; update user
        sess.clear()
        assert User(username='jack', addresses=[Address(username='jack'), Address(username='jack')]) == sess.get(User, u1.id)
        sess.clear()
        
        u1 = sess.get(User, u1.id)
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.clear()
        a1 = sess.get(Address, a1.id)
        self.assertEquals(a1.username, None)

        self.assertEquals(select([addresses.c.username]).execute().fetchall(), [(None,), (None,)])

        u1 = sess.get(User, u1.id)
        self.assertEquals(User(username='fred', fullname='jack'), u1)

        
if __name__ == '__main__':
    testbase.main()
        
        

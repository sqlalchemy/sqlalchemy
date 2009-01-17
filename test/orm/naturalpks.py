"""
Primary key changing capabilities and passive/non-passive cascading updates.

"""
import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, create_session
from testlib.testing import eq_
from orm import _base

class NaturalPKTest(_base.MappedTest):

    def define_tables(self, metadata):
        users = Table('users', metadata,
            Column('username', String(50), primary_key=True),
            Column('fullname', String(100)))

        addresses = Table('addresses', metadata,
            Column('email', String(50), primary_key=True),
            Column('username', String(50), ForeignKey('users.username', onupdate="cascade")))

        items = Table('items', metadata,
            Column('itemname', String(50), primary_key=True),
            Column('description', String(100)))

        users_to_items = Table('users_to_items', metadata,
            Column('username', String(50), ForeignKey('users.username', onupdate='cascade'), primary_key=True),
            Column('itemname', String(50), ForeignKey('items.itemname', onupdate='cascade'), primary_key=True),
        )

    def setup_classes(self):
        class User(_base.ComparableEntity):
            pass
        class Address(_base.ComparableEntity):
            pass
        class Item(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_entity(self):
        mapper(User, users)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get('jack') is u1

        u1.username = 'ed'
        sess.flush()

        def go():
            assert sess.query(User).get('ed') is u1
        self.assert_sql_count(testing.db, go, 0)

        assert sess.query(User).get('jack') is None

        sess.expunge_all()
        u1 = sess.query(User).get('ed')
        self.assertEquals(User(username='ed', fullname='jack'), u1)

    @testing.resolve_artifact_names
    def test_load_after_expire(self):
        mapper(User, users)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get('jack') is u1

        users.update(values={User.username:'jack'}).execute(username='ed')

        # expire/refresh works off of primary key.  the PK is gone
        # in this case so theres no way to look it up.  criterion-
        # based session invalidation could solve this [ticket:911]
        sess.expire(u1)
        self.assertRaises(sa.orm.exc.ObjectDeletedError, getattr, u1, 'username')

        sess.expunge_all()
        assert sess.query(User).get('jack') is None
        assert sess.query(User).get('ed').fullname == 'jack'

    @testing.resolve_artifact_names
    def test_flush_new_pk_after_expire(self):
        mapper(User, users)
        sess = create_session()
        u1 = User(username='jack', fullname='jack')

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get('jack') is u1

        sess.expire(u1)
        u1.username = 'ed'
        sess.flush()
        sess.expunge_all()
        assert sess.query(User).get('ed').fullname == 'jack'
        

    @testing.fails_on('mysql', 'FIXME: unknown')
    @testing.fails_on('sqlite', 'FIXME: unknown')
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    @testing.resolve_artifact_names
    def _test_onetomany(self, passive_updates):
        mapper(User, users, properties={
            'addresses':relation(Address, passive_updates=passive_updates)
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        u1.addresses.append(Address(email='jack1'))
        u1.addresses.append(Address(email='jack2'))
        sess.add(u1)
        sess.flush()

        assert sess.query(Address).get('jack1') is u1.addresses[0]

        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'

        sess.expunge_all()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.query(User).get('ed')
        u1.username = 'jack'
        def go():
            sess.flush()
        if not passive_updates:
            self.assert_sql_count(testing.db, go, 4) # test passive_updates=False; load addresses, update user, update 2 addresses
        else:
            self.assert_sql_count(testing.db, go, 1) # test passive_updates=True; update user
        sess.expunge_all()
        assert User(username='jack', addresses=[Address(username='jack'), Address(username='jack')]) == sess.query(User).get('jack')

        u1 = sess.query(User).get('jack')
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.expunge_all()
        assert sess.query(Address).get('jack1').username is None
        u1 = sess.query(User).get('fred')
        self.assertEquals(User(username='fred', fullname='jack'), u1)
        

    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_manytoone_passive(self):
        self._test_manytoone(True)

    def test_manytoone_nonpassive(self):
        self._test_manytoone(False)

    @testing.resolve_artifact_names
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
        sess.add(a1)
        sess.add(a2)
        sess.flush()

        u1.username = 'ed'

        print id(a1), id(a2), id(u1)
        print sa.orm.attributes.instance_state(u1).parents
        def go():
            sess.flush()
        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)

        def go():
            sess.flush()
        self.assert_sql_count(testing.db, go, 0)

        assert a1.username == a2.username == 'ed'
        sess.expunge_all()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_bidirectional_passive(self):
        self._test_bidirectional(True)

    def test_bidirectional_nonpassive(self):
        self._test_bidirectional(False)

    @testing.resolve_artifact_names
    def _test_bidirectional(self, passive_updates):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relation(User, passive_updates=passive_updates,
                            backref='addresses')})

        sess = create_session()
        a1 = Address(email='jack1')
        a2 = Address(email='jack2')

        u1 = User(username='jack', fullname='jack')
        a1.user = u1
        a2.user = u1
        sess.add(a1)
        sess.add(a2)
        sess.flush()

        u1.username = 'ed'
        (ad1, ad2) = sess.query(Address).all()
        self.assertEquals([Address(username='jack'), Address(username='jack')], [ad1, ad2])
        def go():
            sess.flush()
        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        self.assertEquals([Address(username='ed'), Address(username='ed')], [ad1, ad2])
        sess.expunge_all()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.query(User).get('ed')
        assert len(u1.addresses) == 2    # load addresses
        u1.username = 'fred'
        print "--------------------------------"
        def go():
            sess.flush()
        # check that the passive_updates is on on the other side
        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        sess.expunge_all()
        self.assertEquals([Address(username='fred'), Address(username='fred')], sess.query(Address).all())


    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_manytomany_passive(self):
        self._test_manytomany(True)

    def test_manytomany_nonpassive(self):
        self._test_manytomany(False)

    @testing.resolve_artifact_names
    def _test_manytomany(self, passive_updates):
        mapper(User, users, properties={
            'items':relation(Item, secondary=users_to_items, backref='users',
                             passive_updates=passive_updates)})
        mapper(Item, items)

        sess = create_session()
        u1 = User(username='jack')
        u2 = User(username='fred')
        i1 = Item(itemname='item1')
        i2 = Item(itemname='item2')

        u1.items.append(i1)
        u1.items.append(i2)
        i2.users.append(u2)
        sess.add(u1)
        sess.add(u2)
        sess.flush()

        r = sess.query(Item).all()
        # ComparableEntity can't handle a comparison with the backrefs
        # involved....
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
        self.assert_sql_count(testing.db, go, 0)

        sess.expunge_all()
        r = sess.query(Item).all()
        self.assertEquals(Item(itemname='item1'), r[0])
        self.assertEquals(['jack'], [u.username for u in r[0].users])
        self.assertEquals(Item(itemname='item2'), r[1])
        self.assertEquals(['ed', 'jack'], sorted([u.username for u in r[1].users]))
        
        sess.expunge_all()
        u2 = sess.query(User).get(u2.username)
        u2.username='wendy'
        sess.flush()
        r = sess.query(Item).with_parent(u2).all()
        self.assertEquals(Item(itemname='item2'), r[0])


class SelfRefTest(_base.MappedTest):
    __unsupported_on__ = 'mssql' # mssql doesn't allow ON UPDATE on self-referential keys

    def define_tables(self, metadata):
        Table('nodes', metadata,
              Column('name', String(50), primary_key=True),
              Column('parent', String(50),
                     ForeignKey('nodes.name', onupdate='cascade')))

    def setup_classes(self):
        class Node(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_onetomany(self):
        mapper(Node, nodes, properties={
            'children': relation(Node,
                                 backref=sa.orm.backref('parentnode',
                                                        remote_side=nodes.c.name,
                                                        passive_updates=False),
                                 passive_updates=False)})

        sess = create_session()
        n1 = Node(name='n1')
        n1.children.append(Node(name='n11'))
        n1.children.append(Node(name='n12'))
        n1.children.append(Node(name='n13'))
        sess.add(n1)
        sess.flush()

        n1.name = 'new n1'
        sess.flush()
        eq_(n1.children[1].parent, 'new n1')
        eq_(['new n1', 'new n1', 'new n1'],
            [n.parent
             for n in sess.query(Node).filter(
                 Node.name.in_(['n11', 'n12', 'n13']))])


class NonPKCascadeTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('username', String(50), unique=True),
            Column('fullname', String(100)))

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True),
              Column('email', String(50)),
              Column('username', String(50),
                     ForeignKey('users.username', onupdate="cascade")))

    def setup_classes(self):
        class User(_base.ComparableEntity):
            pass
        class Address(_base.ComparableEntity):
            pass

    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    @testing.resolve_artifact_names
    def _test_onetomany(self, passive_updates):
        mapper(User, users, properties={
            'addresses':relation(Address, passive_updates=passive_updates)})
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        u1.addresses.append(Address(email='jack1'))
        u1.addresses.append(Address(email='jack2'))
        sess.add(u1)
        sess.flush()
        a1 = u1.addresses[0]

        self.assertEquals(sa.select([addresses.c.username]).execute().fetchall(), [('jack',), ('jack',)])

        assert sess.query(Address).get(a1.id) is u1.addresses[0]

        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'
        self.assertEquals(sa.select([addresses.c.username]).execute().fetchall(), [('ed',), ('ed',)])

        sess.expunge_all()
        self.assertEquals([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.query(User).get(u1.id)
        u1.username = 'jack'
        def go():
            sess.flush()
        if not passive_updates:
            self.assert_sql_count(testing.db, go, 4) # test passive_updates=False; load addresses, update user, update 2 addresses
        else:
            self.assert_sql_count(testing.db, go, 1) # test passive_updates=True; update user
        sess.expunge_all()
        assert User(username='jack', addresses=[Address(username='jack'), Address(username='jack')]) == sess.query(User).get(u1.id)
        sess.expunge_all()

        u1 = sess.query(User).get(u1.id)
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.expunge_all()
        a1 = sess.query(Address).get(a1.id)
        self.assertEquals(a1.username, None)

        self.assertEquals(sa.select([addresses.c.username]).execute().fetchall(), [(None,), (None,)])

        u1 = sess.query(User).get(u1.id)
        self.assertEquals(User(username='fred', fullname='jack'), u1)


if __name__ == '__main__':
    testenv.main()

"""
Primary key changing capabilities and passive/non-passive cascading updates.

"""
from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, relation, create_session
from sqlalchemy.test.testing import eq_
from test.orm import _base

class NaturalPKTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        users = Table('users', metadata,
            Column('username', String(50), primary_key=True),
            Column('fullname', String(100)),
            test_needs_fk=True)

        addresses = Table('addresses', metadata,
            Column('email', String(50), primary_key=True),
            Column('username', String(50), ForeignKey('users.username', onupdate="cascade")),
            test_needs_fk=True)

        items = Table('items', metadata,
            Column('itemname', String(50), primary_key=True),
            Column('description', String(100)), 
            test_needs_fk=True)

        users_to_items = Table('users_to_items', metadata,
            Column('username', String(50), ForeignKey('users.username', onupdate='cascade'), primary_key=True),
            Column('itemname', String(50), ForeignKey('items.itemname', onupdate='cascade'), primary_key=True),
            test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
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
        eq_(User(username='ed', fullname='jack'), u1)

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
        assert_raises(sa.orm.exc.ObjectDeletedError, getattr, u1, 'username')

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
        

    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
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
        eq_([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

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
        eq_(User(username='fred', fullname='jack'), u1)
        

    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
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
        eq_([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
    def test_onetoone_passive(self):
        self._test_onetoone(True)

    def test_onetoone_nonpassive(self):
        self._test_onetoone(False)

    @testing.resolve_artifact_names
    def _test_onetoone(self, passive_updates):
        mapper(User, users, properties={
            "address":relation(Address, passive_updates=passive_updates, uselist=False)
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        sess.add(u1)
        sess.flush()
        
        a1 = Address(email='jack1')
        u1.address = a1
        sess.add(a1)
        sess.flush()

        u1.username = 'ed'

        def go():
            sess.flush()
        if passive_updates:
            sess.expire(u1, ['address'])
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 2)

        def go():
            sess.flush()
        self.assert_sql_count(testing.db, go, 0)

        sess.expunge_all()
        eq_([Address(username='ed')], sess.query(Address).all())
        
    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
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
        eq_([Address(username='jack'), Address(username='jack')], [ad1, ad2])
        def go():
            sess.flush()
        if passive_updates:
            sess.expire(u1, ['addresses'])
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        eq_([Address(username='ed'), Address(username='ed')], [ad1, ad2])
        sess.expunge_all()
        eq_([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

        u1 = sess.query(User).get('ed')
        assert len(u1.addresses) == 2    # load addresses
        u1.username = 'fred'
        def go():
            sess.flush()
        # check that the passive_updates is on on the other side
        if passive_updates:
            sess.expire(u1, ['addresses'])
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        sess.expunge_all()
        eq_([Address(username='fred'), Address(username='fred')], sess.query(Address).all())


    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
    def test_manytomany_passive(self):
        self._test_manytomany(True)

    @testing.fails_on('mysql', 'the executemany() of the association table fails to report the correct row count')
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
        eq_(Item(itemname='item1'), r[0])
        eq_(['jack'], [u.username for u in r[0].users])
        eq_(Item(itemname='item2'), r[1])
        eq_(['jack', 'fred'], [u.username for u in r[1].users])

        u2.username='ed'
        def go():
            sess.flush()
        go()
        def go():
            sess.flush()
        self.assert_sql_count(testing.db, go, 0)

        sess.expunge_all()
        r = sess.query(Item).all()
        eq_(Item(itemname='item1'), r[0])
        eq_(['jack'], [u.username for u in r[0].users])
        eq_(Item(itemname='item2'), r[1])
        eq_(['ed', 'jack'], sorted([u.username for u in r[1].users]))
        
        sess.expunge_all()
        u2 = sess.query(User).get(u2.username)
        u2.username='wendy'
        sess.flush()
        r = sess.query(Item).with_parent(u2).all()
        eq_(Item(itemname='item2'), r[0])

class ReversePKsTest(_base.MappedTest):
    """reverse the primary keys of two entities and ensure bookkeeping succeeds."""
    
    
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'user', metadata,
            Column('code', Integer, primary_key=True),
            Column('status', Integer, primary_key=True),
            Column('username', String(50), nullable=False),
            )
    
    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            def __init__(self, code, status, username):
                self.code = code
                self.status = status
                self.username = username

    @testing.resolve_artifact_names
    def test_reverse(self):
        PUBLISHED, EDITABLE, ARCHIVED = 1, 2, 3
        
        mapper(User, user)

        session = sa.orm.sessionmaker()()
        
        a_published = User(1, PUBLISHED, u'a')
        session.add(a_published)
        session.commit()

        a_editable = User(1, EDITABLE, u'a')

        session.add(a_editable)
        session.commit()

        # do the switch in both directions - 
        # one or the other should raise the error
        # based on platform dictionary ordering
        a_published.status = ARCHIVED
        a_editable.status = PUBLISHED

        session.commit()
        assert session.query(User).get([1, PUBLISHED]) is a_editable
        assert session.query(User).get([1, ARCHIVED]) is a_published

        a_published.status = PUBLISHED
        a_editable.status = EDITABLE

        session.commit()

        assert session.query(User).get([1, PUBLISHED]) is a_published
        assert session.query(User).get([1, EDITABLE]) is a_editable

    
class SelfRefTest(_base.MappedTest):
    __unsupported_on__ = 'mssql' # mssql doesn't allow ON UPDATE on self-referential keys

    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
              Column('name', String(50), primary_key=True),
              Column('parent', String(50),
                     ForeignKey('nodes.name', onupdate='cascade')))

    @classmethod
    def setup_classes(cls):
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
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('username', String(50), unique=True),
            Column('fullname', String(100)),
            test_needs_fk=True)

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True),
              Column('email', String(50)),
              Column('username', String(50),
                     ForeignKey('users.username', onupdate="cascade")),
                     test_needs_fk=True
                     )

    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            pass
        class Address(_base.ComparableEntity):
            pass

    @testing.fails_on('sqlite', 'sqlite doesnt support ON UPDATE CASCADE')
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

        eq_(sa.select([addresses.c.username]).execute().fetchall(), [('jack',), ('jack',)])

        assert sess.query(Address).get(a1.id) is u1.addresses[0]

        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'
        eq_(sa.select([addresses.c.username]).execute().fetchall(), [('ed',), ('ed',)])

        sess.expunge_all()
        eq_([Address(username='ed'), Address(username='ed')], sess.query(Address).all())

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
        eq_(a1.username, None)

        eq_(sa.select([addresses.c.username]).execute().fetchall(), [(None,), (None,)])

        u1 = sess.query(User).get(u1.id)
        eq_(User(username='fred', fullname='jack'), u1)



"""
Primary key changing capabilities and passive/non-passive cascading updates.

"""

from sqlalchemy.testing import fixtures, eq_, ne_, assert_raises
import sqlalchemy as sa
from sqlalchemy import testing, Integer, String, ForeignKey
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, Session
from sqlalchemy.orm.session import make_transient
from test.orm import _fixtures


def _backend_specific_fk_args():
    if testing.requires.deferrable_fks.enabled:
        fk_args = dict(deferrable=True, initially='deferred')
    elif not testing.requires.on_update_cascade.enabled:
        fk_args = dict()
    else:
        fk_args = dict(onupdate='cascade')
    return fk_args


class NaturalPKTest(fixtures.MappedTest):
    # MySQL 5.5 on Windows crashes (the entire server, not the client)
    # if you screw around with ON UPDATE CASCADE type of stuff.
    __requires__ = 'skip_mysql_on_windows', 'on_update_or_deferrable_fks'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table('users', metadata,
            Column('username', String(50), primary_key=True),
            Column('fullname', String(100)),
            test_needs_fk=True)

        Table(
            'addresses', metadata,
            Column('email', String(50), primary_key=True),
            Column(
                'username', String(50),
                ForeignKey('users.username', **fk_args)),
            test_needs_fk=True)

        Table(
            'items', metadata,
            Column('itemname', String(50), primary_key=True),
            Column('description', String(100)),
            test_needs_fk=True)

        Table(
            'users_to_items', metadata,
            Column(
                'username', String(50),
                ForeignKey('users.username', **fk_args), primary_key=True),
            Column(
                'itemname', String(50),
                ForeignKey('items.itemname', **fk_args), primary_key=True),
            test_needs_fk=True)

    @classmethod
    def setup_classes(cls):

        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Item(cls.Comparable):
            pass

    def test_entity(self):
        users, User = self.tables.users, self.classes.User

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

    def test_load_after_expire(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')

        sess.add(u1)
        sess.flush()
        assert sess.query(User).get('jack') is u1

        users.update(values={User.username: 'jack'}).execute(username='ed')

        # expire/refresh works off of primary key.  the PK is gone
        # in this case so there's no way to look it up.  criterion-
        # based session invalidation could solve this [ticket:911]
        sess.expire(u1)
        assert_raises(sa.orm.exc.ObjectDeletedError, getattr, u1, 'username')

        sess.expunge_all()
        assert sess.query(User).get('jack') is None
        assert sess.query(User).get('ed').fullname == 'jack'

    def test_flush_new_pk_after_expire(self):
        User, users = self.classes.User, self.tables.users

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

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def _test_onetomany(self, passive_updates):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, passive_updates=passive_updates)})
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
        eq_(
            [Address(username='ed'), Address(username='ed')],
            sess.query(Address).all())

        u1 = sess.query(User).get('ed')
        u1.username = 'jack'

        def go():
            sess.flush()
        if not passive_updates:
            # test passive_updates=False;
            #load addresses, update user, update 2 addresses
            self.assert_sql_count(testing.db, go, 4)
        else:
            # test passive_updates=True; update user
            self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()
        assert User(
            username='jack', addresses=[
                Address(username='jack'),
                Address(username='jack')]) == sess.query(User).get('jack')

        u1 = sess.query(User).get('jack')
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.expunge_all()
        assert sess.query(Address).get('jack1').username is None
        u1 = sess.query(User).get('fred')
        eq_(User(username='fred', fullname='jack'), u1)

    @testing.requires.on_update_cascade
    def test_manytoone_passive(self):
        self._test_manytoone(True)

    def test_manytoone_nonpassive(self):
        self._test_manytoone(False)

    def test_manytoone_nonpassive_cold_mapping(self):
        """test that the mapper-level m2o dependency processor
        is set up even if the opposite side relationship
        hasn't yet been part of a flush.

        """
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        with testing.db.begin() as conn:
            conn.execute(users.insert(), username='jack', fullname='jack')
            conn.execute(addresses.insert(), email='jack1', username='jack')
            conn.execute(addresses.insert(), email='jack2', username='jack')

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User,
                    passive_updates=False)
        })

        sess = create_session()
        u1 = sess.query(User).first()
        a1, a2 = sess.query(Address).all()
        u1.username = 'ed'

        def go():
            sess.flush()
        self.assert_sql_count(testing.db, go, 3)

    def _test_manytoone(self, passive_updates):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, passive_updates=passive_updates)
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
        eq_(
            [Address(username='ed'), Address(username='ed')],
            sess.query(Address).all())

    @testing.requires.on_update_cascade
    def test_onetoone_passive(self):
        self._test_onetoone(True)

    def test_onetoone_nonpassive(self):
        self._test_onetoone(False)

    def _test_onetoone(self, passive_updates):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(
            User, users, properties={
                "address": relationship(
                    Address, passive_updates=passive_updates, uselist=False)})
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

    @testing.requires.on_update_cascade
    def test_bidirectional_passive(self):
        self._test_bidirectional(True)

    def test_bidirectional_nonpassive(self):
        self._test_bidirectional(False)

    def _test_bidirectional(self, passive_updates):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, passive_updates=passive_updates,
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
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        eq_([Address(username='ed'), Address(username='ed')], [ad1, ad2])
        sess.expunge_all()
        eq_(
            [Address(username='ed'), Address(username='ed')],
            sess.query(Address).all())

        u1 = sess.query(User).get('ed')
        assert len(u1.addresses) == 2    # load addresses
        u1.username = 'fred'

        def go():
            sess.flush()
        # check that the passive_updates is on on the other side
        if passive_updates:
            self.assert_sql_count(testing.db, go, 1)
        else:
            self.assert_sql_count(testing.db, go, 3)
        sess.expunge_all()
        eq_(
            [Address(username='fred'), Address(username='fred')],
            sess.query(Address).all())

    @testing.requires.on_update_cascade
    def test_manytomany_passive(self):
        self._test_manytomany(True)

    @testing.requires.non_updating_cascade
    @testing.requires.sane_multi_rowcount.not_()
    def test_manytomany_nonpassive(self):
        self._test_manytomany(False)

    def _test_manytomany(self, passive_updates):
        users, items, Item, User, users_to_items = (self.tables.users,
                                self.tables.items,
                                self.classes.Item,
                                self.classes.User,
                                self.tables.users_to_items)

        mapper(
            User, users, properties={
                'items': relationship(
                    Item, secondary=users_to_items, backref='users',
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

        u2.username = 'ed'

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
        u2.username = 'wendy'
        sess.flush()
        r = sess.query(Item).with_parent(u2).all()
        eq_(Item(itemname='item2'), r[0])


class TransientExceptionTesst(_fixtures.FixtureTest):
    run_inserts = None
    __backend__ = True

    def test_transient_exception(self):
        """An object that goes from a pk value to transient/pending
        doesn't count as a "pk" switch.

        """

        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users)
        mapper(Address, addresses, properties={'user': relationship(User)})

        sess = create_session()
        u1 = User(id=5, name='u1')
        ad1 = Address(email_address='e1', user=u1)
        sess.add_all([u1, ad1])
        sess.flush()

        make_transient(u1)
        u1.id = None
        u1.username = 'u2'
        sess.add(u1)
        sess.flush()

        eq_(ad1.user_id, 5)

        sess.expire_all()
        eq_(ad1.user_id, 5)
        ne_(u1.id, 5)
        ne_(u1.id, None)
        eq_(sess.query(User).count(), 2)


class ReversePKsTest(fixtures.MappedTest):
    """reverse the primary keys of two entities and ensure bookkeeping
    succeeds."""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'user', metadata,
            Column('code', Integer, autoincrement=False, primary_key=True),
            Column('status', Integer, autoincrement=False, primary_key=True),
            Column('username', String(50), nullable=False),
            test_needs_acid=True
            )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            def __init__(self, code, status, username):
                self.code = code
                self.status = status
                self.username = username

    def test_reverse(self):
        user, User = self.tables.user, self.classes.User

        PUBLISHED, EDITABLE, ARCHIVED = 1, 2, 3

        mapper(User, user)

        session = sa.orm.sessionmaker()()

        a_published = User(1, PUBLISHED, 'a')
        session.add(a_published)
        session.commit()

        a_editable = User(1, EDITABLE, 'a')

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

    @testing.requires.savepoints
    def test_reverse_savepoint(self):
        user, User = self.tables.user, self.classes.User

        PUBLISHED, EDITABLE, ARCHIVED = 1, 2, 3

        mapper(User, user)

        session = sa.orm.sessionmaker()()

        a_published = User(1, PUBLISHED, 'a')
        session.add(a_published)
        session.commit()

        a_editable = User(1, EDITABLE, 'a')

        session.add(a_editable)
        session.commit()

        # testing #3108
        session.begin_nested()

        a_published.status = ARCHIVED
        a_editable.status = PUBLISHED

        session.commit()

        session.rollback()
        eq_(a_published.status, PUBLISHED)
        eq_(a_editable.status, EDITABLE)


class SelfReferentialTest(fixtures.MappedTest):
    # mssql, mysql don't allow
    # ON UPDATE on self-referential keys
    __unsupported_on__ = ('mssql', 'mysql')

    __requires__ = 'on_update_or_deferrable_fks',
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            'nodes', metadata,
            Column('name', String(50), primary_key=True),
            Column('parent', String(50), ForeignKey('nodes.name', **fk_args)),
            test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            pass

    def test_one_to_many_on_m2o(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node, nodes, properties={
                'children': relationship(
                    Node,
                    backref=sa.orm.backref(
                        'parentnode', remote_side=nodes.c.name,
                        passive_updates=False),
                    )})

        sess = Session()
        n1 = Node(name='n1')
        sess.add(n1)
        n2 = Node(name='n11', parentnode=n1)
        n3 = Node(name='n12', parentnode=n1)
        n4 = Node(name='n13', parentnode=n1)
        sess.add_all([n2, n3, n4])
        sess.commit()

        n1.name = 'new n1'
        sess.commit()
        eq_(['new n1', 'new n1', 'new n1'],
            [n.parent
             for n in sess.query(Node).filter(
                 Node.name.in_(['n11', 'n12', 'n13']))])

    def test_one_to_many_on_o2m(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node, nodes, properties={
                'children': relationship(
                    Node,
                    backref=sa.orm.backref(
                        'parentnode', remote_side=nodes.c.name),
                    passive_updates=False)})

        sess = Session()
        n1 = Node(name='n1')
        n1.children.append(Node(name='n11'))
        n1.children.append(Node(name='n12'))
        n1.children.append(Node(name='n13'))
        sess.add(n1)
        sess.commit()

        n1.name = 'new n1'
        sess.commit()
        eq_(n1.children[1].parent, 'new n1')
        eq_(['new n1', 'new n1', 'new n1'],
            [n.parent
             for n in sess.query(Node).filter(
                 Node.name.in_(['n11', 'n12', 'n13']))])

    @testing.requires.on_update_cascade
    def test_many_to_one_passive(self):
        self._test_many_to_one(True)

    def test_many_to_one_nonpassive(self):
        self._test_many_to_one(False)

    def _test_many_to_one(self, passive):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node, nodes, properties={
                'parentnode': relationship(
                    Node, remote_side=nodes.c.name, passive_updates=passive)}
        )

        sess = Session()
        n1 = Node(name='n1')
        n11 = Node(name='n11', parentnode=n1)
        n12 = Node(name='n12', parentnode=n1)
        n13 = Node(name='n13', parentnode=n1)
        sess.add_all([n1, n11, n12, n13])
        sess.commit()

        n1.name = 'new n1'
        sess.commit()
        eq_(
            ['new n1', 'new n1', 'new n1'],
            [
                n.parent for n in sess.query(Node).filter(
                    Node.name.in_(['n11', 'n12', 'n13']))])


class NonPKCascadeTest(fixtures.MappedTest):
    __requires__ = 'skip_mysql_on_windows', 'on_update_or_deferrable_fks'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            'users', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('username', String(50), unique=True),
            Column('fullname', String(100)),
            test_needs_fk=True)

        Table(
            'addresses', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('email', String(50)),
            Column(
                'username', String(50),
                ForeignKey('users.username', **fk_args)),
            test_needs_fk=True)

    @classmethod
    def setup_classes(cls):

        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def _test_onetomany(self, passive_updates):
        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, passive_updates=passive_updates)})
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(username='jack', fullname='jack')
        u1.addresses.append(Address(email='jack1'))
        u1.addresses.append(Address(email='jack2'))
        sess.add(u1)
        sess.flush()
        a1 = u1.addresses[0]

        eq_(
            sa.select([addresses.c.username]).execute().fetchall(),
            [('jack',), ('jack',)])

        assert sess.query(Address).get(a1.id) is u1.addresses[0]

        u1.username = 'ed'
        sess.flush()
        assert u1.addresses[0].username == 'ed'
        eq_(
            sa.select([addresses.c.username]).execute().fetchall(),
            [('ed',), ('ed',)])

        sess.expunge_all()
        eq_(
            [Address(username='ed'), Address(username='ed')],
            sess.query(Address).all())

        u1 = sess.query(User).get(u1.id)
        u1.username = 'jack'

        def go():
            sess.flush()
        if not passive_updates:
            # test passive_updates=False; load addresses,
            #  update user, update 2 addresses
            self.assert_sql_count(testing.db, go, 4)
        else:
            # test passive_updates=True; update user
            self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()
        assert User(
            username='jack', addresses=[
                Address(username='jack'),
                Address(username='jack')]) == sess.query(User).get(u1.id)
        sess.expunge_all()

        u1 = sess.query(User).get(u1.id)
        u1.addresses = []
        u1.username = 'fred'
        sess.flush()
        sess.expunge_all()
        a1 = sess.query(Address).get(a1.id)
        eq_(a1.username, None)

        eq_(
            sa.select([addresses.c.username]).execute().fetchall(),
            [(None,), (None,)])

        u1 = sess.query(User).get(u1.id)
        eq_(User(username='fred', fullname='jack'), u1)


class CascadeToFKPKTest(fixtures.MappedTest, testing.AssertsCompiledSQL):
    """A primary key mutation cascades onto a foreign key that is itself a
    primary key."""
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table('users', metadata,
            Column('username', String(50), primary_key=True),
            test_needs_fk=True)

        Table(
            'addresses', metadata,
            Column(
                'username', String(50),
                ForeignKey('users.username', **fk_args),
                primary_key=True),
            Column('email', String(50), primary_key=True),
            Column('etc', String(50)),
            test_needs_fk=True)

    @classmethod
    def setup_classes(cls):

        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @testing.requires.on_update_cascade
    def test_onetomany_passive(self):
        self._test_onetomany(True)

    @testing.requires.non_updating_cascade
    def test_onetomany_nonpassive(self):
        self._test_onetomany(False)

    def test_o2m_change_passive(self):
        self._test_o2m_change(True)

    def test_o2m_change_nonpassive(self):
        self._test_o2m_change(False)

    def _test_o2m_change(self, passive_updates):
        """Change the PK of a related entity to another.

        "on update cascade" is not involved here, so the mapper has
        to do the UPDATE itself.

        """

        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, passive_updates=passive_updates)})
        mapper(Address, addresses)

        sess = create_session()
        a1 = Address(username='ed', email='ed@host1')
        u1 = User(username='ed', addresses=[a1])
        u2 = User(username='jack')

        sess.add_all([a1, u1, u2])
        sess.flush()

        a1.username = 'jack'
        sess.flush()

    def test_o2m_move_passive(self):
        self._test_o2m_move(True)

    def test_o2m_move_nonpassive(self):
        self._test_o2m_move(False)

    def _test_o2m_move(self, passive_updates):
        """Move the related entity to a different collection,
        changing its PK.

        """

        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, passive_updates=passive_updates)})
        mapper(Address, addresses)

        sess = create_session()
        a1 = Address(username='ed', email='ed@host1')
        u1 = User(username='ed', addresses=[a1])
        u2 = User(username='jack')

        sess.add_all([a1, u1, u2])
        sess.flush()

        u1.addresses.remove(a1)
        u2.addresses.append(a1)
        sess.flush()

    @testing.requires.on_update_cascade
    def test_change_m2o_passive(self):
        self._test_change_m2o(True)

    @testing.requires.non_updating_cascade
    def test_change_m2o_nonpassive(self):
        self._test_change_m2o(False)

    def _test_change_m2o(self, passive_updates):
        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, passive_updates=passive_updates)
        })

        sess = create_session()
        u1 = User(username='jack')
        a1 = Address(user=u1, email='foo@bar')
        sess.add_all([u1, a1])
        sess.flush()

        u1.username = 'edmodified'
        sess.flush()
        eq_(a1.username, 'edmodified')

        sess.expire_all()
        eq_(a1.username, 'edmodified')

    def test_move_m2o_passive(self):
        self._test_move_m2o(True)

    def test_move_m2o_nonpassive(self):
        self._test_move_m2o(False)

    def _test_move_m2o(self, passive_updates):
        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        # tests [ticket:1856]
        mapper(User, users)
        mapper(
            Address, addresses, properties={
                'user': relationship(User, passive_updates=passive_updates)})

        sess = create_session()
        u1 = User(username='jack')
        u2 = User(username='ed')
        a1 = Address(user=u1, email='foo@bar')
        sess.add_all([u1, u2, a1])
        sess.flush()

        a1.user = u2
        sess.flush()

    def test_rowswitch_doesntfire(self):
        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, passive_updates=True)
        })

        sess = create_session()
        u1 = User(username='ed')
        a1 = Address(user=u1, email='ed@host1')

        sess.add(u1)
        sess.add(a1)
        sess.flush()

        sess.delete(u1)
        sess.delete(a1)

        u2 = User(username='ed')
        a2 = Address(user=u2, email='ed@host1', etc='foo')
        sess.add(u2)
        sess.add(a2)

        from sqlalchemy.testing.assertsql import CompiledSQL

        # test that the primary key columns of addresses are not
        # being updated as well, since this is a row switch.
        self.assert_sql_execution(
            testing.db, sess.flush, CompiledSQL(
                "UPDATE addresses SET etc=:etc WHERE "
                "addresses.username = :addresses_username AND"
                " addresses.email = :addresses_email", {
                    'etc': 'foo', 'addresses_username': 'ed',
                    'addresses_email': 'ed@host1'}),
        )

    def _test_onetomany(self, passive_updates):
        """Change the PK of a related entity via foreign key cascade.

        For databases that require "on update cascade", the mapper
        has to identify the row by the new value, not the old, when
        it does the update.

        """

        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, passive_updates=passive_updates)})
        mapper(Address, addresses)

        sess = create_session()
        a1, a2 = Address(username='ed', email='ed@host1'), \
            Address(username='ed', email='ed@host2')
        u1 = User(username='ed', addresses=[a1, a2])
        sess.add(u1)
        sess.flush()
        eq_(a1.username, 'ed')
        eq_(a2.username, 'ed')
        eq_(
            sa.select([addresses.c.username]).execute().fetchall(),
            [('ed',), ('ed',)])

        u1.username = 'jack'
        a2.email = 'ed@host3'
        sess.flush()

        eq_(a1.username, 'jack')
        eq_(a2.username, 'jack')
        eq_(
            sa.select([addresses.c.username]).execute().fetchall(),
            [('jack',), ('jack', )])


class JoinedInheritanceTest(fixtures.MappedTest):
    """Test cascades of pk->pk/fk on joined table inh."""

    # mssql doesn't allow ON UPDATE on self-referential keys
    __unsupported_on__ = ('mssql',)

    __requires__ = 'skip_mysql_on_windows',
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        fk_args = _backend_specific_fk_args()

        Table(
            'person', metadata,
            Column('name', String(50), primary_key=True),
            Column('type', String(50), nullable=False),
            test_needs_fk=True)

        Table(
            'engineer', metadata,
            Column(
                'name', String(50), ForeignKey('person.name', **fk_args),
                primary_key=True),
            Column('primary_language', String(50)),
            Column(
                'boss_name', String(50),
                ForeignKey('manager.name', **fk_args)),
            test_needs_fk=True
        )

        Table(
            'manager', metadata, Column('name', String(50),
            ForeignKey('person.name', **fk_args), primary_key=True),
            Column('paperwork', String(50)), test_needs_fk=True
        )

    @classmethod
    def setup_classes(cls):

        class Person(cls.Comparable):
            pass

        class Engineer(Person):
            pass

        class Manager(Person):
            pass

    @testing.requires.on_update_cascade
    def test_pk_passive(self):
        self._test_pk(True)

    @testing.requires.non_updating_cascade
    def test_pk_nonpassive(self):
        self._test_pk(False)

    @testing.requires.on_update_cascade
    def test_fk_passive(self):
        self._test_fk(True)

    # PG etc. need passive=True to allow PK->PK cascade
    @testing.requires.non_updating_cascade
    def test_fk_nonpassive(self):
        self._test_fk(False)

    def _test_pk(self, passive_updates):
        Person, Manager, person, manager, Engineer, engineer = (
            self.classes.Person, self.classes.Manager, self.tables.person,
            self.tables.manager, self.classes.Engineer, self.tables.engineer)

        mapper(
            Person, person, polymorphic_on=person.c.type,
            polymorphic_identity='person', passive_updates=passive_updates)
        mapper(
            Engineer, engineer, inherits=Person,
            polymorphic_identity='engineer', properties={
                'boss': relationship(
                    Manager,
                    primaryjoin=manager.c.name == engineer.c.boss_name,
                    passive_updates=passive_updates)})
        mapper(
            Manager, manager, inherits=Person, polymorphic_identity='manager')

        sess = sa.orm.sessionmaker()()

        e1 = Engineer(name='dilbert', primary_language='java')
        sess.add(e1)
        sess.commit()
        e1.name = 'wally'
        e1.primary_language = 'c++'
        sess.commit()

    def _test_fk(self, passive_updates):
        Person, Manager, person, manager, Engineer, engineer = (
            self.classes.Person, self.classes.Manager, self.tables.person,
            self.tables.manager, self.classes.Engineer, self.tables.engineer)

        mapper(
            Person, person, polymorphic_on=person.c.type,
            polymorphic_identity='person', passive_updates=passive_updates)
        mapper(
            Engineer, engineer, inherits=Person,
            polymorphic_identity='engineer', properties={
                'boss': relationship(
                    Manager,
                    primaryjoin=manager.c.name == engineer.c.boss_name,
                    passive_updates=passive_updates)})
        mapper(
            Manager, manager, inherits=Person, polymorphic_identity='manager')

        sess = sa.orm.sessionmaker()()

        m1 = Manager(name='dogbert', paperwork='lots')
        e1, e2 = Engineer(name='dilbert', primary_language='java', boss=m1),\
            Engineer(name='wally', primary_language='c++', boss=m1)
        sess.add_all([
            e1, e2, m1
        ])
        sess.commit()

        eq_(e1.boss_name, 'dogbert')
        eq_(e2.boss_name, 'dogbert')
        sess.expire_all()

        m1.name = 'pointy haired'
        e1.primary_language = 'scala'
        e2.primary_language = 'cobol'
        sess.commit()

        eq_(e1.boss_name, 'pointy haired')
        eq_(e2.boss_name, 'pointy haired')
